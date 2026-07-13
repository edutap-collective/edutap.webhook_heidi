# Changelog

All notable changes to this project are documented here.

## unreleased

### Features

- Settings (pydantic-settings) für Webhook-Secret, Signatur-Toleranz und Kafka.
- Datenmodelle `WebhookEvent`/`WebhookEventData` (Envelope, bewusst lax
  validiert, `extra="allow"`) und `QueueMessage` inkl. `from_event()` für die
  Pass-Queue-Nachricht.
- HMAC-SHA256-Signaturprüfung (`signing.sign`/`signing.verify`, Stripe-Stil,
  `Heidi-Signature`-Header) gegen die rohen Body-Bytes — nicht gegen
  re-serialisiertes JSON, damit erneut signierte Retries mit anderer
  Key-Reihenfolge verifizierbar bleiben.
- `QueueBackend`-Protocol (`protocols.py`) mit `enqueue`/`consume`/`ack`/`stop`
  — beide Seiten der Pass-Queue in einer Abstraktion, damit der Consumer
  (LMU-Spooler) weder aiokafka noch Offsets kennen muss. Backend-Auswahl
  per setuptools-Entry-Point (`plugins.py`, Gruppe
  `edutap.webhook_heidi.plugins`), analog zu `edutap.wallet_google`/
  `edutap.wallet_apple`; `get_queue_backend()` liefert eine gecachte Instanz,
  `add_plugin()`/`reset_queue_backend()` dienen Tests und Einbettung.
- `InMemoryQueueBackend` (`queues/memory.py`) für Tests und lokale
  Entwicklung ohne Broker; dedupliziert bewusst nicht — das ist Aufgabe des
  Consumers.
- `KafkaQueueBackend` (`queues/kafka.py`, Extra `[kafka]`) — das produktive
  Queue-Backend: der Webhook-Endpoint schreibt Pass-Events hinein
  (`enqueue`), der LMU-Spooler liest sie heraus (`consume`/`ack`). Producer
  idempotent mit `acks="all"` — wir antworten heidi.cloud erst 2xx, wenn der
  Broker den Write bestätigt hat; ein verlorener Produce-Call vor dem Ack
  wäre ein endgültig verlorenes Event, da der Sender nur bei Non-2xx
  wiederholt (analog zu `heidi.cloud/kafka/base.py`). Partition-Key ist
  `passid`, nicht `eventid`: Kafka garantiert Reihenfolge nur innerhalb
  einer Partition, das ist die einzige Garantie, dass z. B. ein
  `pass.uninstalled` nie vor dem zugehörigen `pass.installed` verarbeitet
  wird. Consumer mit manuellem Offset-Commit (`enable_auto_commit=False`),
  damit `ack()` echte Bedeutung hat. Enqueue-Fehler (Broker weg, Timeout via
  `Settings.enqueue_timeout`) werden immer als `QueueUnavailable` geworfen,
  nie als aiokafka-spezifische Exception — nur so löst der Endpoint sein
  503 aus.
- Tests markiert `@pytest.mark.kafka` (`tests/test_queues_kafka.py`) laufen
  gegen einen echten lokalen Broker (`kafka_settings`-Fixture,
  `tests/conftest.py`) und werden ohne erreichbaren Broker auf
  `localhost:9092` übersprungen. CI (`.github/workflows/tests.yaml`) startet
  dafür einen `apache/kafka`-Service-Container auf Port 9092 und installiert
  das Extra `[test,kafka]`.
- FastAPI-Endpoint (`handlers/fastapi.py`, `router` unter `POST
  {handler_prefix}`) für die eingehenden heidi.cloud-Webhooks: prüft die
  Signatur gegen die rohen Body-Bytes, parst danach lax auf `WebhookEvent`
  und schreibt in die konfigurierte Queue. Statuscodes sind Vertrag, nicht
  Geschmack — heidi.cloud wiederholt jedes Non-2xx bis zu 12x über 48 h:
  204 bei erfolgreichem Enqueue, 200 bei `webhook.test` (angenommen, aber
  nicht enqueued), 401 nur bei ungültiger/fehlender Signatur, 400 nur wenn
  strukturell kein Envelope vorliegt, 503 wenn die Queue nicht erreichbar
  ist. Unbekannte `type`-Werte werden durchgereicht und enden mit 204.
- `Settings.max_body_bytes` (Default 1 MiB): Obergrenze für den rohen
  Request-Body, geprüft bevor er gepuffert wird.
- Logging im Webhook-Endpoint (`logging.getLogger(__name__)` in
  `handlers/fastapi.py`): 401 als `warning` (ohne Body/Signaturwert, mit
  Hinweis auf mögliche Secret-Rotation), 400 als `info` mit dem Grund, 413
  als `warning` mit der Größe, 503 als `error` mit `event.id`, erfolgreicher
  Enqueue als `debug` mit `event.id`.
- OpenAPI-Dokumentation der Statuscodes (`responses={...}` an
  `@router.post`) für 200/204/400/401/413/503.

### Fixes

- `tests/conftest.py` (`kafka_settings`-Fixture): Der bisherige Broker-Check
  war ein reiner TCP-Connect gegen `localhost:9092` — auf Entwicklermaschinen
  mit einem Port-Forward auf genau diesem Port (z.B. VS Code) meldete das
  fälschlich "Broker da", und die Tests liefen los, um dann mit
  `KafkaConnectionError` zu scheitern (kein echtes Kafka-Protokoll dahinter).
  Der Check startet jetzt probeweise einen echten `AIOKafkaProducer` (2,5 s
  Timeout) statt nur den Port zu prüfen. Die Adresse kommt konfigurierbar aus
  `EDUTAP_WEBHOOK_HEIDI_KAFKA_BOOTSTRAP_SERVERS` (Default weiterhin
  `localhost:9092`), damit man lokal ohne Codeänderung auf einen freien Port
  ausweichen kann. Neu: `EDUTAP_WEBHOOK_HEIDI_TEST_REQUIRE_KAFKA=1` schaltet
  von "kein Broker -> skip" auf "kein Broker -> `pytest.fail`" um — gesetzt in
  `.github/workflows/tests.yaml` für den Test-Job, damit die CI hart scheitert,
  falls der Kafka-Service-Container aus irgendeinem Grund nicht erreichbar
  ist, statt still (und potenziell dauerhaft) grün zu bleiben.
- `tests/test_queues_kafka_unit.py` (neu): Unit-Tests für
  `KafkaQueueBackend` gegen Fake-Producer/-Consumer, ohne Broker-Abhängigkeit
  — decken `_get_producer`/`_get_consumer` (inkl. Caching), `enqueue`
  (Erfolg und Timeout), `consume`/`ack`-Roundtrip und `stop()` ab, die
  vorher nur über die `@pytest.mark.kafka`-Tests (und damit nur mit
  laufendem Broker) erreichbar waren. Grund: `queues/kafka.py` ist das
  einzige produktive Queue-Backend und muss unabhängig davon vollständig
  gemessen sein, ob gerade ein Broker läuft — sonst fällt die Coverage lokal
  (ohne Broker) unter das 90-%-Gate, obwohl in der CI (mit Broker) alles
  grün wäre. Die bestehenden `@pytest.mark.kafka`-Integrationstests bleiben
  unverändert bestehen und wurden gegen einen echten lokalen Broker
  verifiziert (Roundtrip verlustfrei inkl. `timestamp`/`payload`,
  Partition-Key ist `passid`, `QueueUnavailable` bei nicht erreichbarem
  Broker und bei Enqueue-Timeout).
- `signing.verify`: Nicht-ASCII-Zeichen in einem gefälschten
  `Heidi-Signature`-Header lösten zuvor eine unbehandelte `TypeError`
  in `hmac.compare_digest` aus (Starlette dekodiert Header mit
  latin-1), was aus einer 401 eine 500 machte. Der Vergleich läuft
  jetzt über die dekodierten Digest-Bytes (`bytes.fromhex`) statt über
  Header-Strings; ungültiges Hex ergibt `False`. Zusätzlich: leeres
  Secret wird abgelehnt, und der Zeitstempel muss ein reiner
  Ziffernstring sein (kein `+`-Präfix, keine `_`-Trenner).
- `handlers/fastapi.py`: Zwei Wege führten zuvor zu einem 500 statt 503 —
  kein Backend registriert (`get_queue_backend()` wirft `NotImplementedError`)
  und ein Backend, das etwas anderes als `QueueUnavailable` wirft (z.B.
  `ConnectionResetError`, `asyncio.TimeoutError`). Der Enqueue-Pfad fängt
  jetzt `Exception` breit ab und antwortet immer mit 503 — ein 500 wäre für
  heidi.cloud identisch zu einem 503 (beides Non-2xx, beides löst bis zu 12h
  Retries über 48 h aus), erzeugt aber unnötig Stacktraces/Alerts. Zusätzlich
  wird das Backend beim Modul-Import versuchsweise aufgelöst (Fail-Fast in
  der Produktion, wo der Entry-Point statisch vorhanden ist), ohne den
  Import zu sprengen, falls es dort noch fehlt.
- `handlers/fastapi.py`: `await request.body()` puffert den kompletten Body
  im Speicher, bevor irgendetwas geprüft ist — ein unauthentifizierter
  Memory-DoS. Der Endpoint prüft jetzt `Content-Length` gegen
  `settings.max_body_bytes`, bevor der Body gelesen wird (413 ohne zu
  lesen); fehlt der Header oder ist er falsch, wird die Länge nach dem Lesen
  geprüft (413 bei Überschreitung).
- `handlers/fastapi.py`: Der `Content-Length`-Vorcheck greift nicht, wenn der
  Header fehlt (z.B. Chunked Transfer Encoding ohne den Header) — `await
  request.body()` puffert dann trotzdem unbegrenzt, bevor die
  Größenprüfung zum Zug kommt. Der Body wird jetzt über eine neue
  `_read_body_limited()` inkrementell per `request.stream()` gelesen und
  bricht sofort mit 413 ab, sobald `max_body_bytes` überschritten ist, statt
  den Rest noch zu puffern.
- `handlers/fastapi.py`: Der 400-Log beim Envelope-Parsen loggte `str(exc)`
  einer pydantic-`ValidationError` — die bettet den validierten Eingabewert
  ein (z.B. die Matrikelnummer in `person_id` bei LMU), landete also
  ungewollt auf INFO-Level im Log. Geloggt werden jetzt nur noch
  `exc.error_count()` und die Fehlerorte (`e["loc"]` je Fehler), nie ein Wert.
- `handlers/fastapi.py`: `logger.error(...)` im generischen Enqueue-Fehlerfall
  durch `logger.exception(...)` ersetzt, damit der Traceback erhalten bleibt.
  `QueueMessage.from_event(event)` aus dem `try`-Block vor den `try` gezogen,
  damit ein Modellfehler dort nicht fälschlich als 503 statt als eigener Bug
  erscheint. Import-Zeit-Backend-Resolve loggt jetzt eine `warning`, wenn
  beim Import kein Backend auflösbar ist, statt es still zu schlucken.
- `queues/kafka.py`: Fünf Review-Befunde behoben (alle empirisch belegt,
  jeweils erst per rot werdendem Test reproduziert):
  - **Coverage-Theater um die Kernzusage**: Die Fake-Producer/-Consumer in
    `tests/test_queues_kafka_unit.py` schluckten bislang die
    Konstruktor-kwargs, ohne sie zu prüfen — eine Mutation, die
    `enable_idempotence`/`acks="all"`/`enable_auto_commit` entfernt bzw.
    umdreht, blieb bei 100 % Coverage grün. Neue Tests
    (`test_get_producer_is_idempotent_with_acks_all`,
    `test_get_consumer_uses_manual_commit_and_group_id`) assertieren jetzt
    genau diese Werte; verifiziert, indem die Werte testweise verdreht und
    die Suite dabei rot wurde.
  - **Stiller Datenverlust bei doppelten `eventid`s**: `_records` war nach
    `message.eventid` gekeyt, obwohl Duplikate by design erwartet sind
    (heidi.cloud liefert at-least-once, deshalb dedupliziert der Consumer
    und nicht wir). Zwei Nachrichten mit derselben `eventid` an
    unterschiedlichen Offsets überschrieben sich gegenseitig; `ack()` der
    ersten Kopie committete dadurch den Offset der zweiten mit und ließ
    dazwischenliegende Nachrichten für immer unverarbeitet. Reproduziert
    am echten Broker (Offsets 0/3 mit gleicher `eventid`, 1/2 dazwischen
    verloren) und als Unit-Test nachgebildet
    (`test_ack_commits_only_up_to_its_own_offset_with_duplicate_eventids`).
    Fix: `_records` ist jetzt nach `id(message)` gekeyt, nicht mehr nach
    `eventid`. `QueueBackend.ack()` (`protocols.py`) dokumentiert jetzt
    explizit, dass Kafka-Commits kumulativ sind und Consumer deshalb
    sequenziell arbeiten müssen (konsumieren → verarbeiten → acken, erst
    dann die nächste Nachricht holen).
  - **Race beim Kaltstart**: `_get_producer()`/`_get_consumer()` prüften
    `if self._producer is None`, gaben dann per `await ...start()` die
    Kontrolle ab und wiesen erst danach zu — mehrere gleichzeitige
    `enqueue()`-Aufrufe auf kaltem Backend erzeugten dadurch je einen
    eigenen, nie gestoppten Producer (gemessen: 5 gleichzeitige Aufrufe →
    5 Producer, 4 verwaist). Fix: `asyncio.Lock` je Getter mit
    Double-Checked-Locking.
  - **`enqueue_timeout` deckte den Producer-Start nicht ab**: `wait_for`
    umschloss nur `send_and_wait`, `_get_producer()` lief ungebremst davor
    — gegen einen Listener, der TCP annimmt, aber nicht antwortet, wurde
    `QueueUnavailable` erst nach 40 s statt der konfigurierten 2 s
    geworfen. Fix: Producer-Start und Send laufen jetzt gemeinsam in einem
    `wait_for`.
  - **Leere Fehlermeldung bei Timeout**: `str(asyncio.TimeoutError())` ist
    `""`, `QueueUnavailable(str(exc))` war dadurch inhaltslos. Wirft jetzt
    `f"Enqueue-Timeout nach {timeout}s"` (bzw. bei `KafkaError` den
    Exception-Klassennamen mit Meldung), jeweils mit `from exc`.
- `tests/conftest.py`: Der Kafka-Broker-Probe war ein einmaliger 2,5-s-
  Versuch ohne Retry; der CI-Service-Container in
  `.github/workflows/tests.yaml` hat keinen eigenen Healthcheck. Mit
  `EDUTAP_WEBHOOK_HEIDI_TEST_REQUIRE_KAFKA=1` konnte ein Broker, der beim
  ersten Testlauf noch hochfährt, dadurch einen harten, flakigen CI-Fail
  auslösen. Neu: `_wait_for_kafka_broker()` pollt bis zu 30 s in 2-s-
  Schritten, aber nur im `TEST_REQUIRE_KAFKA`-Pfad — lokal ohne
  absichtlich laufenden Broker bleibt der schnelle Einzelversuch, damit
  der Skip-Hinweis nicht künstlich verzögert wird.
