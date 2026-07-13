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
