# `webhook.test` wird enqueued — Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Der Konnektivitätstest `webhook.test` aus der heidi.cloud-Admin-UI durchläuft denselben Pfad wie jedes andere Event und wird in die Queue geschrieben; der Statuscode 200 bleibt als Marker erhalten, wird aber erst nach bestätigtem Enqueue gesetzt.

**Architecture:** Der frühe Rücksprung in `handlers/fastapi.py` (`if event.type == WEBHOOK_TEST: return Response(200)`) entfällt ersatzlos. Die `webhook.test`-Verzweigung wandert damit von *vor* dem Enqueue an die Stelle *nach* dem bestätigten Enqueue und entscheidet dort nur noch über den Statuscode (200 statt 204). Zusätzlich wird der Erfolgs-Log von `debug` auf `info` gehoben und um `event.type` ergänzt, damit ein Testevent auch ohne Access-Log erkennbar ist. Keine neuen Module, keine neuen Abhängigkeiten, kein eigenes Topic.

**Tech Stack:** Python ≥3.11, FastAPI/Starlette, pydantic v2, pytest + pytest-asyncio (`asyncio_mode = "auto"`), `fastapi.testclient.TestClient`, `InMemoryQueueBackend` über die `memory_backend`-Fixture aus `tests/conftest.py`.

**Spec:** [`docs/superpowers/specs/2026-08-14-webhook-test-enqueue-design.md`](../specs/2026-08-14-webhook-test-enqueue-design.md) — ändert §5.1/§5.2/§7 des [Basis-Designs](../specs/2026-07-13-webhook-heidi-design.md).

## Global Constraints

- **Statuscodes sind Vertrag, nicht Geschmack.** heidi.cloud wertet **jedes** Non-2xx als Fehlschlag und wiederholt bis zu 12× über 48 h — auch bei 4xx. Nichts an den bestehenden Codes 400/401/413/503 ändern.
- **2xx erst nach durable enqueue.** Ein 2xx vor dem bestätigten Kafka-Write macht einen verlorenen Produce-Call zu einem endgültig verlorenen Event. Das gilt ab dieser Änderung **auch für `webhook.test`** — bei kaputter Queue ist die Antwort 503, nicht 200.
- **Keine PII in Logs.** Niemals `str(ValidationError)`, `exc.errors()` im Ganzen, Body-Inhalte oder Feldwerte loggen. Erlaubt sind `event.id`, `event.type`, Größenangaben und strukturelle Fehlerorte (`e["loc"]`).
- **Kein Sonderpfad für `webhook.test` im Paket.** Das Verwerfen der Testnachricht ist Aufgabe des Consumers (beim LMU-Spooler bereits der Fall, Angabe des Betreibers 2026-08-14). Keine Filterung, kein eigenes Topic, keine eigene Queue.
- **Coverage-Gate bleibt bei 90 %** (`fail_under = 90` in `pyproject.toml`).
- **Das Paket konfiguriert selbst kein Logging.** Level und Handler kommen aus dem Deployment; im Code nur `logging.getLogger(__name__)`.
- Testkommandos in diesem Plan gehen von `.venv/bin/pytest` im Projektwurzelverzeichnis aus (`/home/phil/dev/projects/edutap/heidi/lmu/lmu_edutap_full_view/sources/edutap.webhook-heidi`). Das `.venv` existiert bereits.

## File Structure

| Datei | Verantwortung | Änderung |
|---|---|---|
| `src/edutap/webhook_heidi/handlers/fastapi.py` | Der Endpoint — einzige Codedatei mit Verhaltensänderung | Früher Rücksprung raus, Statuscodewahl nach Enqueue, Log auf `info` + `event.type`, OpenAPI-Beschreibung des 200ers |
| `src/edutap/webhook_heidi/models.py` | Datenmodelle + Konstante `WEBHOOK_TEST` | Nur Docstring der Konstanten (Halbsatz „nicht in die Queue geschrieben" ist ab jetzt falsch) |
| `tests/test_handlers_fastapi.py` | Endpoint-Tests gegen `InMemoryQueueBackend` | `TEST_EVENT` als Modulkonstante, zwei Tests ersetzt, einer neu |
| `docs/superpowers/specs/2026-07-13-webhook-heidi-design.md` | Basis-Design | §5 Mermaid, §5.2 Tabelle, §7 Testliste — mit Verweis auf die neue Spec |
| `docs/superpowers/specs/2026-08-14-webhook-test-enqueue-design.md` | Die Spec zu dieser Änderung | Aktuell **untracked** — wird in Task 1 committet |
| `CHANGES.md` | Changelog | Endpoint- und Logging-Eintrag unter `unreleased` korrigieren |

`models.py` ändert sich nur im Docstring, weil `QueueMessage.from_event()` bereits jedes Event unverändert abbildet: `action` trägt `"webhook.test"`, `passid` die Null-UUID, `eventid` die eindeutige `evt_`-ID. Es braucht dort keine Sonderbehandlung.

---

### Task 1: `webhook.test` wird enqueued, 200 erst nach Enqueue

**Files:**
- Modify: `src/edutap/webhook_heidi/handlers/fastapi.py:87-90` (OpenAPI-Beschreibung des 200ers), `:168-169` (früher Rücksprung), `:192` (Statuscode)
- Modify: `src/edutap/webhook_heidi/models.py:15-17` (Docstring `WEBHOOK_TEST`)
- Test: `tests/test_handlers_fastapi.py`
- Commit (Step 1): `docs/superpowers/specs/2026-08-14-webhook-test-enqueue-design.md`, `docs/superpowers/plans/2026-08-14-webhook-test-enqueue.md`

**Interfaces:**
- Consumes: `WEBHOOK_TEST` (`str`-Konstante `"webhook.test"`, `models.py:15`); `QueueMessage.from_event(event: WebhookEvent) -> QueueMessage`; `get_queue_backend()` → Objekt mit `async enqueue(message: QueueMessage) -> None`; Fixtures `memory_backend` (aus `tests/conftest.py`, hängt `InMemoryQueueBackend` mit `.messages: list[QueueMessage]` ein), `failing_queue_backend` und `client` (beide `tests/test_handlers_fastapi.py`); Helper `_post(client, body, *, secret=TEST_SECRET, now=None)`.
- Produces: Modulkonstante `TEST_EVENT: dict` in `tests/test_handlers_fastapi.py` (vollständiger `webhook.test`-Envelope, wird von Task 1 und Task 2 gebraucht). Kein neuer Produktionscode-Name — `handle_pass_event` behält Signatur und Verhalten für alle anderen Event-Typen.

- [ ] **Step 1: Spec und Plan unter Versionskontrolle bringen**

Die Spec ist bislang untracked. Beide Dokumente gehören vor den Codeänderungen ins Repo, damit die folgenden Commits auf einen versionierten Stand verweisen können.

```bash
git add docs/superpowers/specs/2026-08-14-webhook-test-enqueue-design.md \
        docs/superpowers/plans/2026-08-14-webhook-test-enqueue.md
git commit -m "docs(spec): webhook.test wird enqueued -- Design und Plan"
```

- [ ] **Step 2: `TEST_EVENT` als Modulkonstante herausziehen**

Der `webhook.test`-Envelope steckt bisher inline in `test_webhook_test_is_accepted_but_not_enqueued`. Ab jetzt brauchen ihn zwei Tests, also raus damit. Direkt hinter die bestehende `EVENT`-Konstante (nach `tests/test_handlers_fastapi.py:34`) einfügen:

```python
TEST_EVENT = {
    "id": "evt_a1b2c3d4e5f60718293a4b5c6d7e8f90",
    "type": "webhook.test",
    "created": "2026-07-09T12:34:56Z",
    "api_version": "2026-07-09",
    "data": {
        "pass_id": "00000000-0000-0000-0000-000000000000",
        "person_id": "test",
        "wallet_type": "UNSET",
        "state": "NEW",
        "reason": "test",
        "confirmation": "platform",
    },
}
```

Die ID ist bewusst eine echte `evt_` + 32-Hex-ID statt des bisherigen `"evt_test"`: §3.3 der Spec begründet die Dedup-Freiheit mehrerer Testklicks genau damit, und der Testevent soll dem realen Envelope entsprechen.

- [ ] **Step 3: Den Test schreiben, der die neue Zusage prüft**

`test_webhook_test_is_accepted_but_not_enqueued` (aktuell `tests/test_handlers_fastapi.py:168-188`) prüft die **Gegenaussage** und wird komplett durch diesen Test ersetzt — alte Funktion samt Docstring löschen, neue an dieselbe Stelle:

```python
def test_webhook_test_is_enqueued_and_returns_200(client, memory_backend):
    """Der Konnektivitätstest testet ab jetzt die ganze Kette, nicht die halbe:
    Er wird wie jedes andere Event enqueued. 200 bleibt nur als Marker
    erhalten, damit im Access-Log ohne Body-Zugriff erkennbar ist, dass es ein
    Testklick war — gesetzt wird er erst NACH dem bestätigten Enqueue."""
    body = json.dumps(TEST_EVENT).encode()

    assert _post(client, body).status_code == 200

    assert len(memory_backend.messages) == 1
    message = memory_backend.messages[0]
    assert message.action == "webhook.test"
    assert message.eventid == TEST_EVENT["id"]
    assert message.passid == "00000000-0000-0000-0000-000000000000"
```

- [ ] **Step 4: Den 503-Test für den Testevent-Pfad schreiben**

Bisher ist 503 nur für normale Events abgedeckt. Ein Konnektivitätstest, der grün meldet, während der Broker weg ist, ist schlimmer als kein Test. Direkt hinter den Test aus Step 3 einfügen:

```python
def test_webhook_test_yields_503_when_queue_unavailable(client, failing_queue_backend):
    """Auch der Testevent darf bei kaputter Queue kein 2xx bekommen — sonst
    meldet die Admin-UI den Konnektivitätstest als erfolgreich, obwohl der
    Broker weg ist. 503 ist hier korrekt und gewollt: Die UI zeigt den Test
    als fehlgeschlagen an und heidi.cloud wiederholt ihn."""
    assert _post(client, json.dumps(TEST_EVENT).encode()).status_code == 503
```

- [ ] **Step 5: Beide Tests laufen lassen, Fehlschlag verifizieren**

Run:
```bash
.venv/bin/pytest tests/test_handlers_fastapi.py::test_webhook_test_is_enqueued_and_returns_200 \
                 tests/test_handlers_fastapi.py::test_webhook_test_yields_503_when_queue_unavailable -v
```
Expected: **beide FAIL**.
- `..._is_enqueued_and_returns_200`: `AssertionError` bei `len(memory_backend.messages) == 1` — Status 200 stimmt schon, die Queue ist aber leer, weil der frühe Rücksprung greift.
- `..._yields_503_when_queue_unavailable`: `assert 200 == 503` — der Handler kehrt zurück, bevor `enqueue()` überhaupt aufgerufen wird.

Wenn stattdessen `NameError: name 'TEST_EVENT' is not defined` erscheint, wurde Step 2 übersprungen.

- [ ] **Step 6: Frühen Rücksprung entfernen und Statuscode nach dem Enqueue wählen**

In `src/edutap/webhook_heidi/handlers/fastapi.py` diese drei Zeilen (aktuell `:168-170`) ersatzlos löschen:

```python
if event.type == WEBHOOK_TEST:
    return Response(status_code=200)
```

Damit gelten `QueueMessage.from_event()` und der `try`-Block für jedes Event. Anschließend die Rückgabe am Ende der Funktion (aktuell `:192`) ersetzen:

```python
    # Der Statuscode ist die einzige Stelle, an der im Access-Log ohne
    # Body-Zugriff erkennbar ist, dass es ein Testklick aus der Admin-UI war
    # und kein Produktionsverkehr. Beides ist 2xx, der Sender wertet also
    # beides als Erfolg — die Unterscheidung kostet nichts und ist beim
    # Debuggen wertvoll. Gesetzt wird sie erst hier, NACH dem bestätigten
    # Enqueue: ein Konnektivitätstest, der grün meldet, während der Broker
    # weg ist, wäre schlimmer als kein Test.
    return Response(status_code=200 if event.type == WEBHOOK_TEST else 204)
```

Der Import `from edutap.webhook_heidi.models import WEBHOOK_TEST` (`:13`) bleibt — die Konstante wird weiterhin gebraucht.

- [ ] **Step 7: OpenAPI-Beschreibung des 200ers korrigieren**

In `handlers/fastapi.py:87-90` — die Beschreibung sagt derzeit das Gegenteil des neuen Verhaltens:

```python
        200: {
            "description": "Konnektivitätstest (`webhook.test`) angenommen "
            "und enqueued."
        },
```

`status_code=204` am Dekorator und `include_in_schema=False` an der Slash-Variante bleiben unverändert — der OpenAPI-Vertrag ist ansonsten stabil.

- [ ] **Step 8: Docstring der Konstanten `WEBHOOK_TEST` korrigieren**

`src/edutap/webhook_heidi/models.py:15-17` — der Halbsatz „nicht in die Queue geschrieben" ist ab jetzt falsch:

```python
WEBHOOK_TEST = "webhook.test"
"""Konnektivitätstest aus der heidi.cloud-Admin-UI (Null-UUID als ``pass_id``).

Durchläuft denselben Pfad wie jedes andere Event und wird enqueued; der
Statuscode 200 bleibt nur als Marker erhalten, damit ein Testklick im
Access-Log erkennbar ist. Das Verwerfen dieser Nachrichten ist Aufgabe des
Consumers — dieses Paket kann es nicht erzwingen."""
```

- [ ] **Step 9: Tests laufen lassen und grün verifizieren**

Run:
```bash
.venv/bin/pytest tests/test_handlers_fastapi.py -v
```
Expected: PASS für alle Tests der Datei, insbesondere `test_webhook_test_is_enqueued_and_returns_200` und `test_webhook_test_yields_503_when_queue_unavailable`.

`test_successful_enqueue_logs_debug_with_event_id` muss hier **noch grün sein** — der Log bleibt in dieser Task auf `debug`, er wird erst in Task 2 angefasst. Ist er rot, wurde vorgegriffen.

- [ ] **Step 10: Volle Suite ohne Kafka-Marker laufen lassen**

Der Endpoint ist die Naht zu allem anderen; ein Regress in `test_plugins.py` oder `test_models.py` fällt hier auf.

Run:
```bash
.venv/bin/pytest -m "not kafka" -q
```
Expected: PASS, keine Fehler, keine Errors (übersprungene `kafka`-Tests sind erwartet).

- [ ] **Step 11: Commit**

```bash
git add src/edutap/webhook_heidi/handlers/fastapi.py \
        src/edutap/webhook_heidi/models.py \
        tests/test_handlers_fastapi.py
git commit -m "$(cat <<'EOF'
feat(handlers): webhook.test wird enqueued, 200 erst nach dem Enqueue

Der Konnektivitaetstest aus der heidi.cloud-Admin-UI kehrte bisher mit 200
zurueck, bevor der Enqueue-Pfad erreicht wurde. Er bewies damit nur, dass
Netzwerkweg, TLS-Terminierung, Routing und Signatur stimmen -- ueber den
Zustand des Brokers sagte er nichts. Genau das ist aber die Frage, die man
beim Klick auf "Test senden" beantwortet haben will.

webhook.test durchlaeuft jetzt denselben Pfad wie jedes andere Event. 200
bleibt als Marker erhalten (einzige Stelle, an der ein Testklick im
Access-Log ohne Body-Zugriff erkennbar ist), wird aber erst nach dem
bestaetigten Enqueue gesetzt. Bei kaputter Queue gibt es damit auch fuer
den Testevent 503 statt eines faelschlich gruenen Tests.

Voraussetzung ausserhalb dieses Pakets: Der Consumer verwirft Nachrichten
mit action == "webhook.test" (beim LMU-Spooler bereits der Fall). Das Paket
kann es nicht erzwingen; eine Filterung hier waere ein Sonderpfad, den die
Spec bewusst ausschliesst.

Siehe docs/superpowers/specs/2026-08-14-webhook-test-enqueue-design.md
EOF
)"
```

---

### Task 2: Erfolgs-Log auf INFO mit `event.type`

**Files:**
- Modify: `src/edutap/webhook_heidi/handlers/fastapi.py:191` (die `logger.debug`-Zeile)
- Test: `tests/test_handlers_fastapi.py:481-491` (`test_successful_enqueue_logs_debug_with_event_id` wird ersetzt)

**Interfaces:**
- Consumes: `EVENT` (Modulkonstante, `tests/test_handlers_fastapi.py:21` — der normale `pass.installed`-Envelope, nicht `TEST_EVENT`), Fixtures `client`, `memory_backend`, `caplog`.
- Produces: nichts, was spätere Tasks konsumieren.

> **Abweichung von §6 der Spec, bewusst:** Die Spec nennt den Test
> `test_successful_enqueue_logs_info_with_event_id`, prüft in der Beschreibung
> aber ausdrücklich `event.id` **und** `event.type`. Der Name hier heißt
> entsprechend `..._with_event_id_and_type`. Nur der Name weicht ab, nicht die
> geprüfte Zusage.

- [ ] **Step 1: Den Log-Test schreiben**

`test_successful_enqueue_logs_debug_with_event_id` (`tests/test_handlers_fastapi.py:481-491`) komplett löschen und durch diesen ersetzen:

```python
def test_successful_enqueue_logs_info_with_event_id_and_type(
    client, memory_backend, caplog
):
    """Bei produktionsueblichem INFO-Level erzeugte der Endpoint fuer ein
    ERFOLGREICH verarbeitetes Event bislang keinerlei Logzeile — sichtbar war
    nur der Statuscode im Access-Log der ASGI-Schicht. Genau diese Luecke hat
    die Fehlersuche zum Testevent unnoetig verlaengert. Mit ``event.type`` in
    der Zeile ist ein Testevent auch ohne Access-Log erkennbar."""
    with caplog.at_level(logging.INFO, logger="edutap.webhook_heidi.handlers.fastapi"):
        response = _post(client, json.dumps(EVENT).encode())

    assert response.status_code == 204
    records = [
        r for r in caplog.records if r.name == "edutap.webhook_heidi.handlers.fastapi"
    ]
    assert len(records) == 1
    assert records[0].levelno == logging.INFO
    logged = records[0].getMessage()
    assert EVENT["id"] in logged
    assert EVENT["type"] in logged
```

- [ ] **Step 2: Test laufen lassen, Fehlschlag verifizieren**

Run:
```bash
.venv/bin/pytest tests/test_handlers_fastapi.py::test_successful_enqueue_logs_info_with_event_id_and_type -v
```
Expected: FAIL mit `assert 0 == 1` bei `len(records) == 1` — auf INFO-Level erreicht die `logger.debug`-Zeile `caplog` gar nicht, die Liste ist leer.

- [ ] **Step 3: Log-Zeile umstellen**

`src/edutap/webhook_heidi/handlers/fastapi.py` — die `logger.debug`-Zeile (nach Task 1 die vorletzte Anweisung der Funktion) ersetzen:

```python
# Bewusst INFO, nicht DEBUG: Bei produktionsueblichem INFO-Level gab es
# fuer ein ERFOLGREICH verarbeitetes Event sonst keinerlei Logzeile —
# sichtbar war nur der Statuscode im Access-Log der ASGI-Schicht, was ein
# korrekt verarbeitetes Event von einem still verschluckten
# ununterscheidbar macht. event.type gehoert dazu, damit ein Testevent
# auch ohne Access-Log erkennbar ist.
logger.info("Event enqueued (event.id=%s, event.type=%s).", event.id, event.type)
```

Nur `event.id` und `event.type` — keine Feldwerte aus `data` (PII: bei LMU steckt die Matrikelnummer in `person_id`).

- [ ] **Step 4: Test laufen lassen und grün verifizieren**

Run:
```bash
.venv/bin/pytest tests/test_handlers_fastapi.py::test_successful_enqueue_logs_info_with_event_id_and_type -v
```
Expected: PASS.

- [ ] **Step 5: Volle Suite laufen lassen — die anderen Log-Tests sind der eigentliche Prüfstein**

Vier bestehende Tests behaupten `len(records) == 1` unter `caplog.at_level(logging.INFO)` bzw. `WARNING`. Eine zusätzliche INFO-Zeile im Erfolgsfall könnte einen davon kippen, wenn ein Pfad wider Erwarten doch enqueued.

Run:
```bash
.venv/bin/pytest -m "not kafka" -q
```
Expected: PASS. Betroffene Kandidaten, die grün bleiben müssen: `test_malformed_envelope_logs_info_with_reason`, `test_malformed_envelope_400_log_does_not_leak_pii`, `test_invalid_signature_logs_warning_without_secrets`, `test_queue_unavailable_logs_error_with_event_id`. Alle vier enden in einem Fehlerpfad **vor** dem Enqueue, dürfen also weiterhin genau einen Record sehen. Kippt einer, ist das ein echter Befund, kein Testartefakt — dann greift superpowers:systematic-debugging, nicht ein hochgezogenes `at_level`.

- [ ] **Step 6: Coverage-Gate prüfen**

Run:
```bash
.venv/bin/pytest -m "not kafka" --cov --cov-report=term-missing -q
```
Expected: PASS, `Required test coverage of 90% reached` (bzw. kein `FAIL_UNDER`-Fehler). Der entfernte frühe Rücksprung nimmt eine Zeile aus der Statistik, die neue Statuscode-Zeile ist durch beide `webhook.test`-Tests abgedeckt.

- [ ] **Step 7: Commit**

```bash
git add src/edutap/webhook_heidi/handlers/fastapi.py tests/test_handlers_fastapi.py
git commit -m "$(cat <<'EOF'
feat(handlers): Erfolgs-Log auf INFO, mit event.type

Bei produktionsueblichem INFO-Level erzeugte der Endpoint fuer ein
erfolgreich verarbeitetes Event keinerlei Logzeile -- sichtbar war nur der
Statuscode im Access-Log der ASGI-Schicht. Ein korrekt verarbeitetes Event
war damit von einem still verschluckten nicht zu unterscheiden; genau diese
Luecke hat die Fehlersuche zum nicht enqueuten Testevent verlaengert.

logger.debug -> logger.info, ergaenzt um event.type, damit ein Testevent
auch ohne Access-Log erkennbar ist. Nur id und type -- keine Feldwerte aus
data (bei LMU steckt die Matrikelnummer in person_id).

Die uebrigen Logstufen bleiben unveraendert: 401 warning, 400 info (ohne
Feldwerte), 413 warning, 503 exception mit Traceback und event.id.
EOF
)"
```

---

### Task 3: Basis-Design und Changelog nachziehen

Reine Dokumentation, aber kein Beiwerk: Das Basis-Design und der Changelog behaupten an vier Stellen wörtlich das Gegenteil des jetzt implementierten Verhaltens. Ein Leser, der die neue Spec nicht kennt, liest dort weiter „angenommen, nicht enqueued".

**Files:**
- Modify: `docs/superpowers/specs/2026-07-13-webhook-heidi-design.md:339-340` (Mermaid §5), `:373` (Tabelle §5.2), `:419` (Testliste §7)
- Modify: `CHANGES.md:53-54` (Endpoint-Eintrag), Logging-Eintrag unter `unreleased`

**Interfaces:**
- Consumes: das in Task 1/2 implementierte Verhalten.
- Produces: nichts.

- [ ] **Step 1: Mermaid-Zweig im Basis-Design umhängen**

`docs/superpowers/specs/2026-07-13-webhook-heidi-design.md`, §5 — im Diagramm liegt die `webhook.test`-Verzweigung derzeit **vor** dem Enqueue. Diese beiden Zeilen (`:339-340`):

```
    D -- ja --> E{"type == webhook.test?"}
    E -- ja --> E1["200<br/>(kein enqueue)"]
    E -- nein --> F["QueueMessage bauen"]
```

ersetzen durch:

```
    D -- ja --> F["QueueMessage bauen"]
```

und den Erfolgszweig (`G -- ja --> G2["204"]`) ersetzen durch:

```
    G -- ja --> H{"type == webhook.test?"}
    H -- ja --> H1["200"]
    H -- nein --> H2["204"]
```

Der Knoten `G2` verschwindet damit; `H1`/`H2` sind die neuen Endknoten. Die gestrichelten Retry-Kanten (`C1 -.-> R`, `D1 -.-> R`, `G1 -.-> R`) bleiben unverändert.

- [ ] **Step 2: Fehlertabelle §5.2 korrigieren**

`docs/superpowers/specs/2026-07-13-webhook-heidi-design.md:373` — diese Zeile:

```markdown
| `webhook.test` | `200` | angenommen, nicht enqueued |
```

ersetzen durch:

```markdown
| `webhook.test`, enqueued | `200` | enqueued wie jedes Event; 200 bleibt der Marker (siehe [2026-08-14](2026-08-14-webhook-test-enqueue-design.md)) |
```

Die 503-Zeile derselben Tabelle gilt ab jetzt ausdrücklich auch für den Testevent — Begründung ergänzen:

```markdown
| Queue nicht erreichbar, Enqueue-Timeout | `503` | Retry ist korrekt; nichts verloren — gilt auch für `webhook.test` |
```

- [ ] **Step 3: Testliste §7 korrigieren**

`docs/superpowers/specs/2026-07-13-webhook-heidi-design.md:419` — diese Zeile:

```markdown
- `webhook.test` → 200, Queue bleibt leer.
```

ersetzen durch:

```markdown
- `webhook.test` → 200 **und** Nachricht liegt in der Queue (`action ==
  "webhook.test"`); bei kaputter Queue 503 statt 200. Geändert am
  2026-08-14, siehe
  [`2026-08-14-webhook-test-enqueue-design.md`](2026-08-14-webhook-test-enqueue-design.md).
```

- [ ] **Step 4: Hinweis auf die Ablösung an §5.1 anfügen**

Damit ein Leser, der bei §5 einsteigt, die Änderung nicht übersieht: am Ende von §5.1 (nach dem Absatz „**2xx erst nach durable enqueue.**", vor §5.2) einfügen:

```markdown
> **Geändert am 2026-08-14:** Der ursprünglich hier beschriebene Sonderzweig
> „`webhook.test` → 200, kein Enqueue" entfällt. Der Testevent durchläuft
> denselben Pfad wie jedes andere Event; 200 bleibt nur als Marker und wird
> erst nach dem bestätigten Enqueue gesetzt. Begründung und Consumer-Vertrag:
> [`2026-08-14-webhook-test-enqueue-design.md`](2026-08-14-webhook-test-enqueue-design.md).
```

- [ ] **Step 5: Changelog-Einträge korrigieren**

`CHANGES.md`, Abschnitt `unreleased` — im Endpoint-Eintrag (`:53-54`) diesen Halbsatz:

```markdown
  204 bei erfolgreichem Enqueue, 200 bei `webhook.test` (angenommen, aber
  nicht enqueued), 401 nur bei ungültiger/fehlender Signatur, 400 nur wenn
```

ersetzen durch:

```markdown
  204 bei erfolgreichem Enqueue, 200 bei `webhook.test` (ebenfalls enqueued;
  200 nur als Marker im Access-Log, gesetzt erst nach dem bestätigten
  Enqueue), 401 nur bei ungültiger/fehlender Signatur, 400 nur wenn
```

Im Logging-Eintrag desselben Abschnitts den Halbsatz „erfolgreicher Enqueue als `debug` mit `event.id`" ersetzen durch:

```markdown
  Enqueue als `info` mit `event.id` und `event.type`.
```

Anschließend unter `unreleased`/`### Fixes` einen neuen Eintrag ergänzen (oben, vor den Abschluss-Review-Einträgen):

```markdown
- **Konnektivitätstest testete nur die halbe Kette:** `webhook.test` kehrte
  mit 200 zurück, bevor der Enqueue-Pfad erreicht war — der Testklick in der
  heidi.cloud-Admin-UI bewies damit Netzwerkweg, TLS-Terminierung, Routing
  und Signatur, sagte über den Zustand des Brokers aber nichts. Zusätzlich
  gab es für diesen Pfad keine Logzeile, wodurch das Verhalten von außen
  ununterscheidbar von einem stillen Fehlschlag war (genau das hat eine
  Fehlersuche im Produktivbetrieb verlängert). `webhook.test` durchläuft
  jetzt denselben Pfad wie jedes andere Event und wird enqueued; 200 bleibt
  als Marker erhalten, wird aber erst nach dem bestätigten Enqueue gesetzt.
  Bei nicht erreichbarer Queue antwortet auch der Testevent mit 503 — ein
  Konnektivitätstest, der grün meldet, während der Broker weg ist, ist
  schlimmer als kein Test. **Voraussetzung außerhalb dieses Pakets:** Der
  Consumer verwirft Nachrichten mit `action == "webhook.test"` (beim
  LMU-Spooler bereits der Fall); `edutap.webhook_heidi` kann das nicht
  erzwingen. Siehe
  `docs/superpowers/specs/2026-08-14-webhook-test-enqueue-design.md`.
```

- [ ] **Step 6: Verifizieren, dass keine veraltete Aussage übrig ist**

Run:
```bash
grep -rn "nicht enqueued\|kein enqueue\|Queue bleibt leer\|nicht in die Queue" \
     docs/ CHANGES.md README.md src/ tests/
```
Expected: **keine Treffer** außer denen in `docs/superpowers/specs/2026-08-14-webhook-test-enqueue-design.md` und `docs/superpowers/plans/2026-08-14-webhook-test-enqueue.md` selbst, die das alte Verhalten zitieren, um die Änderung zu begründen. Jeder andere Treffer ist eine übersehene Stelle — nachziehen und erneut greppen.

Ebenfalls prüfen, dass `docs/HANDOFF.md` nicht betroffen ist: Es trägt seit dem Abschluss-Review oben einen Hinweis, dass es historisch und überholt ist, und wird bewusst nicht mehr gepflegt.

- [ ] **Step 7: Volle Suite ein letztes Mal**

Run:
```bash
.venv/bin/pytest -m "not kafka" -q
```
Expected: PASS. Doku-Änderungen dürfen nichts brechen — das ist der Punkt der Prüfung, nicht ihre Erwartung.

Wenn ein Kafka-Broker auf `localhost:9092` läuft, zusätzlich den Naht-Test mitnehmen (er geht den kompletten Pfad Endpoint → Broker und ist der einzige Test, der die Änderung gegen einen echten Broker prüft):

```bash
.venv/bin/pytest tests/test_integration_kafka.py -v
```
Expected: PASS — oder `skipped`, wenn kein Broker erreichbar ist. Ein Skip ist hier akzeptabel; die CI startet einen Service-Container.

- [ ] **Step 8: Commit**

```bash
git add docs/superpowers/specs/2026-07-13-webhook-heidi-design.md CHANGES.md
git commit -m "$(cat <<'EOF'
docs: Basis-Design und Changelog auf enqueuten webhook.test nachziehen

Basis-Design (2026-07-13) und Changelog beschrieben an vier Stellen woertlich
das Gegenteil des implementierten Verhaltens ("angenommen, nicht enqueued",
"Queue bleibt leer"). Ein Leser, der die neue Spec nicht kennt, haette dort
weiter das alte Verhalten gelesen.

Geaendert: Mermaid-Zweig in §5 (webhook.test-Verzweigung wandert hinter den
Enqueue), Fehlertabelle §5.2 inkl. Klarstellung, dass 503 auch fuer den
Testevent gilt, Testliste §7, plus ein Aenderungshinweis am Ende von §5.1 --
jeweils mit Verweis auf 2026-08-14-webhook-test-enqueue-design.md.
CHANGES.md: Endpoint- und Logging-Eintrag unter unreleased korrigiert, neuer
Fixes-Eintrag mit dem Consumer-Vertrag.
EOF
)"
```

---

## Nicht Teil dieses Plans

Aus §8 der Spec, damit es beim Umsetzen nicht versehentlich mitgebaut wird:

- **Keine Filterung oder Sonderbehandlung von `webhook.test` im Paket.** Das Verwerfen ist Aufgabe des Consumers. Wer hier einen `if action == WEBHOOK_TEST: continue`-Zweig in `queues/` einbaut, hat den Punkt der Änderung umgekehrt.
- **Kein eigenes Topic und keine eigene Queue für Testevents.** Partition-Key bleibt `passid` (beim Testevent die Null-UUID, alle Testevents also in einer Partition — bei manuellen Admin-UI-Klicks bedeutungslos).
- **Keine Logging-Konfiguration im Paket.** Nur `logging.getLogger(__name__)`; Level und Handler kommen aus dem Deployment.
- **Keine Änderung an `QueueMessage` oder `WebhookEvent`.** `from_event()` bildet den Testevent bereits korrekt ab.
