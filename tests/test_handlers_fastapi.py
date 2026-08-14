from conftest import TEST_SECRET
from edutap.webhook_heidi.plugins import add_plugin
from edutap.webhook_heidi.plugins import get_queue_backend
from edutap.webhook_heidi.plugins import reset_queue_backend
from edutap.webhook_heidi.signing import sign
from edutap.webhook_heidi.signing import SIGNATURE_HEADER
from fastapi import FastAPI
from fastapi.testclient import TestClient
from plugins import ExplodingQueueBackend
from plugins import FailingQueueBackend

import httpx
import json
import logging
import pytest
import time


PATH = "/webhook/heidi"

EVENT = {
    "id": "evt_9f4c2a1e6b8d4f0eae7cd3a2b1f0c9e8",
    "type": "pass.installed",
    "created": "2026-07-09T12:34:56Z",
    "api_version": "2026-07-09",
    "data": {
        "pass_id": "0c0ffee0-0000-0000-0000-000000000001",
        "person_id": "12345",
        "wallet_type": "APPLE_ACCESS",
        "state": "ACTIVE",
        "reason": "provisioning",
        "confirmation": "device",
    },
}

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


@pytest.fixture
def client() -> TestClient:
    from edutap.webhook_heidi.handlers.fastapi import router

    app = FastAPI()
    app.include_router(router)
    return TestClient(app)


@pytest.fixture
def failing_queue_backend() -> FailingQueueBackend:
    """Hängt ein Backend ein, das ``QueueUnavailable`` wirft, und räumt danach
    auf — auch wenn ein Assert vorher fehlschlägt (sonst leckt das Backend in
    alle folgenden Tests)."""
    reset_queue_backend()
    add_plugin(FailingQueueBackend)
    backend = get_queue_backend()
    yield backend
    reset_queue_backend()


@pytest.fixture
def exploding_queue_backend() -> ExplodingQueueBackend:
    """Backend, das einen unerwarteten Fehler wirft (kein ``QueueUnavailable``,
    z.B. wie ``ConnectionResetError``/``asyncio.TimeoutError`` es wären)."""
    reset_queue_backend()
    add_plugin(ExplodingQueueBackend)
    backend = get_queue_backend()
    yield backend
    reset_queue_backend()


@pytest.fixture
def no_queue_backend() -> None:
    """Stellt sicher, dass kein Backend registriert ist (weder Entry-Point
    noch programmatisch), und räumt danach auf."""
    reset_queue_backend()
    yield
    reset_queue_backend()


def _post(
    client: TestClient,
    body: bytes,
    *,
    secret: str = TEST_SECRET,
    now: int | None = None,
):
    timestamp = int(time.time()) if now is None else now
    return client.post(
        PATH,
        content=body,
        headers={
            "Content-Type": "application/json",
            SIGNATURE_HEADER: sign(secret, timestamp, body),
        },
    )


def test_valid_event_is_enqueued(client, memory_backend):
    body = json.dumps(EVENT).encode()
    response = _post(client, body)

    assert response.status_code == 204
    assert len(memory_backend.messages) == 1
    message = memory_backend.messages[0]
    assert message.eventid == EVENT["id"]
    assert message.passid == EVENT["data"]["pass_id"]
    assert message.action == "pass.installed"
    assert message.payload["state"] == "ACTIVE"


def test_retry_with_different_bytes_is_accepted(client, memory_backend):
    """Retries kommen aus einem JSONB-Roundtrip: gleiche Nachricht, andere Bytes."""
    retry_body = json.dumps(EVENT, sort_keys=True, separators=(",", ":")).encode()
    assert retry_body != json.dumps(EVENT).encode()

    assert _post(client, retry_body).status_code == 204
    assert len(memory_backend.messages) == 1


def test_unknown_event_type_is_accepted(client, memory_backend):
    """Unbekannte Typen dürfen NICHT abgelehnt werden — sonst 48 h Retry-Sturm."""
    body = json.dumps({**EVENT, "type": "pass.teleported"}).encode()

    assert _post(client, body).status_code == 204
    assert memory_backend.messages[0].action == "pass.teleported"


def test_missing_signature_header(client, memory_backend):
    response = client.post(PATH, content=json.dumps(EVENT).encode())
    assert response.status_code == 401
    assert memory_backend.messages == []


def test_wrong_secret(client, memory_backend):
    body = json.dumps(EVENT).encode()
    assert _post(client, body, secret="falsch").status_code == 401
    assert memory_backend.messages == []


def test_tampered_body(client, memory_backend):
    body = json.dumps(EVENT).encode()
    timestamp = int(time.time())
    response = client.post(
        PATH,
        content=body.replace(b"12345", b"99999"),
        headers={SIGNATURE_HEADER: sign(TEST_SECRET, timestamp, body)},
    )
    assert response.status_code == 401
    assert memory_backend.messages == []


def test_stale_timestamp(client, memory_backend):
    body = json.dumps(EVENT).encode()
    assert _post(client, body, now=int(time.time()) - 301).status_code == 401
    assert memory_backend.messages == []


def test_malformed_envelope(client, memory_backend):
    """Signatur gültig, aber strukturell kein Envelope -> 400."""
    body = json.dumps({"id": "evt_1"}).encode()
    assert _post(client, body).status_code == 400
    assert memory_backend.messages == []


def test_body_is_not_json(client, memory_backend):
    assert _post(client, b"kein json").status_code == 400
    assert memory_backend.messages == []


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


def test_webhook_test_yields_503_when_queue_unavailable(
    client, failing_queue_backend
):
    """Auch der Testevent darf bei kaputter Queue kein 2xx bekommen — sonst
    meldet die Admin-UI den Konnektivitätstest als erfolgreich, obwohl der
    Broker weg ist. 503 ist hier korrekt und gewollt: Die UI zeigt den Test
    als fehlgeschlagen an und heidi.cloud wiederholt ihn."""
    assert _post(client, json.dumps(TEST_EVENT).encode()).status_code == 503


def test_queue_unavailable_yields_503(client, failing_queue_backend):
    """Kein 2xx bei kaputter Queue — sonst ist das Event endgültig verloren."""
    assert _post(client, json.dumps(EVENT).encode()).status_code == 503


def test_no_backend_registered_yields_503_not_500(client, no_queue_backend):
    """Fehlender/fehlkonfigurierter Entry-Point (z.B. Tippfehler im Namen) darf
    keinen 500 erzeugen — der Sender bekommt sonst einen Retry-Sturm plus wir
    einen Stacktrace statt eines klaren Infrastruktur-Signals."""
    response = _post(client, json.dumps(EVENT).encode())
    assert response.status_code == 503


def test_unexpected_backend_error_yields_503_not_500(client, exploding_queue_backend):
    """Ein Backend, das etwas anderes als QueueUnavailable wirft, muss
    trotzdem 503 liefern, nicht 500."""
    response = _post(client, json.dumps(EVENT).encode())
    assert response.status_code == 503
    assert exploding_queue_backend.messages == []


def test_oversized_body_is_rejected_with_413(client, memory_backend):
    """Beliebig große, unauthentifizierte Bodies dürfen nicht gepuffert
    werden — Memory-DoS."""
    body = b"a" * (2 * 1024 * 1024)  # 2 MiB, deutlich über dem 1-MiB-Default

    response = _post(client, body)

    assert response.status_code == 413
    assert memory_backend.messages == []


def test_malformed_content_length_header_falls_back_to_reading_body(
    client, memory_backend
):
    """Ein nicht-numerischer Content-Length-Header darf den Endpoint nicht
    zum Absturz bringen — die Größe wird stattdessen nach dem Lesen anhand
    des tatsächlichen Bodys geprüft."""
    body = json.dumps(EVENT).encode()

    response = client.post(
        PATH,
        content=body,
        headers={
            "Content-Type": "application/json",
            "Content-Length": "not-a-number",
            SIGNATURE_HEADER: sign(TEST_SECRET, int(time.time()), body),
        },
    )

    assert response.status_code == 204
    assert len(memory_backend.messages) == 1


def test_oversized_body_without_content_length_is_rejected_with_413(
    client, memory_backend
):
    """Fehlt Content-Length (z.B. Chunked-Transfer ohne den Header), muss die
    Größe nach dem Lesen des Bodys geprüft werden."""
    body = b"a" * (2 * 1024 * 1024)  # 2 MiB

    def _chunks():
        yield body

    response = client.post(
        PATH,
        content=_chunks(),
        headers={
            "Content-Type": "application/json",
            SIGNATURE_HEADER: sign(TEST_SECRET, int(time.time()), body),
        },
    )

    assert response.status_code == 413
    assert memory_backend.messages == []


async def test_oversized_chunked_body_is_rejected_without_full_buffering(
    memory_backend, monkeypatch
):
    """Ohne ``Content-Length`` (Chunked Transfer Encoding, z.B. weil ein
    Angreifer den Header schlicht weglässt) darf der Body NICHT vollständig
    gepuffert werden, bevor die Größenprüfung greift — sonst Memory-DoS trotz
    konfiguriertem Limit. Der Endpoint muss stattdessen inkrementell lesen und
    abbrechen, sobald das Limit überschritten ist.

    ``fastapi.testclient.TestClient`` liest Generator-Bodies vor dem Senden
    komplett in den Speicher (``httpx.Request.read()``) und sendet dann exakte
    Bytes — das verschleiert genau das Verhalten, das hier geprüft werden
    soll. Deshalb ein roher ``httpx.AsyncClient`` mit ``ASGITransport`` und
    einem async Generator als Body, der wirklich chunked (ohne
    Content-Length) und lazy an die ASGI-App gereicht wird.
    """
    from edutap.webhook_heidi.handlers import fastapi as fastapi_module

    monkeypatch.setattr(fastapi_module.settings, "max_body_bytes", 1024)

    chunk = b"a" * 4096
    total_chunks = 25  # 100 KB gesamt, weit über dem 1-KiB-Testlimit
    consumed = {"n": 0}

    async def body_stream():
        for _ in range(total_chunks):
            consumed["n"] += 1
            yield chunk

    app = FastAPI()
    app.include_router(fastapi_module.router)
    transport = httpx.ASGITransport(app=app)

    async with httpx.AsyncClient(transport=transport, base_url="http://test") as ac:
        response = await ac.post(PATH, content=body_stream())

    assert response.status_code == 413
    # Der eigentliche Kern des Tests: nicht alle 25 Chunks (100 KB) dürfen
    # gelesen worden sein, bevor abgebrochen wurde — mit einem 1-KiB-Limit
    # genügen wenige Chunks, um die Grenze zu überschreiten.
    assert consumed["n"] < total_chunks
    assert memory_backend.messages == []


def test_unknown_reason_is_accepted(client, memory_backend):
    """Unbekannte ``reason``-Werte dürfen NICHT abgelehnt werden — sonst 48 h
    Retry-Sturm, exakt wie beim unbekannten ``type``."""
    body = json.dumps(
        {**EVENT, "data": {**EVENT["data"], "reason": "galactic_alignment"}}
    ).encode()

    assert _post(client, body).status_code == 204


def test_empty_string_error_fields_are_accepted(client, memory_backend):
    """``error: {"category": "", "message": ""}`` ist strukturell gültig
    (leere Strings, kein ``null``) und muss durchgehen."""
    body = json.dumps({**EVENT, "error": {"category": "", "message": ""}}).encode()

    assert _post(client, body).status_code == 204


def test_unknown_fields_in_data_and_envelope_are_accepted(client, memory_backend):
    """Neue, unbekannte Felder — sowohl im Envelope als auch in ``data`` —
    müssen durchgehen (``extra="allow"`` auf beiden Modellen)."""
    body = json.dumps(
        {
            **EVENT,
            "future_envelope_field": "irgendwas",
            "data": {**EVENT["data"], "future_data_field": "irgendwas"},
        }
    ).encode()

    assert _post(client, body).status_code == 204


def test_unsigned_non_json_body_is_rejected_with_401_not_400(client, memory_backend):
    """Reihenfolge-Test: Die Signaturprüfung muss VOR dem Parsen laufen. Ein
    unsignierter, nicht-JSON Body muss deshalb 401 ergeben, nicht 400 — sonst
    hätte ein Angreifer ohne gültige Signatur schon strukturelles Feedback."""
    response = client.post(PATH, content=b"kein json und unsigniert")

    assert response.status_code == 401
    assert memory_backend.messages == []


def test_invalid_signature_logs_warning_without_secrets(client, memory_backend, caplog):
    """401 wird geloggt (Hinweis auf mögliche Secret-Rotation), aber ohne Body
    oder Signaturwert — keine Payloads/Secrets in Logs."""
    body = json.dumps(EVENT).encode()

    with caplog.at_level(
        logging.WARNING, logger="edutap.webhook_heidi.handlers.fastapi"
    ):
        response = client.post(
            PATH,
            content=body,
            headers={SIGNATURE_HEADER: "t=1,v1=deadbeefdeadbeef"},
        )

    assert response.status_code == 401
    records = [
        r for r in caplog.records if r.name == "edutap.webhook_heidi.handlers.fastapi"
    ]
    assert len(records) == 1
    assert records[0].levelno == logging.WARNING
    logged = records[0].getMessage()
    assert "deadbeefdeadbeef" not in logged
    assert EVENT["data"]["person_id"] not in logged
    assert EVENT["id"] not in logged


def test_malformed_envelope_logs_info_with_reason(client, memory_backend, caplog):
    body = json.dumps({"id": "evt_1"}).encode()

    with caplog.at_level(logging.INFO, logger="edutap.webhook_heidi.handlers.fastapi"):
        response = _post(client, body)

    assert response.status_code == 400
    records = [
        r for r in caplog.records if r.name == "edutap.webhook_heidi.handlers.fastapi"
    ]
    assert len(records) == 1
    assert records[0].levelno == logging.INFO


def test_malformed_envelope_400_log_does_not_leak_pii(client, memory_backend, caplog):
    """Der 400-Log darf nur Struktur (Fehlerorte), niemals Feldwerte enthalten
    — sonst landet z.B. die Matrikelnummer (``person_id`` bei LMU) auf
    INFO-Level im Log. pydantic v2 bettet Werte in ``str(ValidationError)``
    und in jedem einzelnen ``exc.errors()``-Eintrag (Key ``input``) ein, daher
    darf weder das eine noch das andere ungefiltert geloggt werden."""
    body = json.dumps(
        {
            **EVENT,
            "data": {
                # pass_id fehlt bewusst -> ValidationError mit eingebettetem
                # input_value, der die Matrikelnummer enthält.
                "person_id": "MATRIKELNUMMER-12345",
            },
        }
    ).encode()

    with caplog.at_level(logging.INFO, logger="edutap.webhook_heidi.handlers.fastapi"):
        response = _post(client, body)

    assert response.status_code == 400
    records = [
        r for r in caplog.records if r.name == "edutap.webhook_heidi.handlers.fastapi"
    ]
    assert len(records) == 1
    logged = records[0].getMessage()
    assert "MATRIKELNUMMER-12345" not in logged


def test_oversized_body_logs_warning_with_size(client, memory_backend, caplog):
    body = b"a" * (2 * 1024 * 1024)

    with caplog.at_level(
        logging.WARNING, logger="edutap.webhook_heidi.handlers.fastapi"
    ):
        response = _post(client, body)

    assert response.status_code == 413
    records = [
        r for r in caplog.records if r.name == "edutap.webhook_heidi.handlers.fastapi"
    ]
    assert len(records) == 1
    assert records[0].levelno == logging.WARNING
    assert str(len(body)) in records[0].getMessage()


def test_queue_unavailable_logs_error_with_event_id(
    client, failing_queue_backend, caplog
):
    with caplog.at_level(logging.ERROR, logger="edutap.webhook_heidi.handlers.fastapi"):
        response = _post(client, json.dumps(EVENT).encode())

    assert response.status_code == 503
    records = [
        r for r in caplog.records if r.name == "edutap.webhook_heidi.handlers.fastapi"
    ]
    assert len(records) == 1
    assert records[0].levelno == logging.ERROR
    assert EVENT["id"] in records[0].getMessage()


def test_trailing_slash_is_accepted_directly(client, memory_backend):
    """IMPORTANT 2 (Abschluss-Review): ``POST {handler_prefix}/`` (mit
    Slash) darf NICHT auf 307 umgeleitet werden. heidi.cloud folgt keinen
    Redirects (httpx-Default: ``follow_redirects=False``) und wertet nur 2xx
    als Erfolg -- ein 307 wäre für den Sender ein Fehlschlag, der 12x über
    48 h wiederholt wird und dann endgültig verloren geht, ohne dass unser
    Code je etwas davon loggt (der Redirect passiert vor dem Handler).

    ``follow_redirects=False`` hier ist bewusst explizit, nicht der
    TestClient-Default -- er soll genau das reale heidi.cloud-Verhalten
    nachbilden, nicht das großzügigere httpx-Test-Default."""
    body = json.dumps(EVENT).encode()
    response = client.post(
        PATH + "/",
        content=body,
        headers={
            "Content-Type": "application/json",
            SIGNATURE_HEADER: sign(TEST_SECRET, int(time.time()), body),
        },
        follow_redirects=False,
    )

    assert response.status_code == 204
    assert len(memory_backend.messages) == 1


def test_successful_enqueue_logs_debug_with_event_id(client, memory_backend, caplog):
    with caplog.at_level(logging.DEBUG, logger="edutap.webhook_heidi.handlers.fastapi"):
        response = _post(client, json.dumps(EVENT).encode())

    assert response.status_code == 204
    records = [
        r for r in caplog.records if r.name == "edutap.webhook_heidi.handlers.fastapi"
    ]
    assert len(records) == 1
    assert records[0].levelno == logging.DEBUG
    assert EVENT["id"] in records[0].getMessage()
