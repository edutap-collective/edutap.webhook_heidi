from conftest import TEST_SECRET
from edutap.webhook_heidi.plugins import add_plugin
from edutap.webhook_heidi.plugins import reset_queue_backend
from edutap.webhook_heidi.signing import sign
from edutap.webhook_heidi.signing import SIGNATURE_HEADER
from fastapi import FastAPI
from fastapi.testclient import TestClient
from plugins import FailingQueueBackend

import json
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


@pytest.fixture
def client() -> TestClient:
    from edutap.webhook_heidi.handlers.fastapi import router

    app = FastAPI()
    app.include_router(router)
    return TestClient(app)


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


def test_webhook_test_is_accepted_but_not_enqueued(client, memory_backend):
    """Konnektivitätstest: 200, aber keine Null-UUID in der Queue."""
    body = json.dumps(
        {
            "id": "evt_test",
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
    ).encode()

    assert _post(client, body).status_code == 200
    assert memory_backend.messages == []


def test_queue_unavailable_yields_503(client):
    """Kein 2xx bei kaputter Queue — sonst ist das Event endgültig verloren."""
    reset_queue_backend()
    add_plugin(FailingQueueBackend)

    assert _post(client, json.dumps(EVENT).encode()).status_code == 503

    reset_queue_backend()
