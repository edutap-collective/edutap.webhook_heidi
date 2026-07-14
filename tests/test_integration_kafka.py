"""MINOR 6 (Abschluss-Review): Naht-Test Endpoint -> Kafka.

Jeder bisherige Test deckt nur seine eigene Schicht: ``test_handlers_fastapi.py``
prüft den Endpoint nur gegen das In-Memory-Backend, ``test_queues_kafka.py``
prüft ``KafkaQueueBackend`` nur direkt (ohne den Endpoint dazwischen). Ein
Bruch genau an der Naht -- z.B. ein Feld, das beim Serialisieren im Endpoint
verlorengeht, oder ein falscher Partition-Key, der erst beim echten
Enqueue/Consume-Roundtrip sichtbar wird -- fiele in der CI nicht auf.

Dieser Test geht deshalb den kompletten, echten Pfad: ein ASGI-POST (signiertes
Event) -> ``router`` -> ``KafkaQueueBackend`` als registriertes Plugin ->
echter Broker -> ``consume()``/``ack()``.

**Bewusste Abweichung von der Hauskonvention** (Spec §7: "Hauskonvention ist
``fastapi.testclient.TestClient``"): Dieser Test nutzt stattdessen einen rohen
``httpx.AsyncClient`` mit ``ASGITransport`` (wie bereits in
``test_handlers_fastapi.py::test_oversized_chunked_body_is_rejected_without_full_buffering``
präzediert). Grund: ``TestClient`` öffnet für jeden ``.post()``-Aufruf, der
NICHT innerhalb von ``with TestClient(...) as client:`` läuft, einen eigenen,
kurzlebigen ``anyio``-Portal-Thread mit einem EIGENEN Event-Loop (siehe
``starlette.testclient.TestClient._portal_factory``) -- dieser Loop wird direkt
nach dem Request wieder geschlossen. Der lazy erzeugte aiokafka-Producer
(``KafkaQueueBackend._get_producer()``) bindet sich beim ersten Request an
GENAU diesen Loop; jeder weitere Zugriff (zweiter Request, ``consume()``/
``ack()``/``stop()`` im Test selbst) läuft dann auf einem ANDEREN Loop und
scheitert -- empirisch reproduziert: zweiter ``TestClient``-Call hing bis zum
Enqueue-Timeout ("Topic ... not found in cluster metadata", weil der
Producer-Hintergrundtask auf dem toten Loop nie mehr lief), und
``backend.stop()`` beim Teardown warf ``RuntimeError: Event loop is closed``.
``httpx.AsyncClient`` mit ``ASGITransport`` führt den ASGI-Call dagegen direkt
im aktuell laufenden (pytest-asyncio-)Loop aus -- demselben Loop, in dem auch
``kafka_backend_client`` den Producer erzeugt und in dem der Test selbst
``consume()``/``ack()``/``stop()`` aufruft.
"""

from conftest import TEST_SECRET
from edutap.webhook_heidi.plugins import add_plugin
from edutap.webhook_heidi.plugins import get_queue_backend
from edutap.webhook_heidi.plugins import reset_queue_backend
from edutap.webhook_heidi.queues.kafka import KafkaQueueBackend
from edutap.webhook_heidi.settings import Settings
from edutap.webhook_heidi.signing import sign
from edutap.webhook_heidi.signing import SIGNATURE_HEADER
from fastapi import FastAPI

import httpx
import json
import pytest
import time


pytestmark = pytest.mark.kafka

PATH = "/webhook/heidi"


def _event(
    *,
    eventid: str,
    pass_id: str,
    created: str,
    event_type: str = "pass.installed",
) -> dict:
    return {
        "id": eventid,
        "type": event_type,
        "created": created,
        "api_version": "2026-07-09",
        "data": {
            "pass_id": pass_id,
            "person_id": "12345",
            "wallet_type": "APPLE_ACCESS",
            "state": "ACTIVE",
            "reason": "provisioning",
            "confirmation": "device",
            "preset": {
                "name": "Standard",
                "options": {"color": "blue", "nested": {"depth": 2}},
            },
            "device": {
                "bundle_identifier": "com.example.app",
                "platform": "ios",
            },
        },
    }


@pytest.fixture
async def kafka_backend_client(kafka_settings: Settings):
    """Registriert das echte ``KafkaQueueBackend`` -- gegen ``kafka_settings``
    (eigener, zufälliger Topic/Consumer-Group pro Testlauf) -- als Plugin und
    liefert einen ``httpx.AsyncClient`` gegen den echten ``router`` (kein Mock
    dazwischen). Siehe Moduldocstring zur Begründung von ``httpx.AsyncClient``
    statt ``TestClient``.

    Async-Generator-Fixture (nicht ``asyncio.run()`` im Teardown): Producer/
    Consumer werden im Event-Loop DIESES async Tests erzeugt (asyncio.Lock,
    aiokafka-interne Tasks) und müssen im selben Loop wieder gestoppt
    werden."""

    class _KafkaBackendUnderTest(KafkaQueueBackend):
        def __init__(self, settings: Settings | None = None) -> None:
            super().__init__(kafka_settings)

    reset_queue_backend()
    add_plugin(_KafkaBackendUnderTest)

    from edutap.webhook_heidi.handlers.fastapi import router

    app = FastAPI()
    app.include_router(router)
    transport = httpx.ASGITransport(app=app)
    async with httpx.AsyncClient(transport=transport, base_url="http://test") as client:
        try:
            yield client
        finally:
            await get_queue_backend().stop()
            reset_queue_backend()


async def _post(client: httpx.AsyncClient, event: dict) -> httpx.Response:
    body = json.dumps(event).encode()
    return await client.post(
        PATH,
        content=body,
        headers={
            "Content-Type": "application/json",
            SIGNATURE_HEADER: sign(TEST_SECRET, int(time.time()), body),
        },
    )


async def test_endpoint_to_kafka_roundtrip_preserves_payload_and_timestamp(
    kafka_backend_client,
):
    """Der komplette Pfad: signiertes POST -> Endpoint -> echter Broker ->
    consume()/ack(). ``payload`` (inkl. verschachtelter ``preset``/``device``)
    muss unverändert ankommen, ``timestamp`` muss exakt ``created`` sein."""
    event = _event(
        eventid="evt_seam_1",
        pass_id="pass-seam-1",
        created="2026-07-09T12:34:56Z",
    )

    response = await _post(kafka_backend_client, event)
    assert response.status_code == 204

    backend = get_queue_backend()
    seen = []
    async for message in backend.consume():
        seen.append(message)
        await backend.ack(message)
        break

    assert len(seen) == 1
    message = seen[0]
    assert message.eventid == "evt_seam_1"
    assert message.passid == "pass-seam-1"
    assert message.personid == "12345"
    assert message.action == "pass.installed"
    assert message.timestamp.year == 2026
    assert message.timestamp.hour == 12
    assert message.timestamp.minute == 34
    assert message.timestamp.second == 56
    # payload muss `data` VOLLSTÄNDIG und UNVERÄNDERT enthalten, inkl.
    # verschachtelter Felder -- ein Bruch an der Naht (z.B. verlustbehaftete
    # Serialisierung) würde genau hier sichtbar.
    assert message.payload["preset"] == {
        "name": "Standard",
        "options": {"color": "blue", "nested": {"depth": 2}},
    }
    assert message.payload["device"] == {
        "bundle_identifier": "com.example.app",
        "platform": "ios",
    }
    assert message.payload["state"] == "ACTIVE"
    assert message.payload["wallet_type"] == "APPLE_ACCESS"


async def test_endpoint_to_kafka_preserves_order_per_passid(kafka_backend_client):
    """Zwei Events mit DERSELBEN ``passid``, über den echten Endpoint
    gesendet, müssen in Sendereihenfolge ankommen -- das ist die einzige
    Ordnungsgarantie, die verhindert, dass z.B. ein ``pass.uninstalled`` vor
    dem zugehörigen ``pass.installed`` verarbeitet wird."""
    same_pass_id = "pass-seam-order"
    first = _event(
        eventid="evt_seam_order_1",
        pass_id=same_pass_id,
        created="2026-07-09T12:00:00Z",
        event_type="pass.installed",
    )
    second = _event(
        eventid="evt_seam_order_2",
        pass_id=same_pass_id,
        created="2026-07-09T12:05:00Z",
        event_type="pass.updated",
    )

    assert (await _post(kafka_backend_client, first)).status_code == 204
    assert (await _post(kafka_backend_client, second)).status_code == 204

    backend = get_queue_backend()
    seen = []
    async for message in backend.consume():
        seen.append(message)
        await backend.ack(message)
        if len(seen) == 2:
            break

    assert [m.eventid for m in seen] == ["evt_seam_order_1", "evt_seam_order_2"]
    assert [m.action for m in seen] == ["pass.installed", "pass.updated"]
