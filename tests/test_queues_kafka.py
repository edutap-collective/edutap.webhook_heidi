from datetime import datetime
from datetime import timezone
from edutap.webhook_heidi.models import QueueMessage
from edutap.webhook_heidi.protocols import QueueBackend
from edutap.webhook_heidi.protocols import QueueUnavailable
from edutap.webhook_heidi.queues.kafka import KafkaQueueBackend
from edutap.webhook_heidi.settings import Settings

import asyncio
import pytest


pytestmark = pytest.mark.kafka


def _message(eventid: str, passid: str = "p1") -> QueueMessage:
    return QueueMessage(
        eventid=eventid,
        passid=passid,
        personid="x",
        action="pass.installed",
        timestamp=datetime(2026, 7, 9, 12, 34, 56, tzinfo=timezone.utc),
        payload={"state": "ACTIVE", "reason": "provisioning"},
    )


def test_conforms_to_protocol():
    assert issubclass(KafkaQueueBackend, QueueBackend)


async def test_roundtrip(kafka_settings):
    """enqueue -> consume -> ack, verlustfrei."""
    backend = KafkaQueueBackend(kafka_settings)
    try:
        await backend.enqueue(_message("evt_1"))
        await backend.enqueue(_message("evt_2"))

        seen = []
        async for message in backend.consume():
            seen.append(message)
            await backend.ack(message)
            if len(seen) == 2:
                break

        assert [m.eventid for m in seen] == ["evt_1", "evt_2"]
        assert seen[0].payload == {"state": "ACTIVE", "reason": "provisioning"}
        assert seen[0].timestamp.year == 2026
    finally:
        await backend.stop()


async def test_partition_key_is_the_passid(kafka_settings):
    """Reihenfolge je Pass hängt daran: gleicher passid -> gleiche Partition."""
    backend = KafkaQueueBackend(kafka_settings)
    try:
        await backend.enqueue(_message("evt_1", passid="pass-a"))
        async for _received in backend.consume():
            break
        assert backend.last_key == b"pass-a"
    finally:
        await backend.stop()


async def test_unreachable_broker_raises_queue_unavailable():
    """Der Endpoint braucht QueueUnavailable, um 503 zu liefern."""
    settings = Settings(
        _env_file=None,
        webhook_secret="s3cret",
        kafka_bootstrap_servers="localhost:1",  # nichts hört hier
        enqueue_timeout=2.0,
    )
    backend = KafkaQueueBackend(settings)
    with pytest.raises(QueueUnavailable):
        await backend.enqueue(_message("evt_1"))
    await backend.stop()


async def test_enqueue_timeout_raises_queue_unavailable(kafka_settings, monkeypatch):
    """Ein hängender Broker darf uns nicht ins 30-s-Timeout des Senders laufen lassen."""
    backend = KafkaQueueBackend(kafka_settings)
    await backend.enqueue(_message("evt_warmup"))  # Producer hochfahren

    async def _hang(*args, **kwargs):
        await asyncio.sleep(10)

    monkeypatch.setattr(backend._producer, "send_and_wait", _hang)
    backend._settings.enqueue_timeout = 0.1

    try:
        with pytest.raises(QueueUnavailable):
            await backend.enqueue(_message("evt_1"))
    finally:
        await backend.stop()
