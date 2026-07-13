from datetime import datetime
from datetime import timezone
from edutap.webhook_heidi.models import QueueMessage
from edutap.webhook_heidi.queues.memory import InMemoryQueueBackend


def _message(eventid: str = "evt_1") -> QueueMessage:
    return QueueMessage(
        eventid=eventid,
        passid="p1",
        personid="x",
        action="pass.installed",
        timestamp=datetime(2026, 7, 9, 12, 34, 56, tzinfo=timezone.utc),
        payload={"state": "ACTIVE"},
    )


async def test_enqueue_records_the_message():
    backend = InMemoryQueueBackend()
    await backend.enqueue(_message())
    assert [m.eventid for m in backend.messages] == ["evt_1"]


async def test_consume_yields_enqueued_messages():
    backend = InMemoryQueueBackend()
    await backend.enqueue(_message("evt_1"))
    await backend.enqueue(_message("evt_2"))

    seen = []
    async for message in backend.consume():
        seen.append(message.eventid)
        await backend.ack(message)

    assert seen == ["evt_1", "evt_2"]
    assert backend.acked == ["evt_1", "evt_2"]


async def test_duplicates_are_kept():
    """Der Webhook dedupliziert NICHT — das ist Aufgabe des Consumers."""
    backend = InMemoryQueueBackend()
    await backend.enqueue(_message("evt_1"))
    await backend.enqueue(_message("evt_1"))
    assert len(backend.messages) == 2
