"""In-Memory-Backend — für Tests und lokale Entwicklung, ohne Broker."""

from collections.abc import AsyncIterator
from edutap.webhook_heidi.models import QueueMessage


class InMemoryQueueBackend:
    """Hält die Nachrichten in einer Liste. Nicht für den Produktivbetrieb."""

    def __init__(self) -> None:
        self.messages: list[QueueMessage] = []
        self.acked: list[str] = []

    async def enqueue(self, message: QueueMessage) -> None:
        self.messages.append(message)

    async def consume(self) -> AsyncIterator[QueueMessage]:
        for message in list(self.messages):
            yield message

    async def ack(self, message: QueueMessage) -> None:
        self.acked.append(message.eventid)

    async def stop(self) -> None:
        return None
