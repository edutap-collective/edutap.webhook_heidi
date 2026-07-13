"""Test-Backends. Getrennt von den Tests, damit sie per Entry-Point ladbar wären."""

from edutap.webhook_heidi.models import QueueMessage
from edutap.webhook_heidi.protocols import QueueUnavailable
from edutap.webhook_heidi.queues.memory import InMemoryQueueBackend


class FailingQueueBackend(InMemoryQueueBackend):
    """Simuliert eine nicht erreichbare Queue -> der Endpoint muss 503 liefern."""

    async def enqueue(self, message: QueueMessage) -> None:
        raise QueueUnavailable("Broker nicht erreichbar")
