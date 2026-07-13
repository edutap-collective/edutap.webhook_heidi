"""Die austauschbare Queue-Abstraktion."""

from collections.abc import AsyncIterator
from edutap.webhook_heidi.models import QueueMessage
from typing import Protocol
from typing import runtime_checkable


class QueueUnavailable(RuntimeError):
    """Der Write konnte nicht bestätigt werden.

    Der Endpoint übersetzt das in 503 — heidi.cloud wiederholt dann. Auf keinen
    Fall 2xx antworten: der Sender wiederholt nur bei Non-2xx, ein verschluckter
    Fehler wäre ein endgültig verlorenes Event.
    """


@runtime_checkable
class QueueBackend(Protocol):
    """Beide Seiten der Pass-Queue — Producer für den Webhook, Consumer für den
    Spooler. Ein Consumer soll weder aiokafka noch Offsets kennen müssen."""

    async def enqueue(self, message: QueueMessage) -> None:
        """Schreibt die Nachricht dauerhaft in die Queue.

        :raises QueueUnavailable: wenn der Write nicht bestätigt wurde.
        """
        ...

    def consume(self) -> AsyncIterator[QueueMessage]:
        """Liefert Nachrichten, bis der Consumer abbricht."""
        ...

    async def ack(self, message: QueueMessage) -> None:
        """Bestätigt genau die übergebene Nachricht (Kafka: Offset-Commit).

        Kafka-Offset-Commits sind kumulativ (es gibt kein "committe nur
        diese eine Nachricht"), deshalb MUSS ein Consumer sequenziell
        arbeiten: konsumieren -> verarbeiten -> acken, erst danach die
        nächste Nachricht aus ``consume()`` holen. Wer Nachrichten stapelt
        und außerhalb dieser Reihenfolge (oder unvollständig) ackt,
        überspringt dabei stillschweigend die dazwischenliegenden
        Nachrichten — sie gelten als verarbeitet, obwohl sie es nie waren.
        """
        ...

    async def stop(self) -> None:
        """Fährt Producer/Consumer sauber herunter."""
        ...
