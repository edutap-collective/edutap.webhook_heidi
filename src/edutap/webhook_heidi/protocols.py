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

        Identität, nicht Gleichheit: ``ack()`` muss dasselbe Objekt
        bekommen, das ``consume()`` geliefert hat, nicht eine gleichwertige
        Kopie. Ein Consumer, der die Nachricht z.B. durch eine eigene Queue
        schickt (``QueueMessage.model_validate(m.model_dump())``) und dann
        die Kopie ackt, committet dadurch nichts — ohne Fehler, ohne Log
        (sofern das Backend das nicht selbst meldet) —, was zu einer
        Redelivery-Endlosschleife ohne jeden Hinweis führt. Der Aufrufer
        muss die Nachricht also bis zum ``ack()`` referenzieren, statt sie
        zu re-serialisieren.
        """
        ...

    async def stop(self) -> None:
        """Fährt Producer/Consumer sauber herunter."""
        ...
