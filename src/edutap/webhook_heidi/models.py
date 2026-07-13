"""Datenmodelle: der eingehende Envelope und die Queue-Nachricht.

Der Envelope wird bewusst LAX validiert. heidi.cloud behandelt jedes Non-2xx als
Fehlschlag und wiederholt 12x über 48 h — strenge Validierung erzeugt also keinen
sauberen Reject, sondern einen Retry-Sturm. Unbekannte Event-Typen, unbekannte
``reason``-Werte und neue Felder müssen deshalb durchgehen.
"""

from datetime import datetime
from pydantic import BaseModel
from pydantic import ConfigDict
from typing import Any


WEBHOOK_TEST = "webhook.test"
"""Konnektivitätstest aus der heidi.cloud-Admin-UI. Wird angenommen, aber nicht
in die Queue geschrieben (Null-UUID als pass_id)."""


class WebhookEventData(BaseModel):
    """``data`` aus dem Envelope. Nur die Felder, die wir wirklich brauchen —
    alles Übrige wird durchgereicht (``extra="allow"``)."""

    model_config = ConfigDict(extra="allow")

    pass_id: str
    person_id: str


class WebhookEvent(BaseModel):
    """Der Envelope von heidi.cloud.

    ``type`` ist bewusst ``str`` und kein Enum: der Sender darf Event-Typen
    additiv ergänzen, ohne dass wir mit 400 antworten.
    """

    model_config = ConfigDict(extra="allow")

    id: str
    type: str
    created: datetime
    api_version: str | None = None
    data: WebhookEventData


class QueueMessage(BaseModel):
    """Was in die Pass-Queue geschrieben wird."""

    eventid: str
    """``evt_…`` — Dedup-Key. Ausgewertet wird er vom Consumer, nicht von uns."""

    passid: str
    """Kafka-Partition-Key: garantiert Reihenfolge je Pass."""

    personid: str
    action: str
    timestamp: datetime
    payload: dict[str, Any]
    """``data`` roh und vollständig."""

    @classmethod
    def from_event(cls, event: WebhookEvent) -> "QueueMessage":
        return cls(
            eventid=event.id,
            passid=event.data.pass_id,
            personid=event.data.person_id,
            action=event.type,
            timestamp=event.created,
            payload=event.data.model_dump(mode="json"),
        )
