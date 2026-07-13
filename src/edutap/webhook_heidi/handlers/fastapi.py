"""Der Webhook-Endpoint.

Bewusst dünn: Signatur prüfen, Envelope parsen, enqueuen, 2xx. Keine
Geschäftslogik — die gehört in den Consumer.

Die Statuscodes sind Teil des Vertrags: heidi.cloud wertet JEDES Non-2xx als
Fehlschlag und wiederholt bis zu 12x über 48 h (auch bei 4xx). Deshalb wird hier
so lax wie möglich validiert und nur abgelehnt, was wir kryptografisch nicht
verifizieren können (401) oder was strukturell kein Envelope ist (400).
"""

from edutap.webhook_heidi.models import QueueMessage
from edutap.webhook_heidi.models import WEBHOOK_TEST
from edutap.webhook_heidi.models import WebhookEvent
from edutap.webhook_heidi.plugins import get_queue_backend
from edutap.webhook_heidi.protocols import QueueUnavailable
from edutap.webhook_heidi.settings import Settings
from edutap.webhook_heidi.signing import SIGNATURE_HEADER
from edutap.webhook_heidi.signing import verify
from fastapi import APIRouter
from fastapi import HTTPException
from fastapi import Request
from fastapi import Response

import pydantic
import time


settings = Settings()

router = APIRouter(
    prefix=settings.handler_prefix,
    tags=["edutap.webhook_heidi"],
)


@router.post("", status_code=204)
async def handle_pass_event(request: Request) -> Response:
    """Nimmt ein Pass-Event von heidi.cloud entgegen und schreibt es in die Queue."""
    # Raw Bytes, VOR jedem Parsen: Retries tragen dieselbe Nachricht mit anderen
    # Bytes (JSONB-Roundtrip), pro Versuch neu signiert.
    body = await request.body()

    if not verify(
        settings.webhook_secret.get_secret_value(),
        request.headers.get(SIGNATURE_HEADER, ""),
        body,
        now=int(time.time()),
        tolerance_seconds=settings.signature_tolerance_seconds,
    ):
        raise HTTPException(status_code=401, detail="Invalid signature.")

    try:
        event = WebhookEvent.model_validate_json(body)
    except pydantic.ValidationError as exc:
        raise HTTPException(
            status_code=400, detail="Malformed event envelope."
        ) from exc

    if event.type == WEBHOOK_TEST:
        return Response(status_code=200)

    try:
        await get_queue_backend().enqueue(QueueMessage.from_event(event))
    except QueueUnavailable as exc:
        # Kein 2xx: der Sender wiederholt nur bei Non-2xx.
        raise HTTPException(status_code=503, detail="Queue unavailable.") from exc

    return Response(status_code=204)
