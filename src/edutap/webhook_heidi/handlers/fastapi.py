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
from edutap.webhook_heidi.settings import Settings
from edutap.webhook_heidi.signing import SIGNATURE_HEADER
from edutap.webhook_heidi.signing import verify
from fastapi import APIRouter
from fastapi import HTTPException
from fastapi import Request
from fastapi import Response

import logging
import pydantic
import time


logger = logging.getLogger(__name__)

settings = Settings()

# Fail-fast-Versuch: im Produktivbetrieb ist das Backend über einen
# statischen Entry-Point (pyproject.toml) registriert, also schon beim Import
# auflösbar — schlägt das fehl, soll der Prozess möglichst beim Start
# scheitern, nicht erst beim ersten Request (genau wie beim fehlenden
# Secret). In Tests wird das Backend dagegen oft erst per Fixture NACH
# diesem Import registriert (siehe tests/conftest.py::memory_backend); ein
# hier unaufgelöstes Backend darf den Import deshalb NICHT sprengen. Bleibt
# es auch beim ersten echten Request unauflösbar, fängt der Enqueue-Pfad das
# unten ab (503, nicht 500).
try:
    get_queue_backend()
except (NotImplementedError, ValueError):
    logger.warning(
        "Kein Queue-Backend beim Import auflösbar — wird beim ersten Request "
        "erneut versucht; bleibt es dann weiterhin unauflösbar, antwortet der "
        "Endpoint mit 503 statt zu enqueuen."
    )

router = APIRouter(
    prefix=settings.handler_prefix,
    tags=["edutap.webhook_heidi"],
)


async def _read_body_limited(request: Request, limit: int) -> bytes:
    """Liest den Request-Body inkrementell über ``request.stream()`` und
    bricht ab, sobald ``limit`` überschritten wird.

    Der ``Content-Length``-Vorcheck im Aufrufer greift nur, wenn der Header
    gesetzt und korrekt ist — ein Angreifer kann ihn (z.B. via Chunked
    Transfer Encoding) einfach weglassen. Ohne inkrementelles Lesen würde
    ``await request.body()`` den kompletten Body erst unbegrenzt puffern und
    die Größenprüfung käme zu spät (Memory-DoS). Deshalb hier chunkweise
    lesen und bei Überschreitung SOFORT abbrechen, statt weiterzupuffern.
    """
    chunks: list[bytes] = []
    size = 0
    async for chunk in request.stream():
        size += len(chunk)
        if size > limit:
            logger.warning(
                "Payload zu groß (mindestens %s Bytes gelesen, Limit=%s Bytes).",
                size,
                limit,
            )
            raise HTTPException(status_code=413, detail="Payload too large.")
        chunks.append(chunk)
    return b"".join(chunks)


@router.post(
    "",
    status_code=204,
    responses={
        200: {
            "description": "Konnektivitätstest (`webhook.test`) angenommen, "
            "nicht enqueued."
        },
        204: {"description": "Event erfolgreich enqueued."},
        400: {
            "description": "Body ist strukturell kein Envelope (kein gültiges "
            "JSON oder Pflichtfelder fehlen)."
        },
        401: {"description": "Signatur fehlt, ist ungültig oder abgelaufen."},
        413: {
            "description": "Body überschreitet die konfigurierte "
            "Maximalgröße (`max_body_bytes`)."
        },
        503: {
            "description": "Queue-Backend nicht erreichbar oder nicht "
            "konfiguriert — bitte später erneut senden."
        },
    },
)
@router.post("/", status_code=204, include_in_schema=False)
async def handle_pass_event(request: Request) -> Response:
    """Nimmt ein Pass-Event von heidi.cloud entgegen und schreibt es in die Queue."""
    # Größe VOR jedem Puffern prüfen — unauthentifiziert beliebig viel
    # Speicher zu belegen ist ein DoS. Ist Content-Length vorhanden, lässt
    # sich das ganz ohne Body-Lesen entscheiden.
    content_length = request.headers.get("content-length")
    if content_length is not None:
        try:
            declared_length: int | None = int(content_length)
        except ValueError:
            declared_length = None
        if declared_length is not None and declared_length > settings.max_body_bytes:
            logger.warning(
                "Payload zu groß (Content-Length=%s Bytes, Limit=%s Bytes).",
                declared_length,
                settings.max_body_bytes,
            )
            raise HTTPException(status_code=413, detail="Payload too large.")

    # Raw Bytes, VOR jedem Parsen: Retries tragen dieselbe Nachricht mit anderen
    # Bytes (JSONB-Roundtrip), pro Versuch neu signiert. Inkrementell lesen
    # (nicht ``request.body()``) — fehlt Content-Length (Chunked Transfer
    # Encoding), ist das die eigentliche Absicherung gegen Memory-DoS, nicht
    # nur ein Nachtrag.
    body = await _read_body_limited(request, settings.max_body_bytes)

    if not verify(
        settings.webhook_secret.get_secret_value(),
        request.headers.get(SIGNATURE_HEADER, ""),
        body,
        now=int(time.time()),
        tolerance_seconds=settings.signature_tolerance_seconds,
    ):
        # Kein Body, keine Signatur im Log — nur der Hinweis, dass es
        # passiert ist (mögliche Ursache: rotiertes Secret).
        logger.warning(
            "Signaturprüfung fehlgeschlagen — evtl. rotiertes Secret? (%s %s)",
            request.method,
            request.url.path,
        )
        raise HTTPException(status_code=401, detail="Invalid signature.")

    try:
        event = WebhookEvent.model_validate_json(body)
    except pydantic.ValidationError as exc:
        # NIEMALS str(exc) oder exc.errors() (im Ganzen) loggen: pydantic v2
        # bettet in beiden den validierten Eingabewert ein (``input_value``
        # bzw. Key ``input``) — bei LMU z.B. die Matrikelnummer in
        # ``person_id``. Deshalb gezielt nur die strukturelle Fehlerort
        # herausziehen, nie den Wert.
        error_locations = [e["loc"] for e in exc.errors()]
        logger.info(
            "Envelope-Validierung fehlgeschlagen (%s Fehler, Orte=%s).",
            exc.error_count(),
            error_locations,
        )
        raise HTTPException(
            status_code=400, detail="Malformed event envelope."
        ) from exc

    if event.type == WEBHOOK_TEST:
        return Response(status_code=200)

    # Bewusst VOR dem try-Block: ein Modellfehler hier wäre ein eigener Bug
    # (nicht ein Backend-/Infrastrukturproblem) und soll deshalb nicht vom
    # breiten except unten fälschlich als 503 maskiert werden.
    message = QueueMessage.from_event(event)

    try:
        await get_queue_backend().enqueue(message)
    except Exception as exc:
        # Absichtlich breit: sowohl der erwartete Fall (QueueUnavailable, kein
        # Backend registriert -> NotImplementedError/ValueError) als auch
        # alles Unerwartete (ConnectionResetError, asyncio.TimeoutError, ...)
        # werden zu 503, nicht 500. Ein 503 sagt dem Sender "später nochmal";
        # ein 500 täte dasselbe (Non-2xx -> Retry), sähe aber wie unser Bug
        # aus statt wie ein Infrastrukturproblem und würde unnötig Stacktraces
        # und Alerts erzeugen. logger.exception() (statt .error()), damit der
        # Traceback trotzdem erhalten bleibt — sonst maskieren wir eigene Bugs
        # spurlos.
        logger.exception("Enqueue fehlgeschlagen (event.id=%s).", event.id)
        raise HTTPException(status_code=503, detail="Queue unavailable.") from exc

    logger.debug("Event enqueued (event.id=%s).", event.id)
    return Response(status_code=204)
