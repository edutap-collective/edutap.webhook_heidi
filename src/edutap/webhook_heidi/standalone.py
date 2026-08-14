"""Standalone ASGI application — the webhook endpoint as a service of its own.

This package is primarily a library. A consumer mounts :data:`router` into its
own FastAPI application and registers a queue backend from its own entry points;
that is the shape the reference consumer uses. This module is the other
deployment shape: the webhook running on its own, with the Kafka backend wired
in, so that it can be deployed next to the services that read the queue.

Two decisions here are load-bearing.

**The Kafka backend is registered in this module, not as an entry point in
``pyproject.toml``.** An entry point cannot be made conditional on an extra, so
declaring it would make the backend visible to every embedding consumer as well.
A consumer that registers its own backend would then trip the "multiple
QueueBackends" guard in :func:`~edutap.webhook_heidi.plugins.get_queue_backend`
and fail — for a package whose whole point is a swappable queue. Which backend
runs is a property of the deployment, not of the package.

**The registration happens before the router is imported.**
:mod:`~edutap.webhook_heidi.handlers.fastapi` resolves the backend at import
time on purpose, so that a misconfigured deployment dies at startup instead of
answering 503 to the first real event. Registering afterwards would silently
downgrade that fail-fast to a warning plus a lookup on the first request.

``Settings.webhook_secret`` has no default, so the process refuses to start
without one. That is deliberate too: the alternative is a service that comes up
happily and rejects every signature it is sent.

**Observability is installed here, and only here.** ``install_observability``
configures structlog, Sentry and the OTLP exporter for the whole process — which a
library must never do to its consumer. An application that embeds the router instead
calls it itself, or does not, and this module is not involved either way. It is the
first thing to run, before the settings the service needs are resolved, so that a
process refusing to start is reported rather than silently absent.
"""

from collections.abc import AsyncIterator
from contextlib import asynccontextmanager
from edutap.observability_settings import install_observability
from edutap.webhook_heidi.plugins import add_plugin
from edutap.webhook_heidi.plugins import get_queue_backend
from edutap.webhook_heidi.queues.kafka import KafkaQueueBackend
from fastapi import FastAPI
from importlib.metadata import version


__version__ = version("edutap.webhook-heidi")

# Vor allem Übrigen -- auch vor add_plugin() und dem Router-Import darunter, die
# beide scheitern können. Ein Start, der fehlschlägt, soll berichtet werden und
# nicht bloß ausbleiben.
#
# Der Log-Level kommt von EDUTAP_LOG_LEVEL, nicht aus den Settings dieses Pakets:
# eine zweite Variable für denselben Wert wäre eine, die irgendwann von der
# abweicht, die tatsächlich wirkt. DEBUG macht den Weg eines Events sichtbar --
# zur Inbetriebnahme gedacht, für den Dauerbetrieb zu viel.
install_observability(service_name="edutap.webhook_heidi", service_version=__version__)

add_plugin(KafkaQueueBackend)

# Imported after the registration above, deliberately — see the module
# docstring. Keeping the fail-fast intact is worth the lint exception.
from edutap.webhook_heidi.handlers.fastapi import router  # noqa: E402


@asynccontextmanager
async def lifespan(app: FastAPI) -> AsyncIterator[None]:
    """Shut the queue backend down cleanly.

    Nothing is started eagerly: the backend connects on first use, and an
    unreachable broker is answered with 503 so that the sender retries. Stopping
    it, on the other hand, has to happen here — the producer holds a broker
    connection that would otherwise be torn down mid-flight.
    """
    yield
    await get_queue_backend().stop()


app = FastAPI(
    title="eduTAP HEIDI Webhook",
    description=(
        "Receives pass events from heidi.cloud, verifies their signature and "
        "writes them to the pass-event queue."
    ),
    version=__version__,
    lifespan=lifespan,
)
app.include_router(router)
