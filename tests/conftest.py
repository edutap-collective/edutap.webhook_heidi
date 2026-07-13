"""Gemeinsame Fixtures.

Wichtig: ``handlers.fastapi`` instanziiert ``Settings()`` beim Import (der
Router-Prefix kommt aus den Settings — Hauskonvention von wallet_google/apple).
``webhook_secret`` hat keinen Default, also muss die Env-Var gesetzt sein, BEVOR
das Modul importiert wird. Deshalb hier auf Modulebene, nicht in einer Fixture.
"""

from edutap.webhook_heidi.plugins import add_plugin
from edutap.webhook_heidi.plugins import get_queue_backend
from edutap.webhook_heidi.plugins import reset_queue_backend
from edutap.webhook_heidi.queues.memory import InMemoryQueueBackend
from edutap.webhook_heidi.settings import ENV_PREFIX
from edutap.webhook_heidi.settings import Settings

import asyncio
import contextlib
import os
import pytest
import uuid


TEST_SECRET = "0123456789abcdef" * 4

os.environ.setdefault("EDUTAP_WEBHOOK_HEIDI_WEBHOOK_SECRET", TEST_SECRET)

# Name, nicht Wert: der Default ("localhost:9092") lebt bereits in
# ``Settings.kafka_bootstrap_servers``; hier nur der Env-Var-Name für die
# Fixture, damit Fixture und Settings garantiert dieselbe Adresse verwenden.
KAFKA_BOOTSTRAP_ENV = f"{ENV_PREFIX}KAFKA_BOOTSTRAP_SERVERS"
# Harte Gegenmaßnahme gegen stille grüne CI: gesetzt (z.B. in der CI), heißt
# "ein Broker MUSS da sein" -> pytest.fail statt skip, wenn er fehlt.
KAFKA_REQUIRE_ENV = f"{ENV_PREFIX}TEST_REQUIRE_KAFKA"


def _kafka_bootstrap_servers() -> str:
    default = Settings.model_fields["kafka_bootstrap_servers"].default
    return os.environ.get(KAFKA_BOOTSTRAP_ENV, default)


def _kafka_required() -> bool:
    return os.environ.get(KAFKA_REQUIRE_ENV, "").strip().lower() not in (
        "",
        "0",
        "false",
        "no",
    )


@pytest.fixture
def memory_backend() -> InMemoryQueueBackend:
    """Hängt das In-Memory-Backend ein und räumt danach auf."""
    reset_queue_backend()
    add_plugin(InMemoryQueueBackend)
    backend = get_queue_backend()
    yield backend
    reset_queue_backend()


async def _kafka_broker_available(bootstrap_servers: str, timeout: float = 2.5) -> bool:
    """Startet probeweise einen Producer gegen ``bootstrap_servers``.

    Ein reiner TCP-Connect reicht nicht: Auf diesem Rechner hält z.B. VS Code
    einen Port-Forward auf 127.0.0.1:9092 offen, der TCP-Connects annimmt,
    aber kein Kafka spricht. Erst ein echter Protokoll-Handshake (Producer
    startet -> Broker-Metadaten abrufen) beweist, dass dort wirklich Kafka
    lauscht.
    """
    try:
        from aiokafka import AIOKafkaProducer
    except ImportError:
        return False

    producer = AIOKafkaProducer(
        bootstrap_servers=bootstrap_servers,
        request_timeout_ms=int(timeout * 1000),
    )
    try:
        await asyncio.wait_for(producer.start(), timeout=timeout)
    except Exception:
        return False
    else:
        return True
    finally:
        with contextlib.suppress(Exception):
            await asyncio.wait_for(producer.stop(), timeout=timeout)


async def _wait_for_kafka_broker(
    bootstrap_servers: str, retry_seconds: float = 30.0, interval: float = 2.0
) -> bool:
    """Pollt ``_kafka_broker_available`` bis zu ``retry_seconds`` lang.

    MINOR 6: Der CI-Service-Container (``.github/workflows/tests.yaml``) hat
    keinen Healthcheck, und der bisherige Probe war ein einmaliger
    2,5-s-Versuch. Da der Test-Job ``EDUTAP_WEBHOOK_HEIDI_TEST_REQUIRE_KAFKA=1``
    setzt, wurde ein Broker, der beim ersten Testlauf noch hochfährt, damit
    zu einem harten CI-Fail statt zu einem sauberen Warten — Flake-Risiko.

    Bewusst als eigene Funktion (statt den Timeout in
    ``_kafka_broker_available`` selbst zu erhöhen): lokal, ohne einen
    absichtlich laufenden Broker, soll ein Entwickler weiterhin nach ~2,5s
    den Skip-Hinweis sehen, nicht 30s auf einen Broker warten, der nie
    kommt. Aufgerufen wird diese Funktion deshalb nur im
    ``TEST_REQUIRE_KAFKA``-Pfad, siehe ``kafka_settings``.
    """
    deadline = asyncio.get_event_loop().time() + retry_seconds
    while True:
        if await _kafka_broker_available(bootstrap_servers):
            return True
        if asyncio.get_event_loop().time() >= deadline:
            return False
        await asyncio.sleep(interval)


@pytest.fixture
async def kafka_settings() -> Settings:
    """Settings gegen einen echten Kafka-Broker.

    Die Adresse kommt aus ``EDUTAP_WEBHOOK_HEIDI_KAFKA_BOOTSTRAP_SERVERS``
    (Default ``localhost:9092``) — lokal auf einen freien Port ausweichen,
    ohne Code anzufassen, z.B.::

        docker run -d -p 19092:19092 ... apache/kafka:latest
        EDUTAP_WEBHOOK_HEIDI_KAFKA_BOOTSTRAP_SERVERS=localhost:19092 uv run pytest

    Ist kein Broker erreichbar:

    - lokal (``EDUTAP_WEBHOOK_HEIDI_TEST_REQUIRE_KAFKA`` nicht gesetzt): Test
      wird übersprungen, mit Hinweis, wie man einen Broker startet.
    - in der CI (``EDUTAP_WEBHOOK_HEIDI_TEST_REQUIRE_KAFKA=1``): Test schlägt
      hart fehl (``pytest.fail``), statt still grün zu bleiben — sonst wäre
      das einzige produktive Queue-Backend in der Pipeline nie getestet.
      Da der CI-Service-Container keinen eigenen Healthcheck hat, wird hier
      bis zu 30s in 2-s-Schritten erneut geprobt (``_wait_for_kafka_broker``),
      statt beim ersten von einem noch startenden Broker verpassten Versuch
      sofort hart zu scheitern (Flake-Risiko).
    """
    bootstrap_servers = _kafka_bootstrap_servers()
    required = _kafka_required()
    probe = _wait_for_kafka_broker if required else _kafka_broker_available
    if not await probe(bootstrap_servers):
        message = (
            f"Kein Kafka-Broker unter {bootstrap_servers} erreichbar. Lokal "
            "z.B. `docker run -d -p 19092:19092 ... apache/kafka:latest` "
            f"starten und {KAFKA_BOOTSTRAP_ENV}=localhost:19092 setzen."
        )
        if required:
            pytest.fail(message)
        pytest.skip(message)
    return Settings(
        _env_file=None,
        webhook_secret=TEST_SECRET,
        kafka_bootstrap_servers=bootstrap_servers,
        kafka_topic=f"test.pass-events.{uuid.uuid4().hex[:8]}",
        kafka_consumer_group=f"test-group-{uuid.uuid4().hex[:8]}",
    )
