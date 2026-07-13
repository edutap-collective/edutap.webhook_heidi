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
from edutap.webhook_heidi.settings import Settings

import os
import pytest
import socket
import uuid


TEST_SECRET = "0123456789abcdef" * 4

os.environ.setdefault("EDUTAP_WEBHOOK_HEIDI_WEBHOOK_SECRET", TEST_SECRET)


@pytest.fixture
def memory_backend() -> InMemoryQueueBackend:
    """Hängt das In-Memory-Backend ein und räumt danach auf."""
    reset_queue_backend()
    add_plugin(InMemoryQueueBackend)
    backend = get_queue_backend()
    yield backend
    reset_queue_backend()


def _broker_reachable(host: str, port: int, timeout: float = 0.5) -> bool:
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
        sock.settimeout(timeout)
        return sock.connect_ex((host, port)) == 0


@pytest.fixture
def kafka_settings() -> Settings:
    """Settings gegen den lokalen Broker. Ohne Broker: Test überspringen.

    In der CI liefert der Service-Container den Broker. Lokal:
    ``docker run -p 9092:9092 apache/kafka:latest``
    """
    if not _broker_reachable("localhost", 9092):
        pytest.skip("Kein Kafka-Broker auf localhost:9092")
    return Settings(
        _env_file=None,
        webhook_secret=TEST_SECRET,
        kafka_topic=f"test.pass-events.{uuid.uuid4().hex[:8]}",
        kafka_consumer_group=f"test-group-{uuid.uuid4().hex[:8]}",
    )
