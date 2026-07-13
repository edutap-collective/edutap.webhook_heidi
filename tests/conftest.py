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

import os
import pytest


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
