from edutap.webhook_heidi.plugins import add_plugin
from edutap.webhook_heidi.plugins import get_queue_backend
from edutap.webhook_heidi.plugins import reset_queue_backend
from edutap.webhook_heidi.protocols import QueueBackend
from edutap.webhook_heidi.queues.memory import InMemoryQueueBackend
from plugins import EntryPointQueueBackend

import pytest


def test_no_backend_registered():
    reset_queue_backend()
    with pytest.raises(NotImplementedError):
        get_queue_backend()


def test_registered_backend_is_returned():
    reset_queue_backend()
    add_plugin(InMemoryQueueBackend)
    assert isinstance(get_queue_backend(), InMemoryQueueBackend)


def test_backend_instance_is_cached():
    """Ein Kafka-Producer darf nicht pro Request neu aufgebaut werden."""
    reset_queue_backend()
    add_plugin(InMemoryQueueBackend)
    assert get_queue_backend() is get_queue_backend()


def test_two_backends_are_rejected():
    reset_queue_backend()
    add_plugin(InMemoryQueueBackend)
    add_plugin(InMemoryQueueBackend)
    with pytest.raises(ValueError):
        get_queue_backend()


def test_non_conforming_class_is_rejected():
    class NotABackend:
        pass

    reset_queue_backend()
    with pytest.raises(TypeError):
        add_plugin(NotABackend)


def test_memory_backend_conforms_to_protocol():
    assert issubclass(InMemoryQueueBackend, QueueBackend)


def test_backend_loaded_via_real_entry_point_mechanism(entrypoints_testing):
    """IMPORTANT 1 (Abschluss-Review): der einzige dokumentierte
    Integrationsweg für Consumer ist der Entry-Point (README/Spec §3.2), aber
    bis zu diesem Test hing keine einzige Suite jemals über
    ``importlib.metadata.entry_points()`` ein Backend ein -- alle nutzten
    ``add_plugin()``. Ein Tippfehler oder Rename an
    ``plugins.ENTRY_POINT_GROUP``/``plugins.PLUGIN_NAME`` ging dadurch grün
    durch die CI und hätte in Produktion jedes Event mit 503 abgewiesen.

    Bewusst OHNE ``add_plugin()``: die ``entrypoints_testing``-Fixture
    (``tests/conftest.py``) hängt das Backend ausschließlich über den echten,
    von Consumern genutzten Entry-Point-Mechanismus ein. Muss rot werden,
    wenn ``ENTRY_POINT_GROUP`` oder ``PLUGIN_NAME`` in ``plugins.py``
    verändert werden -- verifiziert im Abschluss-Review durch testweises
    Verdrehen beider Konstanten.
    """
    reset_queue_backend()
    try:
        backend = get_queue_backend()
        assert isinstance(backend, EntryPointQueueBackend)
    finally:
        reset_queue_backend()
