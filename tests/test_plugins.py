from edutap.webhook_heidi.plugins import add_plugin
from edutap.webhook_heidi.plugins import get_queue_backend
from edutap.webhook_heidi.plugins import reset_queue_backend
from edutap.webhook_heidi.protocols import QueueBackend
from edutap.webhook_heidi.queues.memory import InMemoryQueueBackend

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
