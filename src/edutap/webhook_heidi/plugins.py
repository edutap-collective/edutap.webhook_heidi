"""Backend-Auswahl per setuptools-Entry-Point — eduTAP-Hauskonvention.

Ein Consumer registriert sein Backend in der eigenen ``pyproject.toml``::

    [project.entry-points.'edutap.webhook_heidi.plugins']
    QueueBackend = 'edutap.webhook_heidi.queues.kafka:KafkaQueueBackend'
"""

from edutap.webhook_heidi.protocols import QueueBackend
from importlib.metadata import entry_points


ENTRY_POINT_GROUP = "edutap.webhook_heidi.plugins"
PLUGIN_NAME = "QueueBackend"

_registry: list[type] = []
_backend: QueueBackend | None = None


def add_plugin(klass: type) -> None:
    """Registriert ein Backend programmatisch (für Tests und Einbettung)."""
    # Hinweis: issubclass() gegen ein runtime_checkable Protocol prüft nur die Existenz
    # der Methodennamen — nicht ihre Signatur, Arity oder async-Eigenschaft. Ein Backend
    # mit synchronem enqueue() besteht diese Prüfung und scheitert erst zur Laufzeit.
    if not issubclass(klass, QueueBackend):
        raise TypeError(f"{klass!r} implementiert QueueBackend nicht.")
    _registry.append(klass)


def reset_queue_backend() -> None:
    """Leert Registry und Instanz-Cache. Für Tests."""
    global _backend
    _registry.clear()
    _backend = None


def get_queue_backend() -> QueueBackend:
    """Liefert das konfigurierte Backend — genau eines, gecacht."""
    global _backend
    if _backend is not None:
        return _backend

    candidates = [
        ep.load()
        for ep in entry_points(group=ENTRY_POINT_GROUP)
        if ep.name == PLUGIN_NAME
    ]
    candidates += _registry

    if not candidates:
        raise NotImplementedError(
            f"Kein QueueBackend registriert. Entry-Point '{PLUGIN_NAME}' in der "
            f"Gruppe '{ENTRY_POINT_GROUP}' setzen oder add_plugin() nutzen."
        )
    if len(candidates) > 1:
        raise ValueError(f"Mehrere QueueBackends gefunden: {candidates!r}")

    _backend = candidates[0]()
    return _backend
