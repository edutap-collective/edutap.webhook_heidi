"""The standalone deployment shape.

These tests run the import in a *subprocess*. The interesting property of
``standalone`` is what happens at import time — it registers the Kafka backend
before it imports the router — and that cannot be observed once the module is
already in ``sys.modules``. Importing it in-process would also leave the Kafka
backend in the global plugin registry and leak into every later test.
"""

from edutap.webhook_heidi.settings import ENV_PREFIX

import os
import pytest
import subprocess
import sys
import textwrap


pytest.importorskip(
    "aiokafka", reason="the standalone app hard-wires the Kafka backend"
)


def run_import(code: str) -> subprocess.CompletedProcess:
    """Import ``standalone`` in a fresh interpreter and run ``code`` after it."""
    script = textwrap.dedent(
        """
        from edutap.webhook_heidi import standalone
        """
    ) + textwrap.dedent(code)
    environment = dict(os.environ)
    # No default in Settings, so the import fails without it — same contract the
    # deployment has to satisfy.
    environment[f"{ENV_PREFIX}WEBHOOK_SECRET"] = "0123456789abcdef" * 4
    return subprocess.run(
        [sys.executable, "-c", script],
        capture_output=True,
        text=True,
        env=environment,
        timeout=60,
    )


def test_import_registers_the_kafka_backend():
    result = run_import(
        """
        from edutap.webhook_heidi.plugins import get_queue_backend
        from edutap.webhook_heidi.queues.kafka import KafkaQueueBackend

        assert isinstance(get_queue_backend(), KafkaQueueBackend)
        print("ok")
        """
    )
    assert result.returncode == 0, result.stderr
    assert "ok" in result.stdout


def test_backend_is_registered_before_the_router_is_imported():
    """The fail-fast in ``handlers.fastapi`` must still be intact.

    That module resolves the backend at import time so a misconfigured
    deployment dies at startup. If ``standalone`` registered the backend after
    importing the router, the resolution would fail and the module would log
    its warning — the service would still work, but only find out about a
    missing backend on the first real event. The absence of that warning is the
    only externally visible proof of the import order.
    """
    result = run_import("print('ok')")
    assert result.returncode == 0, result.stderr
    assert "Kein Queue-Backend beim Import auflösbar" not in result.stderr


def test_app_serves_the_webhook_route():
    """Asserted against the OpenAPI schema, not ``app.routes``.

    ``include_router`` no longer flattens the routes into ``app.routes``; it
    appends a single wrapper object without a ``path``. The generated schema is
    both stable across that change and the actual contract the sender sees.
    """
    result = run_import(
        """
        from edutap.webhook_heidi.settings import Settings

        prefix = Settings().handler_prefix
        paths = standalone.app.openapi()["paths"]
        assert prefix in paths, sorted(paths)
        assert "post" in paths[prefix], paths[prefix]
        print("ok")
        """
    )
    assert result.returncode == 0, result.stderr
    assert "ok" in result.stdout


def test_missing_secret_stops_the_process():
    """No secret, no start — rather than a service that rejects every signature."""
    environment = dict(os.environ)
    environment.pop(f"{ENV_PREFIX}WEBHOOK_SECRET", None)
    result = subprocess.run(
        [sys.executable, "-c", "from edutap.webhook_heidi import standalone"],
        capture_output=True,
        text=True,
        env=environment,
        timeout=60,
    )
    assert result.returncode != 0
    assert "webhook_secret" in result.stderr.lower()
