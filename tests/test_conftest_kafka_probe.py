"""Tests für den Kafka-Broker-Probe in ``conftest.py``.

Hintergrund (MINOR 6): Der CI-Service-Container hat keinen Healthcheck, und
der bisherige Probe war ein einmaliger 2,5-s-Versuch ohne Retry. Da
``EDUTAP_WEBHOOK_HEIDI_TEST_REQUIRE_KAFKA=1`` in der CI gesetzt ist, wurde
ein Broker, der beim ersten Testlauf noch hochfährt, zum harten,
flakigen CI-Fail. ``_wait_for_kafka_broker`` pollt deshalb mit Retries,
bevor aufgegeben wird -- aber NUR, wenn ein Broker verpflichtend ist (CI);
lokal ohne laufenden Broker bleibt der schnelle Einzelversuch, damit ein
Entwickler ohne Kafka nicht 30s auf den Skip wartet.
"""

import asyncio
import conftest
import pytest


pytestmark = pytest.mark.asyncio


async def test_wait_for_kafka_broker_retries_until_available(monkeypatch):
    """Muss so lange pollen, bis der Probe True liefert -- simuliert einen
    Broker, der erst nach zwei Fehlversuchen bereit ist."""
    attempts = []

    async def _fake_probe(bootstrap_servers, timeout=2.5):
        attempts.append(bootstrap_servers)
        return len(attempts) >= 3

    monkeypatch.setattr(conftest, "_kafka_broker_available", _fake_probe)

    result = await conftest._wait_for_kafka_broker(
        "localhost:19092", retry_seconds=10.0, interval=0.01
    )

    assert result is True
    assert len(attempts) == 3


async def test_wait_for_kafka_broker_gives_up_after_deadline(monkeypatch):
    """Bleibt der Broker durchgehend nicht erreichbar, gibt der Retry nach
    ``retry_seconds`` False zurück, statt endlos zu pollen."""

    async def _always_unavailable(bootstrap_servers, timeout=2.5):
        return False

    monkeypatch.setattr(conftest, "_kafka_broker_available", _always_unavailable)

    loop = asyncio.get_event_loop()
    start = loop.time()
    result = await conftest._wait_for_kafka_broker(
        "localhost:19092", retry_seconds=0.05, interval=0.01
    )
    elapsed = loop.time() - start

    assert result is False
    assert elapsed < 1.0
