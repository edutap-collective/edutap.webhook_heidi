"""Unit-Tests für ``KafkaQueueBackend`` — ohne echten Broker, per Fake-Klassen.

Bewusst getrennt von ``test_queues_kafka.py`` (``@pytest.mark.kafka``, braucht
einen echten Broker): Diese Tests hier laufen IMMER, auch lokal ohne Kafka.
Grund: Coverage darf nicht davon abhängen, ob gerade ein Broker erreichbar
ist — ``queues/kafka.py`` ist das einzige produktive Backend und muss auch
dann gemessen sein, wenn niemand einen Broker laufen hat. Die
``@pytest.mark.kafka``-Tests bleiben zusätzlich bestehen: sie verifizieren das
echte Protokollverhalten (Serialisierung, Partitionierung, Offsets) gegen
einen echten Broker, was Fakes hier bewusst nicht leisten.
"""

from datetime import datetime
from datetime import timezone
from edutap.webhook_heidi.models import QueueMessage
from edutap.webhook_heidi.protocols import QueueUnavailable
from edutap.webhook_heidi.queues import kafka as kafka_module
from edutap.webhook_heidi.queues.kafka import KafkaQueueBackend
from edutap.webhook_heidi.settings import Settings

import asyncio
import pytest


def _message(eventid: str = "evt_1", passid: str = "p1") -> QueueMessage:
    return QueueMessage(
        eventid=eventid,
        passid=passid,
        personid="x",
        action="pass.installed",
        timestamp=datetime(2026, 7, 9, 12, 34, 56, tzinfo=timezone.utc),
        payload={"state": "ACTIVE", "reason": "provisioning"},
    )


class _FakeProducer:
    def __init__(self, *args, **kwargs) -> None:
        self.args = args
        self.kwargs = kwargs
        self.started = False
        self.stopped = False
        self.sent: list[tuple[str, object, bytes]] = []
        self.send_and_wait = self._send_and_wait

    async def start(self) -> None:
        self.started = True

    async def stop(self) -> None:
        self.stopped = True

    async def _send_and_wait(self, topic, value, key):
        self.sent.append((topic, value, key))


class _HangingProducer(_FakeProducer):
    async def _send_and_wait(self, topic, value, key):
        await asyncio.sleep(10)


class _FailingProducer(_FakeProducer):
    async def start(self) -> None:
        from aiokafka.errors import KafkaConnectionError

        raise KafkaConnectionError("boom")


class _FakeRecord:
    def __init__(self, topic, partition, offset, value, key) -> None:
        self.topic = topic
        self.partition = partition
        self.offset = offset
        self.value = value
        self.key = key


class _FakeConsumer:
    def __init__(self, *topics, **kwargs) -> None:
        self.topics = topics
        self.kwargs = kwargs
        self.started = False
        self.stopped = False
        self.commits: list[dict] = []
        self.records: list[_FakeRecord] = []

    async def start(self) -> None:
        self.started = True

    async def stop(self) -> None:
        self.stopped = True

    async def commit(self, offsets: dict) -> None:
        self.commits.append(offsets)

    def __aiter__(self):
        return self._aiter()

    async def _aiter(self):
        for record in self.records:
            yield record


def _settings() -> Settings:
    return Settings(_env_file=None, webhook_secret="s3cret")


async def test_get_producer_starts_and_caches(monkeypatch):
    monkeypatch.setattr(kafka_module, "AIOKafkaProducer", _FakeProducer)
    backend = KafkaQueueBackend(_settings())

    producer = await backend._get_producer()

    assert producer.started is True
    assert await backend._get_producer() is producer


async def test_get_producer_kafka_error_raises_queue_unavailable(monkeypatch):
    monkeypatch.setattr(kafka_module, "AIOKafkaProducer", _FailingProducer)
    backend = KafkaQueueBackend(_settings())

    with pytest.raises(QueueUnavailable):
        await backend._get_producer()


async def test_get_consumer_starts_and_caches(monkeypatch):
    monkeypatch.setattr(kafka_module, "AIOKafkaConsumer", _FakeConsumer)
    backend = KafkaQueueBackend(_settings())

    consumer = await backend._get_consumer()

    assert consumer.started is True
    assert consumer.topics == (backend._settings.kafka_topic,)
    assert await backend._get_consumer() is consumer


async def test_enqueue_happy_path_sets_last_key(monkeypatch):
    monkeypatch.setattr(kafka_module, "AIOKafkaProducer", _FakeProducer)
    backend = KafkaQueueBackend(_settings())
    message = _message(passid="pass-a")

    await backend.enqueue(message)

    producer = await backend._get_producer()
    assert producer.sent == [
        (
            backend._settings.kafka_topic,
            message.model_dump(mode="json"),
            b"pass-a",
        )
    ]
    assert backend.last_key == b"pass-a"


async def test_enqueue_timeout_raises_queue_unavailable(monkeypatch):
    monkeypatch.setattr(kafka_module, "AIOKafkaProducer", _HangingProducer)
    settings = _settings()
    settings.enqueue_timeout = 0.05
    backend = KafkaQueueBackend(settings)

    with pytest.raises(QueueUnavailable):
        await backend.enqueue(_message())


async def test_consume_and_ack_roundtrip(monkeypatch):
    monkeypatch.setattr(kafka_module, "AIOKafkaConsumer", _FakeConsumer)
    backend = KafkaQueueBackend(_settings())
    message = _message(eventid="evt_1", passid="pass-a")
    record = _FakeRecord(
        topic=backend._settings.kafka_topic,
        partition=0,
        offset=41,
        value=message.model_dump(mode="json"),
        key=b"pass-a",
    )
    consumer = await backend._get_consumer()
    consumer.records = [record]

    seen = []
    async for received in backend.consume():
        seen.append(received)
        await backend.ack(received)

    assert [m.eventid for m in seen] == ["evt_1"]
    assert seen[0].payload == {"state": "ACTIVE", "reason": "provisioning"}
    assert backend.last_key == b"pass-a"
    assert consumer.commits == [{(record.topic, record.partition): 42}]


async def test_ack_unknown_eventid_is_a_noop():
    backend = KafkaQueueBackend(_settings())
    # Kein consume() vorher: _records ist leer, kein Consumer gestartet.
    await backend.ack(_message(eventid="never-seen"))


async def test_stop_stops_producer_and_consumer(monkeypatch):
    monkeypatch.setattr(kafka_module, "AIOKafkaProducer", _FakeProducer)
    monkeypatch.setattr(kafka_module, "AIOKafkaConsumer", _FakeConsumer)
    backend = KafkaQueueBackend(_settings())
    producer = await backend._get_producer()
    consumer = await backend._get_consumer()

    await backend.stop()

    assert producer.stopped is True
    assert consumer.stopped is True
    assert backend._producer is None
    assert backend._consumer is None


async def test_stop_without_producer_or_consumer_is_a_noop():
    backend = KafkaQueueBackend(_settings())
    await backend.stop()
