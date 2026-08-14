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
from structlog.testing import capture_logs
from typing import ClassVar

import asyncio
import pytest
import ssl


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


class _SlowStartProducer(_FakeProducer):
    """Simuliert einen Listener, der TCP annimmt, aber nicht antwortet:
    ``start()`` hängt lange. ``send_and_wait`` selbst wäre danach schnell."""

    async def start(self) -> None:
        await asyncio.sleep(2)
        self.started = True


class _LeakTrackingSlowStartProducer(_FakeProducer):
    """Wie ``_SlowStartProducer``, zählt aber offene "Sockets" (open_count)
    und Instanzen, damit ein Test nachweisen kann, dass ein wegen
    ``enqueue_timeout`` abgebrochener Producer-Start sein Socket wieder
    schließt, statt es zu leaken. ``open_count`` steigt beim (simulierten)
    TCP-Connect zu Beginn von ``start()`` -- wie ein echter Listener, der
    TCP annimmt, aber nie antwortet -- und sinkt nur, wenn ``stop()``
    tatsächlich aufgerufen wird."""

    open_count: ClassVar[int] = 0
    instances: ClassVar[list["_LeakTrackingSlowStartProducer"]] = []

    def __init__(self, *args, **kwargs) -> None:
        super().__init__(*args, **kwargs)
        type(self).instances.append(self)

    async def start(self) -> None:
        type(self).open_count += 1
        await asyncio.sleep(10)
        self.started = True

    async def stop(self) -> None:
        if not self.stopped:
            type(self).open_count -= 1
        self.stopped = True


class _FailingProducer(_FakeProducer):
    async def start(self) -> None:
        from aiokafka.errors import KafkaConnectionError

        raise KafkaConnectionError("boom")


class _FailingSendProducer(_FakeProducer):
    """Producer startet erfolgreich, aber send_and_wait scheitert direkt
    (z.B. Broker während des Sends weggebrochen) -- anders als der
    Timeout-Fall, der über asyncio.TimeoutError läuft."""

    async def _send_and_wait(self, topic, value, key):
        from aiokafka.errors import KafkaError

        raise KafkaError("send failed")


class _CountingProducer(_FakeProducer):
    """Zählt Instanzen und gibt in ``start()`` bewusst die Kontrolle ab
    (wie ein echter Netzwerk-Connect), um die Race in ``_get_producer()``
    zu provozieren."""

    instances: ClassVar[list["_CountingProducer"]] = []

    def __init__(self, *args, **kwargs) -> None:
        super().__init__(*args, **kwargs)
        type(self).instances.append(self)

    async def start(self) -> None:
        await asyncio.sleep(0)
        self.started = True


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


class _CountingConsumer(_FakeConsumer):
    """Zählt Instanzen und gibt in ``start()`` bewusst die Kontrolle ab,
    um die Race in ``_get_consumer()`` zu provozieren."""

    instances: ClassVar[list["_CountingConsumer"]] = []

    def __init__(self, *args, **kwargs) -> None:
        super().__init__(*args, **kwargs)
        type(self).instances.append(self)

    async def start(self) -> None:
        await asyncio.sleep(0)
        self.started = True


class _SlowStartTrackingConsumer(_FakeConsumer):
    """Wie ``_CountingConsumer``, aber ``start()`` hängt lange -- zum
    Testen, dass ein von außen abgebrochener Consumer-Start aufgeräumt
    wird (analog zum Producer-Leak-Fix in ``_get_producer()``)."""

    instances: ClassVar[list["_SlowStartTrackingConsumer"]] = []

    def __init__(self, *args, **kwargs) -> None:
        super().__init__(*args, **kwargs)
        type(self).instances.append(self)

    async def start(self) -> None:
        await asyncio.sleep(2)
        self.started = True


def _settings() -> Settings:
    return Settings(_env_file=None, webhook_secret="s3cret")


async def test_get_producer_starts_and_caches(monkeypatch):
    monkeypatch.setattr(kafka_module, "AIOKafkaProducer", _FakeProducer)
    backend = KafkaQueueBackend(_settings())

    producer = await backend._get_producer()

    assert producer.started is True
    assert await backend._get_producer() is producer


async def test_get_producer_is_idempotent_with_acks_all(monkeypatch):
    """Die Kernzusage des Backends: ohne acks="all" + Idempotenz kann ein
    bestätigter Write verlorengehen -- der Endpoint antwortet danach trotzdem
    2xx, heidi.cloud wiederholt dann NICHT, das Event wäre für immer weg.
    Muss rot werden, wenn diese Werte in kafka.py verändert werden."""
    monkeypatch.setattr(kafka_module, "AIOKafkaProducer", _FakeProducer)
    backend = KafkaQueueBackend(_settings())

    producer = await backend._get_producer()

    assert producer.kwargs["enable_idempotence"] is True
    assert producer.kwargs["acks"] == "all"


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


async def test_get_consumer_uses_manual_commit_and_group_id(monkeypatch):
    """Ohne enable_auto_commit=False hat ack() keine Bedeutung mehr -- der
    Broker würde Offsets selbständig fortschreiben, auch für Nachrichten,
    die nie verarbeitet wurden. Muss rot werden, wenn diese Werte in
    kafka.py verändert werden."""
    monkeypatch.setattr(kafka_module, "AIOKafkaConsumer", _FakeConsumer)
    settings = _settings()
    backend = KafkaQueueBackend(settings)

    consumer = await backend._get_consumer()

    assert consumer.kwargs["enable_auto_commit"] is False
    assert consumer.kwargs["auto_offset_reset"] == "earliest"
    assert consumer.kwargs["group_id"] == settings.kafka_consumer_group


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


async def test_enqueue_timeout_message_is_not_empty(monkeypatch):
    """str(asyncio.TimeoutError()) == "" -- QueueUnavailable(str(exc)) wäre
    dann eine leere Fehlermeldung. Muss eine sprechende Meldung tragen."""
    monkeypatch.setattr(kafka_module, "AIOKafkaProducer", _HangingProducer)
    settings = _settings()
    settings.enqueue_timeout = 0.05
    backend = KafkaQueueBackend(settings)

    with pytest.raises(QueueUnavailable) as exc_info:
        await backend.enqueue(_message())

    assert str(exc_info.value) != ""
    assert "0.05" in str(exc_info.value)


async def test_enqueue_timeout_covers_producer_start(monkeypatch):
    """enqueue_timeout muss die GESAMTE Operation (Producer-Start + Send)
    umschließen, nicht nur send_and_wait -- sonst hängt ein Worker gegen
    einen Listener, der TCP annimmt, aber nicht antwortet, viel länger als
    der Sender selbst wartet (gemessen: 40s statt der konfigurierten 2s)."""
    monkeypatch.setattr(kafka_module, "AIOKafkaProducer", _SlowStartProducer)
    settings = _settings()
    settings.enqueue_timeout = 0.05
    backend = KafkaQueueBackend(settings)

    loop = asyncio.get_event_loop()
    start = loop.time()
    with pytest.raises(QueueUnavailable):
        await backend.enqueue(_message())
    elapsed = loop.time() - start

    assert elapsed < 1.0, f"enqueue() dauerte {elapsed}s, enqueue_timeout=0.05s"


async def test_repeated_enqueue_timeout_during_producer_start_does_not_leak(
    monkeypatch,
):
    """Regression: seit enqueue_timeout auch den Producer-Start umschließt
    (test_enqueue_timeout_covers_producer_start), bricht asyncio.wait_for
    den _get_producer()-Aufruf per CancelledError MITTEN in
    ``await producer.start()`` ab. Die lokale ``producer``-Variable geht
    dabei verloren, ``self._producer`` bleibt None -- der halb gestartete
    Producer (offener Socket) ist damit für niemanden mehr erreichbar,
    auch nicht für ``backend.stop()``.

    Gemessen gegen einen hängenden Broker, 5x enqueue() im Timeout:
    nach enqueue Nr. N ohne Fix sind N Sockets offen und self._producer
    bleibt None; nach backend.stop() bleiben alle N offen. Muss OHNE den
    ``except BaseException: await producer.stop(); raise``-Fix in
    ``_get_producer()`` rot sein (open_count wächst linear)."""
    _LeakTrackingSlowStartProducer.open_count = 0
    _LeakTrackingSlowStartProducer.instances = []
    monkeypatch.setattr(
        kafka_module, "AIOKafkaProducer", _LeakTrackingSlowStartProducer
    )
    settings = _settings()
    settings.enqueue_timeout = 0.05
    backend = KafkaQueueBackend(settings)

    for i in range(5):
        with pytest.raises(QueueUnavailable):
            await backend.enqueue(_message(eventid=f"evt_{i}"))
        assert _LeakTrackingSlowStartProducer.open_count == 0, (
            f"nach enqueue #{i + 1}: "
            f"{_LeakTrackingSlowStartProducer.open_count} Sockets offen "
            f"({len(_LeakTrackingSlowStartProducer.instances)} Producer "
            "erzeugt), self._producer sollte None sein"
        )
        assert backend._producer is None

    await backend.stop()
    assert _LeakTrackingSlowStartProducer.open_count == 0
    assert len(_LeakTrackingSlowStartProducer.instances) == 5


async def test_enqueue_kafka_error_from_send_raises_queue_unavailable(monkeypatch):
    """send_and_wait kann auch direkt (nicht nur per Timeout) scheitern, z.B.
    wenn der Broker während des Sends wegbricht."""
    monkeypatch.setattr(kafka_module, "AIOKafkaProducer", _FailingSendProducer)
    backend = KafkaQueueBackend(_settings())

    with pytest.raises(QueueUnavailable) as exc_info:
        await backend.enqueue(_message())

    assert str(exc_info.value) != ""


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


async def test_ack_unknown_message_warns_and_does_not_commit():
    """Die id(message)-Identitätsregel (siehe ack()-Docstring in
    protocols.py/kafka.py) bricht STILL, wenn ein Consumer die Nachricht vor
    dem ack() kopiert/neu erzeugt hat (z.B.
    QueueMessage.model_validate(m.model_dump())): kein Commit, keine
    Exception -- eine Redelivery-Endlosschleife ohne jeden Hinweis. ack()
    muss diesen Fall deshalb mindestens loggen."""
    backend = KafkaQueueBackend(_settings())
    # Kein consume() vorher: _records ist leer, kein Consumer gestartet.
    with capture_logs() as logs:
        await backend.ack(_message(eventid="never-seen"))

    assert any(
        entry.get("eventid") == "never-seen" and "ack()" in entry["event"]
        for entry in logs
    )


async def test_ack_after_stop_is_a_noop_and_does_not_warn(monkeypatch, caplog):
    """Ein Shutdown (``stop()``) während eine Nachricht noch in Verarbeitung
    ist, darf ``ack()`` nicht crashen lassen: ``self._consumer`` ist dann
    None, obwohl der Record noch bekannt ist (kein Kopie-Fall, also auch
    keine Warnung)."""
    monkeypatch.setattr(kafka_module, "AIOKafkaConsumer", _FakeConsumer)
    backend = KafkaQueueBackend(_settings())
    message = _message(eventid="evt_1", passid="pass-a")
    record = _FakeRecord(
        topic=backend._settings.kafka_topic,
        partition=0,
        offset=5,
        value=message.model_dump(mode="json"),
        key=b"pass-a",
    )
    consumer = await backend._get_consumer()
    consumer.records = [record]

    seen = []
    async for received in backend.consume():
        seen.append(received)
        break
    backend._consumer = None  # simuliert stop() waehrend der Verarbeitung

    with caplog.at_level("WARNING", logger="edutap.webhook_heidi.queues.kafka"):
        await backend.ack(seen[0])

    assert consumer.commits == []
    assert caplog.records == []


async def test_ack_commits_only_up_to_its_own_offset_with_duplicate_eventids(
    monkeypatch,
):
    """Duplikate SIND by design erwartet (heidi.cloud liefert at-least-once;
    deshalb dedupliziert der Consumer, nicht wir). Werden mehrere Nachrichten
    konsumiert, bevor geackt wird, darf ``ack()`` der ersten Nachricht nicht
    versehentlich bis zum Offset einer späteren Kopie mit gleicher
    ``eventid`` committen — sonst gelten dazwischenliegende Nachrichten
    fälschlich als verarbeitet und werden nie wieder ausgeliefert.

    Reproduziert das am echten Broker beobachtete Szenario: Offset 0 und 3
    tragen dieselbe ``eventid`` ("evt_DUP"), dazwischen liegen evt_B/evt_C.
    """
    monkeypatch.setattr(kafka_module, "AIOKafkaConsumer", _FakeConsumer)
    backend = KafkaQueueBackend(_settings())
    topic = backend._settings.kafka_topic
    dup_value = _message(eventid="evt_DUP", passid="pass-a").model_dump(mode="json")
    records = [
        _FakeRecord(topic, 0, 0, dup_value, b"pass-a"),
        _FakeRecord(
            topic, 0, 1, _message(eventid="evt_B").model_dump(mode="json"), b"p1"
        ),
        _FakeRecord(
            topic, 0, 2, _message(eventid="evt_C").model_dump(mode="json"), b"p1"
        ),
        _FakeRecord(topic, 0, 3, dup_value, b"pass-a"),
    ]
    consumer = await backend._get_consumer()
    consumer.records = records

    seen = []
    async for received in backend.consume():
        seen.append(received)
        if len(seen) == 4:
            break

    await backend.ack(seen[0])

    assert consumer.commits == [{(topic, 0): 1}]


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


async def test_concurrent_enqueue_on_cold_backend_creates_one_producer(monkeypatch):
    """5 gleichzeitige enqueue() auf kaltem Backend dürfen nur EINEN Producer
    erzeugen. ``_get_producer()`` prüft ``if self._producer is None``, dann
    ``await producer.start()`` (gibt die Kontrolle ab), dann erst die
    Zuweisung — ohne Lock laufen mehrere Tasks durch die Prüfung, bevor
    irgendeiner zuweist, und erzeugen je einen eigenen (verwaisten)
    Producer."""
    _CountingProducer.instances = []
    monkeypatch.setattr(kafka_module, "AIOKafkaProducer", _CountingProducer)
    backend = KafkaQueueBackend(_settings())

    await asyncio.gather(
        *(backend.enqueue(_message(eventid=f"evt_{i}")) for i in range(5))
    )

    assert len(_CountingProducer.instances) == 1


async def test_concurrent_get_consumer_on_cold_backend_creates_one_consumer(
    monkeypatch,
):
    """Dieselbe Race wie bei enqueue(), gespiegelt für _get_consumer()."""
    _CountingConsumer.instances = []
    monkeypatch.setattr(kafka_module, "AIOKafkaConsumer", _CountingConsumer)
    backend = KafkaQueueBackend(_settings())

    await asyncio.gather(*(backend._get_consumer() for _ in range(5)))

    assert len(_CountingConsumer.instances) == 1


async def test_get_consumer_stops_half_started_consumer_on_cancel(monkeypatch):
    """Gleiches Muster/gleicher Fix wie beim Producer-Leak: ``consume()``
    kann von außen (z.B. beim Herunterfahren des Spooler-Tasks) mitten in
    ``consumer.start()`` abgebrochen werden. Ohne den
    ``except BaseException: await consumer.stop(); raise``-Fix in
    ``_get_consumer()`` bliebe der halb gestartete Consumer mit offenem
    Socket zurück, self._consumer bleibt None."""
    _SlowStartTrackingConsumer.instances = []
    monkeypatch.setattr(kafka_module, "AIOKafkaConsumer", _SlowStartTrackingConsumer)
    backend = KafkaQueueBackend(_settings())

    with pytest.raises(asyncio.TimeoutError):
        await asyncio.wait_for(backend._get_consumer(), timeout=0.05)

    assert backend._consumer is None
    assert len(_SlowStartTrackingConsumer.instances) == 1
    assert _SlowStartTrackingConsumer.instances[0].stopped is True


async def test_plaintext_passes_no_ssl_context(monkeypatch):
    """Der Default darf keinen TLS-Kontext erfinden: ein PLAINTEXT-Broker
    lehnt einen TLS-Handshake ab."""
    monkeypatch.setattr(kafka_module, "AIOKafkaProducer", _FakeProducer)
    backend = KafkaQueueBackend(_settings())

    producer = await backend._get_producer()

    assert producer.kwargs["security_protocol"] == "PLAINTEXT"
    assert producer.kwargs["ssl_context"] is None


async def test_ssl_producer_gets_context_with_client_material(
    monkeypatch, tmp_path, ssl_material
):
    """Mit SSL muss ein echter ssl.SSLContext an aiokafka gehen.

    Ohne ihn baut aiokafka gegen einen Broker mit
    ``ssl.client.auth=required`` keine Verbindung auf -- und weil der
    Producer erst beim ersten Enqueue verbindet, faellt das nicht beim
    Deploy auf, sondern beim ersten echten Pass-Event.
    """
    monkeypatch.setattr(kafka_module, "AIOKafkaProducer", _FakeProducer)
    settings = Settings(
        _env_file=None,
        webhook_secret="s3cret",
        kafka_security_protocol="SSL",
        kafka_ssl_cafile=str(ssl_material["cafile"]),
        kafka_ssl_certfile=str(ssl_material["certfile"]),
        kafka_ssl_keyfile=str(ssl_material["keyfile"]),
    )
    backend = KafkaQueueBackend(settings)

    producer = await backend._get_producer()

    context = producer.kwargs["ssl_context"]
    assert isinstance(context, ssl.SSLContext)
    assert context.verify_mode is ssl.CERT_REQUIRED
    # Das Client-Material ist wirklich geladen, nicht nur der Pfad gemerkt:
    # ein Kontext ohne Kette scheitert am Broker, der Client-Auth verlangt.
    assert context.get_ca_certs(), "Truststore leer -- cafile nicht geladen"


async def test_ssl_context_is_built_once(monkeypatch, ssl_material):
    """Der Kontext liest Dateien von der Platte -- pro Enqueue neu waere
    Arbeit fuer nichts."""
    monkeypatch.setattr(kafka_module, "AIOKafkaProducer", _FakeProducer)
    monkeypatch.setattr(kafka_module, "AIOKafkaConsumer", _FakeConsumer)
    settings = Settings(
        _env_file=None,
        webhook_secret="s3cret",
        kafka_security_protocol="SSL",
        kafka_ssl_cafile=str(ssl_material["cafile"]),
        kafka_ssl_certfile=str(ssl_material["certfile"]),
        kafka_ssl_keyfile=str(ssl_material["keyfile"]),
    )
    backend = KafkaQueueBackend(settings)

    producer = await backend._get_producer()
    consumer = await backend._get_consumer()

    assert producer.kwargs["ssl_context"] is consumer.kwargs["ssl_context"]
