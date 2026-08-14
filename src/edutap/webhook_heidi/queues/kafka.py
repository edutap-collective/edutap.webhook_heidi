"""Kafka-Backend.

Producer idempotent mit ``acks="all"``: wir antworten dem Sender erst dann mit
2xx, wenn der Broker den Write bestätigt hat. Consumer mit manuellem
Offset-Commit — nur so hat ``ack()`` Bedeutung.
"""

from aiokafka import AIOKafkaConsumer
from aiokafka import AIOKafkaProducer
from aiokafka import TopicPartition
from aiokafka.errors import KafkaError
from aiokafka.helpers import create_ssl_context
from collections.abc import AsyncIterator
from edutap.webhook_heidi.models import QueueMessage
from edutap.webhook_heidi.protocols import QueueUnavailable
from edutap.webhook_heidi.settings import Settings

import asyncio
import json
import ssl
import structlog


logger = structlog.get_logger(__name__)


class KafkaQueueBackend:
    """Pass-Queue auf Kafka."""

    def __init__(self, settings: Settings | None = None) -> None:
        self._settings = settings or Settings()
        self._producer: AIOKafkaProducer | None = None
        self._consumer: AIOKafkaConsumer | None = None
        self._records: dict[int, tuple[str, int, int]] = {}
        self.last_key: bytes | None = None
        # Einmal gebaut, von Producer UND Consumer benutzt — siehe
        # _ssl_context().
        self._ssl_context_cache: ssl.SSLContext | None = None
        # Schützt gegen die Kaltstart-Race: mehrere gleichzeitige enqueue()/
        # consume()-Aufrufe dürfen nicht je einen eigenen (verwaisten)
        # Producer/Consumer erzeugen, siehe _get_producer()/_get_consumer().
        self._producer_lock = asyncio.Lock()
        self._consumer_lock = asyncio.Lock()

    def _ssl_context(self) -> ssl.SSLContext | None:
        """Der TLS-Kontext, oder ``None`` bei einem Klartext-Broker.

        Gebaut wird er nur, wenn das Protokoll TLS verlangt. Ein Kontext bei
        PLAINTEXT wäre nicht bloß nutzlos: aiokafka nähme ihn und der Handshake
        gegen einen Klartext-Listener schlüge fehl.

        Einmal gebaut, dann gemerkt — er liest PEM-Dateien von der Platte, und
        Producer und Consumer sollen sich denselben teilen.
        """
        if "SSL" not in self._settings.kafka_security_protocol:
            return None
        if self._ssl_context_cache is None:
            # aiokafkas eigener Helfer: er setzt verify_mode und check_hostname
            # so, wie die Bibliothek sie erwartet. Ein handgebauter Kontext
            # weicht erfahrungsgemäß genau dort ab, wo es später weh tut.
            self._ssl_context_cache = create_ssl_context(
                cafile=self._settings.kafka_ssl_cafile,
                certfile=self._settings.kafka_ssl_certfile,
                keyfile=self._settings.kafka_ssl_keyfile,
            )
        return self._ssl_context_cache

    def _auth(self) -> dict:
        password = self._settings.kafka_sasl_password
        return {
            "security_protocol": self._settings.kafka_security_protocol,
            "sasl_mechanism": self._settings.kafka_sasl_mechanism,
            "sasl_plain_username": self._settings.kafka_sasl_username,
            "sasl_plain_password": (
                password.get_secret_value() if password is not None else None
            ),
            "ssl_context": self._ssl_context(),
        }

    async def _get_producer(self) -> AIOKafkaProducer:
        # Double-checked locking: der erste (ungelockte) Check erspart das
        # Lock im Steady-State (Producer schon da). Ohne das Lock gibt das
        # `await producer.start()` weiter unten die Kontrolle ab -- mehrere
        # gleichzeitige Aufrufer auf kaltem Backend laufen sonst alle durch
        # den `is None`-Check, bevor irgendeiner zuweist, und erzeugen je
        # einen eigenen, nie gestoppten Producer samt Broker-Verbindung.
        if self._producer is None:
            async with self._producer_lock:
                if self._producer is None:
                    producer = AIOKafkaProducer(
                        bootstrap_servers=self._settings.kafka_bootstrap_servers,
                        value_serializer=lambda value: json.dumps(value).encode(),
                        # Broker dedupliziert producer-seitige Retries; impliziert acks="all".
                        enable_idempotence=True,
                        acks="all",
                        **self._auth(),
                    )
                    try:
                        await producer.start()
                    except KafkaError as exc:
                        # aiokafka räumt bei einem Verbindungsfehler (Broker
                        # weg, Connection refused, ...) selbst auf -- der
                        # `stop()` hier ist defensiv, nicht der Kernfix.
                        await producer.stop()
                        raise QueueUnavailable(f"{type(exc).__name__}: {exc}") from exc
                    except BaseException:
                        # Auch (und vor allem) asyncio.CancelledError: der
                        # Producer-Start läuft innerhalb von enqueue()s
                        # asyncio.wait_for(enqueue_timeout) -- das kann uns
                        # HIER, mitten in producer.start(), abbrechen.
                        # CancelledError erbt seit Python 3.8 von
                        # BaseException, nicht von Exception; ein
                        # "except Exception" würde es durchlassen. Ohne den
                        # Stop hier bliebe ein halb gestarteter Producer mit
                        # offenem Socket zurück, den niemand mehr erreicht:
                        # self._producer wird erst NACH start() zugewiesen,
                        # also bleibt es None und stop() (auch
                        # KafkaQueueBackend.stop()) greift ins Leere.
                        # Gemessener Leak ohne diesen Fix: 1 offener Socket +
                        # 2 Tasks pro abgebrochenem enqueue(), linear
                        # wachsend, nie freigegeben.
                        await producer.stop()
                        raise
                    self._producer = producer
        return self._producer

    async def _get_consumer(self) -> AIOKafkaConsumer:
        # Gleiche Race wie in _get_producer(), gleicher Fix.
        if self._consumer is None:
            async with self._consumer_lock:
                if self._consumer is None:
                    consumer = AIOKafkaConsumer(
                        self._settings.kafka_topic,
                        bootstrap_servers=self._settings.kafka_bootstrap_servers,
                        group_id=self._settings.kafka_consumer_group,
                        value_deserializer=lambda value: json.loads(value),
                        enable_auto_commit=False,
                        auto_offset_reset="earliest",
                        **self._auth(),
                    )
                    try:
                        await consumer.start()
                    except BaseException:
                        # Gleiches Muster/gleicher Grund wie in
                        # _get_producer(): consume() kann von außen (z.B.
                        # beim Herunterfahren des Spooler-Tasks) mitten in
                        # consumer.start() abgebrochen werden
                        # (CancelledError). Ohne den Stop hier bliebe ein
                        # halb gestarteter Consumer mit offenem Socket
                        # zurück, den stop() nie erreicht (self._consumer
                        # bleibt None).
                        await consumer.stop()
                        raise
                    self._consumer = consumer
        return self._consumer

    async def _enqueue_unbounded(self, message: QueueMessage, key: bytes) -> None:
        # Producer-Start MIT im wait_for-Zeitfenster von enqueue(): sonst
        # hängt ein Worker gegen einen Listener, der TCP annimmt, aber nicht
        # antwortet, viel länger als enqueue_timeout (gemessen: 40s statt
        # der konfigurierten 2s) -- der Sender-Timeout (30s) wäre da längst
        # abgelaufen.
        producer = await self._get_producer()
        await producer.send_and_wait(
            self._settings.kafka_topic,
            value=message.model_dump(mode="json"),
            key=key,
        )

    async def enqueue(self, message: QueueMessage) -> None:
        key = message.passid.encode()
        timeout = self._settings.enqueue_timeout
        try:
            await asyncio.wait_for(
                self._enqueue_unbounded(message, key), timeout=timeout
            )
        except asyncio.TimeoutError as exc:
            # str(asyncio.TimeoutError()) ist leer -- QueueUnavailable(str(exc))
            # wäre dann eine leere, nichtssagende Fehlermeldung.
            raise QueueUnavailable(f"Enqueue-Timeout nach {timeout}s") from exc
        except KafkaError as exc:
            raise QueueUnavailable(f"{type(exc).__name__}: {exc}") from exc
        self.last_key = key

    async def consume(self) -> AsyncIterator[QueueMessage]:
        consumer = await self._get_consumer()
        async for record in consumer:
            message = QueueMessage.model_validate(record.value)
            # Bewusst NICHT nach eventid gekeyt: Duplikate sind by design
            # erwartet (heidi.cloud liefert at-least-once, deshalb
            # dedupliziert der Consumer und nicht wir). Zwei Nachrichten mit
            # gleicher eventid an unterschiedlichen Offsets dürfen sich hier
            # nicht überschreiben, sonst committet ack() der ersten Kopie den
            # Offset der zweiten mit -> Nachrichten dazwischen gelten
            # fälschlich als verarbeitet und werden nie wieder ausgeliefert.
            # id(message) ist eindeutig, solange der Aufrufer die Nachricht
            # bis zum ack() referenziert (siehe ack()-Docstring).
            self._records[id(message)] = (
                record.topic,
                record.partition,
                record.offset,
            )
            self.last_key = record.key
            yield message

    async def ack(self, message: QueueMessage) -> None:
        """Bestätigt genau DIESES Nachrichtenobjekt (Identität, nicht ``eventid``).

        Kafka-Offset-Commits sind kumulativ: ``commit()`` setzt den
        Consumer-Offset für die Partition auf einen Wert, es gibt kein
        "commit nur diese eine Nachricht". Deshalb MUSS der Consumer
        sequenziell arbeiten — konsumieren, verarbeiten, acken, danach erst
        die nächste Nachricht holen. Wer mehrere Nachrichten aus
        ``consume()`` stapelt und sie außer der Reihe (oder gar nicht alle)
        ackt, committet dabei stillschweigend auch die übersprungenen
        Offsets mit und verliert die dazwischenliegenden Nachrichten für
        immer.
        """
        record = self._records.pop(id(message), None)
        if record is None:
            # Nicht dasselbe Objekt, das consume() geliefert hat -- z.B. weil
            # der Aufrufer die Nachricht durch eine eigene Queue geschickt
            # hat (QueueMessage.model_validate(m.model_dump())). Ohne dieses
            # Log wäre das eine Redelivery-Endlosschleife ohne jeden
            # Hinweis: kein Commit, keine Exception.
            logger.warning(
                "ack() found no record, nothing committed",
                eventid=message.eventid,
                hint=(
                    "ack() needs the very object consume() returned; a copy "
                    "acks into the void and redelivers forever"
                ),
            )
            return
        if self._consumer is None:
            return
        topic, partition, offset = record
        await self._consumer.commit({TopicPartition(topic, partition): offset + 1})

    async def stop(self) -> None:
        if self._producer is not None:
            await self._producer.stop()
            self._producer = None
        if self._consumer is not None:
            await self._consumer.stop()
            self._consumer = None
