"""Kafka-Backend.

Producer idempotent mit ``acks="all"``: wir antworten dem Sender erst dann mit
2xx, wenn der Broker den Write bestätigt hat. Consumer mit manuellem
Offset-Commit — nur so hat ``ack()`` Bedeutung.
"""

from aiokafka import AIOKafkaConsumer
from aiokafka import AIOKafkaProducer
from aiokafka import TopicPartition
from aiokafka.errors import KafkaError
from collections.abc import AsyncIterator
from edutap.webhook_heidi.models import QueueMessage
from edutap.webhook_heidi.protocols import QueueUnavailable
from edutap.webhook_heidi.settings import Settings

import asyncio
import json


class KafkaQueueBackend:
    """Pass-Queue auf Kafka."""

    def __init__(self, settings: Settings | None = None) -> None:
        self._settings = settings or Settings()
        self._producer: AIOKafkaProducer | None = None
        self._consumer: AIOKafkaConsumer | None = None
        self._records: dict[str, tuple[str, int, int]] = {}
        self.last_key: bytes | None = None

    def _auth(self) -> dict:
        password = self._settings.kafka_sasl_password
        return {
            "security_protocol": self._settings.kafka_security_protocol,
            "sasl_mechanism": self._settings.kafka_sasl_mechanism,
            "sasl_plain_username": self._settings.kafka_sasl_username,
            "sasl_plain_password": (
                password.get_secret_value() if password is not None else None
            ),
        }

    async def _get_producer(self) -> AIOKafkaProducer:
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
                raise QueueUnavailable(str(exc)) from exc
            self._producer = producer
        return self._producer

    async def _get_consumer(self) -> AIOKafkaConsumer:
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
            await consumer.start()
            self._consumer = consumer
        return self._consumer

    async def enqueue(self, message: QueueMessage) -> None:
        producer = await self._get_producer()
        key = message.passid.encode()
        try:
            await asyncio.wait_for(
                producer.send_and_wait(
                    self._settings.kafka_topic,
                    value=message.model_dump(mode="json"),
                    key=key,
                ),
                timeout=self._settings.enqueue_timeout,
            )
        except (KafkaError, asyncio.TimeoutError) as exc:
            raise QueueUnavailable(str(exc)) from exc
        self.last_key = key

    async def consume(self) -> AsyncIterator[QueueMessage]:
        consumer = await self._get_consumer()
        async for record in consumer:
            message = QueueMessage.model_validate(record.value)
            self._records[message.eventid] = (
                record.topic,
                record.partition,
                record.offset,
            )
            self.last_key = record.key
            yield message

    async def ack(self, message: QueueMessage) -> None:
        record = self._records.pop(message.eventid, None)
        if record is None or self._consumer is None:
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
