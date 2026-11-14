"""Kafka consumer source, event publisher and dead-letter sink."""
import json
import logging
import time
from collections.abc import Iterator

from kafka import KafkaConsumer, KafkaProducer
from kafka.structs import OffsetAndMetadata, TopicPartition

from tenant_fair_streaming.config import KafkaSettings
from tenant_fair_streaming.events import ConversationEvent
from tenant_fair_streaming.kafka_adapter.offset_tracker import OffsetTracker

logger = logging.getLogger(__name__)


def _encode_json(value: dict) -> bytes:
    return json.dumps(value).encode("utf-8")


def _decode_json(raw_value: bytes) -> dict:
    return json.loads(raw_value.decode("utf-8"))


def create_producer(settings: KafkaSettings) -> KafkaProducer:
    return KafkaProducer(
        bootstrap_servers=settings.bootstrap_servers,
        key_serializer=lambda key: key.encode("utf-8"),
        value_serializer=_encode_json,
        acks="all",  # wait for all in-sync replicas so no event is lost
        enable_idempotence=True,  # producer retries never create duplicate records
        linger_ms=10,
    )


class KafkaConversationEventPublisher:
    """Publishes events keyed by account, so each account's events stay in one partition, in order."""

    def __init__(self, settings: KafkaSettings) -> None:
        self._settings = settings
        self._producer = create_producer(settings)

    def publish(self, event: ConversationEvent) -> None:
        self._producer.send(self._settings.events_topic, key=event.ordering_key, value=event.to_dict())

    def close(self) -> None:
        self._producer.flush()
        self._producer.close()


class KafkaDeadLetterSink:
    def __init__(self, settings: KafkaSettings) -> None:
        self._settings = settings
        self._producer = create_producer(settings)

    def publish(self, event: ConversationEvent, reason: str) -> None:
        dead_letter = {"reason": reason, "event": event.to_dict()}
        self._producer.send(self._settings.dead_letter_topic, key=event.ordering_key, value=dead_letter)

    def close(self) -> None:
        self._producer.flush()
        self._producer.close()


class KafkaConversationEventSource:
    """Yields events and commits offsets only after the dispatcher reports them completed."""

    def __init__(
        self,
        settings: KafkaSettings,
        consumer_group: str,
        run_seconds: float,
        start_from_beginning: bool = False,
    ) -> None:
        self._settings = settings
        self._run_seconds = run_seconds
        self._offset_tracker = OffsetTracker()
        self._consumer = KafkaConsumer(
            settings.events_topic,
            bootstrap_servers=settings.bootstrap_servers,
            group_id=consumer_group,
            enable_auto_commit=False,  # commit manually, only after processing finishes
            auto_offset_reset="earliest" if start_from_beginning else "latest",
            value_deserializer=_decode_json,
        )

    def events(self) -> Iterator[ConversationEvent]:
        deadline = time.monotonic() + self._run_seconds
        while time.monotonic() < deadline:
            records_by_partition = self._consumer.poll(timeout_ms=500)
            for records in records_by_partition.values():
                for record in records:
                    event = self._decode_or_skip(record)
                    if event is not None:
                        yield event
            self.commit_completed_offsets()

    def mark_completed(self, event: ConversationEvent) -> None:
        self._offset_tracker.mark_completed(event.event_id)

    def commit_completed_offsets(self) -> None:
        offsets_to_commit = {
            TopicPartition(self._settings.events_topic, partition): OffsetAndMetadata(offset, "", -1)
            for partition, offset in self._offset_tracker.committable_offsets().items()
        }
        if offsets_to_commit:
            self._consumer.commit(offsets_to_commit)

    def close(self) -> None:
        self.commit_completed_offsets()
        self._consumer.close()

    def _decode_or_skip(self, record) -> ConversationEvent | None:
        try:
            event = ConversationEvent.from_dict(record.value)
        except (KeyError, TypeError, ValueError) as error:
            # A malformed record can never succeed, so it is logged and its offset is released.
            logger.error("skipping malformed record at partition %d offset %d: %r", record.partition, record.offset, error)
            placeholder_id = f"malformed-{record.partition}-{record.offset}"
            self._offset_tracker.track(placeholder_id, record.partition, record.offset)
            self._offset_tracker.mark_completed(placeholder_id)
            return None
        self._offset_tracker.track(event.event_id, record.partition, record.offset)
        return event
