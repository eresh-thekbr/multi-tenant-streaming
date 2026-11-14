"""Reads conversation events from Kafka and commits offsets only after processing."""
import json
import logging
import time
from collections.abc import Iterator

from kafka import KafkaConsumer
from kafka.consumer.fetcher import ConsumerRecord
from kafka.structs import OffsetAndMetadata, TopicPartition

from tenant_fair_streaming.config import KafkaSettings
from tenant_fair_streaming.conversation_event import ConversationEvent
from tenant_fair_streaming.kafka_adapter.offset_tracker import OffsetTracker

logger = logging.getLogger(__name__)


def decode_json_payload(message_bytes: bytes) -> dict:
    return json.loads(message_bytes.decode("utf-8"))


class KafkaConversationEventSource:
    """Yields events and commits offsets only after the dispatcher reports them completed."""

    def __init__(
        self,
        kafka_settings: KafkaSettings,
        consumer_group: str,
        run_seconds: float,
        start_from_beginning: bool = False,
    ) -> None:
        self._kafka_settings = kafka_settings
        self._run_seconds = run_seconds
        self._offset_tracker = OffsetTracker()
        self._consumer = KafkaConsumer(
            kafka_settings.events_topic,
            bootstrap_servers=kafka_settings.bootstrap_servers,
            group_id=consumer_group,
            enable_auto_commit=False,  # commit manually, only after processing finishes
            auto_offset_reset="earliest" if start_from_beginning else "latest",
            value_deserializer=decode_json_payload,
        )

    def events(self) -> Iterator[ConversationEvent]:
        run_deadline = time.monotonic() + self._run_seconds
        while time.monotonic() < run_deadline:
            records_by_partition = self._consumer.poll(timeout_ms=500)
            for partition_records in records_by_partition.values():
                for consumer_record in partition_records:
                    event = self._decode_or_skip(consumer_record)
                    if event is not None:
                        yield event
            self.commit_completed_offsets()

    def mark_completed(self, event: ConversationEvent) -> None:
        self._offset_tracker.mark_completed(event.event_id)

    def commit_completed_offsets(self) -> None:
        offsets_to_commit = {
            TopicPartition(self._kafka_settings.events_topic, partition): OffsetAndMetadata(offset, "", -1)
            for partition, offset in self._offset_tracker.committable_offsets().items()
        }
        if offsets_to_commit:
            self._consumer.commit(offsets_to_commit)

    def close(self) -> None:
        self.commit_completed_offsets()
        self._consumer.close()

    def _decode_or_skip(self, consumer_record: ConsumerRecord) -> ConversationEvent | None:
        try:
            event = ConversationEvent.from_dict(consumer_record.value)
        except (KeyError, TypeError, ValueError) as decode_error:
            # A malformed record can never succeed, so it is logged and its offset is released.
            logger.error(
                "skipping malformed record at partition %d offset %d: %r",
                consumer_record.partition,
                consumer_record.offset,
                decode_error,
            )
            placeholder_event_id = f"malformed-{consumer_record.partition}-{consumer_record.offset}"
            self._offset_tracker.track(placeholder_event_id, consumer_record.partition, consumer_record.offset)
            self._offset_tracker.mark_completed(placeholder_event_id)
            return None
        self._offset_tracker.track(event.event_id, consumer_record.partition, consumer_record.offset)
        return event
