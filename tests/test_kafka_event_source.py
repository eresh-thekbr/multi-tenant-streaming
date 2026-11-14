import threading
from dataclasses import dataclass

from multi_tenant_streaming.config import KafkaSettings
from multi_tenant_streaming.messaging.kafka_event_source import KafkaConversationEventSource


@dataclass(frozen=True)
class StubConsumerRecord:
    partition: int
    offset: int
    value: dict


class StubKafkaConsumer:
    """Returns one prepared batch of records, then empty polls; records commits."""

    def __init__(self, records: list[StubConsumerRecord]) -> None:
        self._pending_batch = {"events-partition-0": records}
        self.committed_offsets: list[dict] = []
        self.is_closed = False

    def poll(self, timeout_ms: int) -> dict:
        polled_records, self._pending_batch = self._pending_batch, {}
        return polled_records

    def commit(self, offsets: dict) -> None:
        self.committed_offsets.append(
            {topic_partition.partition: offset_metadata.offset for topic_partition, offset_metadata in offsets.items()}
        )

    def close(self) -> None:
        self.is_closed = True


def build_source(stub_consumer, stop_requested, run_seconds=None):
    return KafkaConversationEventSource(
        KafkaSettings(),
        consumer_group="test-group",
        stop_requested=stop_requested,
        run_seconds=run_seconds,
        kafka_consumer=stub_consumer,
    )


def records_for(events):
    return [
        StubConsumerRecord(partition=0, offset=offset, value=event.to_dict()) for offset, event in enumerate(events)
    ]


def test_source_stops_yielding_once_stop_is_requested(event_factory):
    events = [event_factory(sequence_number=number) for number in (1, 2, 3)]
    stop_requested = threading.Event()
    event_source = build_source(StubKafkaConsumer(records_for(events)), stop_requested)

    received_event_ids = []
    for event in event_source.events():
        received_event_ids.append(event.event_id)
        if len(received_event_ids) == len(events):
            stop_requested.set()

    assert received_event_ids == [event.event_id for event in events]


def test_close_commits_only_completed_offsets_and_closes_the_consumer(event_factory):
    events = [event_factory(sequence_number=number) for number in (1, 2, 3)]
    stub_consumer = StubKafkaConsumer(records_for(events))
    event_source = build_source(stub_consumer, threading.Event(), run_seconds=0.6)

    for event in event_source.events():
        if event.sequence_number != 2:
            event_source.mark_completed(event)
    event_source.close()

    assert stub_consumer.committed_offsets[-1] == {0: 1}
    assert stub_consumer.is_closed


def test_malformed_record_is_skipped_and_its_offset_released():
    malformed_record = StubConsumerRecord(partition=0, offset=0, value={"unexpected": "shape"})
    stub_consumer = StubKafkaConsumer([malformed_record])
    event_source = build_source(stub_consumer, threading.Event(), run_seconds=0.6)

    received_events = list(event_source.events())
    event_source.close()

    assert received_events == []
    assert stub_consumer.committed_offsets[-1] == {0: 1}
