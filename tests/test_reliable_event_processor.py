from tenant_fair_streaming.dead_letter import InMemoryDeadLetterSink
from tenant_fair_streaming.idempotency import InMemoryIdempotencyStore
from tenant_fair_streaming.metrics import PipelineMetrics
from tenant_fair_streaming.processing import ProcessingOutcome, ReliableEventProcessor


class RecordingHandler:
    def __init__(self, failures_before_success: int = 0) -> None:
        self.remaining_failures = failures_before_success
        self.handled_event_ids: list[str] = []

    def handle(self, event) -> None:
        if self.remaining_failures > 0:
            self.remaining_failures -= 1
            raise RuntimeError("downstream unavailable")
        self.handled_event_ids.append(event.event_id)


def build_processor(handler, max_attempts=3):
    dead_letter_sink = InMemoryDeadLetterSink()
    processor = ReliableEventProcessor(
        handler=handler,
        idempotency_store=InMemoryIdempotencyStore(),
        dead_letter_sink=dead_letter_sink,
        metrics=PipelineMetrics(),
        max_attempts=max_attempts,
    )
    return processor, dead_letter_sink


def test_redelivered_event_is_applied_only_once(event_factory):
    handler = RecordingHandler()
    processor, _ = build_processor(handler)
    event = event_factory()

    first_outcome = processor.process(event)
    second_outcome = processor.process(event)

    assert (first_outcome, second_outcome) == (ProcessingOutcome.PROCESSED, ProcessingOutcome.DUPLICATE_SKIPPED)
    assert handler.handled_event_ids == [event.event_id]


def test_transient_failure_is_retried_until_success(event_factory):
    handler = RecordingHandler(failures_before_success=2)
    processor, dead_letter_sink = build_processor(handler, max_attempts=3)

    outcome = processor.process(event_factory())

    assert outcome is ProcessingOutcome.PROCESSED
    assert dead_letter_sink.dead_letters == []


def test_event_is_dead_lettered_after_max_attempts(event_factory):
    handler = RecordingHandler(failures_before_success=10)
    processor, dead_letter_sink = build_processor(handler, max_attempts=3)
    event = event_factory()

    outcome = processor.process(event)

    assert outcome is ProcessingOutcome.DEAD_LETTERED
    assert [dead_letter.event for dead_letter in dead_letter_sink.dead_letters] == [event]


def test_failed_event_is_not_marked_processed_so_redelivery_is_retried(event_factory):
    handler = RecordingHandler(failures_before_success=3)
    processor, _ = build_processor(handler, max_attempts=3)
    event = event_factory()

    processor.process(event)
    redelivery_outcome = processor.process(event)

    assert redelivery_outcome is ProcessingOutcome.PROCESSED
    assert handler.handled_event_ids == [event.event_id]
