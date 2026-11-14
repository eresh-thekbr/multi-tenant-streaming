"""At-least-once processing: idempotency check, bounded retries, dead-letter on failure."""
import logging
import time
from collections.abc import Callable
from enum import Enum
from typing import Protocol

from tenant_fair_streaming.dead_letter_sink import DeadLetterSink
from tenant_fair_streaming.conversation_event import ConversationEvent
from tenant_fair_streaming.idempotency_store import IdempotencyStore
from tenant_fair_streaming.pipeline_metrics import PipelineMetrics

logger = logging.getLogger(__name__)


class EventHandler(Protocol):
    def handle(self, event: ConversationEvent) -> None: ...


class ProcessingOutcome(str, Enum):
    PROCESSED = "processed"
    DUPLICATE_SKIPPED = "duplicate_skipped"
    DEAD_LETTERED = "dead_lettered"


class ReliableEventProcessor:
    def __init__(
        self,
        handler: EventHandler,
        idempotency_store: IdempotencyStore,
        dead_letter_sink: DeadLetterSink,
        metrics: PipelineMetrics,
        max_attempts: int = 3,
        clock: Callable[[], float] = time.time,
    ) -> None:
        self._handler = handler
        self._idempotency_store = idempotency_store
        self._dead_letter_sink = dead_letter_sink
        self._metrics = metrics
        self._max_attempts = max_attempts
        self._clock = clock

    def process(self, event: ConversationEvent) -> ProcessingOutcome:
        if self._idempotency_store.was_processed(event.event_id):
            self._metrics.record_duplicate_skipped(event)
            return ProcessingOutcome.DUPLICATE_SKIPPED
        last_handler_error: Exception | None = None
        for attempt in range(1, self._max_attempts + 1):
            try:
                self._handler.handle(event)
            except Exception as handler_error:  # any handler failure is retried, then dead-lettered
                last_handler_error = handler_error
                logger.warning("attempt %d/%d failed for event %s: %r", attempt, self._max_attempts, event.event_id, handler_error)
                continue
            # Mark only after success, so a failed event is not skipped when redelivered.
            self._idempotency_store.mark_processed(event.event_id)
            self._metrics.record_processed(event, completed_at=self._clock())
            return ProcessingOutcome.PROCESSED
        self.reject(event, reason=f"failed after {self._max_attempts} attempts: {last_handler_error!r}")
        return ProcessingOutcome.DEAD_LETTERED

    def reject(self, event: ConversationEvent, reason: str) -> None:
        logger.error("dead-lettering event %s: %s", event.event_id, reason)
        self._dead_letter_sink.publish(event, reason)
        self._metrics.record_dead_lettered(event)
