"""Baseline: one shared consumer processing events one at a time, like a plain Kafka consumer."""
from collections.abc import Iterable

from tenant_fair_streaming.dispatchers.base import CompletionListener, ignore_completion
from tenant_fair_streaming.events import ConversationEvent
from tenant_fair_streaming.processing import ReliableEventProcessor


class SequentialDispatcher:
    def __init__(
        self, processor: ReliableEventProcessor, on_event_completed: CompletionListener = ignore_completion
    ) -> None:
        self._processor = processor
        self._on_event_completed = on_event_completed

    def run(self, events: Iterable[ConversationEvent]) -> None:
        for event in events:
            self._processor.process(event)
            self._on_event_completed(event)
