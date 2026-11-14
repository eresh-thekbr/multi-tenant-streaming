"""Baseline: one shared consumer processing events one at a time, like a plain Kafka consumer."""
from collections.abc import Iterable

from multi_tenant_streaming.dispatch.dispatcher_protocol import CompletionListener, ignore_completion
from multi_tenant_streaming.domain.conversation_event import ConversationEvent
from multi_tenant_streaming.processing.reliable_event_processor import ReliableEventProcessor


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
