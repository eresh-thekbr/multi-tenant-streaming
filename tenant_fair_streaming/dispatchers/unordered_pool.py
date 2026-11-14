"""Baseline: a plain worker pool. Fast, but one customer's events can finish out of order."""
from collections.abc import Iterable
from concurrent.futures import ThreadPoolExecutor, wait

from tenant_fair_streaming.dispatchers.base import CompletionListener, ignore_completion
from tenant_fair_streaming.events import ConversationEvent
from tenant_fair_streaming.processing import ReliableEventProcessor


class UnorderedPoolDispatcher:
    def __init__(
        self,
        processor: ReliableEventProcessor,
        worker_count: int,
        on_event_completed: CompletionListener = ignore_completion,
    ) -> None:
        self._processor = processor
        self._worker_count = worker_count
        self._on_event_completed = on_event_completed

    def run(self, events: Iterable[ConversationEvent]) -> None:
        with ThreadPoolExecutor(max_workers=self._worker_count, thread_name_prefix="pool-worker") as executor:
            futures = [executor.submit(self._process_and_notify, event) for event in events]
            wait(futures)
        for future in futures:
            future.result()  # re-raise unexpected errors instead of silently losing them

    def _process_and_notify(self, event: ConversationEvent) -> None:
        self._processor.process(event)
        self._on_event_completed(event)
