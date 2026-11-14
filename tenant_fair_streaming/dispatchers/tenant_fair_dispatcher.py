"""Tenant-fair dispatch: a bounded lane per product, round-robin across products, in order per account."""
import logging
import threading
from collections.abc import Iterable

from tenant_fair_streaming.config import DispatcherSettings
from tenant_fair_streaming.dispatchers.dispatcher_protocol import CompletionListener, ignore_completion
from tenant_fair_streaming.dispatchers.product_lane import ProductLane
from tenant_fair_streaming.conversation_event import ConversationEvent
from tenant_fair_streaming.pipeline_metrics import PipelineMetrics
from tenant_fair_streaming.reliable_event_processor import ReliableEventProcessor

logger = logging.getLogger(__name__)


class TenantFairDispatcher:
    def __init__(
        self,
        processor: ReliableEventProcessor,
        products: tuple[str, ...],
        dispatcher_settings: DispatcherSettings,
        metrics: PipelineMetrics,
        on_event_completed: CompletionListener = ignore_completion,
    ) -> None:
        self._processor = processor
        self._dispatcher_settings = dispatcher_settings
        self._metrics = metrics
        self._on_event_completed = on_event_completed
        self._lanes = {product: ProductLane(product, dispatcher_settings.lane_capacity) for product in products}
        self._lane_rotation = list(self._lanes.values())
        self._next_lane_index = 0
        self._unfinished_event_count = 0
        self._intake_finished = False
        self._state_changed = threading.Condition()

    def run(self, events: Iterable[ConversationEvent]) -> None:
        worker_threads = [
            threading.Thread(target=self._run_worker, name=f"tenant-fair-worker-{index}", daemon=True)
            for index in range(self._dispatcher_settings.worker_count)
        ]
        for worker_thread in worker_threads:
            worker_thread.start()
        try:
            for event in events:
                self._admit(event)
        finally:
            with self._state_changed:
                self._intake_finished = True
                self._state_changed.notify_all()
            for worker_thread in worker_threads:
                worker_thread.join()

    def _admit(self, event: ConversationEvent) -> None:
        lane = self._lanes.get(event.product)
        if lane is None:
            self._processor.reject(event, reason=f"unknown product {event.product!r}")
            self._on_event_completed(event)
            return
        with self._state_changed:
            # A full lane blocks intake: this is backpressure, the same effect as pausing the Kafka consumer.
            while not lane.has_capacity():
                self._metrics.record_intake_blocked(lane.product)
                self._state_changed.wait()
            lane.enqueue(event)
            self._unfinished_event_count += 1
            self._state_changed.notify_all()

    def _run_worker(self) -> None:
        while True:
            with self._state_changed:
                lane = self._next_lane_with_ready_account()
                while lane is None:
                    if self._intake_finished and self._unfinished_event_count == 0:
                        return
                    self._state_changed.wait()
                    lane = self._next_lane_with_ready_account()
                event = lane.claim_next_event()
            self._process_safely(event)
            with self._state_changed:
                lane.release_account(event.ordering_key)
                self._unfinished_event_count -= 1
                self._state_changed.notify_all()
            self._on_event_completed(event)

    def _next_lane_with_ready_account(self) -> ProductLane | None:
        for _ in range(len(self._lane_rotation)):
            lane = self._lane_rotation[self._next_lane_index]
            self._next_lane_index = (self._next_lane_index + 1) % len(self._lane_rotation)
            if lane.has_ready_account():
                return lane
        return None

    def _process_safely(self, event: ConversationEvent) -> None:
        try:
            self._processor.process(event)
        except Exception as processing_error:  # a bug in processing must never kill a worker and stall the pipeline
            logger.exception("unexpected error while processing event %s", event.event_id)
            self._processor.reject(event, reason=f"unexpected processing error: {processing_error!r}")
