"""Composition root: builds a dispatcher for a chosen strategy with all its dependencies."""
from dataclasses import dataclass, field
from enum import Enum

from tenant_fair_streaming.config import PRODUCT_NAMES, DispatcherSettings, ProcessingCostSettings
from tenant_fair_streaming.dead_letter_sink import DeadLetterSink, InMemoryDeadLetterSink
from tenant_fair_streaming.dispatchers import (
    CompletionListener,
    Dispatcher,
    SequentialDispatcher,
    TenantFairDispatcher,
    UnorderedPoolDispatcher,
)
from tenant_fair_streaming.dispatchers.dispatcher_protocol import ignore_completion
from tenant_fair_streaming.account_state import AccountStateStore
from tenant_fair_streaming.collections_event_handler import CollectionsEventHandler
from tenant_fair_streaming.idempotency_store import IdempotencyStore, InMemoryIdempotencyStore
from tenant_fair_streaming.pipeline_metrics import PipelineMetrics
from tenant_fair_streaming.reliable_event_processor import EventHandler, ReliableEventProcessor


class DispatchStrategy(str, Enum):
    SEQUENTIAL = "sequential"
    UNORDERED_POOL = "unordered-pool"
    TENANT_FAIR = "tenant-fair"


@dataclass
class Pipeline:
    dispatcher: Dispatcher
    metrics: PipelineMetrics
    account_states: AccountStateStore
    dead_letter_sink: DeadLetterSink


@dataclass
class PipelineOptions:
    strategy: DispatchStrategy
    dispatcher_settings: DispatcherSettings = field(default_factory=DispatcherSettings)
    processing_costs: ProcessingCostSettings = field(default_factory=ProcessingCostSettings)
    products: tuple[str, ...] = PRODUCT_NAMES
    on_event_completed: CompletionListener = ignore_completion
    handler: EventHandler | None = None
    idempotency_store: IdempotencyStore | None = None
    dead_letter_sink: DeadLetterSink | None = None


def build_pipeline(pipeline_options: PipelineOptions) -> Pipeline:
    metrics = PipelineMetrics()
    account_states = AccountStateStore()
    dead_letter_sink = pipeline_options.dead_letter_sink or InMemoryDeadLetterSink()
    processor = ReliableEventProcessor(
        handler=pipeline_options.handler or CollectionsEventHandler(account_states, pipeline_options.processing_costs),
        idempotency_store=pipeline_options.idempotency_store or InMemoryIdempotencyStore(),
        dead_letter_sink=dead_letter_sink,
        metrics=metrics,
        max_attempts=pipeline_options.dispatcher_settings.max_processing_attempts,
    )
    dispatcher = _build_dispatcher(pipeline_options, processor, metrics)
    return Pipeline(dispatcher, metrics, account_states, dead_letter_sink)


def _build_dispatcher(
    pipeline_options: PipelineOptions, processor: ReliableEventProcessor, metrics: PipelineMetrics
) -> Dispatcher:
    if pipeline_options.strategy is DispatchStrategy.SEQUENTIAL:
        return SequentialDispatcher(processor, pipeline_options.on_event_completed)
    if pipeline_options.strategy is DispatchStrategy.UNORDERED_POOL:
        return UnorderedPoolDispatcher(processor, pipeline_options.dispatcher_settings.worker_count, pipeline_options.on_event_completed)
    return TenantFairDispatcher(
        processor, pipeline_options.products, pipeline_options.dispatcher_settings, metrics, pipeline_options.on_event_completed
    )
