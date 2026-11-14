"""Composition root: builds a dispatcher for a chosen strategy with all its dependencies."""
from dataclasses import dataclass, field
from enum import Enum

from tenant_fair_streaming.config import PRODUCT_NAMES, DispatcherSettings, ProcessingCostSettings
from tenant_fair_streaming.dead_letter import DeadLetterSink, InMemoryDeadLetterSink
from tenant_fair_streaming.dispatchers import (
    CompletionListener,
    Dispatcher,
    SequentialDispatcher,
    TenantFairDispatcher,
    UnorderedPoolDispatcher,
)
from tenant_fair_streaming.dispatchers.base import ignore_completion
from tenant_fair_streaming.handlers import AccountStateStore, CollectionsEventHandler
from tenant_fair_streaming.idempotency import IdempotencyStore, InMemoryIdempotencyStore
from tenant_fair_streaming.metrics import PipelineMetrics
from tenant_fair_streaming.processing import EventHandler, ReliableEventProcessor


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


def build_pipeline(options: PipelineOptions) -> Pipeline:
    metrics = PipelineMetrics()
    account_states = AccountStateStore()
    dead_letter_sink = options.dead_letter_sink or InMemoryDeadLetterSink()
    processor = ReliableEventProcessor(
        handler=options.handler or CollectionsEventHandler(account_states, options.processing_costs),
        idempotency_store=options.idempotency_store or InMemoryIdempotencyStore(),
        dead_letter_sink=dead_letter_sink,
        metrics=metrics,
        max_attempts=options.dispatcher_settings.max_processing_attempts,
    )
    dispatcher = _build_dispatcher(options, processor, metrics)
    return Pipeline(dispatcher, metrics, account_states, dead_letter_sink)


def _build_dispatcher(
    options: PipelineOptions, processor: ReliableEventProcessor, metrics: PipelineMetrics
) -> Dispatcher:
    if options.strategy is DispatchStrategy.SEQUENTIAL:
        return SequentialDispatcher(processor, options.on_event_completed)
    if options.strategy is DispatchStrategy.UNORDERED_POOL:
        return UnorderedPoolDispatcher(processor, options.dispatcher_settings.worker_count, options.on_event_completed)
    return TenantFairDispatcher(
        processor, options.products, options.dispatcher_settings, metrics, options.on_event_completed
    )
