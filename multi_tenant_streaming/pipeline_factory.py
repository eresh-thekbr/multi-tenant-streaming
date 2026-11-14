"""Composition root: wires the processor, metrics, stores and dispatcher together."""
from collections.abc import Callable
from dataclasses import dataclass, field

from multi_tenant_streaming.config import TENANT_IDS, DispatcherSettings, ProcessingCostSettings
from multi_tenant_streaming.dispatch.dispatcher_protocol import CompletionListener, Dispatcher, ignore_completion
from multi_tenant_streaming.dispatch.tenant_fair_dispatcher import TenantFairDispatcher
from multi_tenant_streaming.domain.account_state import AccountStateStore
from multi_tenant_streaming.observability.pipeline_metrics import PipelineMetrics
from multi_tenant_streaming.processing.collections_event_handler import CollectionsEventHandler
from multi_tenant_streaming.processing.dead_letter_sink import DeadLetterSink, InMemoryDeadLetterSink
from multi_tenant_streaming.processing.idempotency_store import IdempotencyStore, InMemoryIdempotencyStore
from multi_tenant_streaming.processing.reliable_event_processor import EventHandler, ReliableEventProcessor


@dataclass
class Pipeline:
    dispatcher: Dispatcher
    metrics: PipelineMetrics
    account_states: AccountStateStore
    dead_letter_sink: DeadLetterSink


@dataclass
class PipelineOptions:
    dispatcher_settings: DispatcherSettings = field(default_factory=DispatcherSettings)
    processing_costs: ProcessingCostSettings = field(default_factory=ProcessingCostSettings)
    tenant_ids: tuple[str, ...] = TENANT_IDS
    on_event_completed: CompletionListener = ignore_completion
    handler: EventHandler | None = None
    idempotency_store: IdempotencyStore | None = None
    dead_letter_sink: DeadLetterSink | None = None


DispatcherBuilder = Callable[[ReliableEventProcessor, PipelineMetrics, PipelineOptions], Dispatcher]


def build_tenant_fair_dispatcher(
    processor: ReliableEventProcessor, metrics: PipelineMetrics, pipeline_options: PipelineOptions
) -> Dispatcher:
    return TenantFairDispatcher(
        processor,
        pipeline_options.tenant_ids,
        pipeline_options.dispatcher_settings,
        metrics,
        pipeline_options.on_event_completed,
    )


def build_pipeline(
    pipeline_options: PipelineOptions, dispatcher_builder: DispatcherBuilder = build_tenant_fair_dispatcher
) -> Pipeline:
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
    dispatcher = dispatcher_builder(processor, metrics, pipeline_options)
    return Pipeline(dispatcher, metrics, account_states, dead_letter_sink)
