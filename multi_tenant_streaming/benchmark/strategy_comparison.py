"""Benchmark: runs the same campaign burst through each dispatch strategy and compares them."""
import time
from dataclasses import dataclass, field
from enum import Enum

from multi_tenant_streaming.benchmark.arrival_schedule import replay_on_schedule, schedule_campaign_burst
from multi_tenant_streaming.benchmark.load_generator import CampaignLoadGenerator
from multi_tenant_streaming.benchmark.sequential_dispatcher import SequentialDispatcher
from multi_tenant_streaming.benchmark.unordered_pool_dispatcher import UnorderedPoolDispatcher
from multi_tenant_streaming.config import TENANT_IDS, TENANT_PROFILES, ProcessingCostSettings
from multi_tenant_streaming.dispatch.dispatcher_protocol import Dispatcher
from multi_tenant_streaming.domain.account_state import AccountState, AccountStateStore, CollectionStatus
from multi_tenant_streaming.domain.conversation_event import ConversationEvent, EventType
from multi_tenant_streaming.observability.pipeline_metrics import PipelineMetrics, TenantReport
from multi_tenant_streaming.observability.report_table import format_tenant_table
from multi_tenant_streaming.pipeline_factory import (
    DispatcherBuilder,
    PipelineOptions,
    build_pipeline,
    build_tenant_fair_dispatcher,
)
from multi_tenant_streaming.processing.reliable_event_processor import ReliableEventProcessor


class DispatchStrategy(str, Enum):
    SEQUENTIAL = "sequential"
    UNORDERED_POOL = "unordered-pool"
    TENANT_FAIR = "tenant-fair"


def build_sequential_dispatcher(
    processor: ReliableEventProcessor, metrics: PipelineMetrics, pipeline_options: PipelineOptions
) -> Dispatcher:
    return SequentialDispatcher(processor, pipeline_options.on_event_completed)


def build_unordered_pool_dispatcher(
    processor: ReliableEventProcessor, metrics: PipelineMetrics, pipeline_options: PipelineOptions
) -> Dispatcher:
    return UnorderedPoolDispatcher(
        processor, pipeline_options.dispatcher_settings.worker_count, pipeline_options.on_event_completed
    )


DISPATCHER_BUILDERS: dict[DispatchStrategy, DispatcherBuilder] = {
    DispatchStrategy.SEQUENTIAL: build_sequential_dispatcher,
    DispatchStrategy.UNORDERED_POOL: build_unordered_pool_dispatcher,
    DispatchStrategy.TENANT_FAIR: build_tenant_fair_dispatcher,
}


@dataclass(frozen=True)
class BenchmarkScenario:
    total_events: int = 2_000
    seed: int = 42
    burst_tenant: str = TENANT_PROFILES[0].tenant_id  # credit card launches the large campaign
    spread_seconds: float = 4.0  # the other tenants keep arriving steadily during the burst
    processing_costs: ProcessingCostSettings = field(default_factory=ProcessingCostSettings)


@dataclass(frozen=True)
class StrategyResult:
    strategy: DispatchStrategy
    tenant_reports: list[TenantReport]
    wrong_account_states: int
    elapsed_seconds: float


def expected_account_states(events: list[ConversationEvent]) -> dict[str, AccountState]:
    expected_states: dict[str, AccountState] = {}
    for event in events:
        account_state = expected_states.setdefault(event.account_id, AccountState())
        if event.event_type is EventType.PROMISE_TO_PAY:
            account_state.status = CollectionStatus.PROMISE_TO_PAY
        elif event.event_type is EventType.DISPUTE_RAISED:
            account_state.status = CollectionStatus.DISPUTED
        elif event.event_type is EventType.CEASE_CONTACT_REQUESTED:
            account_state.do_not_contact = True
    return expected_states


def count_wrong_account_states(events: list[ConversationEvent], account_states: AccountStateStore) -> int:
    expected_states = expected_account_states(events)
    return sum(
        1 for account_id, expected_state in expected_states.items() if account_states.get(account_id) != expected_state
    )


def run_strategy(strategy: DispatchStrategy, scenario: BenchmarkScenario) -> StrategyResult:
    pipeline = build_pipeline(
        PipelineOptions(processing_costs=scenario.processing_costs), DISPATCHER_BUILDERS[strategy]
    )
    generated_events = list(CampaignLoadGenerator(seed=scenario.seed).generate_events(scenario.total_events))
    scheduled_events = schedule_campaign_burst(generated_events, scenario.burst_tenant, scenario.spread_seconds)
    started_at = time.monotonic()
    pipeline.dispatcher.run(replay_on_schedule(scheduled_events))
    arrival_ordered_events = [scheduled_event.event for scheduled_event in scheduled_events]
    return StrategyResult(
        strategy=strategy,
        tenant_reports=pipeline.metrics.report(TENANT_IDS),
        wrong_account_states=count_wrong_account_states(arrival_ordered_events, pipeline.account_states),
        elapsed_seconds=time.monotonic() - started_at,
    )


def run_comparison(scenario: BenchmarkScenario) -> list[StrategyResult]:
    return [run_strategy(strategy, scenario) for strategy in DispatchStrategy]


def format_result(strategy_result: StrategyResult) -> str:
    tenant_table = format_tenant_table(strategy_result.strategy.value, strategy_result.tenant_reports)
    summary_line = (
        f"accounts left in the wrong state: {strategy_result.wrong_account_states}   "
        f"elapsed: {strategy_result.elapsed_seconds:.1f}s"
    )
    return f"{tenant_table}\n{summary_line}"
