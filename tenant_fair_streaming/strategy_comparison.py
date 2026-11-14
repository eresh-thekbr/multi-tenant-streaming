"""In-memory comparison of the three dispatch strategies on the same campaign burst."""
import time
from dataclasses import dataclass, field

from tenant_fair_streaming.arrival_schedule import replay_on_schedule, schedule_campaign_burst
from tenant_fair_streaming.config import PRODUCT_NAMES, PRODUCT_PROFILES, ProcessingCostSettings
from tenant_fair_streaming.conversation_event import ConversationEvent, EventType
from tenant_fair_streaming.account_state import AccountState, AccountStateStore, CollectionStatus
from tenant_fair_streaming.pipeline_metrics import ProductReport
from tenant_fair_streaming.pipeline_factory import DispatchStrategy, PipelineOptions, build_pipeline
from tenant_fair_streaming.report_table import format_product_table
from tenant_fair_streaming.campaign_simulator import CampaignSimulator


@dataclass(frozen=True)
class StrategyResult:
    strategy: DispatchStrategy
    product_reports: list[ProductReport]
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


@dataclass(frozen=True)
class DemoScenario:
    total_events: int = 2_000
    seed: int = 42
    burst_product: str = PRODUCT_PROFILES[0].name  # credit card launches the large campaign
    spread_seconds: float = 4.0  # other products keep arriving steadily during the burst
    processing_costs: ProcessingCostSettings = field(default_factory=ProcessingCostSettings)


def run_strategy(strategy: DispatchStrategy, scenario: DemoScenario) -> StrategyResult:
    pipeline = build_pipeline(PipelineOptions(strategy=strategy, processing_costs=scenario.processing_costs))
    generated_events = list(CampaignSimulator(seed=scenario.seed).generate_events(scenario.total_events))
    scheduled_events = schedule_campaign_burst(generated_events, scenario.burst_product, scenario.spread_seconds)
    started_at = time.monotonic()
    pipeline.dispatcher.run(replay_on_schedule(scheduled_events))
    arrival_ordered_events = [scheduled_event.event for scheduled_event in scheduled_events]
    return StrategyResult(
        strategy=strategy,
        product_reports=pipeline.metrics.report(PRODUCT_NAMES),
        wrong_account_states=count_wrong_account_states(arrival_ordered_events, pipeline.account_states),
        elapsed_seconds=time.monotonic() - started_at,
    )


def run_comparison(scenario: DemoScenario) -> list[StrategyResult]:
    return [run_strategy(strategy, scenario) for strategy in DispatchStrategy]


def format_result(strategy_result: StrategyResult) -> str:
    product_table = format_product_table(strategy_result.strategy.value, strategy_result.product_reports)
    summary_line = f"accounts left in the wrong state: {strategy_result.wrong_account_states}   elapsed: {strategy_result.elapsed_seconds:.1f}s"
    return f"{product_table}\n{summary_line}"
