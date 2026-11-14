import random
import threading
import time
from collections import defaultdict

import pytest

from multi_tenant_streaming.config import DispatcherSettings, ProcessingCostSettings
from multi_tenant_streaming.domain.conversation_event import EventType
from multi_tenant_streaming.benchmark.strategy_comparison import BenchmarkScenario, DispatchStrategy, run_strategy
from multi_tenant_streaming.pipeline_factory import PipelineOptions, build_pipeline

TENANT_IDS_UNDER_TEST = ("credit_card", "auto_loan", "personal_loan")
RUN_TIMEOUT_SECONDS = 10


class RandomDelayHandler:
    """Sleeps a random few milliseconds per event and records the order events finished in."""

    def __init__(self, seed: int = 7) -> None:
        self._random = random.Random(seed)
        self._lock = threading.Lock()
        self.finished_sequences_by_account: dict[str, list[int]] = defaultdict(list)

    def handle(self, event) -> None:
        with self._lock:
            sleep_seconds = self._random.uniform(0, 0.003)
        time.sleep(sleep_seconds)
        with self._lock:
            self.finished_sequences_by_account[event.account_id].append(event.sequence_number)


class ConcurrencyProbeHandler:
    """Tracks how many events of each account are being processed at the same moment."""

    def __init__(self) -> None:
        self._lock = threading.Lock()
        self._in_flight_by_account: dict[str, int] = defaultdict(int)
        self.max_in_flight_per_account = 0
        self.max_in_flight_total = 0

    def handle(self, event) -> None:
        with self._lock:
            self._in_flight_by_account[event.account_id] += 1
            in_flight_for_account = self._in_flight_by_account[event.account_id]
            self.max_in_flight_per_account = max(self.max_in_flight_per_account, in_flight_for_account)
            self.max_in_flight_total = max(self.max_in_flight_total, sum(self._in_flight_by_account.values()))
        time.sleep(0.005)
        with self._lock:
            self._in_flight_by_account[event.account_id] -= 1


class SelectiveFailureHandler:
    def __init__(self, failing_event_ids: set[str]) -> None:
        self._failing_event_ids = failing_event_ids

    def handle(self, event) -> None:
        if event.event_id in self._failing_event_ids:
            raise ValueError("malformed payload")


class UnreachableIdempotencyStore:
    def was_processed(self, event_id: str) -> bool:
        raise RuntimeError("idempotency store unreachable")

    def mark_processed(self, event_id: str) -> None:
        raise RuntimeError("idempotency store unreachable")


def run_with_timeout(dispatcher, events) -> None:
    dispatcher_thread = threading.Thread(target=dispatcher.run, args=(events,), daemon=True)
    dispatcher_thread.start()
    dispatcher_thread.join(RUN_TIMEOUT_SECONDS)
    assert not dispatcher_thread.is_alive(), "dispatcher did not finish: the pipeline is stuck"


def build_tenant_fair(handler, worker_count=4, lane_capacity=1_000, idempotency_store=None):
    return build_pipeline(
        PipelineOptions(
            dispatcher_settings=DispatcherSettings(worker_count=worker_count, lane_capacity=lane_capacity),
            tenant_ids=TENANT_IDS_UNDER_TEST,
            handler=handler,
            idempotency_store=idempotency_store,
        )
    )


def interleaved_conversation_events(event_factory, accounts_per_tenant=5, events_per_account=20):
    events = []
    for sequence_number in range(1, events_per_account + 1):
        for tenant_id in TENANT_IDS_UNDER_TEST:
            for account_index in range(accounts_per_tenant):
                account_id = f"{tenant_id}-{account_index}"
                events.append(
                    event_factory(
                        tenant_id=tenant_id,
                        account_id=account_id,
                        conversation_id=account_id,
                        sequence_number=sequence_number,
                    )
                )
    return events


def test_each_account_is_processed_in_order(event_factory):
    handler = RandomDelayHandler()
    pipeline = build_tenant_fair(handler)

    run_with_timeout(pipeline.dispatcher, interleaved_conversation_events(event_factory))

    for finished_sequences in handler.finished_sequences_by_account.values():
        assert finished_sequences == sorted(finished_sequences)
    assert all(report.ordering_violations == 0 for report in pipeline.metrics.report(TENANT_IDS_UNDER_TEST))


def test_one_account_is_never_processed_by_two_workers_while_different_accounts_run_in_parallel(event_factory):
    handler = ConcurrencyProbeHandler()
    pipeline = build_tenant_fair(handler, worker_count=4)

    run_with_timeout(pipeline.dispatcher, interleaved_conversation_events(event_factory, events_per_account=5))

    assert handler.max_in_flight_per_account == 1
    assert handler.max_in_flight_total > 1


def test_failing_event_is_dead_lettered_and_the_rest_keep_flowing(event_factory):
    events = interleaved_conversation_events(event_factory, events_per_account=3)
    failing_event_ids = {event.event_id for event in events[:6]}  # more failures than workers
    pipeline = build_tenant_fair(SelectiveFailureHandler(failing_event_ids), worker_count=4)

    run_with_timeout(pipeline.dispatcher, events)

    tenant_reports = pipeline.metrics.report(TENANT_IDS_UNDER_TEST)
    assert sum(report.dead_lettered for report in tenant_reports) == len(failing_event_ids)
    assert sum(report.processed for report in tenant_reports) == len(events) - len(failing_event_ids)


def test_unexpected_processing_error_does_not_kill_workers(event_factory):
    events = [event_factory(account_id=f"account-{index}") for index in range(10)]
    pipeline = build_tenant_fair(RandomDelayHandler(), worker_count=2, idempotency_store=UnreachableIdempotencyStore())

    run_with_timeout(pipeline.dispatcher, events)

    assert len(pipeline.dead_letter_sink.dead_letters) == len(events)


def test_event_for_unknown_tenant_is_dead_lettered(event_factory):
    pipeline = build_tenant_fair(RandomDelayHandler())

    run_with_timeout(pipeline.dispatcher, [event_factory(tenant_id="mortgage")])

    assert [dead_letter.event.tenant_id for dead_letter in pipeline.dead_letter_sink.dead_letters] == ["mortgage"]


def test_full_lane_blocks_intake_and_still_processes_everything(event_factory):
    events = [event_factory(tenant_id="credit_card", account_id=f"account-{index}") for index in range(50)]
    pipeline = build_tenant_fair(RandomDelayHandler(), worker_count=2, lane_capacity=3)

    run_with_timeout(pipeline.dispatcher, events)

    credit_card_report = pipeline.metrics.report(("credit_card",))[0]
    assert credit_card_report.intake_blocked > 0
    assert credit_card_report.processed == len(events)


def p99_for_tenant(strategy_result, tenant_id):
    return next(report.p99_latency_ms for report in strategy_result.tenant_reports if report.tenant_id == tenant_id)


@pytest.mark.parametrize("quiet_tenant", ["auto_loan", "personal_loan"])
def test_credit_card_burst_does_not_delay_other_tenants(quiet_tenant):
    scenario = BenchmarkScenario(
        total_events=600,
        spread_seconds=1.0,
        processing_costs=ProcessingCostSettings(cost_scale_factor=0.5),
    )

    sequential_result = run_strategy(DispatchStrategy.SEQUENTIAL, scenario)
    tenant_fair_result = run_strategy(DispatchStrategy.TENANT_FAIR, scenario)

    sequential_p99 = p99_for_tenant(sequential_result, quiet_tenant)
    tenant_fair_p99 = p99_for_tenant(tenant_fair_result, quiet_tenant)
    assert tenant_fair_p99 * 5 < sequential_p99
    assert tenant_fair_result.wrong_account_states == 0


def test_promise_then_dispute_leaves_account_disputed(event_factory):
    events = [
        event_factory(account_id="account-maria", sequence_number=1, event_type=EventType.PROMISE_TO_PAY),
        event_factory(account_id="account-maria", sequence_number=2, event_type=EventType.DISPUTE_RAISED),
    ]
    pipeline = build_pipeline(
        PipelineOptions(
            tenant_ids=TENANT_IDS_UNDER_TEST,
            processing_costs=ProcessingCostSettings(cost_scale_factor=0.1),
        )
    )

    run_with_timeout(pipeline.dispatcher, events)

    assert pipeline.account_states.get("account-maria").status.value == "disputed"
