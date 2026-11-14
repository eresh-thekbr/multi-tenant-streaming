import random
import threading
import time
from collections import defaultdict

import pytest

from tenant_fair_streaming.config import DispatcherSettings, ProcessingCostSettings
from tenant_fair_streaming.demo import DemoScenario, run_strategy
from tenant_fair_streaming.events import EventType
from tenant_fair_streaming.pipeline import DispatchStrategy, PipelineOptions, build_pipeline

PRODUCTS = ("credit_card", "auto_loan", "personal_loan")
RUN_TIMEOUT_SECONDS = 10


class SleepingHandler:
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
            self.max_in_flight_per_account = max(self.max_in_flight_per_account, self._in_flight_by_account[event.account_id])
            self.max_in_flight_total = max(self.max_in_flight_total, sum(self._in_flight_by_account.values()))
        time.sleep(0.005)
        with self._lock:
            self._in_flight_by_account[event.account_id] -= 1


class FailingHandler:
    def __init__(self, failing_event_ids: set[str]) -> None:
        self._failing_event_ids = failing_event_ids

    def handle(self, event) -> None:
        if event.event_id in self._failing_event_ids:
            raise ValueError("malformed payload")


class ExplodingIdempotencyStore:
    def was_processed(self, event_id: str) -> bool:
        raise RuntimeError("idempotency store unreachable")

    def mark_processed(self, event_id: str) -> None:
        raise RuntimeError("idempotency store unreachable")


def run_with_timeout(dispatcher, events) -> None:
    runner = threading.Thread(target=dispatcher.run, args=(events,), daemon=True)
    runner.start()
    runner.join(RUN_TIMEOUT_SECONDS)
    assert not runner.is_alive(), "dispatcher did not finish: the pipeline is stuck"


def build_tenant_fair(handler, worker_count=4, lane_capacity=1_000, idempotency_store=None):
    return build_pipeline(
        PipelineOptions(
            strategy=DispatchStrategy.TENANT_FAIR,
            dispatcher_settings=DispatcherSettings(worker_count=worker_count, lane_capacity=lane_capacity),
            products=PRODUCTS,
            handler=handler,
            idempotency_store=idempotency_store,
        )
    )


def interleaved_conversation_events(event_factory, accounts_per_product=5, events_per_account=20):
    events = []
    for sequence_number in range(1, events_per_account + 1):
        for product in PRODUCTS:
            for account_index in range(accounts_per_product):
                account_id = f"{product}-{account_index}"
                events.append(
                    event_factory(
                        product=product,
                        account_id=account_id,
                        conversation_id=account_id,
                        sequence_number=sequence_number,
                    )
                )
    return events


def test_each_account_is_processed_in_order(event_factory):
    handler = SleepingHandler()
    pipeline = build_tenant_fair(handler)

    run_with_timeout(pipeline.dispatcher, interleaved_conversation_events(event_factory))

    for finished_sequences in handler.finished_sequences_by_account.values():
        assert finished_sequences == sorted(finished_sequences)
    assert all(report.ordering_violations == 0 for report in pipeline.metrics.report(PRODUCTS))


def test_one_account_is_never_processed_by_two_workers_while_different_accounts_run_in_parallel(event_factory):
    handler = ConcurrencyProbeHandler()
    pipeline = build_tenant_fair(handler, worker_count=4)

    run_with_timeout(pipeline.dispatcher, interleaved_conversation_events(event_factory, events_per_account=5))

    assert handler.max_in_flight_per_account == 1
    assert handler.max_in_flight_total > 1


def test_failing_event_is_dead_lettered_and_the_rest_keep_flowing(event_factory):
    events = interleaved_conversation_events(event_factory, events_per_account=3)
    failing_event_ids = {event.event_id for event in events[:6]}  # more failures than workers
    pipeline = build_tenant_fair(FailingHandler(failing_event_ids), worker_count=4)

    run_with_timeout(pipeline.dispatcher, events)

    reports = pipeline.metrics.report(PRODUCTS)
    assert sum(report.dead_lettered for report in reports) == len(failing_event_ids)
    assert sum(report.processed for report in reports) == len(events) - len(failing_event_ids)


def test_unexpected_processing_error_does_not_kill_workers(event_factory):
    events = [event_factory(account_id=f"account-{index}") for index in range(10)]
    pipeline = build_tenant_fair(SleepingHandler(), worker_count=2, idempotency_store=ExplodingIdempotencyStore())

    run_with_timeout(pipeline.dispatcher, events)

    assert len(pipeline.dead_letter_sink.dead_letters) == len(events)


def test_event_for_unknown_product_is_dead_lettered(event_factory):
    pipeline = build_tenant_fair(SleepingHandler())

    run_with_timeout(pipeline.dispatcher, [event_factory(product="mortgage")])

    assert [dead_letter.event.product for dead_letter in pipeline.dead_letter_sink.dead_letters] == ["mortgage"]


def test_full_lane_blocks_intake_and_still_processes_everything(event_factory):
    events = [event_factory(product="credit_card", account_id=f"account-{index}") for index in range(50)]
    pipeline = build_tenant_fair(SleepingHandler(), worker_count=2, lane_capacity=3)

    run_with_timeout(pipeline.dispatcher, events)

    credit_card_report = pipeline.metrics.report(("credit_card",))[0]
    assert credit_card_report.intake_blocked > 0
    assert credit_card_report.processed == len(events)


@pytest.mark.parametrize("quiet_product", ["auto_loan", "personal_loan"])
def test_credit_card_burst_does_not_delay_other_products(quiet_product):
    scenario = DemoScenario(
        total_events=600,
        spread_seconds=1.0,
        processing_costs=ProcessingCostSettings(scale=0.5),
    )

    sequential = run_strategy(DispatchStrategy.SEQUENTIAL, scenario)
    tenant_fair = run_strategy(DispatchStrategy.TENANT_FAIR, scenario)

    sequential_p99 = next(r.p99_latency_ms for r in sequential.product_reports if r.product == quiet_product)
    tenant_fair_p99 = next(r.p99_latency_ms for r in tenant_fair.product_reports if r.product == quiet_product)
    assert tenant_fair_p99 * 5 < sequential_p99
    assert tenant_fair.wrong_account_states == 0


def test_promise_then_dispute_leaves_account_disputed(event_factory):
    events = [
        event_factory(account_id="account-maria", sequence_number=1, event_type=EventType.PROMISE_TO_PAY),
        event_factory(account_id="account-maria", sequence_number=2, event_type=EventType.DISPUTE_RAISED),
    ]
    pipeline = build_pipeline(
        PipelineOptions(
            strategy=DispatchStrategy.TENANT_FAIR,
            products=PRODUCTS,
            processing_costs=ProcessingCostSettings(scale=0.1),
        )
    )

    run_with_timeout(pipeline.dispatcher, events)

    assert pipeline.account_states.get("account-maria").status.value == "disputed"
