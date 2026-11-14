"""Per-tenant latency, ordering and reliability metrics."""
import math
import threading
from collections import defaultdict
from dataclasses import dataclass

from multi_tenant_streaming.domain.conversation_event import ConversationEvent


@dataclass(frozen=True)
class TenantReport:
    tenant_id: str
    processed: int
    p50_latency_ms: float
    p99_latency_ms: float
    ordering_violations: int
    duplicates_skipped: int
    dead_lettered: int
    intake_blocked: int


def percentile(sorted_values: list[float], fraction: float) -> float:
    if not sorted_values:
        return 0.0
    nearest_rank = max(1, math.ceil(fraction * len(sorted_values)))
    return sorted_values[nearest_rank - 1]


class PipelineMetrics:
    def __init__(self) -> None:
        self._lock = threading.Lock()
        self._latencies_by_tenant: dict[str, list[float]] = defaultdict(list)
        self._ordering_violations_by_tenant: dict[str, int] = defaultdict(int)
        self._duplicates_by_tenant: dict[str, int] = defaultdict(int)
        self._dead_letters_by_tenant: dict[str, int] = defaultdict(int)
        self._intake_blocked_by_tenant: dict[str, int] = defaultdict(int)
        self._last_sequence_by_conversation: dict[str, int] = {}

    def record_processed(self, event: ConversationEvent, completed_at: float) -> None:
        with self._lock:
            self._latencies_by_tenant[event.tenant_id].append(completed_at - event.produced_at)
            last_sequence = self._last_sequence_by_conversation.get(event.conversation_id, 0)
            if event.sequence_number < last_sequence:
                self._ordering_violations_by_tenant[event.tenant_id] += 1
            else:
                self._last_sequence_by_conversation[event.conversation_id] = event.sequence_number

    def record_duplicate_skipped(self, event: ConversationEvent) -> None:
        with self._lock:
            self._duplicates_by_tenant[event.tenant_id] += 1

    def record_dead_lettered(self, event: ConversationEvent) -> None:
        with self._lock:
            self._dead_letters_by_tenant[event.tenant_id] += 1

    def record_intake_blocked(self, tenant_id: str) -> None:
        with self._lock:
            self._intake_blocked_by_tenant[tenant_id] += 1

    def report(self, tenant_ids: tuple[str, ...]) -> list[TenantReport]:
        with self._lock:
            return [self._build_tenant_report(tenant_id) for tenant_id in tenant_ids]

    def _build_tenant_report(self, tenant_id: str) -> TenantReport:
        sorted_latencies = sorted(self._latencies_by_tenant[tenant_id])
        return TenantReport(
            tenant_id=tenant_id,
            processed=len(sorted_latencies),
            p50_latency_ms=round(percentile(sorted_latencies, 0.50) * 1000, 1),
            p99_latency_ms=round(percentile(sorted_latencies, 0.99) * 1000, 1),
            ordering_violations=self._ordering_violations_by_tenant[tenant_id],
            duplicates_skipped=self._duplicates_by_tenant[tenant_id],
            dead_lettered=self._dead_letters_by_tenant[tenant_id],
            intake_blocked=self._intake_blocked_by_tenant[tenant_id],
        )
