"""Logs a per-tenant summary at a fixed interval while the service runs."""
import logging
import threading

from multi_tenant_streaming.observability.pipeline_metrics import PipelineMetrics, TenantReport

logger = logging.getLogger(__name__)


def format_tenant_summary(tenant_report: TenantReport) -> str:
    return (
        f"tenant={tenant_report.tenant_id} processed={tenant_report.processed} "
        f"p50_ms={tenant_report.p50_latency_ms} p99_ms={tenant_report.p99_latency_ms} "
        f"ordering_violations={tenant_report.ordering_violations} "
        f"duplicates_skipped={tenant_report.duplicates_skipped} dead_lettered={tenant_report.dead_lettered}"
    )


def log_tenant_summaries(metrics: PipelineMetrics, tenant_ids: tuple[str, ...]) -> None:
    for tenant_report in metrics.report(tenant_ids):
        logger.info(format_tenant_summary(tenant_report))


class PeriodicMetricsLogger:
    def __init__(self, metrics: PipelineMetrics, tenant_ids: tuple[str, ...], interval_seconds: float) -> None:
        self._metrics = metrics
        self._tenant_ids = tenant_ids
        self._interval_seconds = interval_seconds
        self._stopped = threading.Event()
        self._logging_thread = threading.Thread(target=self._log_until_stopped, name="metrics-logger", daemon=True)

    def start(self) -> None:
        self._logging_thread.start()

    def stop(self) -> None:
        self._stopped.set()
        self._logging_thread.join()

    def _log_until_stopped(self) -> None:
        while not self._stopped.wait(self._interval_seconds):
            log_tenant_summaries(self._metrics, self._tenant_ids)
