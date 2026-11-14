"""Formats per-product reports as a plain-text table."""
from tenant_fair_streaming.pipeline_metrics import ProductReport

_COLUMNS: tuple[tuple[str, str, int], ...] = (
    ("product", "product", 15),
    ("processed", "processed", 10),
    ("p50 ms", "p50_latency_ms", 10),
    ("p99 ms", "p99_latency_ms", 10),
    ("order errors", "ordering_violations", 14),
    ("duplicates", "duplicates_skipped", 12),
    ("dead letters", "dead_lettered", 14),
    ("intake blocked", "intake_blocked", 16),
)


def format_product_table(title: str, product_reports: list[ProductReport]) -> str:
    header_line = "".join(_align(label, width, is_first=index == 0) for index, (label, _, width) in enumerate(_COLUMNS))
    report_lines = [
        "".join(
            _align(str(getattr(report, field_name)), width, is_first=index == 0)
            for index, (_, field_name, width) in enumerate(_COLUMNS)
        )
        for report in product_reports
    ]
    return "\n".join([f"\n=== {title} ===", header_line, *report_lines])


def _align(cell_text: str, width: int, is_first: bool) -> str:
    return cell_text.ljust(width) if is_first else cell_text.rjust(width)
