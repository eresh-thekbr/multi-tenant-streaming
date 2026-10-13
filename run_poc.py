"""Run the PoC: naive (head-of-line blocking) vs staged (tenant-fair dispatch)."""
import argparse
import functools

import config
from dispatcher import Metrics, run_naive, run_staged
from processors import process_event, reset_state
from simulator import generate_events


def print_summary(title, metrics):
    print(f"\n=== {title} ===")
    rows = metrics.summary()
    print(f"{'tenant':<10}{'processed':>10}{'p50_ms':>10}"
          f"{'p99_ms':>10}{'order_viol':>12}{'backpressure':>13}")
    for t in config.TENANTS:
        r = rows.get(t, {})
        print(f"{t:<10}{r.get('processed', 0):>10}"
              f"{r.get('p50_ms', 0):>10}{r.get('p99_ms', 0):>10}"
              f"{r.get('order_violations', 0):>12}{r.get('backpressure', 0):>13}")


def self_test():
    """No Kafka needed: in-memory events through both modes, with assertions.

    NOTE: each mode gets a freshly materialized event list so ts_produced is
    anchored at that mode's start. Materializing upfront (not lazily) is
    essential: it lets the naive mode actually measure queueing delay
    (a quiet event stuck behind slow ones must show high latency).
    """
    fn = functools.partial(process_event, fast_ms=0, slow_ms=4)

    reset_state()
    events_naive = list(generate_events(2000, seed=42))
    m1 = run_naive(iter(events_naive), fn, Metrics())
    print_summary("NAIVE (single-threaded, poll order)", m1)

    reset_state()
    events_staged = list(generate_events(2000, seed=42))
    m2 = run_staged(iter(events_staged), fn, Metrics(), workers=4)
    print_summary("STAGED (per-tenant FIFO + fair round-robin)", m2)

    s2 = m2.summary()
    assert all(r["order_violations"] == 0 for r in s2.values()), \
        "ordering broken in staged mode!"

    n1 = m1.summary()
    quiet = [t for t, c in config.TENANTS.items() if not c["slow"]]
    naive_q_p99 = max(n1[t]["p99_ms"] for t in quiet)
    staged_q_p99 = max(s2[t]["p99_ms"] for t in quiet)
    print(f"\nquiet-tenant p99 latency: naive={naive_q_p99}ms  "
          f"staged={staged_q_p99}ms")
    assert staged_q_p99 < naive_q_p99, \
        "staged dispatch did not isolate the noisy neighbor"
    print("SELF-TEST PASSED: per-tenant order strict, noisy neighbor isolated.")


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--mode", choices=["naive", "staged"])
    ap.add_argument("--seconds", type=float, default=45)
    ap.add_argument("--group", default=None)
    ap.add_argument("--self-test", action="store_true",
                    help="run without Kafka (in-memory transport)")
    args = ap.parse_args()

    if args.self_test:
        self_test()
        return

    if not args.mode:
        ap.error("--mode is required unless --self-test is given")

    from kafka_io import consume_events
    group = args.group or f"poc-{args.mode}"
    events = consume_events(group, args.seconds)
    reset_state()
    m = Metrics()
    if args.mode == "naive":
        run_naive(events, process_event, m)
    else:
        run_staged(events, process_event, m)
    print_summary(args.mode.upper(), m)


if __name__ == "__main__":
    main()
