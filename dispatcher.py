"""Staged dispatch layer — tenant-fair queues and concurrent workers.

Kafka ingestion is decoupled from slower consumers via staged
queues, partitioned by tenant (SQS MessageGroupID), fanned out concurrently
while keeping strict per-tenant order.

This PoC: one bounded FIFO per tenant + a fixed worker pool doing fair
round-robin scheduling. A tenant is processed by at most one worker at a
time, so per-tenant order is strict; tenants never block each other, so the
noisy neighbor is isolated. Intake is per-tenant and non-blocking: when a
tenant's staged queue is full, its events park in a pending buffer (the
analog of Kafka consumer lag growing on a paused partition) while other
tenants keep flowing — one hot tenant can never stall intake for the rest.
"""
import queue
import statistics
import threading
import time
from collections import defaultdict, deque

import config


class Metrics:
    def __init__(self):
        self._lock = threading.Lock()
        self.latencies = defaultdict(list)
        self.processed = defaultdict(int)
        self.order_violations = defaultdict(int)
        self.last_seq = {}
        self.backpressure_events = defaultdict(int)

    def record(self, tenant, seq, latency_s):
        with self._lock:
            self.processed[tenant] += 1
            self.latencies[tenant].append(latency_s)
            prev = self.last_seq.get(tenant, 0)
            if seq <= prev:
                self.order_violations[tenant] += 1
            else:
                self.last_seq[tenant] = seq

    def backpressure(self, tenant):
        # NOTE: called only by the single intake thread -> deliberately
        # lock-free. Taking Metrics._lock here would let the tight intake
        # loop starve workers' record() calls (CPython locks aren't fair),
        # serializing the whole pipeline. See README "lessons".
        self.backpressure_events[tenant] += 1

    def summary(self):
        rows = {}
        with self._lock:
            for tenant, lat in self.latencies.items():
                s = sorted(lat)
                rows[tenant] = {
                    "processed": self.processed[tenant],
                    "p50_ms": round(statistics.median(s) * 1000, 1),
                    "p99_ms": round(s[int(0.99 * (len(s) - 1))] * 1000, 1),
                    "order_violations": self.order_violations[tenant],
                    "backpressure": self.backpressure_events[tenant],
                }
        return rows


class StagedDispatcher:
    def __init__(self, process_fn, metrics, workers=None, queue_max=None):
        self.process_fn = process_fn
        self.metrics = metrics
        self.workers_n = workers or config.STAGED_WORKERS
        self.queues = {
            t: queue.Queue(maxsize=queue_max or config.TENANT_QUEUE_MAX)
            for t in config.TENANTS
        }
        self._order = list(config.TENANTS)
        self._rr = -1
        self._busy = set()          # tenants currently held by a worker
        self._lock = threading.Lock()
        self._stop = threading.Event()
        self._threads = []

    def start(self):
        for i in range(self.workers_n):
            th = threading.Thread(target=self._worker,
                                  name=f"dispatch-{i}", daemon=True)
            th.start()
            self._threads.append(th)

    def stop(self):
        self._stop.set()
        for th in self._threads:
            th.join()

    def submit(self, event):
        """Non-blocking intake. Returns True if the event entered the staged
        queue, False if the tenant's queue is full (caller parks it in the
        per-tenant pending buffer — the partition-pause analog)."""
        try:
            self.queues[event["tenant_id"]].put_nowait(event)
            return True
        except queue.Full:
            self.metrics.backpressure(event["tenant_id"])
            return False

    def _next_tenant(self):
        with self._lock:
            for _ in self._order:
                self._rr = (self._rr + 1) % len(self._order)
                t = self._order[self._rr]
                if t not in self._busy and not self.queues[t].empty():
                    self._busy.add(t)
                    return t
        return None

    def _worker(self):
        while True:
            if self._stop.is_set() and all(q.empty()
                                           for q in self.queues.values()):
                break
            tenant = self._next_tenant()
            if tenant is None:
                time.sleep(0.002)
                continue
            try:
                event = self.queues[tenant].get(timeout=0.1)
            except queue.Empty:
                with self._lock:
                    self._busy.discard(tenant)
                continue
            try:
                self.process_fn(event)
                self.metrics.record(tenant, event["seq"],
                                    time.time() - event["ts_produced"])
            finally:
                with self._lock:
                    self._busy.discard(tenant)
                self.queues[tenant].task_done()


def run_staged(event_iterable, process_fn, metrics, workers=None):
    """Mode B: staged dispatch — per-tenant FIFO + fair round-robin workers.

    Intake is tenant-isolated: a full staged queue never blocks intake for
    other tenants. Overflow parks in a per-tenant pending buffer (the analog
    of pausing a Kafka partition — lag grows only for the hot tenant).
    """
    disp = StagedDispatcher(process_fn, metrics, workers=workers)
    disp.start()
    pending = defaultdict(deque)  # tenant -> events waiting for queue space

    def drain_pending():
        for tenant in list(pending):
            buf = pending[tenant]
            while buf and disp.submit(buf[0]):
                buf.popleft()
            if not buf:
                del pending[tenant]

    try:
        for event in event_iterable:
            drain_pending()
            if not disp.submit(event):
                pending[event["tenant_id"]].append(event)
        while pending:  # let the hot tenant drain at its own pace
            drain_pending()
            time.sleep(0.01)
    finally:
        disp.stop()
    return metrics


def run_naive(event_iterable, process_fn, metrics):
    """Mode A: single-threaded, poll order — the head-of-line-blocking baseline."""
    for event in event_iterable:
        process_fn(event)
        self_ts = time.time() - event["ts_produced"]
        metrics.record(event["tenant_id"], event["seq"], self_ts)
    return metrics
