"""Event processors — the 'continuous knowledge graph + AI ingestion' layer.

Two simulated downstream paths:
  * graph updater (fast): applies the event to an in-memory knowledge graph.
  * webhook fanout (slow): stands in for slow external delivery w/ retries.

At-least-once + idempotent consumer: we dedupe on event_id so a redelivered
event never double-applies. (Exactly-once would need a transactional producer
+ read_committed consumer — see architecture_notes.md.)
"""
import threading
import time

import config

_graph = {}
_graph_lock = threading.Lock()
_seen_ids = set()
_seen_lock = threading.Lock()
_duplicates = 0


def process_event(event, fast_ms=None, slow_ms=None):
    global _duplicates
    fast_ms = config.FAST_PROCESS_MS if fast_ms is None else fast_ms
    slow_ms = config.SLOW_PROCESS_MS if slow_ms is None else slow_ms

    with _seen_lock:
        if event["event_id"] in _seen_ids:
            _duplicates += 1
            return "duplicate-skipped"
        _seen_ids.add(event["event_id"])

    tenant = event["tenant_id"]
    if config.TENANTS[tenant]["slow"]:
        time.sleep(slow_ms / 1000.0)   # slow downstream for the noisy tenant
    else:
        time.sleep(fast_ms / 1000.0)

    with _graph_lock:  # real-time knowledge-graph update
        _graph.setdefault(tenant, {})[event["entity_id"]] = {
            "last_type": event["event_type"],
            "seq": event["seq"],
        }
    return "processed"


def duplicate_count():
    with _seen_lock:
        return _duplicates


def reset_state():
    global _duplicates
    with _seen_lock:
        _seen_ids.clear()
        _duplicates = 0
    with _graph_lock:
        _graph.clear()
