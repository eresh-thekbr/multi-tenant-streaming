"""Tenant event simulator — stands in for the CDC ingestion layer.

In a production architecture this could be Debezium / MongoDB change streams
tailing operational databases. Here we generate the same shape of domain events
(ticket.created, conversation.message_added, ...) across tenants, keyed by
tenant_id so Kafka partitioning keeps each tenant's events ordered.
"""
import argparse
import random
import time
import uuid

import config


def make_event(tenant_id, seq):
    return {
        "event_id": str(uuid.uuid4()),
        "tenant_id": tenant_id,
        "seq": seq,  # per-tenant sequence number -> lets the dispatcher verify ordering
        "event_type": random.choice(config.EVENT_TYPES),
        "entity_id": f"{random.choice(['t', 'c', 'i'])}-{random.randint(1000, 9999)}",
        "ts_produced": time.time(),
        "payload": {"note": "simulated operational change"},
    }


def pick_tenant(rng):
    r = rng.random()
    cumulative = 0.0
    for tenant, cfg in config.TENANTS.items():
        cumulative += cfg["share"]
        if r < cumulative:
            return tenant
    return list(config.TENANTS)[-1]


def generate_events(n, seed=7):
    """In-memory event generator (used by --self-test; no Kafka needed)."""
    rng = random.Random(seed)
    seqs = {t: 0 for t in config.TENANTS}
    for _ in range(n):
        t = pick_tenant(rng)
        seqs[t] += 1
        yield make_event(t, seqs[t])


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--seconds", type=float, default=60)
    ap.add_argument("--rate", type=float, default=40,
                    help="events/sec total across tenants")
    args = ap.parse_args()

    from kafka_io import get_producer
    producer = get_producer()
    rng = random.Random()
    seqs = {t: 0 for t in config.TENANTS}
    interval = 1.0 / args.rate
    end = time.time() + args.seconds
    sent = 0
    while time.time() < end:
        t = pick_tenant(rng)
        seqs[t] += 1
        evt = make_event(t, seqs[t])
        producer.send(config.TOPIC, key=t.encode(), value=evt)
        sent += 1
        time.sleep(interval)
    producer.flush()
    print(f"simulator done: sent {sent} events {seqs}")


if __name__ == "__main__":
    main()
