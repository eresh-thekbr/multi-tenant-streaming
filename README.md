# multi-tenant-streaming — tenant-fair staged dispatch

A proof of concept of a multi-tenant event-driven streaming architecture:
CDC-style ingestion → Kafka backbone → **staged dispatch partitioned by tenant**
(the SQS `MessageGroupID` pattern) → real-time knowledge-graph updates.
It compares the **noisy-neighbor problem** with tenant-fair staged dispatch
that isolates slow consumers.

## Interactive concept

[Tenant-Fair Streaming concept](docs/tenant-fair-streaming-concept.html) is an
interactive visual illustration of naive processing versus tenant-fair dispatch.
Download the HTML file and open it in a browser to explore the idea. It loads
React and fonts from public CDNs, so an internet connection is needed.

## What it demonstrates

| Architecture layer | PoC component |
|---|---|
| 1. CDC ingestion (Mongo change streams / Debezium) | `simulator.py` — generates `ticket.created`, `conversation.message_added`, … events across 3 tenants, keyed by `tenant_id` |
| 2. Kafka backbone + SQS staging, per-tenant order | `dispatcher.py` — one bounded FIFO per tenant + fair round-robin worker pool; a tenant is processed by at most one worker at a time → strict per-tenant order, tenants never block each other |
| 3. Real-time knowledge-graph / vector updates | `processors.py` — applies every event to an in-memory knowledge graph; idempotent consumer dedupe on `event_id` (at-least-once) |
| 4. Reactive workflows / event-driven actions | Extension point (see `architecture_notes.md`) |

The money demo: **mode A (naive)** processes events single-threaded in poll
order — the noisy tenant (`acme`: 70% of volume, slow downstream) blocks
everyone (head-of-line blocking). **Mode B (staged)** isolates it: quiet
tenants keep low latency while per-tenant ordering stays strict.

## Prerequisites (Mac)

- Kafka running locally on `localhost:9092` (you have it installed).
  With Homebrew: `brew install kafka`, then start ZooKeeper + Kafka
  (or KRaft mode), per the formulae docs.
- Python 3.10+.

## Setup

```bash
cd multi-tenant-streaming
python3 -m venv .venv && source .venv/bin/activate
pip install -r requirements.txt

# create the topic (6 partitions so tenant keys spread)
/opt/homebrew/opt/kafka/bin/kafka-topics.sh --create \
  --topic multi-tenant.ops.events --bootstrap-server localhost:9092 \
  --partitions 6 --replication-factor 1
```

## Run it (no Kafka needed first)

```bash
python run_poc.py --self-test
```

Runs 2,000 in-memory events through both modes with assertions:
per-tenant order must hold in staged mode, and quiet-tenant p99 must beat
naive. This validates the dispatch logic end to end.

## Run it on real Kafka (3 terminals)

```bash
# T1 — event simulator (~40 events/sec, 2 min)
python simulator.py --rate 40 --seconds 120

# T2 — naive baseline (its own consumer group, reads everything)
python run_poc.py --mode naive --seconds 45

# T3 — staged dispatch (after T2 finishes)
python run_poc.py --mode staged --seconds 45
```

Compare the printed tables: in naive mode the quiet tenants'
p99 explodes behind `acme`; in staged mode their p99 stays flat while
`order_viol` stays 0 for every tenant.

## What to look at

- `dispatcher.py::_next_tenant` — the fair scheduler: round-robin across
  tenants, one worker per tenant at a time (the per-tenant-order guarantee).
- `dispatcher.py::submit` — bounded queues: when a tenant's queue is full,
  the submitter blocks → backpressure propagates to the Kafka poll loop
  (watch consumer lag grow).
- `processors.py::process_event` — at-least-once + idempotent dedupe on
  `event_id`; the `slow` path simulates the slow downstream that creates
  the noisy-neighbor effect.

## Design notes (earned while building)

Two bugs caught by the self-test:

1. **Non-blocking per-tenant intake.** The first version blocked the submit
   loop when a tenant's staged queue filled — which reintroduced
   head-of-line blocking at intake: one hot tenant stalled submissions for
   everyone. The fix mirrors Kafka's `consumer.pause(partition)`: intake
   never blocks on a full tenant queue; overflow parks in a per-tenant
   pending buffer (the analog of consumer lag growing on a paused
   partition) while other tenants keep flowing.

2. **Lock-free backpressure accounting.** Counting backpressure events under
   the shared metrics lock let the tight intake loop starve the workers
   (CPython locks aren't fair). Since only the intake thread writes that
   counter, it doesn't take the lock at all.

## Scope and simplifications

- Single-node Kafka, in-memory per-tenant queues instead of SQS.
- The "CDC" is a simulator; prod would be Debezium / Mongo change streams.
- Processing cost is simulated with `sleep`, not real I/O.
- Dedupe set is in-memory; prod would be Redis/DynamoDB with TTL.
- Exactly-once would need a transactional producer + `read_committed`
  consumer; this PoC is at-least-once + idempotent (which is what most
  real systems actually run).
