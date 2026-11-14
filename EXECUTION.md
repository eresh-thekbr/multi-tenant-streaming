# Execution guide

How to run the tests, the in-memory demo and the Kafka setup, and how to read the output.
For what the project does and why, see the [README](README.md).

## 1. Prerequisites

| Tool | Needed for | Check |
|---|---|---|
| Python 3.10 or newer | everything | `python3 --version` |
| make | short commands (optional, see section 7) | `make --version` |
| Docker Desktop, running | Kafka run only | `docker info` |

## 2. Setup

```bash
git clone https://github.com/eresh-thekbr/multi-tenant-streaming.git
cd multi-tenant-streaming
python3 -m venv .venv
source .venv/bin/activate
make install
```

`make install` installs `kafka-python` and `pytest` (pinned in `requirements-dev.txt`).

## 3. Run the tests

```bash
make test
```

Expected: `27 passed` in about 5 seconds. The tests cover:

| Test file | What it proves |
|---|---|
| `test_tenant_fair_dispatcher.py` | credit card burst does not delay other products; order per account; one account never on two workers; failing events dead-lettered; workers survive bugs; full lane applies backpressure; unknown product dead-lettered |
| `test_reliable_event_processor.py` | duplicates skipped; retries; dead-letter after max attempts; failed events are not marked processed |
| `test_product_lane.py` | per-account queueing inside one product lane |
| `test_offset_tracker.py` | Kafka offsets are never committed past an unfinished event |
| `test_simulator_and_arrivals.py` | simulated calls are deterministic and in sequence; burst and steady arrival patterns |

## 4. Run the in-memory demo (no Kafka needed)

```bash
make demo
```

It takes about 15 seconds. The same 2,000 events run through three strategies. Credit
card events arrive as one burst; auto loan and personal loan events arrive steadily over
4 seconds.

Sample output from a real run (your numbers will differ slightly):

```
=== sequential ===
product         processed    p50 ms    p99 ms  order errors  duplicates  dead letters  intake blocked
credit_card          1394    2342.5    4690.6             0           0             0               0
auto_loan             410    3773.5    4711.0             0           0             0               0
personal_loan         196    3776.5    4724.4             0           0             0               0
accounts left in the wrong state: 0   elapsed: 6.8s

=== unordered-pool ===
product         processed    p50 ms    p99 ms  order errors  duplicates  dead letters  intake blocked
credit_card          1394     582.6    1159.1           114           0             0               0
auto_loan             410       4.9    1134.3            10           0             0               0
personal_loan         196       4.2    1151.2             2           0             0               0
accounts left in the wrong state: 0   elapsed: 4.0s

=== tenant-fair ===
product         processed    p50 ms    p99 ms  order errors  duplicates  dead letters  intake blocked
credit_card          1394     779.6    1334.7             0           0             0               0
auto_loan             410       4.6      13.3             0           0             0               0
personal_loan         196       3.8      12.7             0           0             0               0
accounts left in the wrong state: 0   elapsed: 4.0s
```

### How to read it

| Column | Meaning |
|---|---|
| `p50 ms` / `p99 ms` | time from the agent publishing an event to the platform finishing it; p99 is the slowest 1% |
| `order errors` | events of a conversation finished before an earlier event of the same conversation |
| `duplicates` | redelivered events skipped by the idempotency check |
| `dead letters` | events that failed after all retries |
| `intake blocked` | times intake had to wait because a product's lane was full (backpressure) |
| `accounts left in the wrong state` | accounts whose final status differs from what the events say it should be |

What to look for:

- **sequential**: auto loan and personal loan wait about as long as credit card (~4.7 s), the noisy-neighbor problem.
- **unordered-pool**: faster, but `order errors` is above zero, so customers' events are applied out of order.
- **tenant-fair**: auto loan and personal loan p99 drop to ~13 ms with zero order errors.

Options: `python -m tenant_fair_streaming demo --events 600 --seed 7`.

## 5. Run on Kafka

### 5.1 Start Kafka

```bash
make kafka-up
```

This starts a single-node Kafka broker in Docker (KRaft mode, no ZooKeeper) on
`localhost:9092` and creates two topics:

| Topic | Partitions | Purpose |
|---|---|---|
| `collections.conversation.events` | 6 | conversation events, keyed by `account_id` |
| `collections.conversation.events.dlq` | 1 | events that failed after all retries |

The first run downloads the `apache/kafka:3.8.0` image.

### 5.2 Compare the strategies

The producer publishes 500 events per second. That is faster than one sequential
consumer can keep up with, so it builds the same backlog as a large campaign.

Use two terminals, both with the virtual environment activated. **Start the consumer
first** and wait about 10 seconds for it to join its consumer group, because it reads
only new events.

```bash
# terminal 1
make consume STRATEGY=sequential RUN_SECONDS=90

# terminal 2, about 10 seconds later
make produce RUN_SECONDS=60
```

When the consumer finishes, it prints the same table as the demo. Repeat with
`STRATEGY=tenant-fair` (and optionally `STRATEGY=unordered-pool`), producing again each
time.

What to look for: with `sequential`, every product's latency grows as the backlog grows.
With `tenant-fair`, the consumer keeps up and every product's latency stays low.

### 5.3 Inspect Kafka

Consumer lag per partition (how far the consumer is behind):

```bash
docker compose exec kafka /opt/kafka/bin/kafka-consumer-groups.sh \
  --bootstrap-server localhost:9092 --describe --group collections-tenant-fair
```

Dead-letter topic contents:

```bash
docker compose exec kafka /opt/kafka/bin/kafka-console-consumer.sh \
  --bootstrap-server localhost:9092 --topic collections.conversation.events.dlq \
  --from-beginning --timeout-ms 5000
```

### 5.4 Stop Kafka

```bash
make kafka-down
```

This stops the broker and deletes its data.

## 6. Configuration

| Setting | Where | Default |
|---|---|---|
| Kafka address | environment variable `KAFKA_BOOTSTRAP_SERVERS` | `localhost:9092` |
| Workers, lane capacity, retry attempts | `DispatcherSettings` in `tenant_fair_streaming/config.py` | 4 workers, 2,000 events per lane, 3 attempts |
| Product traffic shares | `PRODUCT_PROFILES` in `config.py` | credit card 70%, auto loan 20%, personal loan 10% |
| Processing cost per event type | `ProcessingCostSettings` in `config.py` | customer turn 8 ms, other events 1 to 2 ms |
| Run length | `RUN_SECONDS` for `make consume` / `make produce` | 60 seconds |

## 7. Without make

| make | Plain command |
|---|---|
| `make install` | `python -m pip install -r requirements-dev.txt` |
| `make test` | `python -m pytest -q` |
| `make demo` | `python -m tenant_fair_streaming demo` |
| `make consume STRATEGY=tenant-fair` | `python -m tenant_fair_streaming consume --strategy tenant-fair --seconds 60` |
| `make produce` | `python -m tenant_fair_streaming produce --rate 500 --seconds 60` |

All commands: `python -m tenant_fair_streaming --help`.

## 8. Troubleshooting

| Symptom | Cause and fix |
|---|---|
| `NoBrokersAvailable` | Kafka is not running: run `make kafka-up` and check `docker compose ps` |
| `Cannot connect to the Docker daemon` | start Docker Desktop, then retry |
| The consumer prints `processed 0` | it started after the producer finished; start the consumer first, then the producer |
| `make kafka-up` waits for a long time | the first run is downloading the image; check `docker compose logs kafka` |
| Port 9092 already in use | another Kafka is running locally; stop it, or set `KAFKA_BOOTSTRAP_SERVERS` to point at it |
