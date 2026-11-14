# Execution guide

## Prerequisites

- Python 3.10 or newer
- Docker Desktop, running (only for the Kafka stack)

## 1. Install and test

```bash
git clone https://github.com/eresh-thekbr/multi-tenant-streaming.git
cd multi-tenant-streaming
python3 -m venv .venv && source .venv/bin/activate
make install
make test
```

Expected: `36 passed`.

## 2. Benchmark (no Kafka needed)

```bash
make benchmark
```

Runs the same 2,000 events through three strategies in about 15 seconds. Sample output:

```
=== sequential ===
tenant          processed    p50 ms    p99 ms  order errors  duplicates  dead letters  intake blocked
credit_card          1394    2333.6    4645.7             0           0             0               0
auto_loan             410    3715.7    4660.5             0           0             0               0
personal_loan         196    3718.3    4673.9             0           0             0               0

=== unordered-pool ===
credit_card          1394     583.6    1160.6           120           0             0               0
auto_loan             410       4.9    1135.8             9           0             0               0
personal_loan         196       4.3    1152.0             2           0             0               0

=== tenant-fair ===
credit_card          1394     770.7    1322.8             0           0             0               0
auto_loan             410       4.4      13.5             0           0             0               0
personal_loan         196       3.8      18.2             0           0             0               0
```

- **p50 / p99 ms:** time from publish to processed; p99 is the slowest 1%.
- **order errors:** events of one conversation processed out of order.
- **intake blocked:** times intake waited because a tenant's lane was full.

## 3. Full stack on Kafka

```bash
make up      # starts Kafka, creates the topics, starts the consumer
make load    # publishes 60 seconds of traffic at 500 events/sec
make logs    # follow the consumer; per-tenant summary every 30 seconds
make down    # stops everything and deletes Kafka data
```

| Service | Role |
|---|---|
| `kafka` | single-node Kafka (KRaft); `localhost:9092` from the host, `kafka:19092` inside Docker |
| `kafka-setup` | creates `collections.conversation.events` (6 partitions) and the `.dlq` topic, then exits |
| `consumer` | the multi-tenant consumer (`multi-tenant-streaming serve`) |
| `load-generator` | publishes simulated calls; runs only with `make load` |

`docker compose stop consumer` sends SIGTERM: the consumer stops reading, finishes
in-flight events, commits offsets and logs "stopped cleanly".

To compare strategies against Kafka from the host:

```bash
python -m multi_tenant_streaming consume --strategy sequential --seconds 90
python -m multi_tenant_streaming consume --strategy tenant-fair --seconds 90
```

Run each one while `make load` is publishing.

## Configuration

Environment variables, read at start-up. Invalid values stop the service with a clear error.

| Variable | Default |
|---|---|
| `KAFKA_BOOTSTRAP_SERVERS` | `localhost:9092` |
| `EVENTS_TOPIC` | `collections.conversation.events` |
| `DEAD_LETTER_TOPIC` | `collections.conversation.events.dlq` |
| `CONSUMER_GROUP` | `multi-tenant-streaming` |
| `WORKER_COUNT` | `4` |
| `LANE_CAPACITY` | `2000` events per tenant |
| `MAX_PROCESSING_ATTEMPTS` | `3` |
| `LOG_LEVEL` | `INFO` |

## Troubleshooting

| Symptom | Fix |
|---|---|
| `Cannot connect to the Docker daemon` | start Docker Desktop |
| `NoBrokersAvailable` from the host | run `make up` and wait for `kafka` to be healthy (`docker compose ps`) |
| Port 9092 already in use | stop the other local Kafka |
