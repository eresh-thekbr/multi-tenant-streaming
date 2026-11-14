# Multi-Tenant Streaming System

[![ci](https://github.com/eresh-thekbr/multi-tenant-streaming/actions/workflows/ci.yml/badge.svg)](https://github.com/eresh-thekbr/multi-tenant-streaming/actions/workflows/ci.yml)

A Kafka consumer that lets several tenants share one event stream without slowing each
other down, while keeping every customer's events in order and never applying an event twice.

## 1. Architecture

A bank runs one AI voice agent per lending tenant: **Credit Card**, **Auto Loan** and
**Personal Loan**. Each agent calls only its own past-due customers and publishes every
conversation turn to **one shared Kafka topic**, which feeds compliance, account
updates and AI call analysis.

```mermaid
flowchart LR
    subgraph Agents["Three tenants: one AI voice agent each"]
        CCA["Credit Card agent<br/>calls past-due card customers"]
        ALA["Auto Loan agent<br/>calls past-due auto loan customers"]
        PLA["Personal Loan agent<br/>calls past-due personal loan customers"]
    end

    subgraph Kafka["ONE shared Kafka topic: collections.conversation.events<br/>key = account_id, every event carries its tenant_id"]
        P1["Partition 1"]
        P2["Partition 2"]
        P3["Partition N"]
    end

    subgraph Platform["Multi-tenant streaming system (this repo)"]
        D["Multi-tenant dispatcher<br/>one lane per tenant<br/>in order per account"]
    end

    subgraph Consumers["Downstream actions"]
        CG["Compliance guard<br/>stop contact on bankruptcy,<br/>attorney, cease request<br/>(urgent: seconds)"]
        AU["Account updater<br/>promise to pay, payment,<br/>dispute flag"]
        AI["AI call analysis<br/>summary, sentiment,<br/>next best action<br/>(slow)"]
    end

    DL["Dialer / contact system"]

    CCA -- "conversation events" --> Kafka
    ALA -- "conversation events" --> Kafka
    PLA -- "conversation events" --> Kafka
    Kafka --> D
    D --> CG
    D --> AU
    D --> AI
    CG -- "suppress customer" --> DL
```

There is **one** events topic, not one per tenant: the tenants share an event format and
the same downstream services, so adding a tenant needs no new topic or deployment. The
key is `account_id`, so each customer's events stay in one partition, in order.

### One call and the events it sends to Kafka

Every turn of the conversation is published to Kafka. When the AI recognises a business
intent (a promise to pay, a dispute), it also publishes a business event.

```mermaid
sequenceDiagram
    participant C as Customer (Maria)
    participant V as Auto Loan AI agent
    participant K as Kafka
    V->>K: call.started
    V->>C: "Hi Maria, this is an automated assistant from Acme Bank about your auto loan."
    V->>K: turn 1 (agent)
    V->>C: "This is an attempt to collect a debt..." (required disclosure)
    V->>K: turn 2 (agent)
    V->>C: "Your payment of $450 is 30 days past due. Can you make a payment?"
    V->>K: turn 3 (agent)
    C->>V: "I can pay $200 on Friday."
    V->>K: turn 4 (customer) + promise_to_pay.created
    V->>C: "Thank you. I have noted $200 on Friday."
    V->>K: turn 5 (agent)
    C->>V: "Actually, I dispute this debt."
    V->>K: turn 6 (customer) + dispute.raised
    V->>C: "Understood. I have recorded your dispute and will stop collection on it."
    V->>K: turn 7 (agent)
    V->>K: call.ended
```

## 2. The problem

A standard consumer handles each partition **one event at a time**. When Credit Card
launches a large calling campaign, its events fill every partition, and **urgent events
from the other tenants wait behind them**.

```mermaid
flowchart LR
    subgraph Agents["Three tenants: one AI voice agent each"]
        CCA["Credit Card agent<br/>calls past-due card customers<br/>(large campaign)"]
        ALA["Auto Loan agent<br/>calls past-due auto loan customers"]
        PLA["Personal Loan agent<br/>calls past-due personal loan customers"]
    end

    subgraph Kafka["ONE shared Kafka topic: collections.conversation.events<br/>key = account_id, every event carries its tenant_id"]
        P1["Partition 1"]
        P2["Partition 2"]
        P3["Partition N"]
    end

    subgraph Platform["Event processing"]
        D["Shared consumer<br/>one event at a time<br/>per partition"]
    end

    subgraph Consumers["Downstream actions"]
        CG["Compliance guard<br/>stop contact on bankruptcy,<br/>attorney, cease request<br/>(urgent: seconds)"]
        AU["Account updater<br/>promise to pay, payment,<br/>dispute flag"]
        AI["AI call analysis<br/>summary, sentiment,<br/>next best action<br/>(slow)"]
    end

    DL["Dialer / contact system"]

    CCA -- "conversation events" --> Kafka
    ALA -- "conversation events" --> Kafka
    PLA -- "conversation events" --> Kafka
    Kafka --> D
    D --> CG
    D --> AU
    D --> AI
    CG -- "suppress customer" --> DL
    style D fill:#ffd6d6,stroke:#c00,stroke-width:3px,color:#000
```

*Credit Card campaign traffic fills the red box; Auto Loan and Personal Loan events wait there.*

**Impact.** An Auto Loan customer says *"stop calling me"*. The "do not contact" flag is
set only after the Credit Card backlog clears, so the dialer calls the customer again,
a **compliance violation the Auto Loan team didn't cause**.

Measured with `make benchmark` (2,000 events; Credit Card arrives as one burst, the
other tenants arrive steadily):

| Tenant | p99 wait, shared consumer | p99 wait, multi-tenant consumer |
|---|---|---|
| Credit Card (campaign) | 4.6 s | 1.3 s |
| Auto Loan | **4.7 s** | **14 ms** |
| Personal Loan | **4.7 s** | **18 ms** |

Simply adding workers is not enough: a plain worker pool cut the wait but applied
**131 events out of order** (for example, a dispute processed before the promise to pay
that came before it).

## 3. The design that solves it

End-to-end path of one event, from the voice agent to the committed Kafka offset:

![Event flow](docs/event-flow.svg)

Inside the consumer:

```mermaid
flowchart LR
    K["Kafka consumer<br/>(reads all partitions)"] --> R{"Route by<br/>tenant"}

    subgraph L1["Credit Card lane"]
        Q1["bounded queue"]
    end
    subgraph L2["Auto Loan lane"]
        Q2["bounded queue"]
    end
    subgraph L3["Personal Loan lane"]
        Q3["bounded queue"]
    end

    R --> Q1
    R --> Q2
    R --> Q3

    S["Fair scheduler<br/>round-robin across lanes,<br/>one worker per account at a time"]
    Q1 --> S
    Q2 --> S
    Q3 --> S

    S --> W1["Worker 1"]
    S --> W2["Worker 2"]
    S --> W3["Worker 3"]
    S --> W4["Worker 4"]

    W1 --> ID["Idempotency check<br/>(skip events already applied)"]
    W2 --> ID
    W3 --> ID
    W4 --> ID
    ID --> H["Handlers:<br/>compliance guard,<br/>account updater,<br/>AI analysis"]

    Q1 -. "lane full: intake waits<br/>(backpressure)" .-> K
```

| Requirement | How the design meets it |
|---|---|
| One tenant cannot delay another | A lane per tenant and a round-robin scheduler |
| Each customer's events stay in order | Only one worker handles a given account at a time |
| A busy tenant cannot exhaust memory | Bounded lane; when it is full, intake waits until it drains |
| No duplicate promises or payments | `event_id` is recorded only after successful processing |
| A bad event does not stop the system | Retried, then sent to the dead-letter topic |
| No lost events | Offsets are committed only up to the oldest event still in progress |

## 4. Run it

```bash
make install && make test     # 36 tests
make benchmark                # the comparison above, no Kafka needed
make up && make load          # Kafka + the consumer in Docker, then 60 s of traffic
make logs                     # per-tenant progress every 30 s
make down
```

Step-by-step instructions, configuration and troubleshooting: [EXECUTION.md](EXECUTION.md).

## 5. Project layout

```
multi_tenant_streaming/
  domain/          conversation event and account state
  dispatch/        tenant lanes and the multi-tenant dispatcher
  processing/      idempotency, retries, dead-letter, event handler
  messaging/       Kafka consumer, producer, offset tracking
  observability/   per-tenant metrics and logging
  benchmark/       load generator and baseline strategies for comparison
  config.py        settings from environment variables
  cli.py           serve, benchmark, consume, generate-load
tests/
```

## 6. Design decisions and limits

- **One topic, not one per tenant.** A tenant that permanently dominates traffic, or
  needs its own retention or SLA, should get its own topic.
- **At-least-once delivery.** A crash can replay events; the idempotency check absorbs
  them.
- **A full lane pauses all intake.** Partitions mix tenants, so the consumer cannot
  pause just one. Lanes are sized well above normal bursts.
- **In-memory idempotency store.** It is behind an interface; production would back it
  with Redis or DynamoDB with a TTL.
- **Rebalancing is not handled yet.** Scaling to several consumer instances needs a
  commit on partition revoke.
