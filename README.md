# Multi-tenant streaming: tenant-fair Kafka processing

Several product lines of a bank share one Kafka event pipeline for their **AI
debt-recovery voice agents**. This repo shows how to stop one product line's traffic
from delaying the others, while keeping every customer's events in order and never
applying the same event twice.

## 1. Context

A US bank uses **AI voice agents** to talk to customers about overdue balances (debt
recovery / collections). Each product line has its own agent, and an agent only calls
customers whose accounts are **past due**; it never contacts customers who are current.
One shared platform serves all the **product lines**:

| Product line (tenant) | Typical traffic |
|---|---|
| Credit cards | Very high volume, large outbound calling campaigns |
| Auto loans | Medium volume |
| Personal loans | Low volume |

While an agent talks to a customer, the call produces **events** (what was said,
what the customer intends, promises to pay, payments). These events go to **Kafka**
so that other systems can react to them in near real time.

### What is a "tenant" here?

A **tenant** is a group that **shares a system** with other groups but must be
**treated separately**, like apartments in one building. Here the shared system is
the event pipeline, and the tenants are the product lines. Because all product lines
flow through the same pipeline, the system is **multi-tenant**.

## 2. High-level architecture

```mermaid
flowchart LR
    subgraph Calls["Live customer calls"]
        CC["Credit card<br/>customers"]
        AL["Auto loan<br/>customers"]
        PL["Personal loan<br/>customers"]
    end

    VA["AI voice agent<br/>(speech to text, LLM, text to speech)"]

    subgraph Kafka["Kafka topic: collections.conversation.events<br/>key = account_id"]
        P1["Partition 1"]
        P2["Partition 2"]
        P3["Partition N"]
    end

    subgraph Platform["Event processing platform (this repo)"]
        D["Tenant-fair dispatcher<br/>one lane per product line<br/>in-order per account"]
    end

    subgraph Consumers["Downstream actions"]
        CG["Compliance guard<br/>stop contact on bankruptcy,<br/>attorney, cease request<br/>(urgent: seconds)"]
        AU["Account updater<br/>promise to pay, payment,<br/>dispute flag"]
        AI["AI call analysis<br/>summary, sentiment,<br/>next best action<br/>(slow)"]
    end

    DL["Dialer / contact system"]

    CC --> VA
    AL --> VA
    PL --> VA
    VA -- "conversation events" --> Kafka
    Kafka --> D
    D --> CG
    D --> AU
    D --> AI
    CG -- "suppress customer" --> DL
```

Note: the live conversation itself (speech in, AI reply out) does **not** go through
Kafka. Kafka carries **what happened in the call** to the systems that must act on it.

### Example: one call and the events it sends to Kafka

Maria is called **only because her auto loan payment is 30 days past due** (she is in
collections). Customers who are not in collections are never called.

**Every turn of the conversation** is published to Kafka, as a
`conversation.agent_turn` or `conversation.customer_turn` event. When the AI recognises a business intent in a
turn (a promise to pay, a dispute), it also publishes a **business event** for it.

```mermaid
sequenceDiagram
    participant C as Customer (Maria)
    participant V as AI voice agent
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

## 3. Problem statement

**Context.** The bank runs one AI collections agent per loan product: credit card, auto
loan and personal loan. Each agent calls **its own past-due customers**. All three
publish every conversation turn to **one shared Kafka topic**, keyed by account ID. The
topic is shared because this is one platform: the same event format and the same
downstream services (compliance, account updates, AI analysis). Each customer's events
must be processed in order.

**What goes wrong.** The consumer processes each partition **one event at a time**. When
one product runs a large campaign, its events fill every partition, and **urgent events
from the other products wait behind them**.

```mermaid
flowchart LR
    subgraph Calls["Live customer calls"]
        CC["Credit card<br/>customers<br/>(large campaign)"]
        AL["Auto loan<br/>customers"]
        PL["Personal loan<br/>customers"]
    end

    VA["AI voice agent<br/>(speech to text, LLM, text to speech)"]

    subgraph Kafka["Kafka topic: collections.conversation.events<br/>key = account_id"]
        P1["Partition 1"]
        P2["Partition 2"]
        P3["Partition N"]
    end

    subgraph Platform["Event processing platform"]
        D["Shared consumer<br/>one event at a time<br/>per partition"]
    end

    subgraph Consumers["Downstream actions"]
        CG["Compliance guard<br/>stop contact on bankruptcy,<br/>attorney, cease request<br/>(urgent: seconds)"]
        AU["Account updater<br/>promise to pay, payment,<br/>dispute flag"]
        AI["AI call analysis<br/>summary, sentiment,<br/>next best action<br/>(slow)"]
    end

    DL["Dialer / contact system"]

    CC --> VA
    AL --> VA
    PL --> VA
    VA -- "conversation events" --> Kafka
    Kafka --> D
    D --> CG
    D --> AU
    D --> AI
    CG -- "suppress customer" --> DL
    style D fill:#ffd6d6,stroke:#c00,stroke-width:3px,color:#000
```

*Credit card campaign traffic fills the red box; auto loan and personal loan events wait
there.*

**Evidence.** Measured with `make demo` (section 6). It is a scaled-down test: 2,000
events, 70% from the credit card campaign. Credit card events arrive as one burst; auto
loan and personal loan events keep arriving steadily over 4 seconds.

| Product | p99 wait: one shared consumer | p99 wait: tenant-fair dispatch |
|---|---|---|
| Credit card (busy campaign) | 4.7 s | 1.3 s |
| Auto loan | **4.7 s** | **13 ms** |
| Personal loan | **4.7 s** | **13 ms** |

The credit card campaign slows the other products down **about 350 times**. With
tenant-fair dispatch, only credit card events wait for credit card traffic.

**Impact.** An auto loan customer says *"stop calling me"*. The "do not contact" flag is
set only after the credit card backlog clears. Meanwhile, the auto loan dialer's next
scheduled attempt **calls the customer again**, which is a compliance violation. The auto
loan team did nothing wrong; another product's traffic caused it.

**Any fix must keep:**

1. Each customer's events processed in order.
2. Each event applied only once, even if Kafka delivers it twice.
3. Memory bounded when one product floods the platform.

## 4. Target design

```mermaid
flowchart LR
    K["Kafka consumer<br/>(reads all partitions)"] --> R{"Route by<br/>product line"}

    subgraph L1["Credit card lane"]
        Q1["bounded queue"]
    end
    subgraph L2["Auto loan lane"]
        Q2["bounded queue"]
    end
    subgraph L3["Personal loan lane"]
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
| One product line cannot delay another | Separate lane per product line + round-robin scheduler |
| Each customer's events stay in order | Only one worker handles a given account at a time |
| A hot product line cannot exhaust memory | Bounded lane; when it is full, intake waits until it drains (backpressure) |
| No duplicate promises or payments | Idempotency check on `event_id`, recorded only **after** successful processing |
| A bad event does not stop the platform | Retry a few times, then move to a dead-letter queue |

## 5. Results

`make demo` runs the same burst through three strategies:

| Strategy | Auto loan p99 | Personal loan p99 | Ordering errors |
|---|---|---|---|
| Sequential (one shared consumer) | 4.7 s | 4.7 s | 0 |
| Unordered pool ("just add workers") | 1.1 s | 1.2 s | **126** |
| **Tenant-fair** | **13 ms** | **13 ms** | **0** |

`make test` checks the remaining guarantees:

| Scenario | Result |
|---|---|
| Same event delivered twice | Applied once (idempotency check) |
| One event keeps failing | Retried, then dead-lettered; the other events keep flowing |
| Bug inside processing | Worker survives; event is dead-lettered |
| Lane full | Intake waits, nothing is lost |
| One account | Never processed by two workers at once; other accounts run in parallel |

## 6. Run it (no Kafka needed)

```bash
python3 -m venv .venv && source .venv/bin/activate
make install
make test   # 27 tests, about 5 seconds
make demo   # compares three dispatch strategies on the same campaign burst
```

## 7. Run it on Kafka

Requires Docker.

```bash
make kafka-up                                   # single-node Kafka (KRaft) + topics
make consume STRATEGY=tenant-fair RUN_SECONDS=90  # terminal 1: start the consumer first
make produce RUN_SECONDS=60                     # terminal 2: publish simulated calls
make kafka-down
```

`STRATEGY` can be `sequential`, `unordered-pool` or `tenant-fair`.

## 8. Where each guarantee lives in the code

| Concern | Where | How |
|---|---|---|
| Product isolation | `dispatchers/tenant_fair.py` | One bounded lane per product; workers pick lanes round-robin |
| Order per customer | `dispatchers/product_lane.py` | Events queue per account; an account is handed to only one worker at a time, different accounts run in parallel |
| No double-apply | `processing.py`, `idempotency.py` | `event_id` is checked first and recorded only **after** success |
| Failing events | `processing.py`, `dead_letter.py` | Bounded retries, then the dead-letter topic; a worker never dies |
| No lost events | `kafka_adapter/offset_tracker.py` | Auto-commit is off; Kafka offsets are committed only up to the lowest event still in progress |
| Backpressure | `dispatchers/tenant_fair.py` | A full lane makes intake wait, so memory stays bounded |

Design patterns used: **Bulkhead** (a lane per product), **Idempotent Consumer**,
**Dead Letter Channel**, **Strategy** (interchangeable dispatchers behind one
interface), **Ports and Adapters** (the idempotency store, dead-letter sink and event
source are interfaces with in-memory and Kafka implementations), and a **Composition
Root** (`pipeline.py`) that wires them together.

## 9. Project layout

```
tenant_fair_streaming/
  events.py            conversation event model
  simulator.py         simulated AI agent calls per product campaign
  arrivals.py          arrival patterns (campaign burst vs steady traffic)
  dispatchers/         sequential, unordered pool, tenant-fair
  processing.py        idempotency + retries + dead-letter
  handlers.py          compliance guard, account updates, simulated AI analysis
  kafka_adapter/       Kafka source, publisher, dead-letter sink, offset tracking
  pipeline.py          composition root
  cli.py               demo / produce / consume commands
tests/                 unit and behaviour tests
```

## 10. Trade-offs and production gaps

- **Delivery is at-least-once, not exactly-once.** Offsets are committed after
  processing, so a crash replays some events; the idempotency check absorbs the replays.
- **A full lane pauses all intake.** Kafka partitions mix products, so the consumer
  cannot pause only one product. Lanes are sized well above normal bursts. If one
  product keeps saturating its lane, give it its own topic.
- **In-memory state.** The idempotency store and account state are in memory; production
  would use Redis or DynamoDB (with a TTL) and the account system of record.
- **Simulated work.** Downstream calls (LLM analysis, CRM updates) are simulated with
  `sleep`, and the voice agent is simulated by `simulator.py`.
- **Single consumer process.** Scaling out means more consumers in the same group; each
  one runs this dispatcher over the partitions it is assigned. Rebalance handling
  (draining and committing when a partition is revoked) is not implemented yet.
- **Legal rules are simplified** to "stop contact on bankruptcy, attorney or cease request".
