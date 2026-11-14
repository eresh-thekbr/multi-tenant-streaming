# Problem statement: tenant-fair event processing for an AI debt-recovery voice agent

> Draft for review. This document describes the problem and the target design.
> The code in this repo is still being reworked to match it.

## 1. Context

A US bank uses an **AI voice agent** to talk to customers about overdue balances
(debt recovery / collections). One shared platform serves several **product lines**:

| Product line (tenant) | Typical traffic |
|---|---|
| Credit cards | Very high volume, large outbound calling campaigns |
| Auto loans | Medium volume |
| Personal loans | Low volume |

While the agent talks to a customer, the call produces **events** (what was said,
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

### Example events from one call

```mermaid
sequenceDiagram
    participant C as Customer
    participant V as AI voice agent
    participant K as Kafka
    C->>V: answers the call
    V->>K: call.started
    V->>K: disclosure.given
    C->>V: "I can pay $200 on Friday"
    V->>K: utterance.transcribed
    V->>K: promise_to_pay.created
    C->>V: "Actually, I dispute this debt"
    V->>K: intent.detected (dispute)
    V->>K: call.ended
```

## 3. The problem

### 3.1 What goes wrong: a 6-minute story

**Setup today:** all product lines share the same Kafka topic. The consumer reads
each partition **one event at a time, in arrival order** (that is how Kafka keeps
order). Each event takes about **150 ms**, because AI analysis runs on every event.

Follow one auto loan customer, **John**, through a Monday morning:

```mermaid
sequenceDiagram
    participant CC as Credit card<br/>campaign
    participant J as John<br/>(auto loan customer)
    participant VA as AI voice agent
    participant K as Kafka<br/>(one shared line)
    participant EP as Event processor<br/>(one event at a time)
    participant DL as Dialer

    Note over CC,DL: 9:00 AM - credit card team starts calling 5,000 customers
    CC->>K: about 2,000 call events pile up in the line
    K->>EP: processor works through credit card events, 150 ms each

    Note over CC,DL: 9:01 AM - John is on a call about his auto loan
    J->>VA: "I filed for bankruptcy. Stop calling me."
    VA->>K: event - bankruptcy detected for John
    Note over K: John's event is number 2,001 in the line.<br/>Wait time = 2,000 x 150 ms = about 5 minutes

    Note over CC,DL: 9:03 AM - nobody has processed John's event yet
    rect rgb(255, 214, 214)
    DL->>J: Calls John again (no "do not contact" flag yet)
    Note over DL,J: COMPLIANCE VIOLATION - contacting a customer after a bankruptcy filing
    end

    Note over CC,DL: 9:06 AM
    K->>EP: finally reads John's bankruptcy event
    EP->>DL: mark John "do not contact"
    Note over EP,DL: Correct action, but 5 minutes too late
```

**The auto loan team did nothing wrong.** The credit card team's traffic caused the
delay, and the auto loan team pays the price. This is the **noisy-neighbor problem**:
in a shared system, one tenant's load hurts another tenant.

### 3.2 Why it happens

Three things together cause it:

1. **All product lines share one line** (the same Kafka partitions).
2. **The line is processed one event at a time** (to keep events in order).
3. **One product line sends far more events** than the others (a calling campaign).

So an urgent event from a small product line waits behind a big product line's backlog.

```mermaid
flowchart LR
    subgraph Today["TODAY - one shared line"]
        direction LR
        T1["2,000 credit card events"] --> T2["John's bankruptcy event"] --> T3["1 worker"]
    end
    T3 -. "John waits ~5 minutes" .-> X["Too late:<br/>dialer already called again"]
    style T2 fill:#ffd6d6,stroke:#c00,color:#000
    style X fill:#ffd6d6,stroke:#c00,color:#000
```

```mermaid
flowchart LR
    subgraph Needed["NEEDED - one lane per product line"]
        direction LR
        N1["Credit card lane<br/>2,000 events"] --> W1["workers"]
        N2["Auto loan lane<br/>John's bankruptcy event"] --> W2["workers"]
        N3["Personal loan lane"] --> W3["workers"]
    end
    W2 -. "John handled in milliseconds" .-> OK["Dialer stops<br/>before next call"]
    style N2 fill:#d6f5d6,stroke:#080,color:#000
    style OK fill:#d6f5d6,stroke:#080,color:#000
```

### 3.3 Why "just add more workers" breaks something else

If we simply let many workers process events in parallel, **one customer's events can
finish out of order**. Follow customer **Maria**, who says two things on one call:

```mermaid
sequenceDiagram
    participant K as Kafka
    participant W1 as Worker 1
    participant W2 as Worker 2
    participant DB as Maria's account

    K->>W1: event 1 - "I'll pay $200 on Friday"
    K->>W2: event 2 - "Actually, I dispute this debt"
    Note over W1: Worker 1 is slower (busy)
    W2->>DB: status = DISPUTED
    W1->>DB: status = PROMISE TO PAY (overwrites the dispute)
    rect rgb(255, 214, 214)
    Note over DB: Final status is WRONG.<br/>The bank keeps collecting a debt the customer disputed.
    end
```

So we need **both**: parallel lanes so product lines do not block each other, **and**
a guarantee that **each customer's events are processed in order**.

### 3.4 Problem statement

> Several product lines share one event pipeline.
>
> 1. **Isolation:** one product line's traffic must not delay another product line's urgent events (John).
> 2. **Ordering:** each customer's events must be processed in the exact order they happened (Maria).
> 3. **No duplicates:** an event delivered twice must be applied only once (no double payment plans).

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

    Q1 -. "lane full: pause only<br/>credit card intake" .-> K
```

| Requirement | How the design meets it |
|---|---|
| One product line cannot delay another | Separate lane per product line + round-robin scheduler |
| Each customer's events stay in order | Only one worker handles a given account at a time |
| A hot product line cannot exhaust memory | Bounded lane; when full, only that product line's intake pauses (backpressure) |
| No duplicate promises or payments | Idempotency check on `event_id`, recorded only **after** successful processing |
| A bad event does not stop the platform | Retry a few times, then move to a dead-letter queue |

## 5. What the demo will prove

| Scenario | Naive (one shared line) | Tenant-fair design |
|---|---|---|
| Credit card campaign burst | Auto loan bankruptcy event waits minutes | Handled in milliseconds |
| Customer events out of order | Not applicable (single thread) | Zero ordering violations |
| Same event delivered twice | Applied twice | Applied once |
| One event keeps failing | Pipeline stalls | Event goes to dead-letter queue, others continue |

## 6. Out of scope for this proof of concept

- The voice agent itself (speech recognition, LLM, telephony) is simulated.
- Storage is in memory; production would use a database and Redis for idempotency keys.
- Legal rules are simplified to "suppress contact on bankruptcy, attorney or cease request".
