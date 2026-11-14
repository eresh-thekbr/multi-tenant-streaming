# The problem in two pictures

## Picture 1: the problem (one shared queue)

All products put their events in **one queue**. One worker takes them **one at a time,
first come, first served**.

```mermaid
flowchart TB
    subgraph Q["ONE SHARED QUEUE: one worker, one event at a time, top to bottom"]
        direction TB
        q1["1. Credit card event"]
        q2["2. Credit card event"]
        q3["... 997 more credit card events ..."]
        q4["1,000. Credit card event"]
        q5["1,001. AUTO LOAN customer: 'Stop calling me'<br/><b>URGENT</b>"]
        q1 --> q2 --> q3 --> q4 --> q5
    end
    q5 --> L["Urgent event handled after <b>about 5 minutes</b><br/><b>TOO LATE</b>"]
    style q5 fill:#ffe0e0,stroke:#c00,color:#000
    style L fill:#ffe0e0,stroke:#c00,color:#000
```

**Problem:** the auto loan event is urgent, but it waits behind 1,000 credit card events.

## Picture 2: the fix (one queue per product)

Each product gets **its own queue and its own worker**.

```mermaid
flowchart TB
    subgraph A["CREDIT CARD QUEUE"]
        direction TB
        a1["1. Credit card event"] --> a2["... 999 more credit card events ..."]
    end
    subgraph B["AUTO LOAN QUEUE"]
        direction TB
        b1["1. AUTO LOAN customer: 'Stop calling me'<br/><b>URGENT</b>"]
    end
    a2 --> AW["Credit card worker<br/>busy for 5 minutes<br/>(only credit card waits)"]
    b1 --> BW["Auto loan worker<br/>handles it <b>immediately</b><br/><b>ON TIME</b>"]
    style b1 fill:#e0f5e0,stroke:#080,color:#000
    style BW fill:#e0f5e0,stroke:#080,color:#000
```

**Fix:** a busy product only slows down itself. Urgent events from other products are
never stuck behind it.
