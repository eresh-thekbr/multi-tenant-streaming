# Architecture notes — multi-tenant-streaming

## Pipeline

1. **CDC ingestion** — The simulator generates domain events across tenants,
   keyed by `tenant_id` so Kafka partitioning preserves tenant order.
   Production sources could use Debezium or MongoDB change streams.
2. **Staged dispatch** — Each tenant has a bounded FIFO with a fair
   round-robin worker pool, analogous to SQS `MessageGroupID`. At most one
   worker processes a tenant at a time. When a queue fills, intake parks
   excess events in a per-tenant pending buffer while other tenants continue.
3. **Knowledge graph** — An idempotent processor applies events to an
   in-memory graph; a slow downstream path simulates webhook delivery.
4. **Reactive workflows** — An extension point for SLA timers, webhook
   fanout, and other actions triggered by domain events.

## Delivery semantics

The proof of concept demonstrates at-least-once delivery with consumer
deduplication by `event_id`. The deduplication state is in memory and does
not survive restarts. The Kafka consumer uses automatic offset commits;
production delivery guarantees would require coordinating offset commits
with successful downstream processing.

Kafka exactly-once processing requires a transactional producer and
`read_committed` consumers where applicable. External queue or database
handoffs also need their own transaction or idempotency strategy.

## Production considerations

- Define the ordering boundary across Kafka, staged queues, and consumers.
- Handle schema evolution as producers change event shapes.
- Bound pending buffers and propagate backpressure to broker intake.
- Monitor consumer lag and queue depth for slow tenants.
- Persist deduplication state with an appropriate retention policy.
