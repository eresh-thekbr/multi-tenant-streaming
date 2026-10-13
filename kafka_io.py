"""Thin Kafka glue. Only imported when talking to a real broker."""
import json
import time

import config


_producer = None


def get_producer():
    global _producer
    if _producer is None:
        from kafka import KafkaProducer
        _producer = KafkaProducer(
            bootstrap_servers=config.BOOTSTRAP_SERVERS,
            value_serializer=lambda v: json.dumps(v).encode(),
            acks="all",               # durability: don't lose CDC events
            enable_idempotence=True,  # safe retries, no dupes from the producer
            linger_ms=10,
        )
    return _producer


def consume_events(group_id, max_seconds):
    """Yield event dicts from Kafka for up to max_seconds."""
    from kafka import KafkaConsumer
    consumer = KafkaConsumer(
        config.TOPIC,
        bootstrap_servers=config.BOOTSTRAP_SERVERS,
        group_id=group_id,
        auto_offset_reset="earliest",
        enable_auto_commit=True,
        value_deserializer=lambda b: json.loads(b.decode()),
    )
    end = time.time() + max_seconds
    try:
        while time.time() < end:
            batch = consumer.poll(timeout_ms=500)
            for _tp, records in batch.items():
                for r in records:
                    yield r.value
    finally:
        consumer.close()
