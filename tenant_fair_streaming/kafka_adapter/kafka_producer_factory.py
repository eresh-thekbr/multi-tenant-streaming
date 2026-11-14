"""Creates Kafka producers with durable, duplicate-free delivery settings."""
import json

from kafka import KafkaProducer

from tenant_fair_streaming.config import KafkaSettings


def encode_json_payload(payload: dict) -> bytes:
    return json.dumps(payload).encode("utf-8")


def create_producer(kafka_settings: KafkaSettings) -> KafkaProducer:
    return KafkaProducer(
        bootstrap_servers=kafka_settings.bootstrap_servers,
        key_serializer=lambda message_key: message_key.encode("utf-8"),
        value_serializer=encode_json_payload,
        acks="all",  # wait for all in-sync replicas so no event is lost
        enable_idempotence=True,  # producer retries never create duplicate records
        linger_ms=10,
    )
