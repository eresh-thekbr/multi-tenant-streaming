"""Sends events that failed all retries to the Kafka dead-letter topic."""
from multi_tenant_streaming.config import KafkaSettings
from multi_tenant_streaming.domain.conversation_event import ConversationEvent
from multi_tenant_streaming.messaging.kafka_producer_factory import create_producer


class KafkaDeadLetterSink:
    def __init__(self, kafka_settings: KafkaSettings) -> None:
        self._kafka_settings = kafka_settings
        self._producer = create_producer(kafka_settings)

    def publish(self, event: ConversationEvent, reason: str) -> None:
        dead_letter_message = {"reason": reason, "event": event.to_dict()}
        self._producer.send(self._kafka_settings.dead_letter_topic, key=event.ordering_key, value=dead_letter_message)

    def close(self) -> None:
        self._producer.flush()
        self._producer.close()
