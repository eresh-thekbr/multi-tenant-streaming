"""Publishes conversation events to Kafka."""
from tenant_fair_streaming.config import KafkaSettings
from tenant_fair_streaming.conversation_event import ConversationEvent
from tenant_fair_streaming.kafka_adapter.kafka_producer_factory import create_producer


class KafkaConversationEventPublisher:
    """Publishes events keyed by account, so each account's events stay in one partition, in order."""

    def __init__(self, kafka_settings: KafkaSettings) -> None:
        self._kafka_settings = kafka_settings
        self._producer = create_producer(kafka_settings)

    def publish(self, event: ConversationEvent) -> None:
        self._producer.send(self._kafka_settings.events_topic, key=event.ordering_key, value=event.to_dict())

    def close(self) -> None:
        self._producer.flush()
        self._producer.close()
