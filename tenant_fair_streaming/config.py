"""Product profiles, processing costs, dispatcher sizing and Kafka settings."""
import os
from dataclasses import dataclass, field

from tenant_fair_streaming.conversation_event import EventType


@dataclass(frozen=True)
class ProductProfile:
    name: str
    traffic_share: float


# Credit card runs the large campaign: it produces most of the traffic.
PRODUCT_PROFILES: tuple[ProductProfile, ...] = (
    ProductProfile(name="credit_card", traffic_share=0.70),
    ProductProfile(name="auto_loan", traffic_share=0.20),
    ProductProfile(name="personal_loan", traffic_share=0.10),
)

PRODUCT_NAMES: tuple[str, ...] = tuple(product_profile.name for product_profile in PRODUCT_PROFILES)


def _default_processing_seconds() -> dict[EventType, float]:
    # Customer turns run AI analysis (sentiment, intent), so they cost the most.
    return {
        EventType.CALL_STARTED: 0.002,
        EventType.AGENT_TURN: 0.002,
        EventType.CUSTOMER_TURN: 0.008,
        EventType.PROMISE_TO_PAY: 0.001,
        EventType.DISPUTE_RAISED: 0.001,
        EventType.CEASE_CONTACT_REQUESTED: 0.001,
        EventType.CALL_ENDED: 0.002,
    }


@dataclass(frozen=True)
class ProcessingCostSettings:
    seconds_by_event_type: dict[EventType, float] = field(default_factory=_default_processing_seconds)
    cost_scale_factor: float = 1.0

    def seconds_for(self, event_type: EventType) -> float:
        return self.seconds_by_event_type[event_type] * self.cost_scale_factor


@dataclass(frozen=True)
class DispatcherSettings:
    worker_count: int = 4
    lane_capacity: int = 2_000
    max_processing_attempts: int = 3


@dataclass(frozen=True)
class KafkaSettings:
    bootstrap_servers: str = os.environ.get("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
    events_topic: str = "collections.conversation.events"
    dead_letter_topic: str = "collections.conversation.events.dlq"
    topic_partitions: int = 6
