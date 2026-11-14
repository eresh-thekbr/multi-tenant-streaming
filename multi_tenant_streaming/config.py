"""Tenant profiles, processing costs, dispatcher sizing, Kafka settings and environment loading."""
import os
from collections.abc import Mapping
from dataclasses import dataclass, field

from multi_tenant_streaming.domain.conversation_event import EventType


@dataclass(frozen=True)
class TenantProfile:
    tenant_id: str
    traffic_share: float


# Credit card runs the large campaign: it produces most of the traffic.
TENANT_PROFILES: tuple[TenantProfile, ...] = (
    TenantProfile(tenant_id="credit_card", traffic_share=0.70),
    TenantProfile(tenant_id="auto_loan", traffic_share=0.20),
    TenantProfile(tenant_id="personal_loan", traffic_share=0.10),
)

TENANT_IDS: tuple[str, ...] = tuple(tenant_profile.tenant_id for tenant_profile in TENANT_PROFILES)


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
    bootstrap_servers: str = "localhost:9092"
    events_topic: str = "collections.conversation.events"
    dead_letter_topic: str = "collections.conversation.events.dlq"
    consumer_group: str = "multi-tenant-streaming"


@dataclass(frozen=True)
class ServiceSettings:
    kafka: KafkaSettings = field(default_factory=KafkaSettings)
    dispatcher: DispatcherSettings = field(default_factory=DispatcherSettings)
    log_level: str = "INFO"


class InvalidSettingError(ValueError):
    pass


_LOG_LEVELS = ("DEBUG", "INFO", "WARNING", "ERROR")


def load_settings_from_environment(environment: Mapping[str, str] | None = None) -> ServiceSettings:
    environment = os.environ if environment is None else environment
    kafka_defaults = KafkaSettings()
    dispatcher_defaults = DispatcherSettings()
    log_level = environment.get("LOG_LEVEL", "INFO").upper()
    if log_level not in _LOG_LEVELS:
        raise InvalidSettingError(f"LOG_LEVEL must be one of {', '.join(_LOG_LEVELS)}, got {log_level!r}")
    return ServiceSettings(
        kafka=KafkaSettings(
            bootstrap_servers=environment.get("KAFKA_BOOTSTRAP_SERVERS", kafka_defaults.bootstrap_servers),
            events_topic=environment.get("EVENTS_TOPIC", kafka_defaults.events_topic),
            dead_letter_topic=environment.get("DEAD_LETTER_TOPIC", kafka_defaults.dead_letter_topic),
            consumer_group=environment.get("CONSUMER_GROUP", kafka_defaults.consumer_group),
        ),
        dispatcher=DispatcherSettings(
            worker_count=_read_positive_int(environment, "WORKER_COUNT", dispatcher_defaults.worker_count),
            lane_capacity=_read_positive_int(environment, "LANE_CAPACITY", dispatcher_defaults.lane_capacity),
            max_processing_attempts=_read_positive_int(
                environment, "MAX_PROCESSING_ATTEMPTS", dispatcher_defaults.max_processing_attempts
            ),
        ),
        log_level=log_level,
    )


def _read_positive_int(environment: Mapping[str, str], variable_name: str, default_value: int) -> int:
    raw_value = environment.get(variable_name)
    if raw_value is None:
        return default_value
    try:
        parsed_value = int(raw_value)
    except ValueError:
        raise InvalidSettingError(f"{variable_name} must be a whole number, got {raw_value!r}") from None
    if parsed_value < 1:
        raise InvalidSettingError(f"{variable_name} must be at least 1, got {parsed_value}")
    return parsed_value
