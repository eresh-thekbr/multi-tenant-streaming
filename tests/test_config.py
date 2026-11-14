import pytest

from multi_tenant_streaming.config import InvalidSettingError, ServiceSettings, load_settings_from_environment


def test_defaults_apply_when_nothing_is_set():
    assert load_settings_from_environment({}) == ServiceSettings()


def test_environment_overrides_defaults():
    service_settings = load_settings_from_environment(
        {
            "KAFKA_BOOTSTRAP_SERVERS": "kafka:19092",
            "EVENTS_TOPIC": "events",
            "DEAD_LETTER_TOPIC": "events.dlq",
            "CONSUMER_GROUP": "collections-consumer",
            "WORKER_COUNT": "8",
            "LANE_CAPACITY": "500",
            "MAX_PROCESSING_ATTEMPTS": "5",
            "LOG_LEVEL": "debug",
        }
    )

    assert service_settings.kafka.bootstrap_servers == "kafka:19092"
    assert service_settings.kafka.events_topic == "events"
    assert service_settings.kafka.dead_letter_topic == "events.dlq"
    assert service_settings.kafka.consumer_group == "collections-consumer"
    assert service_settings.dispatcher.worker_count == 8
    assert service_settings.dispatcher.lane_capacity == 500
    assert service_settings.dispatcher.max_processing_attempts == 5
    assert service_settings.log_level == "DEBUG"


@pytest.mark.parametrize(
    ("variable_name", "invalid_value"),
    [("WORKER_COUNT", "four"), ("WORKER_COUNT", "0"), ("LANE_CAPACITY", "-1"), ("LOG_LEVEL", "LOUD")],
)
def test_invalid_value_fails_fast_with_the_variable_name(variable_name, invalid_value):
    with pytest.raises(InvalidSettingError, match=variable_name):
        load_settings_from_environment({variable_name: invalid_value})
