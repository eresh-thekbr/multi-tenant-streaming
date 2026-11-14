"""Command-line entry points: serve, benchmark, consume and generate-load."""
import argparse
import logging
import signal
import threading
import time

from multi_tenant_streaming.benchmark.load_generator import CampaignLoadGenerator
from multi_tenant_streaming.benchmark.strategy_comparison import (
    DISPATCHER_BUILDERS,
    BenchmarkScenario,
    DispatchStrategy,
    format_result,
    run_comparison,
)
from multi_tenant_streaming.config import TENANT_IDS, ServiceSettings, load_settings_from_environment
from multi_tenant_streaming.observability.metrics_logger import PeriodicMetricsLogger, log_tenant_summaries
from multi_tenant_streaming.observability.pipeline_metrics import PipelineMetrics
from multi_tenant_streaming.observability.report_table import format_tenant_table
from multi_tenant_streaming.pipeline_factory import (
    DispatcherBuilder,
    PipelineOptions,
    build_pipeline,
    build_tenant_fair_dispatcher,
)

logger = logging.getLogger("multi_tenant_streaming")

METRICS_LOG_INTERVAL_SECONDS = 30


def configure_logging(log_level: str) -> None:
    logging.basicConfig(level=log_level, format="%(asctime)s %(levelname)s %(name)s: %(message)s")


def request_stop_on_termination_signals(stop_requested: threading.Event) -> None:
    def handle_termination_signal(signal_number: int, current_frame: object) -> None:
        logger.info("received %s, finishing in-flight events", signal.Signals(signal_number).name)
        stop_requested.set()

    signal.signal(signal.SIGTERM, handle_termination_signal)
    signal.signal(signal.SIGINT, handle_termination_signal)


def consume_from_kafka(
    service_settings: ServiceSettings,
    dispatcher_builder: DispatcherBuilder,
    consumer_group: str,
    stop_requested: threading.Event,
    run_seconds: float | None = None,
    start_from_beginning: bool = False,
) -> PipelineMetrics:
    from multi_tenant_streaming.messaging.kafka_dead_letter_sink import KafkaDeadLetterSink
    from multi_tenant_streaming.messaging.kafka_event_source import KafkaConversationEventSource

    event_source = KafkaConversationEventSource(
        service_settings.kafka,
        consumer_group=consumer_group,
        stop_requested=stop_requested,
        run_seconds=run_seconds,
        start_from_beginning=start_from_beginning,
    )
    dead_letter_sink = KafkaDeadLetterSink(service_settings.kafka)
    pipeline = build_pipeline(
        PipelineOptions(
            dispatcher_settings=service_settings.dispatcher,
            on_event_completed=event_source.mark_completed,
            dead_letter_sink=dead_letter_sink,
        ),
        dispatcher_builder,
    )
    metrics_logger = PeriodicMetricsLogger(pipeline.metrics, TENANT_IDS, METRICS_LOG_INTERVAL_SECONDS)
    metrics_logger.start()
    try:
        pipeline.dispatcher.run(event_source.events())
    finally:
        metrics_logger.stop()
        event_source.close()  # commits the offsets of every event that finished
        dead_letter_sink.close()
    return pipeline.metrics


def run_service(arguments: argparse.Namespace) -> None:
    service_settings = load_settings_from_environment()
    configure_logging(service_settings.log_level)
    stop_requested = threading.Event()
    request_stop_on_termination_signals(stop_requested)
    logger.info("starting tenant-fair consumer with %s", service_settings)
    metrics = consume_from_kafka(
        service_settings,
        build_tenant_fair_dispatcher,
        consumer_group=service_settings.kafka.consumer_group,
        stop_requested=stop_requested,
        start_from_beginning=True,  # a new consumer group must not skip events already on the topic
    )
    log_tenant_summaries(metrics, TENANT_IDS)
    logger.info("stopped cleanly, offsets committed")


def run_benchmark(arguments: argparse.Namespace) -> None:
    configure_logging("WARNING")
    scenario = BenchmarkScenario(total_events=arguments.events, seed=arguments.seed)
    for strategy_result in run_comparison(scenario):
        print(format_result(strategy_result))


def run_kafka_benchmark(arguments: argparse.Namespace) -> None:
    service_settings = load_settings_from_environment()
    configure_logging(service_settings.log_level)
    strategy = DispatchStrategy(arguments.strategy)
    metrics = consume_from_kafka(
        service_settings,
        DISPATCHER_BUILDERS[strategy],
        consumer_group=f"{service_settings.kafka.consumer_group}-{strategy.value}",
        stop_requested=threading.Event(),
        run_seconds=arguments.seconds,
        start_from_beginning=arguments.from_beginning,
    )
    print(format_tenant_table(strategy.value, metrics.report(TENANT_IDS)))


def run_load_generator(arguments: argparse.Namespace) -> None:
    from multi_tenant_streaming.messaging.kafka_event_publisher import KafkaConversationEventPublisher

    service_settings = load_settings_from_environment()
    configure_logging(service_settings.log_level)
    publisher = KafkaConversationEventPublisher(service_settings.kafka)
    total_events = int(arguments.rate * arguments.seconds)
    seconds_between_events = 1.0 / arguments.rate
    next_send_at = time.monotonic()
    try:
        for event in CampaignLoadGenerator().generate_events(total_events):
            publisher.publish(event)
            # Pace against a fixed schedule so sleep overshoot does not lower the real rate.
            next_send_at += seconds_between_events
            time.sleep(max(0.0, next_send_at - time.monotonic()))
    finally:
        publisher.close()
    logger.info("published %d events to %s", total_events, service_settings.kafka.events_topic)


def build_argument_parser() -> argparse.ArgumentParser:
    argument_parser = argparse.ArgumentParser(prog="multi-tenant-streaming")
    subcommands = argument_parser.add_subparsers(dest="command", required=True)

    serve_command = subcommands.add_parser("serve", help="run the tenant-fair consumer until stopped")
    serve_command.set_defaults(command_handler=run_service)

    benchmark_command = subcommands.add_parser("benchmark", help="compare dispatch strategies in memory")
    benchmark_command.add_argument("--events", type=int, default=2_000)
    benchmark_command.add_argument("--seed", type=int, default=42)
    benchmark_command.set_defaults(command_handler=run_benchmark)

    consume_command = subcommands.add_parser("consume", help="benchmark one strategy against Kafka")
    consume_command.add_argument("--strategy", choices=[strategy.value for strategy in DispatchStrategy], required=True)
    consume_command.add_argument("--seconds", type=float, default=60)
    consume_command.add_argument("--from-beginning", action="store_true", help="read the topic from the start")
    consume_command.set_defaults(command_handler=run_kafka_benchmark)

    load_command = subcommands.add_parser("generate-load", help="publish simulated call events to Kafka")
    load_command.add_argument("--rate", type=float, default=500, help="events per second across all tenants")
    load_command.add_argument("--seconds", type=float, default=60)
    load_command.set_defaults(command_handler=run_load_generator)
    return argument_parser


def main() -> None:
    arguments = build_argument_parser().parse_args()
    arguments.command_handler(arguments)
