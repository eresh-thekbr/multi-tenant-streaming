"""Command-line entry points: demo, produce and consume."""
import argparse
import logging
import time

from tenant_fair_streaming.campaign_simulator import CampaignSimulator
from tenant_fair_streaming.config import PRODUCT_NAMES, KafkaSettings
from tenant_fair_streaming.pipeline_factory import DispatchStrategy, PipelineOptions, build_pipeline
from tenant_fair_streaming.report_table import format_product_table
from tenant_fair_streaming.strategy_comparison import DemoScenario, format_result, run_comparison


def run_demo(arguments: argparse.Namespace) -> None:
    scenario = DemoScenario(total_events=arguments.events, seed=arguments.seed)
    for strategy_result in run_comparison(scenario):
        print(format_result(strategy_result))


def run_producer(arguments: argparse.Namespace) -> None:
    from tenant_fair_streaming.kafka_adapter.kafka_event_publisher import KafkaConversationEventPublisher

    kafka_settings = KafkaSettings()
    publisher = KafkaConversationEventPublisher(kafka_settings)
    total_events = int(arguments.rate * arguments.seconds)
    seconds_between_events = 1.0 / arguments.rate
    next_send_at = time.monotonic()
    try:
        for event in CampaignSimulator().generate_events(total_events):
            publisher.publish(event)
            # Pace against a fixed schedule so sleep overshoot does not lower the real rate.
            next_send_at += seconds_between_events
            time.sleep(max(0.0, next_send_at - time.monotonic()))
    finally:
        publisher.close()
    print(f"published {total_events} events to {kafka_settings.events_topic}")


def run_consumer(arguments: argparse.Namespace) -> None:
    from tenant_fair_streaming.kafka_adapter.kafka_dead_letter_sink import KafkaDeadLetterSink
    from tenant_fair_streaming.kafka_adapter.kafka_event_source import KafkaConversationEventSource

    strategy = DispatchStrategy(arguments.strategy)
    kafka_settings = KafkaSettings()
    event_source = KafkaConversationEventSource(
        kafka_settings,
        consumer_group=arguments.group or f"collections-{strategy.value}",
        run_seconds=arguments.seconds,
        start_from_beginning=arguments.from_beginning,
    )
    dead_letter_sink = KafkaDeadLetterSink(kafka_settings)
    pipeline = build_pipeline(
        PipelineOptions(
            strategy=strategy,
            on_event_completed=event_source.mark_completed,
            dead_letter_sink=dead_letter_sink,
        )
    )
    try:
        pipeline.dispatcher.run(event_source.events())
    finally:
        event_source.close()
        dead_letter_sink.close()
    print(format_product_table(strategy.value, pipeline.metrics.report(PRODUCT_NAMES)))


def build_argument_parser() -> argparse.ArgumentParser:
    argument_parser = argparse.ArgumentParser(prog="tenant_fair_streaming")
    subcommands = argument_parser.add_subparsers(dest="command", required=True)

    demo_command = subcommands.add_parser("demo", help="compare all strategies in memory, no Kafka needed")
    demo_command.add_argument("--events", type=int, default=2_000)
    demo_command.add_argument("--seed", type=int, default=42)
    demo_command.set_defaults(command_handler=run_demo)

    produce_command = subcommands.add_parser("produce", help="publish simulated call events to Kafka")
    produce_command.add_argument("--rate", type=float, default=500, help="events per second across all products")
    produce_command.add_argument("--seconds", type=float, default=60)
    produce_command.set_defaults(command_handler=run_producer)

    consume_command = subcommands.add_parser("consume", help="consume events from Kafka with one strategy")
    consume_command.add_argument("--strategy", choices=[strategy.value for strategy in DispatchStrategy], required=True)
    consume_command.add_argument("--seconds", type=float, default=60)
    consume_command.add_argument("--group", default=None, help="consumer group, defaults to one per strategy")
    consume_command.add_argument("--from-beginning", action="store_true", help="read the topic from the start")
    consume_command.set_defaults(command_handler=run_consumer)
    return argument_parser


def main() -> None:
    logging.basicConfig(level=logging.WARNING, format="%(asctime)s %(levelname)s %(name)s: %(message)s")
    arguments = build_argument_parser().parse_args()
    arguments.command_handler(arguments)
