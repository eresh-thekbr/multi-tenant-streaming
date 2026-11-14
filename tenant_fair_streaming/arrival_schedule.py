"""Arrival patterns: when each event reaches the platform."""
import dataclasses
import time
from collections import defaultdict
from collections.abc import Callable, Iterator
from dataclasses import dataclass

from tenant_fair_streaming.conversation_event import ConversationEvent


@dataclass(frozen=True)
class ScheduledEvent:
    arrival_offset_seconds: float
    event: ConversationEvent


def schedule_campaign_burst(
    events: list[ConversationEvent], burst_product: str, spread_seconds: float
) -> list[ScheduledEvent]:
    """The burst product's events all arrive at once; other products arrive evenly over spread_seconds."""
    events_by_product: dict[str, list[ConversationEvent]] = defaultdict(list)
    for event in events:
        events_by_product[event.product].append(event)
    scheduled_events: list[ScheduledEvent] = []
    for product, product_events in events_by_product.items():
        for position, event in enumerate(product_events):
            arrival_offset = 0.0 if product == burst_product else spread_seconds * position / len(product_events)
            scheduled_events.append(ScheduledEvent(arrival_offset, event))
    # Stable sort keeps each product's original order, so every conversation stays in sequence.
    return sorted(scheduled_events, key=lambda scheduled_event: scheduled_event.arrival_offset_seconds)


def replay_on_schedule(
    scheduled_events: list[ScheduledEvent],
    clock: Callable[[], float] = time.time,
    sleep: Callable[[float], None] = time.sleep,
) -> Iterator[ConversationEvent]:
    """Yields each event at its arrival time, stamped with that time as when it was produced."""
    start_time = clock()
    for scheduled_event in scheduled_events:
        arrival_time = start_time + scheduled_event.arrival_offset_seconds
        wait_seconds = arrival_time - clock()
        if wait_seconds > 0:
            sleep(wait_seconds)
        yield dataclasses.replace(scheduled_event.event, produced_at=arrival_time)
