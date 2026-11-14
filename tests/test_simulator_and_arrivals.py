from collections import Counter, defaultdict

from tenant_fair_streaming.arrivals import replay_on_schedule, schedule_campaign_burst
from tenant_fair_streaming.events import ConversationEvent
from tenant_fair_streaming.simulator import CampaignSimulator


def test_same_seed_generates_the_same_events():
    first_run = [event.event_id for event in CampaignSimulator(seed=1).generate_events(200)]
    second_run = [event.event_id for event in CampaignSimulator(seed=1).generate_events(200)]

    assert first_run == second_run


def test_each_conversation_is_emitted_in_sequence():
    sequences_by_conversation = defaultdict(list)
    for event in CampaignSimulator(seed=3).generate_events(1_000):
        sequences_by_conversation[event.conversation_id].append(event.sequence_number)

    for sequences in sequences_by_conversation.values():
        assert sequences == list(range(1, len(sequences) + 1))


def test_traffic_follows_product_shares():
    product_counts = Counter(event.product for event in CampaignSimulator(seed=5).generate_events(5_000))

    assert 0.65 < product_counts["credit_card"] / 5_000 < 0.75


def test_event_survives_json_round_trip():
    event = next(CampaignSimulator(seed=9).generate_events(1))

    assert ConversationEvent.from_dict(event.to_dict()) == event


def test_burst_product_arrives_at_once_and_others_are_spread_in_order():
    events = list(CampaignSimulator(seed=11).generate_events(300))

    scheduled_events = schedule_campaign_burst(events, burst_product="credit_card", spread_seconds=2.0)

    burst_offsets = {s.arrival_offset_seconds for s in scheduled_events if s.event.product == "credit_card"}
    auto_loan_events = [s.event for s in scheduled_events if s.event.product == "auto_loan"]
    assert burst_offsets == {0.0}
    assert auto_loan_events == [event for event in events if event.product == "auto_loan"]
    assert max(s.arrival_offset_seconds for s in scheduled_events) < 2.0


def test_replay_stamps_each_event_with_its_arrival_time():
    events = list(CampaignSimulator(seed=13).generate_events(20))
    scheduled_events = schedule_campaign_burst(events, burst_product="credit_card", spread_seconds=1.0)
    fake_now = [100.0]

    replayed = list(
        replay_on_schedule(
            scheduled_events,
            clock=lambda: fake_now[0],
            sleep=lambda seconds: fake_now.__setitem__(0, fake_now[0] + seconds),
        )
    )

    assert [event.produced_at for event in replayed] == [100.0 + s.arrival_offset_seconds for s in scheduled_events]
