from multi_tenant_streaming.benchmark.arrival_schedule import replay_on_schedule, schedule_campaign_burst
from multi_tenant_streaming.benchmark.load_generator import CampaignLoadGenerator


def test_burst_tenant_arrives_at_once_and_others_are_spread_in_order():
    events = list(CampaignLoadGenerator(seed=11).generate_events(300))

    scheduled_events = schedule_campaign_burst(events, burst_tenant="credit_card", spread_seconds=2.0)

    credit_card_offsets = {
        scheduled_event.arrival_offset_seconds
        for scheduled_event in scheduled_events
        if scheduled_event.event.tenant_id == "credit_card"
    }
    scheduled_auto_loan_events = [
        scheduled_event.event for scheduled_event in scheduled_events if scheduled_event.event.tenant_id == "auto_loan"
    ]
    latest_arrival_offset = max(scheduled_event.arrival_offset_seconds for scheduled_event in scheduled_events)
    assert credit_card_offsets == {0.0}
    assert scheduled_auto_loan_events == [event for event in events if event.tenant_id == "auto_loan"]
    assert latest_arrival_offset < 2.0


def test_replay_stamps_each_event_with_its_arrival_time():
    events = list(CampaignLoadGenerator(seed=13).generate_events(20))
    scheduled_events = schedule_campaign_burst(events, burst_tenant="credit_card", spread_seconds=1.0)
    simulated_clock_seconds = [100.0]

    def read_simulated_clock() -> float:
        return simulated_clock_seconds[0]

    def advance_simulated_clock(seconds: float) -> None:
        simulated_clock_seconds[0] += seconds

    replayed_events = list(
        replay_on_schedule(scheduled_events, clock=read_simulated_clock, sleep=advance_simulated_clock)
    )

    expected_arrival_times = [100.0 + scheduled_event.arrival_offset_seconds for scheduled_event in scheduled_events]
    assert [event.produced_at for event in replayed_events] == expected_arrival_times
