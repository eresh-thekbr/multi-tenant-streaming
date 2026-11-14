from collections import Counter, defaultdict

from multi_tenant_streaming.benchmark.load_generator import CampaignLoadGenerator
from multi_tenant_streaming.domain.conversation_event import ConversationEvent


def test_same_seed_generates_the_same_events():
    first_run_event_ids = [event.event_id for event in CampaignLoadGenerator(seed=1).generate_events(200)]
    second_run_event_ids = [event.event_id for event in CampaignLoadGenerator(seed=1).generate_events(200)]

    assert first_run_event_ids == second_run_event_ids


def test_each_conversation_is_emitted_in_sequence():
    sequences_by_conversation = defaultdict(list)
    for event in CampaignLoadGenerator(seed=3).generate_events(1_000):
        sequences_by_conversation[event.conversation_id].append(event.sequence_number)

    for conversation_sequences in sequences_by_conversation.values():
        assert conversation_sequences == list(range(1, len(conversation_sequences) + 1))


def test_traffic_follows_tenant_shares():
    event_count_by_tenant = Counter(event.tenant_id for event in CampaignLoadGenerator(seed=5).generate_events(5_000))

    assert 0.65 < event_count_by_tenant["credit_card"] / 5_000 < 0.75


def test_event_survives_json_round_trip():
    event = next(CampaignLoadGenerator(seed=9).generate_events(1))

    assert ConversationEvent.from_dict(event.to_dict()) == event
