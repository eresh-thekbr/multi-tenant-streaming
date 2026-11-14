from tenant_fair_streaming.dispatchers.product_lane import ProductLane


def test_events_of_one_account_are_handed_out_in_arrival_order(event_factory):
    lane = ProductLane("auto_loan", capacity=10)
    first_turn = event_factory(account_id="account-1", sequence_number=1)
    second_turn = event_factory(account_id="account-1", sequence_number=2)
    lane.enqueue(first_turn)
    lane.enqueue(second_turn)

    assert lane.claim_next_event() == first_turn
    lane.release_account("account-1")
    assert lane.claim_next_event() == second_turn


def test_busy_account_is_not_handed_out_until_released(event_factory):
    lane = ProductLane("auto_loan", capacity=10)
    lane.enqueue(event_factory(account_id="account-1", sequence_number=1))
    lane.enqueue(event_factory(account_id="account-1", sequence_number=2))

    lane.claim_next_event()

    assert not lane.has_ready_account()
    lane.release_account("account-1")
    assert lane.has_ready_account()


def test_different_accounts_can_be_claimed_at_the_same_time(event_factory):
    lane = ProductLane("auto_loan", capacity=10)
    lane.enqueue(event_factory(account_id="account-1"))
    lane.enqueue(event_factory(account_id="account-2"))

    claimed_accounts = {lane.claim_next_event().account_id, lane.claim_next_event().account_id}

    assert claimed_accounts == {"account-1", "account-2"}


def test_capacity_counts_queued_events_only(event_factory):
    lane = ProductLane("auto_loan", capacity=1)
    lane.enqueue(event_factory())
    assert not lane.has_capacity()

    lane.claim_next_event()

    assert lane.has_capacity()
