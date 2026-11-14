from tenant_fair_streaming.kafka_adapter.offset_tracker import OffsetTracker


def test_commit_never_passes_an_unfinished_offset():
    tracker = OffsetTracker()
    for offset, event_id in enumerate(["event-a", "event-b", "event-c"]):
        tracker.track(event_id, partition=0, offset=offset)

    tracker.mark_completed("event-a")
    tracker.mark_completed("event-c")

    assert tracker.committable_offsets() == {0: 1}


def test_commit_moves_past_everything_once_all_offsets_complete():
    tracker = OffsetTracker()
    tracker.track("event-a", partition=0, offset=5)
    tracker.track("event-b", partition=0, offset=6)

    tracker.mark_completed("event-b")
    tracker.mark_completed("event-a")

    assert tracker.committable_offsets() == {0: 7}


def test_partitions_are_tracked_independently():
    tracker = OffsetTracker()
    tracker.track("event-a", partition=0, offset=0)
    tracker.track("event-b", partition=1, offset=0)

    tracker.mark_completed("event-b")

    assert tracker.committable_offsets() == {0: 0, 1: 1}


def test_redelivered_event_id_releases_each_offset_once():
    tracker = OffsetTracker()
    tracker.track("event-a", partition=0, offset=0)
    tracker.track("event-a", partition=0, offset=1)

    tracker.mark_completed("event-a")
    assert tracker.committable_offsets() == {0: 1}

    tracker.mark_completed("event-a")
    assert tracker.committable_offsets() == {0: 2}
