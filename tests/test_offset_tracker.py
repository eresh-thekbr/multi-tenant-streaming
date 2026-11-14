from multi_tenant_streaming.messaging.offset_tracker import OffsetTracker


def test_commit_never_passes_an_unfinished_offset():
    offset_tracker = OffsetTracker()
    for offset, event_id in enumerate(["event-a", "event-b", "event-c"]):
        offset_tracker.track(event_id, partition=0, offset=offset)

    offset_tracker.mark_completed("event-a")
    offset_tracker.mark_completed("event-c")

    assert offset_tracker.committable_offsets() == {0: 1}


def test_commit_moves_past_everything_once_all_offsets_complete():
    offset_tracker = OffsetTracker()
    offset_tracker.track("event-a", partition=0, offset=5)
    offset_tracker.track("event-b", partition=0, offset=6)

    offset_tracker.mark_completed("event-b")
    offset_tracker.mark_completed("event-a")

    assert offset_tracker.committable_offsets() == {0: 7}


def test_partitions_are_tracked_independently():
    offset_tracker = OffsetTracker()
    offset_tracker.track("event-a", partition=0, offset=0)
    offset_tracker.track("event-b", partition=1, offset=0)

    offset_tracker.mark_completed("event-b")

    assert offset_tracker.committable_offsets() == {0: 0, 1: 1}


def test_redelivered_event_id_releases_each_offset_once():
    offset_tracker = OffsetTracker()
    offset_tracker.track("event-a", partition=0, offset=0)
    offset_tracker.track("event-a", partition=0, offset=1)

    offset_tracker.mark_completed("event-a")
    assert offset_tracker.committable_offsets() == {0: 1}

    offset_tracker.mark_completed("event-a")
    assert offset_tracker.committable_offsets() == {0: 2}
