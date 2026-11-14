"""Tracks which Kafka offsets are fully processed, so commits never skip unfinished events."""
import threading
from collections import defaultdict, deque


class OffsetTracker:
    """Per partition, the committable offset is the lowest offset still in progress."""

    def __init__(self) -> None:
        self._lock = threading.Lock()
        self._in_progress_offsets: dict[int, set[int]] = defaultdict(set)
        self._highest_seen_offset: dict[int, int] = {}
        self._positions_by_event_id: dict[str, deque[tuple[int, int]]] = defaultdict(deque)

    def track(self, event_id: str, partition: int, offset: int) -> None:
        with self._lock:
            self._in_progress_offsets[partition].add(offset)
            self._highest_seen_offset[partition] = max(offset, self._highest_seen_offset.get(partition, -1))
            self._positions_by_event_id[event_id].append((partition, offset))

    def mark_completed(self, event_id: str) -> None:
        with self._lock:
            positions = self._positions_by_event_id.get(event_id)
            if not positions:
                return
            partition, offset = positions.popleft()
            if not positions:
                del self._positions_by_event_id[event_id]
            self._in_progress_offsets[partition].discard(offset)

    def committable_offsets(self) -> dict[int, int]:
        # Kafka commits the offset of the next record to read, hence the +1 when nothing is in progress.
        with self._lock:
            committable: dict[int, int] = {}
            for partition, highest_seen in self._highest_seen_offset.items():
                in_progress = self._in_progress_offsets[partition]
                committable[partition] = min(in_progress) if in_progress else highest_seen + 1
            return committable
