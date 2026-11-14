"""Records which events were already applied, so redelivered events are skipped."""
import threading
from typing import Protocol


class IdempotencyStore(Protocol):
    def was_processed(self, event_id: str) -> bool: ...

    def mark_processed(self, event_id: str) -> None: ...


class InMemoryIdempotencyStore:
    """Process-local store; production would use Redis or DynamoDB with a TTL."""

    def __init__(self) -> None:
        self._processed_event_ids: set[str] = set()
        self._lock = threading.Lock()

    def was_processed(self, event_id: str) -> bool:
        with self._lock:
            return event_id in self._processed_event_ids

    def mark_processed(self, event_id: str) -> None:
        with self._lock:
            self._processed_event_ids.add(event_id)
