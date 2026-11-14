"""One product's bounded queue, kept as an ordered queue per account."""
from collections import deque

from tenant_fair_streaming.events import ConversationEvent


class ProductLane:
    """Hands out the next account that has queued events and is not already being processed."""

    # Not thread-safe on its own: the dispatcher calls it while holding its lock.

    def __init__(self, product: str, capacity: int) -> None:
        self.product = product
        self._capacity = capacity
        self._queued_event_count = 0
        self._events_by_account: dict[str, deque[ConversationEvent]] = {}
        self._ready_accounts: deque[str] = deque()
        self._busy_accounts: set[str] = set()

    @property
    def queued_event_count(self) -> int:
        return self._queued_event_count

    def has_capacity(self) -> bool:
        return self._queued_event_count < self._capacity

    def has_ready_account(self) -> bool:
        return bool(self._ready_accounts)

    def enqueue(self, event: ConversationEvent) -> None:
        account_queue = self._events_by_account.setdefault(event.ordering_key, deque())
        account_was_idle = not account_queue and event.ordering_key not in self._busy_accounts
        account_queue.append(event)
        self._queued_event_count += 1
        if account_was_idle:
            self._ready_accounts.append(event.ordering_key)

    def claim_next_event(self) -> ConversationEvent:
        account_id = self._ready_accounts.popleft()
        self._busy_accounts.add(account_id)
        self._queued_event_count -= 1
        return self._events_by_account[account_id].popleft()

    def release_account(self, account_id: str) -> None:
        self._busy_accounts.discard(account_id)
        if self._events_by_account.get(account_id):
            self._ready_accounts.append(account_id)
        else:
            self._events_by_account.pop(account_id, None)
