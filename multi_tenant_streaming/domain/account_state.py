"""Each account's collection status, as the dialer and agents see it."""
import threading
from dataclasses import dataclass
from enum import Enum


class CollectionStatus(str, Enum):
    IN_COLLECTIONS = "in_collections"
    PROMISE_TO_PAY = "promise_to_pay"
    DISPUTED = "disputed"


@dataclass
class AccountState:
    status: CollectionStatus = CollectionStatus.IN_COLLECTIONS
    do_not_contact: bool = False


class AccountStateStore:
    """Thread-safe view of each account's collection status, read by the dialer."""

    def __init__(self) -> None:
        self._states_by_account: dict[str, AccountState] = {}
        self._lock = threading.Lock()

    def set_status(self, account_id: str, status: CollectionStatus) -> None:
        with self._lock:
            self._states_by_account.setdefault(account_id, AccountState()).status = status

    def mark_do_not_contact(self, account_id: str) -> None:
        with self._lock:
            self._states_by_account.setdefault(account_id, AccountState()).do_not_contact = True

    def get(self, account_id: str) -> AccountState:
        with self._lock:
            stored_state = self._states_by_account.get(account_id, AccountState())
            return AccountState(status=stored_state.status, do_not_contact=stored_state.do_not_contact)
