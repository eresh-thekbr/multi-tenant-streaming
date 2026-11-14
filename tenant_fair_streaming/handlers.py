"""Downstream actions applied for each conversation event."""
import threading
import time
from dataclasses import dataclass
from enum import Enum

from tenant_fair_streaming.config import ProcessingCostSettings
from tenant_fair_streaming.events import ConversationEvent, EventType


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
        self._states: dict[str, AccountState] = {}
        self._lock = threading.Lock()

    def set_status(self, account_id: str, status: CollectionStatus) -> None:
        with self._lock:
            self._states.setdefault(account_id, AccountState()).status = status

    def mark_do_not_contact(self, account_id: str) -> None:
        with self._lock:
            self._states.setdefault(account_id, AccountState()).do_not_contact = True

    def get(self, account_id: str) -> AccountState:
        with self._lock:
            state = self._states.get(account_id, AccountState())
            return AccountState(status=state.status, do_not_contact=state.do_not_contact)


class CollectionsEventHandler:
    """Runs AI analysis, compliance guard and account updates for one event."""

    def __init__(self, account_states: AccountStateStore, processing_costs: ProcessingCostSettings) -> None:
        self._account_states = account_states
        self._processing_costs = processing_costs

    def handle(self, event: ConversationEvent) -> None:
        # Sleep stands in for the downstream calls (LLM analysis, CRM update).
        time.sleep(self._processing_costs.seconds_for(event.event_type))
        if event.event_type is EventType.PROMISE_TO_PAY:
            self._account_states.set_status(event.account_id, CollectionStatus.PROMISE_TO_PAY)
        elif event.event_type is EventType.DISPUTE_RAISED:
            self._account_states.set_status(event.account_id, CollectionStatus.DISPUTED)
        elif event.event_type is EventType.CEASE_CONTACT_REQUESTED:
            self._account_states.mark_do_not_contact(event.account_id)
