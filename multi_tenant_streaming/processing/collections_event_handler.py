"""Downstream actions applied for each conversation event."""
import time

from multi_tenant_streaming.domain.account_state import AccountStateStore, CollectionStatus
from multi_tenant_streaming.config import ProcessingCostSettings
from multi_tenant_streaming.domain.conversation_event import ConversationEvent, EventType


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
