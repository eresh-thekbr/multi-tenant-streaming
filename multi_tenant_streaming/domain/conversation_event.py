"""Conversation event model published by the AI voice agent."""
from dataclasses import asdict, dataclass
from enum import Enum
from typing import Any


class EventType(str, Enum):
    CALL_STARTED = "call.started"
    AGENT_TURN = "conversation.agent_turn"
    CUSTOMER_TURN = "conversation.customer_turn"
    PROMISE_TO_PAY = "promise_to_pay.created"
    DISPUTE_RAISED = "dispute.raised"
    CEASE_CONTACT_REQUESTED = "contact.cease_requested"
    CALL_ENDED = "call.ended"


BUSINESS_EVENT_TYPES = frozenset(
    {EventType.PROMISE_TO_PAY, EventType.DISPUTE_RAISED, EventType.CEASE_CONTACT_REQUESTED}
)


@dataclass(frozen=True)
class ConversationEvent:
    event_id: str
    tenant_id: str
    account_id: str
    conversation_id: str
    sequence_number: int  # position of this event within its conversation, starting at 1
    event_type: EventType
    transcript_text: str
    produced_at: float  # epoch seconds when the voice agent published the event

    @property
    def ordering_key(self) -> str:
        return self.account_id

    def to_dict(self) -> dict[str, Any]:
        payload = asdict(self)
        payload["event_type"] = self.event_type.value
        return payload

    @classmethod
    def from_dict(cls, payload: dict[str, Any]) -> "ConversationEvent":
        return cls(**{**payload, "event_type": EventType(payload["event_type"])})
