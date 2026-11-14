import itertools
import time

import pytest

from multi_tenant_streaming.domain.conversation_event import ConversationEvent, EventType

_event_counter = itertools.count(1)


def make_event(
    tenant_id: str = "auto_loan",
    account_id: str = "account-1",
    conversation_id: str = "conversation-1",
    sequence_number: int = 1,
    event_type: EventType = EventType.AGENT_TURN,
    event_id: str | None = None,
) -> ConversationEvent:
    return ConversationEvent(
        event_id=event_id or f"event-{next(_event_counter)}",
        tenant_id=tenant_id,
        account_id=account_id,
        conversation_id=conversation_id,
        sequence_number=sequence_number,
        event_type=event_type,
        transcript_text="",
        produced_at=time.time(),
    )


@pytest.fixture
def event_factory():
    return make_event
