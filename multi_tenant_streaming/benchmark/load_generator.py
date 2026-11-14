"""Simulates AI voice agent calls for each tenant's collections campaign."""
import random
import time
import uuid
from collections.abc import Callable, Iterator

from multi_tenant_streaming.config import TENANT_PROFILES, TenantProfile
from multi_tenant_streaming.domain.conversation_event import ConversationEvent, EventType

ScriptLine = tuple[EventType, str]

_OPENING: list[ScriptLine] = [
    (EventType.CALL_STARTED, ""),
    (EventType.AGENT_TURN, "Hi, this is an automated assistant from Acme Bank about your account."),
    (EventType.AGENT_TURN, "This is an attempt to collect a debt."),
    (EventType.AGENT_TURN, "Your payment is 30 days past due. Can you make a payment?"),
]

_OUTCOMES: dict[str, list[ScriptLine]] = {
    "promise_to_pay": [
        (EventType.CUSTOMER_TURN, "I can pay $200 on Friday."),
        (EventType.PROMISE_TO_PAY, "200.00 on Friday"),
        (EventType.AGENT_TURN, "Thank you. I have noted $200 on Friday."),
    ],
    "promise_then_dispute": [
        (EventType.CUSTOMER_TURN, "I can pay $200 on Friday."),
        (EventType.PROMISE_TO_PAY, "200.00 on Friday"),
        (EventType.AGENT_TURN, "Thank you. I have noted $200 on Friday."),
        (EventType.CUSTOMER_TURN, "Actually, I dispute this debt."),
        (EventType.DISPUTE_RAISED, "customer disputes the debt"),
        (EventType.AGENT_TURN, "Understood. I have recorded your dispute."),
    ],
    "cease_contact": [
        (EventType.CUSTOMER_TURN, "Stop calling me."),
        (EventType.CEASE_CONTACT_REQUESTED, "customer asked not to be contacted"),
        (EventType.AGENT_TURN, "Understood. We will not call you again."),
    ],
}

_CLOSING: list[ScriptLine] = [
    (EventType.AGENT_TURN, "Thank you, goodbye."),
    (EventType.CALL_ENDED, ""),
]


class CampaignLoadGenerator:
    """Interleaves events from many concurrent calls, weighted by each tenant's traffic share."""

    def __init__(
        self,
        seed: int | None = None,
        concurrent_calls_per_tenant: int = 8,
        tenant_profiles: tuple[TenantProfile, ...] = TENANT_PROFILES,
        clock: Callable[[], float] = time.time,
    ) -> None:
        self._random = random.Random(seed)
        self._concurrent_calls_per_tenant = concurrent_calls_per_tenant
        self._tenant_profiles = tenant_profiles
        self._clock = clock

    def generate_events(self, total_events: int) -> Iterator[ConversationEvent]:
        active_calls: dict[str, list[Iterator[ConversationEvent]]] = {
            tenant_profile.tenant_id: [] for tenant_profile in self._tenant_profiles
        }
        for _ in range(total_events):
            tenant_id = self._pick_tenant()
            tenant_calls = active_calls[tenant_id]
            if len(tenant_calls) < self._concurrent_calls_per_tenant:
                tenant_calls.append(self._start_call(tenant_id))
            active_call = self._random.choice(tenant_calls)
            event = next(active_call, None)
            if event is None:
                tenant_calls.remove(active_call)
                active_call = self._start_call(tenant_id)
                tenant_calls.append(active_call)
                event = next(active_call)
            yield event

    def _pick_tenant(self) -> str:
        traffic_weights = [tenant_profile.traffic_share for tenant_profile in self._tenant_profiles]
        chosen_profile = self._random.choices(self._tenant_profiles, weights=traffic_weights)[0]
        return chosen_profile.tenant_id

    def _start_call(self, tenant_id: str) -> Iterator[ConversationEvent]:
        account_id = f"{tenant_id}-{self._random.randint(100_000, 999_999)}"
        conversation_id = str(uuid.UUID(int=self._random.getrandbits(128)))
        call_outcome = self._random.choice(list(_OUTCOMES))
        call_script = _OPENING + _OUTCOMES[call_outcome] + _CLOSING
        return self._play_script(tenant_id, account_id, conversation_id, call_script)

    def _play_script(
        self, tenant_id: str, account_id: str, conversation_id: str, call_script: list[ScriptLine]
    ) -> Iterator[ConversationEvent]:
        for sequence_number, (event_type, transcript_text) in enumerate(call_script, start=1):
            yield ConversationEvent(
                event_id=str(uuid.UUID(int=self._random.getrandbits(128))),
                tenant_id=tenant_id,
                account_id=account_id,
                conversation_id=conversation_id,
                sequence_number=sequence_number,
                event_type=event_type,
                transcript_text=transcript_text,
                produced_at=self._clock(),
            )
