"""Destinations for events that cannot be processed."""
import threading
from dataclasses import dataclass
from typing import Protocol

from tenant_fair_streaming.conversation_event import ConversationEvent


@dataclass(frozen=True)
class DeadLetter:
    event: ConversationEvent
    reason: str


class DeadLetterSink(Protocol):
    def publish(self, event: ConversationEvent, reason: str) -> None: ...


class InMemoryDeadLetterSink:
    def __init__(self) -> None:
        self._dead_letters: list[DeadLetter] = []
        self._lock = threading.Lock()

    def publish(self, event: ConversationEvent, reason: str) -> None:
        with self._lock:
            self._dead_letters.append(DeadLetter(event=event, reason=reason))

    @property
    def dead_letters(self) -> list[DeadLetter]:
        with self._lock:
            return list(self._dead_letters)
