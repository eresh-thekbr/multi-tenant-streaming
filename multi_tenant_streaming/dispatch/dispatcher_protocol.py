"""Common interface for dispatch strategies."""
from collections.abc import Callable, Iterable
from typing import Protocol

from multi_tenant_streaming.domain.conversation_event import ConversationEvent

CompletionListener = Callable[[ConversationEvent], None]


def ignore_completion(event: ConversationEvent) -> None:
    return None


class Dispatcher(Protocol):
    def run(self, events: Iterable[ConversationEvent]) -> None:
        """Process every event from the iterable and return once all are finished."""
        ...
