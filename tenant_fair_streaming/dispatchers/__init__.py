"""Dispatch strategies: how events are scheduled onto workers."""
from tenant_fair_streaming.dispatchers.dispatcher_protocol import CompletionListener, Dispatcher
from tenant_fair_streaming.dispatchers.sequential_dispatcher import SequentialDispatcher
from tenant_fair_streaming.dispatchers.tenant_fair_dispatcher import TenantFairDispatcher
from tenant_fair_streaming.dispatchers.unordered_pool_dispatcher import UnorderedPoolDispatcher

__all__ = [
    "CompletionListener",
    "Dispatcher",
    "SequentialDispatcher",
    "TenantFairDispatcher",
    "UnorderedPoolDispatcher",
]
