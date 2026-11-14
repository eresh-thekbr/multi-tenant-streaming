"""Dispatch strategies: how events are scheduled onto workers."""
from tenant_fair_streaming.dispatchers.base import CompletionListener, Dispatcher
from tenant_fair_streaming.dispatchers.sequential import SequentialDispatcher
from tenant_fair_streaming.dispatchers.tenant_fair import TenantFairDispatcher
from tenant_fair_streaming.dispatchers.unordered_pool import UnorderedPoolDispatcher

__all__ = [
    "CompletionListener",
    "Dispatcher",
    "SequentialDispatcher",
    "TenantFairDispatcher",
    "UnorderedPoolDispatcher",
]
