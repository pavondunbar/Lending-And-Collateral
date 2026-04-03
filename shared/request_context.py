"""
shared/request_context.py -- Per-request context via contextvars.

Stores trace_id and actor_id for the current request, consumed
automatically by record_status() and event serialization.
"""

import uuid
from contextvars import ContextVar

_trace_id: ContextVar[str] = ContextVar("trace_id", default="")
_actor_id: ContextVar[str] = ContextVar("actor_id", default="")


def set_trace_id(value: str) -> None:
    _trace_id.set(value)


def get_trace_id() -> str:
    return _trace_id.get()


def set_actor_id(value: str) -> None:
    _actor_id.set(value)


def get_actor_id() -> str:
    return _actor_id.get()


def set_context(
    trace_id: str = "",
    actor_id: str = "",
) -> None:
    """Set both trace_id and actor_id in one call."""
    if trace_id:
        _trace_id.set(trace_id)
    if actor_id:
        _actor_id.set(actor_id)


def generate_trace_id() -> str:
    """Generate a new trace ID (UUID4 hex string)."""
    return str(uuid.uuid4())
