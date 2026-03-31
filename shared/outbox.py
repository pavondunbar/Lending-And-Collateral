"""
shared/outbox.py — Transactional outbox for reliable event publishing.

Events are written to the outbox_events table within the same DB transaction
as the business operation. A separate outbox-publisher service polls the
table and publishes to Kafka.
"""

import json
from datetime import datetime

from sqlalchemy.orm import Session

from shared.models import OutboxEvent


def _serialize_event(event) -> dict:
    """Convert a Pydantic model or dict to a JSON-serializable dict."""
    if hasattr(event, "model_dump"):
        return event.model_dump(mode="json")
    if hasattr(event, "dict"):
        data = event.dict()
        for key, val in data.items():
            if isinstance(val, datetime):
                data[key] = val.isoformat()
            elif hasattr(val, "value"):
                data[key] = val.value
        return data
    if isinstance(event, dict):
        return event
    raise TypeError(f"Cannot serialize event of type {type(event)}")


def insert_outbox_event(
    db: Session,
    aggregate_id: str,
    event_type: str,
    event,
) -> OutboxEvent:
    """Insert an event into the transactional outbox.

    Args:
        db: SQLAlchemy session (within an active transaction).
        aggregate_id: Identifier for the aggregate root
            (e.g. loan_ref).
        event_type: Kafka topic name
            (e.g. 'loan.originated').
        event: Pydantic model or dict to serialize as JSON payload.

    Returns:
        The created OutboxEvent row.
    """
    payload = _serialize_event(event)
    row = OutboxEvent(
        aggregate_id=aggregate_id,
        event_type=event_type,
        payload=payload,
    )
    db.add(row)
    db.flush()
    return row
