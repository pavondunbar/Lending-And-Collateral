"""
shared/status.py — Status history tracking for entity lifecycle.

Each entity (Loan, CollateralPosition, MarginCall, etc.) has a companion
*StatusHistory table. Status changes are recorded as append-only rows,
and DB triggers auto-sync the parent entity's status column.
"""

import uuid

from sqlalchemy import desc, select
from sqlalchemy.orm import Session


def record_status(
    db: Session,
    model_class,
    entity_id_field: str,
    entity_id,
    status: str,
    detail: dict = None,
    actor_id: str = None,
    trace_id: str = None,
    **extra_fields,
) -> object:
    """Insert a status history row for the given entity.

    Args:
        db: SQLAlchemy session.
        model_class: The status history ORM class
            (e.g. LoanStatusHistory).
        entity_id_field: Column name linking to parent
            (e.g. 'loan_id').
        entity_id: The parent entity's primary key.
        status: New status string value.
        detail: Optional JSONB detail dict.
        actor_id: Actor identifier (defaults to contextvar).
        trace_id: Trace identifier (defaults to contextvar).
        **extra_fields: Additional columns.

    Returns:
        The created status history row.
    """
    from shared.request_context import get_actor_id, get_trace_id

    entity_uuid = (
        uuid.UUID(str(entity_id))
        if not isinstance(entity_id, uuid.UUID)
        else entity_id
    )
    resolved_actor = actor_id or get_actor_id()
    resolved_trace = trace_id or get_trace_id()

    kwargs = {
        entity_id_field: entity_uuid,
        "status": status,
        "detail": detail,
    }
    if resolved_actor:
        kwargs["actor_id"] = resolved_actor
    if resolved_trace:
        kwargs["trace_id"] = resolved_trace
    kwargs.update(extra_fields)
    row = model_class(**kwargs)
    db.add(row)
    db.flush()
    return row


def get_current_status(
    db: Session,
    model_class,
    entity_id_field: str,
    entity_id,
) -> str:
    """Query the most recent status for an entity.

    Returns the status string, or None if no history exists.
    """
    entity_uuid = (
        uuid.UUID(str(entity_id))
        if not isinstance(entity_id, uuid.UUID)
        else entity_id
    )
    col = getattr(model_class, entity_id_field)
    row = db.execute(
        select(model_class)
        .where(col == entity_uuid)
        .order_by(desc(model_class.created_at))
        .limit(1)
    ).scalar_one_or_none()
    return row.status if row else None
