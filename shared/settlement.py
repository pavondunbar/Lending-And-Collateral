"""
shared/settlement.py -- Settlement state machine.

Manages the PENDING -> APPROVED -> SIGNED -> BROADCASTED -> CONFIRMED
lifecycle for on-chain settlement transactions.
"""

import uuid
from datetime import datetime, timezone

from sqlalchemy.orm import Session

from shared.models import Settlement, SettlementStatusHistory
from shared.status import record_status


VALID_TRANSITIONS: dict[str, set[str]] = {
    "pending":     {"approved", "failed"},
    "approved":    {"signed", "failed"},
    "signed":      {"broadcasted", "failed"},
    "broadcasted": {"confirmed", "failed"},
    "confirmed":   set(),
    "failed":      {"pending"},
}


def validate_transition(
    current: str,
    target: str,
) -> bool:
    """Check if a status transition is valid."""
    allowed = VALID_TRANSITIONS.get(current, set())
    return target in allowed


def create_settlement(
    db: Session,
    related_entity_type: str,
    related_entity_id,
    operation: str,
    asset_type: str,
    quantity,
    actor_id: str = "",
    trace_id: str = "",
) -> Settlement:
    """Create a new settlement in PENDING status."""
    entity_uuid = (
        uuid.UUID(str(related_entity_id))
        if not isinstance(related_entity_id, uuid.UUID)
        else related_entity_id
    )
    settlement_ref = (
        f"STL-{uuid.uuid4().hex[:16].upper()}"
    )
    settlement = Settlement(
        settlement_ref=settlement_ref,
        related_entity_type=related_entity_type,
        related_entity_id=entity_uuid,
        operation=operation,
        asset_type=asset_type,
        quantity=quantity,
        status="pending",
    )
    db.add(settlement)
    db.flush()

    record_status(
        db,
        SettlementStatusHistory,
        "settlement_id",
        settlement.id,
        "pending",
        actor_id=actor_id,
        trace_id=trace_id,
    )

    return settlement


def transition_settlement(
    db: Session,
    settlement: Settlement,
    target_status: str,
    actor_id: str = "",
    trace_id: str = "",
    detail: dict = None,
    **updates,
) -> Settlement:
    """Transition a settlement to a new status.

    Validates the transition, updates the settlement record,
    and records status history.

    Raises ValueError for invalid transitions.
    """
    current = settlement.status
    if not validate_transition(current, target_status):
        raise ValueError(
            f"Invalid transition: {current} -> {target_status}"
        )

    settlement.status = target_status
    now = datetime.now(timezone.utc)

    if target_status == "signed" and not settlement.signed_at:
        settlement.signed_at = now
    elif target_status == "broadcasted":
        settlement.broadcasted_at = now
    elif target_status == "confirmed":
        settlement.confirmed_at = now

    for field, value in updates.items():
        if hasattr(settlement, field):
            setattr(settlement, field, value)

    db.flush()

    record_status(
        db,
        SettlementStatusHistory,
        "settlement_id",
        settlement.id,
        target_status,
        detail=detail,
        actor_id=actor_id,
        trace_id=trace_id,
    )

    return settlement
