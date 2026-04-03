"""
shared/idempotency.py -- Idempotency layer for POST endpoints.

Reads the Idempotency-Key header, checks for existing responses,
and stores new responses for replay.
"""

from datetime import datetime, timedelta, timezone

from sqlalchemy import select, text
from sqlalchemy.orm import Session

from shared.models import IdempotencyKey


def get_idempotency_key(request) -> str:
    """Extract the Idempotency-Key header from a request.

    Raises ValueError if the header is absent.
    """
    key = request.headers.get("Idempotency-Key", "")
    if not key:
        key = request.headers.get("idempotency-key", "")
    return key


def check_idempotency(
    db: Session,
    key: str,
) -> IdempotencyKey | None:
    """Look up an existing idempotency key.

    Uses SELECT FOR UPDATE on Postgres, plain SELECT on SQLite.
    Returns the IdempotencyKey row if found, None otherwise.
    """
    dialect = db.bind.dialect.name if db.bind else "sqlite"
    if dialect == "postgresql":
        result = db.execute(
            select(IdempotencyKey)
            .where(IdempotencyKey.key == key)
            .with_for_update()
        ).scalar_one_or_none()
    else:
        result = db.execute(
            select(IdempotencyKey)
            .where(IdempotencyKey.key == key)
        ).scalar_one_or_none()
    return result


def store_idempotency_result(
    db: Session,
    key: str,
    status_code: int,
    body: dict,
) -> IdempotencyKey:
    """Store the response for an idempotency key."""
    row = IdempotencyKey(
        key=key,
        response_status=status_code,
        response_body=body,
        expires_at=(
            datetime.now(timezone.utc) + timedelta(hours=24)
        ),
    )
    db.add(row)
    db.flush()
    return row
