"""
shared/journal.py — Double-entry journal operations for the immutable ledger.

Provides functions to record balanced debit/credit pairs, derive balances
from journal entries (no mutable balance columns), and acquire advisory
locks to prevent double-spend.
"""

import hashlib
import uuid
from decimal import Decimal

from sqlalchemy import func, select, text
from sqlalchemy.orm import Session

from shared.models import EscrowHold, JournalEntry

ZERO = Decimal("0")


def record_journal_pair(
    db: Session,
    account_id: str,
    coa_code: str,
    currency: str,
    amount: Decimal,
    entry_type: str,
    reference_id: str,
    counter_account_id: str,
    counter_coa_code: str,
    narrative: str = "",
) -> str:
    """Insert a balanced debit+credit journal entry pair.

    Returns the shared journal_id linking both entries.
    """
    journal_id = uuid.uuid4()
    acct_uuid = (
        uuid.UUID(account_id)
        if isinstance(account_id, str) else account_id
    )
    counter_uuid = (
        uuid.UUID(counter_account_id)
        if isinstance(counter_account_id, str)
        else counter_account_id
    )
    ref_uuid = (
        uuid.UUID(reference_id)
        if isinstance(reference_id, str) else reference_id
    )

    debit_entry = JournalEntry(
        journal_id=journal_id,
        account_id=acct_uuid,
        coa_code=coa_code,
        currency=currency,
        debit=amount,
        credit=ZERO,
        entry_type=entry_type,
        reference_id=ref_uuid,
        narrative=narrative,
    )
    credit_entry = JournalEntry(
        journal_id=journal_id,
        account_id=counter_uuid,
        coa_code=counter_coa_code,
        currency=currency,
        debit=ZERO,
        credit=amount,
        entry_type=entry_type,
        reference_id=ref_uuid,
        narrative=narrative,
    )
    db.add(debit_entry)
    db.add(credit_entry)
    db.flush()
    return str(journal_id)


def get_balance(
    db: Session,
    account_id: str,
    currency: str,
    coa_code: str = "LENDER_POOL",
) -> Decimal:
    """Derive balance from journal entries: SUM(credit) - SUM(debit).

    Scoped to the specified COA code (default LENDER_POOL).
    """
    acct_uuid = (
        uuid.UUID(account_id)
        if isinstance(account_id, str) else account_id
    )
    result = db.execute(
        select(
            func.coalesce(func.sum(JournalEntry.credit), ZERO)
            - func.coalesce(func.sum(JournalEntry.debit), ZERO)
        ).where(
            JournalEntry.account_id == acct_uuid,
            JournalEntry.currency == currency,
            JournalEntry.coa_code == coa_code,
        )
    ).scalar()
    return result if result is not None else ZERO


def get_available_balance(
    db: Session, account_id: str, currency: str,
) -> Decimal:
    """Balance minus active (unreleased) holds."""
    balance = get_balance(db, account_id, currency)
    acct_uuid = (
        uuid.UUID(account_id)
        if isinstance(account_id, str) else account_id
    )

    held = db.execute(
        select(
            func.coalesce(func.sum(EscrowHold.amount), ZERO)
        ).where(
            EscrowHold.account_id == acct_uuid,
            EscrowHold.currency == currency,
            EscrowHold.hold_type == "reserve",
        )
    ).scalar() or ZERO

    released = db.execute(
        select(
            func.coalesce(func.sum(EscrowHold.amount), ZERO)
        ).where(
            EscrowHold.account_id == acct_uuid,
            EscrowHold.currency == currency,
            EscrowHold.hold_type == "release",
        )
    ).scalar() or ZERO

    active_holds = held - released
    return balance - active_holds


def acquire_balance_lock(
    db: Session, account_id: str, currency: str,
) -> None:
    """Acquire a PostgreSQL advisory lock to prevent double-spend.

    Uses pg_advisory_xact_lock with a deterministic hash of
    account+currency. Falls back to no-op on SQLite (used in tests).
    """
    dialect = db.bind.dialect.name if db.bind else "sqlite"
    if dialect == "sqlite":
        return

    lock_key = int(
        hashlib.sha256(
            f"{account_id}:{currency}".encode()
        ).hexdigest()[:15],
        16,
    )
    db.execute(
        text("SELECT pg_advisory_xact_lock(:key)"),
        {"key": lock_key},
    )
