"""
tests/test_liquidation_engine.py -- Tests for liquidation logic.

The liquidation-engine service does not have a main.py yet, so these
tests exercise the liquidation data model and waterfall distribution
logic directly against the DB using the shared modules.

Covers:
  - LiquidationEvent creation
  - Waterfall distribution: interest -> principal -> fees -> remainder
  - Journal entries from waterfall are balanced
"""

import uuid
from decimal import Decimal

import pytest
from sqlalchemy import select

from shared.models import (
    LiquidationEvent, LiquidationStatusHistory,
    JournalEntry,
)
from shared.journal import record_journal_pair
from shared.status import record_status
from shared.outbox import insert_outbox_event
from shared.events import LiquidationExecuted
from tests.conftest import (
    make_account,
    make_loan,
    make_collateral,
    make_price_feed,
    fund_lender_pool,
    get_outbox_events,
    SYSTEM_LENDING_POOL_ID,
)


def _create_liquidation(db, loan, trigger_ltv=Decimal("90")):
    """Create a LiquidationEvent row for a loan."""
    liq_ref = f"LIQ-{uuid.uuid4().hex[:16].upper()}"
    liq = LiquidationEvent(
        liquidation_ref=liq_ref,
        loan_id=loan.id,
        trigger_ltv=trigger_ltv,
        collateral_sold_qty=Decimal("0"),
        collateral_sold_asset="PENDING",
        sale_price_usd=Decimal("0"),
        total_proceeds=Decimal("0"),
        status="initiated",
    )
    db.add(liq)
    db.flush()
    record_status(
        db, LiquidationStatusHistory, "liquidation_id",
        liq.id, "initiated",
    )
    return liq


def _execute_waterfall(
    db,
    liq,
    loan,
    total_proceeds,
    interest_owed,
    fees_owed=Decimal("0"),
):
    """Simulate waterfall distribution and record journal entries.

    Priority: interest -> principal -> fees -> borrower remainder.
    """
    lender_pool_id = str(loan.lender_pool_id)
    principal = Decimal(str(loan.principal))
    remaining = total_proceeds

    interest_recovered = min(remaining, interest_owed)
    remaining -= interest_recovered

    principal_recovered = min(remaining, principal)
    remaining -= principal_recovered

    fees_recovered = min(remaining, fees_owed)
    remaining -= fees_recovered

    borrower_remainder = remaining

    journal_ids = []

    if interest_recovered > Decimal("0"):
        jid = record_journal_pair(
            db, lender_pool_id, "LENDER_POOL",
            loan.currency, interest_recovered,
            "liquidation_interest_recovery",
            str(liq.id), lender_pool_id,
            "INTEREST_RECEIVABLE",
            f"Liquidation interest recovery {liq.liquidation_ref}",
        )
        journal_ids.append(jid)

    if principal_recovered > Decimal("0"):
        jid = record_journal_pair(
            db, lender_pool_id, "LENDER_POOL",
            loan.currency, principal_recovered,
            "liquidation_principal_recovery",
            str(liq.id), lender_pool_id,
            "LOANS_RECEIVABLE",
            f"Liquidation principal recovery {liq.liquidation_ref}",
        )
        journal_ids.append(jid)

    if fees_recovered > Decimal("0"):
        jid = record_journal_pair(
            db, lender_pool_id, "FEE_REVENUE",
            loan.currency, fees_recovered,
            "liquidation_fee_recovery",
            str(liq.id), lender_pool_id,
            "LIQUIDATION_PROCEEDS",
            f"Liquidation fee recovery {liq.liquidation_ref}",
        )
        journal_ids.append(jid)

    liq.interest_recovered = interest_recovered
    liq.principal_recovered = principal_recovered
    liq.fees_recovered = fees_recovered
    liq.borrower_remainder = borrower_remainder
    liq.total_proceeds = total_proceeds
    liq.status = "completed"
    db.flush()

    record_status(
        db, LiquidationStatusHistory, "liquidation_id",
        liq.id, "completed",
    )

    insert_outbox_event(
        db, liq.liquidation_ref, "liquidation.executed",
        LiquidationExecuted(
            service="test",
            liquidation_ref=liq.liquidation_ref,
            loan_ref=loan.loan_ref,
            proceeds=total_proceeds,
            waterfall={
                "interest": str(interest_recovered),
                "principal": str(principal_recovered),
                "fees": str(fees_recovered),
                "remainder": str(borrower_remainder),
            },
        ),
    )

    return {
        "interest": interest_recovered,
        "principal": principal_recovered,
        "fees": fees_recovered,
        "remainder": borrower_remainder,
        "journal_ids": journal_ids,
    }


class TestLiquidationEvent:

    def test_create_liquidation_event(self, db):
        pool = make_account(
            db, "System Lending Pool", "lending_pool",
            account_id=SYSTEM_LENDING_POOL_ID,
        )
        borrower = make_account(db, "Atlas Capital")
        loan = make_loan(
            db, borrower.id, pool.id,
            Decimal("50000"), status="liquidating",
        )
        liq = _create_liquidation(db, loan)
        assert liq.status == "initiated"
        assert liq.liquidation_ref.startswith("LIQ-")

    def test_liquidation_status_history(self, db):
        pool = make_account(
            db, "System Lending Pool", "lending_pool",
            account_id=SYSTEM_LENDING_POOL_ID,
        )
        borrower = make_account(db, "Atlas Capital")
        loan = make_loan(
            db, borrower.id, pool.id,
            Decimal("50000"), status="liquidating",
        )
        liq = _create_liquidation(db, loan)
        history = db.execute(
            select(LiquidationStatusHistory).where(
                LiquidationStatusHistory.liquidation_id == liq.id
            )
        ).scalars().all()
        assert len(history) >= 1
        assert history[0].status == "initiated"


class TestWaterfallDistribution:

    def test_waterfall_interest_then_principal(self, db):
        pool = make_account(
            db, "System Lending Pool", "lending_pool",
            account_id=SYSTEM_LENDING_POOL_ID,
        )
        fund_lender_pool(
            db, SYSTEM_LENDING_POOL_ID, "USD",
            Decimal("100_000_000"),
        )
        borrower = make_account(db, "Atlas Capital")
        loan = make_loan(
            db, borrower.id, pool.id,
            Decimal("50000"), status="liquidating",
        )
        liq = _create_liquidation(db, loan)
        result = _execute_waterfall(
            db, liq, loan,
            total_proceeds=Decimal("55000"),
            interest_owed=Decimal("2000"),
            fees_owed=Decimal("500"),
        )
        assert result["interest"] == Decimal("2000")
        assert result["principal"] == Decimal("50000")
        assert result["fees"] == Decimal("500")
        assert result["remainder"] == Decimal("2500")

    def test_waterfall_insufficient_proceeds(self, db):
        pool = make_account(
            db, "System Lending Pool", "lending_pool",
            account_id=SYSTEM_LENDING_POOL_ID,
        )
        fund_lender_pool(
            db, SYSTEM_LENDING_POOL_ID, "USD",
            Decimal("100_000_000"),
        )
        borrower = make_account(db, "Atlas Capital")
        loan = make_loan(
            db, borrower.id, pool.id,
            Decimal("50000"), status="liquidating",
        )
        liq = _create_liquidation(db, loan)
        result = _execute_waterfall(
            db, liq, loan,
            total_proceeds=Decimal("30000"),
            interest_owed=Decimal("5000"),
        )
        assert result["interest"] == Decimal("5000")
        assert result["principal"] == Decimal("25000")
        assert result["fees"] == Decimal("0")
        assert result["remainder"] == Decimal("0")

    def test_waterfall_journal_entries_balanced(self, db):
        pool = make_account(
            db, "System Lending Pool", "lending_pool",
            account_id=SYSTEM_LENDING_POOL_ID,
        )
        fund_lender_pool(
            db, SYSTEM_LENDING_POOL_ID, "USD",
            Decimal("100_000_000"),
        )
        borrower = make_account(db, "Atlas Capital")
        loan = make_loan(
            db, borrower.id, pool.id,
            Decimal("50000"), status="liquidating",
        )
        liq = _create_liquidation(db, loan)
        result = _execute_waterfall(
            db, liq, loan,
            total_proceeds=Decimal("55000"),
            interest_owed=Decimal("2000"),
            fees_owed=Decimal("500"),
        )
        for jid in result["journal_ids"]:
            entries = db.execute(
                select(JournalEntry).where(
                    JournalEntry.journal_id == uuid.UUID(jid)
                )
            ).scalars().all()
            total_debit = sum(e.debit for e in entries)
            total_credit = sum(e.credit for e in entries)
            assert total_debit == total_credit

    def test_waterfall_outbox_event_created(self, db):
        pool = make_account(
            db, "System Lending Pool", "lending_pool",
            account_id=SYSTEM_LENDING_POOL_ID,
        )
        fund_lender_pool(
            db, SYSTEM_LENDING_POOL_ID, "USD",
            Decimal("100_000_000"),
        )
        borrower = make_account(db, "Atlas Capital")
        loan = make_loan(
            db, borrower.id, pool.id,
            Decimal("50000"), status="liquidating",
        )
        liq = _create_liquidation(db, loan)
        _execute_waterfall(
            db, liq, loan,
            total_proceeds=Decimal("55000"),
            interest_owed=Decimal("2000"),
        )
        events = get_outbox_events(db, "liquidation.executed")
        assert len(events) >= 1
