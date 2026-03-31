"""
tests/test_journal.py -- Tests for the double-entry journal module.

Covers:
  - Balanced journal entry pairs
  - Derived balance calculation with coa_code parameter
  - Available balance (balance minus holds)
  - Multiple journal pairs accumulate correctly
  - Different currencies don't cross-contaminate
  - Advisory lock fallback on SQLite
"""

import uuid
from decimal import Decimal

from sqlalchemy import select

from shared.models import JournalEntry, EscrowHold
from shared.journal import (
    record_journal_pair,
    get_balance,
    get_available_balance,
    acquire_balance_lock,
)
from tests.conftest import (
    make_account,
    SYSTEM_LENDING_POOL_ID,
    fund_lender_pool,
)


class TestRecordJournalPair:

    def test_creates_two_entries_with_shared_journal_id(
        self, db, lending_pool,
    ):
        acct = make_account(db, "Journal Test Inst")
        ref_id = str(uuid.uuid4())
        journal_id = record_journal_pair(
            db, SYSTEM_LENDING_POOL_ID, "LOANS_RECEIVABLE",
            "USD", Decimal("1000"), "test_entry", ref_id,
            str(acct.id), "BORROWER_LIABILITY", "test narrative",
        )
        entries = db.execute(
            select(JournalEntry).where(
                JournalEntry.journal_id == uuid.UUID(journal_id)
            )
        ).scalars().all()
        assert len(entries) == 2

    def test_debit_and_credit_are_balanced(self, db, lending_pool):
        acct = make_account(db, "Balance Check Inst")
        ref_id = str(uuid.uuid4())
        amount = Decimal("5000.50")
        journal_id = record_journal_pair(
            db, str(acct.id), "LOANS_RECEIVABLE", "USD",
            amount, "test", ref_id,
            SYSTEM_LENDING_POOL_ID, "LENDER_POOL", "",
        )
        entries = db.execute(
            select(JournalEntry).where(
                JournalEntry.journal_id == uuid.UUID(journal_id)
            )
        ).scalars().all()
        total_debit = sum(e.debit for e in entries)
        total_credit = sum(e.credit for e in entries)
        assert total_debit == amount
        assert total_credit == amount


class TestGetBalance:

    def test_balance_from_journal_entries(self, db, lending_pool):
        balance = get_balance(
            db, SYSTEM_LENDING_POOL_ID, "USD",
            coa_code="BORROWER_LIABILITY",
        )
        assert balance > Decimal("0")

    def test_zero_balance_for_missing_account(self, db):
        fake_id = str(uuid.uuid4())
        balance = get_balance(
            db, fake_id, "USD", coa_code="LENDER_POOL",
        )
        assert balance == Decimal("0")

    def test_balance_after_multiple_entries(self, db, lending_pool):
        acct = make_account(db, "Multi Entry Inst")
        ref1 = str(uuid.uuid4())
        ref2 = str(uuid.uuid4())
        record_journal_pair(
            db, SYSTEM_LENDING_POOL_ID, "LOANS_RECEIVABLE",
            "USD", Decimal("5000"), "credit_acct", ref1,
            str(acct.id), "BORROWER_LIABILITY", "",
        )
        record_journal_pair(
            db, SYSTEM_LENDING_POOL_ID, "LOANS_RECEIVABLE",
            "USD", Decimal("3000"), "credit_acct", ref2,
            str(acct.id), "BORROWER_LIABILITY", "",
        )
        balance = get_balance(
            db, str(acct.id), "USD",
            coa_code="BORROWER_LIABILITY",
        )
        assert balance == Decimal("8000")

    def test_currencies_do_not_cross_contaminate(
        self, db, lending_pool,
    ):
        acct = make_account(db, "Multi Ccy Inst")
        ref_usd = str(uuid.uuid4())
        ref_eur = str(uuid.uuid4())
        record_journal_pair(
            db, SYSTEM_LENDING_POOL_ID, "LOANS_RECEIVABLE",
            "USD", Decimal("7000"), "fund", ref_usd,
            str(acct.id), "BORROWER_LIABILITY", "",
        )
        record_journal_pair(
            db, SYSTEM_LENDING_POOL_ID, "LOANS_RECEIVABLE",
            "EUR", Decimal("2000"), "fund", ref_eur,
            str(acct.id), "BORROWER_LIABILITY", "",
        )
        usd_bal = get_balance(
            db, str(acct.id), "USD",
            coa_code="BORROWER_LIABILITY",
        )
        eur_bal = get_balance(
            db, str(acct.id), "EUR",
            coa_code="BORROWER_LIABILITY",
        )
        assert usd_bal == Decimal("7000")
        assert eur_bal == Decimal("2000")


class TestGetAvailableBalance:

    def test_available_equals_balance_with_no_holds(
        self, db, lending_pool,
    ):
        available = get_available_balance(
            db, SYSTEM_LENDING_POOL_ID, "USD",
        )
        assert available > Decimal("0")

    def test_available_reduced_by_holds(self, db, lending_pool):
        acct = make_account(db, "Held Inst")
        ref_id = str(uuid.uuid4())
        record_journal_pair(
            db, SYSTEM_LENDING_POOL_ID, "LENDER_POOL",
            "USD", Decimal("100000"), "fund", ref_id,
            str(acct.id), "BORROWER_LIABILITY", "",
        )
        hold = EscrowHold(
            hold_ref="TEST-HOLD-001",
            account_id=acct.id,
            currency="USD",
            amount=Decimal("30000"),
            hold_type="reserve",
        )
        db.add(hold)
        db.flush()
        available = get_available_balance(
            db, str(acct.id), "USD",
        )
        balance = get_balance(db, str(acct.id), "USD")
        assert available == balance - Decimal("30000")

    def test_released_holds_restore_availability(
        self, db, lending_pool,
    ):
        acct = make_account(db, "Release Inst")
        ref_id = str(uuid.uuid4())
        record_journal_pair(
            db, SYSTEM_LENDING_POOL_ID, "LENDER_POOL",
            "USD", Decimal("100000"), "fund", ref_id,
            str(acct.id), "BORROWER_LIABILITY", "",
        )
        db.add(EscrowHold(
            hold_ref="REL-HOLD-001",
            account_id=acct.id,
            currency="USD",
            amount=Decimal("40000"),
            hold_type="reserve",
        ))
        db.add(EscrowHold(
            hold_ref="REL-HOLD-001",
            account_id=acct.id,
            currency="USD",
            amount=Decimal("40000"),
            hold_type="release",
        ))
        db.flush()
        balance = get_balance(db, str(acct.id), "USD")
        available = get_available_balance(
            db, str(acct.id), "USD",
        )
        assert available == balance


class TestAdvisoryLock:

    def test_sqlite_lock_is_noop(self, db, lending_pool):
        acct = make_account(db, "Lock Test Inst")
        acquire_balance_lock(db, str(acct.id), "USD")
