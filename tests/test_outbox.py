"""
tests/test_outbox.py -- Tests for the transactional outbox module.

Covers:
  - Event insertion with proper fields
  - Pydantic model serialization (model_dump)
  - Dict event serialization
  - Aggregate ID assignment
  - published_at starts as NULL
"""

from decimal import Decimal

from sqlalchemy import select

from shared.models import OutboxEvent
from shared.outbox import insert_outbox_event
from shared.events import LoanOriginated
from tests.conftest import make_account


class TestInsertOutboxEvent:

    def test_inserts_event_row(self, db):
        event = insert_outbox_event(
            db, "AGG-001", "loan.originated",
            {"account_id": "abc", "amount": "1000"},
        )
        assert event.id is not None
        assert event.aggregate_id == "AGG-001"
        assert event.event_type == "loan.originated"

    def test_payload_is_serialized_dict(self, db):
        payload = {"key": "value", "nested": {"a": 1}}
        event = insert_outbox_event(
            db, "AGG-002", "test.event", payload,
        )
        assert event.payload == payload

    def test_published_at_is_null(self, db):
        event = insert_outbox_event(
            db, "AGG-003", "test.unpublished", {"data": True},
        )
        assert event.published_at is None

    def test_multiple_events_for_same_aggregate(self, db):
        insert_outbox_event(
            db, "MULTI-AGG", "event.one", {"step": 1},
        )
        insert_outbox_event(
            db, "MULTI-AGG", "event.two", {"step": 2},
        )
        events = db.execute(
            select(OutboxEvent).where(
                OutboxEvent.aggregate_id == "MULTI-AGG"
            )
        ).scalars().all()
        assert len(events) == 2

    def test_pydantic_model_serialization(self, db):

        class FakeEvent:
            def model_dump(self, mode=None):
                return {"field": "value", "amount": "100"}

        event = insert_outbox_event(
            db, "PYDANTIC-001", "test.pydantic", FakeEvent(),
        )
        assert event.payload == {
            "field": "value", "amount": "100",
        }

    def test_legacy_dict_method_serialization(self, db):

        class LegacyEvent:
            def dict(self):
                return {"legacy": True, "count": 42}

        event = insert_outbox_event(
            db, "LEGACY-001", "test.legacy", LegacyEvent(),
        )
        assert event.payload["legacy"] is True

    def test_real_pydantic_event_serialization(self, db):
        loan_event = LoanOriginated(
            service="test",
            loan_ref="LOAN-TEST001",
            borrower_id="borrower-123",
            lender_pool_id="pool-456",
            principal=Decimal("50000"),
            currency="USD",
            interest_rate_bps=800,
            initial_ltv_pct=Decimal("60"),
            collateral_summary={"BTC": "1.5"},
        )
        event = insert_outbox_event(
            db, "LOAN-TEST001", "loan.originated", loan_event,
        )
        assert event.payload["loan_ref"] == "LOAN-TEST001"
        assert event.payload["service"] == "test"
