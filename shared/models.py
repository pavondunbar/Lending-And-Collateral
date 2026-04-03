"""
shared/models.py — SQLAlchemy ORM models for the lending & collateral platform.
All services import from this module to keep the domain model canonical.
"""

import enum
import uuid
from decimal import Decimal

from sqlalchemy import (
    Boolean, Column, Date, DateTime, ForeignKey,
    Index, Integer, Numeric, SmallInteger, String, Text,
)
from sqlalchemy.dialects.postgresql import JSONB, UUID
from sqlalchemy.orm import DeclarativeBase
from sqlalchemy.sql import func


# ─── Enumerations ────────────────────────────────────────────────────────────

class AccountType(str, enum.Enum):
    INSTITUTIONAL = "institutional"
    CUSTODIAN     = "custodian"
    LENDING_POOL  = "lending_pool"
    SYSTEM        = "system"


class LoanStatus(str, enum.Enum):
    PENDING     = "pending"
    APPROVED    = "approved"
    ACTIVE      = "active"
    MARGIN_CALL = "margin_call"
    LIQUIDATING = "liquidating"
    REPAID      = "repaid"
    DEFAULTED   = "defaulted"
    CLOSED      = "closed"


class CollateralStatus(str, enum.Enum):
    PENDING_DEPOSIT = "pending_deposit"
    ACTIVE          = "active"
    MARGIN_HOLD     = "margin_hold"
    LIQUIDATING     = "liquidating"
    RELEASED        = "released"
    SEIZED          = "seized"


class MarginCallStatus(str, enum.Enum):
    TRIGGERED              = "triggered"
    MET                    = "met"
    EXPIRED                = "expired"
    LIQUIDATION_INITIATED  = "liquidation_initiated"


class LiquidationStatus(str, enum.Enum):
    INITIATED = "initiated"
    EXECUTING = "executing"
    COMPLETED = "completed"
    FAILED    = "failed"
    PARTIAL   = "partial"


class PriceSource(str, enum.Enum):
    COINBASE   = "coinbase"
    BINANCE    = "binance"
    KRAKEN     = "kraken"
    CHAINLINK  = "chainlink"
    INTERNAL   = "internal"
    AGGREGATED = "aggregated"


class AssetType(str, enum.Enum):
    BTC  = "BTC"
    ETH  = "ETH"
    SOL  = "SOL"
    AVAX = "AVAX"


class FiatCurrency(str, enum.Enum):
    USD = "USD"
    EUR = "EUR"
    GBP = "GBP"


class InterestMethod(str, enum.Enum):
    SIMPLE           = "simple"
    COMPOUND_DAILY   = "compound_daily"
    COMPOUND_MONTHLY = "compound_monthly"


class NormalBalance(str, enum.Enum):
    DEBIT  = "debit"
    CREDIT = "credit"


class COAAccountType(str, enum.Enum):
    ASSET     = "asset"
    LIABILITY = "liability"
    REVENUE   = "revenue"


class HoldType(str, enum.Enum):
    RESERVE = "reserve"
    RELEASE = "release"


# ─── Base ─────────────────────────────────────────────────────────────────────

class Base(DeclarativeBase):
    pass


# ─── Account ─────────────────────────────────────────────────────────────────

class Account(Base):
    __tablename__ = "accounts"

    id = Column(
        UUID(as_uuid=True), primary_key=True, default=uuid.uuid4,
    )
    entity_name = Column(String(255), nullable=False)
    account_type = Column(
        String(20), nullable=False,
    )
    legal_entity_identifier = Column(
        "lei", String(20), unique=True,
    )
    is_active = Column(Boolean, nullable=False, default=True)
    kyc_verified = Column(Boolean, nullable=False, default=False)
    aml_cleared = Column(Boolean, nullable=False, default=False)
    risk_tier = Column(SmallInteger, nullable=False, default=3)
    extra_metadata = Column(
        "metadata", JSONB, nullable=False, default=dict,
    )
    created_at = Column(
        DateTime(timezone=True), server_default=func.now(),
    )
    updated_at = Column(
        DateTime(timezone=True), onupdate=func.now(),
    )


# ─── ChartOfAccounts ─────────────────────────────────────────────────────────

class ChartOfAccounts(Base):
    __tablename__ = "chart_of_accounts"

    code = Column(String(30), primary_key=True)
    name = Column(String(255), nullable=False)
    account_type = Column(String(20), nullable=False)
    normal_balance = Column(String(10), nullable=False)


# ─── JournalEntry (append-only double-entry ledger) ──────────────────────────

class JournalEntry(Base):
    __tablename__ = "journal_entries"

    id = Column(
        UUID(as_uuid=True), primary_key=True, default=uuid.uuid4,
    )
    journal_id = Column(
        UUID(as_uuid=True), nullable=False, index=True,
    )
    account_id = Column(
        UUID(as_uuid=True),
        ForeignKey("accounts.id"),
        nullable=False,
    )
    coa_code = Column(
        String(30),
        ForeignKey("chart_of_accounts.code"),
        nullable=False,
    )
    currency = Column(String(10), nullable=False)
    debit = Column(
        Numeric(38, 18), nullable=False, default=Decimal("0"),
    )
    credit = Column(
        Numeric(38, 18), nullable=False, default=Decimal("0"),
    )
    entry_type = Column(String(50), nullable=False)
    reference_id = Column(UUID(as_uuid=True), index=True)
    narrative = Column(Text)
    created_at = Column(
        DateTime(timezone=True), server_default=func.now(),
    )


# ─── OutboxEvent (transactional outbox) ──────────────────────────────────────

class OutboxEvent(Base):
    __tablename__ = "outbox_events"

    id = Column(
        UUID(as_uuid=True), primary_key=True, default=uuid.uuid4,
    )
    aggregate_id = Column(String(255), nullable=False)
    event_type = Column(String(255), nullable=False)
    payload = Column(JSONB, nullable=False)
    created_at = Column(
        DateTime(timezone=True), server_default=func.now(),
    )
    published_at = Column(DateTime(timezone=True))


# ─── EscrowHold (append-only holds for reserved funds) ──────────────────────

class EscrowHold(Base):
    __tablename__ = "escrow_holds"

    id = Column(
        UUID(as_uuid=True), primary_key=True, default=uuid.uuid4,
    )
    hold_ref = Column(String(64), nullable=False, index=True)
    account_id = Column(
        UUID(as_uuid=True),
        ForeignKey("accounts.id"),
        nullable=False,
    )
    currency = Column(String(10), nullable=False)
    amount = Column(Numeric(38, 18), nullable=False)
    hold_type = Column(String(10), nullable=False)
    related_entity_type = Column(String(50))
    related_entity_id = Column(UUID(as_uuid=True))
    created_at = Column(
        DateTime(timezone=True), server_default=func.now(),
    )


# ─── Loan ────────────────────────────────────────────────────────────────────

class Loan(Base):
    __tablename__ = "loans"

    id = Column(
        UUID(as_uuid=True), primary_key=True, default=uuid.uuid4,
    )
    loan_ref = Column(String(64), unique=True, nullable=False)
    borrower_id = Column(
        UUID(as_uuid=True),
        ForeignKey("accounts.id"),
        nullable=False,
    )
    lender_pool_id = Column(
        UUID(as_uuid=True),
        ForeignKey("accounts.id"),
        nullable=False,
    )
    currency = Column(String(10), nullable=False, default="USD")
    principal = Column(Numeric(38, 18), nullable=False)
    interest_rate_bps = Column(Integer, nullable=False)
    initial_ltv_pct = Column(
        Numeric(8, 4), nullable=False, default=Decimal("65"),
    )
    maintenance_ltv_pct = Column(
        Numeric(8, 4), nullable=False, default=Decimal("75"),
    )
    liquidation_ltv_pct = Column(
        Numeric(8, 4), nullable=False, default=Decimal("85"),
    )
    interest_method = Column(
        String(20), nullable=False, default="compound_daily",
    )
    origination_fee_bps = Column(
        Integer, nullable=False, default=50,
    )
    status = Column(String(20), nullable=False, default="pending")
    disbursed_at = Column(DateTime(timezone=True))
    maturity_date = Column(DateTime(timezone=True))
    closed_at = Column(DateTime(timezone=True))
    created_at = Column(
        DateTime(timezone=True), server_default=func.now(),
    )
    extra_metadata = Column(
        "metadata", JSONB, nullable=False, default=dict,
    )


# ─── CollateralPosition ──────────────────────────────────────────────────────

class CollateralPosition(Base):
    __tablename__ = "collateral_positions"

    id = Column(
        UUID(as_uuid=True), primary_key=True, default=uuid.uuid4,
    )
    collateral_ref = Column(
        String(64), unique=True, nullable=False,
    )
    loan_id = Column(
        UUID(as_uuid=True),
        ForeignKey("loans.id"),
        nullable=False,
    )
    asset_type = Column(String(10), nullable=False)
    quantity = Column(Numeric(28, 8), nullable=False)
    haircut_pct = Column(
        Numeric(8, 4), nullable=False, default=Decimal("10"),
    )
    custodian_id = Column(
        UUID(as_uuid=True), ForeignKey("accounts.id"),
    )
    status = Column(
        String(20), nullable=False, default="pending_deposit",
    )
    deposited_at = Column(DateTime(timezone=True))
    released_at = Column(DateTime(timezone=True))
    created_at = Column(
        DateTime(timezone=True), server_default=func.now(),
    )


# ─── PriceFeed ───────────────────────────────────────────────────────────────

class PriceFeed(Base):
    __tablename__ = "price_feeds"

    id = Column(
        UUID(as_uuid=True), primary_key=True, default=uuid.uuid4,
    )
    asset_type = Column(String(10), nullable=False)
    price_usd = Column(Numeric(28, 8), nullable=False)
    source = Column(String(50), nullable=False)
    volume_24h = Column(Numeric(28, 8))
    is_valid = Column(Boolean, nullable=False, default=True)
    recorded_at = Column(
        DateTime(timezone=True), server_default=func.now(),
    )


# ─── MarginCall ──────────────────────────────────────────────────────────────

class MarginCall(Base):
    __tablename__ = "margin_calls"

    id = Column(
        UUID(as_uuid=True), primary_key=True, default=uuid.uuid4,
    )
    margin_call_ref = Column(
        String(64), unique=True, nullable=False,
    )
    loan_id = Column(
        UUID(as_uuid=True),
        ForeignKey("loans.id"),
        nullable=False,
    )
    triggered_ltv = Column(Numeric(8, 4), nullable=False)
    current_collateral_value = Column(
        Numeric(38, 18), nullable=False,
    )
    required_additional_collateral = Column(
        Numeric(38, 18), nullable=False,
    )
    deadline = Column(DateTime(timezone=True), nullable=False)
    status = Column(
        String(20), nullable=False, default="triggered",
    )
    met_at = Column(DateTime(timezone=True))
    expired_at = Column(DateTime(timezone=True))
    created_at = Column(
        DateTime(timezone=True), server_default=func.now(),
    )


# ─── LiquidationEvent ───────────────────────────────────────────────────────

class LiquidationEvent(Base):
    __tablename__ = "liquidation_events"

    id = Column(
        UUID(as_uuid=True), primary_key=True, default=uuid.uuid4,
    )
    liquidation_ref = Column(
        String(64), unique=True, nullable=False,
    )
    loan_id = Column(
        UUID(as_uuid=True),
        ForeignKey("loans.id"),
        nullable=False,
    )
    trigger_ltv = Column(Numeric(8, 4), nullable=False)
    collateral_sold_qty = Column(Numeric(28, 8), nullable=False)
    collateral_sold_asset = Column(String(10), nullable=False)
    sale_price_usd = Column(Numeric(28, 8), nullable=False)
    total_proceeds = Column(Numeric(38, 18), nullable=False)
    interest_recovered = Column(
        Numeric(38, 18), nullable=False, default=Decimal("0"),
    )
    principal_recovered = Column(
        Numeric(38, 18), nullable=False, default=Decimal("0"),
    )
    fees_recovered = Column(
        Numeric(38, 18), nullable=False, default=Decimal("0"),
    )
    borrower_remainder = Column(
        Numeric(38, 18), nullable=False, default=Decimal("0"),
    )
    status = Column(
        String(20), nullable=False, default="initiated",
    )
    executed_at = Column(DateTime(timezone=True))
    created_at = Column(
        DateTime(timezone=True), server_default=func.now(),
    )


# ─── InterestAccrualEvent ───────────────────────────────────────────────────

class InterestAccrualEvent(Base):
    __tablename__ = "interest_accrual_events"

    id = Column(
        UUID(as_uuid=True), primary_key=True, default=uuid.uuid4,
    )
    loan_id = Column(
        UUID(as_uuid=True),
        ForeignKey("loans.id"),
        nullable=False,
    )
    accrual_date = Column(Date, nullable=False)
    principal_balance = Column(
        Numeric(38, 18), nullable=False,
    )
    daily_rate = Column(Numeric(28, 18), nullable=False)
    accrued_amount = Column(Numeric(38, 18), nullable=False)
    cumulative_interest = Column(
        Numeric(38, 18), nullable=False,
    )
    journal_id = Column(UUID(as_uuid=True))
    created_at = Column(
        DateTime(timezone=True), server_default=func.now(),
    )


# ─── Status History Models ───────────────────────────────────────────────────

class LoanStatusHistory(Base):
    __tablename__ = "loan_status_history"

    id = Column(
        UUID(as_uuid=True), primary_key=True, default=uuid.uuid4,
    )
    loan_id = Column(
        UUID(as_uuid=True),
        ForeignKey("loans.id"),
        nullable=False,
        index=True,
    )
    status = Column(String(20), nullable=False)
    detail = Column(JSONB)
    actor_id = Column(String(255))
    trace_id = Column(String(255))
    created_at = Column(
        DateTime(timezone=True), server_default=func.now(),
    )


class CollateralStatusHistory(Base):
    __tablename__ = "collateral_status_history"

    id = Column(
        UUID(as_uuid=True), primary_key=True, default=uuid.uuid4,
    )
    collateral_id = Column(
        UUID(as_uuid=True),
        ForeignKey("collateral_positions.id"),
        nullable=False,
        index=True,
    )
    status = Column(String(20), nullable=False)
    detail = Column(JSONB)
    actor_id = Column(String(255))
    trace_id = Column(String(255))
    created_at = Column(
        DateTime(timezone=True), server_default=func.now(),
    )


class MarginCallStatusHistory(Base):
    __tablename__ = "margin_call_status_history"

    id = Column(
        UUID(as_uuid=True), primary_key=True, default=uuid.uuid4,
    )
    margin_call_id = Column(
        UUID(as_uuid=True),
        ForeignKey("margin_calls.id"),
        nullable=False,
        index=True,
    )
    status = Column(String(20), nullable=False)
    detail = Column(JSONB)
    actor_id = Column(String(255))
    trace_id = Column(String(255))
    created_at = Column(
        DateTime(timezone=True), server_default=func.now(),
    )


class LiquidationStatusHistory(Base):
    __tablename__ = "liquidation_status_history"

    id = Column(
        UUID(as_uuid=True), primary_key=True, default=uuid.uuid4,
    )
    liquidation_id = Column(
        UUID(as_uuid=True),
        ForeignKey("liquidation_events.id"),
        nullable=False,
        index=True,
    )
    status = Column(String(20), nullable=False)
    detail = Column(JSONB)
    actor_id = Column(String(255))
    trace_id = Column(String(255))
    created_at = Column(
        DateTime(timezone=True), server_default=func.now(),
    )


# ─── ComplianceEvent ────────────────────────────────────────────────────────

class ComplianceEvent(Base):
    __tablename__ = "compliance_events"

    id = Column(
        UUID(as_uuid=True), primary_key=True, default=uuid.uuid4,
    )
    entity_type = Column(String(50), nullable=False)
    entity_id = Column(UUID(as_uuid=True), nullable=False)
    event_type = Column(String(50), nullable=False)
    result = Column(String(20), nullable=False)
    score = Column(Numeric(5, 2))
    details = Column(JSONB, nullable=False, default=dict)
    checked_at = Column(
        DateTime(timezone=True), server_default=func.now(),
    )


# ─── RBAC ─────────────────────────────────────────────────────────────────────

class ApiRole(str, enum.Enum):
    ADMIN    = "admin"
    OPERATOR = "operator"
    SIGNER   = "signer"
    VIEWER   = "viewer"
    SYSTEM   = "system"


class ApiKey(Base):
    __tablename__ = "api_keys"

    id = Column(
        UUID(as_uuid=True), primary_key=True, default=uuid.uuid4,
    )
    key_hash = Column(
        String(255), unique=True, nullable=False,
    )
    role = Column(String(20), nullable=False)
    label = Column(String(255), nullable=False)
    is_active = Column(Boolean, nullable=False, default=True)
    created_at = Column(
        DateTime(timezone=True), server_default=func.now(),
    )


# ─── Idempotency ──────────────────────────────────────────────────────────────

class IdempotencyKey(Base):
    __tablename__ = "idempotency_keys"

    key = Column(String(255), primary_key=True)
    response_status = Column(Integer, nullable=False)
    response_body = Column(JSONB, nullable=False)
    created_at = Column(
        DateTime(timezone=True), server_default=func.now(),
    )
    expires_at = Column(DateTime(timezone=True))


class ProcessedEvent(Base):
    __tablename__ = "processed_events"

    event_id = Column(String(255), primary_key=True)
    topic = Column(String(255), nullable=False)
    processed_at = Column(
        DateTime(timezone=True), server_default=func.now(),
    )


# ─── Dead Letter Queue ────────────────────────────────────────────────────────

class DlqEvent(Base):
    __tablename__ = "dlq_events"

    id = Column(
        UUID(as_uuid=True), primary_key=True, default=uuid.uuid4,
    )
    original_topic = Column(String(255), nullable=False)
    event_id = Column(String(255), nullable=False)
    payload = Column(JSONB, nullable=False)
    error_message = Column(Text, nullable=False)
    retry_count = Column(Integer, nullable=False, default=0)
    first_failed_at = Column(
        DateTime(timezone=True), server_default=func.now(),
    )
    last_failed_at = Column(
        DateTime(timezone=True), server_default=func.now(),
    )
    created_at = Column(
        DateTime(timezone=True), server_default=func.now(),
    )


# ─── Settlement ───────────────────────────────────────────────────────────────

class SettlementStatus(str, enum.Enum):
    PENDING     = "pending"
    APPROVED    = "approved"
    SIGNED      = "signed"
    BROADCASTED = "broadcasted"
    CONFIRMED   = "confirmed"
    FAILED      = "failed"


class Settlement(Base):
    __tablename__ = "settlements"

    id = Column(
        UUID(as_uuid=True), primary_key=True, default=uuid.uuid4,
    )
    settlement_ref = Column(
        String(64), unique=True, nullable=False,
    )
    related_entity_type = Column(String(50), nullable=False)
    related_entity_id = Column(
        UUID(as_uuid=True), nullable=False,
    )
    operation = Column(String(50), nullable=False)
    asset_type = Column(String(10), nullable=False)
    quantity = Column(Numeric(28, 8), nullable=False)
    tx_hash = Column(String(255))
    block_number = Column(Integer)
    confirmations = Column(
        Integer, nullable=False, default=0,
    )
    required_confirmations = Column(
        Integer, nullable=False, default=6,
    )
    status = Column(
        String(20), nullable=False, default="pending",
    )
    approved_by = Column(String(255))
    signed_at = Column(DateTime(timezone=True))
    broadcasted_at = Column(DateTime(timezone=True))
    confirmed_at = Column(DateTime(timezone=True))
    created_at = Column(
        DateTime(timezone=True), server_default=func.now(),
    )


class SettlementStatusHistory(Base):
    __tablename__ = "settlement_status_history"

    id = Column(
        UUID(as_uuid=True), primary_key=True, default=uuid.uuid4,
    )
    settlement_id = Column(
        UUID(as_uuid=True),
        ForeignKey("settlements.id"),
        nullable=False,
        index=True,
    )
    status = Column(String(20), nullable=False)
    detail = Column(JSONB)
    actor_id = Column(String(255))
    trace_id = Column(String(255))
    created_at = Column(
        DateTime(timezone=True), server_default=func.now(),
    )
