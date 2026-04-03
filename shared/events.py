"""
shared/events.py -- Canonical Pydantic schemas for all Kafka events.

Every service uses these schemas to publish and consume messages, ensuring
a shared contract across the platform.
"""

from __future__ import annotations

import uuid
from datetime import datetime
from decimal import Decimal
from typing import Optional

from pydantic import BaseModel, Field


def _now() -> datetime:
    from datetime import timezone
    return datetime.now(timezone.utc)


def _uuid() -> str:
    return str(uuid.uuid4())


# ── Base ──────────────────────────────────────────────────────────

class BaseEvent(BaseModel):
    event_id:   str      = Field(default_factory=_uuid)
    event_time: datetime = Field(default_factory=_now)
    service:    str      = "unknown"
    actor_id:   str      = ""
    trace_id:   str      = ""

    class Config:
        json_encoders = {
            Decimal: str,
            datetime: lambda v: v.isoformat(),
        }


# ── Loan Events ──────────────────────────────────────────────────

class LoanOriginated(BaseEvent):
    loan_ref:            str
    borrower_id:         str
    lender_pool_id:      str
    principal:           Decimal
    currency:            str
    interest_rate_bps:   int
    initial_ltv_pct:     Optional[Decimal] = None
    collateral_summary:  Optional[dict] = None


class LoanDisbursed(BaseEvent):
    loan_ref:     str
    borrower_id:  Optional[str] = None
    principal:    Optional[Decimal] = None
    amount:       Optional[Decimal] = None
    currency:     str
    disbursed_at: Optional[datetime] = Field(
        default_factory=_now,
    )


class LoanRepaymentReceived(BaseEvent):
    loan_ref:           str
    amount:             Decimal
    currency:           str
    interest_portion:   Decimal
    principal_portion:  Decimal
    remaining_balance:  Optional[Decimal] = None


class LoanRepaymentCompleted(BaseEvent):
    loan_ref:     str
    total_repaid: Decimal
    currency:     str


class LoanClosed(BaseEvent):
    loan_ref:  str
    closed_at: Optional[datetime] = Field(
        default_factory=_now,
    )


class InterestAccrued(BaseEvent):
    loan_ref:            str
    accrual_date:        Optional[str] = None
    principal_balance:   Optional[Decimal] = None
    daily_rate:          Optional[Decimal] = None
    accrued_amount:      Optional[Decimal] = None
    cumulative_interest: Optional[Decimal] = None


# ── Collateral Events ────────────────────────────────────────────

class CollateralDeposited(BaseEvent):
    collateral_ref:      str
    loan_ref:            str
    asset_type:          str
    quantity:            Decimal
    haircut_pct:         Optional[Decimal] = None
    estimated_value_usd: Optional[Decimal] = None


class CollateralWithdrawn(BaseEvent):
    collateral_ref:      str
    loan_ref:            str
    asset_type:          Optional[str] = None
    quantity:            Optional[Decimal] = None
    quantity_withdrawn:  Optional[Decimal] = None
    remaining_quantity:  Optional[Decimal] = None
    tx_hash:             Optional[str] = None


class CollateralSubstituted(BaseEvent):
    old_ref:              Optional[str] = None
    new_ref:              Optional[str] = None
    removed_ref:          Optional[str] = None
    added_ref:            Optional[str] = None
    loan_ref:             str
    removed_asset:        Optional[str] = None
    removed_qty:          Optional[Decimal] = None
    added_asset:          Optional[str] = None
    added_asset_type:     Optional[str] = None
    added_qty:            Optional[Decimal] = None
    added_quantity:       Optional[Decimal] = None
    post_substitution_ltv: Optional[Decimal] = None


class CollateralValued(BaseEvent):
    loan_ref:                   str
    total_collateral_value_usd: Decimal
    current_ltv:                Decimal


# Alias used by collateral-manager service.
CollateralValuationComputed = CollateralValued


# ── Margin Events ────────────────────────────────────────────────

class MarginCallTriggered(BaseEvent):
    margin_call_ref:                str
    loan_ref:                       str
    triggered_ltv:                  Decimal
    required_additional_collateral: Decimal
    deadline:                       datetime


class MarginCallMet(BaseEvent):
    margin_call_ref: str
    loan_ref:        str
    new_ltv:         Decimal


class MarginCallExpired(BaseEvent):
    margin_call_ref: str
    loan_ref:        str


# ── Liquidation Events ───────────────────────────────────────────

class LiquidationInitiated(BaseEvent):
    liquidation_ref:    str
    loan_ref:           str
    trigger_ltv:        Decimal
    collateral_to_sell: dict


class LiquidationExecuted(BaseEvent):
    liquidation_ref: str
    loan_ref:        str
    proceeds:        Decimal
    waterfall:       dict


class LiquidationCompleted(BaseEvent):
    liquidation_ref: str
    loan_ref:        str
    total_recovered: Decimal


# ── Price Events ─────────────────────────────────────────────────

class PriceFeedUpdated(BaseEvent):
    asset_type: str
    price_usd:  Decimal
    source:     str
    volume_24h: Optional[Decimal] = None


class PriceFeedReceived(BaseEvent):
    asset_type: str
    price_usd:  Decimal
    source:     str
    volume_24h: Optional[Decimal] = None


class PriceAggregated(BaseEvent):
    asset_type:    str
    vwap_price_usd: Decimal
    source_count:  int


# ── Compliance / Audit Events ────────────────────────────────────

class ComplianceEvent(BaseEvent):
    entity_type: str
    entity_id:   str
    event_type:  str
    result:      str
    score:       Optional[Decimal] = None
    details:     dict = Field(default_factory=dict)


class AuditTrailEntry(BaseEvent):
    actor_service: str
    action:        str
    entity_type:   str
    entity_id:     str
    before_state:  Optional[dict] = None
    after_state:   Optional[dict] = None
    ip_address:    Optional[str]  = None


# ── Settlement Events ──────────────────────────────────────────────

class SettlementCreated(BaseEvent):
    settlement_ref:       str
    related_entity_type:  str
    related_entity_id:    str
    operation:            str
    asset_type:           str
    quantity:             Decimal


class SettlementApproved(BaseEvent):
    settlement_ref: str
    approved_by:    str


class SettlementSigned(BaseEvent):
    settlement_ref: str
    tx_hash:        str


class SettlementBroadcasted(BaseEvent):
    settlement_ref: str
    tx_hash:        str
    block_number:   Optional[int] = None


class SettlementConfirmed(BaseEvent):
    settlement_ref:  str
    tx_hash:         str
    block_number:    int
    confirmations:   int
