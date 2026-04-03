"""
services/margin-engine/main.py
-------------------------------
Margin Engine -- LTV monitoring and margin call management.

Responsibilities:
  - Evaluate loan-to-value ratios for individual and all active loans
  - Trigger margin calls when LTV breaches maintenance threshold
  - Initiate liquidation when LTV breaches liquidation threshold
  - Process margin call responses (additional collateral deposits)
  - Expire unmet margin calls and escalate to liquidation
  - Optional background polling for continuous LTV monitoring
  - Write events to the transactional outbox for reliable publishing
  - Track margin call status via append-only status history

Trust boundary: only reachable from the internal Docker network.
"""

import logging
import os
import threading
import time
import uuid
from contextlib import asynccontextmanager
from datetime import datetime, timedelta, timezone
from decimal import Decimal, ROUND_HALF_EVEN
from typing import Optional

from fastapi import Depends, FastAPI, HTTPException, Query, status
from fastapi.responses import JSONResponse
from pydantic import BaseModel, Field
from sqlalchemy import select, func
from sqlalchemy.orm import Session
from starlette.requests import Request

# ── path setup so we can import the shared package ──────────────────
import sys
sys.path.insert(0, "/app/shared")

from database import get_db_session, SessionLocal
from metrics import instrument_app
from models import (
    Loan, CollateralPosition, PriceFeed,
    InterestAccrualEvent, MarginCall, LiquidationEvent,
)
from shared.outbox import insert_outbox_event
from shared.status import record_status
from shared.models import (
    LoanStatusHistory, MarginCallStatusHistory,
    LiquidationStatusHistory,
)
from shared.idempotency import (
    get_idempotency_key, check_idempotency,
    store_idempotency_result,
)
from events import (
    MarginCallTriggered, MarginCallMet, MarginCallExpired,
    LiquidationInitiated,
)

# ────────────────────────────────────────────────────────────────────
log = logging.getLogger(__name__)
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(name)s: %(message)s",
)

SERVICE = os.environ.get("SERVICE_NAME", "margin-engine")

PRECISION = Decimal("0.000000000000000001")
ZERO = Decimal("0")

MARGIN_CALL_DEADLINE_HOURS = int(
    os.environ.get("MARGIN_CALL_DEADLINE_HOURS", "24")
)
MARGIN_POLL_ENABLED = (
    os.environ.get("MARGIN_POLL_ENABLED", "false").lower() == "true"
)
MARGIN_POLL_INTERVAL = int(
    os.environ.get("MARGIN_POLL_INTERVAL_SECONDS", "5")
)

_shutdown_event = threading.Event()


# ─── Helpers ───────────────────────────────────────────────────────

def normalize(amount: Decimal) -> Decimal:
    return amount.quantize(PRECISION, rounding=ROUND_HALF_EVEN)


def _get_latest_prices(db: Session) -> dict[str, Decimal]:
    """Get the latest price per asset type from price_feeds."""
    subq = (
        select(
            PriceFeed.asset_type,
            func.max(PriceFeed.recorded_at).label("max_ts"),
        )
        .where(PriceFeed.is_valid.is_(True))
        .group_by(PriceFeed.asset_type)
        .subquery()
    )
    rows = db.execute(
        select(PriceFeed.asset_type, PriceFeed.price_usd).join(
            subq,
            (PriceFeed.asset_type == subq.c.asset_type)
            & (PriceFeed.recorded_at == subq.c.max_ts),
        )
    ).all()
    return {
        row.asset_type: Decimal(str(row.price_usd)) for row in rows
    }


# ─── API Schemas ───────────────────────────────────────────────────

class EvaluateLoanRequest(BaseModel):
    loan_ref: str


class MarginCallRespondRequest(BaseModel):
    additional_collateral_value: Decimal = Field(gt=0)


class LtvStatusResponse(BaseModel):
    loan_ref: str
    current_ltv_pct: Decimal
    maintenance_ltv_pct: Decimal
    liquidation_ltv_pct: Decimal
    total_collateral_value: Decimal
    total_outstanding: Decimal
    status: str


# ─── Business Logic ────────────────────────────────────────────────

def _evaluate_loan_ltv(
    db: Session,
    loan: Loan,
) -> dict:
    """Evaluate LTV for a loan and trigger margin/liquidation."""
    latest_prices = _get_latest_prices(db)

    positions = db.execute(
        select(CollateralPosition).where(
            CollateralPosition.loan_id == loan.id,
            CollateralPosition.status == "active",
        )
    ).scalars().all()

    total_collateral_value = ZERO
    for pos in positions:
        price = latest_prices.get(pos.asset_type, ZERO)
        haircut = (
            Decimal(str(pos.haircut_pct)) / Decimal("100")
        )
        adjusted = (
            Decimal(str(pos.quantity))
            * price
            * (Decimal("1") - haircut)
        )
        total_collateral_value += adjusted

    accrued_interest = db.execute(
        select(
            func.coalesce(
                func.sum(InterestAccrualEvent.accrued_amount),
                ZERO,
            )
        ).where(InterestAccrualEvent.loan_id == loan.id)
    ).scalar() or ZERO

    principal = Decimal(str(loan.principal))
    outstanding = principal + accrued_interest

    if total_collateral_value <= ZERO:
        current_ltv = Decimal("999.99")
    else:
        current_ltv = normalize(
            (outstanding / total_collateral_value) * Decimal("100")
        )

    maintenance_ltv = Decimal(str(loan.maintenance_ltv_pct))
    liquidation_ltv = Decimal(str(loan.liquidation_ltv_pct))
    action = "none"

    active_margin_call = db.execute(
        select(MarginCall).where(
            MarginCall.loan_id == loan.id,
            MarginCall.status == "triggered",
        )
    ).scalar_one_or_none()

    if current_ltv > liquidation_ltv:
        action = "liquidation_initiated"
        loan.status = "liquidating"
        record_status(
            db, LoanStatusHistory, "loan_id",
            loan.id, "liquidating",
        )

        collateral_summary = {}
        for pos in positions:
            collateral_summary[pos.asset_type] = str(pos.quantity)

        liq_ref = f"LIQ-{uuid.uuid4().hex[:16].upper()}"
        liq_event = LiquidationEvent(
            liquidation_ref=liq_ref,
            loan_id=loan.id,
            trigger_ltv=current_ltv,
            collateral_sold_qty=ZERO,
            collateral_sold_asset="PENDING",
            sale_price_usd=ZERO,
            total_proceeds=ZERO,
            status="initiated",
        )
        db.add(liq_event)
        db.flush()

        record_status(
            db, LiquidationStatusHistory, "liquidation_id",
            liq_event.id, "initiated",
        )

        if active_margin_call:
            active_margin_call.status = "liquidation_initiated"
            record_status(
                db, MarginCallStatusHistory, "margin_call_id",
                active_margin_call.id, "liquidation_initiated",
            )

        insert_outbox_event(
            db, loan.loan_ref, "liquidation.initiated",
            LiquidationInitiated(
                service=SERVICE,
                liquidation_ref=liq_ref,
                loan_ref=loan.loan_ref,
                trigger_ltv=current_ltv,
                collateral_to_sell=collateral_summary,
            ),
        )

        log.info(
            "Liquidation initiated: %s LTV=%.2f%%",
            loan.loan_ref, current_ltv,
        )

        return {
            "loan_ref": loan.loan_ref,
            "current_ltv_pct": str(current_ltv),
            "current_ltv": str(current_ltv),
            "maintenance_ltv_pct": str(maintenance_ltv),
            "liquidation_ltv_pct": str(liquidation_ltv),
            "total_collateral_value": str(
                total_collateral_value
            ),
            "total_outstanding": str(outstanding),
            "status": loan.status,
            "action": action,
            "liquidation_ref": liq_ref,
            "triggered_ltv": str(current_ltv),
        }

    elif current_ltv > maintenance_ltv:
        if not active_margin_call:
            action = "margin_call_triggered"
            deadline = (
                datetime.now(timezone.utc)
                + timedelta(hours=MARGIN_CALL_DEADLINE_HOURS)
            )

            target_ltv = Decimal(str(loan.initial_ltv_pct))
            if target_ltv <= ZERO:
                target_ltv = Decimal("65")
            target_collateral = (
                outstanding / (target_ltv / Decimal("100"))
            )
            required_additional = normalize(
                target_collateral - total_collateral_value
            )
            if required_additional < ZERO:
                required_additional = ZERO

            mc_ref = f"MC-{uuid.uuid4().hex[:16].upper()}"
            margin_call = MarginCall(
                margin_call_ref=mc_ref,
                loan_id=loan.id,
                triggered_ltv=current_ltv,
                current_collateral_value=total_collateral_value,
                required_additional_collateral=required_additional,
                deadline=deadline,
                status="triggered",
            )
            db.add(margin_call)
            db.flush()

            loan.status = "margin_call"
            record_status(
                db, LoanStatusHistory, "loan_id",
                loan.id, "margin_call",
            )
            record_status(
                db, MarginCallStatusHistory, "margin_call_id",
                margin_call.id, "triggered",
            )

            insert_outbox_event(
                db, loan.loan_ref, "margin.call.triggered",
                MarginCallTriggered(
                    service=SERVICE,
                    margin_call_ref=mc_ref,
                    loan_ref=loan.loan_ref,
                    triggered_ltv=current_ltv,
                    required_additional_collateral=(
                        required_additional
                    ),
                    deadline=deadline,
                ),
            )

            log.info(
                "Margin call triggered: %s LTV=%.2f%%",
                loan.loan_ref, current_ltv,
            )
        else:
            action = "margin_call_active"

    elif active_margin_call and current_ltv <= maintenance_ltv:
        action = "margin_call_met"
        active_margin_call.status = "met"
        active_margin_call.met_at = datetime.now(timezone.utc)
        record_status(
            db, MarginCallStatusHistory, "margin_call_id",
            active_margin_call.id, "met",
        )

        loan.status = "active"
        record_status(
            db, LoanStatusHistory, "loan_id",
            loan.id, "active",
        )

        insert_outbox_event(
            db, loan.loan_ref, "margin.call.met",
            MarginCallMet(
                service=SERVICE,
                margin_call_ref=active_margin_call.margin_call_ref,
                loan_ref=loan.loan_ref,
                new_ltv=current_ltv,
            ),
        )

        log.info(
            "Margin call met: %s LTV=%.2f%%",
            loan.loan_ref, current_ltv,
        )

    margin_call_ref_out = None
    if active_margin_call:
        margin_call_ref_out = active_margin_call.margin_call_ref
    elif action == "margin_call_triggered":
        margin_call_ref_out = mc_ref

    return {
        "loan_ref": loan.loan_ref,
        "current_ltv_pct": str(current_ltv),
        "current_ltv": str(current_ltv),
        "triggered_ltv": str(current_ltv),
        "maintenance_ltv_pct": str(maintenance_ltv),
        "liquidation_ltv_pct": str(liquidation_ltv),
        "total_collateral_value": str(total_collateral_value),
        "total_outstanding": str(outstanding),
        "status": loan.status,
        "action": action,
        "margin_call_ref": margin_call_ref_out,
    }


def _check_expired_margin_calls(db: Session) -> list[dict]:
    """Find and expire overdue margin calls, initiate liquidation."""
    now = datetime.now(timezone.utc)
    expired_calls = db.execute(
        select(MarginCall).where(
            MarginCall.status == "triggered",
            MarginCall.deadline < now,
        )
    ).scalars().all()

    results = []
    for mc in expired_calls:
        mc.status = "expired"
        mc.expired_at = now
        record_status(
            db, MarginCallStatusHistory, "margin_call_id",
            mc.id, "expired",
        )

        loan = db.get(Loan, mc.loan_id)
        if not loan:
            continue

        insert_outbox_event(
            db, loan.loan_ref, "margin.call.expired",
            MarginCallExpired(
                service=SERVICE,
                margin_call_ref=mc.margin_call_ref,
                loan_ref=loan.loan_ref,
            ),
        )

        loan.status = "liquidating"
        record_status(
            db, LoanStatusHistory, "loan_id",
            loan.id, "liquidating",
        )

        positions = db.execute(
            select(CollateralPosition).where(
                CollateralPosition.loan_id == loan.id,
                CollateralPosition.status == "active",
            )
        ).scalars().all()

        collateral_summary = {}
        for pos in positions:
            collateral_summary[pos.asset_type] = str(pos.quantity)

        liq_ref = f"LIQ-{uuid.uuid4().hex[:16].upper()}"
        liq_event = LiquidationEvent(
            liquidation_ref=liq_ref,
            loan_id=loan.id,
            trigger_ltv=mc.triggered_ltv,
            collateral_sold_qty=ZERO,
            collateral_sold_asset="PENDING",
            sale_price_usd=ZERO,
            total_proceeds=ZERO,
            status="initiated",
        )
        db.add(liq_event)
        db.flush()

        record_status(
            db, LiquidationStatusHistory, "liquidation_id",
            liq_event.id, "initiated",
        )

        insert_outbox_event(
            db, loan.loan_ref, "liquidation.initiated",
            LiquidationInitiated(
                service=SERVICE,
                liquidation_ref=liq_ref,
                loan_ref=loan.loan_ref,
                trigger_ltv=mc.triggered_ltv,
                collateral_to_sell=collateral_summary,
            ),
        )

        results.append({
            "margin_call_ref": mc.margin_call_ref,
            "loan_ref": loan.loan_ref,
            "liquidation_ref": liq_ref,
        })

        log.info(
            "Margin call expired: %s -> liquidation %s",
            mc.margin_call_ref, liq_ref,
        )

    return results


# ─── Background Polling Thread ────────────────────────────────────

def _margin_poll_thread():
    """Background thread that periodically evaluates all loans."""
    log.info(
        "Margin polling started (interval=%ds)",
        MARGIN_POLL_INTERVAL,
    )
    while not _shutdown_event.is_set():
        try:
            session = SessionLocal()
            try:
                active_loans = session.execute(
                    select(Loan).where(
                        Loan.status.in_(
                            ["active", "margin_call"]
                        )
                    )
                ).scalars().all()

                for loan in active_loans:
                    _evaluate_loan_ltv(session, loan)

                _check_expired_margin_calls(session)
                session.commit()
            except Exception:
                session.rollback()
                log.exception("Error in margin polling cycle")
            finally:
                session.close()
        except Exception:
            log.exception("Error creating session for margin poll")

        _shutdown_event.wait(MARGIN_POLL_INTERVAL)

    log.info("Margin polling stopped")


# ─── FastAPI App ───────────────────────────────────────────────────

@asynccontextmanager
async def lifespan(app: FastAPI):
    log.info("Margin Engine starting up...")
    poll_thread = None
    if MARGIN_POLL_ENABLED:
        poll_thread = threading.Thread(
            target=_margin_poll_thread, daemon=True,
        )
        poll_thread.start()
    yield
    _shutdown_event.set()
    if poll_thread:
        poll_thread.join(timeout=5)
    log.info("Margin Engine shutting down.")


app = FastAPI(
    title="Margin Engine",
    version="1.0.0",
    description=(
        "LTV monitoring, margin call management, and"
        " liquidation escalation for institutional lending"
    ),
    lifespan=lifespan,
)
instrument_app(app, SERVICE)


@app.middleware("http")
async def extract_context(request, call_next):
    from shared.request_context import set_context
    trace_id = request.headers.get("X-Trace-ID", "")
    actor_id = request.headers.get("X-Actor-Id", "")
    set_context(trace_id=trace_id, actor_id=actor_id)
    response = await call_next(request)
    return response


@app.get("/health")
def health():
    return {"status": "ok", "service": SERVICE}


@app.post("/margin/evaluate")
def evaluate_loan(
    request: Request,
    req: EvaluateLoanRequest,
    db: Session = Depends(get_db_session),
):
    """Evaluate LTV for a single loan and trigger actions."""
    idem_key = get_idempotency_key(request)
    if idem_key:
        existing = check_idempotency(db, idem_key)
        if existing:
            return JSONResponse(
                status_code=existing.response_status,
                content=existing.response_body,
            )
    loan = db.execute(
        select(Loan).where(Loan.loan_ref == req.loan_ref)
    ).scalar_one_or_none()
    if not loan:
        raise HTTPException(
            status_code=404, detail="Loan not found",
        )
    if loan.status not in ("active", "margin_call"):
        raise HTTPException(
            status_code=422,
            detail=f"Cannot evaluate {loan.status} loan",
        )
    result = _evaluate_loan_ltv(db, loan)
    if idem_key:
        store_idempotency_result(db, idem_key, 200, result)
    return result


@app.post("/margin/evaluate-all")
def evaluate_all_loans(
    request: Request,
    db: Session = Depends(get_db_session),
):
    """Batch LTV evaluation of all active loans."""
    idem_key = get_idempotency_key(request)
    if idem_key:
        existing = check_idempotency(db, idem_key)
        if existing:
            return JSONResponse(
                status_code=existing.response_status,
                content=existing.response_body,
            )
    active_loans = db.execute(
        select(Loan).where(
            Loan.status.in_(["active", "margin_call"])
        )
    ).scalars().all()

    results = []
    for loan in active_loans:
        result = _evaluate_loan_ltv(db, loan)
        results.append(result)

    log.info(
        "Batch LTV evaluation completed: %d loans",
        len(results),
    )
    result = {
        "evaluated": len(results),
        "loans": results,
    }
    if idem_key:
        store_idempotency_result(db, idem_key, 200, result)
    return result


@app.get("/margin/status/{loan_ref}")
def get_ltv_status(
    loan_ref: str,
    db: Session = Depends(get_db_session),
):
    """Get current LTV status for a loan."""
    loan = db.execute(
        select(Loan).where(Loan.loan_ref == loan_ref)
    ).scalar_one_or_none()
    if not loan:
        raise HTTPException(
            status_code=404, detail="Loan not found",
        )

    latest_prices = _get_latest_prices(db)
    positions = db.execute(
        select(CollateralPosition).where(
            CollateralPosition.loan_id == loan.id,
            CollateralPosition.status == "active",
        )
    ).scalars().all()

    total_collateral_value = ZERO
    for pos in positions:
        price = latest_prices.get(pos.asset_type, ZERO)
        haircut = (
            Decimal(str(pos.haircut_pct)) / Decimal("100")
        )
        adjusted = (
            Decimal(str(pos.quantity))
            * price
            * (Decimal("1") - haircut)
        )
        total_collateral_value += adjusted

    accrued = db.execute(
        select(
            func.coalesce(
                func.sum(InterestAccrualEvent.accrued_amount),
                ZERO,
            )
        ).where(InterestAccrualEvent.loan_id == loan.id)
    ).scalar() or ZERO

    outstanding = Decimal(str(loan.principal)) + accrued

    if total_collateral_value <= ZERO:
        current_ltv = Decimal("999.99")
    else:
        current_ltv = normalize(
            (outstanding / total_collateral_value)
            * Decimal("100")
        )

    return LtvStatusResponse(
        loan_ref=loan.loan_ref,
        current_ltv_pct=current_ltv,
        maintenance_ltv_pct=Decimal(str(loan.maintenance_ltv_pct)),
        liquidation_ltv_pct=Decimal(str(loan.liquidation_ltv_pct)),
        total_collateral_value=total_collateral_value,
        total_outstanding=outstanding,
        status=loan.status,
    )


@app.get("/margin/calls")
def list_margin_calls(
    status_filter: Optional[str] = Query(
        None, alias="status",
    ),
    db: Session = Depends(get_db_session),
):
    """List active margin calls."""
    query = select(MarginCall)
    if status_filter:
        query = query.where(MarginCall.status == status_filter)
    else:
        query = query.where(MarginCall.status == "triggered")

    margin_calls = db.execute(query).scalars().all()

    results = []
    for mc in margin_calls:
        loan = db.get(Loan, mc.loan_id)
        results.append({
            "margin_call_ref": mc.margin_call_ref,
            "loan_ref": loan.loan_ref if loan else "unknown",
            "triggered_ltv": str(mc.triggered_ltv),
            "current_collateral_value": str(
                mc.current_collateral_value
            ),
            "required_additional_collateral": str(
                mc.required_additional_collateral
            ),
            "deadline": (
                mc.deadline.isoformat() if mc.deadline else None
            ),
            "status": mc.status,
            "created_at": (
                mc.created_at.isoformat()
                if mc.created_at else None
            ),
        })

    return results


@app.post("/margin/calls/{ref}/respond")
def respond_to_margin_call(
    request: Request,
    ref: str,
    req: MarginCallRespondRequest,
    db: Session = Depends(get_db_session),
):
    """Respond to a margin call by depositing more collateral.

    After additional collateral is acknowledged, re-evaluates
    the loan LTV. If LTV falls below maintenance, the margin
    call is marked as met.
    """
    idem_key = get_idempotency_key(request)
    if idem_key:
        existing = check_idempotency(db, idem_key)
        if existing:
            return JSONResponse(
                status_code=existing.response_status,
                content=existing.response_body,
            )
    margin_call = db.execute(
        select(MarginCall).where(
            MarginCall.margin_call_ref == ref,
        )
    ).scalar_one_or_none()
    if not margin_call:
        raise HTTPException(
            status_code=404, detail="Margin call not found",
        )
    if margin_call.status != "triggered":
        raise HTTPException(
            status_code=422,
            detail=(
                f"Cannot respond to {margin_call.status}"
                " margin call"
            ),
        )

    loan = db.get(Loan, margin_call.loan_id)
    if not loan:
        raise HTTPException(
            status_code=404, detail="Associated loan not found",
        )

    evaluation = _evaluate_loan_ltv(db, loan)

    result = {
        "margin_call_ref": ref,
        "evaluation": evaluation,
    }
    if idem_key:
        store_idempotency_result(db, idem_key, 200, result)
    return result


@app.post("/margin/calls/check-expired")
def check_expired_margin_calls(
    request: Request,
    db: Session = Depends(get_db_session),
):
    """Check for expired margin calls and initiate liquidation."""
    idem_key = get_idempotency_key(request)
    if idem_key:
        existing = check_idempotency(db, idem_key)
        if existing:
            return JSONResponse(
                status_code=existing.response_status,
                content=existing.response_body,
            )
    results = _check_expired_margin_calls(db)
    result = {
        "expired_count": len(results),
        "expired": results,
    }
    if idem_key:
        store_idempotency_result(db, idem_key, 200, result)
    return result


if __name__ == "__main__":
    import uvicorn
    port = int(os.environ.get("PORT", 8003))
    uvicorn.run(
        "main:app", host="0.0.0.0", port=port, log_level="info",
    )
