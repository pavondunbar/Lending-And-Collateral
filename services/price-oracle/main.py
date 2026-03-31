"""
services/price-oracle/main.py
------------------------------
Price Oracle -- Ingest, aggregate, and serve asset price feeds.

Responsibilities:
  - Accept price feeds from external sources
  - Compute VWAP (volume-weighted average price) aggregates
  - Serve latest and historical prices per asset type
  - Simulate price feeds for demo/development environments
  - Write events to the transactional outbox for reliable publishing

Trust boundary: only reachable from the internal Docker network.
"""

import logging
import os
import random
import threading
import time
import uuid
from contextlib import asynccontextmanager
from datetime import datetime, timezone
from decimal import Decimal, ROUND_HALF_EVEN
from typing import Optional

from fastapi import Depends, FastAPI, HTTPException, Query, status
from pydantic import BaseModel, Field
from sqlalchemy import select, func, desc
from sqlalchemy.orm import Session

# ── path setup so we can import the shared package ──────────────────
import sys
sys.path.insert(0, "/app/shared")

from database import get_db_session, SessionLocal
from metrics import instrument_app
from models import PriceFeed
from shared.outbox import insert_outbox_event
from events import PriceFeedReceived, PriceAggregated

# ────────────────────────────────────────────────────────────────────
log = logging.getLogger(__name__)
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(name)s: %(message)s",
)

SERVICE = os.environ.get("SERVICE_NAME", "price-oracle")

PRECISION = Decimal("0.000000000000000001")
ZERO = Decimal("0")

DEMO_MODE = os.environ.get("DEMO_PRICE_FEEDS", "true").lower() == "true"
DEMO_INTERVAL_SECONDS = int(
    os.environ.get("DEMO_INTERVAL_SECONDS", "10")
)

DEMO_ASSETS = {
    "BTC":  {"base": 62500, "spread": 2500},
    "ETH":  {"base": 3250,  "spread": 250},
    "SOL":  {"base": 125,   "spread": 25},
    "AVAX": {"base": 30,    "spread": 5},
}

_shutdown_event = threading.Event()


# ─── Helpers ───────────────────────────────────────────────────────

def normalize(amount: Decimal) -> Decimal:
    return amount.quantize(PRECISION, rounding=ROUND_HALF_EVEN)


# ─── API Schemas ───────────────────────────────────────────────────

class IngestPriceRequest(BaseModel):
    asset_type: str
    price_usd: Decimal = Field(gt=0)
    source: str
    volume_24h: Optional[Decimal] = None


class AggregatePriceRequest(BaseModel):
    asset_type: str
    lookback_minutes: int = 60
    max_entries: int = 100


class PriceResponse(BaseModel):
    asset_type: str
    price_usd: Decimal
    source: str
    volume_24h: Optional[Decimal] = None
    recorded_at: Optional[datetime] = None


# ─── Business Logic ────────────────────────────────────────────────

def _ingest_price(
    db: Session,
    req: IngestPriceRequest,
) -> PriceFeed:
    feed = PriceFeed(
        asset_type=req.asset_type.upper(),
        price_usd=req.price_usd,
        source=req.source,
        volume_24h=req.volume_24h,
        is_valid=True,
    )
    db.add(feed)
    db.flush()

    insert_outbox_event(
        db, str(feed.id), "price.feed.received",
        PriceFeedReceived(
            service=SERVICE,
            asset_type=req.asset_type.upper(),
            price_usd=req.price_usd,
            source=req.source,
        ),
    )

    log.info(
        "Price ingested: %s $%s source=%s",
        req.asset_type, req.price_usd, req.source,
    )
    return feed


def _aggregate_vwap(
    db: Session,
    asset_type: str,
    lookback_minutes: int,
    max_entries: int,
) -> PriceFeed:
    """Compute VWAP from recent feeds and store as aggregated."""
    cutoff = datetime.now(timezone.utc)

    recent_feeds = db.execute(
        select(PriceFeed)
        .where(
            PriceFeed.asset_type == asset_type.upper(),
            PriceFeed.is_valid.is_(True),
            PriceFeed.source != "aggregated",
        )
        .order_by(desc(PriceFeed.recorded_at))
        .limit(max_entries)
    ).scalars().all()

    if not recent_feeds:
        raise HTTPException(
            status_code=404,
            detail=f"No recent price feeds for {asset_type}",
        )

    has_volume = any(
        f.volume_24h is not None and f.volume_24h > 0
        for f in recent_feeds
    )

    if has_volume:
        weighted_sum = ZERO
        total_volume = ZERO
        for feed in recent_feeds:
            vol = (
                Decimal(str(feed.volume_24h))
                if feed.volume_24h else ZERO
            )
            if vol > ZERO:
                weighted_sum += (
                    Decimal(str(feed.price_usd)) * vol
                )
                total_volume += vol

        if total_volume > ZERO:
            vwap = normalize(weighted_sum / total_volume)
        else:
            prices = [
                Decimal(str(f.price_usd)) for f in recent_feeds
            ]
            vwap = normalize(sum(prices) / len(prices))
    else:
        prices = [
            Decimal(str(f.price_usd)) for f in recent_feeds
        ]
        vwap = normalize(sum(prices) / len(prices))

    aggregated = PriceFeed(
        asset_type=asset_type.upper(),
        price_usd=vwap,
        source="aggregated",
        is_valid=True,
    )
    db.add(aggregated)
    db.flush()

    insert_outbox_event(
        db, str(aggregated.id), "price.aggregated",
        PriceAggregated(
            service=SERVICE,
            asset_type=asset_type.upper(),
            vwap_price_usd=vwap,
            source_count=len(recent_feeds),
        ),
    )

    log.info(
        "VWAP aggregated: %s $%s from %d feeds",
        asset_type, vwap, len(recent_feeds),
    )
    return aggregated


def _get_latest_price(
    db: Session, asset_type: str,
) -> Optional[PriceFeed]:
    """Get the most recent valid price for an asset."""
    return db.execute(
        select(PriceFeed)
        .where(
            PriceFeed.asset_type == asset_type.upper(),
            PriceFeed.is_valid.is_(True),
        )
        .order_by(desc(PriceFeed.recorded_at))
        .limit(1)
    ).scalar_one_or_none()


def _get_all_latest_prices(db: Session) -> list[PriceFeed]:
    """Get the latest price for each asset type."""
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
        select(PriceFeed).join(
            subq,
            (PriceFeed.asset_type == subq.c.asset_type)
            & (PriceFeed.recorded_at == subq.c.max_ts),
        )
    ).scalars().all()
    return rows


# ─── Demo Price Feed Simulator ─────────────────────────────────────

def _demo_price_thread():
    """Background thread that simulates price feeds for demo."""
    log.info(
        "Demo price feed simulator started"
        " (interval=%ds)", DEMO_INTERVAL_SECONDS,
    )
    while not _shutdown_event.is_set():
        try:
            session = SessionLocal()
            try:
                for asset, config in DEMO_ASSETS.items():
                    base = config["base"]
                    spread = config["spread"]
                    variation = random.uniform(-spread, spread)
                    price = Decimal(str(round(base + variation, 2)))
                    volume = Decimal(
                        str(round(random.uniform(100, 10000), 2))
                    )

                    sources = [
                        "coinbase", "binance",
                        "kraken", "internal",
                    ]
                    source = random.choice(sources)

                    feed = PriceFeed(
                        asset_type=asset,
                        price_usd=price,
                        source=source,
                        volume_24h=volume,
                        is_valid=True,
                    )
                    session.add(feed)

                session.commit()
                log.debug("Demo price feeds inserted")
            except Exception:
                session.rollback()
                log.exception("Error inserting demo price feeds")
            finally:
                session.close()
        except Exception:
            log.exception("Error creating session for demo feeds")

        _shutdown_event.wait(DEMO_INTERVAL_SECONDS)

    log.info("Demo price feed simulator stopped")


# ─── FastAPI App ───────────────────────────────────────────────────

@asynccontextmanager
async def lifespan(app: FastAPI):
    log.info("Price Oracle starting up...")
    demo_thread = None
    if DEMO_MODE:
        demo_thread = threading.Thread(
            target=_demo_price_thread, daemon=True,
        )
        demo_thread.start()
    yield
    _shutdown_event.set()
    if demo_thread:
        demo_thread.join(timeout=5)
    log.info("Price Oracle shutting down.")


app = FastAPI(
    title="Price Oracle",
    version="1.0.0",
    description=(
        "Asset price feed ingestion, VWAP aggregation,"
        " and serving for institutional lending"
    ),
    lifespan=lifespan,
)
instrument_app(app, SERVICE)


@app.get("/health")
def health():
    return {"status": "ok", "service": SERVICE}


@app.post(
    "/prices/feed",
    response_model=PriceResponse,
    status_code=status.HTTP_201_CREATED,
)
def ingest_price(
    req: IngestPriceRequest,
    db: Session = Depends(get_db_session),
):
    """Ingest a price feed from an external source."""
    feed = _ingest_price(db, req)
    return PriceResponse(
        asset_type=feed.asset_type,
        price_usd=feed.price_usd,
        source=feed.source,
        volume_24h=feed.volume_24h,
        recorded_at=feed.recorded_at,
    )


@app.post("/prices/aggregate", response_model=PriceResponse)
def aggregate_price(
    req: AggregatePriceRequest,
    db: Session = Depends(get_db_session),
):
    """Compute VWAP from recent feeds for an asset.

    Uses volume-weighted average when volume data is available,
    falls back to simple average otherwise.
    """
    feed = _aggregate_vwap(
        db, req.asset_type,
        req.lookback_minutes, req.max_entries,
    )
    return PriceResponse(
        asset_type=feed.asset_type,
        price_usd=feed.price_usd,
        source=feed.source,
        recorded_at=feed.recorded_at,
    )


@app.get("/prices/{asset_type}", response_model=PriceResponse)
def get_latest_price(
    asset_type: str,
    db: Session = Depends(get_db_session),
):
    """Get the latest price for an asset."""
    feed = _get_latest_price(db, asset_type)
    if not feed:
        raise HTTPException(
            status_code=404,
            detail=f"No price data for {asset_type}",
        )
    return PriceResponse(
        asset_type=feed.asset_type,
        price_usd=feed.price_usd,
        source=feed.source,
        volume_24h=feed.volume_24h,
        recorded_at=feed.recorded_at,
    )


@app.get("/prices")
def get_all_latest_prices(
    db: Session = Depends(get_db_session),
):
    """Get the latest price for each asset type."""
    feeds = _get_all_latest_prices(db)
    return [
        {
            "asset_type": f.asset_type,
            "price_usd": str(f.price_usd),
            "source": f.source,
            "volume_24h": (
                str(f.volume_24h) if f.volume_24h else None
            ),
            "recorded_at": (
                f.recorded_at.isoformat()
                if f.recorded_at else None
            ),
        }
        for f in feeds
    ]


@app.get("/prices/{asset_type}/history")
def get_price_history(
    asset_type: str,
    limit: int = Query(default=100, le=1000),
    db: Session = Depends(get_db_session),
):
    """Get price history for an asset (most recent first)."""
    feeds = db.execute(
        select(PriceFeed)
        .where(
            PriceFeed.asset_type == asset_type.upper(),
            PriceFeed.is_valid.is_(True),
        )
        .order_by(desc(PriceFeed.recorded_at))
        .limit(limit)
    ).scalars().all()

    if not feeds:
        raise HTTPException(
            status_code=404,
            detail=f"No price history for {asset_type}",
        )

    return [
        {
            "asset_type": f.asset_type,
            "price_usd": str(f.price_usd),
            "source": f.source,
            "volume_24h": (
                str(f.volume_24h) if f.volume_24h else None
            ),
            "recorded_at": (
                f.recorded_at.isoformat()
                if f.recorded_at else None
            ),
        }
        for f in feeds
    ]


if __name__ == "__main__":
    import uvicorn
    port = int(os.environ.get("PORT", 8005))
    uvicorn.run(
        "main:app", host="0.0.0.0", port=port, log_level="info",
    )
