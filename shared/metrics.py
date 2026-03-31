"""
shared/metrics.py — Prometheus instrumentation shared across all services.

Each service imports this module and calls register_metrics(app) after
creating their FastAPI instance.  The module exposes:

  - HTTP request duration histogram (by route, method, status)
  - In-flight request gauge
  - Business-domain counters for lending, collateral, liquidation
  - Kafka publish counter

Usage:
    from shared.metrics import register_metrics, LOANS_ORIGINATED
    register_metrics(app)

    LOANS_ORIGINATED.labels(service="loan-engine", currency="USD").inc()
"""

import os
import time

try:
    from prometheus_client import (
        Counter, Gauge, Histogram,
        generate_latest, CONTENT_TYPE_LATEST, REGISTRY,
    )
    _PROM_AVAILABLE = True
except ImportError:
    _PROM_AVAILABLE = False

SERVICE_NAME = os.environ.get("SERVICE_NAME", "unknown")

if _PROM_AVAILABLE:
    HTTP_REQUESTS_TOTAL = Counter(
        "http_requests_total",
        "Total HTTP requests",
        ["service", "method", "path", "status_code"],
    )
    HTTP_REQUEST_DURATION = Histogram(
        "http_request_duration_seconds",
        "HTTP request duration in seconds",
        ["service", "method", "path"],
        buckets=[
            0.005, 0.01, 0.025, 0.05, 0.1,
            0.25, 0.5, 1, 2.5, 5,
        ],
    )
    HTTP_REQUESTS_IN_FLIGHT = Gauge(
        "http_requests_in_flight",
        "Current in-flight HTTP requests",
        ["service"],
    )
    LOANS_ORIGINATED = Counter(
        "loans_originated_total",
        "Total loans originated",
        ["service", "currency"],
    )
    LOAN_AMOUNT = Counter(
        "loan_amount_total",
        "Total nominal value of loans originated",
        ["service", "currency"],
    )
    INTEREST_ACCRUED = Counter(
        "interest_accrued_total",
        "Total interest accrued across all loans",
        ["service", "currency"],
    )
    COLLATERAL_DEPOSITED = Counter(
        "collateral_deposited_total",
        "Total collateral deposit operations",
        ["service", "asset_type"],
    )
    COLLATERAL_VALUE_USD = Gauge(
        "collateral_value_usd",
        "Current total collateral value in USD",
        ["service"],
    )
    MARGIN_CALLS = Counter(
        "margin_calls_total",
        "Total margin call events",
        ["service", "status"],
    )
    LIQUIDATIONS = Counter(
        "liquidations_total",
        "Total liquidation events",
        ["service", "status"],
    )
    LIQUIDATION_VOLUME = Counter(
        "liquidation_volume_total",
        "Total nominal value of liquidation proceeds",
        ["service", "currency"],
    )
    PRICE_UPDATES = Counter(
        "price_updates_total",
        "Total price feed update events",
        ["service", "asset_type", "source"],
    )
    ACTIVE_LOANS = Gauge(
        "active_loans",
        "Number of currently active loans",
        ["service"],
    )
    LOAN_LTV_PCT = Histogram(
        "loan_ltv_pct",
        "Loan-to-value percentage distribution",
        ["service"],
        buckets=[
            10, 20, 30, 40, 50, 60, 65,
            70, 75, 80, 85, 90, 95, 100, 110, 120,
        ],
    )
    KAFKA_PUBLISHES = Counter(
        "kafka_publishes_total",
        "Kafka publish operations",
        ["service", "topic", "status"],
    )
    COMPLIANCE_EVENTS = Counter(
        "compliance_events_total",
        "Compliance screening events",
        ["service", "result", "event_type"],
    )


class _Noop:
    """Fallback when prometheus_client is not installed."""
    def __getattr__(self, _):
        return lambda *a, **kw: self
    def labels(self, **_):
        return self
    def inc(self, *_):
        pass
    def observe(self, *_):
        pass
    def set(self, *_):
        pass


if not _PROM_AVAILABLE:
    for _name in [
        "HTTP_REQUESTS_TOTAL",
        "HTTP_REQUEST_DURATION",
        "HTTP_REQUESTS_IN_FLIGHT",
        "LOANS_ORIGINATED",
        "LOAN_AMOUNT",
        "INTEREST_ACCRUED",
        "COLLATERAL_DEPOSITED",
        "COLLATERAL_VALUE_USD",
        "MARGIN_CALLS",
        "LIQUIDATIONS",
        "LIQUIDATION_VOLUME",
        "PRICE_UPDATES",
        "ACTIVE_LOANS",
        "LOAN_LTV_PCT",
        "KAFKA_PUBLISHES",
        "COMPLIANCE_EVENTS",
    ]:
        globals()[_name] = _Noop()


def instrument_app(app, service_name: str = "") -> None:
    """Shortcut: set service name and register metrics."""
    global SERVICE_NAME
    if service_name:
        SERVICE_NAME = service_name
    register_metrics(app)


def record_business_event(
    metric_name: str, labels: dict, value: float = 1.0,
) -> None:
    """Increment a named business metric by looking it up in globals."""
    metric = globals().get(metric_name)
    if metric is not None:
        metric.labels(**labels).inc(value)


def register_metrics(app) -> None:
    """Attach Prometheus middleware and /metrics endpoint to a FastAPI app."""
    if not _PROM_AVAILABLE:
        @app.get("/metrics", include_in_schema=False)
        def _stub():
            return {"status": "prometheus_client not installed"}
        return

    from starlette.middleware.base import BaseHTTPMiddleware

    class PrometheusMiddleware(BaseHTTPMiddleware):
        async def dispatch(self, request, call_next):
            if request.url.path == "/metrics":
                return await call_next(request)
            path = request.url.path
            method = request.method
            HTTP_REQUESTS_IN_FLIGHT.labels(
                service=SERVICE_NAME,
            ).inc()
            start = time.perf_counter()
            try:
                response = await call_next(request)
                status = str(response.status_code)
                return response
            except Exception:
                status = "500"
                raise
            finally:
                elapsed = time.perf_counter() - start
                HTTP_REQUESTS_IN_FLIGHT.labels(
                    service=SERVICE_NAME,
                ).dec()
                HTTP_REQUESTS_TOTAL.labels(
                    service=SERVICE_NAME,
                    method=method,
                    path=path,
                    status_code=status,
                ).inc()
                HTTP_REQUEST_DURATION.labels(
                    service=SERVICE_NAME,
                    method=method,
                    path=path,
                ).observe(elapsed)

    app.add_middleware(PrometheusMiddleware)

    @app.get("/metrics", include_in_schema=False)
    def metrics_endpoint():
        from fastapi.responses import PlainTextResponse
        return PlainTextResponse(
            generate_latest(REGISTRY),
            media_type=CONTENT_TYPE_LATEST,
        )
