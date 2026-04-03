"""
services/outbox-publisher/main.py
---------------------------------
Transactional Outbox Publisher

Polls the outbox_events table for unpublished rows, forwards each
event to Kafka, and marks them as published. Uses advisory locking
via FOR UPDATE SKIP LOCKED to allow safe horizontal scaling.

Health endpoint on port 8010 (aiohttp).
"""

import asyncio
import json
import logging
import os
import signal
import sys
from datetime import datetime, timezone

import asyncpg
from aiohttp import web
from aiokafka import AIOKafkaProducer

log = logging.getLogger("outbox-publisher")
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(name)s: %(message)s",
)

DATABASE_URL = os.environ.get("DATABASE_URL", "")
KAFKA_BOOTSTRAP = os.environ.get("KAFKA_BOOTSTRAP", "localhost:9092")
POLL_INTERVAL_MS = int(os.environ.get("POLL_INTERVAL_MS", "100"))
BATCH_SIZE = 100
HEALTH_PORT = int(os.environ.get("HEALTH_PORT", "8010"))
MAX_PUBLISH_FAILURES = 3

_publish_failures: dict[str, int] = {}

FETCH_QUERY = """
    SELECT id, event_type, aggregate_id, payload, created_at
    FROM outbox_events
    WHERE published_at IS NULL
    ORDER BY created_at
    LIMIT $1
    FOR UPDATE SKIP LOCKED
"""

MARK_PUBLISHED_QUERY = """
    UPDATE outbox_events
    SET published_at = $1
    WHERE id = ANY($2::uuid[])
"""


async def publish_batch(
    pool: asyncpg.Pool,
    producer: AIOKafkaProducer,
) -> int:
    """Fetch one batch of unpublished events and send to Kafka.

    Returns the number of events published.
    """
    async with pool.acquire() as conn:
        async with conn.transaction():
            rows = await conn.fetch(FETCH_QUERY, BATCH_SIZE)
            if not rows:
                return 0

            published_ids = []
            for row in rows:
                event_id = str(row["id"])
                topic = row["event_type"]
                key = row["aggregate_id"]
                payload = row["payload"]
                value = (
                    payload.encode("utf-8")
                    if isinstance(payload, str)
                    else json.dumps(payload).encode("utf-8")
                )
                try:
                    await producer.send_and_wait(
                        topic=topic,
                        key=(
                            key.encode("utf-8") if key
                            else None
                        ),
                        value=value,
                    )
                    _publish_failures.pop(event_id, None)
                    published_ids.append(row["id"])
                except Exception as exc:
                    count = _publish_failures.get(event_id, 0) + 1
                    _publish_failures[event_id] = count
                    if count > MAX_PUBLISH_FAILURES:
                        log.error(
                            "Event %s exceeded %d publish "
                            "retries, sending to DLQ",
                            event_id, MAX_PUBLISH_FAILURES,
                        )
                        await conn.execute(
                            """
                            INSERT INTO dlq_events
                                (original_topic, event_id,
                                 payload, error_message,
                                 retry_count)
                            VALUES ($1, $2, $3, $4, $5)
                            """,
                            topic,
                            event_id,
                            (
                                payload
                                if isinstance(payload, str)
                                else json.dumps(payload)
                            ),
                            str(exc),
                            count,
                        )
                        published_ids.append(row["id"])
                        _publish_failures.pop(event_id, None)
                    else:
                        log.warning(
                            "Failed to publish event %s "
                            "(attempt %d/%d): %s",
                            event_id, count,
                            MAX_PUBLISH_FAILURES, exc,
                        )

            if published_ids:
                now = datetime.now(timezone.utc)
                await conn.execute(
                    MARK_PUBLISHED_QUERY, now, published_ids,
                )

            log.info("Published %d events", len(published_ids))
            return len(published_ids)


async def poll_loop(
    pool: asyncpg.Pool,
    producer: AIOKafkaProducer,
    shutdown_event: asyncio.Event,
) -> None:
    """Continuously poll the outbox table until shutdown is signaled."""
    interval = POLL_INTERVAL_MS / 1000.0
    while not shutdown_event.is_set():
        try:
            published = await publish_batch(pool, producer)
            if published == 0:
                await asyncio.sleep(interval)
        except asyncpg.PostgresError as exc:
            log.error("Database error during poll: %s", exc)
            await asyncio.sleep(interval * 10)
        except Exception as exc:
            log.error("Unexpected error during poll: %s", exc)
            await asyncio.sleep(interval * 10)


async def health_handler(request: web.Request) -> web.Response:
    """Return HTTP 200 if the service is running."""
    return web.json_response(
        {"status": "ok", "service": "outbox-publisher"},
    )


async def run_health_server(
    shutdown_event: asyncio.Event,
) -> None:
    """Run the aiohttp health endpoint until shutdown."""
    app = web.Application()
    app.router.add_get("/health", health_handler)
    runner = web.AppRunner(app)
    await runner.setup()
    site = web.TCPSite(runner, "0.0.0.0", HEALTH_PORT)
    await site.start()
    log.info("Health endpoint listening on port %d", HEALTH_PORT)
    await shutdown_event.wait()
    await runner.cleanup()


async def main() -> None:
    """Entry point: connect to Postgres and Kafka, then poll."""
    if not DATABASE_URL:
        log.error("DATABASE_URL is not set")
        sys.exit(1)

    shutdown_event = asyncio.Event()
    loop = asyncio.get_running_loop()
    for sig in (signal.SIGINT, signal.SIGTERM):
        loop.add_signal_handler(sig, shutdown_event.set)

    pool = await asyncpg.create_pool(
        DATABASE_URL, min_size=2, max_size=5,
    )
    producer = AIOKafkaProducer(
        bootstrap_servers=KAFKA_BOOTSTRAP,
        acks="all",
    )
    await producer.start()
    log.info(
        "Connected to Postgres and Kafka, polling every %d ms",
        POLL_INTERVAL_MS,
    )

    try:
        await asyncio.gather(
            poll_loop(pool, producer, shutdown_event),
            run_health_server(shutdown_event),
        )
    finally:
        await producer.stop()
        await pool.close()
        log.info("Outbox publisher shut down")


if __name__ == "__main__":
    asyncio.run(main())
