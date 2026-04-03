"""
shared/kafka_client.py — Confluent Kafka producer and consumer wrappers.

Design decisions:
  - Producer is a module-level singleton (thread-safe, connection-pooled)
  - All messages are JSON-serialised Pydantic events
  - Producer flushes after each publish for at-least-once delivery
  - Consumer wraps confluent_kafka.Consumer with auto-deserialization
"""

import json
import logging
import os
import time
from typing import Callable, Optional, TypeVar

from confluent_kafka import (
    Consumer, KafkaError, KafkaException, Producer,
)
from pydantic import BaseModel

log = logging.getLogger(__name__)

KAFKA_BOOTSTRAP = os.environ.get("KAFKA_BOOTSTRAP", "kafka:9092")
SERVICE_NAME = os.environ.get("SERVICE_NAME", "unknown-service")

T = TypeVar("T", bound=BaseModel)

# ─── Producer ────────────────────────────────────────────────────────────────

_producer: Optional[Producer] = None


def _get_producer() -> Producer:
    global _producer
    if _producer is None:
        _producer = Producer(
            {
                "bootstrap.servers": KAFKA_BOOTSTRAP,
                "client.id": SERVICE_NAME,
                "acks": "all",
                "retries": 5,
                "retry.backoff.ms": 300,
                "compression.type": "lz4",
                "enable.idempotence": True,
                "message.timeout.ms": 10000,
            }
        )
    return _producer


def _delivery_report(err, msg):
    if err:
        log.error(
            "Kafka delivery failed | topic=%s err=%s",
            msg.topic(), err,
        )
    else:
        log.debug(
            "Kafka delivered | topic=%s partition=%d offset=%d",
            msg.topic(), msg.partition(), msg.offset(),
        )


def publish(
    topic: str,
    event: BaseModel,
    key: Optional[str] = None,
) -> None:
    """Publish a Pydantic event to a Kafka topic."""
    producer = _get_producer()
    payload = event.model_dump_json().encode("utf-8")
    producer.produce(
        topic=topic,
        value=payload,
        key=key.encode("utf-8") if key else None,
        on_delivery=_delivery_report,
    )
    producer.flush(timeout=5)
    log.info("Published | topic=%s key=%s", topic, key)


def publish_dict(
    topic: str,
    data: dict,
    key: Optional[str] = None,
) -> None:
    """Publish a raw dict to a Kafka topic."""
    producer = _get_producer()
    payload = json.dumps(data, default=str).encode("utf-8")
    producer.produce(
        topic=topic,
        value=payload,
        key=key.encode("utf-8") if key else None,
        on_delivery=_delivery_report,
    )
    producer.flush(timeout=5)


# ─── Consumer ────────────────────────────────────────────────────────────────

def build_consumer(
    group_id: str, topics: list[str],
) -> Consumer:
    """Create and subscribe a Kafka consumer."""
    consumer = Consumer(
        {
            "bootstrap.servers": KAFKA_BOOTSTRAP,
            "group.id": group_id,
            "client.id": f"{SERVICE_NAME}-{group_id}",
            "auto.offset.reset": "earliest",
            "enable.auto.commit": False,
            "max.poll.interval.ms": 300000,
            "session.timeout.ms": 45000,
        }
    )
    consumer.subscribe(topics)
    log.info(
        "Consumer subscribed | group=%s topics=%s",
        group_id, topics,
    )
    return consumer


def _is_already_processed(
    db_session_factory, event_id: str,
) -> bool:
    """Check if an event was already processed (dedup)."""
    try:
        from shared.models import ProcessedEvent
        session = db_session_factory()
        try:
            existing = session.get(ProcessedEvent, event_id)
            return existing is not None
        finally:
            session.close()
    except Exception as exc:
        log.warning("Dedup check failed: %s", exc)
        return False


def _mark_processed(
    db_session_factory, event_id: str, topic: str,
) -> None:
    """Mark an event as processed in the dedup table."""
    try:
        from shared.models import ProcessedEvent
        session = db_session_factory()
        try:
            row = ProcessedEvent(
                event_id=event_id, topic=topic,
            )
            session.add(row)
            session.commit()
        except Exception:
            session.rollback()
        finally:
            session.close()
    except Exception as exc:
        log.warning("Dedup mark failed: %s", exc)


def _send_to_dlq(
    topic: str,
    event_id: str,
    payload: dict,
    error_message: str,
    retry_count: int,
    db_session_factory=None,
) -> None:
    """Send a failed event to the dead letter queue."""
    dlq_topic = f"{topic}.dlq"
    dlq_payload = {
        "original_topic": topic,
        "event_id": event_id,
        "payload": payload,
        "error_message": error_message,
        "retry_count": retry_count,
    }
    try:
        publish_dict(dlq_topic, dlq_payload)
        log.warning(
            "Event sent to DLQ | topic=%s event_id=%s"
            " retries=%d",
            dlq_topic, event_id, retry_count,
        )
    except Exception as exc:
        log.error("Failed to publish to DLQ: %s", exc)

    if db_session_factory:
        try:
            from shared.models import DlqEvent
            session = db_session_factory()
            try:
                row = DlqEvent(
                    original_topic=topic,
                    event_id=event_id,
                    payload=payload,
                    error_message=error_message,
                    retry_count=retry_count,
                )
                session.add(row)
                session.commit()
            except Exception:
                session.rollback()
            finally:
                session.close()
        except Exception as exc:
            log.error(
                "Failed to record DLQ event in DB: %s", exc,
            )


def consume_loop(
    consumer: Consumer,
    handler: Callable[[str, dict], None],
    poll_timeout: float = 1.0,
    max_errors: int = 10,
    max_retries: int = 3,
    db_session_factory=None,
) -> None:
    """Blocking consume loop with DLQ and dedup support.

    Calls handler(topic, payload_dict) for each message.
    Commits offset only after successful handler execution.
    After max_retries failures on a specific event, sends
    to DLQ topic and commits offset.
    """
    consecutive_errors = 0
    failure_counts: dict[str, int] = {}
    try:
        while True:
            msg = consumer.poll(timeout=poll_timeout)
            if msg is None:
                continue
            if msg.error():
                if msg.error().code() == KafkaError._PARTITION_EOF:
                    continue
                log.error("Consumer error: %s", msg.error())
                consecutive_errors += 1
                if consecutive_errors >= max_errors:
                    raise KafkaException(msg.error())
                time.sleep(1)
                continue

            consecutive_errors = 0
            topic = msg.topic()
            raw = msg.value()
            try:
                payload = json.loads(raw)
                event_id = payload.get("event_id", "")

                if db_session_factory and event_id:
                    if _is_already_processed(
                        db_session_factory, event_id,
                    ):
                        consumer.commit(
                            message=msg, asynchronous=False,
                        )
                        continue

                handler(topic, payload)

                if db_session_factory and event_id:
                    _mark_processed(
                        db_session_factory, event_id, topic,
                    )

                if event_id in failure_counts:
                    del failure_counts[event_id]

                consumer.commit(
                    message=msg, asynchronous=False,
                )
            except Exception as exc:
                log.exception(
                    "Handler error | topic=%s exc=%s",
                    topic, exc,
                )
                event_id = ""
                try:
                    p = json.loads(raw)
                    event_id = p.get("event_id", "")
                except Exception:
                    pass

                if event_id:
                    count = failure_counts.get(event_id, 0) + 1
                    failure_counts[event_id] = count
                    if count >= max_retries:
                        _send_to_dlq(
                            topic, event_id, payload,
                            str(exc), count,
                            db_session_factory,
                        )
                        del failure_counts[event_id]
                        consumer.commit(
                            message=msg, asynchronous=False,
                        )
    finally:
        consumer.close()
        log.info("Consumer closed.")
