"""RelayOps delivery worker.

Polls the events table and delivers pending events to the configured
webhook receiver. Every request is signed with HMAC-SHA256.

On SIGTERM/SIGINT the worker finishes (or abandons) the event it is
currently delivering, rolls back any open transaction, and moves every
event it claimed but did not finish from "processing" back to "pending".
"""
import hashlib
import hmac
import os
import signal
import threading

import httpx
import psycopg
from httpx import Client
from psycopg import Connection
from psycopg.rows import dict_row, class_row

from typing import Any
from pydantic import BaseModel, ConfigDict

class DeliveryAttemptCreate(BaseModel):
    event_pk: int
    status_code: int | None = None
    error: str | None = None


class Event(BaseModel):
    model_config = ConfigDict(from_attributes=True)

    id: int
    event_id: str
    type: str
    payload: dict[str, Any]


DATABASE_URL = os.environ.get(
    "DATABASE_URL", "postgresql://relayops:relayops@db:5432/relayops"
)
RECEIVER_URL = os.environ.get("RECEIVER_URL", "http://receiver:9000/webhook")
WEBHOOK_SECRET = os.environ.get("WEBHOOK_SECRET", "")
POLL_INTERVAL = float(os.environ.get("POLL_INTERVAL", "1"))
BATCH_SIZE = int(os.environ.get("BATCH_SIZE", "10"))
MAX_ATTEMPTS = 3

# Set by the signal handler; checked by the main loop and the retry loop.
shutdown = threading.Event()


def _handle_signal(signum, frame):
    print(f"received {signal.Signals(signum).name}, shutting down")
    shutdown.set()


def backoff(seconds: float) -> bool:
    """Sleep for `seconds`, waking early on shutdown. Returns True if shutting down."""
    return shutdown.wait(seconds)


def sign(raw: bytes) -> str:
    return "sha256=" + hmac.new(WEBHOOK_SECRET.encode(), raw, hashlib.sha256).hexdigest()


def claim_pending(conn: Connection) -> list[Event]:
    with conn.cursor(row_factory=class_row(Event)) as cur:
        rows = cur.execute(
            """
            UPDATE events
            SET status = 'processing'
            WHERE id IN (
              SELECT id FROM events
              WHERE status = 'pending'
              ORDER BY created_at
              LIMIT %s
              FOR UPDATE SKIP LOCKED
            )
            RETURNING id, event_id, type, payload
            """,
            (BATCH_SIZE,),
        ).fetchall()
        conn.commit()
        return rows


def release_events(conn: Connection, event_ids: list[int]) -> None:
    """Move events that were claimed but not finished back to 'pending'."""
    if not event_ids:
        return
    conn.execute(
        """
        UPDATE events
        SET status = 'pending', updated_at = now()
        WHERE id = ANY(%s) AND status = 'processing'
        """,
        (list(event_ids),),
    )
    conn.commit()


def deliver(client: Client, event: Event):
    """Attempt delivery with retries.

    Returns (status, attempts). status is "delivered", "failed", or
    "pending" (shutdown interrupted the retries; the event should be requeued).
    """
    body = {
        "event_id": event.event_id,
        "type": event.type,
        "payload": event.payload,
    }
    request = client.build_request("POST", RECEIVER_URL, json=body)
    request.headers["X-RelayOps-Signature"] = sign(request.content)
    delivery_attempts: list[DeliveryAttemptCreate] = []
    status = "failed"
    for attempt in range(1, MAX_ATTEMPTS + 1):
        if shutdown.is_set():
            return "pending", delivery_attempts
        try:
            response = client.send(request)
            response.raise_for_status()
            print(f"delivered {event.event_id} attempt={attempt} -> {response.status_code}")
            delivery_attempts.append(DeliveryAttemptCreate(
                event_pk=event.id,
                status_code=response.status_code,
                error=None
            ))
            status = "delivered"
            break
        except httpx.RequestError as exc:
            print(f"error delivering {event.event_id} attempt={attempt}: {exc}")
            delivery_attempts.append(DeliveryAttemptCreate(
                event_pk=event.id,
                status_code=None,
                error=str(exc)
            ))
            if attempt < MAX_ATTEMPTS and backoff(2 ** attempt):
                return "pending", delivery_attempts
        except httpx.HTTPStatusError as exc:
            code = exc.response.status_code
            print(f"error http code {code} while delivering {event.event_id}: {exc}")
            delivery_attempts.append(DeliveryAttemptCreate(
                event_pk=event.id,
                status_code=code,
                error=None
            ))
            if code == 429 or 500 <= code < 600:
                retry_seconds = 0
                try:
                    retry_seconds = int(exc.response.headers["Retry-After"])
                except (ValueError, KeyError):
                    pass
                if attempt < MAX_ATTEMPTS and backoff(max(2 ** attempt, retry_seconds)):
                    return "pending", delivery_attempts
            else:
                break
    return status, delivery_attempts


def cleanup(conn: Connection, unfinished: list[int]) -> None:
    """Roll back open work and requeue unfinished events."""
    print(f"cleaning up, requeueing {len(unfinished)} unfinished event(s)")
    try:
        conn.rollback()
        release_events(conn, unfinished)
    except psycopg.Error as exc:
        print(f"cleanup on existing connection failed: {exc}; retrying on a new one")
        try:
            with psycopg.connect(DATABASE_URL) as fresh:
                release_events(fresh, unfinished)
        except psycopg.Error as exc2:
            print(f"cleanup failed, events {unfinished} may be stuck in 'processing': {exc2}")
    finally:
        try:
            conn.close()
        except psycopg.Error:
            pass


def main():
    signal.signal(signal.SIGTERM, _handle_signal)
    signal.signal(signal.SIGINT, _handle_signal)

    print(f"worker started, receiver={RECEIVER_URL}")
    conn = psycopg.connect(DATABASE_URL, row_factory=dict_row)
    unfinished: list[int] = []  # ids claimed ('processing') but not yet resolved
    try:
        with httpx.Client(timeout=5) as client:
            while not shutdown.is_set():
                events = claim_pending(conn)
                unfinished = [e.id for e in events]
                for event in events:
                    if shutdown.is_set():
                        break  # remaining events are released in cleanup
                    status, delivery_attempts = deliver(client, event)
                    conn.execute(
                        """
                        UPDATE events
                        SET status = %s, updated_at = now()
                        WHERE id = %s
                        """,
                        (status, event.id),
                    )
                    conn.cursor().executemany(
                        """
                        INSERT INTO delivery_attempts (event_pk, status_code, error)
                        VALUES (%s, %s, %s)
                        """,
                        [(d.event_pk, d.status_code, d.error) for d in delivery_attempts]
                    )
                    conn.commit()
                    unfinished.remove(event.id)
                if not events:
                    backoff(POLL_INTERVAL)
    finally:
        cleanup(conn, unfinished)
        print("worker stopped")


if __name__ == "__main__":
    main()
