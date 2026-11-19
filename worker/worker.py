"""RelayOps delivery worker.

Polls the events table and delivers pending events to the configured
webhook receiver. Every request is signed with HMAC-SHA256.
"""
import hashlib
import hmac
import json
import os
import time

import httpx
import psycopg
from httpx import Client
from psycopg.rows import dict_row

DATABASE_URL = os.environ.get(
    "DATABASE_URL", "postgresql://relayops:relayops@db:5432/relayops"
)
RECEIVER_URL = os.environ.get("RECEIVER_URL", "http://receiver:9000/webhook")
WEBHOOK_SECRET = os.environ.get("WEBHOOK_SECRET", "")
POLL_INTERVAL = float(os.environ.get("POLL_INTERVAL", "1"))
BATCH_SIZE = int(os.environ.get("BATCH_SIZE", "10"))
MAX_ATTEMPTS = 3


def sign(raw: bytes) -> str:
    return "sha256=" + hmac.new(WEBHOOK_SECRET.encode(), raw, hashlib.sha256).hexdigest()


def fetch_pending(conn):
    return conn.execute(
        """
        SELECT id, event_id, type, payload
        FROM events
        WHERE status = 'pending'
        ORDER BY id
        LIMIT %s
        """,
        (BATCH_SIZE,),
    ).fetchall()


def deliver(client: Client, event):
    body = {
        "event_id": event["event_id"],
        "type": event["type"],
        "payload": event["payload"],
    }
    request = client.build_request("POST", RECEIVER_URL, json=body)
    request.headers["X-RelayOps-Signature"] = sign(request.content)
    delivery_attempts = []
    status = "failed"
    for attempt in range(1, MAX_ATTEMPTS + 1):
        try:
            response = client.send(request)
            response.raise_for_status()
            print(f"delivered {event['event_id']} attempt={attempt} -> {response.status_code}")
            delivery_attempts.append({
                "attempt": attempt,
                "status_code": response.status_code,
                "error": None
            })
            status = "delivered"
            break
        except httpx.RequestError as exc:
            print(f"error delivering {event['event_id']} attempt={attempt}: {exc}")
            delivery_attempts.append({
                "attempt": attempt,
                "status_code": None,
                "error": exc
            })
            time.sleep(2 ** attempt)
        except httpx.HTTPStatusError as exc:
            print(f"error http code {exc.response.status_code} while delivering {event['event_id']}: {exc}")
            if exc.response.status_code == 429 or 500 <= exc.response.status_code < 599:
                retry_seconds = 0
                try:
                    retry_seconds = int(exc.response.headers["Retry-After"])
                except ValueError | KeyError:
                    pass
                delivery_attempts.append({
                    "attempt": attempt,
                    "status_code": exc.response.status_code,
                    "error": None
                })
                time.sleep(max(2 ** attempt, retry_seconds))
            else:
                delivery_attempts.append({
                    "attempt": attempt,
                    "status_code": exc.response.status_code,
                    "error": None
                })
                break
    return status, delivery_attempts


def main():
    print(f"worker started, receiver={RECEIVER_URL}")
    conn = psycopg.connect(DATABASE_URL, row_factory=dict_row, autocommit=False)
    with httpx.Client(timeout=5) as client:
        while True:
            events = fetch_pending(conn)
            for event in events:
                status, delivery_attempts = deliver(client, event)
                conn.execute(
                    "UPDATE events SET status = %s, attempts = %s, updated_at = now() WHERE id = %s",
                    (status, len(delivery_attempts), event["id"]),
                )
                conn.commit()
            if not events:
                time.sleep(POLL_INTERVAL)


if __name__ == "__main__":
    main()
