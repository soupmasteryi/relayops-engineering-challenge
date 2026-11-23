from contextlib import asynccontextmanager

from fastapi import FastAPI, HTTPException, Request, Response
from fastapi.responses import JSONResponse
from psycopg.types.json import Jsonb
from psycopg_pool import PoolTimeout

from .db import metrics_pool, pool
from .models import EventIn


@asynccontextmanager
async def lifespan(_: FastAPI):
    pool.open()
    metrics_pool.open()
    try:
        yield
    finally:
        metrics_pool.close()
        pool.close()


app = FastAPI(title="RelayOps API", lifespan=lifespan)


@app.exception_handler(PoolTimeout)
async def pool_timeout_handler(request: Request, exc: PoolTimeout):
    return JSONResponse(
        status_code=503,
        content={"detail": "database busy, retry shortly"},
        headers={"Retry-After": "5"},
    )


@app.get("/health")
def health():
    return {"status": "ok"}


@app.post("/events")
def create_event(event: EventIn, response: Response):
    # Commits on normal exit, rolls back if an exception propagates
    # (including the HTTPException below), and always returns the
    # connection to the pool.
    with pool.connection() as conn:
        # Serialize concurrent creates of the same event_id. Must be the first
        # statement; the lock is released automatically on commit/rollback.
        conn.execute(
            "SELECT pg_advisory_xact_lock(hashtextextended(%s, 0))",
            (event.event_id,),
        )

        row = conn.execute(
            """
            SELECT event_id, type, payload, status
            FROM events WHERE event_id = %s
            ORDER BY id LIMIT 1
            """,
            (event.event_id,),
        ).fetchone()

        if row is not None:
            if row["type"] == event.type and row["payload"] == event.payload:
                return {"event_id": event.event_id, "status": row["status"]}
            raise HTTPException(status_code=409)

        row = conn.execute(
            """
            INSERT INTO events (event_id, type, payload)
            VALUES (%s, %s, %s)
            RETURNING event_id, status
            """,
            (event.event_id, event.type, Jsonb(event.payload)),
        ).fetchone()

    response.status_code = 202
    return row


@app.get("/events/{event_id}")
def get_event(event_id: str):
    with pool.connection() as conn:
        row = conn.execute(
            """
            SELECT id, event_id, type, payload, status, attempts, created_at, updated_at
            FROM events_with_attempts WHERE event_id = %s
            ORDER BY id LIMIT 1
            """,
            (event_id,),
        ).fetchone()
        if row is None:
            raise HTTPException(status_code=404, detail=f"event not found \"{event_id}\"")

        row["delivery_attempts"] = [] if row["attempts"] == 0 else conn.execute(
            """
            SELECT event_pk, attempt, status_code, error, created_at
            FROM delivery_attempts_numbered WHERE event_pk = %s
            ORDER BY id
            """,
            (row["id"],),
        ).fetchall()
    return row


EVENT_STATUSES = ("pending", "processing", "delivered", "failed")


@app.get("/metrics")
def get_metrics():
    # Both queries run in one transaction, so they share one snapshot.
    with metrics_pool.connection() as conn:
        rows = conn.execute(
            "SELECT status, count(*) AS n FROM events GROUP BY status"
        ).fetchall()
        attempts = conn.execute(
            "SELECT count(*) AS n FROM delivery_attempts"
        ).fetchone()["n"]

    counts = {status: 0 for status in EVENT_STATUSES}  # emit zeros too
    for r in rows:
        if r["status"] in counts:
            counts[r["status"]] = r["n"]

    lines = [
        "# HELP relayops_events Number of events by status.",
        "# TYPE relayops_events gauge",
        *(f'relayops_events{{status="{s}"}} {n}' for s, n in counts.items()),
        "# HELP relayops_delivery_attempts_total Total delivery attempts made.",
        "# TYPE relayops_delivery_attempts_total counter",
        f"relayops_delivery_attempts_total {attempts}",
    ]
    return Response(
        content="\n".join(lines) + "\n",
        media_type="text/plain; version=0.0.4; charset=utf-8",
    )
