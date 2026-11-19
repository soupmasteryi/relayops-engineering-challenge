from fastapi import FastAPI, HTTPException, Response
from psycopg.types.json import Jsonb

from .db import get_conn
from .models import EventIn

app = FastAPI(title="RelayOps API")


@app.get("/health")
def health():
    return {"status": "ok"}


@app.post("/events")
def create_event(event: EventIn, response: Response):
    conn = get_conn()
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
            return { "event_id": event.event_id, "status": row["status"]}
        else:
            raise HTTPException(status_code=409)

    row = conn.execute(
        """
        INSERT INTO events (event_id, type, payload)
        VALUES (%s, %s, %s)
        RETURNING event_id, status
        """,
        (event.event_id, event.type, Jsonb(event.payload)),
    ).fetchone()
    conn.commit()
    response.status_code = 202
    return row


@app.get("/events/{event_id}")
def get_event(event_id: str):
    conn = get_conn()
    row = conn.execute(
        """
        SELECT event_id, type, payload, status, attempts, created_at, updated_at
        FROM events WHERE event_id = %s
        ORDER BY id LIMIT 1
        """,
        (event_id,),
    ).fetchone()
    if row is None:
        raise HTTPException(status_code=404, detail=f"event not found \"{event_id}\"")

    row["delivery_attempts"] = [] if row["attempts"] == 0 else conn.execute(
        """
        SELECT event_pk, attempt, status_code, error, created_at
        FROM delivery_attempts WHERE event_pk = %s
        ORDER BY id
        """,
        (row["id"],),
    ).fetchall()
    return row

@app.get("/metrics")
def get_metrics():
    pass
