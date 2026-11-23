-- RelayOps schema (applied by the postgres image on first start)

CREATE TABLE IF NOT EXISTS events (
    id          BIGSERIAL PRIMARY KEY,
    event_id    TEXT        NOT NULL,
    type        TEXT        NOT NULL,
    payload     JSONB       NOT NULL,
    status      TEXT        NOT NULL DEFAULT 'pending',
    created_at  TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at  TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS events_status_idx ON events (status);

CREATE TABLE IF NOT EXISTS delivery_attempts (
    id           BIGSERIAL PRIMARY KEY,
    event_pk     BIGINT      NOT NULL REFERENCES events (id),
    status_code  INTEGER,
    error        TEXT,
    created_at   TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp()
);

CREATE INDEX IF NOT EXISTS delivery_attempts_event_pk_created_idx
    ON delivery_attempts (event_pk, created_at, id);

CREATE VIEW events_with_attempts AS
SELECT e.*,
       (SELECT count(*) FROM delivery_attempts d WHERE d.event_pk = e.id)::int AS attempts
FROM events e;

CREATE VIEW delivery_attempts_numbered AS
SELECT d.*,
       row_number() OVER (PARTITION BY d.event_pk ORDER BY d.created_at, d.id)::int AS attempt
FROM delivery_attempts d;

