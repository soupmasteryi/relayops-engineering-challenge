"""Tests for the RelayOps delivery worker, run against the LIVE mock receiver.

* The receiver is the real service (default http://receiver:9000). Its behaviour
  is scripted through POST /control and verified through GET /received. It is
  reset (DELETE /received) before and after every test.
* The receiver can only apply ONE failure mode at a time (N failures with one
  status code / Retry-After value, optionally preceded by delayed responses).
  Sequences that change mid-delivery (e.g. timeout -> 429 -> timeout) are
  produced by "hooks": the patched `worker.backoff` runs a callable between
  attempts that re-configures the receiver for the next attempt.
* Network errors are produced by (a) a response delay longer than the client
  timeout -> httpx.ReadTimeout, or (b) pointing the worker at a closed port ->
  httpx.ConnectError.
* The database is a `FakeConn` that just records what the worker writes.
* `worker.backoff` is replaced so no test actually sleeps; the requested sleep
  durations are recorded instead.

Environment:
  WEBHOOK_SECRET        must equal the receiver's secret (required)
  RECEIVER_BASE_URL     default http://receiver:9000
"""
import hashlib
import hmac
import os
import time

import httpx
import pytest

from worker import worker

SECRET = os.environ.get("WEBHOOK_SECRET", "SUPERSECURES3CRET")
BASE_URL = os.environ.get("RECEIVER_BASE_URL", "http://localhost:9000")
REAL_CLIENT = httpx.Client  # keep a reference before any monkeypatching

CLIENT_TIMEOUT = 0.5   # worker-side timeout used in these tests (seconds)
DELAY_MS = 1000        # receiver delay; must exceed CLIENT_TIMEOUT to force a timeout
CLOSED_PORT_URL = "http://127.0.0.1:1/webhook"  # nothing listens -> ConnectError


# --------------------------------------------------------------------------
# Live receiver helper
# --------------------------------------------------------------------------
class LiveReceiver:
    """Thin client for the receiver's control / inspection endpoints."""

    def __init__(self, base_url):
        self.http = REAL_CLIENT(base_url=base_url, timeout=5)
        self._settle_s = 0.0

    def health(self):
        return self.http.get("/health").json()

    def reset(self):
        self.http.delete("/received").raise_for_status()

    def control(self, **kw):
        """POST /control. Fields: fail_next, status_code, retry_after, delay_ms, delay_next."""
        self.http.post("/control", json=kw).raise_for_status()
        if kw.get("delay_ms"):
            # Delayed requests are only logged by the receiver once they are
            # answered, i.e. after the worker already gave up on them.
            self._settle_s = max(self._settle_s, kw["delay_ms"] / 1000 + 0.2)

    def settle(self):
        """Wait for in-flight delayed requests to be answered and logged."""
        if self._settle_s:
            time.sleep(self._settle_s)
            self._settle_s = 0.0

    def received(self):
        """Every request the receiver saw, in arrival order."""
        self.settle()
        rows = self.http.get("/received").json()
        return sorted(rows, key=lambda r: r["arrived_at"])


@pytest.fixture(scope="session")
def _live():
    receiver = LiveReceiver(BASE_URL)
    try:
        assert receiver.health() == {"status": "ok"}
    except Exception as exc:  # noqa: BLE001
        pytest.skip(f"live receiver not reachable at {BASE_URL}: {exc!r}", allow_module_level=True)
    return receiver


@pytest.fixture(autouse=True)
def receiver(_live, monkeypatch):
    monkeypatch.setattr(worker, "RECEIVER_URL", f"{BASE_URL}/webhook")
    _live.reset()
    yield _live
    _live.settle()  # let delayed requests land before wiping the log
    _live.reset()


# --------------------------------------------------------------------------
# Fixtures / helpers
# --------------------------------------------------------------------------
@pytest.fixture(autouse=True)
def secret(monkeypatch):
    monkeypatch.setattr(worker, "WEBHOOK_SECRET", SECRET)


@pytest.fixture(autouse=True)
def fresh_shutdown_flag():
    """`worker.shutdown` is module-global; main() tests set it, so clear it."""
    worker.shutdown.clear()
    yield
    worker.shutdown.clear()


class Sleeps(list):
    """Recorded backoff durations. `hooks` run (FIFO) after each backoff call,
    letting a test re-configure the live receiver between attempts."""

    def __init__(self):
        super().__init__()
        self.hooks = []


@pytest.fixture
def sleeps(monkeypatch):
    """Record backoff durations instead of sleeping; never signals shutdown."""
    recorded = Sleeps()

    def fake_backoff(seconds):
        recorded.append(seconds)
        if recorded.hooks:
            recorded.hooks.pop(0)()
        return False

    monkeypatch.setattr(worker, "backoff", fake_backoff)
    return recorded


def make_event(pk=1):
    return worker.Event(id=pk, event_id=f"evt_{pk}", type="user.created", payload={"name": "Ada", "n": pk})


def run_deliver(event=None):
    event = event or make_event()
    with REAL_CLIENT(timeout=CLIENT_TIMEOUT) as client:
        status, attempts = worker.deliver(client, event)
    return status, attempts


def codes(attempts):
    return [a.status_code for a in attempts]


def arm(receiver, failure, times):
    """Make the next `times` requests fail with `failure`.

    `failure` is "timeout" (response delayed past the client timeout) or an
    HTTP status code. After `times` requests the receiver answers 200 again.
    """
    if failure == "timeout":
        receiver.control(delay_ms=DELAY_MS, delay_next=times)
    else:
        receiver.control(fail_next=times, status_code=failure)


# --------------------------------------------------------------------------
# 1. Signing
# --------------------------------------------------------------------------
def test_sign_is_hmac_sha256_of_raw_body():
    raw = b'{"a":1}'
    expected = "sha256=" + hmac.new(SECRET.encode(), raw, hashlib.sha256).hexdigest()
    assert worker.sign(raw) == expected


def test_every_request_has_valid_signature_over_exact_body(receiver, sleeps):
    # attempt 1: 503, attempt 2: timeout, attempt 3: 200
    receiver.control(fail_next=1, status_code=503)
    sleeps.hooks = [lambda: receiver.control(delay_ms=DELAY_MS, delay_next=1)]
    status, attempts = run_deliver()

    assert status == "delivered"
    seen = receiver.received()
    assert len(seen) == 3  # includes retries
    # The receiver verifies the HMAC over the raw body it received.
    assert all(r["signature_valid"] for r in seen)


def test_body_contains_event_fields(receiver):
    event = make_event()
    run_deliver(event)
    seen = receiver.received()
    assert len(seen) == 1
    assert seen[0]["body"] == {
        "event_id": event.event_id,
        "type": event.type,
        "payload": event.payload,
    }
    assert seen[0]["event_id"] == event.event_id


def test_wrong_secret_gets_401_and_fails_without_retry(receiver, sleeps, monkeypatch):
    monkeypatch.setattr(worker, "WEBHOOK_SECRET", "a-different-secret")
    status, attempts = run_deliver()

    assert status == "failed"
    assert codes(attempts) == [401]
    seen = receiver.received()
    assert len(seen) == 1
    assert seen[0]["signature_valid"] is False
    assert seen[0]["status_code"] == 401
    assert sleeps == []


# --------------------------------------------------------------------------
# 2. Success on 2xx
# --------------------------------------------------------------------------
# 204 is omitted: the receiver always answers with a JSON body, which is not
# valid for a 204.
@pytest.mark.parametrize("code", [200, 201, 202])
def test_2xx_is_success(code, receiver, sleeps):
    receiver.control(fail_next=1, status_code=code)
    status, attempts = run_deliver()

    assert status == "delivered"
    assert len(receiver.received()) == 1
    assert sleeps == []
    assert codes(attempts) == [code]


# --------------------------------------------------------------------------
# 3. Retries
# --------------------------------------------------------------------------
RETRYABLE = [
    pytest.param("timeout", id="timeout"),
    pytest.param(429, id="429"),
    pytest.param(500, id="500"),
    pytest.param(502, id="502"),
    pytest.param(503, id="503"),
    pytest.param(599, id="599"),
]


@pytest.mark.parametrize("failure", RETRYABLE)
def test_retryable_failure_then_success(failure, receiver, sleeps):
    arm(receiver, failure, 1)
    status, attempts = run_deliver()

    assert status == "delivered"
    assert len(receiver.received()) == 2
    assert sleeps == [2]
    assert len(attempts) == 2


@pytest.mark.parametrize("failure", RETRYABLE)
def test_max_three_attempts_then_failed(failure, receiver, sleeps):
    arm(receiver, failure, 3)  # a 4th request would get 200, but must never be sent
    status, attempts = run_deliver()

    assert status == "failed"
    assert len(receiver.received()) == 3
    assert len(attempts) == 3
    assert sleeps == [2, 4]  # the final failure does not wait


def test_connection_refused_is_retried_then_failed(sleeps, monkeypatch):
    monkeypatch.setattr(worker, "RECEIVER_URL", CLOSED_PORT_URL)
    status, attempts = run_deliver()

    assert status == "failed"
    assert len(attempts) == 3
    assert all(a.status_code is None and isinstance(a.error, str) for a in attempts)
    assert sleeps == [2, 4]


def test_exponential_backoff_sequence(receiver, sleeps):
    arm(receiver, 500, 3)
    run_deliver()
    assert sleeps == [2, 4]  # no wait after the final attempt


def test_backoff_applies_to_network_errors_too(receiver, sleeps):
    arm(receiver, "timeout", 3)
    run_deliver()
    assert sleeps == [2, 4]


def test_success_on_last_attempt(receiver, sleeps):
    arm(receiver, 503, 2)
    status, attempts = run_deliver()

    assert status == "delivered"
    assert sleeps == [2, 4]
    assert codes(attempts) == [503, 503, 200]


def test_retry_after_larger_than_default_is_used(receiver, sleeps):
    receiver.control(fail_next=1, status_code=429, retry_after=30)
    status, _ = run_deliver()
    assert status == "delivered"
    assert sleeps == [30]


def test_retry_after_smaller_than_default_is_ignored(receiver, sleeps):
    # defaults are 2s (attempt 1) and 4s (attempt 2); Retry-After 1 is smaller than both
    receiver.control(fail_next=2, status_code=503, retry_after=1)
    run_deliver()
    assert sleeps == [2, 4]


def test_retry_after_applies_per_attempt(receiver, sleeps):
    receiver.control(fail_next=1, status_code=429, retry_after=10)
    sleeps.hooks = [
        lambda: receiver.control(fail_next=1, status_code=503, retry_after=1),
        lambda: receiver.control(fail_next=1, status_code=500, retry_after=60),
    ]
    status, _ = run_deliver()
    assert status == "failed"
    assert sleeps == [10, 4]  # Retry-After on the last attempt is not waited on


# NOTE: the old "non-numeric Retry-After falls back to default" test is gone:
# /control only accepts an integer retry_after, so the live receiver cannot
# emit "soon", "", an HTTP-date or "1.5".


# --------------------------------------------------------------------------
# 4. Non-retryable codes fail immediately
# --------------------------------------------------------------------------
@pytest.mark.parametrize("code", [301, 400, 401, 403, 404, 410, 422])
def test_other_codes_fail_immediately(code, receiver, sleeps):
    receiver.control(fail_next=1, status_code=code)
    status, attempts = run_deliver()

    assert status == "failed"
    assert len(receiver.received()) == 1
    assert sleeps == []
    assert codes(attempts) == [code]


def test_retry_after_ignored_on_non_retryable_code(receiver, sleeps):
    receiver.control(fail_next=1, status_code=400, retry_after=30)
    status, _ = run_deliver()
    assert status == "failed"
    assert sleeps == []


# --------------------------------------------------------------------------
# 5. Delivery attempt format
# --------------------------------------------------------------------------
def test_attempt_format_when_receiver_responds(receiver, sleeps):
    arm(receiver, 503, 1)
    _, attempts = run_deliver(make_event(pk=7))

    assert len(attempts) == 2
    for a in attempts:
        assert a.event_pk == 7
        assert isinstance(a.status_code, int)
        assert a.error is None
    assert codes(attempts) == [503, 200]


def test_attempt_format_on_http_error_is_not_an_error_string(receiver):
    receiver.control(fail_next=1, status_code=404)
    _, attempts = run_deliver()
    assert attempts[0].status_code == 404
    assert attempts[0].error is None


def test_attempt_format_on_timeout(receiver, sleeps):
    arm(receiver, "timeout", 1)
    _, attempts = run_deliver(make_event(pk=3))

    first, second = attempts
    assert first.event_pk == 3
    assert first.status_code is None
    assert isinstance(first.error, str)  # str(ReadTimeout) may legitimately be empty
    # the following successful attempt is in the "responded" format
    assert second.status_code == 200 and second.error is None


def test_attempt_format_on_connection_error(sleeps, monkeypatch):
    monkeypatch.setattr(worker, "RECEIVER_URL", CLOSED_PORT_URL)
    _, attempts = run_deliver(make_event(pk=3))

    first = attempts[0]
    assert first.event_pk == 3
    assert first.status_code is None
    assert isinstance(first.error, str) and first.error != ""


def test_mixed_attempt_history(receiver, sleeps):
    # attempt 1: timeout, attempt 2: 429, attempt 3: timeout
    arm(receiver, "timeout", 1)
    sleeps.hooks = [
        lambda: receiver.control(fail_next=1, status_code=429),
        lambda: receiver.control(delay_ms=DELAY_MS, delay_next=1),
    ]
    status, attempts = run_deliver()

    assert status == "failed"
    assert [(a.status_code, a.error is None) for a in attempts] == [
        (None, False),
        (429, True),
        (None, False),
    ]


# --------------------------------------------------------------------------
# End-to-end through main() with a fake database (real HTTP to the receiver)
# --------------------------------------------------------------------------
class FakeCursor:
    def __init__(self, conn):
        self.conn = conn

    def executemany(self, sql, rows):
        self.conn.executemany_calls.append((sql, list(rows)))


class FakeConn:
    def __init__(self):
        self.execute_calls = []
        self.executemany_calls = []
        self.commits = 0
        self.closed = False

    def execute(self, sql, params=None):
        self.execute_calls.append((sql, params))

    def cursor(self, *a, **kw):
        return FakeCursor(self)

    def commit(self):
        self.commits += 1

    def rollback(self):
        pass

    def close(self):
        self.closed = True


def run_main(monkeypatch, events):
    """Run worker.main() once over `events` against the live receiver / fake DB."""
    conn = FakeConn()
    batches = [list(events)]

    def fake_claim(_conn):
        if batches:
            return batches.pop(0)
        worker.shutdown.set()  # second poll: stop the loop
        return []

    monkeypatch.setattr(worker.psycopg, "connect", lambda *a, **kw: conn)
    monkeypatch.setattr(worker, "claim_pending", fake_claim)
    monkeypatch.setattr(worker.signal, "signal", lambda *a, **kw: None)
    # Real HTTP, but with a short timeout so the timeout scenario stays fast.
    monkeypatch.setattr(worker.httpx, "Client", lambda **kw: REAL_CLIENT(timeout=CLIENT_TIMEOUT))
    worker.main()
    return conn


def test_main_marks_delivered_and_records_attempts(monkeypatch, receiver, sleeps):
    arm(receiver, 503, 1)
    conn = run_main(monkeypatch, [make_event(pk=5)])

    (sql, params), = conn.execute_calls
    assert "UPDATE events" in sql
    assert params == ("delivered", 5)

    (sql, rows), = conn.executemany_calls
    assert "INSERT INTO delivery_attempts" in sql
    assert rows == [(5, 503, None), (5, 200, None)]
    assert conn.closed


def test_main_marks_failed_and_records_network_errors(monkeypatch, receiver, sleeps):
    arm(receiver, "timeout", 3)
    conn = run_main(monkeypatch, [make_event(pk=9)])

    assert conn.execute_calls[0][1] == ("failed", 9)
    rows = conn.executemany_calls[0][1]
    assert len(rows) == 3
    assert all(r[0] == 9 and r[1] is None and isinstance(r[2], str) for r in rows)


def test_main_processes_each_event_independently(monkeypatch, receiver, sleeps):
    # Event 1 gets the default 200; just before event 2 is delivered, make
    # the receiver answer its next request with 404.
    real_deliver = worker.deliver

    def deliver_with_setup(client, event):
        if event.id == 2:
            receiver.control(fail_next=1, status_code=404)
        return real_deliver(client, event)

    monkeypatch.setattr(worker, "deliver", deliver_with_setup)
    conn = run_main(monkeypatch, [make_event(1), make_event(2)])

    statuses = [params for _, params in conn.execute_calls]
    assert statuses == [("delivered", 1), ("failed", 2)]
    assert [rows for _, rows in conn.executemany_calls] == [[(1, 200, None)], [(2, 404, None)]]
