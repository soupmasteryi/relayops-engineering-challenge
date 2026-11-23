import os
import re
import uuid

import httpx
import pytest

BASE_URL = os.environ.get("API_BASE_URL", "http://localhost:8080")


# --------------------------------------------------------------------------
# Fixtures
# --------------------------------------------------------------------------
@pytest.fixture(scope="session")
def client():
    with httpx.Client(base_url=BASE_URL, timeout=10.0) as c:
        try:
            c.get("/health")
        except httpx.ConnectError:
            pytest.skip(f"API not reachable at {BASE_URL}", allow_module_level=True)
        yield c


@pytest.fixture
def uid():
    """Unique suffix so repeated runs against a persistent DB don't collide."""
    return uuid.uuid4().hex


@pytest.fixture
def event(uid):
    return {
        "event_id": f"evt-{uid}",
        "type": "invoice.created",
        "payload": {"invoice_id": 1234, "amount": 250},
    }


VALID_REQUEST_ID = re.compile(r"^[A-Za-z0-9._-]{2,256}$")
REQUEST_ID_HEADER = "X-Request-ID"


@pytest.mark.parametrize(
    "request_id",
    [
        pytest.param("ab", id="min-length-2"),
        pytest.param("a" * 256, id="max-length-256"),
        pytest.param("abc123", id="lowercase-and-digits"),
        pytest.param("ABCdef789", id="mixed-case"),
        pytest.param("1234567890", id="digits-only"),
        pytest.param("tests.symbols_1-2-3", id="symbols"),
        pytest.param("a1" * 32, id="typical-length"),
    ],
)
def test_request_id_valid_client_value_is_echoed(client, request_id):
    response = client.get("/health", headers={REQUEST_ID_HEADER: request_id})

    assert response.status_code == 200
    assert response.headers.get(REQUEST_ID_HEADER) == request_id


def test_request_id_valid_client_value_is_echoed_with_unique_value(client, uid):
    response = client.get("/health", headers={REQUEST_ID_HEADER: uid})

    assert response.status_code == 200
    assert response.headers.get(REQUEST_ID_HEADER) == uid


def test_request_id_header_name_is_case_insensitive(client, uid):
    response = client.get("/health", headers={REQUEST_ID_HEADER.lower(): uid})

    assert response.status_code == 200
    assert response.headers.get(REQUEST_ID_HEADER) == uid


def test_request_id_missing_header_gets_generated_id(client):
    response = client.get("/health")

    assert response.status_code == 200
    generated = response.headers.get(REQUEST_ID_HEADER)
    assert generated, "nginx should generate an X-Request-ID when none is sent"
    assert VALID_REQUEST_ID.match(generated)


def test_request_id_generated_ids_are_unique_per_request(client):
    ids = {client.get("/health").headers.get(REQUEST_ID_HEADER) for _ in range(5)}

    assert None not in ids
    assert len(ids) == 5


@pytest.mark.parametrize(
    "request_id",
    [
        pytest.param("", id="empty"),
        pytest.param("a", id="too-short-1"),
        pytest.param("a" * 257, id="too-long-257"),
        pytest.param("a" * 1000, id="way-too-long"),
        pytest.param("abc def", id="contains-space"),
        pytest.param("abc/def", id="contains-slash"),
        pytest.param("abc;def", id="contains-semicolon"),
        pytest.param("abc!", id="contains-exclamation"),
        pytest.param("<script>", id="contains-angle-brackets"),
    ],
)
def test_request_id_invalid_client_value_is_replaced(client, request_id):
    response = client.get("/health", headers={REQUEST_ID_HEADER: request_id})

    assert response.status_code == 200
    returned = response.headers.get(REQUEST_ID_HEADER)
    assert returned, "nginx should generate an X-Request-ID for an invalid one"
    assert returned != request_id
    assert VALID_REQUEST_ID.match(returned)


def test_request_id_invalid_values_get_distinct_generated_ids(client):
    first = client.get("/health", headers={REQUEST_ID_HEADER: "a"})
    second = client.get("/health", headers={REQUEST_ID_HEADER: "a"})

    assert first.headers.get(REQUEST_ID_HEADER)
    assert second.headers.get(REQUEST_ID_HEADER)
    assert first.headers[REQUEST_ID_HEADER] != second.headers[REQUEST_ID_HEADER]


# --------------------------------------------------------------------------
# GET /health
# --------------------------------------------------------------------------
def test_health_returns_ok(client):
    response = client.get("/health")

    assert response.status_code == 200
    assert response.json() == {"status": "ok"}


# --------------------------------------------------------------------------
# POST /events - new event
# --------------------------------------------------------------------------
def test_post_new_event_returns_202(client, event):
    response = client.post("/events", json=event)

    assert response.status_code == 202
    body = response.json()
    assert body["event_id"] == event["event_id"]
    assert "status" in body


def test_post_new_event_is_persisted(client, event):
    assert client.post("/events", json=event).status_code == 202

    response = client.get(f"/events/{event['event_id']}")

    assert response.status_code == 200
    stored = response.json()
    assert stored["event_id"] == event["event_id"]
    assert stored["type"] == event["type"]
    assert stored["payload"] == event["payload"]


def test_post_different_event_ids_create_separate_events(client, event):
    other = {**event, "event_id": f"{event['event_id']}-other"}

    assert client.post("/events", json=event).status_code == 202
    assert client.post("/events", json=other).status_code == 202

    assert client.get(f"/events/{event['event_id']}").status_code == 200
    assert client.get(f"/events/{other['event_id']}").status_code == 200


# --------------------------------------------------------------------------
# POST /events - existing event_id (idempotency / conflict)
# --------------------------------------------------------------------------
def test_post_duplicate_same_type_and_payload_returns_200_with_old_event(client, event):
    first = client.post("/events", json=event)
    second = client.post("/events", json=event)

    assert first.status_code == 202
    assert second.status_code == 200
    body = second.json()
    assert body["event_id"] == event["event_id"]
    assert body["status"] in ["pending", "processing", "delivered", "failed"]


def test_post_duplicate_does_not_create_second_event(client, event):
    client.post("/events", json=event)
    client.post("/events", json=event)

    # The API exposes one event per event_id; it must still be the original.
    response = client.get(f"/events/{event['event_id']}")
    assert response.status_code == 200
    assert response.json()["type"] == event["type"]
    assert response.json()["payload"] == event["payload"]


def test_post_duplicate_ignores_payload_key_order(client, event):
    client.post("/events", json=event)
    reordered = {**event, "payload": {"amount": 250, "invoice_id": 1234}}

    response = client.post("/events", json=reordered)

    assert response.status_code == 200


@pytest.mark.parametrize(
    "changes",
    [
        pytest.param({"type": "invoice.paid"}, id="different-type"),
        pytest.param({"payload": {"invoice_id": 1234, "amount": 999}}, id="different-payload"),
        pytest.param({"payload": {"invoice_id": 1234}}, id="payload-missing-key"),
        pytest.param({"payload": {}}, id="empty-payload"),
        pytest.param(
            {"type": "invoice.paid", "payload": {"other": True}},
            id="different-type-and-payload",
        ),
    ],
)
def test_post_duplicate_event_id_with_different_content_returns_409(client, event, changes):
    client.post("/events", json=event)

    response = client.post("/events", json={**event, **changes})

    assert response.status_code == 409


def test_conflict_does_not_modify_existing_event(client, event):
    client.post("/events", json=event)
    client.post("/events", json={**event, "type": "invoice.paid"})

    stored = client.get(f"/events/{event['event_id']}").json()
    assert stored["type"] == event["type"]
    assert stored["payload"] == event["payload"]


# --------------------------------------------------------------------------
# POST /events - validation
# --------------------------------------------------------------------------
# Each param is a function of a unique id, so the "nothing was created" check
# can look the event up without colliding with data from earlier runs.
@pytest.mark.parametrize(
    "make_body",
    [
        pytest.param(lambda u: {"type": "t", "payload": {}}, id="missing-event_id"),
        pytest.param(lambda u: {"event_id": u, "payload": {}}, id="missing-type"),
        pytest.param(lambda u: {"event_id": u, "type": "t"}, id="missing-payload"),
        pytest.param(lambda u: {}, id="empty-object"),
        pytest.param(lambda u: {"event_id": "", "type": "t", "payload": {}}, id="empty-event_id"),
        pytest.param(lambda u: {"event_id": u, "type": "", "payload": {}}, id="empty-type"),
        pytest.param(
            lambda u: {"event_id": "x" * 201, "type": "t", "payload": {}},
            id="event_id-too-long",
        ),
        pytest.param(
            lambda u: {"event_id": u, "type": "x" * 201, "payload": {}},
            id="type-too-long",
        ),
        pytest.param(
            lambda u: {"event_id": u, "type": "t", "payload": [1, 2, 3]},
            id="payload-is-list",
        ),
        pytest.param(
            lambda u: {"event_id": u, "type": "t", "payload": "string"},
            id="payload-is-string",
        ),
        pytest.param(
            lambda u: {"event_id": u, "type": "t", "payload": None},
            id="payload-is-null",
        ),
        pytest.param(
            lambda u: {"event_id": None, "type": "t", "payload": {}},
            id="event_id-is-null",
        ),
        pytest.param(
            lambda u: {"event_id": 123, "type": "t", "payload": {}},
            id="event_id-is-int",
        ),
    ],
)
def test_post_invalid_body_returns_422(client, uid, make_body):
    body = make_body(uid)

    response = client.post("/events", json=body)

    assert response.status_code == 422
    # If the body carried a usable event_id, confirm nothing was stored.
    event_id = body.get("event_id")
    if isinstance(event_id, str) and event_id:
        assert client.get(f"/events/{event_id}").status_code == 404


def test_post_non_json_body_returns_422(client):
    response = client.post(
        "/events",
        content="this is not json",
        headers={"Content-Type": "application/json"},
    )

    assert response.status_code == 422


def test_post_without_body_returns_422(client):
    response = client.post("/events")

    assert response.status_code == 422


def test_post_accepts_boundary_length_fields(client, uid):
    event = {
        "event_id": uid + "x" * (200 - len(uid)),
        "type": "y" * 200,
        "payload": {},
    }
    assert len(event["event_id"]) == 200

    response = client.post("/events", json=event)

    assert response.status_code == 202


# --------------------------------------------------------------------------
# GET /events/{event_id}
# --------------------------------------------------------------------------
def test_get_existing_event_returns_200(client, event):
    client.post("/events", json=event)

    response = client.get(f"/events/{event['event_id']}")

    assert response.status_code == 200
    body = response.json()
    assert body["event_id"] == event["event_id"]
    assert body["type"] == event["type"]
    assert body["payload"] == event["payload"]
    assert "status" in body
    assert "attempts" in body
    assert "delivery_attempts" in body
    assert "created_at" in body
    assert "updated_at" in body


def test_get_unknown_event_returns_404(client, uid):
    response = client.get(f"/events/does-not-exist-{uid}")

    assert response.status_code == 404


def test_get_returns_the_requested_event_among_several(client, event):
    other = {**event, "event_id": f"{event['event_id']}-2", "type": "invoice.paid"}
    client.post("/events", json=event)
    client.post("/events", json=other)

    response = client.get(f"/events/{other['event_id']}")

    assert response.status_code == 200
    assert response.json()["event_id"] == other["event_id"]
    assert response.json()["type"] == "invoice.paid"




# --------------------------------------------------------------------------
# GET /metrics
# --------------------------------------------------------------------------
METRICS_SAMPLES = re.compile(
    r'relayops_events\{status="pending"} \d+\n'
    r'relayops_events\{status="processing"} \d+\n'
    r'relayops_events\{status="delivered"} \d+\n'
    r'relayops_events\{status="failed"} \d+\n'
    r"relayops_delivery_attempts_total \d+\n"
)


def test_metrics_matches_expected_format(client):
    response = client.get("/metrics")

    assert response.status_code == 200
    assert response.headers["content-type"].startswith("text/plain")

    # Ignore the "# HELP" / "# TYPE" comment lines; the rest must look like:
    #   relayops_events{status="pending"} 3
    #   relayops_events{status="processing"} 0
    #   relayops_events{status="delivered"} 120
    #   relayops_events{status="failed"} 2
    #   relayops_delivery_attempts_total 131
    samples = "".join(
        line + "\n"
        for line in response.text.splitlines()
        if not line.startswith("#")
    )
    assert METRICS_SAMPLES.fullmatch(samples), samples
