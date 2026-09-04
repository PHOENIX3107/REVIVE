import hashlib
import hmac
from datetime import datetime, timezone
import json
import os

import pytest
from fastapi.testclient import TestClient

from backend.db import Database
from backend.main import create_app
from backend.schemas import Order, OrderStatus, PaymentAttempt, PaymentError, PaymentMethod, PaymentStatus
from backend.webhooks.razorpay import RazorpayWebhookProcessor


DATABASE_URL = os.getenv("REVIVE_TEST_DATABASE_URL")
pytestmark = pytest.mark.skipif(
    not DATABASE_URL,
    reason="REVIVE_TEST_DATABASE_URL is required for PostgreSQL integration tests",
)


NOW = datetime(2026, 1, 1, tzinfo=timezone.utc)
NOW_TS = int(NOW.timestamp())
TEST_WEBHOOK_SECRET = "revive-step-12c-test-secret"


@pytest.fixture
def webhook_secret(monkeypatch):
    monkeypatch.setenv("RAZORPAY_WEBHOOK_SECRET", TEST_WEBHOOK_SECRET)
    return TEST_WEBHOOK_SECRET


@pytest.fixture
def database():
    database = Database(DATABASE_URL)
    database.initialize()
    with database.transaction() as connection:
        connection.execute(
            "TRUNCATE payment_outcomes, audit_events, executions, decisions, diagnoses, "
            "recovery_cases, webhook_events, payment_attempts, orders CASCADE"
        )
    return database


@pytest.fixture
def client(database, webhook_secret):
    app = create_app(database)
    with TestClient(app) as test_client:
        yield test_client


def _count_rows(database, table_name):
    with database.connection() as connection:
        return connection.execute(f"SELECT count(*) AS count FROM {table_name}").fetchone()["count"]


def _webhook_payload(
    *,
    event_id="evt_api_1",
    event_type="payment.failed",
    payment_id="pay_api_1",
    order_id="order_api_1",
    amount=49900,
    currency="INR",
    status="failed",
    method="card",
    include_order=False,
):
    payload = {
        "event": event_type,
        "created_at": NOW_TS,
        "payload": {
            "payment": {
                "entity": {
                    "id": payment_id,
                    "order_id": order_id,
                    "amount": amount,
                    "currency": currency,
                    "status": status,
                    "captured": status == "captured",
                    "method": method,
                    "created_at": NOW_TS,
                    "card": {"iin": "411111"},
                    "error": {
                        "code": "insufficient_funds",
                        "description": "The card has insufficient funds.",
                        "source": "bank",
                        "step": "payment_authentication",
                        "reason": "funds",
                    }
                    if status == "failed"
                    else None,
                }
            }
        },
    }
    if include_order:
        payload["payload"]["order"] = {
            "entity": {
                "id": order_id,
                "amount": amount,
                "amount_paid": amount if status == "captured" else 0,
                "amount_due": 0 if status == "captured" else amount,
                "currency": currency,
                "status": "paid" if status == "captured" else "attempted",
                "attempts": 1,
                "created_at": NOW_TS,
            }
        }
    return event_id, json.dumps(payload).encode("utf-8")


def _order_paid_payload(*, event_id="evt_order_paid", order_id="order_api_1"):
    payload = {
        "event": "order.paid",
        "created_at": NOW_TS,
        "payload": {
            "order": {
                "entity": {
                    "id": order_id,
                    "amount": 49900,
                    "amount_paid": 49900,
                    "amount_due": 0,
                    "currency": "INR",
                    "status": "paid",
                    "attempts": 1,
                    "created_at": NOW_TS,
                }
            }
        },
    }
    return event_id, json.dumps(payload).encode("utf-8")


def _webhook_headers(event_id, body, secret=TEST_WEBHOOK_SECRET):
    signature = hmac.new(secret.encode("utf-8"), body, hashlib.sha256).hexdigest()
    return {
        "x-razorpay-event-id": event_id,
        "X-Razorpay-Signature": signature,
    }


def test_health_endpoint_reports_ok(client):
    response = client.get("/health")
    assert response.status_code == 200
    assert response.json() == {"status": "ok"}


def test_lookup_endpoints_return_saved_resources(client, database):
    database.save_order(
        Order(
            order_id="order_lookup",
            amount=49900,
            amount_paid=0,
            amount_due=49900,
            currency="INR",
            status=OrderStatus.attempted,
            attempts=1,
            created_at=NOW,
        )
    )
    database.save_payment_attempt(
        PaymentAttempt(
            payment_id="pay_lookup",
            order_id="order_lookup",
            amount=49900,
            method=PaymentMethod.card,
            status=PaymentStatus.failed,
            captured=False,
            created_at=NOW,
            error=PaymentError(code="insufficient_funds"),
            issuer_bin="411111",
            failed_at=NOW,
        )
    )
    database.save_recovery_case("case_lookup", "pay_lookup", "order_lookup", "open")

    order_response = client.get("/orders/order_lookup")
    payment_response = client.get("/payments/pay_lookup")
    recovery_case_response = client.get("/recovery-cases/case_lookup")

    assert order_response.status_code == 200
    assert order_response.json()["order_id"] == "order_lookup"
    assert order_response.json()["status"] == "attempted"

    assert payment_response.status_code == 200
    assert payment_response.json()["payment_id"] == "pay_lookup"
    assert payment_response.json()["status"] == "failed"

    assert recovery_case_response.status_code == 200
    assert recovery_case_response.json()["case_id"] == "case_lookup"
    assert recovery_case_response.json()["status"] == "open"


def test_webhook_persists_event_and_state(client, database):
    event_id, body = _webhook_payload(include_order=True)
    response = client.post(
        "/webhooks/razorpay",
        content=body,
        headers=_webhook_headers(event_id, body),
    )

    assert response.status_code == 200
    assert response.json()["status"] == "accepted"
    assert response.json()["event_id"] == event_id

    assert _count_rows(database, "webhook_events") == 1
    assert _count_rows(database, "payment_attempts") == 1

    order = database.get_order("order_api_1")
    payment = database.get_payment_attempt("pay_api_1")
    assert order is not None
    assert payment is not None
    assert order.status.value == "attempted"
    assert payment.status.value == "failed"


def test_duplicate_webhook_is_safe(client, database):
    event_id, body = _webhook_payload(event_id="evt_api_duplicate")
    first = client.post(
        "/webhooks/razorpay",
        content=body,
        headers=_webhook_headers(event_id, body),
    )
    second = client.post(
        "/webhooks/razorpay",
        content=body,
        headers=_webhook_headers(event_id, body),
    )

    assert first.status_code == 200
    assert first.json()["status"] == "accepted"
    assert second.status_code == 200
    assert second.json()["status"] == "duplicate"
    assert _count_rows(database, "webhook_events") == 1
    assert _count_rows(database, "payment_attempts") == 1


def test_malformed_webhook_returns_400_and_persists_nothing(client, database):
    response = client.post(
        "/webhooks/razorpay",
        content=b"{not-json",
        headers=_webhook_headers("evt_api_malformed", b"{not-json"),
    )

    assert response.status_code == 400
    assert _count_rows(database, "webhook_events") == 0
    assert _count_rows(database, "payment_attempts") == 0


def test_webhook_transaction_rolls_back_on_internal_error(database, webhook_secret):
    app = create_app(database)

    def _raise(*_args, **_kwargs):
        raise RuntimeError("forced failure")

    original = RazorpayWebhookProcessor._persist_order_state
    RazorpayWebhookProcessor._persist_order_state = _raise
    try:
        with TestClient(app, raise_server_exceptions=False) as client:
            event_id, body = _webhook_payload(event_id="evt_api_rollback", include_order=True)
            response = client.post(
                "/webhooks/razorpay",
                content=body,
                headers=_webhook_headers(event_id, body),
            )

        assert response.status_code == 500
        assert _count_rows(database, "webhook_events") == 0
        assert _count_rows(database, "payment_attempts") == 0
        assert database.get_order("order_api_1") is None
    finally:
        RazorpayWebhookProcessor._persist_order_state = original


def test_invalid_webhook_signature_returns_400_and_persists_nothing(client, database):
    event_id, body = _webhook_payload(event_id="evt_api_invalid_signature")
    response = client.post(
        "/webhooks/razorpay",
        content=body,
        headers={
            "x-razorpay-event-id": event_id,
            "X-Razorpay-Signature": "invalid",
        },
    )

    assert response.status_code == 400
    assert _count_rows(database, "webhook_events") == 0
    assert _count_rows(database, "payment_attempts") == 0


def test_missing_webhook_signature_returns_400_and_persists_nothing(client, database):
    event_id, body = _webhook_payload(event_id="evt_api_missing_signature")
    response = client.post(
        "/webhooks/razorpay",
        content=body,
        headers={"x-razorpay-event-id": event_id},
    )

    assert response.status_code == 400
    assert _count_rows(database, "webhook_events") == 0
    assert _count_rows(database, "payment_attempts") == 0


def test_failed_then_captured_supersedes_failed_state(client, database):
    failed_id, failed_body = _webhook_payload(event_id="evt_failed_first")
    captured_id, captured_body = _webhook_payload(
        event_id="evt_captured_second",
        event_type="payment.captured",
        status="captured",
    )

    assert client.post(
        "/webhooks/razorpay",
        content=failed_body,
        headers=_webhook_headers(failed_id, failed_body),
    ).status_code == 200
    assert client.post(
        "/webhooks/razorpay",
        content=captured_body,
        headers=_webhook_headers(captured_id, captured_body),
    ).status_code == 200

    payment = database.get_payment_attempt("pay_api_1")
    order = database.get_order("order_api_1")
    recovery_case = database.get_recovery_case("recovery-pay_api_1")
    assert payment is not None and payment.status is PaymentStatus.captured
    assert order is not None and order.status is OrderStatus.paid
    assert recovery_case is not None and recovery_case["status"] == "recovered"


def test_captured_then_failed_does_not_regress_captured_state(client, database):
    captured_id, captured_body = _webhook_payload(
        event_id="evt_captured_first",
        event_type="payment.captured",
        status="captured",
    )
    failed_id, failed_body = _webhook_payload(event_id="evt_failed_second")

    assert client.post(
        "/webhooks/razorpay",
        content=captured_body,
        headers=_webhook_headers(captured_id, captured_body),
    ).status_code == 200
    assert client.post(
        "/webhooks/razorpay",
        content=failed_body,
        headers=_webhook_headers(failed_id, failed_body),
    ).status_code == 200

    payment = database.get_payment_attempt("pay_api_1")
    order = database.get_order("order_api_1")
    assert payment is not None and payment.status is PaymentStatus.captured
    assert order is not None and order.status is OrderStatus.paid


def test_failed_then_order_paid_does_not_regress_order(client, database):
    failed_id, failed_body = _webhook_payload(event_id="evt_failed_before_order_paid")
    paid_id, paid_body = _order_paid_payload(event_id="evt_order_paid_after_failure")

    assert client.post(
        "/webhooks/razorpay",
        content=failed_body,
        headers=_webhook_headers(failed_id, failed_body),
    ).status_code == 200
    assert client.post(
        "/webhooks/razorpay",
        content=paid_body,
        headers=_webhook_headers(paid_id, paid_body),
    ).status_code == 200

    payment = database.get_payment_attempt("pay_api_1")
    order = database.get_order("order_api_1")
    assert payment is not None and payment.status is PaymentStatus.failed
    assert order is not None and order.status is OrderStatus.paid


def test_duplicate_captured_webhook_is_idempotent(client, database):
    event_id, body = _webhook_payload(
        event_id="evt_captured_duplicate",
        event_type="payment.captured",
        status="captured",
    )
    headers = _webhook_headers(event_id, body)

    first = client.post("/webhooks/razorpay", content=body, headers=headers)
    second = client.post("/webhooks/razorpay", content=body, headers=headers)

    assert first.status_code == 200
    assert first.json()["status"] == "accepted"
    assert second.status_code == 200
    assert second.json()["status"] == "duplicate"
    assert _count_rows(database, "webhook_events") == 1
    assert _count_rows(database, "payment_attempts") == 1
    payment = database.get_payment_attempt("pay_api_1")
    assert payment is not None and payment.status is PaymentStatus.captured


def test_eligible_failed_webhook_creates_one_recovery_case(client, database):
    event_id, body = _webhook_payload(event_id="evt_failed_case")

    response = client.post(
        "/webhooks/razorpay",
        content=body,
        headers=_webhook_headers(event_id, body),
    )

    assert response.status_code == 200
    recovery_case = database.get_recovery_case("recovery-pay_api_1")
    assert recovery_case is not None
    assert recovery_case["payment_id"] == "pay_api_1"
    assert recovery_case["order_id"] == "order_api_1"
    assert recovery_case["status"] == "open"
    assert _count_rows(database, "recovery_cases") == 1


def test_duplicate_failed_webhook_keeps_one_recovery_case(client, database):
    event_id, body = _webhook_payload(event_id="evt_failed_case_duplicate")
    headers = _webhook_headers(event_id, body)

    first = client.post("/webhooks/razorpay", content=body, headers=headers)
    second = client.post("/webhooks/razorpay", content=body, headers=headers)

    assert first.status_code == 200
    assert second.status_code == 200
    assert second.json()["status"] == "duplicate"
    assert _count_rows(database, "recovery_cases") == 1


def test_unsupported_non_card_failure_creates_no_recovery_case(client, database):
    event_id, body = _webhook_payload(event_id="evt_upi_failure", method="upi")

    response = client.post(
        "/webhooks/razorpay",
        content=body,
        headers=_webhook_headers(event_id, body),
    )

    assert response.status_code == 200
    assert _count_rows(database, "recovery_cases") == 0


def test_captured_after_failed_updates_existing_case_without_duplicate(client, database):
    failed_id, failed_body = _webhook_payload(event_id="evt_case_failed")
    captured_id, captured_body = _webhook_payload(
        event_id="evt_case_captured",
        event_type="payment.captured",
        status="captured",
    )

    client.post(
        "/webhooks/razorpay",
        content=failed_body,
        headers=_webhook_headers(failed_id, failed_body),
    )
    response = client.post(
        "/webhooks/razorpay",
        content=captured_body,
        headers=_webhook_headers(captured_id, captured_body),
    )

    assert response.status_code == 200
    assert _count_rows(database, "recovery_cases") == 1
    recovery_case = database.get_recovery_case("recovery-pay_api_1")
    assert recovery_case is not None and recovery_case["status"] == "recovered"


def test_failed_webhook_case_write_rolls_back_without_orphan_case(database, webhook_secret):
    app = create_app(database)

    def _raise(*_args, **_kwargs):
        raise RuntimeError("forced recovery case failure")

    original = RazorpayWebhookProcessor._persist_recovery_case
    RazorpayWebhookProcessor._persist_recovery_case = _raise
    try:
        with TestClient(app, raise_server_exceptions=False) as client:
            event_id, body = _webhook_payload(event_id="evt_case_rollback")
            response = client.post(
                "/webhooks/razorpay",
                content=body,
                headers=_webhook_headers(event_id, body),
            )

        assert response.status_code == 500
        assert _count_rows(database, "webhook_events") == 0
        assert _count_rows(database, "payment_attempts") == 0
        assert _count_rows(database, "recovery_cases") == 0
        assert database.get_order("order_api_1") is None
    finally:
        RazorpayWebhookProcessor._persist_recovery_case = original


def test_out_of_order_paid_then_failed_does_not_regress_order(client, database):
    paid_id, paid_body = _order_paid_payload(event_id="evt_paid_first")
    failed_id, failed_body = _webhook_payload(event_id="evt_failed_late")

    assert client.post(
        "/webhooks/razorpay",
        content=paid_body,
        headers=_webhook_headers(paid_id, paid_body),
    ).status_code == 200
    assert client.post(
        "/webhooks/razorpay",
        content=failed_body,
        headers=_webhook_headers(failed_id, failed_body),
    ).status_code == 200

    order = database.get_order("order_api_1")
    assert order is not None and order.status is OrderStatus.paid
