from datetime import datetime, timezone
from concurrent.futures import ThreadPoolExecutor
import hashlib
import hmac
import json
import os

import httpx
import pytest
from fastapi.testclient import TestClient

from backend.db import Database
from backend.integrations.razorpay.client import RazorpayClient, RazorpayClientConfig
from backend.main import create_app
from backend.recovery.reconciliation import RazorpayPaymentReconciler
from backend.schemas import Order, OrderStatus, PaymentAttempt, PaymentError, PaymentMethod, PaymentStatus
from backend.webhooks.razorpay import RazorpayWebhookProcessor


DATABASE_URL = os.getenv("REVIVE_TEST_DATABASE_URL")
pytestmark = pytest.mark.skipif(
    not DATABASE_URL,
    reason="REVIVE_TEST_DATABASE_URL is required for PostgreSQL integration tests",
)


NOW = datetime(2026, 1, 1, tzinfo=timezone.utc)
NOW_TS = int(NOW.timestamp())


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


def _seed_case(database, *, payment_status=PaymentStatus.failed, case_status="open"):
    order_status = OrderStatus.paid if payment_status is PaymentStatus.captured else OrderStatus.attempted
    database.save_order(
        Order(
            order_id="order_verify",
            amount=49900,
            amount_paid=49900 if order_status is OrderStatus.paid else 0,
            amount_due=0 if order_status is OrderStatus.paid else 49900,
            currency="INR",
            status=order_status,
            attempts=1,
            created_at=NOW,
        )
    )
    database.save_payment_attempt(
        PaymentAttempt(
            payment_id="pay_verify",
            order_id="order_verify",
            amount=49900,
            method=PaymentMethod.card,
            status=payment_status,
            captured=payment_status is PaymentStatus.captured,
            created_at=NOW,
            error=PaymentError(code="insufficient_funds") if payment_status is PaymentStatus.failed else None,
            issuer_bin="411111",
            failed_at=NOW if payment_status is PaymentStatus.failed else None,
        )
    )
    database.save_recovery_case("case_verify", "pay_verify", "order_verify", case_status)


def _provider_payment(
    status,
    *,
    amount=49900,
    currency="INR",
    amount_captured=None,
):
    payload = {
        "id": "pay_verify",
        "entity": "payment",
        "amount": amount,
        "currency": currency,
        "status": status,
        "method": "card",
        "order_id": "order_verify",
        "captured": status == "captured",
        "amount_captured": amount if status == "captured" else 0,
        "created_at": NOW_TS,
        "card": {"iin": "411111"},
    }
    if amount_captured is not None:
        payload["amount_captured"] = amount_captured
    if status == "failed":
        payload.update(
            {
                "error_code": "insufficient_funds",
                "error_description": "The card has insufficient funds.",
                "error_source": "bank",
                "error_step": "payment_authentication",
                "error_reason": "funds",
            }
        )
    return payload


def _provider_order(status, *, amount=49900, currency="INR"):
    return {
        "id": "order_verify",
        "entity": "order",
        "amount": amount,
        "amount_paid": amount if status == "paid" else 0,
        "amount_due": 0 if status == "paid" else amount,
        "currency": currency,
        "status": status,
        "attempts": 1,
        "created_at": NOW_TS,
    }


def _reconciler(
    database,
    *,
    payment_status,
    order_status,
    error_status=None,
    payment_amount=49900,
    payment_currency="INR",
    payment_amount_captured=None,
    order_amount=49900,
    order_currency="INR",
):
    requests = []

    def handler(request: httpx.Request) -> httpx.Response:
        requests.append(request)
        if error_status is not None:
            return httpx.Response(error_status, json={"error": "test error"})
        if request.url.path.endswith("/payments/pay_verify"):
            return httpx.Response(
                200,
                json=_provider_payment(
                    payment_status,
                    amount=payment_amount,
                    currency=payment_currency,
                    amount_captured=payment_amount_captured,
                ),
            )
        if request.url.path.endswith("/orders/order_verify"):
            return httpx.Response(
                200,
                json=_provider_order(order_status, amount=order_amount, currency=order_currency),
            )
        return httpx.Response(404)

    http_client = httpx.Client(transport=httpx.MockTransport(handler))
    client = RazorpayClient(
        RazorpayClientConfig("rzp_test_key", "test-secret", "https://test.example/v1"),
        http_client=http_client,
    )
    return RazorpayPaymentReconciler(database, client), http_client, requests


def _count(database, table):
    with database.connection() as connection:
        return connection.execute(f"SELECT count(*) AS count FROM {table}").fetchone()["count"]


def _outcome(database):
    with database.connection() as connection:
        return connection.execute(
            "SELECT status, amount_recovered FROM payment_outcomes WHERE payment_id = %s",
            ("pay_verify",),
        ).fetchone()


def _signed_webhook(event_id, event_type, *, payment_status=None):
    payload = {
        "event": event_type,
        "created_at": NOW_TS,
        "payload": {
            "order": {
                "entity": {
                    "id": "order_verify",
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
    if payment_status is not None:
        payload["payload"]["payment"] = {
            "entity": {
                "id": "pay_verify",
                "order_id": "order_verify",
                "amount": 49900,
                "currency": "INR",
                "status": payment_status,
                "captured": payment_status == "captured",
                "amount_captured": 49900 if payment_status == "captured" else 0,
                "method": "card",
                "created_at": NOW_TS,
                "card": {"iin": "411111"},
            }
        }
    body = json.dumps(payload).encode("utf-8")
    signature = hmac.new(b"reconciliation-secret", body, hashlib.sha256).hexdigest()
    return body, {
        "x-razorpay-event-id": event_id,
        "X-Razorpay-Signature": signature,
    }


def test_failed_payment_provider_captured_reconciles_case_and_outcome(database):
    _seed_case(database)
    reconciler, http_client, requests = _reconciler(
        database, payment_status="captured", order_status="paid"
    )
    try:
        with TestClient(create_app(database, payment_reconciler=reconciler)) as client:
            response = client.post("/recovery-cases/case_verify/verify-payment")
    finally:
        http_client.close()

    assert response.status_code == 200
    body = response.json()
    assert body["provider_payment_status"] == "captured"
    assert body["provider_order_status"] == "paid"
    assert body["revive_payment_status"] == "captured"
    assert body["order_status"] == "paid"
    assert body["state_changed"] is True
    assert body["recovery_confirmed"] is True
    assert body["recovered_amount"] == 49900
    assert body["reconciliation_recorded"] is True
    assert [request.url.path for request in requests] == [
        "/v1/payments/pay_verify",
        "/v1/orders/order_verify",
    ]
    assert database.get_payment_attempt("pay_verify").status is PaymentStatus.captured
    assert database.get_order("order_verify").status is OrderStatus.paid
    assert database.get_recovery_case("case_verify")["status"] == "recovered"
    assert _outcome(database) == {"status": "recovered", "amount_recovered": 49900}
    assert _count(database, "audit_events") == 1


def test_provider_non_success_keeps_case_open_without_recovered_revenue(database):
    _seed_case(database)
    reconciler, http_client, _ = _reconciler(
        database, payment_status="failed", order_status="attempted"
    )
    try:
        with TestClient(create_app(database, payment_reconciler=reconciler)) as client:
            response = client.post("/recovery-cases/case_verify/verify-payment")
    finally:
        http_client.close()

    assert response.status_code == 200
    body = response.json()
    assert body["recovery_confirmed"] is False
    assert body["recovered_amount"] == 0
    assert body["revive_payment_status"] == "failed"
    assert body["order_status"] == "attempted"
    assert database.get_recovery_case("case_verify")["status"] == "open"
    assert _outcome(database) is None
    assert _count(database, "audit_events") == 1


def test_paid_order_does_not_confirm_a_different_failed_payment(database):
    _seed_case(database)
    reconciler, http_client, _ = _reconciler(
        database, payment_status="failed", order_status="paid"
    )
    try:
        result = reconciler.verify_case("case_verify")
    finally:
        http_client.close()

    assert result.recovery_confirmed is False
    assert result.provider_order_status == "paid"
    assert result.revive_payment_status is PaymentStatus.failed
    assert result.order_status is OrderStatus.paid
    assert database.get_recovery_case("case_verify")["status"] == "open"
    assert _outcome(database) is None


@pytest.mark.parametrize("provider_status", ["created", "refunded"])
def test_created_and_refunded_do_not_become_authorized_or_recovered(database, provider_status):
    _seed_case(database)
    reconciler, http_client, _ = _reconciler(
        database, payment_status=provider_status, order_status="attempted"
    )
    try:
        result = reconciler.verify_case("case_verify")
    finally:
        http_client.close()

    assert result.provider_payment_status == provider_status
    assert result.revive_payment_status is PaymentStatus.failed
    assert result.recovery_confirmed is False
    assert database.get_payment_attempt("pay_verify").status is PaymentStatus.failed
    assert database.get_recovery_case("case_verify")["status"] == "open"
    assert _outcome(database) is None


@pytest.mark.parametrize(
    ("payment_amount_captured", "payment_currency"),
    [(49800, "INR"), (49900, "USD")],
)
def test_captured_monetary_mismatch_creates_no_recovered_outcome(
    database,
    payment_amount_captured,
    payment_currency,
):
    _seed_case(database)
    reconciler, http_client, _ = _reconciler(
        database,
        payment_status="captured",
        order_status="paid",
        payment_amount_captured=payment_amount_captured,
        payment_currency=payment_currency,
    )
    try:
        with TestClient(create_app(database, payment_reconciler=reconciler)) as client:
            response = client.post("/recovery-cases/case_verify/verify-payment")
    finally:
        http_client.close()

    assert response.status_code == 409
    assert database.get_payment_attempt("pay_verify").status is PaymentStatus.failed
    assert database.get_order("order_verify").status is OrderStatus.attempted
    assert database.get_recovery_case("case_verify")["status"] == "open"
    assert _outcome(database) is None


def test_captured_payment_is_not_regressed_by_provider_stale_failure(database):
    _seed_case(database, payment_status=PaymentStatus.captured, case_status="recovered")
    first_reconciler, first_http_client, _ = _reconciler(
        database, payment_status="captured", order_status="paid"
    )
    try:
        first_reconciler.verify_case("case_verify")
    finally:
        first_http_client.close()

    reconciler, http_client, _ = _reconciler(
        database, payment_status="failed", order_status="paid"
    )
    try:
        with TestClient(create_app(database, payment_reconciler=reconciler)) as client:
            response = client.post("/recovery-cases/case_verify/verify-payment")
    finally:
        http_client.close()

    assert response.status_code == 200
    body = response.json()
    assert body["provider_payment_status"] == "failed"
    assert body["recovery_confirmed"] is False
    assert body["revive_payment_status"] == "captured"
    assert body["order_status"] == "paid"
    assert database.get_payment_attempt("pay_verify").status is PaymentStatus.captured
    assert database.get_order("order_verify").status is OrderStatus.paid
    assert database.get_recovery_case("case_verify")["status"] == "recovered"
    assert _outcome(database) == {"status": "recovered", "amount_recovered": 49900}
    assert _count(database, "payment_outcomes") == 1


def test_repeated_verification_is_idempotent(database):
    _seed_case(database)
    reconciler, http_client, _ = _reconciler(
        database, payment_status="captured", order_status="paid"
    )
    try:
        with TestClient(create_app(database, payment_reconciler=reconciler)) as client:
            first = client.post("/recovery-cases/case_verify/verify-payment")
            second = client.post("/recovery-cases/case_verify/verify-payment")
    finally:
        http_client.close()

    assert first.status_code == second.status_code == 200
    assert first.json()["reconciliation_recorded"] is True
    assert second.json()["reconciliation_recorded"] is False
    assert second.json()["state_changed"] is False
    assert _count(database, "payment_outcomes") == 1
    assert _count(database, "audit_events") == 1


def test_concurrent_verification_is_idempotent(database):
    _seed_case(database)
    first_reconciler, first_http_client, _ = _reconciler(
        database, payment_status="captured", order_status="paid"
    )
    second_reconciler, second_http_client, _ = _reconciler(
        database, payment_status="captured", order_status="paid"
    )
    try:
        with ThreadPoolExecutor(max_workers=2) as executor:
            results = list(
                executor.map(
                    lambda reconciler: reconciler.verify_case("case_verify"),
                    (first_reconciler, second_reconciler),
                )
            )
    finally:
        first_http_client.close()
        second_http_client.close()

    assert all(result.recovery_confirmed for result in results)
    assert sorted(result.state_changed for result in results) == [False, True]
    assert _count(database, "payment_outcomes") == 1
    assert _count(database, "audit_events") == 1


def test_api_error_does_not_mutate_database(database):
    _seed_case(database)
    reconciler, http_client, _ = _reconciler(
        database, payment_status="captured", order_status="paid", error_status=503
    )
    try:
        with TestClient(create_app(database, payment_reconciler=reconciler)) as client:
            response = client.post("/recovery-cases/case_verify/verify-payment")
    finally:
        http_client.close()

    assert response.status_code == 502
    assert database.get_payment_attempt("pay_verify").status is PaymentStatus.failed
    assert database.get_order("order_verify").status is OrderStatus.attempted
    assert database.get_recovery_case("case_verify")["status"] == "open"
    assert _count(database, "payment_outcomes") == 0
    assert _count(database, "audit_events") == 0


def test_provider_confirmation_keeps_existing_order_id(database):
    _seed_case(database)
    reconciler, http_client, _ = _reconciler(
        database, payment_status="captured", order_status="paid"
    )
    try:
        result = reconciler.verify_case("case_verify")
    finally:
        http_client.close()

    assert result.order_id == "order_verify"
    case = database.get_recovery_case("case_verify")
    assert case["order_id"] == "order_verify"


def test_api_and_webhook_confirmation_sequence_counts_one_payment_once(database):
    _seed_case(database)
    reconciler, http_client, _ = _reconciler(
        database, payment_status="captured", order_status="paid"
    )
    try:
        first = reconciler.verify_case("case_verify")
        webhook_processor = RazorpayWebhookProcessor(database)
        captured_body, captured_headers = _signed_webhook(
            "evt_sequence_captured", "payment.captured", payment_status="captured"
        )
        paid_body, paid_headers = _signed_webhook("evt_sequence_paid", "order.paid")
        webhook_processor.ingest(captured_body, captured_headers)
        assert webhook_processor.ingest(captured_body, captured_headers).status == "duplicate"
        webhook_processor.ingest(paid_body, paid_headers)
        second = reconciler.verify_case("case_verify")
    finally:
        http_client.close()

    assert first.recovery_confirmed is True
    assert second.recovery_confirmed is True
    assert second.state_changed is False
    assert database.get_payment_attempt("pay_verify").status is PaymentStatus.captured
    assert database.get_recovery_case("case_verify")["status"] == "recovered"
    assert _count(database, "payment_outcomes") == 1
    assert _outcome(database) == {"status": "recovered", "amount_recovered": 49900}
    assert _count(database, "audit_events") == 1
