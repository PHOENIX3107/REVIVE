from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timezone
import os
import signal
import threading

import httpx
import psycopg
import pytest

from backend.db import Database
from backend.integrations.razorpay.client import (
    RazorpayAPIError,
    RazorpayClient,
    RazorpayClientConfig,
)
from backend.recovery.reconciliation import RazorpayPaymentReconciler
from backend.schemas import Order, OrderStatus, PaymentAttempt, PaymentError, PaymentMethod, PaymentStatus
from backend.workers.payment_reconciliation import (
    PaymentReconciliationWorker,
    PaymentReconciliationWorkerConfig,
    _is_retryable,
)


DATABASE_URL = os.getenv("REVIVE_TEST_DATABASE_URL")
pytestmark = pytest.mark.skipif(
    not DATABASE_URL,
    reason="REVIVE_TEST_DATABASE_URL is required for PostgreSQL integration tests",
)


NOW = datetime(2026, 1, 1, tzinfo=timezone.utc)


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


def _seed_case(database, index=0, *, payment_status=PaymentStatus.failed, case_status="open"):
    order_id = f"order_worker_{index}"
    payment_id = f"pay_worker_{index}"
    order_status = OrderStatus.paid if payment_status is PaymentStatus.captured else OrderStatus.attempted
    amount = 49900 + index
    order = Order(
        order_id=order_id,
        amount=amount,
        amount_paid=amount if order_status is OrderStatus.paid else 0,
        amount_due=0 if order_status is OrderStatus.paid else amount,
        currency="INR",
        status=order_status,
        attempts=1,
        created_at=NOW,
    )
    payment = PaymentAttempt(
        payment_id=payment_id,
        order_id=order_id,
        amount=amount,
        method=PaymentMethod.card,
        status=payment_status,
        captured=payment_status is PaymentStatus.captured,
        created_at=NOW,
        error=PaymentError(code="insufficient_funds") if payment_status is PaymentStatus.failed else None,
        issuer_bin="411111",
        failed_at=NOW if payment_status is PaymentStatus.failed else None,
    )
    database.save_order(order)
    database.save_payment_attempt(payment)
    database.save_recovery_case(f"case_worker_{index}", payment_id, order_id, case_status)
    return order, payment


def _worker(database, reconciler, **config):
    return PaymentReconciliationWorker(
        database,
        reconciler,
        PaymentReconciliationWorkerConfig(
            polling_interval_seconds=0.01,
            batch_size=10,
            retry_attempts=3,
            retry_backoff_seconds=0,
            retry_max_backoff_seconds=0,
            **config,
        ),
    )


class StubReconciler:
    def __init__(self, outcomes=None):
        self.outcomes = outcomes or {}
        self.calls = []

    def verify_case(self, case_id):
        self.calls.append(case_id)
        outcome = self.outcomes.get(case_id)
        if isinstance(outcome, Exception):
            raise outcome
        return outcome


def _provider_reconciler(database, *, payment_status, order_status):
    def handler(request: httpx.Request) -> httpx.Response:
        if request.url.path.endswith("/payments/pay_worker_0"):
            return httpx.Response(
                200,
                json={
                    "id": "pay_worker_0",
                    "entity": "payment",
                    "amount": 49900,
                    "currency": "INR",
                    "status": payment_status,
                    "method": "card",
                    "order_id": "order_worker_0",
                    "captured": payment_status == "captured",
                    "amount_captured": 49900 if payment_status == "captured" else 0,
                    "created_at": int(NOW.timestamp()),
                    "card": {"iin": "411111"},
                },
            )
        return httpx.Response(
            200,
            json={
                "id": "order_worker_0",
                "entity": "order",
                "amount": 49900,
                "amount_paid": 49900 if order_status == "paid" else 0,
                "amount_due": 0 if order_status == "paid" else 49900,
                "currency": "INR",
                "status": order_status,
                "attempts": 1,
                "created_at": int(NOW.timestamp()),
            },
        )

    http_client = httpx.Client(transport=httpx.MockTransport(handler))
    client = RazorpayClient(
        RazorpayClientConfig("rzp_test_worker", "test-secret"),
        http_client=http_client,
    )
    return RazorpayPaymentReconciler(database, client), http_client


def test_unresolved_case_is_verified_and_captured_payment_creates_recovered_outcome(database):
    _seed_case(database)
    reconciler, http_client = _provider_reconciler(
        database,
        payment_status="captured",
        order_status="paid",
    )
    try:
        result = _worker(database, reconciler).run_once()
    finally:
        http_client.close()

    assert result.discovered == 1
    assert result.attempted == 1
    assert result.verified == 1
    assert result.failed == 0
    assert database.get_recovery_case("case_worker_0")["status"] == "recovered"
    with database.connection() as connection:
        outcome = connection.execute(
            "SELECT status, amount_recovered FROM payment_outcomes WHERE payment_id = %s",
            ("pay_worker_0",),
        ).fetchone()
    assert dict(outcome) == {"status": "recovered", "amount_recovered": 49900}


def test_failed_provider_payment_stays_unrecovered(database):
    _seed_case(database)
    reconciler, http_client = _provider_reconciler(
        database,
        payment_status="failed",
        order_status="attempted",
    )
    try:
        result = _worker(database, reconciler).run_once()
    finally:
        http_client.close()

    assert result.verified == 1
    assert database.get_recovery_case("case_worker_0")["status"] == "open"
    with database.connection() as connection:
        assert connection.execute("SELECT count(*) FROM payment_outcomes").fetchone()["count"] == 0


def test_provider_exception_does_not_kill_worker_and_other_cases_continue(database):
    _seed_case(database, 0)
    _seed_case(database, 1)
    reconciler = StubReconciler(
        {
            "case_worker_0": RazorpayAPIError("temporary provider outage"),
            "case_worker_1": object(),
        }
    )

    result = _worker(database, reconciler).run_once()

    assert result.attempted == 2
    assert result.failed == 1
    assert result.verified == 1
    assert reconciler.calls == ["case_worker_0", "case_worker_0", "case_worker_0", "case_worker_1"]


@pytest.mark.parametrize("status_code", [429, 500, 502, 503])
def test_transient_razorpay_statuses_are_retryable(status_code):
    assert _is_retryable(RazorpayAPIError("temporary provider response", status_code=status_code))


@pytest.mark.parametrize("status_code", [400, 401, 403, 404])
def test_permanent_razorpay_statuses_are_not_retryable(status_code):
    assert not _is_retryable(RazorpayAPIError("permanent provider response", status_code=status_code))


@pytest.mark.parametrize(
    "error",
    [
        psycopg.OperationalError("database unavailable"),
        psycopg.InterfaceError("database interface unavailable"),
        TimeoutError("request timed out"),
        ConnectionError("connection dropped"),
        OSError("socket unavailable"),
    ],
)
def test_existing_transient_database_and_network_errors_remain_retryable(error):
    assert _is_retryable(error)


def test_permanent_provider_error_is_attempted_once(database):
    _seed_case(database)
    reconciler = StubReconciler(
        {"case_worker_0": RazorpayAPIError("invalid provider request", status_code=400)}
    )

    result = _worker(database, reconciler).run_once()

    assert result.attempted == 1
    assert result.failed == 1
    assert reconciler.calls == ["case_worker_0"]


def test_database_discovery_exception_does_not_escape_worker():
    class UnavailableDatabase:
        def get_reconciliation_candidates(self, _limit):
            raise OSError("database unavailable")

    worker = PaymentReconciliationWorker(
        UnavailableDatabase(),
        StubReconciler(),
        PaymentReconciliationWorkerConfig(
            polling_interval_seconds=0.01,
            batch_size=1,
            retry_attempts=1,
            retry_backoff_seconds=0,
            retry_max_backoff_seconds=0,
        ),
    )

    result = worker.run_once()

    assert result.failed == 1
    assert result.discovered == 0


def test_recovered_case_is_not_selected_again(database):
    order, payment = _seed_case(
        database,
        payment_status=PaymentStatus.captured,
        case_status="recovered",
    )
    database.reconcile_provider_state(
        "case_worker_0",
        payment,
        order,
        provider_payment_status="captured",
        provider_order_status="paid",
        recovery_confirmed=True,
        recovered_amount=order.amount,
        observed_at=NOW,
        audit_reason="seeded provider confirmation",
    )
    reconciler = StubReconciler()

    result = _worker(database, reconciler).run_once()

    assert result.discovered == 0
    assert reconciler.calls == []


def test_worker_shutdown_is_graceful(database):
    _seed_case(database)
    reconciler = StubReconciler({"case_worker_0": object()})
    worker = _worker(database, reconciler)
    worker._handle_signal(signal.SIGTERM, None)

    worker.run_forever(install_signal_handlers=False)

    assert reconciler.calls == []
    assert worker.stop_event.is_set()


def test_concurrent_workers_claim_a_case_only_once(database):
    _seed_case(database)
    entered = threading.Event()
    release = threading.Event()

    class BlockingReconciler:
        def __init__(self):
            self.calls = []

        def verify_case(self, case_id):
            self.calls.append(case_id)
            entered.set()
            assert release.wait(timeout=5)
            return object()

    first_reconciler = BlockingReconciler()
    second_reconciler = BlockingReconciler()
    first_worker = _worker(database, first_reconciler)
    second_worker = _worker(database, second_reconciler)

    with ThreadPoolExecutor(max_workers=2) as executor:
        first_future = executor.submit(first_worker.run_once)
        assert entered.wait(timeout=5)
        second_result = second_worker.run_once()
        release.set()
        first_result = first_future.result(timeout=5)

    assert second_result.discovered == 1
    assert second_result.attempted == 0
    assert second_result.skipped == 1
    assert first_result.verified == 1
    assert first_reconciler.calls == ["case_worker_0"]
    assert second_reconciler.calls == []
