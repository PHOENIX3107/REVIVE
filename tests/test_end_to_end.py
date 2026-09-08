from datetime import datetime, timedelta, timezone
import hashlib
import hmac
import json
import os

import pytest
from fastapi.testclient import TestClient

from backend.agents.recovery_agent import RecoveryAgent, local_diagnosis_provider
from backend.db import Database
from backend.main import create_app
from backend.pipeline.signal_detector import SignalDetector
from backend.recovery.case_processor import RecoveryCaseProcessor
from backend.recovery.executor import RecoveryExecutor
from backend.cache import RedisFailureCache, RedisIdempotencyCache
from backend.schemas import Order, OrderStatus, PaymentAttempt, PaymentError, PaymentMethod, PaymentStatus


DATABASE_URL = os.getenv("REVIVE_TEST_DATABASE_URL")
pytestmark = pytest.mark.skipif(
    not DATABASE_URL,
    reason="REVIVE_TEST_DATABASE_URL is required for PostgreSQL integration tests",
)


NOW = datetime(2026, 1, 1, tzinfo=timezone.utc)
WEBHOOK_SECRET = "revive-e2e-test-secret"


class FakeRedis:
    def __init__(self):
        self.sorted_sets = {}
        self.values = {}

    def zadd(self, key, members):
        self.sorted_sets.setdefault(key, {}).update(members)

    def zremrangebyscore(self, key, minimum, maximum):
        cutoff = float(str(maximum).lstrip("("))
        for member, score in list(self.sorted_sets.get(key, {}).items()):
            if score <= cutoff:
                del self.sorted_sets[key][member]

    def zcard(self, key):
        return len(self.sorted_sets.get(key, {}))

    def zcount(self, key, minimum, maximum):
        minimum = float(minimum)
        return sum(score >= minimum for score in self.sorted_sets.get(key, {}).values())

    def expire(self, key, seconds):
        return True

    def set(self, key, value, *, ex, nx):
        if nx and key in self.values:
            return False
        self.values[key] = (value, ex)
        return True


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


def _processor(database, redis=None):
    redis = redis or FakeRedis()
    return RecoveryCaseProcessor(
        database=database,
        signal_detector=SignalDetector(RedisFailureCache(redis)),
        recovery_agent=RecoveryAgent(local_diagnosis_provider),
        recovery_executor=RecoveryExecutor(RedisIdempotencyCache(redis)),
    )


def _webhook(
    *,
    event_id,
    payment_id,
    order_id,
    status,
    amount=49900,
    occurred_at=NOW,
):
    body = {
        "event": "payment.captured" if status == "captured" else "payment.failed",
        "created_at": int(occurred_at.timestamp()),
        "payload": {
            "payment": {
                "entity": {
                    "id": payment_id,
                    "order_id": order_id,
                    "amount": amount,
                    "currency": "INR",
                    "status": status,
                    "captured": status == "captured",
                    "amount_captured": amount if status == "captured" else 0,
                    "method": "card",
                    "created_at": int(occurred_at.timestamp()),
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
            },
            "order": {
                "entity": {
                    "id": order_id,
                    "amount": amount,
                    "amount_paid": amount if status == "captured" else 0,
                    "amount_due": 0 if status == "captured" else amount,
                    "currency": "INR",
                    "status": "paid" if status == "captured" else "attempted",
                    "attempts": 1,
                    "created_at": int(occurred_at.timestamp()),
                }
            },
        },
    }
    raw_body = json.dumps(body).encode("utf-8")
    signature = hmac.new(WEBHOOK_SECRET.encode("utf-8"), raw_body, hashlib.sha256).hexdigest()
    return raw_body, {
        "x-razorpay-event-id": event_id,
        "X-Razorpay-Signature": signature,
    }


def _seed_case(database, index):
    order_id = f"order_systemic_{index}"
    payment_id = f"pay_systemic_{index}"
    occurred_at = NOW + timedelta(seconds=index)
    database.save_order(
        Order(
            order_id=order_id,
            amount=1000 + index,
            amount_paid=0,
            amount_due=1000 + index,
            currency="INR",
            status=OrderStatus.attempted,
            attempts=1,
            created_at=occurred_at,
        )
    )
    database.save_payment_attempt(
        PaymentAttempt(
            payment_id=payment_id,
            order_id=order_id,
            amount=1000 + index,
            method=PaymentMethod.card,
            status=PaymentStatus.failed,
            captured=False,
            created_at=occurred_at,
            error=PaymentError(code="issuer_declined"),
            issuer_bin="411111",
            failed_at=occurred_at,
        )
    )
    database.save_recovery_case(
        f"case_systemic_{index}",
        payment_id,
        order_id,
        "open",
    )
    return payment_id, occurred_at


def test_failed_webhook_to_execution_then_provider_confirmation_updates_dashboard(
    database,
    monkeypatch,
):
    monkeypatch.setenv("RAZORPAY_WEBHOOK_SECRET", WEBHOOK_SECRET)
    processor = _processor(database)

    with TestClient(create_app(database, recovery_processor=processor)) as client:
        failed_body, failed_headers = _webhook(
            event_id="evt_e2e_failed",
            payment_id="pay_e2e",
            order_id="order_e2e",
            status="failed",
        )
        failed_response = client.post(
            "/webhooks/razorpay",
            content=failed_body,
            headers=failed_headers,
        )
        assert failed_response.status_code == 200
        assert database.get_recovery_case("recovery-pay_e2e")["status"] == "open"

        process_response = client.post("/recovery-cases/recovery-pay_e2e/process")
        assert process_response.status_code == 200
        assert process_response.json()["decision"]["decision"] == "recover"
        assert process_response.json()["execution"]["status"] == "executed"

        before_confirmation = client.get("/dashboard/overview").json()
        assert before_confirmation["metrics"]["recovery_actions"] == 1
        assert before_confirmation["metrics"]["recovered_revenue"] == 0

        captured_body, captured_headers = _webhook(
            event_id="evt_e2e_captured",
            payment_id="pay_e2e",
            order_id="order_e2e",
            status="captured",
        )
        captured_response = client.post(
            "/webhooks/razorpay",
            content=captured_body,
            headers=captured_headers,
        )
        assert captured_response.status_code == 200

        after_confirmation = client.get("/dashboard/overview").json()
        assert after_confirmation["metrics"]["recovered_revenue"] == 49900
        case = client.get("/dashboard/recoveries").json()[0]
        assert case["payment_outcome_status"] == "recovered"
        assert case["execution_status"] == "executed"


def test_population_signal_cools_down_all_preseeded_systemic_cases(database):
    redis = FakeRedis()
    processor = _processor(database, redis)
    seeded = [_seed_case(database, index) for index in range(5)]

    for payment_id, occurred_at in seeded:
        processor.signal_detector.failure_cache.record_failure(
            "411111",
            "issuer_declined",
            occurred_at=occurred_at,
            payment_id=payment_id,
        )

    results = [processor.process(f"case_systemic_{index}") for index in range(5)]

    assert all(result.diagnosis.category.value == "systemic_issue" for result in results)
    assert all(result.decision.decision.value == "cooldown" for result in results)
    assert all(result.execution.status == "blocked" for result in results)
    with database.connection() as connection:
        assert connection.execute("SELECT count(*) FROM payment_outcomes").fetchone()["count"] == 0

    with TestClient(create_app(database, recovery_processor=processor)) as client:
        metrics = client.get("/dashboard/overview").json()["metrics"]

    assert metrics["systemic_attempts"] == 5
    assert metrics["blocked_actions"] == 5
    assert metrics["recovery_actions"] == 0
    assert metrics["recovered_revenue"] == 0
