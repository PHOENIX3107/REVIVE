from datetime import datetime, timedelta, timezone
import os

import pytest
from fastapi.testclient import TestClient

from backend import main as main_module
from backend.agents.recovery_agent import DiagnosisCategory, RecoveryAgent, local_diagnosis_provider
from backend.cache import RedisFailureCache, RedisIdempotencyCache
from backend.db import Database
from backend.main import create_app
from backend.pipeline.signal_detector import SignalDetector
from backend.policies.recovery_policy import PolicyDecisionType
from backend.recovery.case_processor import RecoveryCaseProcessor
from backend.recovery.executor import RecoveryExecutor
from backend.schemas import Downtime, Order, OrderStatus, PaymentAttempt, PaymentError, PaymentMethod, PaymentStatus


DATABASE_URL = os.getenv("REVIVE_TEST_DATABASE_URL")
pytestmark = pytest.mark.skipif(
    not DATABASE_URL,
    reason="REVIVE_TEST_DATABASE_URL is required for PostgreSQL integration tests",
)


NOW = datetime(2026, 1, 1, tzinfo=timezone.utc)


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


def _seed_case(
    database,
    *,
    case_id="case_process",
    payment_id="pay_process",
    order_id="order_process",
    order_status=OrderStatus.attempted,
):
    database.save_order(
        Order(
            order_id=order_id,
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
            payment_id=payment_id,
            order_id=order_id,
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
    database.save_recovery_case(case_id, payment_id, order_id, "open")


def _processor(database, response, *, downtimes=()):
    redis = FakeRedis()
    return RecoveryCaseProcessor(
        database=database,
        signal_detector=SignalDetector(RedisFailureCache(redis)),
        recovery_agent=RecoveryAgent(lambda _prompt: response),
        recovery_executor=RecoveryExecutor(RedisIdempotencyCache(redis)),
        downtimes=downtimes,
    ), redis


def _count(database, table):
    with database.connection() as connection:
        return connection.execute(f"SELECT count(*) AS count FROM {table}").fetchone()["count"]


def test_default_app_composition_builds_processor_without_injection(database, monkeypatch):
    redis = FakeRedis()
    monkeypatch.setenv("DATABASE_URL", DATABASE_URL)
    monkeypatch.setenv("REDIS_URL", "redis://localhost:6379/0")
    monkeypatch.setattr(main_module.redis.Redis, "from_url", lambda *_args, **_kwargs: redis)

    app = main_module.create_app()
    with TestClient(app):
        processor = app.state.recovery_processor

    assert isinstance(processor, RecoveryCaseProcessor)
    assert processor.database.database_url == DATABASE_URL
    assert isinstance(processor.signal_detector, SignalDetector)
    assert isinstance(processor.signal_detector.failure_cache, RedisFailureCache)
    assert isinstance(processor.recovery_agent, RecoveryAgent)
    assert processor.recovery_agent.provider is local_diagnosis_provider
    assert isinstance(processor.recovery_executor, RecoveryExecutor)
    assert processor.signal_detector.failure_cache.redis is redis
    assert processor.recovery_executor.idempotency_cache.redis is redis
    assert processor.downtimes == ()


def test_default_app_processes_case_without_payment_outcome(database, monkeypatch):
    _seed_case(database)
    redis = FakeRedis()
    monkeypatch.setenv("DATABASE_URL", DATABASE_URL)
    monkeypatch.setattr(main_module.redis.Redis, "from_url", lambda *_args, **_kwargs: redis)

    with TestClient(main_module.create_app()) as client:
        response = client.post("/recovery-cases/case_process/process")

    assert response.status_code == 200
    assert response.json()["decision"]["decision"] == "recover"
    assert response.json()["execution"]["executed"] is True
    assert _count(database, "payment_outcomes") == 0


def test_persisted_failed_payment_runs_diagnosis_and_decision(database):
    _seed_case(database)
    prompts = []
    redis = FakeRedis()
    processor = RecoveryCaseProcessor(
        database=database,
        signal_detector=SignalDetector(RedisFailureCache(redis)),
        recovery_agent=RecoveryAgent(
            lambda prompt: prompts.append(prompt)
            or {
                "category": "customer_issue",
                "confidence": 0.9,
                "reason": "Customer-side failure evidence.",
            }
        ),
        recovery_executor=RecoveryExecutor(RedisIdempotencyCache(redis)),
    )

    result = processor.process("case_process")

    assert result.diagnosis.category is DiagnosisCategory.customer_issue
    assert result.decision.decision is PolicyDecisionType.recover
    assert "pay_process" in prompts[0]
    assert "order_process" in prompts[0]
    assert _count(database, "diagnoses") == 1
    assert _count(database, "decisions") == 1


def test_recover_decision_persists_execution_and_audit(database):
    _seed_case(database)
    processor, _ = _processor(
        database,
        {
            "category": "customer_issue",
            "confidence": 0.9,
            "reason": "Customer-side failure evidence.",
        },
    )

    result = processor.process("case_process")

    assert result.execution.executed is True
    assert result.execution.action == "retry_payment"
    assert _count(database, "executions") == 1
    assert _count(database, "audit_events") == 1


def test_repeated_processing_does_not_create_duplicate_execution(database):
    _seed_case(database)
    processor, _ = _processor(
        database,
        {
            "category": "customer_issue",
            "confidence": 0.9,
            "reason": "Customer-side failure evidence.",
        },
    )

    first = processor.process("case_process")
    second = processor.process("case_process")

    assert first.execution.status == "executed"
    assert second.execution.status == "executed"
    assert second.already_processed is True
    assert _count(database, "diagnoses") == 1
    assert _count(database, "decisions") == 1
    assert _count(database, "executions") == 1
    assert _count(database, "audit_events") == 1


@pytest.mark.parametrize(
    ("expected_decision", "order_status", "category", "downtime"),
    [
        (PolicyDecisionType.stop, OrderStatus.paid, "customer_issue", []),
        (
            PolicyDecisionType.cooldown,
            OrderStatus.attempted,
            "customer_issue",
            [
                # The matching window makes cooldown take precedence over diagnosis.
                {
                    "downtime_id": "down_process",
                    "entity": "payments",
                    "method": "card",
                    "begin": NOW - timedelta(minutes=1),
                    "end": NOW + timedelta(minutes=1),
                    "status": "active",
                    "scheduled": False,
                    "severity": "high",
                }
            ],
        ),
        (PolicyDecisionType.review, OrderStatus.attempted, "unknown", []),
    ],
)
def test_non_recover_decision_does_not_execute(
    database,
    expected_decision,
    order_status,
    category,
    downtime,
):
    _seed_case(database, order_status=order_status)
    downtimes = [Downtime(**item) for item in downtime]
    processor, redis = _processor(
        database,
        {
            "category": category,
            "confidence": 0.9,
            "reason": "Test diagnosis.",
        },
        downtimes=downtimes,
    )

    result = processor.process("case_process")

    assert result.decision.decision is expected_decision
    assert result.execution.executed is False
    assert result.execution.action is None
    assert redis.values == {}
    with database.connection() as connection:
        row = connection.execute(
            "SELECT executed FROM executions WHERE case_id = %s",
            ("case_process",),
        ).fetchone()
    assert row is not None and row["executed"] is False


def test_processing_does_not_create_payment_outcome_or_recovered_revenue(database):
    _seed_case(database)
    processor, _ = _processor(
        database,
        {
            "category": "customer_issue",
            "confidence": 0.9,
            "reason": "Customer-side failure evidence.",
        },
    )

    result = processor.process("case_process")

    assert result.execution.executed is True
    assert _count(database, "payment_outcomes") == 0


def test_process_endpoint_uses_the_durable_case_processor(database):
    _seed_case(database)
    processor, _ = _processor(
        database,
        {
            "category": "customer_issue",
            "confidence": 0.9,
            "reason": "Customer-side failure evidence.",
        },
    )

    with TestClient(create_app(database, recovery_processor=processor)) as client:
        response = client.post("/recovery-cases/case_process/process")

    assert response.status_code == 200
    assert response.json()["decision"]["decision"] == "recover"
    assert response.json()["execution"]["executed"] is True


def test_customer_checkout_uses_existing_order_after_recover_decision(database, monkeypatch):
    _seed_case(database)
    processor, _ = _processor(
        database,
        {
            "category": "customer_issue",
            "confidence": 0.9,
            "reason": "Customer-side failure evidence.",
        },
    )
    processor.process("case_process")
    monkeypatch.setenv("RAZORPAY_KEY_ID", "rzp_test_checkout_key")
    monkeypatch.setenv("RAZORPAY_KEY_SECRET", "test-only-secret")

    with TestClient(create_app(database, recovery_processor=processor)) as client:
        response = client.get("/recovery-cases/case_process/customer-checkout")

    assert response.status_code == 200
    assert response.json() == {
        "case_id": "case_process",
        "payment_id": "pay_process",
        "order_id": "order_process",
        "test_mode": True,
        "checkout": {
            "key": "rzp_test_checkout_key",
            "order_id": "order_process",
            "amount": 49900,
            "currency": "INR",
        },
    }
    assert _count(database, "payment_outcomes") == 0
