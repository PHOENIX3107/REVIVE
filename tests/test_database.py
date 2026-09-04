from datetime import datetime, timezone
import os

import pytest

from backend.agents.recovery_agent import Diagnosis, DiagnosisCategory
from backend.db import Database, DuplicateRecordError
from backend.pipeline.downtime_correlator import DowntimeCorrelationResult
from backend.policies.recovery_policy import PolicyDecision, PolicyDecisionType
from backend.recovery.executor import AuditRecord, ExecutionResult
from backend.schemas import Order, OrderStatus, PaymentAttempt, PaymentError, PaymentMethod, PaymentStatus


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


def order(order_id="order_db"):
    return Order(
        order_id=order_id,
        amount=49900,
        amount_paid=0,
        amount_due=49900,
        currency="INR",
        status=OrderStatus.attempted,
        attempts=1,
        created_at=NOW,
    )


def payment(payment_id="pay_db", order_id="order_db"):
    return PaymentAttempt(
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


def test_database_initializes_and_round_trips_order(database):
    database.save_order(order())
    saved = database.get_order("order_db")
    assert saved == order()
    assert saved.amount == 49900
    assert isinstance(saved.amount, int)


def test_payment_attempt_round_trips_and_duplicate_provider_id_is_rejected(database):
    database.save_order(order())
    database.save_payment_attempt(payment())
    assert database.get_payment_attempt("pay_db") == payment()
    with pytest.raises(DuplicateRecordError):
        database.save_payment_attempt(payment())


def test_duplicate_webhook_event_is_rejected(database):
    database.record_webhook_event("evt_db", "payment.failed", {"amount": 49900}, NOW)
    with pytest.raises(DuplicateRecordError):
        database.record_webhook_event("evt_db", "payment.failed", {"amount": 49900}, NOW)


def test_decision_execution_and_audit_are_persisted(database):
    database.save_order(order())
    database.save_payment_attempt(payment())
    database.save_recovery_case("case_db", "pay_db", "order_db", "open")
    database.save_diagnosis(
        "case_db",
        Diagnosis(category=DiagnosisCategory.customer_issue, confidence=0.8, reason="test"),
    )
    decision = PolicyDecision(decision=PolicyDecisionType.recover, reason="test")
    database.save_decision("case_db", decision)
    audit = AuditRecord(
        payment_id="pay_db", order_id="order_db", policy_decision=decision.decision,
        action="retry_payment", status="executed", reason="test", timestamp=NOW,
    )
    database.save_execution(
        "case_db",
        ExecutionResult(executed=True, action="retry_payment", status="executed", reason="test", audit=audit),
        "idem_db",
    )
    database.save_audit_event("case_db", audit)
    database.save_payment_outcome("pay_db", "captured", 49900, NOW)

    with database.connection() as connection:
        counts = {
            table: connection.execute(f"SELECT count(*) AS count FROM {table}").fetchone()["count"]
            for table in ("decisions", "executions", "audit_events", "payment_outcomes")
        }
    assert counts == {"decisions": 1, "executions": 1, "audit_events": 1, "payment_outcomes": 1}


def test_transaction_rollback_leaves_no_partial_record(database):
    with pytest.raises(RuntimeError):
        with database.transaction() as connection:
            connection.execute(
                "INSERT INTO orders (order_id, amount, amount_paid, amount_due, currency, status, attempts, created_at) "
                "VALUES (%s, %s, %s, %s, %s, %s, %s, %s)",
                ("rollback_order", 1, 0, 1, "INR", "attempted", 1, NOW),
            )
            raise RuntimeError("force rollback")

    assert database.get_order("rollback_order") is None
