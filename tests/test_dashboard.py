from datetime import datetime, timedelta, timezone
import os

import pytest
from fastapi.testclient import TestClient

from backend.agents.recovery_agent import Diagnosis, DiagnosisCategory
from backend.db import Database
from backend.main import create_app
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


@pytest.fixture
def client(database):
    with TestClient(create_app(database)) as test_client:
        yield test_client


def _save_order_and_payment(
    database,
    *,
    order_id,
    payment_id,
    amount,
    order_status,
    payment_status,
    failed_at=None,
    issuer_bin="411111",
):
    is_paid = order_status is OrderStatus.paid
    database.save_order(
        Order(
            order_id=order_id,
            amount=amount,
            amount_paid=amount if is_paid else 0,
            amount_due=0 if is_paid else amount,
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
            amount=amount,
            method=PaymentMethod.card,
            status=payment_status,
            captured=payment_status is PaymentStatus.captured,
            created_at=NOW,
            error=PaymentError(code="insufficient_funds")
            if payment_status is PaymentStatus.failed
            else None,
            issuer_bin=issuer_bin,
            failed_at=failed_at,
        )
    )


def _seed_dashboard_state(database):
    _save_order_and_payment(
        database,
        order_id="order_recover",
        payment_id="pay_recover",
        amount=1000,
        order_status=OrderStatus.attempted,
        payment_status=PaymentStatus.failed,
        failed_at=NOW,
    )
    database.save_recovery_case("case_recover", "pay_recover", "order_recover", "open")
    database.save_diagnosis(
        "case_recover",
        Diagnosis(category=DiagnosisCategory.unknown, confidence=0.1, reason="Older diagnosis."),
    )
    database.save_diagnosis(
        "case_recover",
        Diagnosis(
            category=DiagnosisCategory.customer_issue,
            confidence=0.95,
            reason="Latest customer diagnosis.",
        ),
    )
    database.save_decision(
        "case_recover",
        PolicyDecision(decision=PolicyDecisionType.review, reason="Older decision."),
    )
    recover_decision = PolicyDecision(
        decision=PolicyDecisionType.recover,
        reason="Latest recovery decision.",
    )
    database.save_decision("case_recover", recover_decision)
    recover_audit = AuditRecord(
        payment_id="pay_recover",
        order_id="order_recover",
        policy_decision=PolicyDecisionType.recover,
        action="retry_payment",
        status="executed",
        reason="Simulated execution only.",
        timestamp=NOW,
    )
    database.save_execution(
        "case_recover",
        ExecutionResult(
            executed=True,
            action="retry_payment",
            status="executed",
            reason="Simulated execution only.",
            audit=recover_audit,
        ),
        "dashboard-recover",
    )
    database.save_audit_event("case_recover", recover_audit)

    _save_order_and_payment(
        database,
        order_id="order_block",
        payment_id="pay_block",
        amount=2000,
        order_status=OrderStatus.attempted,
        payment_status=PaymentStatus.failed,
        failed_at=NOW + timedelta(minutes=1),
    )
    database.save_recovery_case("case_block", "pay_block", "order_block", "open")
    database.save_diagnosis(
        "case_block",
        Diagnosis(
            category=DiagnosisCategory.systemic_issue,
            confidence=1,
            reason="Population signal requires cooldown.",
        ),
    )
    block_decision = PolicyDecision(
        decision=PolicyDecisionType.cooldown,
        reason="Systemic failures require cooldown.",
    )
    database.save_decision("case_block", block_decision)
    block_audit = AuditRecord(
        payment_id="pay_block",
        order_id="order_block",
        policy_decision=PolicyDecisionType.cooldown,
        action=None,
        status="blocked",
        reason="Execution blocked by policy.",
        timestamp=NOW + timedelta(minutes=1),
    )
    database.save_execution(
        "case_block",
        ExecutionResult(
            executed=False,
            action=None,
            status="blocked",
            reason="Execution blocked by policy.",
            audit=block_audit,
        ),
        "dashboard-block",
    )
    database.save_audit_event("case_block", block_audit)

    paid_order = Order(
        order_id="order_paid",
        amount=3000,
        amount_paid=3000,
        amount_due=0,
        currency="INR",
        status=OrderStatus.paid,
        attempts=1,
        created_at=NOW,
    )
    paid_payment = PaymentAttempt(
        payment_id="pay_paid",
        order_id="order_paid",
        amount=3000,
        method=PaymentMethod.card,
        status=PaymentStatus.captured,
        captured=True,
        created_at=NOW,
        error=None,
        issuer_bin="555555",
        failed_at=None,
    )
    database.save_order(paid_order)
    database.save_payment_attempt(paid_payment)
    database.save_recovery_case("case_paid", "pay_paid", "order_paid", "open")
    database.reconcile_provider_state(
        "case_paid",
        paid_payment,
        paid_order,
        provider_payment_status="captured",
        provider_order_status="paid",
        recovery_confirmed=True,
        recovered_amount=3000,
        observed_at=NOW + timedelta(minutes=2),
        audit_reason="Provider-confirmed dashboard fixture.",
    )


def _counts(database):
    with database.connection() as connection:
        return {
            table: connection.execute(f"SELECT count(*) AS count FROM {table}").fetchone()["count"]
            for table in (
                "orders",
                "payment_attempts",
                "recovery_cases",
                "diagnoses",
                "decisions",
                "executions",
                "audit_events",
                "payment_outcomes",
            )
        }


def test_dashboard_overview_uses_durable_metrics_and_is_read_only(client, database):
    _seed_dashboard_state(database)
    before = _counts(database)

    response = client.get("/dashboard/overview")

    assert response.status_code == 200
    body = response.json()
    assert body["metrics"] == {
        "payment_attempts": 3,
        "failed_payments": 2,
        "revenue_at_risk": 3000,
        "policy_eligible_revenue": 1000,
        "recovered_revenue": 3000,
        "recovery_actions": 1,
        "blocked_actions": 1,
        "duplicate_actions_prevented": 0,
        "unsafe_actions": 0,
        "systemic_attempts": 1,
        "customer_attempts": 1,
    }
    assert {case["case_id"] for case in body["recent_cases"]} == {
        "case_recover",
        "case_block",
        "case_paid",
    }
    assert _counts(database) == before


def test_dashboard_recoveries_join_latest_pipeline_records_and_outcome(client, database):
    _seed_dashboard_state(database)

    response = client.get("/dashboard/recoveries")

    assert response.status_code == 200
    rows = {row["case_id"]: row for row in response.json()}
    assert rows["case_recover"]["diagnosis_reason"] == "Latest customer diagnosis."
    assert rows["case_recover"]["diagnosis_confidence"] == pytest.approx(0.95)
    assert rows["case_recover"]["decision"] == "recover"
    assert rows["case_recover"]["decision_reason"] == "Latest recovery decision."
    assert rows["case_recover"]["execution_status"] == "executed"
    assert rows["case_recover"]["execution_action"] == "retry_payment"
    assert rows["case_recover"]["payment_outcome_status"] is None
    assert rows["case_block"]["execution_status"] == "blocked"
    assert rows["case_paid"]["payment_status"] == "captured"
    assert rows["case_paid"]["order_status"] == "paid"
    assert rows["case_paid"]["case_status"] == "recovered"
    assert rows["case_paid"]["payment_outcome_status"] == "recovered"
    assert rows["case_paid"]["payment_outcome_amount"] == 3000


def test_dashboard_signals_group_only_failed_payments(client, database):
    _seed_dashboard_state(database)

    response = client.get("/dashboard/signals")

    assert response.status_code == 200
    rows = response.json()
    assert len(rows) == 1
    assert rows[0]["issuer_bin"] == "411111"
    assert rows[0]["error_code"] == "insufficient_funds"
    assert rows[0]["failure_count"] == 2
    assert rows[0]["total_amount"] == 3000
    assert datetime.fromisoformat(rows[0]["latest_failed_at"].replace("Z", "+00:00")) == (
        NOW + timedelta(minutes=1)
    )


def test_dashboard_downtime_is_explicitly_unavailable(client):
    response = client.get("/dashboard/downtime")

    assert response.status_code == 200
    assert response.json() == {"available": False, "items": []}


def test_dashboard_decisions_include_case_payment_order_and_latest_diagnosis(client, database):
    _seed_dashboard_state(database)

    response = client.get("/dashboard/decisions")

    assert response.status_code == 200
    rows = response.json()
    assert len(rows) == 3
    assert {row["case_id"] for row in rows} == {"case_recover", "case_block"}
    recover_rows = [row for row in rows if row["case_id"] == "case_recover"]
    assert {row["decision"] for row in recover_rows} == {"review", "recover"}
    assert all(row["payment_id"] == "pay_recover" for row in recover_rows)
    assert all(row["order_id"] == "order_recover" for row in recover_rows)
    assert all(row["amount"] == 1000 for row in recover_rows)
    assert all(row["diagnosis_reason"] == "Latest customer diagnosis." for row in recover_rows)
    block_rows = [row for row in rows if row["case_id"] == "case_block"]
    assert len(block_rows) == 1
    assert block_rows[0]["decision"] == "cooldown"


def test_dashboard_audit_includes_durable_case_payment_and_order_context(client, database):
    _seed_dashboard_state(database)

    response = client.get("/dashboard/audit")

    assert response.status_code == 200
    rows = response.json()
    assert len(rows) == 3
    by_case = {row["case_id"]: row for row in rows}
    assert by_case["case_recover"]["payment_id"] == "pay_recover"
    assert by_case["case_recover"]["order_id"] == "order_recover"
    assert by_case["case_recover"]["amount"] == 1000
    assert by_case["case_recover"]["status"] == "executed"
    assert by_case["case_block"]["policy_decision"] == "cooldown"
    assert by_case["case_paid"]["action"] == "provider_reconciliation"
    assert by_case["case_paid"]["status"] == "confirmed"
