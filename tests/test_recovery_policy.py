from datetime import datetime, timezone

from backend.agents.recovery_agent import Diagnosis, DiagnosisCategory
from backend.pipeline.downtime_correlator import DowntimeCorrelationResult
from backend.policies.recovery_policy import (
    MAX_RECOVERY_ATTEMPTS,
    PolicyDecisionType,
    evaluate_policy,
)
from backend.schemas import (
    Order,
    OrderStatus,
    PaymentAttempt,
    PaymentError,
    PaymentMethod,
    PaymentStatus,
    SignalContext,
)


NOW = datetime(2026, 1, 1, tzinfo=timezone.utc)


def case(*, order_status=OrderStatus.attempted, payment_status=PaymentStatus.failed, attempts=1, failed_at=NOW, error=True, clustered=False, downtime=False, category=DiagnosisCategory.customer_issue):
    payment = PaymentAttempt(
        payment_id="pay_test",
        order_id="order_test",
        amount=49900,
        method=PaymentMethod.card,
        status=payment_status,
        captured=payment_status is PaymentStatus.captured,
        created_at=NOW,
        error=PaymentError(code="insufficient_funds") if error else None,
        issuer_bin="411111",
        failed_at=failed_at,
    )
    order = Order(
        order_id="order_test",
        amount=49900,
        amount_paid=49900 if order_status is OrderStatus.paid else 0,
        amount_due=0 if order_status is OrderStatus.paid else 49900,
        currency="INR",
        status=order_status,
        attempts=attempts,
        created_at=NOW,
    )
    return evaluate_policy(
        order,
        payment,
        SignalContext(is_cluster_candidate=clustered, matching_failure_count=5 if clustered else 1, window_seconds=600),
        DowntimeCorrelationResult(matched=downtime, downtime_id="down" if downtime else None),
        Diagnosis(category=category, confidence=0.8, reason="test evidence"),
    )


def test_paid_order_stops():
    assert case(order_status=OrderStatus.paid).decision is PolicyDecisionType.stop


def test_captured_payment_stops():
    assert case(payment_status=PaymentStatus.captured).decision is PolicyDecisionType.stop


def test_missing_failure_information_requires_review():
    assert case(failed_at=None).decision is PolicyDecisionType.review
    assert case(error=False).decision is PolicyDecisionType.review


def test_downtime_takes_precedence_with_cooldown():
    assert case(downtime=True).decision is PolicyDecisionType.cooldown


def test_systemic_cluster_and_diagnosis_cool_down():
    assert case(clustered=True, category=DiagnosisCategory.systemic_issue).decision is PolicyDecisionType.cooldown


def test_customer_issue_without_systemic_evidence_recovers():
    assert case().decision is PolicyDecisionType.recover


def test_unknown_diagnosis_requires_review():
    assert case(category=DiagnosisCategory.unknown).decision is PolicyDecisionType.review


def test_maximum_attempts_stop_recovery():
    result = case(attempts=MAX_RECOVERY_ATTEMPTS)
    assert result.decision is PolicyDecisionType.stop


def test_conflicting_evidence_requires_review():
    result = case(clustered=True, category=DiagnosisCategory.customer_issue)
    assert result.decision is PolicyDecisionType.review


def test_safe_default_requires_review():
    result = case(category=DiagnosisCategory.systemic_issue)
    assert result.decision is PolicyDecisionType.review
