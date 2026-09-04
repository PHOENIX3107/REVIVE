from backend.evaluation.metrics import (
    EvaluationRecord,
    customer_side_recovery_attempts,
    number_of_blocked_actions,
    number_of_duplicate_actions_prevented,
    number_of_recovery_actions,
    recovery_rate,
    systemic_cluster_recovery_attempts,
    total_amount_at_risk,
    total_amount_recovered,
    unsafe_recovery_actions,
)
from backend.policies.recovery_policy import PolicyDecisionType
from backend.schemas import PaymentStatus


def record(payment_id, amount, *, decision=PolicyDecisionType.recover, status="executed", success=False, **kwargs):
    return EvaluationRecord(
        payment_id=payment_id,
        amount=amount,
        policy_decision=decision,
        execution_status=status,
        recovery_succeeded=success,
        **kwargs,
    )


def test_recovered_amount_requires_explicit_success():
    records = [record("one", 49900, success=True), record("two", 19900, success=False)]
    assert total_amount_recovered(records) == 49900
    assert total_amount_at_risk(records) == 69800


def test_recovery_rate_uses_amount_at_risk_denominator():
    records = [record("one", 500, success=True), record("two", 500)]
    assert recovery_rate(records) == 0.5


def test_zero_eligible_amount_has_zero_rate():
    assert recovery_rate([record("paid", 500, order_status="paid")]) == 0


def test_at_risk_excludes_non_failed_payments():
    item = record("captured", 500, payment_status=PaymentStatus.captured)
    assert total_amount_at_risk([item]) == 0


def test_action_block_and_duplicate_counts():
    records = [
        record("one", 1, status="executed"),
        record("two", 1, status="blocked", decision=PolicyDecisionType.stop),
        record("three", 1, status="duplicate"),
    ]
    assert number_of_recovery_actions(records) == 1
    assert number_of_blocked_actions(records) == 1
    assert number_of_duplicate_actions_prevented(records) == 1


def test_unsafe_recovery_is_detected():
    records = [
        record("down", 1, downtime_matched=True),
        record("cluster", 1, systemic_cluster=True),
        record("attempts", 1, order_attempts=3),
        record("paid", 1, order_status="paid"),
    ]
    assert unsafe_recovery_actions(records) == 4


def test_systemic_and_customer_attempts_are_separated():
    records = [
        record("systemic", 1, systemic_cluster=True),
        record("customer", 1),
        record("blocked", 1, decision=PolicyDecisionType.stop, status="blocked", systemic_cluster=True),
    ]
    assert systemic_cluster_recovery_attempts(records) == 1
    assert customer_side_recovery_attempts(records) == 1


def test_permitted_recovery_is_not_counted_as_recovered_without_success():
    item = record("permitted", 100, success=False)
    assert number_of_recovery_actions([item]) == 1
    assert total_amount_recovered([item]) == 0
