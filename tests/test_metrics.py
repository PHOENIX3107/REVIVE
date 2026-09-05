from backend.evaluation.metrics import (
    EvaluationRecord,
    ProviderConfirmedOutcome,
    total_amount_recovered,
)
from backend.policies.recovery_policy import PolicyDecisionType


def test_recovered_revenue_requires_provider_confirmed_outcome():
    record = EvaluationRecord(
        payment_id="pay_metrics",
        amount=100,
        policy_decision=PolicyDecisionType.recover,
        execution_status="executed",
        execution_succeeded=True,
    )
    assert total_amount_recovered([record]) == 0
    assert total_amount_recovered(
        [record],
        [ProviderConfirmedOutcome(payment_id="pay_metrics", status="captured", amount_recovered=100)],
    ) == 100
