"""Small deterministic metrics for simulated recovery outcomes."""

from collections.abc import Iterable
from decimal import Decimal
from typing import Any

from pydantic import BaseModel

from backend.policies.recovery_policy import MAX_RECOVERY_ATTEMPTS, PolicyDecisionType
from backend.schemas import PaymentStatus


class EvaluationRecord(BaseModel):
    payment_id: str
    amount: int
    policy_decision: PolicyDecisionType
    execution_status: str
    recovery_succeeded: bool = False
    payment_status: PaymentStatus = PaymentStatus.failed
    order_status: str = "attempted"
    order_attempts: int = 0
    downtime_matched: bool = False
    systemic_cluster: bool = False


def _records(records: Iterable[EvaluationRecord]) -> list[EvaluationRecord]:
    return list(records)


def total_amount_at_risk(records: Iterable[EvaluationRecord]) -> int:
    """Sum failed-payment amounts eligible for recovery evaluation."""
    return sum(
        record.amount
        for record in _records(records)
        if record.payment_status is PaymentStatus.failed and record.order_status != "paid"
    )


def total_amount_recovered(records: Iterable[EvaluationRecord]) -> int:
    """Sum amounts only when the simulation explicitly reports success."""
    return sum(record.amount for record in _records(records) if record.recovery_succeeded)


def recovery_rate(records: Iterable[EvaluationRecord]) -> Decimal:
    """Successful recovered amount divided by eligible amount at risk."""
    records = _records(records)
    eligible = total_amount_at_risk(records)
    if eligible == 0:
        return Decimal("0")
    return Decimal(total_amount_recovered(records)) / Decimal(eligible)


def number_of_recovery_actions(records: Iterable[EvaluationRecord]) -> int:
    return sum(record.execution_status == "executed" for record in _records(records))


def number_of_blocked_actions(records: Iterable[EvaluationRecord]) -> int:
    return sum(record.execution_status == "blocked" for record in _records(records))


def number_of_duplicate_actions_prevented(records: Iterable[EvaluationRecord]) -> int:
    return sum(record.execution_status == "duplicate" for record in _records(records))


def unsafe_recovery_actions(records: Iterable[EvaluationRecord]) -> int:
    """Count executed recoveries that violate any known guardrail."""
    return sum(
        record.execution_status == "executed"
        and (
            record.downtime_matched
            or record.systemic_cluster
            or record.order_attempts >= MAX_RECOVERY_ATTEMPTS
            or record.order_status == "paid"
        )
        for record in _records(records)
    )


def systemic_cluster_recovery_attempts(records: Iterable[EvaluationRecord]) -> int:
    return sum(
        record.policy_decision is PolicyDecisionType.recover and record.systemic_cluster
        for record in _records(records)
    )


def customer_side_recovery_attempts(records: Iterable[EvaluationRecord]) -> int:
    return sum(
        record.policy_decision is PolicyDecisionType.recover and not record.systemic_cluster
        for record in _records(records)
    )
