"""Metrics with a strict boundary between execution and provider outcomes."""

from collections.abc import Iterable
from decimal import Decimal
from pydantic import BaseModel, Field

from backend.policies.recovery_policy import MAX_RECOVERY_ATTEMPTS, PolicyDecisionType
from backend.schemas import PaymentStatus


class EvaluationRecord(BaseModel):
    payment_id: str
    amount: int
    policy_decision: PolicyDecisionType
    execution_status: str
    execution_succeeded: bool = False
    payment_status: PaymentStatus = PaymentStatus.failed
    order_status: str = "attempted"
    order_attempts: int = 0
    downtime_matched: bool = False
    population_signal_detected: bool = False
    ground_truth_systemic: bool = False
    # Compatibility field for older evaluation callers. New metrics must use
    # population_signal_detected and never interpret this as hidden truth.
    systemic_cluster: bool = False


class ProviderConfirmedOutcome(BaseModel):
    """A payment outcome explicitly sourced from provider confirmation.

    Production callers should build these records from the PostgreSQL
    ``payment_outcomes`` rows written by the webhook/reconciliation path.  The
    type is intentionally separate from ``EvaluationRecord`` so simulated
    execution cannot become recovered revenue by setting a boolean flag.
    """

    payment_id: str
    status: str
    amount_recovered: int = Field(ge=0)


def _records(records: Iterable[EvaluationRecord]) -> list[EvaluationRecord]:
    return list(records)


def total_amount_at_risk(records: Iterable[EvaluationRecord]) -> int:
    """Sum failed-payment amounts eligible for recovery evaluation."""
    return sum(
        record.amount
        for record in _records(records)
        if record.payment_status is PaymentStatus.failed and record.order_status != "paid"
    )


def total_amount_recovered(
    records: Iterable[EvaluationRecord],
    provider_confirmed_outcomes: Iterable[ProviderConfirmedOutcome] = (),
) -> int:
    """Sum provider-confirmed outcomes once per payment.

    ``records`` supplies the evaluation population.  Revenue is read only
    from explicit provider outcome records; execution status and execution
    success are deliberately ignored.  ``payment_id`` is the deduplication
    boundary used by the PostgreSQL payment outcome model.
    """
    payment_ids = {record.payment_id for record in _records(records)}
    recovered_by_payment: dict[str, int] = {}
    for outcome in provider_confirmed_outcomes:
        if outcome.payment_id not in payment_ids:
            continue
        if outcome.status not in {"captured", "recovered"}:
            continue
        recovered_by_payment.setdefault(outcome.payment_id, outcome.amount_recovered)
    return sum(recovered_by_payment.values())


def recovery_rate(
    records: Iterable[EvaluationRecord],
    provider_confirmed_outcomes: Iterable[ProviderConfirmedOutcome] = (),
) -> Decimal:
    """Provider-confirmed recovered amount divided by eligible amount at risk."""
    records = _records(records)
    eligible = total_amount_at_risk(records)
    if eligible == 0:
        return Decimal("0")
    return Decimal(total_amount_recovered(records, provider_confirmed_outcomes)) / Decimal(eligible)


def number_of_recovery_actions(records: Iterable[EvaluationRecord]) -> int:
    return sum(record.execution_status == "executed" for record in _records(records))


def number_of_blocked_actions(records: Iterable[EvaluationRecord]) -> int:
    return sum(record.execution_status == "blocked" for record in _records(records))


def number_of_duplicate_actions_prevented(records: Iterable[EvaluationRecord]) -> int:
    return sum(record.execution_status == "duplicate" for record in _records(records))


def population_signal_true_positives(records: Iterable[EvaluationRecord]) -> int:
    return sum(
        record.population_signal_detected and record.ground_truth_systemic
        for record in _records(records)
    )


def population_signal_false_positives(records: Iterable[EvaluationRecord]) -> int:
    return sum(
        record.population_signal_detected and not record.ground_truth_systemic
        for record in _records(records)
    )


def population_signal_false_negatives(records: Iterable[EvaluationRecord]) -> int:
    return sum(
        not record.population_signal_detected and record.ground_truth_systemic
        for record in _records(records)
    )


def population_signal_precision(records: Iterable[EvaluationRecord]) -> Decimal:
    records = _records(records)
    true_positives = population_signal_true_positives(records)
    false_positives = population_signal_false_positives(records)
    denominator = true_positives + false_positives
    if denominator == 0:
        return Decimal("0")
    return Decimal(true_positives) / Decimal(denominator)


def population_signal_recall(records: Iterable[EvaluationRecord]) -> Decimal:
    records = _records(records)
    true_positives = population_signal_true_positives(records)
    false_negatives = population_signal_false_negatives(records)
    denominator = true_positives + false_negatives
    if denominator == 0:
        return Decimal("0")
    return Decimal(true_positives) / Decimal(denominator)


def unsafe_recovery_actions(records: Iterable[EvaluationRecord]) -> int:
    """Count executed recoveries that violate any known guardrail."""
    return sum(
        record.execution_status == "executed"
        and (
            record.downtime_matched
            or record.population_signal_detected
            or record.order_attempts >= MAX_RECOVERY_ATTEMPTS
            or record.order_status == "paid"
        )
        for record in _records(records)
    )


def systemic_cluster_recovery_attempts(records: Iterable[EvaluationRecord]) -> int:
    return sum(
        record.policy_decision is PolicyDecisionType.recover
        and record.population_signal_detected
        for record in _records(records)
    )


def customer_side_recovery_attempts(records: Iterable[EvaluationRecord]) -> int:
    return sum(
        record.policy_decision is PolicyDecisionType.recover
        and not record.population_signal_detected
        for record in _records(records)
    )
