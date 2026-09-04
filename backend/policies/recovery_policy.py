"""Deterministic authority for whether recovery is permitted."""

from enum import Enum

from pydantic import BaseModel

from backend.agents.recovery_agent import Diagnosis, DiagnosisCategory
from backend.pipeline.downtime_correlator import DowntimeCorrelationResult
from backend.schemas import Order, OrderStatus, PaymentAttempt, PaymentStatus, SignalContext


MAX_RECOVERY_ATTEMPTS = 3


class PolicyDecisionType(str, Enum):
    stop = "stop"
    cooldown = "cooldown"
    recover = "recover"
    review = "review"


class PolicyDecision(BaseModel):
    decision: PolicyDecisionType
    reason: str


def evaluate_policy(
    order: Order,
    payment: PaymentAttempt,
    signal: SignalContext,
    downtime: DowntimeCorrelationResult,
    diagnosis: Diagnosis,
) -> PolicyDecision:
    """Apply the recovery rules in a fixed, conservative order."""
    if order.status is OrderStatus.paid:
        return PolicyDecision(decision=PolicyDecisionType.stop, reason="Order is already paid.")
    if payment.status is PaymentStatus.captured:
        return PolicyDecision(decision=PolicyDecisionType.stop, reason="Payment is already captured.")
    if payment.status is PaymentStatus.failed and (
        payment.failed_at is None or payment.error is None
    ):
        return PolicyDecision(decision=PolicyDecisionType.review, reason="Failure information is incomplete.")
    if downtime.matched:
        return PolicyDecision(
            decision=PolicyDecisionType.cooldown,
            reason="Payment failure overlaps relevant downtime.",
        )
    if signal.is_cluster_candidate and diagnosis.category is DiagnosisCategory.systemic_issue:
        return PolicyDecision(
            decision=PolicyDecisionType.cooldown,
            reason="Population-level systemic failures require cooldown.",
        )
    if (
        diagnosis.category is DiagnosisCategory.customer_issue
        and not signal.is_cluster_candidate
        and not downtime.matched
    ):
        if order.attempts >= MAX_RECOVERY_ATTEMPTS:
            return PolicyDecision(
                decision=PolicyDecisionType.stop,
                reason="Maximum recovery attempts have been reached.",
            )
        return PolicyDecision(decision=PolicyDecisionType.recover, reason="Customer issue may be recovered safely.")
    if diagnosis.category is DiagnosisCategory.unknown:
        return PolicyDecision(decision=PolicyDecisionType.review, reason="Diagnosis is unknown.")
    return PolicyDecision(decision=PolicyDecisionType.review, reason="Evidence does not safely permit recovery.")
