"""Run the complete deterministic REVIVE pipeline over one synthetic batch."""

from collections import defaultdict
from datetime import timedelta
import json
from decimal import Decimal
from typing import Any

from pydantic import BaseModel

from backend.agents.recovery_agent import DiagnosisCategory, DiagnosisEvidence, RecoveryAgent
from backend.cache import RedisFailureCache, RedisIdempotencyCache
from backend.evaluation.metrics import (
    EvaluationRecord,
    customer_side_recovery_attempts,
    number_of_blocked_actions,
    number_of_duplicate_actions_prevented,
    number_of_recovery_actions,
    systemic_cluster_recovery_attempts,
    total_amount_at_risk,
    total_amount_recovered,
    unsafe_recovery_actions,
)
from backend.generator import generate_batch
from backend.pipeline.downtime_correlator import correlate_downtime
from backend.pipeline.signal_detector import SignalDetector
from backend.policies.recovery_policy import PolicyDecisionType, evaluate_policy
from backend.recovery.executor import RecoveryExecutor
from backend.schemas import Downtime, PaymentAttempt, PaymentStatus


class BatchResult(BaseModel):
    total_attempts: int
    total_revenue_at_risk: int
    eligible_revenue: int
    recovered_revenue: int
    recovery_rate: Decimal
    recovery_action_count: int
    blocked_action_count: int
    cooldown_count: int
    review_count: int
    systemic_case_count: int
    customer_issue_count: int
    unknown_case_count: int
    successful_recovery_count: int
    duplicate_execution_count: int
    unsafe_action_count: int


class _MemoryRedis:
    """Redis-shaped test storage used for the offline batch experiment."""

    def __init__(self) -> None:
        self.sorted_sets: dict[str, dict[str, float]] = {}
        self.values: dict[str, tuple[str, int]] = {}

    def zadd(self, key: str, members: dict[str, float]) -> None:
        self.sorted_sets.setdefault(key, {}).update(members)

    def zremrangebyscore(self, key: str, minimum: str, maximum: str) -> None:
        cutoff = float(maximum.lstrip("("))
        for member, score in list(self.sorted_sets.get(key, {}).items()):
            if score <= cutoff:
                del self.sorted_sets[key][member]

    def zcard(self, key: str) -> int:
        return len(self.sorted_sets.get(key, {}))

    def expire(self, key: str, seconds: int) -> bool:
        return True

    def set(self, key: str, value: str, *, ex: int, nx: bool) -> bool:
        if nx and key in self.values:
            return False
        self.values[key] = (value, ex)
        return True


def _synthetic_downtimes(attempts: list[PaymentAttempt]) -> list[Downtime]:
    """Create synthetic provider windows from repeated generated failure groups."""
    groups: dict[tuple[str, str], list[PaymentAttempt]] = defaultdict(list)
    for attempt in attempts:
        if attempt.status is PaymentStatus.failed and attempt.error and attempt.failed_at:
            groups[(attempt.issuer_bin, attempt.error.code)].append(attempt)

    downtimes = []
    for index, ((issuer_bin, error_code), members) in enumerate(sorted(groups.items()), start=1):
        if len(members) < 5:
            continue
        downtimes.append(
            Downtime(
                downtime_id=f"downtime_batch_{index}",
                entity="payments",
                method="card",
                begin=min(item.failed_at for item in members) - timedelta(minutes=1),
                end=max(item.failed_at for item in members) + timedelta(minutes=1),
                status="resolved",
                scheduled=False,
                severity="high",
                instrument=f"issuer_{issuer_bin}",
            )
        )
    return downtimes


def _offline_diagnosis_provider(prompt: str) -> dict[str, Any]:
    """Deterministic offline provider used because no AI dependency is configured."""
    evidence = json.loads(prompt.split("Evidence: ", 1)[1])
    signal = evidence["signal"]
    downtime = evidence["downtime"]
    error_code = (evidence["payment"].get("error") or {}).get("code")
    if signal["is_cluster_candidate"] or downtime["matched"]:
        return {
            "category": DiagnosisCategory.systemic_issue.value,
            "confidence": 1.0,
            "reason": "Evidence contains a population signal or matching downtime.",
        }
    if error_code in {"insufficient_funds", "expired_card", "cvv_mismatch"}:
        return {
            "category": DiagnosisCategory.customer_issue.value,
            "confidence": 1.0,
            "reason": "Evidence contains a customer-side card failure without systemic evidence.",
        }
    return {
        "category": DiagnosisCategory.unknown.value,
        "confidence": 0.0,
        "reason": "The supplied evidence is insufficient for classification.",
    }


def run_batch() -> BatchResult:
    """Process exactly 100 seed-42 attempts through the existing pipeline."""
    batch = generate_batch(num_attempts=100, seed=42)
    orders = {order.order_id: order for order in batch["orders"]}
    downtimes = _synthetic_downtimes(batch["payment_attempts"])
    redis = _MemoryRedis()
    detector = SignalDetector(RedisFailureCache(redis))
    executor = RecoveryExecutor(RedisIdempotencyCache(redis))
    records: list[EvaluationRecord] = []
    decisions: list[PolicyDecisionType] = []
    diagnoses: list[DiagnosisCategory] = []

    for attempt in batch["payment_attempts"]:
        signal = detector.detect(attempt)
        downtime = correlate_downtime(attempt, downtimes)
        evidence = DiagnosisEvidence(
            order=orders[attempt.order_id],
            payment=attempt,
            signal=signal,
            downtime=downtime,
        )
        diagnosis = RecoveryAgent(_offline_diagnosis_provider).diagnose(evidence)
        policy = evaluate_policy(orders[attempt.order_id], attempt, signal, downtime, diagnosis)
        execution = executor.execute(
            attempt.payment_id,
            attempt.order_id,
            policy,
            f"batch-seed-42:{attempt.payment_id}",
        )
        truth = batch["ground_truth"]["attempts"].get(attempt.payment_id, {})
        decisions.append(policy.decision)
        if attempt.status is PaymentStatus.failed:
            diagnoses.append(diagnosis.category)
        records.append(
            EvaluationRecord(
                payment_id=attempt.payment_id,
                amount=attempt.amount,
                policy_decision=policy.decision,
                execution_status=execution.status,
                # The simulator's executed result is the explicit simulated outcome.
                recovery_succeeded=execution.executed,
                payment_status=attempt.status,
                order_status=orders[attempt.order_id].status.value,
                order_attempts=orders[attempt.order_id].attempts,
                downtime_matched=downtime.matched,
                systemic_cluster=truth.get("is_clustered", False),
            )
        )

    eligible_revenue = sum(
        record.amount
        for record in records
        if record.policy_decision is PolicyDecisionType.recover
    )
    recovered_revenue = total_amount_recovered(records)
    recovery_rate = (
        Decimal(recovered_revenue) / Decimal(eligible_revenue)
        if eligible_revenue
        else Decimal("0")
    )
    return BatchResult(
        total_attempts=len(batch["payment_attempts"]),
        total_revenue_at_risk=total_amount_at_risk(records),
        eligible_revenue=eligible_revenue,
        recovered_revenue=recovered_revenue,
        recovery_rate=recovery_rate,
        recovery_action_count=number_of_recovery_actions(records),
        blocked_action_count=number_of_blocked_actions(records),
        cooldown_count=sum(decision is PolicyDecisionType.cooldown for decision in decisions),
        review_count=sum(decision is PolicyDecisionType.review for decision in decisions),
        systemic_case_count=sum(category is DiagnosisCategory.systemic_issue for category in diagnoses),
        customer_issue_count=sum(category is DiagnosisCategory.customer_issue for category in diagnoses),
        unknown_case_count=sum(category is DiagnosisCategory.unknown for category in diagnoses),
        successful_recovery_count=sum(record.recovery_succeeded for record in records),
        duplicate_execution_count=number_of_duplicate_actions_prevented(records),
        unsafe_action_count=unsafe_recovery_actions(records),
    )


def format_report(result: BatchResult) -> str:
    """Format metrics without adding nondeterministic timestamps or IDs."""
    rate = f"{result.recovery_rate:.2%}"
    return "\n".join(
        [
            "## REVIVE BATCH EVALUATION",
            "",
            f"Attempts processed:       {result.total_attempts}",
            f"Revenue at risk:          INR {result.total_revenue_at_risk / 100:,.2f}",
            f"Eligible revenue:         INR {result.eligible_revenue / 100:,.2f}",
            f"Recovered revenue:        INR {result.recovered_revenue / 100:,.2f}",
            f"Recovery rate:            {rate}",
            "",
            "Decisions",
            f"Recover:                  {result.recovery_action_count}",
            f"Cooldown:                 {result.cooldown_count}",
            f"Review:                   {result.review_count}",
            f"Stop/Blocked:             {result.blocked_action_count}",
            "",
            "Failure classification",
            f"Customer-side:            {result.customer_issue_count}",
            f"Systemic:                 {result.systemic_case_count}",
            f"Unknown:                  {result.unknown_case_count}",
            "",
            "Safety",
            f"Successful recoveries:    {result.successful_recovery_count}",
            f"Duplicate executions:     {result.duplicate_execution_count}",
            f"Unsafe actions:           {result.unsafe_action_count}",
        ]
    )


if __name__ == "__main__":
    print(format_report(run_batch()))
