"""Run the complete deterministic REVIVE pipeline over one synthetic batch."""

from collections import defaultdict
from collections.abc import Iterable
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
import json
from decimal import Decimal
from typing import Any

from pydantic import BaseModel

from backend.agents.recovery_agent import DiagnosisCategory, DiagnosisEvidence, RecoveryAgent
from backend.cache import RedisFailureCache, RedisIdempotencyCache
from backend.evaluation.metrics import (
    EvaluationRecord,
    ProviderConfirmedOutcome,
    customer_side_recovery_attempts,
    number_of_blocked_actions,
    number_of_duplicate_actions_prevented,
    number_of_recovery_actions,
    population_signal_false_negatives,
    population_signal_false_positives,
    population_signal_precision,
    population_signal_recall,
    population_signal_true_positives,
    systemic_cluster_recovery_attempts,
    total_amount_at_risk,
    total_amount_recovered,
    unsafe_recovery_actions,
)
from backend.generator import generate_batch
from backend.pipeline.downtime_correlator import correlate_downtime
from backend.pipeline.signal_detector import SignalDetector
from backend.population_incidents import (
    POPULATION_THRESHOLD,
    POPULATION_WINDOW_SECONDS,
    population_cohort_key,
)
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
    systemic_attempt_count: int
    customer_attempt_count: int
    blocked_action_count: int
    cooldown_count: int
    review_count: int
    systemic_case_count: int
    customer_issue_count: int
    unknown_case_count: int
    simulated_execution_count: int
    duplicate_execution_count: int
    unsafe_action_count: int
    population_signal_true_positive_count: int
    population_signal_false_positive_count: int
    population_signal_false_negative_count: int
    population_signal_precision: Decimal
    population_signal_recall: Decimal


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

    def zcount(self, key: str, minimum: float, maximum: str) -> int:
        return sum(
            score >= float(minimum)
            for score in self.sorted_sets.get(key, {}).values()
        )

    def expire(self, key: str, seconds: int) -> bool:
        return True

    def set(self, key: str, value: str, *, ex: int, nx: bool) -> bool:
        if nx and key in self.values:
            return False
        self.values[key] = (value, ex)
        return True


@dataclass(frozen=True)
class _OfflinePopulationIncident:
    incident_id: str
    cohort_key: str
    activated_at: datetime
    last_qualifying_observed_at: datetime
    expires_at: datetime


@dataclass(frozen=True)
class _OfflinePopulationObservation:
    cohort_key: str
    unique_observation: bool
    observed_count: int
    incident_active: bool
    incident_id: str | None


class _OfflinePopulationIncidentLifecycle:
    """Evaluation-only model of observed-time incident activation.

    Production activation is durable in PostgreSQL and indexed by Redis. The
    offline evaluator has no external stores, so it keeps the same state
    transitions in memory without changing production behavior or using the
    evaluator's ground truth as an input.
    """

    def __init__(
        self,
        *,
        threshold: int = POPULATION_THRESHOLD,
        window_seconds: int = POPULATION_WINDOW_SECONDS,
    ) -> None:
        self.threshold = threshold
        self.window_seconds = window_seconds
        self._observations: dict[str, dict[str, float]] = {}
        self._active: dict[str, _OfflinePopulationIncident] = {}
        self._generations: dict[str, int] = defaultdict(int)

    def observe_failure(
        self,
        attempt: PaymentAttempt,
        observed_at: datetime,
    ) -> _OfflinePopulationObservation | None:
        """Ingest one observed failure and evaluate its cohort at that time."""
        if attempt.status is not PaymentStatus.failed or attempt.error is None:
            return None

        observed_at = self._utc(observed_at)
        cohort_key = population_cohort_key(attempt.issuer_bin, attempt.error.code)
        score = observed_at.timestamp()
        members = self._observations.setdefault(cohort_key, {})
        unique_observation = attempt.payment_id not in members
        if unique_observation:
            members[attempt.payment_id] = score

        cutoff = score - self.window_seconds
        for payment_id, member_score in list(members.items()):
            if member_score < cutoff:
                del members[payment_id]
        observed_count = sum(
            cutoff <= member_score <= score for member_score in members.values()
        )

        active = self._active.get(cohort_key)
        if active is not None and active.expires_at <= observed_at:
            del self._active[cohort_key]
            active = None

        if active is not None:
            if (
                unique_observation
                and observed_at > active.last_qualifying_observed_at
            ):
                active = _OfflinePopulationIncident(
                    incident_id=active.incident_id,
                    cohort_key=active.cohort_key,
                    activated_at=active.activated_at,
                    last_qualifying_observed_at=observed_at,
                    expires_at=observed_at + timedelta(seconds=self.window_seconds),
                )
                self._active[cohort_key] = active
            return self._observation(cohort_key, unique_observation, observed_count, observed_at)

        if unique_observation and observed_count >= self.threshold:
            self._generations[cohort_key] += 1
            active = _OfflinePopulationIncident(
                incident_id=f"offline-{cohort_key}-{self._generations[cohort_key]}",
                cohort_key=cohort_key,
                activated_at=observed_at,
                last_qualifying_observed_at=observed_at,
                expires_at=observed_at + timedelta(seconds=self.window_seconds),
            )
            self._active[cohort_key] = active

        return self._observation(cohort_key, unique_observation, observed_count, observed_at)

    def is_active(self, cohort_key: str, decision_at: datetime) -> bool:
        """Return whether the incident was active at a case decision time."""
        decision_at = self._utc(decision_at)
        active = self._active.get(cohort_key)
        if active is None:
            return False
        if active.expires_at <= decision_at:
            del self._active[cohort_key]
            return False
        return active.activated_at <= decision_at

    def active_incident(
        self,
        cohort_key: str,
        decision_at: datetime,
    ) -> _OfflinePopulationIncident | None:
        """Return the incident visible at a simulated decision time."""
        if not self.is_active(cohort_key, decision_at):
            return None
        return self._active.get(cohort_key)

    def _observation(
        self,
        cohort_key: str,
        unique_observation: bool,
        observed_count: int,
        decision_at: datetime,
    ) -> _OfflinePopulationObservation:
        incident_active = self.is_active(cohort_key, decision_at)
        active = self._active.get(cohort_key)
        return _OfflinePopulationObservation(
            cohort_key=cohort_key,
            unique_observation=unique_observation,
            observed_count=observed_count,
            incident_active=incident_active,
            incident_id=active.incident_id if incident_active and active is not None else None,
        )

    @staticmethod
    def _utc(value: datetime) -> datetime:
        return value if value.tzinfo else value.replace(tzinfo=timezone.utc)


def _simulated_observed_at(attempt: PaymentAttempt) -> datetime:
    """Use the generated event time as this offline run's ingestion time."""
    return attempt.failed_at or attempt.created_at


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


def run_batch(
    provider_confirmed_outcomes: Iterable[ProviderConfirmedOutcome] = (),
) -> BatchResult:
    """Process the batch, optionally reporting explicit provider outcomes."""
    batch = generate_batch(num_attempts=100, seed=42)
    orders = {order.order_id: order for order in batch["orders"]}
    downtimes = _synthetic_downtimes(batch["payment_attempts"])
    redis = _MemoryRedis()
    detector = SignalDetector(RedisFailureCache(redis))
    population_lifecycle = _OfflinePopulationIncidentLifecycle()
    executor = RecoveryExecutor(RedisIdempotencyCache(redis))
    records: list[EvaluationRecord] = []
    decisions: list[PolicyDecisionType] = []
    diagnoses: list[DiagnosisCategory] = []

    for attempt in batch["payment_attempts"]:
        observed_at = _simulated_observed_at(attempt)
        population_observation = population_lifecycle.observe_failure(
            attempt,
            observed_at,
        )
        instantaneous_signal = detector.detect(attempt)
        incident_active = (
            population_observation.incident_active
            if population_observation is not None
            else False
        )
        # This is the same enrichment boundary used by recovery processing:
        # instantaneous detector evidence OR a durable incident active at the
        # simulated decision time. The detector's own implementation is not
        # changed here.
        signal = instantaneous_signal.model_copy(
            update={
                "is_cluster_candidate": (
                    instantaneous_signal.is_cluster_candidate or incident_active
                )
            }
        )
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
                execution_succeeded=execution.executed,
                payment_status=attempt.status,
                order_status=orders[attempt.order_id].status.value,
                order_attempts=orders[attempt.order_id].attempts,
                downtime_matched=downtime.matched,
                population_signal_detected=signal.is_cluster_candidate,
                ground_truth_systemic=truth.get("is_clustered", False),
                # Compatibility alias: this now reflects the observed signal,
                # not synthetic ground truth.
                systemic_cluster=signal.is_cluster_candidate,
            )
        )

    eligible_revenue = sum(
        record.amount
        for record in records
        if record.policy_decision is PolicyDecisionType.recover
    )
    recovered_revenue = total_amount_recovered(records, provider_confirmed_outcomes)
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
        systemic_attempt_count=systemic_cluster_recovery_attempts(records),
        customer_attempt_count=customer_side_recovery_attempts(records),
        blocked_action_count=number_of_blocked_actions(records),
        cooldown_count=sum(decision is PolicyDecisionType.cooldown for decision in decisions),
        review_count=sum(decision is PolicyDecisionType.review for decision in decisions),
        systemic_case_count=sum(category is DiagnosisCategory.systemic_issue for category in diagnoses),
        customer_issue_count=sum(category is DiagnosisCategory.customer_issue for category in diagnoses),
        unknown_case_count=sum(category is DiagnosisCategory.unknown for category in diagnoses),
        simulated_execution_count=sum(record.execution_succeeded for record in records),
        duplicate_execution_count=number_of_duplicate_actions_prevented(records),
        unsafe_action_count=unsafe_recovery_actions(records),
        population_signal_true_positive_count=population_signal_true_positives(records),
        population_signal_false_positive_count=population_signal_false_positives(records),
        population_signal_false_negative_count=population_signal_false_negatives(records),
        population_signal_precision=population_signal_precision(records),
        population_signal_recall=population_signal_recall(records),
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
            f"Systemic attempts:        {result.systemic_attempt_count}",
            f"Customer attempts:        {result.customer_attempt_count}",
            f"Cooldown:                 {result.cooldown_count}",
            f"Review:                   {result.review_count}",
            f"Stop/Blocked:             {result.blocked_action_count}",
            "",
            "Failure classification",
            f"Customer-side:            {result.customer_issue_count}",
            f"Systemic:                 {result.systemic_case_count}",
            f"Unknown:                  {result.unknown_case_count}",
            "",
            "Population signal",
            f"True positives:            {result.population_signal_true_positive_count}",
            f"False positives:           {result.population_signal_false_positive_count}",
            f"False negatives:           {result.population_signal_false_negative_count}",
            f"Precision:                 {result.population_signal_precision:.2%}",
            f"Recall:                    {result.population_signal_recall:.2%}",
            "",
            "Safety",
            f"Simulated executions:     {result.simulated_execution_count}",
            f"Duplicate executions:     {result.duplicate_execution_count}",
            f"Unsafe actions:           {result.unsafe_action_count}",
        ]
    )


if __name__ == "__main__":
    print(format_report(run_batch()))
