from datetime import timedelta
from decimal import Decimal

from backend.evaluation.metrics import ProviderConfirmedOutcome
from backend.evaluation.run_batch import (
    _OfflinePopulationIncidentLifecycle,
    _simulated_observed_at,
    format_report,
    run_batch,
)
from backend.generator import generate_batch
from backend.population_incidents import population_cohort_key
from backend.schemas import PaymentStatus, SignalContext


def test_batch_processes_exactly_100_attempts():
    assert run_batch().total_attempts == 100


def test_seed_42_metrics_are_reproducible():
    assert run_batch() == run_batch()
    assert format_report(run_batch()) == format_report(run_batch())


def test_revenue_at_risk_comes_from_generated_batch():
    result = run_batch()
    batch = generate_batch(num_attempts=100, seed=42)
    expected = sum(
        attempt.amount
        for attempt in batch["payment_attempts"]
        if attempt.status is PaymentStatus.failed
    )
    assert result.total_revenue_at_risk == expected


def test_recovery_rate_uses_eligible_revenue():
    result = run_batch()
    assert result.recovered_revenue == 0
    assert result.recovery_rate == Decimal("0")


def test_batch_populates_observed_and_ground_truth_population_signal_metrics():
    result = run_batch()
    assert result.population_signal_true_positive_count >= 0
    assert result.population_signal_false_positive_count >= 0
    assert result.population_signal_false_negative_count >= 0
    assert 0 <= result.population_signal_precision <= 1
    assert 0 <= result.population_signal_recall <= 1

    assert result.population_signal_true_positive_count + result.population_signal_false_positive_count > 0
    assert result.population_signal_true_positive_count + result.population_signal_false_negative_count > 0


def test_normal_batch_has_no_duplicates_or_unsafe_actions():
    result = run_batch()
    assert result.duplicate_execution_count == 0
    assert result.unsafe_action_count == 0


def test_simulated_executions_are_separate_from_recovered_revenue():
    result = run_batch()
    assert result.simulated_execution_count == result.recovery_action_count
    assert result.simulated_execution_count > 0
    assert result.recovered_revenue == 0


def test_batch_counts_only_explicit_provider_confirmed_outcomes():
    batch = generate_batch(num_attempts=100, seed=42)
    attempt = next(
        item
        for item in batch["payment_attempts"]
        if item.status is PaymentStatus.failed
        and item.error is not None
        and item.error.code == "insufficient_funds"
    )
    outcome = ProviderConfirmedOutcome(
        payment_id=attempt.payment_id,
        status="captured",
        amount_recovered=attempt.amount,
    )

    result = run_batch([outcome, outcome])

    assert result.recovered_revenue == attempt.amount


def test_systemic_cases_are_not_recovered():
    result = run_batch()
    assert result.systemic_case_count > 0
    assert result.cooldown_count >= result.systemic_case_count
    assert result.systemic_attempt_count == 0
    assert result.customer_attempt_count == result.recovery_action_count


def test_failure_classification_covers_failed_attempts_only():
    result = run_batch()
    assert result.customer_issue_count + result.systemic_case_count + result.unknown_case_count == 26


def test_offline_incident_lifecycle_does_not_preseed_future_failures_and_expires():
    batch = generate_batch(num_attempts=100, seed=42)
    cluster_ids = batch["ground_truth"]["clusters"]["synthetic_cluster_1"]
    attempts_by_id = {attempt.payment_id: attempt for attempt in batch["payment_attempts"]}
    cluster = [attempts_by_id[payment_id] for payment_id in cluster_ids]
    lifecycle = _OfflinePopulationIncidentLifecycle()

    observations = [
        lifecycle.observe_failure(attempt, _simulated_observed_at(attempt))
        for attempt in cluster
    ]
    cohort_key = population_cohort_key(cluster[0].issuer_bin, cluster[0].error.code)

    assert all(observation is not None for observation in observations)
    assert all(not observation.incident_active for observation in observations[:4])
    assert all(
        batch["ground_truth"]["attempts"][attempt.payment_id]["is_clustered"]
        for attempt in cluster[:4]
    )
    assert observations[4].incident_active is True
    assert observations[4].observed_count >= 5
    assert observations[4].cohort_key == cohort_key
    assert observations[5].incident_active is True

    last_observed_at = _simulated_observed_at(cluster[-1])
    assert lifecycle.is_active(
        cohort_key,
        last_observed_at + timedelta(seconds=601),
    ) is False


def test_offline_duplicate_observation_does_not_extend_incident():
    batch = generate_batch(num_attempts=100, seed=42)
    cluster_ids = batch["ground_truth"]["clusters"]["synthetic_cluster_1"]
    attempts_by_id = {attempt.payment_id: attempt for attempt in batch["payment_attempts"]}
    cluster = [attempts_by_id[payment_id] for payment_id in cluster_ids]
    lifecycle = _OfflinePopulationIncidentLifecycle()

    for attempt in cluster[:5]:
        lifecycle.observe_failure(attempt, _simulated_observed_at(attempt))
    cohort_key = population_cohort_key(cluster[0].issuer_bin, cluster[0].error.code)
    before = lifecycle.active_incident(cohort_key, _simulated_observed_at(cluster[4]))

    duplicate = lifecycle.observe_failure(
        cluster[4],
        _simulated_observed_at(cluster[4]) + timedelta(seconds=10),
    )
    after = lifecycle.active_incident(
        cohort_key,
        _simulated_observed_at(cluster[4]) + timedelta(seconds=10),
    )

    assert before is not None
    assert duplicate is not None
    assert duplicate.unique_observation is False
    assert duplicate.incident_id == before.incident_id
    assert after is not None
    assert after.expires_at == before.expires_at


def test_batch_population_signal_uses_observed_lifecycle_without_detector_or_truth(
    monkeypatch,
):
    def no_instantaneous_signal(_self, _attempt):
        return SignalContext(
            is_cluster_candidate=False,
            matching_failure_count=0,
            window_seconds=600,
        )

    monkeypatch.setattr(
        "backend.evaluation.run_batch.SignalDetector.detect",
        no_instantaneous_signal,
    )

    result = run_batch()

    assert result.population_signal_true_positive_count == 8
    assert result.population_signal_false_positive_count == 0
    assert result.population_signal_false_negative_count == 8
