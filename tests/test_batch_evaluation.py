from decimal import Decimal

from backend.evaluation.metrics import ProviderConfirmedOutcome
from backend.evaluation.run_batch import format_report, run_batch
from backend.generator import generate_batch
from backend.schemas import PaymentStatus


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


def test_failure_classification_covers_failed_attempts_only():
    result = run_batch()
    assert result.customer_issue_count + result.systemic_case_count + result.unknown_case_count == 26
