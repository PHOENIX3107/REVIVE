import json
from datetime import datetime, timezone

from backend.agents.recovery_agent import (
    DiagnosisCategory,
    DiagnosisEvidence,
    RecoveryAgent,
)
from backend.pipeline.downtime_correlator import DowntimeCorrelationResult
from backend.schemas import (
    Order,
    OrderStatus,
    PaymentAttempt,
    PaymentError,
    PaymentMethod,
    PaymentStatus,
    SignalContext,
)


def evidence():
    now = datetime(2026, 1, 1, tzinfo=timezone.utc)
    return DiagnosisEvidence(
        order=Order(
            order_id="order_test",
            amount=49900,
            amount_paid=0,
            amount_due=49900,
            currency="INR",
            status=OrderStatus.attempted,
            attempts=1,
            created_at=now,
        ),
        payment=PaymentAttempt(
            payment_id="pay_test",
            order_id="order_test",
            amount=49900,
            method=PaymentMethod.card,
            status=PaymentStatus.failed,
            captured=False,
            created_at=now,
            error=PaymentError(code="insufficient_funds"),
            issuer_bin="411111",
            failed_at=now,
        ),
        signal=SignalContext(is_cluster_candidate=False, matching_failure_count=1, window_seconds=600),
        downtime=DowntimeCorrelationResult(matched=False),
    )


def agent(response):
    return RecoveryAgent(lambda prompt: response)


def test_valid_categories_are_validated():
    for category in ("customer_issue", "systemic_issue", "unknown"):
        result = agent({"category": category, "confidence": 0.8, "reason": "Evidence supports this."}).diagnose(evidence())
        assert result.category.value == category


def test_provider_receives_structured_evidence_prompt():
    prompts = []
    result = RecoveryAgent(lambda prompt: prompts.append(prompt) or {"category": "unknown", "confidence": 0.2, "reason": "Insufficient evidence."}).diagnose(evidence())
    assert result.category is DiagnosisCategory.unknown
    assert '"order"' in prompts[0]
    assert '"payment"' in prompts[0]
    assert "do not recommend or execute recovery" in prompts[0]


def test_confidence_outside_range_becomes_unknown():
    result = agent({"category": "customer_issue", "confidence": 1.1, "reason": "Invalid."}).diagnose(evidence())
    assert result.category is DiagnosisCategory.unknown
    assert result.confidence == 0


def test_malformed_provider_output_becomes_unknown():
    result = agent("not json").diagnose(evidence())
    assert result.category is DiagnosisCategory.unknown
    assert "malformed" in result.reason


def test_diagnosis_has_no_execution_authority_fields():
    result = agent({"category": "customer_issue", "confidence": 0.9, "reason": "Card error."}).diagnose(evidence())
    assert set(result.model_dump()) == {"category", "confidence", "reason"}
