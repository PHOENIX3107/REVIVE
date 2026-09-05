"""Advisory, structured diagnosis for failed payment attempts."""

import json
from collections.abc import Callable
from enum import Enum
from typing import Any

from pydantic import BaseModel, Field

from backend.pipeline.downtime_correlator import DowntimeCorrelationResult
from backend.schemas import Order, PaymentAttempt, SignalContext


class DiagnosisCategory(str, Enum):
    customer_issue = "customer_issue"
    systemic_issue = "systemic_issue"
    unknown = "unknown"


class Diagnosis(BaseModel):
    category: DiagnosisCategory
    confidence: float = Field(ge=0, le=1)
    reason: str


class DiagnosisEvidence(BaseModel):
    order: Order
    payment: PaymentAttempt
    signal: SignalContext
    downtime: DowntimeCorrelationResult


class RecoveryAgent:
    """Ask an injected model provider for diagnosis, without taking action."""

    def __init__(self, provider: Callable[[str], Any]) -> None:
        self.provider = provider

    def diagnose(self, evidence: DiagnosisEvidence) -> Diagnosis:
        prompt = self._build_prompt(evidence)
        try:
            response = self.provider(prompt)
            if isinstance(response, str):
                response = json.loads(response)
            return Diagnosis.model_validate(response)
        except (json.JSONDecodeError, TypeError, ValueError):
            return Diagnosis(
                category=DiagnosisCategory.unknown,
                confidence=0,
                reason="The diagnosis provider returned malformed or invalid evidence.",
            )

    @staticmethod
    def _build_prompt(evidence: DiagnosisEvidence) -> str:
        evidence_json = json.dumps(evidence.model_dump(mode="json"), sort_keys=True)
        return (
            "Diagnose this failed payment using only the supplied evidence. "
            "A failed payment is an attempt against an Order. Interpret the Order "
            "using the Razorpay-native model. A population-level cluster or matching "
            "downtime is evidence of a possible systemic issue. Customer-side error "
            "information is evidence of a customer issue. Conflicting or insufficient "
            "evidence is unknown. Do not invent facts. Return JSON with exactly "
            "category (customer_issue, systemic_issue, or unknown), confidence "
            "between 0 and 1, and reason. Diagnose only; do not recommend or execute "
            "recovery. Evidence: "
            f"{evidence_json}"
        )


def local_diagnosis_provider(prompt: str) -> dict[str, Any]:
    """Return a conservative deterministic diagnosis without an external model.

    ``RecoveryAgent`` remains the provider boundary.  This local provider is the
    default application composition for development and deliberately classifies
    insufficient or malformed evidence as unknown, which keeps the policy in
    control of whether recovery is allowed.
    """
    try:
        evidence = json.loads(prompt.split("Evidence: ", 1)[1])
        signal = evidence["signal"]
        downtime = evidence["downtime"]
        error_code = (evidence["payment"].get("error") or {}).get("code")
    except (IndexError, KeyError, TypeError, json.JSONDecodeError):
        return {
            "category": DiagnosisCategory.unknown.value,
            "confidence": 0.0,
            "reason": "The supplied evidence is insufficient for classification.",
        }

    if signal.get("is_cluster_candidate") or downtime.get("matched"):
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
