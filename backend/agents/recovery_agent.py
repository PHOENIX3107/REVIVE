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
