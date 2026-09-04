"""Bounded, simulated recovery execution."""

from datetime import datetime, timezone

from pydantic import BaseModel

from backend.cache import RedisIdempotencyCache
from backend.policies.recovery_policy import PolicyDecision, PolicyDecisionType


class AuditRecord(BaseModel):
    payment_id: str
    order_id: str
    policy_decision: PolicyDecisionType
    action: str | None
    status: str
    reason: str
    timestamp: datetime


class ExecutionResult(BaseModel):
    executed: bool
    action: str | None
    status: str
    reason: str
    audit: AuditRecord


class RecoveryExecutor:
    """Execute only a policy-approved, simulated payment retry."""

    def __init__(self, idempotency_cache: RedisIdempotencyCache) -> None:
        self.idempotency_cache = idempotency_cache

    def execute(
        self,
        payment_id: str,
        order_id: str,
        policy_decision: PolicyDecision,
        idempotency_key: str,
    ) -> ExecutionResult:
        timestamp = datetime.now(timezone.utc)
        decision = policy_decision.decision
        if decision is not PolicyDecisionType.recover:
            reason = f"Recovery blocked because policy decision is {decision.value}."
            return self._result(payment_id, order_id, decision, None, "blocked", reason, timestamp)

        if not self.idempotency_cache.claim(idempotency_key):
            return self._result(
                payment_id,
                order_id,
                decision,
                None,
                "duplicate",
                "Recovery request was already processed.",
                timestamp,
            )

        return self._result(
            payment_id,
            order_id,
            decision,
            "retry_payment",
            "executed",
            "Simulated retry_payment recovery executed.",
            timestamp,
        )

    @staticmethod
    def _result(
        payment_id: str,
        order_id: str,
        decision: PolicyDecisionType,
        action: str | None,
        status: str,
        reason: str,
        timestamp: datetime,
    ) -> ExecutionResult:
        audit = AuditRecord(
            payment_id=payment_id,
            order_id=order_id,
            policy_decision=decision,
            action=action,
            status=status,
            reason=reason,
            timestamp=timestamp,
        )
        return ExecutionResult(
            executed=status == "executed",
            action=action,
            status=status,
            reason=reason,
            audit=audit,
        )
