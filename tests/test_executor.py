from backend.cache import RedisIdempotencyCache
from backend.policies.recovery_policy import PolicyDecision, PolicyDecisionType
from backend.recovery.executor import RecoveryExecutor


class FakeRedis:
    def __init__(self):
        self.values = {}

    def set(self, key, value, *, ex, nx):
        if nx and key in self.values:
            return False
        self.values[key] = (value, ex)
        return True


def executor():
    return RecoveryExecutor(RedisIdempotencyCache(FakeRedis()))


def decision(kind):
    return PolicyDecision(decision=kind, reason="policy test")


def test_recover_executes_simulated_retry():
    result = executor().execute("pay_1", "order_1", decision(PolicyDecisionType.recover), "key_1")
    assert result.executed is True
    assert result.action == "retry_payment"
    assert result.status == "executed"
    assert "simulated" in result.reason.lower()


def test_non_recover_decisions_do_not_execute():
    for kind in (PolicyDecisionType.stop, PolicyDecisionType.cooldown, PolicyDecisionType.review):
        result = executor().execute("pay_1", "order_1", decision(kind), f"key_{kind.value}")
        assert result.executed is False
        assert result.action is None
        assert result.status == "blocked"


def test_duplicate_idempotency_key_executes_only_once():
    instance = executor()
    first = instance.execute("pay_1", "order_1", decision(PolicyDecisionType.recover), "same_key")
    second = instance.execute("pay_1", "order_1", decision(PolicyDecisionType.recover), "same_key")
    assert first.status == "executed"
    assert second.status == "duplicate"
    assert second.executed is False


def test_audit_contains_decision_action_reason_identity_and_timestamp():
    result = executor().execute("pay_1", "order_1", decision(PolicyDecisionType.recover), "key_1")
    audit = result.audit
    assert audit.payment_id == "pay_1"
    assert audit.order_id == "order_1"
    assert audit.policy_decision is PolicyDecisionType.recover
    assert audit.action == "retry_payment"
    assert audit.status == "executed"
    assert audit.reason
    assert audit.timestamp is not None


def test_executor_does_not_override_policy():
    result = executor().execute("pay_1", "order_1", decision(PolicyDecisionType.cooldown), "key_1")
    assert result.status == "blocked"
    assert result.audit.policy_decision is PolicyDecisionType.cooldown
