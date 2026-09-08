from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timedelta, timezone
import hashlib
import hmac
import json
import os
import threading

import pytest
from fastapi.testclient import TestClient

from backend.agents.recovery_agent import RecoveryAgent
from backend.cache import RedisFailureCache, RedisIdempotencyCache
from backend.db import Database
from backend.pipeline.signal_detector import SignalDetector
from backend.main import create_app
from backend.policies.recovery_policy import PolicyDecisionType
from backend.population_incidents import (
    PopulationIncidentActivator,
    PopulationIncidentStatus,
    population_cohort_key,
)
from backend.recovery.case_processor import RecoveryCaseProcessor
from backend.recovery.executor import RecoveryExecutor
from backend.schemas import (
    Order,
    OrderStatus,
    PaymentAttempt,
    PaymentError,
    PaymentMethod,
    PaymentStatus,
)
from backend.webhooks.razorpay import RazorpayWebhookProcessor


DATABASE_URL = os.getenv("REVIVE_TEST_DATABASE_URL")
pytestmark = pytest.mark.skipif(
    not DATABASE_URL,
    reason="REVIVE_TEST_DATABASE_URL is required for PostgreSQL integration tests",
)


NOW = datetime(2026, 1, 1, tzinfo=timezone.utc)


class FakeClock:
    def __init__(self, value: datetime = NOW) -> None:
        self.value = value

    def __call__(self) -> datetime:
        return self.value

    def advance(self, **kwargs: int) -> None:
        self.value += timedelta(**kwargs)


class FakeRedis:
    """Small Redis sorted-set subset with the production command semantics."""

    def __init__(self) -> None:
        self.sorted_sets: dict[str, dict[str, float]] = {}
        self.values: dict[str, tuple[str, int]] = {}
        self._lock = threading.RLock()

    def zadd(self, key, members, *, nx=False):
        with self._lock:
            sorted_set = self.sorted_sets.setdefault(key, {})
            added = 0
            for member, score in members.items():
                if nx and member in sorted_set:
                    continue
                if member not in sorted_set:
                    added += 1
                sorted_set[member] = float(score)
            return added

    def zremrangebyscore(self, key, minimum, maximum):
        with self._lock:
            lower = float(minimum)
            upper_text = str(maximum)
            upper_exclusive = upper_text.startswith("(")
            upper = float(upper_text.lstrip("("))
            sorted_set = self.sorted_sets.get(key, {})
            removed = 0
            for member, score in list(sorted_set.items()):
                if score >= lower and (score < upper if upper_exclusive else score <= upper):
                    del sorted_set[member]
                    removed += 1
            return removed

    def zcard(self, key):
        with self._lock:
            return len(self.sorted_sets.get(key, {}))

    def zcount(self, key, minimum, maximum):
        with self._lock:
            lower_text = str(minimum)
            upper_text = str(maximum)
            lower_exclusive = lower_text.startswith("(")
            upper_exclusive = upper_text.startswith("(")
            lower = float(lower_text.lstrip("("))
            upper = float("inf") if upper_text == "+inf" else float(upper_text.lstrip("("))
            return sum(
                (score > lower if lower_exclusive else score >= lower)
                and (score < upper if upper_exclusive else score <= upper)
                for score in self.sorted_sets.get(key, {}).values()
            )

    def expire(self, key, seconds):
        return True

    def set(self, key, value, *, ex, nx):
        with self._lock:
            if nx and key in self.values:
                return False
            self.values[key] = (value, ex)
            return True


@pytest.fixture
def database():
    database = Database(DATABASE_URL)
    database.initialize()
    with database.transaction() as connection:
        connection.execute(
            "TRUNCATE population_incidents, payment_outcomes, audit_events, executions, "
            "decisions, diagnoses, recovery_cases, webhook_events, payment_attempts, orders CASCADE"
        )
    return database


def _payment(
    database: Database,
    index: int,
    *,
    failed_at: datetime = NOW,
    issuer_bin: str = "411111",
    error_code: str = "issuer_declined",
) -> PaymentAttempt:
    order_id = f"order_population_{index}"
    payment = PaymentAttempt(
        payment_id=f"pay_population_{index}",
        order_id=order_id,
        amount=49900,
        method=PaymentMethod.card,
        status=PaymentStatus.failed,
        captured=False,
        created_at=NOW,
        error=PaymentError(code=error_code),
        issuer_bin=issuer_bin,
        failed_at=failed_at,
    )
    database.save_order(
        Order(
            order_id=order_id,
            amount=49900,
            amount_paid=0,
            amount_due=49900,
            currency="INR",
            status=OrderStatus.attempted,
            attempts=1,
            created_at=NOW,
        )
    )
    database.save_payment_attempt(payment)
    return payment


def _activator(database, clock: FakeClock, redis: FakeRedis | None = None):
    return PopulationIncidentActivator(database, redis or FakeRedis(), clock=clock)


def _rows(database):
    with database.connection() as connection:
        return connection.execute(
            "SELECT * FROM population_incidents ORDER BY activated_at, incident_id"
        ).fetchall()


def test_four_observed_failures_do_not_activate(database):
    clock = FakeClock()
    activator = _activator(database, clock)

    results = [activator.observe_failure(_payment(database, index)) for index in range(4)]

    assert all(result is not None and not result.incident.population_incident_active for result in results)
    assert _rows(database) == []


def test_fifth_unique_observation_activates_exactly_one_incident(database):
    clock = FakeClock()
    activator = _activator(database, clock)

    results = [activator.observe_failure(_payment(database, index)) for index in range(5)]

    assert results[-1] is not None
    assert results[-1].observed_count == 5
    assert results[-1].incident.population_incident_active is True
    assert len(_rows(database)) == 1
    assert _rows(database)[0]["status"] == PopulationIncidentStatus.ACTIVE.value


def test_failed_payment_ingestion_activates_without_recovery_case_processing(database):
    clock = FakeClock()
    activator = _activator(database, clock)
    processor = RazorpayWebhookProcessor(
        database,
        population_incident_activator=activator,
    )

    for index in range(5):
        payment_id = f"pay_webhook_population_{index}"
        order_id = f"order_webhook_population_{index}"
        body = json.dumps(
            {
                "event": "payment.failed",
                "created_at": int((NOW + timedelta(days=30)).timestamp()),
                "payload": {
                    "payment": {
                        "entity": {
                            "id": payment_id,
                            "order_id": order_id,
                            "amount": 49900,
                            "currency": "INR",
                            "status": "failed",
                            "captured": False,
                            "amount_captured": 0,
                            "method": "card",
                            "created_at": int((NOW + timedelta(days=30)).timestamp()),
                            "card": {"iin": "411111"},
                            "error_code": "issuer_declined",
                        }
                    },
                    "order": {
                        "entity": {
                            "id": order_id,
                            "amount": 49900,
                            "amount_paid": 0,
                            "amount_due": 49900,
                            "currency": "INR",
                            "status": "attempted",
                            "attempts": 1,
                            "created_at": int(NOW.timestamp()),
                        }
                    },
                },
            }
        ).encode()
        processor.ingest(body, {"x-razorpay-event-id": f"evt_population_{index}"})

    assert len(_rows(database)) == 1
    assert _rows(database)[0]["status"] == PopulationIncidentStatus.ACTIVE.value


def test_duplicate_payment_id_does_not_increase_count_or_extend_expiry(database):
    clock = FakeClock()
    redis = FakeRedis()
    activator = _activator(database, clock, redis)
    payments = [_payment(database, index) for index in range(5)]
    for payment in payments:
        activator.observe_failure(payment)
    before = _rows(database)[0]

    clock.advance(seconds=100)
    duplicate = activator.observe_failure(payments[-1])
    after = _rows(database)[0]

    assert duplicate is not None
    assert duplicate.unique_observation is False
    assert duplicate.observed_count == 5
    assert after["expires_at"] == before["expires_at"]
    assert after["last_qualifying_observed_at"] == before["last_qualifying_observed_at"]


def test_active_incident_expiry_moves_with_unique_qualifying_failures(database):
    clock = FakeClock()
    activator = _activator(database, clock)
    for index in range(5):
        activator.observe_failure(_payment(database, index))
    initial = _rows(database)[0]

    clock.advance(seconds=100)
    activator.observe_failure(_payment(database, 5))
    current = _rows(database)[0]

    assert current["status"] == PopulationIncidentStatus.ACTIVE.value
    assert current["expires_at"] > initial["expires_at"]
    assert current["expires_at"] == clock.value + timedelta(seconds=600)


def test_incident_expires_after_six_hundred_seconds_without_activity(database):
    clock = FakeClock()
    activator = _activator(database, clock)
    payment = _payment(database, 0)
    for index in range(1, 5):
        activator.observe_failure(_payment(database, index))
    activator.observe_failure(payment)

    clock.advance(seconds=601)
    context = activator.active_context(payment)

    assert context.population_incident_active is False
    assert _rows(database)[0]["status"] == PopulationIncidentStatus.EXPIRED.value


def test_later_qualifying_cohort_creates_a_new_generation_after_expiry(database):
    clock = FakeClock()
    activator = _activator(database, clock)
    for index in range(5):
        activator.observe_failure(_payment(database, index))

    clock.advance(seconds=601)
    for index in range(5, 10):
        result = activator.observe_failure(_payment(database, index))

    rows = _rows(database)
    assert result is not None and result.incident.population_incident_active is True
    assert [row["status"] for row in rows] == [
        PopulationIncidentStatus.EXPIRED.value,
        PopulationIncidentStatus.ACTIVE.value,
    ]
    assert rows[0]["incident_id"] != rows[1]["incident_id"]


def test_concurrent_threshold_crossings_create_one_active_incident(database):
    clock = FakeClock()
    redis = FakeRedis()
    activator = _activator(database, clock, redis)
    payments = [_payment(database, index) for index in range(5)]

    with ThreadPoolExecutor(max_workers=5) as executor:
        list(executor.map(activator.observe_failure, payments))

    rows = _rows(database)
    assert sum(row["status"] == PopulationIncidentStatus.ACTIVE.value for row in rows) == 1


def test_future_provider_timestamp_does_not_activate_before_observation(database):
    clock = FakeClock()
    activator = _activator(database, clock)
    for index in range(4):
        activator.observe_failure(
            _payment(database, index, failed_at=NOW + timedelta(days=30))
        )
    future_payment = _payment(database, 4, failed_at=NOW + timedelta(days=30))

    assert _rows(database) == []
    result = activator.observe_failure(future_payment)

    assert result is not None and result.incident.population_incident_active is True
    assert _rows(database)[0]["activated_at"] == NOW


def test_out_of_order_provider_timestamps_do_not_change_observed_time_activation(database):
    clock = FakeClock()
    activator = _activator(database, clock)
    provider_times = [NOW + timedelta(days=4 - index) for index in range(5)]

    for index, provider_time in enumerate(provider_times):
        activator.observe_failure(_payment(database, index, failed_at=provider_time))
        clock.advance(seconds=1)

    row = _rows(database)[0]
    assert row["status"] == PopulationIncidentStatus.ACTIVE.value
    assert row["activated_at"] == NOW + timedelta(seconds=4)
    assert row["activated_at"] != provider_times[-1]


def test_recovery_case_processed_after_activation_receives_incident_context(database):
    clock = FakeClock()
    redis = FakeRedis()
    activator = _activator(database, clock, redis)
    payments = [_payment(database, index) for index in range(5)]
    for payment in payments:
        activator.observe_failure(payment)
    database.save_recovery_case(
        "case_population_context",
        payments[-1].payment_id,
        payments[-1].order_id,
        "open",
    )

    processor = RecoveryCaseProcessor(
        database=database,
        signal_detector=SignalDetector(RedisFailureCache(FakeRedis())),
        recovery_agent=RecoveryAgent(
            lambda _prompt: {
                "category": "systemic_issue",
                "confidence": 1.0,
                "reason": "The active incident is systemic evidence.",
            }
        ),
        recovery_executor=RecoveryExecutor(RedisIdempotencyCache(FakeRedis())),
        population_incident_activator=activator,
    )

    result = processor.process("case_population_context")

    assert result.signal is not None and result.signal.is_cluster_candidate is False
    assert result.population_incident.population_incident_active is True
    assert result.population_incident.population_incident_id == _rows(database)[0]["incident_id"]
    assert result.population_incident.population_incident_activated_at == NOW
    assert result.population_incident.population_incident_expires_at == NOW + timedelta(seconds=600)
    assert population_cohort_key("411111", "issuer_declined") == result.population_incident.cohort_key


def test_end_to_end_ingestion_activation_enrichment_duplicate_and_temporal_boundary(database):
    """Verify one real webhook-to-recovery population-incident lifecycle."""
    clock = FakeClock()
    population_redis = FakeRedis()
    activator = _activator(database, clock, population_redis)
    recovery_redis = FakeRedis()
    prompts: list[str] = []
    recovery_processor = RecoveryCaseProcessor(
        database=database,
        signal_detector=SignalDetector(RedisFailureCache(recovery_redis)),
        recovery_agent=RecoveryAgent(
            lambda prompt: prompts.append(prompt)
            or {
                "category": "systemic_issue",
                "confidence": 1.0,
                "reason": "Deterministic systemic test evidence.",
            }
        ),
        recovery_executor=RecoveryExecutor(RedisIdempotencyCache(recovery_redis)),
        population_incident_activator=activator,
    )
    secret = "population-e2e-secret"
    app = create_app(
        database,
        recovery_processor=recovery_processor,
        population_incident_activator=activator,
    )

    def post_failure(client: TestClient, index: int, *, event_id: str | None = None):
        payment_id = f"pay_population_e2e_{index}"
        order_id = f"order_population_e2e_{index}"
        body = json.dumps(
            {
                "event": "payment.failed",
                # Provider timestamps are deliberately unrelated to activation time.
                "created_at": int((NOW + timedelta(days=30)).timestamp()),
                "payload": {
                    "payment": {
                        "entity": {
                            "id": payment_id,
                            "order_id": order_id,
                            "amount": 49900,
                            "currency": "INR",
                            "status": "failed",
                            "captured": False,
                            "amount_captured": 0,
                            "method": "card",
                            "created_at": int((NOW + timedelta(days=30)).timestamp()),
                            "card": {"iin": "411111"},
                            "error_code": "issuer_declined",
                        }
                    },
                    "order": {
                        "entity": {
                            "id": order_id,
                            "amount": 49900,
                            "amount_paid": 0,
                            "amount_due": 49900,
                            "currency": "INR",
                            "status": "attempted",
                            "attempts": 1,
                            "created_at": int(NOW.timestamp()),
                        }
                    },
                },
            }
        ).encode()
        identifier = event_id or f"evt_population_e2e_{index}"
        signature = hmac.new(secret.encode(), body, hashlib.sha256).hexdigest()
        return client.post(
            "/webhooks/razorpay",
            content=body,
            headers={
                "X-Razorpay-Signature": signature,
                "x-razorpay-event-id": identifier,
            },
        )

    with TestClient(app) as client:
        monkeypatch_secret = os.environ.get("RAZORPAY_WEBHOOK_SECRET")
        os.environ["RAZORPAY_WEBHOOK_SECRET"] = secret
        try:
            assert post_failure(client, 0).status_code == 200
            before_activation = client.post(
                "/recovery-cases/recovery-pay_population_e2e_0/process"
            )
            assert before_activation.status_code == 200
            before_result = before_activation.json()
            assert before_result["population_incident"]["population_incident_active"] is False
            assert before_result["decision"]["decision"] != PolicyDecisionType.cooldown.value

            for index in range(1, 5):
                assert post_failure(client, index).status_code == 200

            with database.connection() as connection:
                active_rows = connection.execute(
                    """
                    SELECT incident_id, cohort_key, observed_count_at_activation,
                           trigger_payment_id, activated_at, expires_at
                    FROM population_incidents
                    WHERE status = 'ACTIVE'
                    """
                ).fetchall()
            assert len(active_rows) == 1
            incident = active_rows[0]
            assert incident["cohort_key"] == "population:v1:411111:issuer_declined"
            assert incident["observed_count_at_activation"] >= 5
            assert incident["trigger_payment_id"] == "pay_population_e2e_4"
            assert incident["activated_at"] is not None
            assert incident["expires_at"] > incident["activated_at"]
            assert incident["expires_at"] - incident["activated_at"] == timedelta(seconds=600)

            after_activation = client.post(
                "/recovery-cases/recovery-pay_population_e2e_4/process"
            )
            assert after_activation.status_code == 200
            after_result = after_activation.json()
            assert after_result["population_incident"]["population_incident_active"] is True
            assert after_result["population_incident"]["population_incident_id"] == incident["incident_id"]
            assert after_result["diagnosis"]["category"] == "systemic_issue"
            assert after_result["decision"]["decision"] == PolicyDecisionType.cooldown.value
            assert after_result["execution"]["executed"] is False
            assert after_result["execution"]["action"] is None
            assert json.loads(prompts[-1].split("Evidence: ", 1)[1])["signal"]["is_cluster_candidate"] is True

            recovery_row = next(
                row
                for row in client.get("/dashboard/recoveries").json()
                if row["case_id"] == "recovery-pay_population_e2e_4"
            )
            assert recovery_row["population_signal_detected"] is True
            assert recovery_row["population_incident_active"] is True
            assert recovery_row["population_incident_id"] == incident["incident_id"]
            assert recovery_row["population_incident_cohort_key"] == incident["cohort_key"]

            with database.connection() as connection:
                assert connection.execute(
                    "SELECT count(*) AS count FROM payment_outcomes"
                ).fetchone()["count"] == 0
            assert client.get("/dashboard/overview").json()["metrics"]["recovered_revenue"] == 0

            expiry_before_duplicate = incident["expires_at"]
            assert post_failure(
                client,
                4,
                event_id="evt_population_e2e_duplicate",
            ).status_code == 200
            with database.connection() as connection:
                active_after_duplicate = connection.execute(
                    """
                    SELECT incident_id, expires_at
                    FROM population_incidents
                    WHERE status = 'ACTIVE'
                    """
                ).fetchall()
            assert len(active_after_duplicate) == 1
            assert active_after_duplicate[0]["incident_id"] == incident["incident_id"]
            assert active_after_duplicate[0]["expires_at"] == expiry_before_duplicate
        finally:
            if monkeypatch_secret is None:
                os.environ.pop("RAZORPAY_WEBHOOK_SECRET", None)
            else:
                os.environ["RAZORPAY_WEBHOOK_SECRET"] = monkeypatch_secret
