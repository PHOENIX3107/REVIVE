"""Run deterministic, provider-honest REVIVE demo scenarios.

This runner uses the existing webhook ingestion, population incident,
recovery-case, executor, and reconciliation components. Scenario C uses a
local HTTP fixture through the existing Razorpay read-only client; it does not
call Razorpay or invent a retry endpoint.

Usage:

    REVIVE_TEST_DATABASE_URL=... REDIS_URL=... \
      ./.venv/bin/python -m scripts.demo_scenarios --reset
"""

from argparse import ArgumentParser
from datetime import datetime, timezone
import hashlib
import hmac
import json
import os
from typing import Any

import httpx
import redis

from backend.db import Database
from backend.integrations.razorpay.client import RazorpayClient, RazorpayClientConfig
from backend.integrations.razorpay.signature import verify_webhook_signature
from backend.main import build_recovery_case_processor
from backend.population_incidents import PopulationIncidentActivator
from backend.recovery.reconciliation import RazorpayPaymentReconciler
from backend.webhooks.razorpay import RazorpayWebhookProcessor


DEMO_SECRET = "revive-local-demo-webhook-secret"
DEMO_NOW = datetime(2026, 1, 1, tzinfo=timezone.utc)


class DemoEnvironment:
    def __init__(self, database: Database, redis_client: Any) -> None:
        self.database = database
        self.redis = redis_client
        self.activator = PopulationIncidentActivator(database, redis_client)
        self.recovery_processor = build_recovery_case_processor(
            database,
            redis_client=redis_client,
            population_incident_activator=self.activator,
        )
        self.webhook_processor = RazorpayWebhookProcessor(
            database=database,
            population_incident_activator=self.activator,
        )

    def ingest_failure(
        self,
        *,
        scenario: str,
        index: int,
        error_code: str,
        issuer_bin: str,
        amount: int = 1000,
    ) -> str:
        payment_id = f"pay_demo_{scenario}_{index}"
        order_id = f"order_demo_{scenario}_{index}"
        event_id = f"evt_demo_{scenario}_{index}"
        timestamp = int(DEMO_NOW.timestamp()) + index
        payload = {
            "event": "payment.failed",
            "created_at": timestamp,
            "payload": {
                "payment": {
                    "entity": {
                        "id": payment_id,
                        "order_id": order_id,
                        "amount": amount,
                        "currency": "INR",
                        "status": "failed",
                        "captured": False,
                        "amount_captured": 0,
                        "method": "card",
                        "created_at": timestamp,
                        "card": {"iin": issuer_bin},
                        "error_code": error_code,
                        "error_description": f"Demo failure: {error_code}",
                        "error_source": "bank",
                        "error_step": "payment_authentication",
                        "error_reason": error_code,
                    }
                },
                "order": {
                    "entity": {
                        "id": order_id,
                        "amount": amount,
                        "amount_paid": 0,
                        "amount_due": amount,
                        "currency": "INR",
                        "status": "attempted",
                        "attempts": 1,
                        "created_at": timestamp,
                    }
                },
            },
        }
        body = json.dumps(payload).encode("utf-8")
        signature = hmac.new(DEMO_SECRET.encode("utf-8"), body, hashlib.sha256).hexdigest()
        verify_webhook_signature(body, signature, DEMO_SECRET)
        result = self.webhook_processor.ingest(
            body,
            {
                "x-razorpay-event-id": event_id,
                "X-Razorpay-Signature": signature,
            },
        )
        if result.status not in {"accepted", "duplicate"}:
            raise RuntimeError(f"Demo ingestion failed for {payment_id}: {result.status}")
        return f"recovery-{payment_id}"


def reset_state(database: Database, redis_client: Any) -> None:
    database.initialize()
    with database.transaction() as connection:
        connection.execute(
            "DELETE FROM population_incidents WHERE trigger_payment_id LIKE 'pay_demo_%'"
        )
        connection.execute(
            "DELETE FROM payment_outcomes WHERE payment_id LIKE 'pay_demo_%'"
        )
        connection.execute(
            "DELETE FROM audit_events WHERE payment_id LIKE 'pay_demo_%'"
        )
        connection.execute(
            "DELETE FROM executions WHERE case_id LIKE 'recovery-pay_demo_%'"
        )
        connection.execute(
            "DELETE FROM decisions WHERE case_id LIKE 'recovery-pay_demo_%'"
        )
        connection.execute(
            "DELETE FROM diagnoses WHERE case_id LIKE 'recovery-pay_demo_%'"
        )
        connection.execute(
            "DELETE FROM recovery_cases WHERE case_id LIKE 'recovery-pay_demo_%'"
        )
        connection.execute(
            "DELETE FROM webhook_events WHERE event_id LIKE 'evt_demo_%'"
        )
        connection.execute(
            "DELETE FROM payment_attempts WHERE payment_id LIKE 'pay_demo_%'"
        )
        connection.execute(
            "DELETE FROM orders WHERE order_id LIKE 'order_demo_%'"
        )
    for cohort in (
        "population:v1:411111:insufficient_funds",
        "population:v1:453002:issuer_declined",
        "population:v1:411112:insufficient_funds",
    ):
        redis_client.delete(f"population:window:{cohort}")
    for issuer_bin, error_code in (
        ("411111", "insufficient_funds"),
        ("453002", "issuer_declined"),
        ("411112", "insufficient_funds"),
    ):
        redis_client.delete(f"failures:{issuer_bin}:{error_code}")
    for scenario, count in (("customer", 1), ("systemic", 5), ("confirmed", 1)):
        for index in range(count):
            redis_client.delete(
                f"idempotency:recovery-case:recovery-pay_demo_{scenario}_{index}"
            )


def run_customer_failure(environment: DemoEnvironment) -> dict[str, object]:
    case_id = environment.ingest_failure(
        scenario="customer",
        index=0,
        error_code="insufficient_funds",
        issuer_bin="411111",
    )
    result = environment.recovery_processor.process(case_id)
    if result.decision.decision.value != "recover" or not result.execution.executed:
        raise RuntimeError("Customer-side demo did not produce a simulated recovery decision.")
    return {
        "input": "insufficient_funds",
        "observation": "one failed payment ingested",
        "signal": "no population incident",
        "diagnosis": result.diagnosis.category.value,
        "policy": result.decision.decision.value,
        "execution": "simulated recovery execution",
        "provider_outcome": "none",
        "revenue_effect": "INR 0.00; no provider confirmation",
    }


def run_systemic_failure(environment: DemoEnvironment) -> dict[str, object]:
    first_case = environment.ingest_failure(
        scenario="systemic",
        index=0,
        error_code="issuer_declined",
        issuer_bin="453002",
    )
    before = environment.recovery_processor.process(first_case)
    case_ids = [first_case]
    for index in range(1, 5):
        case_ids.append(
            environment.ingest_failure(
                scenario="systemic",
                index=index,
                error_code="issuer_declined",
                issuer_bin="453002",
            )
        )
    after = environment.recovery_processor.process(case_ids[-1])
    if before.population_incident.population_incident_active:
        raise RuntimeError("Systemic demo exposed an incident before the fifth observation.")
    if (
        not after.population_incident.population_incident_active
        or after.decision.decision.value != "cooldown"
        or after.execution.executed
    ):
        raise RuntimeError("Systemic demo did not produce an active-incident cooldown.")
    return {
        "input": "5 unique issuer_declined failures, issuer 453002",
        "observation": "5 failures ingested within the 600-second cohort window",
        "signal": f"ACTIVE incident {after.population_incident.population_incident_id}",
        "diagnosis": after.diagnosis.category.value,
        "policy": after.decision.decision.value,
        "execution": "blocked; no simulated recovery execution",
        "provider_outcome": "none",
        "revenue_effect": "INR 0.00 recovered; systemic retry suppressed",
    }


def run_confirmed_recovery(environment: DemoEnvironment) -> dict[str, object]:
    case_id = environment.ingest_failure(
        scenario="confirmed",
        index=0,
        error_code="insufficient_funds",
        issuer_bin="411112",
        amount=1500,
    )
    decision = environment.recovery_processor.process(case_id)
    payment = environment.database.get_payment_attempt(decision.payment_id)
    order = environment.database.get_order(decision.order_id)
    if payment is None or order is None:
        raise RuntimeError("Confirmed-recovery demo is missing durable payment/order state.")

    def provider_response(request: httpx.Request) -> httpx.Response:
        if request.url.path.endswith(f"/payments/{payment.payment_id}"):
            return httpx.Response(
                200,
                json={
                    "id": payment.payment_id,
                    "entity": "payment",
                    "amount": payment.amount,
                    "currency": order.currency,
                    "status": "captured",
                    "method": "card",
                    "order_id": order.order_id,
                    "captured": True,
                    "amount_captured": payment.amount,
                    "created_at": int(DEMO_NOW.timestamp()),
                    "card": {"iin": payment.issuer_bin},
                },
            )
        if request.url.path.endswith(f"/orders/{order.order_id}"):
            return httpx.Response(
                200,
                json={
                    "id": order.order_id,
                    "entity": "order",
                    "amount": order.amount,
                    "amount_paid": order.amount,
                    "amount_due": 0,
                    "currency": order.currency,
                    "status": "paid",
                    "attempts": order.attempts,
                    "created_at": int(DEMO_NOW.timestamp()),
                },
            )
        return httpx.Response(404)

    http_client = httpx.Client(transport=httpx.MockTransport(provider_response))
    provider_client = RazorpayClient(
        RazorpayClientConfig(
            "rzp_test_demo_key",
            "demo-only-secret",
            "https://demo.example/v1",
        ),
        http_client=http_client,
    )
    try:
        reconciled = RazorpayPaymentReconciler(
            environment.database,
            provider_client,
        ).verify_case(case_id)
    finally:
        http_client.close()
    if not reconciled.recovery_confirmed or reconciled.recovered_amount != payment.amount:
        raise RuntimeError("Confirmed-recovery demo did not persist provider-confirmed revenue.")
    return {
        "input": "customer-side failure followed by a provider captured response",
        "observation": "failed payment ingested and recovery decision persisted",
        "signal": "no population incident",
        "diagnosis": decision.diagnosis.category.value,
        "policy": decision.decision.decision.value,
        "execution": "simulated recovery execution remains separate",
        "provider_outcome": "captured payment and paid order confirmed by reconciliation fixture",
        "revenue_effect": f"INR {payment.amount / 100:.2f} recovered",
    }


def main() -> int:
    parser = ArgumentParser(description=__doc__)
    parser.add_argument(
        "--reset",
        action="store_true",
        help="Remove only this runner's namespaced demo records and Redis keys before seeding.",
    )
    args = parser.parse_args()
    database = Database(os.getenv("REVIVE_TEST_DATABASE_URL") or os.getenv("DATABASE_URL"))
    redis_client = redis.Redis.from_url(
        os.getenv("REDIS_URL", "redis://localhost:6379/0"),
        decode_responses=True,
    )
    database.initialize()
    if args.reset:
        reset_state(database, redis_client)
    environment = DemoEnvironment(database, redis_client)
    scenarios = {
        "A_customer_side_failure": run_customer_failure(environment),
        "B_systemic_failure": run_systemic_failure(environment),
        "C_confirmed_recovery": run_confirmed_recovery(environment),
    }
    print("REVIVE DEMO SCENARIOS")
    print(json.dumps(scenarios, indent=2, default=str))
    print("\nDurable dashboard metrics:")
    print(json.dumps(database.get_dashboard_metrics(), indent=2, default=str))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
