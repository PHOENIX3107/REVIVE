"""Minimal PostgreSQL persistence for REVIVE domain and execution records."""

from contextlib import contextmanager
from datetime import datetime, timezone
import json
import os
from pathlib import Path
from typing import Any, Iterator

import psycopg
from psycopg.rows import dict_row

from backend.agents.recovery_agent import Diagnosis, DiagnosisCategory
from backend.recovery.executor import AuditRecord, ExecutionResult
from backend.schemas import Order, PaymentAttempt, PaymentError, PaymentMethod, PaymentStatus
from backend.policies.recovery_policy import PolicyDecision, PolicyDecisionType


SCHEMA_PATH = Path(__file__).with_name("migrations") / "001_initial.sql"


class DuplicateRecordError(RuntimeError):
    """Raised when a provider or idempotency identifier already exists."""


class Database:
    """Own PostgreSQL connections and transaction boundaries per operation."""

    def __init__(self, database_url: str | None = None) -> None:
        self.database_url = database_url or os.getenv("DATABASE_URL")
        if not self.database_url:
            raise ValueError("DATABASE_URL is required")

    @contextmanager
    def connection(self) -> Iterator[psycopg.Connection[Any]]:
        with psycopg.connect(self.database_url, row_factory=dict_row) as connection:
            yield connection

    @contextmanager
    def transaction(self) -> Iterator[psycopg.Connection[Any]]:
        with self.connection() as connection:
            try:
                yield connection
                connection.commit()
            except Exception:
                connection.rollback()
                raise

    def initialize(self) -> None:
        with self.transaction() as connection:
            connection.execute(SCHEMA_PATH.read_text(encoding="utf-8"))

    def ping(self) -> None:
        """Open and immediately close a connection to verify availability."""
        with self.connection() as connection:
            connection.execute("SELECT 1")

    def _save_order(self, connection: psycopg.Connection[Any], order: Order) -> None:
        connection.execute(
            """
            INSERT INTO orders
                (order_id, amount, amount_paid, amount_due, currency, status, attempts, created_at)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
            ON CONFLICT (order_id) DO UPDATE SET
                amount = GREATEST(orders.amount, EXCLUDED.amount),
                amount_paid = GREATEST(orders.amount_paid, EXCLUDED.amount_paid),
                amount_due = LEAST(orders.amount_due, EXCLUDED.amount_due),
                currency = EXCLUDED.currency,
                status = CASE
                    WHEN orders.status = 'paid' OR EXCLUDED.status = 'paid' THEN 'paid'
                    WHEN orders.status = 'attempted' OR EXCLUDED.status = 'attempted' THEN 'attempted'
                    ELSE 'created'
                END,
                attempts = GREATEST(orders.attempts, EXCLUDED.attempts),
                created_at = LEAST(orders.created_at, EXCLUDED.created_at)
            """,
            (order.order_id, order.amount, order.amount_paid, order.amount_due,
             order.currency, order.status.value, order.attempts, order.created_at),
        )

    def save_order(self, order: Order) -> None:
        with self.transaction() as connection:
            self._save_order(connection, order)

    def get_order(self, order_id: str) -> Order | None:
        with self.connection() as connection:
            row = connection.execute("SELECT * FROM orders WHERE order_id = %s", (order_id,)).fetchone()
        return Order(**row) if row else None

    def _save_payment_attempt(self, connection: psycopg.Connection[Any], attempt: PaymentAttempt) -> None:
        connection.execute(
            """
            INSERT INTO payment_attempts
                (payment_id, order_id, amount, method, status, captured, created_at,
                 error, issuer_bin, failed_at)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s::jsonb, %s, %s)
            ON CONFLICT (payment_id) DO UPDATE SET
                order_id = COALESCE(EXCLUDED.order_id, payment_attempts.order_id),
                amount = EXCLUDED.amount,
                method = EXCLUDED.method,
                status = CASE
                    WHEN payment_attempts.status = 'captured' OR EXCLUDED.status = 'captured' THEN 'captured'
                    WHEN payment_attempts.status = 'failed' OR EXCLUDED.status = 'failed' THEN 'failed'
                    ELSE 'authorized'
                END,
                captured = CASE
                    WHEN payment_attempts.status = 'captured' OR EXCLUDED.status = 'captured' THEN TRUE
                    ELSE FALSE
                END,
                created_at = LEAST(payment_attempts.created_at, EXCLUDED.created_at),
                error = CASE
                    WHEN payment_attempts.status = 'captured' OR EXCLUDED.status = 'captured' THEN NULL
                    WHEN payment_attempts.status = 'failed' OR EXCLUDED.status = 'failed' THEN COALESCE(EXCLUDED.error, payment_attempts.error)
                    ELSE NULL
                END,
                issuer_bin = COALESCE(NULLIF(EXCLUDED.issuer_bin, ''), payment_attempts.issuer_bin),
                failed_at = CASE
                    WHEN payment_attempts.status = 'captured' OR EXCLUDED.status = 'captured' THEN NULL
                    WHEN payment_attempts.status = 'failed' OR EXCLUDED.status = 'failed' THEN COALESCE(EXCLUDED.failed_at, payment_attempts.failed_at)
                    ELSE NULL
                END
            """,
            (
                attempt.payment_id,
                attempt.order_id,
                attempt.amount,
                attempt.method.value,
                attempt.status.value,
                attempt.captured,
                attempt.created_at,
                json.dumps(attempt.error.model_dump(mode="json")) if attempt.error else None,
                attempt.issuer_bin,
                attempt.failed_at,
            ),
        )

    def save_payment_attempt(self, attempt: PaymentAttempt) -> None:
        try:
            with self.transaction() as connection:
                connection.execute(
                    """
                    INSERT INTO payment_attempts
                        (payment_id, order_id, amount, method, status, captured, created_at,
                         error, issuer_bin, failed_at)
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s::jsonb, %s, %s)
                    """,
                    (
                        attempt.payment_id, attempt.order_id, attempt.amount,
                        attempt.method.value, attempt.status.value, attempt.captured,
                        attempt.created_at,
                        json.dumps(attempt.error.model_dump(mode="json")) if attempt.error else None,
                        attempt.issuer_bin, attempt.failed_at,
                    ),
                )
        except psycopg.errors.UniqueViolation as exc:
            raise DuplicateRecordError(f"payment_id already exists: {attempt.payment_id}") from exc

    def get_payment_attempt(self, payment_id: str) -> PaymentAttempt | None:
        with self.connection() as connection:
            row = connection.execute(
                "SELECT * FROM payment_attempts WHERE payment_id = %s", (payment_id,)
            ).fetchone()
        if not row:
            return None
        return PaymentAttempt(
            payment_id=row["payment_id"], order_id=row["order_id"], amount=row["amount"],
            method=PaymentMethod(row["method"]), status=PaymentStatus(row["status"]),
            captured=row["captured"], created_at=row["created_at"],
            error=PaymentError(**row["error"]) if row["error"] else None,
            issuer_bin=row["issuer_bin"], failed_at=row["failed_at"],
        )

    def count_payment_attempts_for_order(
        self,
        connection: psycopg.Connection[Any],
        order_id: str,
    ) -> int:
        row = connection.execute(
            "SELECT count(*) AS count FROM payment_attempts WHERE order_id = %s",
            (order_id,),
        ).fetchone()
        return int(row["count"]) if row else 0

    def record_webhook_event(
        self,
        event_id: str,
        event_type: str,
        payload: dict[str, Any],
        occurred_at: datetime | None = None,
    ) -> None:
        with self.transaction() as connection:
            if not self._record_webhook_event(connection, event_id, event_type, payload, occurred_at):
                raise DuplicateRecordError(f"event_id already exists: {event_id}")

    def _record_webhook_event(
        self,
        connection: psycopg.Connection[Any],
        event_id: str,
        event_type: str,
        payload: dict[str, Any],
        occurred_at: datetime | None = None,
    ) -> bool:
        row = connection.execute(
            """
            INSERT INTO webhook_events (event_id, event_type, payload, occurred_at)
            VALUES (%s, %s, %s::jsonb, %s)
            ON CONFLICT (event_id) DO NOTHING
            RETURNING event_id
            """,
            (event_id, event_type, json.dumps(payload), occurred_at),
        ).fetchone()
        return row is not None

    def get_recovery_case(self, case_id: str) -> dict[str, Any] | None:
        with self.connection() as connection:
            row = connection.execute(
                "SELECT * FROM recovery_cases WHERE case_id = %s",
                (case_id,),
            ).fetchone()
        return dict(row) if row else None

    def get_recovery_diagnosis(self, case_id: str) -> Diagnosis | None:
        with self.connection() as connection:
            row = connection.execute(
                """
                SELECT category, confidence, reason
                FROM diagnoses
                WHERE case_id = %s
                ORDER BY diagnosis_id DESC
                LIMIT 1
                """,
                (case_id,),
            ).fetchone()
        if not row:
            return None
        return Diagnosis(
            category=DiagnosisCategory(row["category"]),
            confidence=float(row["confidence"]),
            reason=row["reason"],
        )

    def get_recovery_decision(self, case_id: str) -> PolicyDecision | None:
        with self.connection() as connection:
            row = connection.execute(
                """
                SELECT decision, reason
                FROM decisions
                WHERE case_id = %s
                ORDER BY decision_id DESC
                LIMIT 1
                """,
                (case_id,),
            ).fetchone()
        if not row:
            return None
        return PolicyDecision(
            decision=PolicyDecisionType(row["decision"]),
            reason=row["reason"],
        )

    def get_recovery_execution(self, case_id: str) -> dict[str, Any] | None:
        with self.connection() as connection:
            row = connection.execute(
                """
                SELECT executed, action, status, reason, timestamp, idempotency_key
                FROM executions
                WHERE case_id = %s
                ORDER BY execution_id DESC
                LIMIT 1
                """,
                (case_id,),
            ).fetchone()
        return dict(row) if row else None

    def _save_recovery_case(
        self,
        connection: psycopg.Connection[Any],
        case_id: str,
        payment_id: str,
        order_id: str,
        status: str,
    ) -> None:
        connection.execute(
            """
            INSERT INTO recovery_cases (case_id, payment_id, order_id, status)
            VALUES (%s, %s, %s, %s)
            ON CONFLICT (case_id) DO UPDATE SET status = EXCLUDED.status, updated_at = now()
            """,
            (case_id, payment_id, order_id, status),
        )

    def save_recovery_case(self, case_id: str, payment_id: str, order_id: str, status: str) -> None:
        with self.transaction() as connection:
            self._save_recovery_case(connection, case_id, payment_id, order_id, status)

    def save_diagnosis(self, case_id: str, diagnosis: Diagnosis) -> None:
        with self.transaction() as connection:
            connection.execute(
                "INSERT INTO diagnoses (case_id, category, confidence, reason) VALUES (%s, %s, %s, %s)",
                (case_id, diagnosis.category.value, diagnosis.confidence, diagnosis.reason),
            )

    def save_decision(self, case_id: str, decision: PolicyDecision) -> None:
        with self.transaction() as connection:
            connection.execute(
                "INSERT INTO decisions (case_id, decision, reason) VALUES (%s, %s, %s)",
                (case_id, decision.decision.value, decision.reason),
            )

    def save_execution(self, case_id: str, execution: ExecutionResult, idempotency_key: str) -> None:
        try:
            with self.transaction() as connection:
                connection.execute(
                    """
                    INSERT INTO executions
                        (case_id, idempotency_key, executed, action, status, reason, timestamp)
                    VALUES (%s, %s, %s, %s, %s, %s, %s)
                    """,
                    (case_id, idempotency_key, execution.executed, execution.action,
                     execution.status, execution.reason, execution.audit.timestamp),
                )
        except psycopg.errors.UniqueViolation as exc:
            raise DuplicateRecordError(f"idempotency_key already exists: {idempotency_key}") from exc

    def save_audit_event(self, case_id: str, audit: AuditRecord) -> None:
        with self.transaction() as connection:
            connection.execute(
                """
                INSERT INTO audit_events
                    (case_id, payment_id, order_id, policy_decision, action, status, reason, timestamp)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
                """,
                (case_id, audit.payment_id, audit.order_id, audit.policy_decision.value,
                 audit.action, audit.status, audit.reason, audit.timestamp),
            )

    def save_recovery_processing(
        self,
        case_id: str,
        diagnosis: Diagnosis,
        decision: PolicyDecision,
        execution: ExecutionResult,
        idempotency_key: str,
    ) -> bool:
        """Atomically persist one case's diagnosis, decision, execution, and audit.

        A recovery case has one durable execution record for processing purposes.
        The unique idempotency key also protects concurrent processors racing on
        the same case. Returning false means another processing attempt already
        persisted the result.
        """
        try:
            with self.transaction() as connection:
                existing = connection.execute(
                    """
                    SELECT execution_id
                    FROM executions
                    WHERE case_id = %s OR idempotency_key = %s
                    LIMIT 1
                    """,
                    (case_id, idempotency_key),
                ).fetchone()
                if existing:
                    return False

                connection.execute(
                    "INSERT INTO diagnoses (case_id, category, confidence, reason) VALUES (%s, %s, %s, %s)",
                    (case_id, diagnosis.category.value, diagnosis.confidence, diagnosis.reason),
                )
                connection.execute(
                    "INSERT INTO decisions (case_id, decision, reason) VALUES (%s, %s, %s)",
                    (case_id, decision.decision.value, decision.reason),
                )
                connection.execute(
                    """
                    INSERT INTO executions
                        (case_id, idempotency_key, executed, action, status, reason, timestamp)
                    VALUES (%s, %s, %s, %s, %s, %s, %s)
                    """,
                    (
                        case_id,
                        idempotency_key,
                        execution.executed,
                        execution.action,
                        execution.status,
                        execution.reason,
                        execution.audit.timestamp,
                    ),
                )
                connection.execute(
                    """
                    INSERT INTO audit_events
                        (case_id, payment_id, order_id, policy_decision, action, status, reason, timestamp)
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
                    """,
                    (
                        case_id,
                        execution.audit.payment_id,
                        execution.audit.order_id,
                        execution.audit.policy_decision.value,
                        execution.audit.action,
                        execution.audit.status,
                        execution.audit.reason,
                        execution.audit.timestamp,
                    ),
                )
        except psycopg.errors.UniqueViolation:
            # A concurrent processor may have inserted the same idempotency key
            # after the pre-insert check. Its durable result is authoritative.
            return False
        return True

    def save_payment_outcome(
        self,
        payment_id: str,
        status: str,
        amount_recovered: int,
        observed_at: datetime | None = None,
    ) -> None:
        with self.transaction() as connection:
            connection.execute(
                """
                INSERT INTO payment_outcomes (payment_id, status, amount_recovered, observed_at)
                VALUES (%s, %s, %s, %s)
                ON CONFLICT (payment_id) DO UPDATE SET
                    status = EXCLUDED.status,
                    amount_recovered = EXCLUDED.amount_recovered,
                    observed_at = EXCLUDED.observed_at
                """,
                (payment_id, status, amount_recovered, observed_at or datetime.now(timezone.utc)),
            )
