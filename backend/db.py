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

    def get_reconciliation_candidates(self, limit: int) -> list[str]:
        """Return durable recovery cases that still need provider verification.

        The recovery case status and provider outcome are the durable queue
        boundary. No in-memory queue or worker-specific columns are required.
        Cases are ordered oldest-first so a repeatedly failing case cannot
        permanently starve newer unresolved cases.
        """
        if limit <= 0:
            return []
        with self.connection() as connection:
            rows = connection.execute(
                """
                SELECT rc.case_id
                FROM recovery_cases AS rc
                JOIN payment_attempts AS p ON p.payment_id = rc.payment_id
                JOIN orders AS o ON o.order_id = rc.order_id
                WHERE rc.status = 'open'
                  AND NOT EXISTS (
                      SELECT 1
                      FROM payment_outcomes AS po
                      WHERE po.payment_id = rc.payment_id
                        AND po.status = 'recovered'
                  )
                ORDER BY rc.updated_at ASC, rc.created_at ASC, rc.case_id ASC
                LIMIT %s
                """,
                (limit,),
            ).fetchall()
        return [str(row["case_id"]) for row in rows]

    def case_needs_reconciliation(self, case_id: str) -> bool:
        """Re-check a candidate after a worker acquires its case lock."""
        with self.connection() as connection:
            row = connection.execute(
                """
                SELECT 1
                FROM recovery_cases AS rc
                JOIN payment_attempts AS p ON p.payment_id = rc.payment_id
                JOIN orders AS o ON o.order_id = rc.order_id
                WHERE rc.case_id = %s
                  AND rc.status = 'open'
                  AND NOT EXISTS (
                      SELECT 1
                      FROM payment_outcomes AS po
                      WHERE po.payment_id = rc.payment_id
                        AND po.status = 'recovered'
                  )
                """,
                (case_id,),
            ).fetchone()
        return row is not None

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

    def get_dashboard_metrics(self) -> dict[str, int]:
        """Aggregate dashboard metrics from the durable PostgreSQL state."""
        with self.connection() as connection:
            row = connection.execute(
                """
                SELECT
                    (SELECT count(*) FROM payment_attempts) AS payment_attempts,
                    (SELECT count(*) FROM payment_attempts WHERE status = 'failed') AS failed_payments,
                    (
                        SELECT COALESCE(sum(p.amount), 0)
                        FROM payment_attempts AS p
                        JOIN orders AS o ON o.order_id = p.order_id
                        WHERE p.status = 'failed' AND o.status <> 'paid'
                    ) AS revenue_at_risk,
                    (
                        SELECT COALESCE(sum(p.amount), 0)
                        FROM recovery_cases AS rc
                        JOIN payment_attempts AS p ON p.payment_id = rc.payment_id
                        JOIN orders AS o ON o.order_id = rc.order_id
                        JOIN LATERAL (
                            SELECT d.decision
                            FROM decisions AS d
                            WHERE d.case_id = rc.case_id
                            ORDER BY d.created_at DESC, d.decision_id DESC
                            LIMIT 1
                        ) AS latest_decision ON TRUE
                        WHERE latest_decision.decision = 'recover'
                          AND p.status = 'failed'
                          AND o.status <> 'paid'
                    ) AS policy_eligible_revenue,
                    (
                        SELECT COALESCE(sum(amount_recovered), 0)
                        FROM payment_outcomes
                        WHERE status = 'recovered'
                    ) AS recovered_revenue,
                    (SELECT count(*) FROM executions WHERE status = 'executed') AS recovery_actions,
                    (SELECT count(*) FROM executions WHERE status = 'blocked') AS blocked_actions,
                    (SELECT count(*) FROM executions WHERE status = 'duplicate') AS duplicate_actions_prevented,
                    (
                        SELECT count(*)
                        FROM executions AS e
                        JOIN recovery_cases AS rc ON rc.case_id = e.case_id
                        JOIN orders AS o ON o.order_id = rc.order_id
                        LEFT JOIN LATERAL (
                            SELECT d.category
                            FROM diagnoses AS d
                            WHERE d.case_id = rc.case_id
                            ORDER BY d.created_at DESC, d.diagnosis_id DESC
                            LIMIT 1
                        ) AS latest_diagnosis ON TRUE
                        WHERE e.status = 'executed'
                          AND (
                              o.status = 'paid'
                              OR o.attempts >= 3
                              OR latest_diagnosis.category = 'systemic_issue'
                          )
                    ) AS unsafe_actions,
                    (
                        SELECT count(DISTINCT rc.case_id)
                        FROM recovery_cases AS rc
                        JOIN LATERAL (
                            SELECT d.category
                            FROM diagnoses AS d
                            WHERE d.case_id = rc.case_id
                            ORDER BY d.created_at DESC, d.diagnosis_id DESC
                            LIMIT 1
                        ) AS latest_diagnosis ON TRUE
                        WHERE latest_diagnosis.category = 'systemic_issue'
                    ) AS systemic_attempts,
                    (
                        SELECT count(DISTINCT rc.case_id)
                        FROM recovery_cases AS rc
                        JOIN LATERAL (
                            SELECT d.category
                            FROM diagnoses AS d
                            WHERE d.case_id = rc.case_id
                            ORDER BY d.created_at DESC, d.diagnosis_id DESC
                            LIMIT 1
                        ) AS latest_diagnosis ON TRUE
                        WHERE latest_diagnosis.category = 'customer_issue'
                    ) AS customer_attempts
                """
            ).fetchone()
        if row is None:
            return {
                "payment_attempts": 0,
                "failed_payments": 0,
                "revenue_at_risk": 0,
                "policy_eligible_revenue": 0,
                "recovered_revenue": 0,
                "recovery_actions": 0,
                "blocked_actions": 0,
                "duplicate_actions_prevented": 0,
                "unsafe_actions": 0,
                "systemic_attempts": 0,
                "customer_attempts": 0,
            }
        return {key: int(value or 0) for key, value in dict(row).items()}

    def get_dashboard_recoveries(self, limit: int | None = None) -> list[dict[str, Any]]:
        """Return recovery cases with their latest durable pipeline records."""
        query = """
            SELECT
                rc.case_id,
                rc.payment_id,
                rc.order_id,
                p.amount,
                p.status AS payment_status,
                o.status AS order_status,
                rc.status AS case_status,
                latest_diagnosis.category AS diagnosis_category,
                latest_diagnosis.confidence AS diagnosis_confidence,
                latest_diagnosis.reason AS diagnosis_reason,
                latest_decision.decision,
                latest_decision.reason AS decision_reason,
                latest_execution.status AS execution_status,
                latest_execution.action AS execution_action,
                latest_execution.reason AS execution_reason,
                latest_execution.timestamp AS execution_timestamp,
                po.status AS payment_outcome_status,
                po.amount_recovered AS payment_outcome_amount,
                po.observed_at AS payment_outcome_observed_at
            FROM recovery_cases AS rc
            JOIN payment_attempts AS p ON p.payment_id = rc.payment_id
            JOIN orders AS o ON o.order_id = rc.order_id
            LEFT JOIN LATERAL (
                SELECT d.category, d.confidence, d.reason
                FROM diagnoses AS d
                WHERE d.case_id = rc.case_id
                ORDER BY d.created_at DESC, d.diagnosis_id DESC
                LIMIT 1
            ) AS latest_diagnosis ON TRUE
            LEFT JOIN LATERAL (
                SELECT d.decision, d.reason
                FROM decisions AS d
                WHERE d.case_id = rc.case_id
                ORDER BY d.created_at DESC, d.decision_id DESC
                LIMIT 1
            ) AS latest_decision ON TRUE
            LEFT JOIN LATERAL (
                SELECT e.status, e.action, e.reason, e.timestamp
                FROM executions AS e
                WHERE e.case_id = rc.case_id
                ORDER BY e.timestamp DESC, e.execution_id DESC
                LIMIT 1
            ) AS latest_execution ON TRUE
            LEFT JOIN payment_outcomes AS po ON po.payment_id = rc.payment_id
            ORDER BY rc.updated_at DESC, rc.created_at DESC, rc.case_id DESC
        """
        parameters: tuple[int, ...] = ()
        if limit is not None:
            query += " LIMIT %s"
            parameters = (max(0, limit),)
        with self.connection() as connection:
            rows = connection.execute(query, parameters).fetchall()
        return [dict(row) for row in rows]

    def get_dashboard_signals(self) -> list[dict[str, Any]]:
        """Group persisted failed payments by issuer BIN and error code."""
        with self.connection() as connection:
            rows = connection.execute(
                """
                SELECT
                    issuer_bin,
                    error ->> 'code' AS error_code,
                    count(*) AS failure_count,
                    COALESCE(sum(amount), 0) AS total_amount,
                    max(failed_at) AS latest_failed_at
                FROM payment_attempts
                WHERE status = 'failed'
                GROUP BY issuer_bin, error ->> 'code'
                ORDER BY latest_failed_at DESC NULLS LAST, issuer_bin, error_code
                """
            ).fetchall()
        return [dict(row) for row in rows]

    def get_dashboard_decisions(self) -> list[dict[str, Any]]:
        """Return persisted decisions with their case and latest diagnosis."""
        with self.connection() as connection:
            rows = connection.execute(
                """
                SELECT
                    d.decision_id,
                    rc.case_id,
                    rc.payment_id,
                    rc.order_id,
                    p.amount,
                    p.status AS payment_status,
                    o.status AS order_status,
                    latest_diagnosis.category AS diagnosis_category,
                    latest_diagnosis.confidence AS diagnosis_confidence,
                    latest_diagnosis.reason AS diagnosis_reason,
                    d.decision,
                    d.reason AS decision_reason,
                    d.created_at AS decision_created_at
                FROM decisions AS d
                JOIN recovery_cases AS rc ON rc.case_id = d.case_id
                JOIN payment_attempts AS p ON p.payment_id = rc.payment_id
                JOIN orders AS o ON o.order_id = rc.order_id
                LEFT JOIN LATERAL (
                    SELECT diagnosis.category, diagnosis.confidence, diagnosis.reason
                    FROM diagnoses AS diagnosis
                    WHERE diagnosis.case_id = rc.case_id
                    ORDER BY diagnosis.created_at DESC, diagnosis.diagnosis_id DESC
                    LIMIT 1
                ) AS latest_diagnosis ON TRUE
                ORDER BY d.created_at DESC, d.decision_id DESC
                """
            ).fetchall()
        return [dict(row) for row in rows]

    def get_dashboard_audit(self) -> list[dict[str, Any]]:
        """Return persisted audit events with associated durable resources."""
        with self.connection() as connection:
            rows = connection.execute(
                """
                SELECT
                    a.audit_id,
                    a.case_id,
                    a.payment_id,
                    a.order_id,
                    p.amount,
                    p.status AS payment_status,
                    o.status AS order_status,
                    a.policy_decision,
                    a.action,
                    a.status,
                    a.reason,
                    a.timestamp
                FROM audit_events AS a
                JOIN recovery_cases AS rc ON rc.case_id = a.case_id
                JOIN payment_attempts AS p ON p.payment_id = a.payment_id
                JOIN orders AS o ON o.order_id = a.order_id
                ORDER BY a.timestamp DESC, a.audit_id DESC
                """
            ).fetchall()
        return [dict(row) for row in rows]

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
            self._save_payment_outcome(
                connection,
                payment_id,
                status,
                amount_recovered,
                observed_at,
            )

    def _save_payment_outcome(
        self,
        connection: psycopg.Connection[Any],
        payment_id: str,
        status: str,
        amount_recovered: int,
        observed_at: datetime | None = None,
        *,
        provider_confirmed: bool = False,
    ) -> None:
        if status == "recovered" and not provider_confirmed:
            raise ValueError("Recovered payment outcomes require provider confirmation.")
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

    def _save_provider_recovered_outcome(
        self,
        connection: psycopg.Connection[Any],
        payment_id: str,
        amount_recovered: int,
        observed_at: datetime | None = None,
    ) -> None:
        """Record recovery only after the local payment is provider-confirmed."""
        row = connection.execute(
            """
            SELECT p.status AS payment_status, p.amount AS payment_amount,
                   o.status AS order_status, o.amount AS order_amount
            FROM payment_attempts AS p
            JOIN orders AS o ON o.order_id = p.order_id
            WHERE p.payment_id = %s
            FOR UPDATE OF p, o
            """,
            (payment_id,),
        ).fetchone()
        if (
            not row
            or row["payment_status"] != PaymentStatus.captured.value
            or row["order_status"] != "paid"
            or row["payment_amount"] != amount_recovered
            or row["order_amount"] != amount_recovered
        ):
            raise ValueError("Recovered payment outcome requires a captured payment and paid order.")
        self._save_payment_outcome(
            connection,
            payment_id,
            "recovered",
            amount_recovered,
            observed_at,
            provider_confirmed=True,
        )

    def reconcile_provider_state(
        self,
        case_id: str,
        payment: PaymentAttempt,
        order: Order,
        *,
        provider_payment_status: str,
        provider_order_status: str,
        recovery_confirmed: bool,
        recovered_amount: int,
        observed_at: datetime,
        audit_reason: str,
    ) -> dict[str, Any]:
        """Atomically apply one provider verification to a recovery case.

        Payment and order writes use the existing monotonic upserts. The case
        row is locked to make the deterministic reconciliation audit record
        idempotent when verification requests race.
        """
        with self.transaction() as connection:
            case = connection.execute(
                """
                SELECT case_id, payment_id, order_id, status
                FROM recovery_cases
                WHERE case_id = %s
                FOR UPDATE
                """,
                (case_id,),
            ).fetchone()
            if not case:
                raise ValueError(f"Recovery case not found: {case_id}")
            if case["payment_id"] != payment.payment_id or case["order_id"] != order.order_id:
                raise ValueError("Provider reconciliation identities do not match the recovery case.")

            current_payment = connection.execute(
                "SELECT status FROM payment_attempts WHERE payment_id = %s FOR UPDATE",
                (payment.payment_id,),
            ).fetchone()
            current_order = connection.execute(
                "SELECT status FROM orders WHERE order_id = %s FOR UPDATE",
                (order.order_id,),
            ).fetchone()
            if not current_payment or not current_order:
                raise ValueError("Provider reconciliation state is missing its payment or order.")

            payment_status_before = current_payment["status"]
            order_status_before = current_order["status"]
            case_status_before = case["status"]

            self._save_order(connection, order)
            self._save_payment_attempt(connection, payment)

            outcome_changed = False
            if recovery_confirmed:
                outcome = connection.execute(
                    """
                    SELECT status, amount_recovered
                    FROM payment_outcomes
                    WHERE payment_id = %s
                    FOR UPDATE
                    """,
                    (payment.payment_id,),
                ).fetchone()
                outcome_changed = (
                    outcome is None
                    or outcome["status"] != "recovered"
                    or outcome["amount_recovered"] != recovered_amount
                )
                self._save_provider_recovered_outcome(
                    connection,
                    payment.payment_id,
                    recovered_amount,
                    observed_at,
                )
                if case_status_before != "recovered":
                    connection.execute(
                        """
                        UPDATE recovery_cases
                        SET status = 'recovered', updated_at = now()
                        WHERE case_id = %s
                        """,
                        (case_id,),
                    )

            audit_status = "confirmed" if recovery_confirmed else "observed"
            audit_action = "provider_reconciliation"
            existing_audit = connection.execute(
                """
                SELECT audit_id
                FROM audit_events
                WHERE case_id = %s AND action = %s AND status = %s AND reason = %s
                LIMIT 1
                """,
                (case_id, audit_action, audit_status, audit_reason),
            ).fetchone()
            reconciliation_recorded = existing_audit is None
            if reconciliation_recorded:
                connection.execute(
                    """
                    INSERT INTO audit_events
                        (case_id, payment_id, order_id, policy_decision, action, status, reason, timestamp)
                    VALUES (%s, %s, %s, 'reconciliation', %s, %s, %s, %s)
                    """,
                    (
                        case_id,
                        payment.payment_id,
                        order.order_id,
                        audit_action,
                        audit_status,
                        audit_reason,
                        observed_at,
                    ),
                )

            payment_after = connection.execute(
                "SELECT status FROM payment_attempts WHERE payment_id = %s",
                (payment.payment_id,),
            ).fetchone()
            order_after = connection.execute(
                "SELECT status FROM orders WHERE order_id = %s",
                (order.order_id,),
            ).fetchone()
            case_after = connection.execute(
                "SELECT status FROM recovery_cases WHERE case_id = %s",
                (case_id,),
            ).fetchone()
            if not payment_after or not order_after or not case_after:
                raise ValueError("Provider reconciliation did not leave complete local state.")

            return {
                "case_id": case_id,
                "payment_id": payment.payment_id,
                "order_id": order.order_id,
                "provider_payment_status": provider_payment_status,
                "provider_order_status": provider_order_status,
                "revive_payment_status": payment_after["status"],
                "order_status": order_after["status"],
                "state_changed": (
                    payment_status_before != payment_after["status"]
                    or order_status_before != order_after["status"]
                    or case_status_before != case_after["status"]
                    or outcome_changed
                ),
                "recovery_confirmed": recovery_confirmed,
                "recovered_amount": recovered_amount if recovery_confirmed else 0,
                "reconciliation_recorded": reconciliation_recorded,
            }
