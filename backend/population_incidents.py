"""Durable population-incident activation for observed payment failures.

The existing :class:`SignalDetector` answers an instantaneous question for a
single payment attempt.  This module adds the separate durable fact that a
cohort has crossed the population threshold.  It is intentionally fed by
payment-failure ingestion and never by recovery-case processing or synthetic
evaluation data.
"""

from datetime import datetime, timedelta, timezone
from enum import Enum
import uuid
from typing import Any

from pydantic import BaseModel

from backend.db import Database
from backend.schemas import PaymentAttempt, PaymentStatus, PopulationIncidentContext


POPULATION_WINDOW_SECONDS = 600
POPULATION_THRESHOLD = 5


class PopulationIncidentStatus(str, Enum):
    ACTIVE = "ACTIVE"
    EXPIRED = "EXPIRED"
    RESOLVED = "RESOLVED"

    # Lowercase aliases keep the Python API compatible with the repository's
    # existing enum naming convention while the persisted contract stays
    # explicit and uppercase.
    active = ACTIVE
    expired = EXPIRED
    resolved = RESOLVED


class PopulationObservationResult(BaseModel):
    payment_id: str
    cohort_key: str
    observed_at: datetime
    unique_observation: bool
    observed_count: int
    incident: PopulationIncidentContext


def _utc(value: datetime) -> datetime:
    return value if value.tzinfo else value.replace(tzinfo=timezone.utc)


def normalize_cohort_component(value: str | None) -> str:
    normalized = (value or "unknown").strip().lower()
    return normalized or "unknown"


def population_cohort_key(issuer_bin: str | None, error_code: str | None) -> str:
    return (
        "population:v1:"
        f"{normalize_cohort_component(issuer_bin)}:"
        f"{normalize_cohort_component(error_code)}"
    )


class PopulationIncidentWindow:
    """Redis rolling window used as the fast population observation index."""

    def __init__(
        self,
        redis_client: Any,
        *,
        window_seconds: int = POPULATION_WINDOW_SECONDS,
        key_prefix: str = "population:window",
    ) -> None:
        self.redis = redis_client
        self.window_seconds = window_seconds
        self.key_prefix = key_prefix

    def observe(
        self,
        *,
        cohort_key: str,
        payment_id: str,
        observed_at: datetime,
    ) -> tuple[bool, int]:
        """Record one observed payment and count the current observed window.

        Redis is deliberately only a fast index.  PostgreSQL remains the
        durable authority for the incident state and concurrency boundary.
        Provider timestamps are not used here; ``observed_at`` is established
        by the ingestion process.
        """
        observed_at = _utc(observed_at)
        score = observed_at.timestamp()
        key = f"{self.key_prefix}:{cohort_key}"
        added = bool(self.redis.zadd(key, {payment_id: score}, nx=True))
        cutoff = score - self.window_seconds
        self.redis.zremrangebyscore(key, "-inf", f"({cutoff}")
        count = int(self.redis.zcount(key, cutoff, score))
        self.redis.expire(key, self.window_seconds)
        return added, count


class PopulationIncidentActivator:
    """Activate at most one durable incident for each failure cohort."""

    def __init__(
        self,
        database: Database,
        redis_client: Any,
        *,
        threshold: int = POPULATION_THRESHOLD,
        window_seconds: int = POPULATION_WINDOW_SECONDS,
        clock: Any | None = None,
    ) -> None:
        self.database = database
        self.threshold = threshold
        self.window_seconds = window_seconds
        self.clock = clock or (lambda: datetime.now(timezone.utc))
        self.window = PopulationIncidentWindow(
            redis_client,
            window_seconds=window_seconds,
        )

    def observe_failure(
        self,
        payment: PaymentAttempt,
        *,
        observed_at: datetime | None = None,
        connection: Any | None = None,
    ) -> PopulationObservationResult | None:
        """Observe a failed payment and activate its cohort when eligible.

        ``connection`` lets webhook ingestion persist the incident in the same
        PostgreSQL transaction as the payment and order.  The standalone path
        is useful for other durable failure-ingestion adapters and tests.
        """
        if payment.status is not PaymentStatus.failed or payment.error is None:
            return None

        observed_at = _utc(observed_at or self.clock())
        cohort_key = population_cohort_key(payment.issuer_bin, payment.error.code)
        unique_observation, observed_count = self.window.observe(
            cohort_key=cohort_key,
            payment_id=payment.payment_id,
            observed_at=observed_at,
        )
        incident_id = f"population-incident-{uuid.uuid4().hex}"

        if connection is None:
            with self.database.transaction() as transaction:
                incident = self.database._activate_population_incident(
                    transaction,
                    cohort_key=cohort_key,
                    issuer_bin=normalize_cohort_component(payment.issuer_bin),
                    error_code=normalize_cohort_component(payment.error.code),
                    threshold=self.threshold,
                    window_seconds=self.window_seconds,
                    observed_count=observed_count,
                    trigger_payment_id=payment.payment_id,
                    observed_at=observed_at,
                    unique_observation=unique_observation,
                    incident_id=incident_id,
                )
        else:
            incident = self.database._activate_population_incident(
                connection,
                cohort_key=cohort_key,
                issuer_bin=normalize_cohort_component(payment.issuer_bin),
                error_code=normalize_cohort_component(payment.error.code),
                threshold=self.threshold,
                window_seconds=self.window_seconds,
                observed_count=observed_count,
                trigger_payment_id=payment.payment_id,
                observed_at=observed_at,
                unique_observation=unique_observation,
                incident_id=incident_id,
            )

        return PopulationObservationResult(
            payment_id=payment.payment_id,
            cohort_key=cohort_key,
            observed_at=observed_at,
            unique_observation=unique_observation,
            observed_count=observed_count,
            incident=self._context_from_row(incident, cohort_key=cohort_key),
        )

    def active_context(
        self,
        payment: PaymentAttempt,
        *,
        observed_at: datetime | None = None,
    ) -> PopulationIncidentContext:
        """Read the currently active incident for a failed payment's cohort."""
        if payment.status is not PaymentStatus.failed or payment.error is None:
            return PopulationIncidentContext()

        cohort_key = population_cohort_key(payment.issuer_bin, payment.error.code)
        row = self.database.get_active_population_incident(
            cohort_key,
            _utc(observed_at or self.clock()),
        )
        return self._context_from_row(row, cohort_key=cohort_key)

    @staticmethod
    def _context_from_row(
        row: dict[str, Any] | None,
        *,
        cohort_key: str,
    ) -> PopulationIncidentContext:
        if row is None:
            return PopulationIncidentContext(cohort_key=cohort_key)
        return PopulationIncidentContext(
            population_incident_active=row["status"] == PopulationIncidentStatus.ACTIVE.value,
            population_incident_id=row["incident_id"],
            cohort_key=row["cohort_key"],
            population_incident_activated_at=row["activated_at"],
            population_incident_expires_at=row["expires_at"],
        )
