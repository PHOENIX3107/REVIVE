"""Poll Razorpay for unresolved recovery cases.

This worker is deliberately a thin operational layer around
``RazorpayPaymentReconciler``. It discovers work from durable PostgreSQL state,
claims one case with a PostgreSQL advisory lock, and lets the existing
reconciler perform all provider validation and payment-outcome writes.
"""

from collections.abc import Iterator
from contextlib import contextmanager
from dataclasses import dataclass
import logging
import os
import signal
import threading
from typing import Any

import psycopg

from backend.db import Database
from backend.integrations.razorpay.client import RazorpayAPIError
from backend.recovery.reconciliation import RazorpayPaymentReconciler


LOGGER = logging.getLogger(__name__)
_ADVISORY_LOCK_NAMESPACE = "revive:payment-reconciliation"


@dataclass(frozen=True)
class PaymentReconciliationWorkerConfig:
    """Runtime settings for the polling worker."""

    polling_interval_seconds: float = 60.0
    batch_size: int = 50
    retry_attempts: int = 3
    retry_backoff_seconds: float = 1.0
    retry_max_backoff_seconds: float = 30.0

    @classmethod
    def from_env(cls) -> "PaymentReconciliationWorkerConfig":
        return cls(
            polling_interval_seconds=_positive_float(
                "REVIVE_RECONCILIATION_POLL_INTERVAL_SECONDS", 60.0
            ),
            batch_size=_positive_int("REVIVE_RECONCILIATION_BATCH_SIZE", 50),
            retry_attempts=_positive_int("REVIVE_RECONCILIATION_RETRY_ATTEMPTS", 3),
            retry_backoff_seconds=_non_negative_float(
                "REVIVE_RECONCILIATION_RETRY_BACKOFF_SECONDS", 1.0
            ),
            retry_max_backoff_seconds=_non_negative_float(
                "REVIVE_RECONCILIATION_RETRY_MAX_BACKOFF_SECONDS", 30.0
            ),
        )


@dataclass(frozen=True)
class ReconciliationRunResult:
    """Counters for one polling pass."""

    discovered: int = 0
    attempted: int = 0
    verified: int = 0
    failed: int = 0
    skipped: int = 0


class _ShutdownRequested(Exception):
    """Stop the current retry sequence without treating shutdown as a failure."""


def _positive_int(name: str, default: int) -> int:
    raw = os.getenv(name)
    if raw is None:
        return default
    value = int(raw)
    if value <= 0:
        raise ValueError(f"{name} must be greater than zero")
    return value


def _positive_float(name: str, default: float) -> float:
    raw = os.getenv(name)
    if raw is None:
        return default
    value = float(raw)
    if value <= 0:
        raise ValueError(f"{name} must be greater than zero")
    return value


def _non_negative_float(name: str, default: float) -> float:
    raw = os.getenv(name)
    if raw is None:
        return default
    value = float(raw)
    if value < 0:
        raise ValueError(f"{name} must not be negative")
    return value


@contextmanager
def _case_advisory_lock(database: Database, case_id: str) -> Iterator[bool]:
    """Hold a session advisory lock while one case is being verified.

    The verification call opens its own database connection, so a row lock
    would be released before the provider request completed. A session-level
    advisory lock gives us a durable PostgreSQL-backed claim while allowing the
    existing reconciler to retain its own transaction boundaries.
    """
    lock_key = f"{_ADVISORY_LOCK_NAMESPACE}:{case_id}"
    with database.connection() as connection:
        row = connection.execute(
            "SELECT pg_try_advisory_lock(hashtextextended(%s, 0)) AS locked",
            (lock_key,),
        ).fetchone()
        locked = bool(row and row["locked"])
        if not locked:
            yield False
            return
        try:
            yield True
        finally:
            connection.execute(
                "SELECT pg_advisory_unlock(hashtextextended(%s, 0))",
                (lock_key,),
            )


class PaymentReconciliationWorker:
    """Continuously verify unresolved recovery cases against Razorpay."""

    def __init__(
        self,
        database: Database,
        reconciler: RazorpayPaymentReconciler,
        config: PaymentReconciliationWorkerConfig | None = None,
    ) -> None:
        self.database = database
        self.reconciler = reconciler
        self.config = config or PaymentReconciliationWorkerConfig.from_env()
        self._stop_event = threading.Event()

    @property
    def stop_event(self) -> threading.Event:
        return self._stop_event

    def request_shutdown(self) -> None:
        """Request a graceful stop after the current safe operation."""
        self._stop_event.set()

    stop = request_shutdown

    def run_once(self) -> ReconciliationRunResult:
        """Process one bounded batch and return its operational counters."""
        try:
            case_ids = self.database.get_reconciliation_candidates(self.config.batch_size)
        except Exception:
            LOGGER.exception("Unable to discover payment reconciliation candidates")
            return ReconciliationRunResult(failed=1)

        result = ReconciliationRunResult(discovered=len(case_ids))

        for case_id in case_ids:
            if self._stop_event.is_set():
                break

            try:
                with _case_advisory_lock(self.database, case_id) as claimed:
                    if not claimed:
                        result = _replace_result(result, skipped=result.skipped + 1)
                        continue
                    if not self.database.case_needs_reconciliation(case_id):
                        result = _replace_result(result, skipped=result.skipped + 1)
                        continue

                    result = _replace_result(result, attempted=result.attempted + 1)
                    self._verify_with_retry(case_id)
            except _ShutdownRequested:
                break
            except Exception:
                # A bad provider response or unavailable dependency must not
                # prevent the remainder of the batch from running. This also
                # covers database failures during claim/state re-check.
                LOGGER.exception("Payment reconciliation failed for case %s", case_id)
                result = _replace_result(result, failed=result.failed + 1)
            else:
                result = _replace_result(result, verified=result.verified + 1)

        return result

    def run_forever(self, *, install_signal_handlers: bool = True) -> None:
        """Poll until SIGTERM/SIGINT or ``request_shutdown`` is received."""
        previous_handlers: dict[int, Any] = {}
        can_install_handlers = (
            install_signal_handlers
            and threading.current_thread() is threading.main_thread()
        )
        if can_install_handlers:
            for signum in (signal.SIGTERM, signal.SIGINT):
                previous_handlers[signum] = signal.getsignal(signum)
                signal.signal(signum, self._handle_signal)

        try:
            while not self._stop_event.is_set():
                try:
                    self.run_once()
                except Exception:
                    # Keep the daemon alive even if an unexpected operational
                    # error escapes the bounded per-case handling above.
                    LOGGER.exception("Unexpected payment reconciliation worker failure")
                if not self._stop_event.is_set():
                    self._stop_event.wait(self.config.polling_interval_seconds)
        finally:
            for signum, handler in previous_handlers.items():
                signal.signal(signum, handler)

    def _handle_signal(self, signum: int, _frame: Any) -> None:
        LOGGER.info("Received signal %s; stopping payment reconciliation worker", signum)
        self.request_shutdown()

    def _verify_with_retry(self, case_id: str) -> Any:
        for attempt in range(1, self.config.retry_attempts + 1):
            try:
                return self.reconciler.verify_case(case_id)
            except Exception as exc:
                retryable = _is_retryable(exc)
                if not retryable or attempt >= self.config.retry_attempts:
                    raise
                delay = min(
                    self.config.retry_backoff_seconds * (2 ** (attempt - 1)),
                    self.config.retry_max_backoff_seconds,
                )
                LOGGER.warning(
                    "Transient reconciliation failure for case %s; retry %s/%s in %.2fs",
                    case_id,
                    attempt,
                    self.config.retry_attempts - 1,
                    delay,
                )
                if self._stop_event.wait(delay):
                    raise _ShutdownRequested from exc
        raise AssertionError("unreachable")


def _replace_result(result: ReconciliationRunResult, **changes: int) -> ReconciliationRunResult:
    values = {
        "discovered": result.discovered,
        "attempted": result.attempted,
        "verified": result.verified,
        "failed": result.failed,
        "skipped": result.skipped,
    }
    values.update(changes)
    return ReconciliationRunResult(**values)


def _is_retryable(exc: Exception) -> bool:
    if isinstance(exc, RazorpayAPIError):
        # A missing status means the client did not receive an HTTP response
        # (for example, a transport or invalid-response failure). HTTP 429 and
        # 5xx responses are transient; ordinary 4xx responses are permanent
        # for this verification attempt and should not be retried.
        status_code = exc.status_code
        return (
            status_code is None
            or status_code == 429
            or 500 <= status_code < 600
        )

    return isinstance(
        exc,
        (
            psycopg.OperationalError,
            psycopg.InterfaceError,
            TimeoutError,
            ConnectionError,
            OSError,
        ),
    )


def build_worker_from_env() -> PaymentReconciliationWorker:
    """Create and initialize the production-oriented polling worker."""
    database = Database()
    database.initialize()
    return PaymentReconciliationWorker(
        database,
        RazorpayPaymentReconciler(database),
        PaymentReconciliationWorkerConfig.from_env(),
    )


def main() -> None:
    logging.basicConfig(
        level=os.getenv("REVIVE_LOG_LEVEL", "INFO").upper(),
        format="%(asctime)s %(levelname)s %(name)s %(message)s",
    )
    worker = build_worker_from_env()
    worker.run_forever()


if __name__ == "__main__":
    main()
