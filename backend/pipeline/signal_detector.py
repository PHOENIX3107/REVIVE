"""Deterministic population-level failure signal detection."""

from backend.cache import FAILURE_WINDOW_SECONDS, RedisFailureCache
from backend.schemas import PaymentAttempt, PaymentStatus, SignalContext


CLUSTER_THRESHOLD = 5


class SignalDetector:
    """Identify matching failed payment attempts within a short time window."""

    def __init__(
        self,
        failure_cache: RedisFailureCache,
        window_seconds: int = FAILURE_WINDOW_SECONDS,
        cluster_threshold: int = CLUSTER_THRESHOLD,
    ) -> None:
        self.failure_cache = failure_cache
        self.window_seconds = window_seconds
        self.cluster_threshold = cluster_threshold

    def detect(self, attempt: PaymentAttempt) -> SignalContext:
        """Return a signal context without diagnosing or acting on the payment."""
        if attempt.status is not PaymentStatus.failed:
            return SignalContext(
                is_cluster_candidate=False,
                matching_failure_count=0,
                window_seconds=self.window_seconds,
            )

        if attempt.failed_at is None or attempt.error is None:
            return SignalContext(
                is_cluster_candidate=False,
                matching_failure_count=0,
                window_seconds=self.window_seconds,
            )

        count = self.failure_cache.record_failure(
            attempt.issuer_bin,
            attempt.error.code,
            occurred_at=attempt.failed_at,
            payment_id=attempt.payment_id,
        )
        return SignalContext(
            is_cluster_candidate=count >= self.cluster_threshold,
            matching_failure_count=count,
            window_seconds=self.window_seconds,
        )


def detect_signal(attempt: PaymentAttempt, failure_cache: RedisFailureCache) -> SignalContext:
    """Convenience wrapper for callers that do not need a detector instance."""
    return SignalDetector(failure_cache).detect(attempt)
