"""Redis-backed storage for short-lived payment failure signals."""

from datetime import datetime, timezone
from typing import Any


FAILURE_WINDOW_SECONDS = 600
IDEMPOTENCY_TTL_SECONDS = 24 * 60 * 60


class RedisFailureCache:
    """Store unique payment failures in Redis sorted sets.

    A Redis client is injected so production can use Redis while tests can use
    a small fake without adding a Redis dependency to this project.
    """

    def __init__(
        self,
        redis_client: Any,
        window_seconds: int = FAILURE_WINDOW_SECONDS,
        key_prefix: str = "failures",
    ) -> None:
        self.redis = redis_client
        self.window_seconds = window_seconds
        self.key_prefix = key_prefix

    def record_failure(
        self,
        issuer_bin: str,
        error_code: str,
        *,
        occurred_at: datetime | None = None,
        payment_id: str | None = None,
    ) -> int:
        """Record a failure and return the number still inside the window."""
        occurred_at = occurred_at or datetime.now(timezone.utc)
        score = occurred_at.timestamp()
        key = f"{self.key_prefix}:{issuer_bin}:{error_code}"
        # Payment IDs make repeated webhook/event delivery idempotent here.
        member = payment_id or f"event:{score:.6f}"

        self.redis.zadd(key, {member: score})
        cutoff = score - self.window_seconds
        self.redis.zremrangebyscore(key, "-inf", f"({cutoff}")
        count = int(self.redis.zcard(key))
        self.redis.expire(key, self.window_seconds)
        return count


class RedisIdempotencyCache:
    """Atomically claim recovery requests for the existing 24-hour window."""

    def __init__(self, redis_client: Any, key_prefix: str = "idempotency") -> None:
        self.redis = redis_client
        self.key_prefix = key_prefix

    def claim(self, idempotency_key: str) -> bool:
        """Return true only for the first claim of an idempotency key."""
        return bool(
            self.redis.set(
                f"{self.key_prefix}:{idempotency_key}",
                "processed",
                ex=IDEMPOTENCY_TTL_SECONDS,
                nx=True,
            )
        )
