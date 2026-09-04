"""Deterministic synthetic Razorpay-shaped payment data for evaluation."""

from datetime import datetime, timedelta, timezone
import random
from typing import Any

from backend.schemas import (
    Order,
    OrderStatus,
    PaymentAttempt,
    PaymentError,
    PaymentMethod,
    PaymentStatus,
)


_START = datetime(2026, 1, 1, tzinfo=timezone.utc)
_AMOUNTS = (9900, 19900, 49900, 99900, 249900)
_CUSTOMER_ERRORS = (
    ("insufficient_funds", "The card has insufficient funds.", "funds"),
    ("expired_card", "The card has expired.", "card"),
    ("cvv_mismatch", "The card security code did not match.", "card"),
)
_SYSTEMIC_ERRORS = (
    ("issuer_declined", "The issuer declined the payment.", "issuer"),
    ("network_error", "The payment network was unavailable.", "network"),
    ("gateway_timeout", "The payment gateway timed out.", "gateway"),
)


def _payment_error(code: str, description: str, reason: str) -> PaymentError:
    """Build the same compact synthetic error envelope for every failure."""
    return PaymentError(
        code=code,
        description=description,
        field="card",
        source="synthetic",
        step="payment_authentication",
        reason=reason,
        metadata={"environment": "synthetic", "provider": "razorpay-shaped"},
    )


def _cluster_sizes(num_attempts: int) -> list[int]:
    if num_attempts < 10:
        return []
    size = min(8, max(5, num_attempts // 12))
    return [size, size]


def generate_batch(
    num_attempts: int = 100,
    seed: int | None = None,
) -> dict[str, Any]:
    """Generate synthetic orders, payment attempts, and separate ground truth.

    The returned dictionary contains ``orders``, ``payment_attempts``, and
    ``ground_truth``. No external payment provider is contacted.
    """
    if num_attempts < 1:
        raise ValueError("num_attempts must be positive")

    rng = random.Random(seed)
    cluster_sizes = _cluster_sizes(num_attempts)
    clustered_count = sum(cluster_sizes)
    isolated_count = min(max(2, num_attempts // 10), num_attempts - clustered_count)
    failed_count = clustered_count + isolated_count

    attempts: list[PaymentAttempt] = []
    ground_truth_attempts: dict[str, dict[str, Any]] = {}
    clusters: dict[str, list[str]] = {}

    def add_attempt(
        index: int,
        occurred_at: datetime,
        status: PaymentStatus,
        issuer_bin: str,
        error: PaymentError | None = None,
        cluster_id: str | None = None,
    ) -> None:
        payment_id = f"pay_synthetic_{index:05d}"
        order_id = f"order_synthetic_{index:05d}"
        amount = rng.choice(_AMOUNTS)
        failed_at = occurred_at if status is PaymentStatus.failed else None
        captured = status is PaymentStatus.captured
        attempts.append(
            PaymentAttempt(
                payment_id=payment_id,
                order_id=order_id,
                amount=amount,
                method=PaymentMethod.card,
                status=status,
                captured=captured,
                created_at=occurred_at,
                error=error,
                issuer_bin=issuer_bin,
                failed_at=failed_at,
            )
        )
        if status is PaymentStatus.failed:
            population = "systemic_cluster" if cluster_id else "isolated"
            ground_truth_attempts[payment_id] = {
                "is_clustered": cluster_id is not None,
                "population": population,
                "cluster_id": cluster_id,
            }
            if cluster_id:
                clusters.setdefault(cluster_id, []).append(payment_id)

    index = 0
    for cluster_number, cluster_size in enumerate(cluster_sizes, start=1):
        cluster_id = f"synthetic_cluster_{cluster_number}"
        code, description, reason = _SYSTEMIC_ERRORS[cluster_number - 1]
        issuer_bin = f"{453000 + cluster_number:06d}"
        cluster_start = _START + timedelta(days=cluster_number)
        for offset in sorted(rng.randint(0, 600) for _ in range(cluster_size)):
            add_attempt(
                index,
                cluster_start + timedelta(seconds=offset),
                PaymentStatus.failed,
                issuer_bin,
                _payment_error(code, description, reason),
                cluster_id,
            )
            index += 1

    # Isolated failures are deliberately separated by more than the detector window.
    for isolated_number in range(isolated_count):
        code, description, reason = _CUSTOMER_ERRORS[isolated_number % len(_CUSTOMER_ERRORS)]
        occurred_at = _START + timedelta(days=4, hours=isolated_number * 2)
        add_attempt(
            index,
            occurred_at,
            PaymentStatus.failed,
            f"{521000 + isolated_number:06d}",
            _payment_error(code, description, reason),
        )
        index += 1

    for success_number in range(num_attempts - failed_count):
        occurred_at = _START + timedelta(days=6, minutes=success_number)
        status = PaymentStatus.captured if success_number % 5 else PaymentStatus.authorized
        add_attempt(index, occurred_at, status, f"{411111 + success_number:06d}"[-6:])
        index += 1

    attempts.sort(key=lambda attempt: attempt.failed_at or attempt.created_at)
    orders = [
        Order(
            order_id=attempt.order_id,
            amount=attempt.amount,
            amount_paid=attempt.amount if attempt.captured else 0,
            amount_due=0 if attempt.captured else attempt.amount,
            currency="INR",
            status=OrderStatus.paid if attempt.captured else OrderStatus.attempted,
            attempts=1,
            created_at=attempt.created_at,
        )
        for attempt in attempts
    ]
    orders.sort(key=lambda order: order.created_at)

    return {
        "orders": orders,
        "payment_attempts": attempts,
        "ground_truth": {
            "attempts": ground_truth_attempts,
            "clusters": clusters,
            "clustered_payment_ids": [
                payment_id
                for payment_id, truth in ground_truth_attempts.items()
                if truth["is_clustered"]
            ],
            "isolated_payment_ids": [
                payment_id
                for payment_id, truth in ground_truth_attempts.items()
                if not truth["is_clustered"]
            ],
        },
    }
