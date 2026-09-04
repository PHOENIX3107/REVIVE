"""Correlate failed payment attempts with applicable downtime records."""

from datetime import datetime
from collections.abc import Iterable

from pydantic import BaseModel

from backend.schemas import Downtime, PaymentAttempt, PaymentStatus


class DowntimeCorrelationResult(BaseModel):
    matched: bool
    downtime_id: str | None = None
    severity: str | None = None
    status: str | None = None


def _method_matches(payment: PaymentAttempt, downtime: Downtime) -> bool:
    return payment.method.value.lower() == downtime.method.strip().lower()


def correlate_downtime(
    payment: PaymentAttempt,
    downtimes: Iterable[Downtime],
) -> DowntimeCorrelationResult:
    """Return the deterministic best downtime match for one payment failure."""
    if payment.status is not PaymentStatus.failed or payment.failed_at is None:
        return DowntimeCorrelationResult(matched=False)

    matching: list[Downtime] = []
    for downtime in downtimes:
        if not _method_matches(payment, downtime):
            continue
        if payment.failed_at < downtime.begin:
            continue
        if downtime.end is not None and payment.failed_at > downtime.end:
            continue
        matching.append(downtime)

    if not matching:
        return DowntimeCorrelationResult(matched=False)

    def selection_key(downtime: Downtime) -> tuple[datetime, bool, datetime, str]:
        # Prefer the most recently started record, then an active record, then
        # the latest end time and finally the ID for stable tie-breaking.
        end = downtime.end or datetime.max.replace(tzinfo=downtime.begin.tzinfo)
        return downtime.begin, downtime.end is None, end, downtime.downtime_id

    selected = max(matching, key=selection_key)
    return DowntimeCorrelationResult(
        matched=True,
        downtime_id=selected.downtime_id,
        severity=selected.severity,
        status=selected.status,
    )
