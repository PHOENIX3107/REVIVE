from datetime import datetime, timedelta, timezone

from backend.pipeline.downtime_correlator import correlate_downtime
from backend.schemas import Downtime, PaymentAttempt, PaymentMethod, PaymentStatus


START = datetime(2026, 1, 1, tzinfo=timezone.utc)


def payment(*, at=START, status=PaymentStatus.failed, failed_at=START):
    return PaymentAttempt(
        payment_id="pay_test",
        order_id="order_test",
        amount=49900,
        method=PaymentMethod.card,
        status=status,
        captured=status is PaymentStatus.captured,
        created_at=at,
        issuer_bin="411111",
        failed_at=failed_at,
    )


def downtime(
    downtime_id="down_test",
    *,
    begin=START - timedelta(minutes=5),
    end=START + timedelta(minutes=5),
    method="card",
    severity="high",
    status="active",
):
    return Downtime(
        downtime_id=downtime_id,
        entity="payments",
        method=method,
        begin=begin,
        end=end,
        status=status,
        scheduled=False,
        severity=severity,
    )


def test_failure_inside_downtime_matches():
    result = correlate_downtime(payment(), [downtime()])
    assert result.model_dump() == {
        "matched": True,
        "downtime_id": "down_test",
        "severity": "high",
        "status": "active",
    }


def test_failure_before_downtime_does_not_match():
    result = correlate_downtime(payment(at=START - timedelta(minutes=6), failed_at=START - timedelta(minutes=6)), [downtime()])
    assert result.matched is False


def test_failure_after_downtime_does_not_match():
    result = correlate_downtime(payment(at=START + timedelta(minutes=6), failed_at=START + timedelta(minutes=6)), [downtime()])
    assert result.matched is False


def test_open_downtime_matches_failure_after_begin():
    result = correlate_downtime(payment(at=START + timedelta(days=1), failed_at=START + timedelta(days=1)), [downtime(end=None)])
    assert result.matched is True


def test_different_payment_method_does_not_match():
    result = correlate_downtime(payment(), [downtime(method="upi")])
    assert result.matched is False


def test_multiple_matches_choose_latest_current_record_deterministically():
    records = [
        downtime("older", begin=START - timedelta(minutes=10)),
        downtime("newer", begin=START - timedelta(minutes=2), severity="critical"),
    ]
    result = correlate_downtime(payment(), records)
    assert result.downtime_id == "newer"
    assert result.severity == "critical"


def test_no_downtime_records_does_not_match():
    result = correlate_downtime(payment(), [])
    assert result.matched is False


def test_non_failed_payment_does_not_match():
    result = correlate_downtime(payment(status=PaymentStatus.captured, failed_at=None), [downtime()])
    assert result.matched is False


def test_failed_payment_without_failed_at_does_not_match():
    result = correlate_downtime(payment(failed_at=None), [downtime()])
    assert result.matched is False
