from datetime import datetime, timedelta, timezone

from backend.cache import RedisFailureCache
from backend.pipeline.signal_detector import SignalDetector
from backend.schemas import PaymentAttempt, PaymentError, PaymentMethod, PaymentStatus


class FakeRedis:
    def __init__(self):
        self.sorted_sets = {}
        self.ttls = {}

    def zadd(self, key, members):
        self.sorted_sets.setdefault(key, {}).update(members)

    def zremrangebyscore(self, key, minimum, maximum):
        values = self.sorted_sets.get(key, {})
        if isinstance(maximum, str) and maximum.startswith("("):
            cutoff = float(maximum[1:])
            values_copy = {member: score for member, score in values.items() if score <= cutoff}
        else:
            cutoff = float(maximum)
            values_copy = {member: score for member, score in values.items() if score < cutoff}
        for member in values_copy:
            values.pop(member, None)

    def zcard(self, key):
        return len(self.sorted_sets.get(key, {}))

    def zcount(self, key, minimum, maximum):
        minimum = float(minimum)
        return sum(score >= minimum for score in self.sorted_sets.get(key, {}).values())

    def expire(self, key, seconds):
        self.ttls[key] = seconds


def payment(payment_id, *, at, issuer_bin="411111", code="issuer_declined", status=PaymentStatus.failed):
    return PaymentAttempt(
        payment_id=payment_id,
        order_id=f"order_{payment_id}",
        amount=49900,
        method=PaymentMethod.card,
        status=status,
        captured=status is PaymentStatus.captured,
        created_at=at,
        issuer_bin=issuer_bin,
        failed_at=at if status is PaymentStatus.failed else None,
        error=PaymentError(code=code) if status is PaymentStatus.failed else None,
    )


def detector():
    return SignalDetector(RedisFailureCache(FakeRedis()))


def test_five_matching_failures_form_a_cluster():
    signal_detector = detector()
    start = datetime(2026, 1, 1, tzinfo=timezone.utc)
    contexts = [
        signal_detector.detect(payment(f"pay_{index}", at=start + timedelta(minutes=index)))
        for index in range(5)
    ]
    assert contexts[-1].is_cluster_candidate is True
    assert contexts[-1].matching_failure_count == 5


def test_four_matching_failures_do_not_form_a_cluster():
    signal_detector = detector()
    start = datetime(2026, 1, 1, tzinfo=timezone.utc)
    contexts = [
        signal_detector.detect(payment(f"pay_{index}", at=start + timedelta(minutes=index)))
        for index in range(4)
    ]
    assert contexts[-1].is_cluster_candidate is False
    assert contexts[-1].matching_failure_count == 4


def test_old_failures_are_removed_from_the_window():
    signal_detector = detector()
    start = datetime(2026, 1, 1, tzinfo=timezone.utc)
    signal_detector.detect(payment("old", at=start))
    context = signal_detector.detect(payment("new", at=start + timedelta(seconds=601)))
    assert context.matching_failure_count == 1


def test_bin_and_error_code_are_part_of_the_grouping_key():
    signal_detector = detector()
    start = datetime(2026, 1, 1, tzinfo=timezone.utc)
    for index in range(4):
        signal_detector.detect(payment(f"same_{index}", at=start + timedelta(seconds=index)))
    context = signal_detector.detect(payment("other", at=start + timedelta(seconds=4), issuer_bin="400000"))
    assert context.matching_failure_count == 1
    context = signal_detector.detect(payment("different_code", at=start + timedelta(seconds=5), code="network_error"))
    assert context.matching_failure_count == 1


def test_non_failed_payment_does_not_enter_the_signal():
    signal_detector = detector()
    at = datetime(2026, 1, 1, tzinfo=timezone.utc)
    context = signal_detector.detect(payment("captured", at=at, status=PaymentStatus.captured))
    assert context.is_cluster_candidate is False
    assert context.matching_failure_count == 0


def test_duplicate_payment_event_does_not_inflate_count():
    signal_detector = detector()
    at = datetime(2026, 1, 1, tzinfo=timezone.utc)
    signal_detector.detect(payment("duplicate", at=at))
    context = signal_detector.detect(payment("duplicate", at=at))
    assert context.matching_failure_count == 1


def test_count_failures_is_read_only():
    redis = FakeRedis()
    failure_cache = RedisFailureCache(redis)
    at = datetime(2026, 1, 1, tzinfo=timezone.utc)
    failure_cache.record_failure(
        "411111",
        "issuer_declined",
        occurred_at=at,
        payment_id="old",
    )
    before = {key: values.copy() for key, values in redis.sorted_sets.items()}

    assert failure_cache.count_failures(
        "411111",
        "issuer_declined",
        occurred_at=at + timedelta(seconds=601),
    ) == 0
    assert redis.sorted_sets == before


def test_detect_records_once_then_reads_the_population_count():
    class CountingFailureCache(RedisFailureCache):
        def __init__(self):
            super().__init__(FakeRedis())
            self.record_calls = 0
            self.count_calls = 0

        def record_failure(self, *args, **kwargs):
            self.record_calls += 1
            return super().record_failure(*args, **kwargs)

        def count_failures(self, *args, **kwargs):
            self.count_calls += 1
            return super().count_failures(*args, **kwargs)

    failure_cache = CountingFailureCache()
    signal_detector = SignalDetector(failure_cache)
    at = datetime(2026, 1, 1, tzinfo=timezone.utc)

    context = signal_detector.detect(payment("current", at=at))

    assert context.matching_failure_count == 1
    assert failure_cache.record_calls == 1
    assert failure_cache.count_calls == 1
