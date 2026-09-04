"""Razorpay webhook parsing and persistence helpers for Step 12B."""

from collections.abc import Mapping
from datetime import datetime, timezone
import json
from typing import Any

from pydantic import BaseModel

from backend.db import Database
from backend.schemas import Order, OrderStatus, PaymentAttempt, PaymentError, PaymentMethod, PaymentStatus


class RazorpayWebhookParseError(ValueError):
    """Raised when a webhook body cannot be parsed safely."""


class RazorpayWebhookEvent(BaseModel):
    event_id: str
    event_type: str
    payload: dict[str, Any]
    occurred_at: datetime | None = None


class RazorpayPaymentState(BaseModel):
    attempt: PaymentAttempt
    currency: str


class WebhookIngestResult(BaseModel):
    event_id: str
    event_type: str
    status: str
    payment_id: str | None = None
    order_id: str | None = None


_ORDER_STATUS_RANK = {
    OrderStatus.created: 0,
    OrderStatus.attempted: 1,
    OrderStatus.paid: 2,
}


def _nested_dict(payload: dict[str, Any], *path: str) -> dict[str, Any] | None:
    current: Any = payload
    for key in path:
        if not isinstance(current, dict):
            return None
        current = current.get(key)
    return current if isinstance(current, dict) else None


def _coerce_datetime(value: Any) -> datetime | None:
    if value is None:
        return None
    if isinstance(value, datetime):
        return value if value.tzinfo else value.replace(tzinfo=timezone.utc)
    try:
        return datetime.fromtimestamp(float(value), tz=timezone.utc)
    except (TypeError, ValueError, OverflowError):
        return None


def _coerce_int(value: Any) -> int | None:
    if value is None:
        return None
    try:
        return int(value)
    except (TypeError, ValueError):
        return None


def _event_id(payload: dict[str, Any], headers: Mapping[str, str]) -> str | None:
    header_value = headers.get("x-razorpay-event-id") or headers.get("X-Razorpay-Event-Id")
    if header_value:
        return header_value
    for key in ("event_id", "id", "eventId"):
        value = payload.get(key)
        if isinstance(value, str) and value:
            return value
    return None


def _payment_entity(payload: dict[str, Any]) -> dict[str, Any] | None:
    return _nested_dict(payload, "payload", "payment", "entity")


def _order_entity(payload: dict[str, Any]) -> dict[str, Any] | None:
    return _nested_dict(payload, "payload", "order", "entity")


def _payment_error(entity: dict[str, Any]) -> PaymentError | None:
    raw_error = entity.get("error") if isinstance(entity.get("error"), dict) else None
    code = raw_error.get("code") if raw_error else entity.get("error_code")
    description = raw_error.get("description") if raw_error else entity.get("error_description")
    field = raw_error.get("field") if raw_error else entity.get("error_field")
    source = raw_error.get("source") if raw_error else entity.get("error_source")
    step = raw_error.get("step") if raw_error else entity.get("error_step")
    reason = raw_error.get("reason") if raw_error else entity.get("error_reason")

    if not any([code, description, field, source, step, reason]):
        return None

    metadata: dict[str, str] = {}
    extra_metadata = raw_error.get("metadata") if raw_error else entity.get("error_metadata")
    if isinstance(extra_metadata, dict):
        metadata = {str(key): str(value) for key, value in extra_metadata.items() if value is not None}

    return PaymentError(
        code=str(code or "unknown"),
        description=str(description) if description is not None else None,
        field=str(field) if field is not None else None,
        source=str(source) if source is not None else None,
        step=str(step) if step is not None else None,
        reason=str(reason) if reason is not None else None,
        metadata=metadata,
    )


def _payment_status(event_type: str, entity: dict[str, Any]) -> PaymentStatus:
    raw_status = entity.get("status")
    if isinstance(raw_status, str) and raw_status in PaymentStatus._value2member_map_:
        return PaymentStatus(raw_status)

    suffix = event_type.rsplit(".", 1)[-1].lower()
    if suffix in PaymentStatus._value2member_map_:
        return PaymentStatus(suffix)

    if entity.get("captured") is True:
        return PaymentStatus.captured
    if _payment_error(entity) is not None:
        return PaymentStatus.failed
    return PaymentStatus.authorized


def _order_status(raw_status: Any, payment_status: PaymentStatus | None = None) -> OrderStatus:
    if isinstance(raw_status, str) and raw_status in OrderStatus._value2member_map_:
        status = OrderStatus(raw_status)
    else:
        status = OrderStatus.created

    if payment_status is PaymentStatus.captured:
        return OrderStatus.paid
    if payment_status in {PaymentStatus.authorized, PaymentStatus.failed} and status is OrderStatus.created:
        return OrderStatus.attempted
    return status


def _issuer_bin(entity: dict[str, Any]) -> str:
    card = entity.get("card") if isinstance(entity.get("card"), dict) else None
    for candidate in (
        card.get("iin") if card else None,
        card.get("bin") if card else None,
        entity.get("issuer_bin"),
    ):
        if candidate not in (None, ""):
            return str(candidate)
    return "unknown"


def _payment_method(entity: dict[str, Any]) -> PaymentMethod | None:
    raw_method = entity.get("method")
    if raw_method is None:
        return PaymentMethod.card
    if isinstance(raw_method, str) and raw_method.lower() == PaymentMethod.card.value:
        return PaymentMethod.card
    return None


class RazorpayWebhookAdapter:
    """Parse Razorpay-shaped webhook payloads into domain models."""

    def parse(self, raw_body: bytes, headers: Mapping[str, str]) -> RazorpayWebhookEvent:
        try:
            payload = json.loads(raw_body)
        except json.JSONDecodeError as exc:
            raise RazorpayWebhookParseError("Webhook body must be valid JSON.") from exc

        if not isinstance(payload, dict):
            raise RazorpayWebhookParseError("Webhook body must be a JSON object.")

        event_id = _event_id(payload, headers)
        if not event_id:
            raise RazorpayWebhookParseError("Webhook event id is missing.")

        event_type = payload.get("event")
        if not isinstance(event_type, str) or not event_type:
            raise RazorpayWebhookParseError("Webhook event type is missing.")

        occurred_at = _coerce_datetime(payload.get("created_at"))
        return RazorpayWebhookEvent(
            event_id=event_id,
            event_type=event_type,
            payload=payload,
            occurred_at=occurred_at,
        )

    def extract_order_snapshot(self, event: RazorpayWebhookEvent) -> Order | None:
        entity = _order_entity(event.payload)
        if not entity:
            return None

        order_id = entity.get("id")
        amount = _coerce_int(entity.get("amount"))
        currency = entity.get("currency")
        created_at = _coerce_datetime(entity.get("created_at")) or event.occurred_at
        if not isinstance(order_id, str) or not order_id or amount is None or not isinstance(currency, str) or not currency:
            return None

        amount_paid = _coerce_int(entity.get("amount_paid")) or 0
        amount_due = _coerce_int(entity.get("amount_due"))
        if amount_due is None:
            amount_due = max(amount - amount_paid, 0)

        attempts = _coerce_int(entity.get("attempts")) or 0
        return Order(
            order_id=order_id,
            amount=amount,
            amount_paid=amount_paid,
            amount_due=amount_due,
            currency=currency,
            status=_order_status(entity.get("status")),
            attempts=attempts,
            created_at=created_at or datetime.now(timezone.utc),
        )

    def extract_payment_state(
        self,
        event: RazorpayWebhookEvent,
        *,
        order_id_override: str | None = None,
    ) -> RazorpayPaymentState | None:
        entity = _payment_entity(event.payload)
        if not entity:
            return None

        if _payment_method(entity) is None:
            return None

        payment_id = entity.get("id")
        order_id = order_id_override or entity.get("order_id")
        amount = _coerce_int(entity.get("amount"))
        currency = entity.get("currency")
        if not isinstance(payment_id, str) or not payment_id or not isinstance(order_id, str) or not order_id:
            return None
        if amount is None or not isinstance(currency, str) or not currency:
            return None

        payment_status = _payment_status(event.event_type, entity)
        created_at = _coerce_datetime(entity.get("created_at")) or event.occurred_at or datetime.now(timezone.utc)
        error = _payment_error(entity)
        if payment_status is PaymentStatus.failed and error is None:
            error = PaymentError(code="unknown")

        failed_at = _coerce_datetime(entity.get("failed_at")) if payment_status is PaymentStatus.failed else None
        if payment_status is PaymentStatus.failed and failed_at is None:
            failed_at = created_at

        attempt = PaymentAttempt(
            payment_id=payment_id,
            order_id=order_id,
            amount=amount,
            method=PaymentMethod.card,
            status=payment_status,
            captured=payment_status is PaymentStatus.captured,
            created_at=created_at,
            error=error if payment_status is PaymentStatus.failed else None,
            issuer_bin=_issuer_bin(entity),
            failed_at=failed_at,
        )
        return RazorpayPaymentState(attempt=attempt, currency=currency)

    def placeholder_order_from_payment(self, payment_state: RazorpayPaymentState) -> Order:
        attempt = payment_state.attempt
        status = OrderStatus.paid if attempt.status is PaymentStatus.captured else OrderStatus.attempted
        return Order(
            order_id=attempt.order_id,
            amount=attempt.amount,
            amount_paid=attempt.amount if attempt.status is PaymentStatus.captured else 0,
            amount_due=0 if attempt.status is PaymentStatus.captured else attempt.amount,
            currency=payment_state.currency,
            status=status,
            attempts=0,
            created_at=attempt.created_at,
        )

    def merge_order_state(
        self,
        order: Order,
        payment_state: RazorpayPaymentState | None,
        attempt_count: int,
    ) -> Order:
        if payment_state is None:
            return order.model_copy(update={"attempts": max(order.attempts, attempt_count)})

        payment = payment_state.attempt
        payment_status = OrderStatus.paid if payment.status is PaymentStatus.captured else OrderStatus.attempted
        current_rank = _ORDER_STATUS_RANK[order.status]
        candidate_rank = _ORDER_STATUS_RANK[payment_status]
        merged_status = order.status if current_rank >= candidate_rank else payment_status
        merged_amount_paid = max(order.amount_paid, payment.amount if payment.status is PaymentStatus.captured else 0)
        merged_amount_due = 0 if payment.status is PaymentStatus.captured else order.amount_due
        return order.model_copy(
            update={
                "status": merged_status,
                "amount_paid": merged_amount_paid,
                "amount_due": merged_amount_due,
                "attempts": max(order.attempts, attempt_count),
            }
        )


class RazorpayWebhookProcessor:
    """Persist Razorpay webhook events with monotonic state updates."""

    def __init__(self, database: Database, adapter: RazorpayWebhookAdapter | None = None) -> None:
        self.database = database
        self.adapter = adapter or RazorpayWebhookAdapter()

    def ingest(self, raw_body: bytes, headers: Mapping[str, str]) -> WebhookIngestResult:
        event = self.adapter.parse(raw_body, headers)
        with self.database.transaction() as connection:
            inserted = self.database._record_webhook_event(
                connection,
                event.event_id,
                event.event_type,
                event.payload,
                event.occurred_at,
            )
            if not inserted:
                return WebhookIngestResult(
                    event_id=event.event_id,
                    event_type=event.event_type,
                    status="duplicate",
                )

            self._apply_event(connection, event)

        payment_state = self.adapter.extract_payment_state(
            event,
            order_id_override=self._order_id_for_result(event),
        )
        return WebhookIngestResult(
            event_id=event.event_id,
            event_type=event.event_type,
            status="accepted",
            payment_id=payment_state.attempt.payment_id if payment_state else None,
            order_id=payment_state.attempt.order_id if payment_state else self._order_id_for_result(event),
        )

    def _order_id_for_result(self, event: RazorpayWebhookEvent) -> str | None:
        order_snapshot = self.adapter.extract_order_snapshot(event)
        if order_snapshot is not None:
            return order_snapshot.order_id
        payment_state = self.adapter.extract_payment_state(event)
        if payment_state is not None:
            return payment_state.attempt.order_id
        return None

    def _apply_event(self, connection: Any, event: RazorpayWebhookEvent) -> None:
        order_snapshot = self.adapter.extract_order_snapshot(event)
        payment_state = self.adapter.extract_payment_state(
            event,
            order_id_override=order_snapshot.order_id if order_snapshot is not None else None,
        )

        if payment_state is not None:
            base_order = order_snapshot or self.adapter.placeholder_order_from_payment(payment_state)
            self.database._save_order(connection, base_order)
            self._persist_payment_state(connection, payment_state)
            attempt_count = self.database.count_payment_attempts_for_order(connection, payment_state.attempt.order_id)
            merged_order = self.adapter.merge_order_state(base_order, payment_state, attempt_count)
            self._persist_order_state(connection, merged_order)
            if payment_state.attempt.status is PaymentStatus.failed:
                self._create_recovery_case_if_eligible(connection, payment_state, base_order)
            elif payment_state.attempt.status is PaymentStatus.captured:
                self._mark_recovery_cases_recovered(connection, payment_state.attempt.order_id)
            return

        if order_snapshot is not None:
            self._persist_order_state(connection, order_snapshot)
            if event.event_type == "order.paid":
                self._mark_recovery_cases_recovered(connection, order_snapshot.order_id)

    def _persist_payment_state(self, connection: Any, payment_state: RazorpayPaymentState) -> None:
        self.database._save_payment_attempt(connection, payment_state.attempt)

    def _persist_order_state(self, connection: Any, order: Order) -> None:
        self.database._save_order(connection, order)

    def _create_recovery_case_if_eligible(
        self,
        connection: Any,
        payment_state: RazorpayPaymentState,
        incoming_order: Order,
    ) -> None:
        if incoming_order.status not in {OrderStatus.created, OrderStatus.attempted}:
            return

        row = connection.execute(
            "SELECT status FROM orders WHERE order_id = %s",
            (payment_state.attempt.order_id,),
        ).fetchone()
        if not row or row["status"] not in {OrderStatus.created.value, OrderStatus.attempted.value}:
            return

        self._persist_recovery_case(
            connection,
            case_id=f"recovery-{payment_state.attempt.payment_id}",
            payment_id=payment_state.attempt.payment_id,
            order_id=payment_state.attempt.order_id,
            status="open",
        )

    def _mark_recovery_cases_recovered(self, connection: Any, order_id: str) -> None:
        rows = connection.execute(
            "SELECT case_id, payment_id, order_id FROM recovery_cases WHERE order_id = %s",
            (order_id,),
        ).fetchall()
        for row in rows:
            self._persist_recovery_case(
                connection,
                case_id=row["case_id"],
                payment_id=row["payment_id"],
                order_id=row["order_id"],
                status="recovered",
            )

    def _persist_recovery_case(
        self,
        connection: Any,
        *,
        case_id: str,
        payment_id: str,
        order_id: str,
        status: str,
    ) -> None:
        self.database._save_recovery_case(connection, case_id, payment_id, order_id, status)
