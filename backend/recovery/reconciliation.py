"""Razorpay API verification and durable state reconciliation."""

from collections.abc import Mapping
from datetime import datetime, timezone
from typing import Any

from pydantic import BaseModel

from backend.db import Database
from backend.integrations.razorpay.client import RazorpayClient
from backend.schemas import Order, OrderStatus, PaymentAttempt, PaymentStatus
from backend.webhooks.razorpay import RazorpayWebhookAdapter, RazorpayWebhookEvent


_PAYMENT_STATUSES = frozenset({"created", "authorized", "captured", "failed", "refunded"})
_ORDER_STATUSES = frozenset({"created", "attempted", "paid"})


class PaymentVerificationError(RuntimeError):
    """Raised when a provider response cannot be safely reconciled."""


class PaymentVerificationCaseNotFoundError(PaymentVerificationError):
    """Raised when the requested recovery case does not exist."""


class PaymentVerificationStateError(PaymentVerificationError):
    """Raised when local and provider payment identities or amounts conflict."""


class RazorpayPaymentSnapshot(BaseModel):
    """A provider payment normalized into REVIVE's payment model."""

    # The current REVIVE PaymentStatus enum intentionally has no ``created``
    # or ``refunded`` members.  Those provider states therefore have no
    # internal payment model and must not be coerced into ``authorized``.
    payment: PaymentAttempt | None = None
    payment_id: str
    order_id: str
    amount: int
    provider_status: str
    amount_captured: int | None = None
    currency: str


class RazorpayOrderSnapshot(BaseModel):
    """A provider order normalized into REVIVE's order model."""

    order: Order
    provider_status: str


class PaymentVerificationResult(BaseModel):
    case_id: str
    payment_id: str
    order_id: str
    provider_payment_status: str
    provider_order_status: str
    revive_payment_status: PaymentStatus
    order_status: OrderStatus
    state_changed: bool
    recovery_confirmed: bool
    recovered_amount: int = 0
    reconciliation_recorded: bool = False


def _required_status(payload: Mapping[str, Any], field: str, allowed: frozenset[str]) -> str:
    value = payload.get(field)
    if not isinstance(value, str) or value not in allowed:
        raise PaymentVerificationError(f"Razorpay response has an invalid {field}.")
    return value


def _coerce_amount_captured(payload: Mapping[str, Any]) -> int | None:
    value = payload.get("amount_captured")
    if value is None:
        return None
    try:
        amount = int(value)
    except (TypeError, ValueError):
        raise PaymentVerificationError("Razorpay response has an invalid amount_captured.") from None
    if amount < 0:
        raise PaymentVerificationError("Razorpay response has an invalid amount_captured.")
    return amount


def _provider_payment_fields(payload: Mapping[str, Any]) -> tuple[str, str, int, str]:
    payment_id = payload.get("id")
    order_id = payload.get("order_id")
    currency = payload.get("currency")
    try:
        amount = int(payload.get("amount"))
    except (TypeError, ValueError):
        amount = -1
    if (
        not isinstance(payment_id, str)
        or not payment_id
        or not isinstance(order_id, str)
        or not order_id
        or amount < 0
        or not isinstance(currency, str)
        or not currency
    ):
        raise PaymentVerificationError("Razorpay payment response is missing required fields.")
    return payment_id, order_id, amount, currency


def _payment_event(payload: Mapping[str, Any]) -> RazorpayWebhookEvent:
    return RazorpayWebhookEvent(
        event_id="api-payment-verification",
        event_type="payment.verification",
        payload={"payload": {"payment": {"entity": dict(payload)}}},
    )


def _order_event(payload: Mapping[str, Any]) -> RazorpayWebhookEvent:
    return RazorpayWebhookEvent(
        event_id="api-order-verification",
        event_type="order.verification",
        payload={"payload": {"order": {"entity": dict(payload)}}},
    )


def normalize_payment_response(payload: Mapping[str, Any]) -> RazorpayPaymentSnapshot:
    """Normalize the direct response from GET /v1/payments/:id."""
    if not isinstance(payload, Mapping):
        raise PaymentVerificationError("Razorpay payment response must be an object.")
    provider_status = _required_status(payload, "status", _PAYMENT_STATUSES)
    payment_id, order_id, amount, currency = _provider_payment_fields(payload)

    attempt = None
    if provider_status in {"authorized", "captured", "failed"}:
        state = RazorpayWebhookAdapter().extract_payment_state(_payment_event(payload))
        if state is None:
            raise PaymentVerificationError("Razorpay payment response is missing required fields.")
        attempt = state.attempt

    return RazorpayPaymentSnapshot(
        payment=attempt,
        payment_id=payment_id,
        order_id=order_id,
        amount=amount,
        provider_status=provider_status,
        amount_captured=_coerce_amount_captured(payload),
        currency=currency,
    )


def normalize_order_response(payload: Mapping[str, Any]) -> RazorpayOrderSnapshot:
    """Normalize the direct response from GET /v1/orders/:id."""
    if not isinstance(payload, Mapping):
        raise PaymentVerificationError("Razorpay order response must be an object.")
    provider_status = _required_status(payload, "status", _ORDER_STATUSES)
    order = RazorpayWebhookAdapter().extract_order_snapshot(_order_event(payload))
    if order is None:
        raise PaymentVerificationError("Razorpay order response is missing required fields.")
    return RazorpayOrderSnapshot(
        order=order.model_copy(update={"status": OrderStatus(provider_status)}),
        provider_status=provider_status,
    )


class RazorpayPaymentReconciler:
    """Verify a durable recovery case against Razorpay and reconcile it once."""

    def __init__(self, database: Database, razorpay_client: RazorpayClient | None = None) -> None:
        self.database = database
        self.razorpay_client = razorpay_client

    def verify_case(self, case_id: str) -> PaymentVerificationResult:
        case = self.database.get_recovery_case(case_id)
        if case is None:
            raise PaymentVerificationCaseNotFoundError(f"Recovery case not found: {case_id}")

        payment_id = case.get("payment_id")
        order_id = case.get("order_id")
        if not isinstance(payment_id, str) or not isinstance(order_id, str):
            raise PaymentVerificationStateError("Recovery case has invalid payment or order identity.")

        payment = self.database.get_payment_attempt(payment_id)
        order = self.database.get_order(order_id)
        if payment is None or order is None:
            raise PaymentVerificationStateError("Recovery case is missing its associated payment or order.")
        if payment.order_id != order.order_id or payment.order_id != order_id:
            raise PaymentVerificationStateError("Recovery case payment and order do not match.")

        if self.razorpay_client is not None:
            return self._verify_with_client(case_id, payment, order, self.razorpay_client)

        with RazorpayClient() as client:
            return self._verify_with_client(case_id, payment, order, client)

    def _verify_with_client(
        self,
        case_id: str,
        payment: PaymentAttempt,
        order: Order,
        client: RazorpayClient,
    ) -> PaymentVerificationResult:
        # Fetch and validate every provider resource before opening the write
        # transaction, so provider/API errors cannot leave partial local state.
        provider_payment = normalize_payment_response(client.get_payment(payment.payment_id))
        provider_order = normalize_order_response(client.get_order(order.order_id))
        self._validate_identity(payment, order, provider_payment, provider_order)

        recovery_confirmed = self._is_recovery_confirmed(provider_payment)
        recovered_amount = self._recovered_amount(provider_payment, recovery_confirmed, order)
        # Only the exact requested provider payment can confirm this case.
        # ``created`` and ``refunded`` have no safe representation in the
        # current internal enum, so preserve the local attempt for those
        # observations rather than silently rewriting it as authorized.
        provider_attempt = provider_payment.payment or payment
        if provider_attempt.issuer_bin == "unknown":
            provider_attempt = provider_attempt.model_copy(update={"issuer_bin": payment.issuer_bin})

        effective_order = provider_order.order
        if recovery_confirmed:
            effective_order = effective_order.model_copy(
                update={
                    "status": OrderStatus.paid,
                    "amount_paid": max(effective_order.amount_paid, recovered_amount),
                    "amount_due": 0,
                }
            )

        audit_reason = (
            "Razorpay API verification: "
            f"payment_status={provider_payment.provider_status}, "
            f"order_status={provider_order.provider_status}, "
            f"amount_recovered={recovered_amount}"
        )
        reconciled = self.database.reconcile_provider_state(
            case_id,
            provider_attempt,
            effective_order,
            provider_payment_status=provider_payment.provider_status,
            provider_order_status=provider_order.provider_status,
            recovery_confirmed=recovery_confirmed,
            recovered_amount=recovered_amount,
            observed_at=datetime.now(timezone.utc),
            audit_reason=audit_reason,
        )
        return PaymentVerificationResult.model_validate(reconciled)

    @staticmethod
    def _validate_identity(
        payment: PaymentAttempt,
        order: Order,
        provider_payment: RazorpayPaymentSnapshot,
        provider_order: RazorpayOrderSnapshot,
    ) -> None:
        remote_order = provider_order.order
        if provider_payment.payment_id != payment.payment_id:
            raise PaymentVerificationStateError("Razorpay payment identity does not match the recovery case.")
        if provider_payment.order_id != order.order_id or remote_order.order_id != order.order_id:
            raise PaymentVerificationStateError("Razorpay order identity does not match the recovery case.")
        if provider_payment.amount != payment.amount or provider_payment.amount != order.amount:
            raise PaymentVerificationStateError("Razorpay payment amount does not match local state.")
        if remote_order.amount != order.amount:
            raise PaymentVerificationStateError("Razorpay order amount does not match local state.")
        if provider_payment.currency != order.currency or remote_order.currency != order.currency:
            raise PaymentVerificationStateError("Razorpay currency does not match local state.")
        if provider_order.provider_status == "paid" and remote_order.amount_paid != order.amount:
            raise PaymentVerificationStateError("Razorpay paid order amount does not match local state.")
        if provider_payment.provider_status == "captured" and provider_payment.amount_captured != order.amount:
            raise PaymentVerificationStateError("Razorpay captured amount does not match local state.")

    @staticmethod
    def _is_recovery_confirmed(
        payment: RazorpayPaymentSnapshot,
    ) -> bool:
        return payment.provider_status == "captured"

    @staticmethod
    def _recovered_amount(
        payment: RazorpayPaymentSnapshot,
        confirmed: bool,
        local_order: Order,
    ) -> int:
        if not confirmed:
            return 0
        amount = payment.amount_captured
        if amount != local_order.amount:
            raise PaymentVerificationStateError("Razorpay confirmed an invalid recovered amount.")
        return amount
