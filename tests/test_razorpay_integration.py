import hashlib
import hmac

import httpx
import pytest

from backend.integrations.razorpay.client import (
    RazorpayAPIError,
    RazorpayCheckoutOptions,
    RazorpayClient,
    RazorpayClientConfig,
    build_checkout_options,
)
from backend.integrations.razorpay.signature import (
    RazorpayWebhookSignatureError,
    verify_webhook_signature,
)
from backend.recovery.reconciliation import (
    normalize_order_response,
    normalize_payment_response,
)


def test_verify_webhook_signature_uses_exact_raw_body() -> None:
    raw_body = b'{ "event": "payment.captured" }\n'
    secret = "webhook-secret"
    signature = hmac.new(secret.encode(), raw_body, hashlib.sha256).hexdigest()

    assert verify_webhook_signature(raw_body, signature, secret) is None


@pytest.mark.parametrize("signature", ["", "wrong", None])
def test_verify_webhook_signature_rejects_missing_or_invalid_signature(signature: str | None) -> None:
    with pytest.raises(RazorpayWebhookSignatureError):
        verify_webhook_signature(b"{}", signature, "secret")  # type: ignore[arg-type]


def test_razorpay_client_gets_order_with_basic_auth() -> None:
    def handler(request: httpx.Request) -> httpx.Response:
        assert request.url == httpx.URL("https://test.example/v1/orders/order_123")
        assert request.headers["authorization"] == "Basic cnpwX3Rlc3Rfa2V5OnNlY3JldA=="
        return httpx.Response(200, json={"id": "order_123"})

    transport = httpx.MockTransport(handler)
    http_client = httpx.Client(transport=transport)
    client = RazorpayClient(
        RazorpayClientConfig("rzp_test_key", "secret", "https://test.example/v1/"),
        http_client=http_client,
    )

    try:
        assert client.get_order("order_123") == {"id": "order_123"}
    finally:
        http_client.close()


def test_razorpay_client_gets_payment() -> None:
    transport = httpx.MockTransport(
        lambda request: httpx.Response(200, json={"id": "pay_123"})
    )
    http_client = httpx.Client(transport=transport)
    client = RazorpayClient(
        RazorpayClientConfig("rzp_test_key", "secret", "https://test.example/v1"),
        http_client=http_client,
    )

    try:
        assert client.get_payment("pay_123") == {"id": "pay_123"}
    finally:
        http_client.close()


@pytest.mark.parametrize("failure", ["http", "transport"])
def test_razorpay_client_raises_dedicated_api_error(failure: str) -> None:
    def handler(request: httpx.Request) -> httpx.Response:
        if failure == "http":
            return httpx.Response(401, json={"error": "unauthorized"})
        raise httpx.ConnectError("connection refused", request=request)

    http_client = httpx.Client(transport=httpx.MockTransport(handler))
    client = RazorpayClient(
        RazorpayClientConfig("rzp_test_key", "secret", "https://test.example/v1"),
        http_client=http_client,
    )

    try:
        with pytest.raises(RazorpayAPIError) as exc_info:
            client.get_payment("pay_123")
        if failure == "http":
            assert exc_info.value.status_code == 401
        else:
            assert exc_info.value.status_code is None
    finally:
        http_client.close()


def test_checkout_options_use_existing_test_order_without_http_call() -> None:
    config = RazorpayClientConfig("rzp_test_key", "secret")

    options = build_checkout_options(
        config,
        order_id="order_existing",
        amount=49900,
        currency="INR",
    )

    assert isinstance(options, RazorpayCheckoutOptions)
    assert options.model_dump() == {
        "key": "rzp_test_key",
        "order_id": "order_existing",
        "amount": 49900,
        "currency": "INR",
    }


def test_client_config_rejects_non_test_mode_key(monkeypatch) -> None:
    monkeypatch.setenv("RAZORPAY_KEY_ID", "rzp_live_key")
    monkeypatch.setenv("RAZORPAY_KEY_SECRET", "secret")

    with pytest.raises(ValueError, match="Test Mode"):
        RazorpayClientConfig.from_env()


def test_normalize_direct_api_payment_and_order_responses() -> None:
    payment = normalize_payment_response(
        {
            "id": "pay_123",
            "entity": "payment",
            "amount": 49900,
            "currency": "INR",
            "status": "captured",
            "method": "card",
            "order_id": "order_123",
            "captured": True,
            "amount_captured": 49900,
            "created_at": 1767225600,
            "card": {"iin": "411111"},
        }
    )
    order = normalize_order_response(
        {
            "id": "order_123",
            "entity": "order",
            "amount": 49900,
            "amount_paid": 49900,
            "amount_due": 0,
            "currency": "INR",
            "status": "paid",
            "attempts": 1,
            "created_at": 1767225600,
        }
    )

    assert payment.provider_status == "captured"
    assert payment.payment.payment_id == "pay_123"
    assert payment.payment.order_id == "order_123"
    assert payment.payment.status.value == "captured"
    assert payment.amount_captured == 49900
    assert order.provider_status == "paid"
    assert order.order.order_id == "order_123"
    assert order.order.status.value == "paid"


@pytest.mark.parametrize("provider_status", ["created", "refunded"])
def test_normalize_created_and_refunded_keeps_provider_state_without_internal_coercion(
    provider_status: str,
) -> None:
    payment = normalize_payment_response(
        {
            "id": "pay_123",
            "entity": "payment",
            "amount": 49900,
            "currency": "INR",
            "status": provider_status,
            "method": "card",
            "order_id": "order_123",
            "captured": False,
            "amount_captured": 0,
            "created_at": 1767225600,
        }
    )

    assert payment.provider_status == provider_status
    assert payment.payment is None
