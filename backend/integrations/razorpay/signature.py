"""Razorpay webhook signature verification."""

import hashlib
import hmac


class RazorpayWebhookSignatureError(ValueError):
    """Raised when a webhook signature cannot be verified."""


def verify_webhook_signature(raw_body: bytes, signature: str, secret: str) -> None:
    """Verify a Razorpay signature against the exact received request body."""
    if not isinstance(raw_body, bytes):
        raise RazorpayWebhookSignatureError("Webhook body must be bytes.")
    if not signature or not isinstance(signature, str):
        raise RazorpayWebhookSignatureError("Webhook signature is missing.")
    if not secret:
        raise RazorpayWebhookSignatureError("Webhook secret is missing.")

    expected = hmac.new(secret.encode("utf-8"), raw_body, hashlib.sha256).hexdigest()
    if not hmac.compare_digest(expected, signature):
        raise RazorpayWebhookSignatureError("Webhook signature is invalid.")
