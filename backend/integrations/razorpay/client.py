"""Minimal Razorpay HTTP API client for read-only lookups."""

from dataclasses import dataclass
import os
from typing import Any

import httpx
from pydantic import BaseModel


DEFAULT_BASE_URL = "https://api.razorpay.com/v1/"
DEFAULT_TIMEOUT_SECONDS = 10.0
TEST_KEY_PREFIX = "rzp_test_"


@dataclass(frozen=True)
class RazorpayClientConfig:
    key_id: str
    key_secret: str
    base_url: str = DEFAULT_BASE_URL

    def __post_init__(self) -> None:
        if not isinstance(self.key_id, str) or not self.key_id.startswith(TEST_KEY_PREFIX):
            raise ValueError("Only Razorpay Test Mode key IDs are supported.")

    @classmethod
    def from_env(cls) -> "RazorpayClientConfig":
        key_id = os.getenv("RAZORPAY_KEY_ID")
        key_secret = os.getenv("RAZORPAY_KEY_SECRET")
        if not key_id or not key_secret:
            raise ValueError(
                "RAZORPAY_KEY_ID and RAZORPAY_KEY_SECRET must be configured."
            )
        return cls(key_id=key_id, key_secret=key_secret)


class RazorpayAPIError(RuntimeError):
    """Raised when a Razorpay API request fails."""

    def __init__(self, message: str, *, status_code: int | None = None) -> None:
        super().__init__(message)
        self.status_code = status_code


class RazorpayCheckoutOptions(BaseModel):
    """Client-side Checkout options for an existing Razorpay Test order."""

    key: str
    order_id: str
    amount: int
    currency: str


def build_checkout_options(
    config: RazorpayClientConfig,
    *,
    order_id: str,
    amount: int,
    currency: str,
) -> RazorpayCheckoutOptions:
    """Build Checkout options for an existing Test Mode order."""
    if not order_id:
        raise ValueError("order_id is required for Checkout.")
    if not isinstance(amount, int) or amount <= 0:
        raise ValueError("A positive order amount is required for Checkout.")
    if not currency:
        raise ValueError("currency is required for Checkout.")
    return RazorpayCheckoutOptions(
        key=config.key_id,
        order_id=order_id,
        amount=amount,
        currency=currency,
    )


class RazorpayClient:
    """Read-only client for the Razorpay Orders and Payments endpoints."""

    def __init__(
        self,
        config: RazorpayClientConfig | None = None,
        *,
        http_client: httpx.Client | None = None,
        timeout: float = DEFAULT_TIMEOUT_SECONDS,
    ) -> None:
        self.config = config or RazorpayClientConfig.from_env()
        self._owns_http_client = http_client is None
        self._http_client = http_client or httpx.Client(
            auth=(self.config.key_id, self.config.key_secret),
            timeout=timeout,
        )
        self._http_client.auth = (self.config.key_id, self.config.key_secret)

    def get_order(self, order_id: str) -> dict[str, Any]:
        return self._get(f"orders/{order_id}")

    def get_payment(self, payment_id: str) -> dict[str, Any]:
        return self._get(f"payments/{payment_id}")

    def build_checkout_options(
        self,
        *,
        order_id: str,
        amount: int,
        currency: str,
    ) -> RazorpayCheckoutOptions:
        """Build customer-side Checkout options without calling Razorpay.

        The caller supplies the durable order snapshot. This intentionally does
        not create an order or attempt to collect a payment server-side.
        """
        return build_checkout_options(
            self.config,
            order_id=order_id,
            amount=amount,
            currency=currency,
        )

    def close(self) -> None:
        if self._owns_http_client:
            self._http_client.close()

    def __enter__(self) -> "RazorpayClient":
        return self

    def __exit__(self, *args: object) -> None:
        self.close()

    def _get(self, resource: str) -> dict[str, Any]:
        url = f"{self.config.base_url.rstrip('/')}/{resource}"
        try:
            response = self._http_client.get(url)
        except httpx.HTTPError as exc:
            raise RazorpayAPIError(f"Razorpay request failed: {exc}") from exc

        if not 200 <= response.status_code < 300:
            raise RazorpayAPIError(
                f"Razorpay API returned HTTP {response.status_code}.",
                status_code=response.status_code,
            )

        try:
            payload = response.json()
        except ValueError as exc:
            raise RazorpayAPIError("Razorpay API returned invalid JSON.") from exc
        if not isinstance(payload, dict):
            raise RazorpayAPIError("Razorpay API returned an unexpected payload.")
        return payload
