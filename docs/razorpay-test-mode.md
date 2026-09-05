# Razorpay Test Mode manual check

This procedure uses Razorpay Test Mode only. It must not use production keys,
live customers, or real money.

## Configure the local environment

Create Test Mode API keys in the Razorpay Dashboard and keep all values in the
shell environment or an ignored `.env` file:

```sh
export POSTGRES_USER="revive"
export POSTGRES_PASSWORD="replace_with_local_password"
export POSTGRES_DB="revive"
export DATABASE_URL="postgresql://${POSTGRES_USER}:${POSTGRES_PASSWORD}@localhost:5432/${POSTGRES_DB}"
export REDIS_URL="redis://localhost:6379/0"
export RAZORPAY_KEY_ID="rzp_test_..."
export RAZORPAY_KEY_SECRET="..."
export RAZORPAY_WEBHOOK_SECRET="..."
```

The application rejects key IDs that are not prefixed with `rzp_test_`. Never
put a key or secret in source control.

Start the local dependencies and the API:

```sh
docker compose up -d postgres redis
./.venv/bin/uvicorn backend.main:app --reload
```

Configure a Razorpay Test Mode webhook for the reachable API URL
`/webhooks/razorpay`. Use the same webhook secret as
`RAZORPAY_WEBHOOK_SECRET` and subscribe to `payment.failed`,
`payment.captured`, and `order.paid`. For a local API, use an HTTPS tunnel
only for this test.

If webhook delivery is unavailable, the API verification fallback can be run
against the same Test Mode credentials. It uses only the documented read-only
payment and order resources and does not replace the webhook path.

## Run one customer retry flow

1. Create one initial Test Mode order through the normal merchant checkout or
   Test Mode setup. This is only test setup. REVIVE must not create a
   replacement order.
2. Open Razorpay Checkout with that order’s `order_id` and use the failure
   test credentials supplied by Razorpay. Wait for the signed
   `payment.failed` webhook.
3. Confirm the payment and case were persisted:

   ```sh
   curl http://localhost:8000/payments/<payment_id>
   curl http://localhost:8000/recovery-cases/recovery-<payment_id>
   ```

4. Run `POST /recovery-cases/<case_id>/process` from an application instance
   configured with the existing `RecoveryCaseProcessor` and its configured
   Redis/diagnosis provider. A successful simulated execution is only a
   decision/execution record; it is not payment success.
5. Request the customer-facing Checkout options:

   ```sh
   curl http://localhost:8000/recovery-cases/<case_id>/customer-checkout
   ```

   The response must contain the original `order_id`, the original amount and
   currency, and the `rzp_test_...` public key. It does not create an order or
   call a payment/retry API. Pass these options to the browser’s Razorpay
   Checkout and let the customer complete the payment.
6. Use Razorpay’s Test Mode success credentials. Wait for the signed
   `payment.captured` or `order.paid` webhook, then verify:

   ```sh
   curl http://localhost:8000/recovery-cases/<case_id>
   ```

   The case may become `recovered` only after that provider confirmation. A
   recovery decision or simulated execution alone must not mark revenue
   recovered. Existing HMAC verification, event deduplication, monotonic state
   handling, and PostgreSQL transactions remain in force throughout the flow.

## Verify through the Test Mode API fallback

When a signed webhook cannot be delivered, run this endpoint after the payment
and recovery case have been persisted:

```sh
curl -X POST http://localhost:8000/recovery-cases/<case_id>/verify-payment
```

REVIVE retrieves the existing `payment_id` and `order_id` from PostgreSQL,
then performs read-only `GET /v1/payments/<payment_id>` and
`GET /v1/orders/<order_id>` calls using Test Mode credentials. Only the exact
requested provider payment in `captured` status, with the full expected amount
and currency, advances that payment and recovery case and writes its one
recovered payment outcome. A provider `paid` order without that exact captured
payment only advances the order state; it never attributes recovery to a
different payment attempt. Failed, authorized, created, or refunded responses
do not create recovered revenue. Repeating the verification is safe and does
not duplicate the outcome or reconciliation audit record.

If the case has no persisted `recover` decision, or the payment/order is
already successful, the customer-checkout endpoint rejects the request.
