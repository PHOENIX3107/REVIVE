# REVIVE

Population-aware, safety-first payment recovery for Razorpay.

REVIVE is a payment recovery agent built for Razorpay AI Buildathon 2026 — Track 03: AI Revenue Recovery.

The system detects population-level payment failure patterns, correlates them with downtime context, uses AI for structured diagnosis, and applies deterministic safety policies before recovery is permitted.

## Architecture

Razorpay API polling → State normalization → Population signal detection → Downtime correlation → AI diagnosis → Deterministic policy → Bounded executor → Audit and evaluation

Signed Razorpay webhooks remain an optional compatibility/fast-path ingestion
mechanism; recovery reconciliation does not depend on webhook delivery.

## Safety boundary

**Decision ≠ Execution ≠ Payment Outcome**

A recovery decision or simulated execution is not counted as recovered revenue. Revenue is considered recovered only after provider confirmation through the payment lifecycle/webhook.

The default diagnosis provider is deterministic and local. The recovery executor
is intentionally simulated: it records a bounded action but never retries or
collects a Razorpay payment. The synthetic generator and batch evaluator are
evaluation-only. Downtime is reported as unavailable until a real persisted
source is added.

## Razorpay integration

The integration is restricted to Test Mode. REVIVE uses the existing Razorpay order_id and does not create replacement orders.

The polling worker discovers unresolved recovery cases from PostgreSQL and uses
read-only Razorpay Payment/Order API verification. The system also supports
`payment.failed`, `payment.captured`, and `order.paid` webhooks as an optional
compatibility path with deduplication, out-of-order delivery handling,
monotonic state transitions, and HMAC signature verification.

## Tech stack

- Python
- FastAPI
- PostgreSQL
- Redis
- Pydantic
- Razorpay Test Mode
- pytest
- Docker Compose

## Run locally

Start PostgreSQL and Redis:

```bash
cp .env.example .env
# Replace local placeholders in .env; never commit this file.
docker compose --env-file .env up -d postgres redis
```

Install dependencies:

```bash
uv sync
source .venv/bin/activate
```

Configure environment variables:

```bash
export DATABASE_URL="postgresql://revive:revive@localhost:5432/revive"
export REDIS_URL="redis://localhost:6379/0"
export RAZORPAY_KEY_ID="rzp_test_..."
export RAZORPAY_KEY_SECRET="..."
# Optional: only needed when enabling the webhook compatibility path.
export RAZORPAY_WEBHOOK_SECRET="..."
export REVIVE_API_BASE_URL="http://127.0.0.1:8000"
export REVIVE_FRONTEND_PORT="3000"
export REVIVE_RECONCILIATION_POLL_INTERVAL_SECONDS="60"
export REVIVE_RECONCILIATION_BATCH_SIZE="50"
```

Start the API:

```bash
uvicorn backend.main:app --reload
```

Start the primary reconciliation worker in another terminal. It does not
require `RAZORPAY_WEBHOOK_SECRET`:

```bash
./.venv/bin/python -m backend.workers.payment_reconciliation
```

The worker polls unresolved PostgreSQL recovery cases, verifies each existing
Razorpay payment/order pair, and relies on the existing reconciler to persist
provider-confirmed outcomes. It handles transient failures with bounded retry
and backoff and shuts down cleanly on SIGTERM/SIGINT.

Health check:

```bash
curl http://localhost:8000/health
```

The read-only dashboard projection is available at:

```text
GET /dashboard/overview
GET /dashboard/recoveries
GET /dashboard/signals
GET /dashboard/downtime
GET /dashboard/decisions
GET /dashboard/audit
```

Run the dependency-light operations UI in a second terminal:

```bash
REVIVE_API_BASE_URL="http://127.0.0.1:8000" \
  REVIVE_FRONTEND_PORT="3000" ./.venv/bin/python frontend/app.py
```

The UI reads only these PostgreSQL-backed APIs. If the API or database is
unavailable, it shows an explicit unavailable state; it does not generate
synthetic dashboard data.

## What a real recovery means

The customer checkout boundary preserves the original Razorpay `order_id` and
returns options for Razorpay Checkout. It does not create a replacement order
or invent a retry API. Successful recovery is recorded only after a signed
`payment.captured`/`order.paid` lifecycle event, or the documented read-only
Test Mode verification fallback confirms the exact payment and amount. A
decision or simulated execution alone cannot create a payment outcome or
recovered revenue. See `docs/razorpay-test-mode.md` for the manual Test Mode
procedure.

## Tests

```bash
REVIVE_TEST_DATABASE_URL="postgresql://revive:revive@localhost:5432/revive" \
  ./.venv/bin/pytest -q
```

For the complete local validation pass:

```bash
./.venv/bin/python -m compileall backend tests
git diff --check
```

## Documentation

- `docs/decisions.md` — engineering and policy decisions
- `docs/razorpay-test-mode.md` — Test Mode integration procedure
- `architecture/architecture.png` — system architecture

## Project principles

1. Razorpay-grounded
2. Population-aware
3. AI-bounded
4. Deterministic safety
5. Idempotent processing
6. Auditable decisions
7. Measurable outcomes
8. Narrow scope
