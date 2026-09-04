# REVIVE

Population-aware, safety-first payment recovery for Razorpay.

REVIVE is a payment recovery agent built for Razorpay AI Buildathon 2026 — Track 03: AI Revenue Recovery.

The system detects population-level payment failure patterns, correlates them with downtime context, uses AI for structured diagnosis, and applies deterministic safety policies before recovery is permitted.

## Architecture

Razorpay → Webhook ingestion → State normalization → Population signal detection → Downtime correlation → AI diagnosis → Deterministic policy → Bounded executor → Audit and evaluation

## Safety boundary

**Decision ≠ Execution ≠ Payment Outcome**

A recovery decision or simulated execution is not counted as recovered revenue. Revenue is considered recovered only after provider confirmation through the payment lifecycle/webhook.

## Razorpay integration

The integration is restricted to Test Mode. REVIVE uses the existing Razorpay order_id and does not create replacement orders.

The system handles payment.failed, payment.captured, order.paid, webhook deduplication, out-of-order delivery, monotonic state transitions, and HMAC webhook signature verification.

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
docker compose up -d postgres redis
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
export RAZORPAY_WEBHOOK_SECRET="..."
```

Start the API:

```bash
uvicorn backend.main:app --reload
```

Health check:

```bash
curl http://localhost:8000/health
```

## Tests

```bash
pytest
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
