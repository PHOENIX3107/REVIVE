# REVIVE

Population-aware, safety-first payment recovery for Razorpay.

REVIVE distinguishes individual customer payment failures from systemic
failures affecting a population of payments. It uses AI for structured
diagnosis, deterministic policy for authority, bounded execution, and
provider-confirmed state for revenue accounting.

## Problem

Payment failures are not all individual customer failures. Repeating recovery
actions during an issuer or network incident can amplify the failure, create
duplicate work, and produce misleading revenue metrics.

## Solution

```text
Observed payment.failed
        ↓
Population signal (issuer_bin + error_code)
        ↓ 5 unique observations / 600 seconds
Durable ACTIVE population incident
        ↓
AI diagnosis (advisory evidence)
        ↓
Deterministic policy (authority)
        ↓
Simulated recovery execution or systemic cooldown
        ↓
Razorpay webhook / read-only reconciliation
        ↓
Provider-confirmed outcome
        ↓
Revenue metrics
```

The critical boundary is:

```text
DECISION ≠ EXECUTION ≠ PAYMENT OUTCOME
```

A recovery decision or simulated execution never counts as recovered revenue.
Only an exact provider-confirmed captured payment with a matching paid order,
amount, currency, and payment identity creates recovered revenue.

## Why REVIVE

- Population-aware recovery instead of blind retries.
- Durable incident activation after five unique observed failures in 600 seconds.
- AI diagnosis inside a deterministic safety boundary.
- PostgreSQL as the durable source of truth, with Redis for sliding-window and
  idempotency acceleration.
- Webhook deduplication, monotonic state handling, advisory locking, and
  webhook-independent provider reconciliation.
- Honest revenue reporting: simulated execution is not payment success.

## Architecture

```text
Razorpay
   ↓ payment.failed / payment.captured / order.paid
FastAPI ingestion + signature validation
   ├── PostgreSQL durable state
   └── Redis observation window / idempotency
              ↓
       Population signal
              ↓
       Population incident
              ↓
       AI diagnosis
              ↓
       Deterministic policy
              ↓
       Recovery executor
              ↓
       Razorpay read-only reconciliation
              ↓
       Provider-confirmed outcome
              ↓
       Dashboard + audit + evaluation
```

The dependency-light operations UI is served by `frontend/app.py` and reads
only the PostgreSQL-backed dashboard API. It does not seed synthetic data in
the browser.

## Population intelligence

The current cohort is:

```text
cohort_key = issuer_bin + error_code
window     = 600 seconds
threshold  = 5 unique payment IDs
```

Each failed payment becomes observed at ingestion time. Incident activation is
sequential and temporal: earlier cases cannot see later failures. The durable
case record preserves whether the systemic signal and incident context were
active at the case decision time; the dashboard does not recompute historical
context from today’s incidents.

`observed_at` is established by the ingestion process. A provider-supplied
failure timestamp is retained as payment data but is not used to make an
earlier decision see the future.

## Safety and provider honesty

- Systemic incidents produce cooldown decisions to prevent retry amplification.
- Redis idempotency and PostgreSQL uniqueness protect duplicate processing.
- PostgreSQL advisory locks serialize concurrent incident activation and
  reconciliation work.
- Signed webhooks are verified over the exact raw request body.
- Reconciliation validates provider payment/order identity, amount, currency,
  captured status, and paid-order state.
- REVIVE preserves the existing Razorpay `order_id`; it does not create
  replacement orders or invent a generic server-side retry API.
- The current recovery executor records a bounded simulated action. It does not
  collect money from Razorpay.
- The only recovered-revenue source is provider-confirmed payment state.

## Demo scenarios

The deterministic runner uses the existing ingestion, incident, recovery, and
reconciliation components. It requires an explicit reset before clearing local
demo state:

```bash
REVIVE_TEST_DATABASE_URL="postgresql://revive:replace_with_local_password@localhost:5432/revive" \
REDIS_URL="redis://localhost:6379/0" \
  ./.venv/bin/python -m scripts.demo_scenarios --reset
```

It prints the full chain for three scenarios:

### A — Customer-side failure

```text
insufficient_funds
→ no population incident
→ customer-side diagnosis
→ recover policy
→ simulated execution
→ no provider outcome
→ INR 0 recovered
```

### B — Systemic failure

```text
five unique issuer_declined failures
→ observed cohort threshold
→ ACTIVE population incident
→ systemic signal
→ systemic diagnosis
→ cooldown
→ no recovery execution
→ INR 0 recovered
```

### C — Confirmed recovery

The runner uses a local HTTP fixture through the existing read-only Razorpay
client and reconciler. It demonstrates captured payment + paid order
confirmation without calling a retry endpoint or creating an order:

```text
failed payment
→ recover decision
→ simulated execution record
→ provider captured / paid response
→ reconciliation
→ recovered outcome
→ recovered revenue
```

## Local setup

```bash
cp .env.example .env
# Replace only local placeholders in .env; never commit this file.
docker compose --env-file .env up -d postgres redis
uv sync
source .venv/bin/activate
uvicorn backend.main:app --reload
```

Run the UI in a second terminal:

```bash
REVIVE_API_BASE_URL="http://127.0.0.1:8000" \
REVIVE_FRONTEND_PORT="3000" ./.venv/bin/python frontend/app.py
```

Open <http://127.0.0.1:3000>. If the API or database is unavailable, the UI
shows an explicit unavailable state and does not fabricate dashboard values.

## Environment variables

`.env.example` contains placeholders only. Important names:

```text
POSTGRES_USER
POSTGRES_PASSWORD
POSTGRES_DB
DATABASE_URL
REDIS_URL
REVIVE_API_BASE_URL
REVIVE_FRONTEND_PORT
REVIVE_RECONCILIATION_POLL_INTERVAL_SECONDS
REVIVE_RECONCILIATION_BATCH_SIZE
REVIVE_RECONCILIATION_RETRY_ATTEMPTS
REVIVE_RECONCILIATION_RETRY_BACKOFF_SECONDS
REVIVE_RECONCILIATION_RETRY_MAX_BACKOFF_SECONDS
RAZORPAY_MODE
RAZORPAY_KEY_ID
RAZORPAY_KEY_SECRET
RAZORPAY_WEBHOOK_SECRET
AI_PROVIDER
AI_API_KEY
```

`RAZORPAY_MODE` defaults safely to `test`; live mode requires explicit
`RAZORPAY_MODE=live` and a matching `rzp_live_...` key. Credentials are read
from the environment, never logged, and never returned in API responses. Any
previously exposed provider credential must be revoked and regenerated before
use.

## Dashboard projections

The read-only API exposes:

```text
GET /dashboard/overview
GET /dashboard/recoveries
GET /dashboard/incidents
GET /dashboard/signals
GET /dashboard/downtime
GET /dashboard/decisions
GET /dashboard/audit
```

The dashboard separates:

```text
AI diagnosis
→ deterministic policy decision
→ simulated recovery execution
→ provider-confirmed outcome
```

Active incidents show cohort, issuer BIN, error code, activation threshold,
window, observed count at activation, activation/expiry times, trigger payment,
and the systemic cooldown response. Recovery case history retains incident and
signal context as observed at decision time.

## Evaluation

The deterministic batch evaluator processes failures sequentially and models
the same observed-time incident lifecycle. It does not pre-seed future
failures, use `ground_truth_systemic` to create an observed signal, or count
simulated executions as recovered revenue.

Verified batch result:

| Metric | Result |
| --- | ---: |
| Attempts | 100 |
| Revenue at risk | INR 15,774 |
| Eligible revenue | INR 5,990 |
| Recovered revenue | INR 0 |
| Recovery actions | 10 |
| Population TP | 8 |
| Population FP | 0 |
| Population FN | 8 |
| Precision | 100% |
| Recall | 50% |
| Unsafe actions | 0 |

The 50% recall is intentional. Under sequential observed-time activation,
the first four failures in each synthetic systemic cluster cannot see the
future fifth failure, so they remain false negatives relative to evaluator
truth. This is a temporal measurement, not a production shortcut.

Run it with:

```bash
./.venv/bin/python -m backend.evaluation.run_batch
```

## Testing

With PostgreSQL and Redis available:

```bash
export REVIVE_TEST_DATABASE_URL="postgresql://revive:replace_with_local_password@localhost:5432/revive"
./.venv/bin/pytest -q
./.venv/bin/python -m compileall backend frontend tests scripts
git diff --check
./.venv/bin/python -m backend.evaluation.run_batch
```

The final PostgreSQL-inclusive suite reports 185 passed tests with 89 known
warnings after productization changes. The warnings are framework/test-client
deprecations; no payment or incident test failed.

## Limitations

- The current recovery executor is simulated/instructional, not an autonomous
  provider collection mechanism.
- Razorpay live operation is configuration-gated and has not been externally
  verified by this repository.
- Population cohorts currently use only issuer BIN and error code.
- Incident activation is observed-time and intentionally cannot recover cases
  that happened before the fifth qualifying observation.
- Downtime data remains unavailable unless an explicit persisted source is
  configured.
- The FastAPI startup event API may emit a deprecation warning under newer
  framework versions; it is retained to avoid an unnecessary framework
  migration.

## Documentation

- `docs/decisions.md` — engineering and policy decisions.
- `docs/razorpay-test-mode.md` — provider Test Mode and reconciliation flow.
- `architecture/architecture.png` — system architecture reference.

## Principles

1. Razorpay-grounded
2. Population-aware
3. AI-bounded
4. Deterministic safety
5. Idempotent processing
6. Auditable decisions
7. Measurable outcomes
8. Narrow scope
