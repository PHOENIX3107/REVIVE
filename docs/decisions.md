# REVIVE architecture decisions

## Durable state is authoritative

PostgreSQL is the source of truth for orders, payment attempts, recovery cases,
diagnoses, decisions, executions, audit events, webhook deduplication, and
provider-confirmed payment outcomes. The dashboard is a read-only projection of
that state and does not run recovery logic.

## Decision, execution, and payment outcome stay separate

The deterministic policy decides whether a recovery action is allowed. The
current executor is simulated and records only an execution result. Neither a
policy decision nor execution success creates a payment outcome or recovered
revenue. Recovered revenue requires provider-confirmed success.

## Existing Razorpay orders are preserved

REVIVE uses the persisted `order_id` throughout the recovery flow. The customer
checkout boundary presents that existing order to Razorpay Checkout; REVIVE does
not create replacement orders or expose an invented server-side retry API.

## Provider confirmation is polling-first

The payment reconciliation worker polls unresolved PostgreSQL recovery cases and
uses read-only Test Mode Payment/Order API verification as the primary
operational path. The API endpoint performs the same verification for a single
case. Signed `payment.captured` and `order.paid` events remain an optional
compatibility/fast-path path. Every path validates the exact payment, order,
amount, and currency before persisting a recovered outcome. Webhook
deduplication, monotonic state handling, and transaction boundaries are
preserved.

## Safe local defaults are explicit

The default diagnosis implementation is deterministic and local so an external
LLM is not required to start the application. No downtime records are fabricated
when a persisted downtime source is unavailable. Synthetic generation is kept
inside evaluation code and is not used by the API or frontend.

## Evaluation metrics do not imply revenue

Batch evaluation reports attempts, amount at risk, policy eligibility, decisions,
and simulated executions separately. Without explicit provider-confirmed
outcomes, evaluated recovered revenue is zero.
