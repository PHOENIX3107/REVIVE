CREATE TABLE IF NOT EXISTS orders (
    order_id TEXT PRIMARY KEY,
    amount BIGINT NOT NULL CHECK (amount >= 0),
    amount_paid BIGINT NOT NULL CHECK (amount_paid >= 0),
    amount_due BIGINT NOT NULL CHECK (amount_due >= 0),
    currency CHAR(3) NOT NULL,
    status TEXT NOT NULL CHECK (status IN ('created', 'attempted', 'paid')),
    attempts INTEGER NOT NULL CHECK (attempts >= 0),
    created_at TIMESTAMPTZ NOT NULL
);

CREATE TABLE IF NOT EXISTS payment_attempts (
    payment_id TEXT PRIMARY KEY,
    order_id TEXT NOT NULL REFERENCES orders(order_id),
    amount BIGINT NOT NULL CHECK (amount >= 0),
    method TEXT NOT NULL,
    status TEXT NOT NULL CHECK (status IN ('authorized', 'captured', 'failed')),
    captured BOOLEAN NOT NULL,
    created_at TIMESTAMPTZ NOT NULL,
    error JSONB,
    issuer_bin TEXT NOT NULL,
    failed_at TIMESTAMPTZ
);

CREATE TABLE IF NOT EXISTS webhook_events (
    event_id TEXT PRIMARY KEY,
    event_type TEXT NOT NULL,
    payload JSONB NOT NULL,
    occurred_at TIMESTAMPTZ,
    received_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS recovery_cases (
    case_id TEXT PRIMARY KEY,
    payment_id TEXT NOT NULL UNIQUE REFERENCES payment_attempts(payment_id),
    order_id TEXT NOT NULL REFERENCES orders(order_id),
    status TEXT NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS diagnoses (
    diagnosis_id BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    case_id TEXT NOT NULL REFERENCES recovery_cases(case_id),
    category TEXT NOT NULL CHECK (category IN ('customer_issue', 'systemic_issue', 'unknown')),
    confidence NUMERIC(5, 4) NOT NULL CHECK (confidence >= 0 AND confidence <= 1),
    reason TEXT NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS decisions (
    decision_id BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    case_id TEXT NOT NULL REFERENCES recovery_cases(case_id),
    decision TEXT NOT NULL CHECK (decision IN ('stop', 'cooldown', 'recover', 'review')),
    reason TEXT NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS executions (
    execution_id BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    case_id TEXT NOT NULL REFERENCES recovery_cases(case_id),
    idempotency_key TEXT NOT NULL UNIQUE,
    executed BOOLEAN NOT NULL,
    action TEXT,
    status TEXT NOT NULL,
    reason TEXT NOT NULL,
    timestamp TIMESTAMPTZ NOT NULL
);

CREATE TABLE IF NOT EXISTS audit_events (
    audit_id BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    case_id TEXT NOT NULL REFERENCES recovery_cases(case_id),
    payment_id TEXT NOT NULL REFERENCES payment_attempts(payment_id),
    order_id TEXT NOT NULL REFERENCES orders(order_id),
    policy_decision TEXT NOT NULL,
    action TEXT,
    status TEXT NOT NULL,
    reason TEXT NOT NULL,
    timestamp TIMESTAMPTZ NOT NULL
);

CREATE TABLE IF NOT EXISTS payment_outcomes (
    payment_id TEXT PRIMARY KEY REFERENCES payment_attempts(payment_id),
    status TEXT NOT NULL,
    amount_recovered BIGINT NOT NULL CHECK (amount_recovered >= 0),
    observed_at TIMESTAMPTZ NOT NULL
);
