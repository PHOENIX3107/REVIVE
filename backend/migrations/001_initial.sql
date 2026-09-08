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

CREATE TABLE IF NOT EXISTS population_incidents (
    incident_id TEXT PRIMARY KEY,
    cohort_key TEXT NOT NULL,
    issuer_bin TEXT NOT NULL,
    error_code TEXT NOT NULL,
    status TEXT NOT NULL CHECK (status IN ('ACTIVE', 'EXPIRED', 'RESOLVED')),
    threshold INTEGER NOT NULL CHECK (threshold > 0),
    window_seconds INTEGER NOT NULL CHECK (window_seconds > 0),
    observed_count_at_activation INTEGER NOT NULL CHECK (observed_count_at_activation >= 0),
    trigger_payment_id TEXT NOT NULL REFERENCES payment_attempts(payment_id),
    activated_at TIMESTAMPTZ NOT NULL,
    last_qualifying_observed_at TIMESTAMPTZ NOT NULL,
    expires_at TIMESTAMPTZ NOT NULL,
    resolved_at TIMESTAMPTZ,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

-- Keep schema initialization safe for databases created by the earlier
-- lowercase-status draft of this table.
ALTER TABLE population_incidents
    DROP CONSTRAINT IF EXISTS population_incidents_status_check;

UPDATE population_incidents
SET status = upper(status)
WHERE status IN ('active', 'expired', 'resolved');

ALTER TABLE population_incidents
    ADD CONSTRAINT population_incidents_status_check
    CHECK (status IN ('ACTIVE', 'EXPIRED', 'RESOLVED'));

DROP INDEX IF EXISTS population_incidents_one_active_cohort;

CREATE UNIQUE INDEX IF NOT EXISTS population_incidents_one_active_cohort
    ON population_incidents(cohort_key) WHERE status = 'ACTIVE';

CREATE INDEX IF NOT EXISTS population_incidents_cohort_status
    ON population_incidents(cohort_key, status, expires_at);

-- Preserve the observed signal and incident context that existed when a
-- recovery case was processed. These fields are immutable evidence for the
-- dashboard; they must not be recomputed from today's active incidents.
ALTER TABLE recovery_cases
    ADD COLUMN IF NOT EXISTS population_signal_detected BOOLEAN NOT NULL DEFAULT FALSE,
    ADD COLUMN IF NOT EXISTS population_incident_active BOOLEAN NOT NULL DEFAULT FALSE,
    ADD COLUMN IF NOT EXISTS population_incident_id TEXT,
    ADD COLUMN IF NOT EXISTS population_incident_cohort_key TEXT,
    ADD COLUMN IF NOT EXISTS population_incident_activated_at TIMESTAMPTZ,
    ADD COLUMN IF NOT EXISTS population_incident_expires_at TIMESTAMPTZ;

CREATE INDEX IF NOT EXISTS recovery_cases_population_incident_id
    ON recovery_cases(population_incident_id);
