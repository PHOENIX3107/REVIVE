"""Typed response models for the read-only durable dashboard projections."""

from datetime import datetime
from typing import Any

from pydantic import BaseModel


class DashboardMetrics(BaseModel):
    payment_attempts: int
    failed_payments: int
    revenue_at_risk: int
    policy_eligible_revenue: int
    recovered_revenue: int
    recovery_actions: int
    blocked_actions: int
    duplicate_actions_prevented: int
    unsafe_actions: int
    systemic_attempts: int
    customer_attempts: int


class DashboardRecoveryRow(BaseModel):
    case_id: str
    payment_id: str
    order_id: str
    amount: int
    payment_status: str
    order_status: str
    case_status: str
    issuer_bin: str | None = None
    error_code: str | None = None
    diagnosis_category: str | None = None
    diagnosis_confidence: float | None = None
    diagnosis_reason: str | None = None
    decision: str | None = None
    decision_reason: str | None = None
    execution_status: str | None = None
    execution_action: str | None = None
    execution_reason: str | None = None
    execution_timestamp: datetime | None = None
    payment_outcome_status: str | None = None
    payment_outcome_amount: int | None = None
    payment_outcome_observed_at: datetime | None = None
    population_signal_detected: bool = False
    population_incident_active: bool = False
    population_incident_id: str | None = None
    population_incident_cohort_key: str | None = None
    population_incident_activated_at: datetime | None = None
    population_incident_expires_at: datetime | None = None


class DashboardOverviewResponse(BaseModel):
    metrics: DashboardMetrics
    recent_cases: list[DashboardRecoveryRow]


class DashboardSignalRow(BaseModel):
    issuer_bin: str
    error_code: str | None = None
    failure_count: int
    total_amount: int
    latest_failed_at: datetime | None = None


class DashboardDowntimeResponse(BaseModel):
    available: bool
    items: list[dict[str, Any]]


class DashboardPopulationIncidentRow(BaseModel):
    incident_id: str
    cohort_key: str
    issuer_bin: str
    error_code: str
    status: str
    threshold: int
    window_seconds: int
    observed_count_at_activation: int
    trigger_payment_id: str
    activated_at: datetime
    last_qualifying_observed_at: datetime
    expires_at: datetime


class DashboardDecisionRow(BaseModel):
    decision_id: int
    case_id: str
    payment_id: str
    order_id: str
    amount: int
    payment_status: str
    order_status: str
    diagnosis_category: str | None = None
    diagnosis_confidence: float | None = None
    diagnosis_reason: str | None = None
    decision: str
    decision_reason: str
    decision_created_at: datetime


class DashboardAuditRow(BaseModel):
    audit_id: int
    case_id: str
    payment_id: str
    order_id: str
    amount: int
    payment_status: str
    order_status: str
    policy_decision: str
    action: str | None = None
    status: str
    reason: str
    timestamp: datetime
