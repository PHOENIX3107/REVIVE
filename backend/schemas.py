from datetime import datetime
from enum import Enum

from pydantic import BaseModel, ConfigDict, Field


class OrderStatus(str, Enum):
    created = "created"
    attempted = "attempted"
    paid = "paid"


class PaymentStatus(str, Enum):
    authorized = "authorized"
    captured = "captured"
    failed = "failed"


class PaymentMethod(str, Enum):
    card = "card"


class PaymentError(BaseModel):
    code: str
    description: str | None = None
    field: str | None = None
    source: str | None = None
    step: str | None = None
    reason: str | None = None
    metadata: dict[str, str] = Field(default_factory=dict)


class Order(BaseModel):
    order_id: str
    amount: int
    amount_paid: int
    amount_due: int
    currency: str
    status: OrderStatus
    attempts: int
    created_at: datetime


class PaymentAttempt(BaseModel):
    payment_id: str
    order_id: str
    amount: int
    method: PaymentMethod
    status: PaymentStatus
    captured: bool
    created_at: datetime
    error: PaymentError | None = None
    issuer_bin: str
    failed_at: datetime | None = None


class Downtime(BaseModel):
    downtime_id: str
    entity: str
    method: str
    begin: datetime
    end: datetime | None = None
    status: str
    scheduled: bool
    severity: str
    instrument: str | None = None
    card_type: str | None = None


class SignalContext(BaseModel):
    is_cluster_candidate: bool
    matching_failure_count: int
    window_seconds: int


class PopulationIncidentContext(BaseModel):
    """Immutable durable population-incident evidence for one case."""

    model_config = ConfigDict(frozen=True)

    population_incident_active: bool = False
    population_incident_id: str | None = None
    population_incident_activated_at: datetime | None = None
    population_incident_expires_at: datetime | None = None
    cohort_key: str | None = None
