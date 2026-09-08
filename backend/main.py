"""FastAPI application entrypoint for the REVIVE backend."""

from collections.abc import Callable, Iterable
import os
from datetime import datetime
from typing import Any

from fastapi import Depends, FastAPI, HTTPException, Request, status
from pydantic import BaseModel
import redis

from backend.agents.recovery_agent import RecoveryAgent, local_diagnosis_provider
from backend.cache import RedisFailureCache, RedisIdempotencyCache
from backend.dashboard import (
    DashboardAuditRow,
    DashboardDecisionRow,
    DashboardDowntimeResponse,
    DashboardMetrics,
    DashboardOverviewResponse,
    DashboardPopulationIncidentRow,
    DashboardRecoveryRow,
    DashboardSignalRow,
)
from backend.db import Database
from backend.integrations.razorpay.client import (
    RazorpayCheckoutOptions,
    RazorpayAPIError,
    RazorpayClientConfig,
    build_checkout_options,
)
from backend.integrations.razorpay.signature import (
    RazorpayWebhookSignatureError,
    verify_webhook_signature,
)
from backend.pipeline.signal_detector import SignalDetector
from backend.population_incidents import PopulationIncidentActivator
from backend.recovery.case_processor import (
    RecoveryCaseNotFoundError,
    RecoveryCaseProcessor,
    RecoveryCaseProcessingResult,
    RecoveryCaseStateError,
)
from backend.recovery.executor import RecoveryExecutor
from backend.recovery.reconciliation import (
    PaymentVerificationCaseNotFoundError,
    PaymentVerificationError,
    PaymentVerificationResult,
    PaymentVerificationStateError,
    RazorpayPaymentReconciler,
)
from backend.policies.recovery_policy import PolicyDecisionType
from backend.schemas import Downtime, Order, OrderStatus, PaymentAttempt, PaymentStatus
from backend.webhooks.razorpay import (
    RazorpayWebhookAdapter,
    RazorpayWebhookParseError,
    RazorpayWebhookProcessor,
    WebhookIngestResult,
)


class HealthResponse(BaseModel):
    status: str = "ok"


class RecoveryCaseResponse(BaseModel):
    case_id: str
    payment_id: str
    order_id: str
    status: str
    created_at: datetime
    updated_at: datetime


class CustomerCheckoutResponse(BaseModel):
    case_id: str
    payment_id: str
    order_id: str
    test_mode: bool
    checkout: RazorpayCheckoutOptions


def create_app(
    database: Database | None = None,
    recovery_processor: RecoveryCaseProcessor | None = None,
    payment_reconciler: RazorpayPaymentReconciler | None = None,
    downtimes: Iterable[Downtime] = (),
    population_incident_activator: PopulationIncidentActivator | None = None,
) -> FastAPI:
    app = FastAPI(title="REVIVE", version="0.1.0")
    app.state.database = database
    app.state.recovery_processor = recovery_processor
    app.state.payment_reconciler = payment_reconciler
    app.state.downtimes = tuple(downtimes)
    app.state.population_incident_activator = population_incident_activator

    @app.on_event("startup")
    def _initialize_schema() -> None:
        configured_database = getattr(app.state, "database", None)
        if configured_database is None:
            database_url = os.getenv("DATABASE_URL")
            if not database_url:
                return
            configured_database = Database(database_url)
            app.state.database = configured_database

        configured_database.initialize()
        redis_client = None
        if (
            getattr(app.state, "population_incident_activator", None) is None
            or getattr(app.state, "recovery_processor", None) is None
        ):
            redis_client = redis.Redis.from_url(
                os.getenv("REDIS_URL", "redis://localhost:6379/0"),
                decode_responses=True,
            )
        if getattr(app.state, "population_incident_activator", None) is None:
            app.state.population_incident_activator = PopulationIncidentActivator(
                configured_database,
                redis_client,
            )
        if getattr(app.state, "recovery_processor", None) is None:
            app.state.recovery_processor = build_recovery_case_processor(
                configured_database,
                redis_client=redis_client,
                population_incident_activator=app.state.population_incident_activator,
                downtimes=app.state.downtimes,
            )

    @app.get("/health", response_model=HealthResponse)
    def health(database: Database = Depends(get_database)) -> HealthResponse:
        try:
            database.ping()
        except Exception as exc:
            raise HTTPException(
                status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
                detail="Database health check failed.",
            ) from exc
        return HealthResponse()

    @app.get("/dashboard/overview", response_model=DashboardOverviewResponse)
    def dashboard_overview(database: Database = Depends(get_database)) -> DashboardOverviewResponse:
        return DashboardOverviewResponse(
            metrics=DashboardMetrics(**database.get_dashboard_metrics()),
            recent_cases=[
                DashboardRecoveryRow.model_validate(row)
                for row in database.get_dashboard_recoveries(limit=10)
            ],
        )

    @app.get("/dashboard/recoveries", response_model=list[DashboardRecoveryRow])
    def dashboard_recoveries(
        database: Database = Depends(get_database),
    ) -> list[DashboardRecoveryRow]:
        return [
            DashboardRecoveryRow.model_validate(row)
            for row in database.get_dashboard_recoveries()
        ]

    @app.get(
        "/dashboard/incidents",
        response_model=list[DashboardPopulationIncidentRow],
    )
    def dashboard_incidents(
        database: Database = Depends(get_database),
    ) -> list[DashboardPopulationIncidentRow]:
        return [
            DashboardPopulationIncidentRow.model_validate(row)
            for row in database.get_dashboard_population_incidents()
        ]

    @app.get("/dashboard/signals", response_model=list[DashboardSignalRow])
    def dashboard_signals(
        database: Database = Depends(get_database),
    ) -> list[DashboardSignalRow]:
        return [DashboardSignalRow.model_validate(row) for row in database.get_dashboard_signals()]

    @app.get("/dashboard/downtime", response_model=DashboardDowntimeResponse)
    def dashboard_downtime() -> DashboardDowntimeResponse:
        return DashboardDowntimeResponse(available=False, items=[])

    @app.get("/dashboard/decisions", response_model=list[DashboardDecisionRow])
    def dashboard_decisions(
        database: Database = Depends(get_database),
    ) -> list[DashboardDecisionRow]:
        return [
            DashboardDecisionRow.model_validate(row)
            for row in database.get_dashboard_decisions()
        ]

    @app.get("/dashboard/audit", response_model=list[DashboardAuditRow])
    def dashboard_audit(
        database: Database = Depends(get_database),
    ) -> list[DashboardAuditRow]:
        return [DashboardAuditRow.model_validate(row) for row in database.get_dashboard_audit()]

    @app.get("/orders/{order_id}", response_model=Order)
    def get_order(order_id: str, database: Database = Depends(get_database)) -> Order:
        order = database.get_order(order_id)
        if order is None:
            raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Order not found.")
        return order

    @app.get("/payments/{payment_id}", response_model=PaymentAttempt)
    def get_payment(payment_id: str, database: Database = Depends(get_database)) -> PaymentAttempt:
        payment = database.get_payment_attempt(payment_id)
        if payment is None:
            raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Payment not found.")
        return payment

    @app.get("/recovery-cases/{case_id}", response_model=RecoveryCaseResponse)
    def get_recovery_case(
        case_id: str,
        database: Database = Depends(get_database),
    ) -> RecoveryCaseResponse:
        recovery_case = database.get_recovery_case(case_id)
        if recovery_case is None:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail="Recovery case not found.",
            )
        return RecoveryCaseResponse.model_validate(recovery_case)

    @app.get(
        "/recovery-cases/{case_id}/customer-checkout",
        response_model=CustomerCheckoutResponse,
    )
    def customer_checkout_options(
        case_id: str,
        database: Database = Depends(get_database),
    ) -> CustomerCheckoutResponse:
        recovery_case = database.get_recovery_case(case_id)
        if recovery_case is None:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail="Recovery case not found.",
            )

        payment_id = recovery_case["payment_id"]
        order_id = recovery_case["order_id"]
        order = database.get_order(order_id)
        payment = database.get_payment_attempt(payment_id)
        if (
            order is None
            or payment is None
            or payment.order_id != order_id
            or payment.order_id != order.order_id
        ):
            raise HTTPException(
                status_code=status.HTTP_409_CONFLICT,
                detail="Recovery case is missing matching payment or order state.",
            )

        decision = database.get_recovery_decision(case_id)
        if decision is None or decision.decision is not PolicyDecisionType.recover:
            raise HTTPException(
                status_code=status.HTTP_409_CONFLICT,
                detail="Customer checkout is not permitted by the persisted recovery decision.",
            )
        if payment.status is not PaymentStatus.failed or order.status is OrderStatus.paid:
            raise HTTPException(
                status_code=status.HTTP_409_CONFLICT,
                detail="Customer checkout is not available for the current payment state.",
            )

        try:
            config = RazorpayClientConfig.from_env()
            checkout = build_checkout_options(
                config,
                order_id=order.order_id,
                amount=order.amount,
                currency=order.currency,
            )
        except ValueError as exc:
            raise HTTPException(
                status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
                detail="Razorpay credentials are not configured or mode/key configuration is invalid.",
            ) from exc

        return CustomerCheckoutResponse(
            case_id=case_id,
            payment_id=payment.payment_id,
            order_id=order.order_id,
            test_mode=config.mode == "test",
            checkout=checkout,
        )

    @app.post(
        "/recovery-cases/{case_id}/verify-payment",
        response_model=PaymentVerificationResult,
    )
    def verify_recovery_payment(
        case_id: str,
        reconciler: RazorpayPaymentReconciler = Depends(get_payment_reconciler),
    ) -> PaymentVerificationResult:
        try:
            return reconciler.verify_case(case_id)
        except PaymentVerificationCaseNotFoundError as exc:
            raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail=str(exc)) from exc
        except PaymentVerificationStateError as exc:
            raise HTTPException(status_code=status.HTTP_409_CONFLICT, detail=str(exc)) from exc
        except PaymentVerificationError as exc:
            raise HTTPException(
                status_code=status.HTTP_502_BAD_GATEWAY,
                detail="Razorpay payment verification returned invalid data.",
            ) from exc
        except RazorpayAPIError as exc:
            raise HTTPException(
                status_code=status.HTTP_502_BAD_GATEWAY,
                detail="Razorpay payment verification failed.",
            ) from exc
        except ValueError as exc:
            raise HTTPException(
                status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
                detail="Razorpay credentials are not configured or mode/key configuration is invalid.",
            ) from exc

    @app.post(
        "/recovery-cases/{case_id}/process",
        response_model=RecoveryCaseProcessingResult,
    )
    def process_recovery_case(
        case_id: str,
        processor: RecoveryCaseProcessor = Depends(get_recovery_case_processor),
    ) -> RecoveryCaseProcessingResult:
        try:
            return processor.process(case_id)
        except RecoveryCaseNotFoundError as exc:
            raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail=str(exc)) from exc
        except RecoveryCaseStateError as exc:
            raise HTTPException(status_code=status.HTTP_409_CONFLICT, detail=str(exc)) from exc

    @app.post("/webhooks/razorpay", response_model=WebhookIngestResult)
    async def razorpay_webhook(
        request: Request,
        processor: RazorpayWebhookProcessor = Depends(get_razorpay_processor),
    ) -> WebhookIngestResult:
        raw_body = await request.body()
        webhook_secret = os.getenv("RAZORPAY_WEBHOOK_SECRET")
        if not webhook_secret:
            raise HTTPException(
                status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
                detail="RAZORPAY_WEBHOOK_SECRET is not configured.",
            )

        try:
            verify_webhook_signature(
                raw_body,
                request.headers.get("X-Razorpay-Signature", ""),
                webhook_secret,
            )
        except RazorpayWebhookSignatureError as exc:
            raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail=str(exc)) from exc

        try:
            return processor.ingest(raw_body, request.headers)
        except RazorpayWebhookParseError as exc:
            raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail=str(exc)) from exc

    return app


def build_recovery_case_processor(
    database: Database,
    *,
    redis_client: Any | None = None,
    diagnosis_provider: Callable[[str], Any] | None = None,
    downtimes: Iterable[Downtime] = (),
    population_incident_activator: PopulationIncidentActivator | None = None,
) -> RecoveryCaseProcessor:
    """Compose the durable recovery pipeline for the default application.

    Redis is created from ``REDIS_URL`` only when a client is not supplied.  A
    local diagnosis provider is used by default; callers can inject a real
    provider through the existing ``RecoveryAgent`` boundary.  Downtime data is
    intentionally an explicit input and defaults to an empty collection because
    REVIVE has no downtime source yet.
    """
    if redis_client is None:
        redis_client = redis.Redis.from_url(
            os.getenv("REDIS_URL", "redis://localhost:6379/0"),
            decode_responses=True,
        )
    if population_incident_activator is None:
        population_incident_activator = PopulationIncidentActivator(
            database,
            redis_client,
        )

    return RecoveryCaseProcessor(
        database=database,
        signal_detector=SignalDetector(RedisFailureCache(redis_client)),
        recovery_agent=RecoveryAgent(diagnosis_provider or local_diagnosis_provider),
        recovery_executor=RecoveryExecutor(RedisIdempotencyCache(redis_client)),
        downtimes=downtimes,
        population_incident_activator=population_incident_activator,
    )


def get_database(request: Request) -> Database:
    configured_database = getattr(request.app.state, "database", None)
    if configured_database is not None:
        return configured_database

    try:
        return Database()
    except ValueError as exc:
        raise HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            detail="Database is not configured.",
        ) from exc


def get_razorpay_processor(
    request: Request,
    database: Database = Depends(get_database),
) -> RazorpayWebhookProcessor:
    return RazorpayWebhookProcessor(
        database=database,
        adapter=RazorpayWebhookAdapter(),
        population_incident_activator=getattr(
            request.app.state,
            "population_incident_activator",
            None,
        ),
    )


def get_recovery_case_processor(request: Request) -> RecoveryCaseProcessor:
    processor = getattr(request.app.state, "recovery_processor", None)
    if processor is None:
        raise HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            detail="Recovery case processor is not configured.",
        )
    return processor


def get_payment_reconciler(
    request: Request,
    database: Database = Depends(get_database),
) -> RazorpayPaymentReconciler:
    reconciler = getattr(request.app.state, "payment_reconciler", None)
    if reconciler is not None:
        return reconciler
    return RazorpayPaymentReconciler(database)


app = create_app()
