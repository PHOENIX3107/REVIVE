"""FastAPI application entrypoint for the REVIVE backend."""

import os
from datetime import datetime

from fastapi import Depends, FastAPI, HTTPException, Request, status
from pydantic import BaseModel

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
from backend.recovery.case_processor import (
    RecoveryCaseNotFoundError,
    RecoveryCaseProcessor,
    RecoveryCaseProcessingResult,
    RecoveryCaseStateError,
)
from backend.recovery.reconciliation import (
    PaymentVerificationCaseNotFoundError,
    PaymentVerificationError,
    PaymentVerificationResult,
    PaymentVerificationStateError,
    RazorpayPaymentReconciler,
)
from backend.policies.recovery_policy import PolicyDecisionType
from backend.schemas import Order, OrderStatus, PaymentAttempt, PaymentStatus
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
    test_mode: bool = True
    checkout: RazorpayCheckoutOptions


def create_app(
    database: Database | None = None,
    recovery_processor: RecoveryCaseProcessor | None = None,
    payment_reconciler: RazorpayPaymentReconciler | None = None,
) -> FastAPI:
    app = FastAPI(title="REVIVE", version="0.1.0")
    app.state.database = database
    app.state.recovery_processor = recovery_processor
    app.state.payment_reconciler = payment_reconciler

    @app.on_event("startup")
    def _initialize_schema() -> None:
        configured_database = getattr(app.state, "database", None)
        if configured_database is not None:
            configured_database.initialize()
            return

        database_url = os.getenv("DATABASE_URL")
        if database_url:
            Database(database_url).initialize()

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
                detail="Razorpay Test Mode credentials are not configured.",
            ) from exc

        return CustomerCheckoutResponse(
            case_id=case_id,
            payment_id=payment.payment_id,
            order_id=order.order_id,
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
                detail="Razorpay Test Mode credentials are not configured.",
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


def get_razorpay_processor(database: Database = Depends(get_database)) -> RazorpayWebhookProcessor:
    return RazorpayWebhookProcessor(database=database, adapter=RazorpayWebhookAdapter())


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
