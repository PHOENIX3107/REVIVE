"""Run the existing recovery pipeline for a durable recovery case."""

from collections.abc import Iterable

from pydantic import BaseModel

from backend.agents.recovery_agent import (
    Diagnosis,
    DiagnosisCategory,
    DiagnosisEvidence,
    RecoveryAgent,
)
from backend.db import Database
from backend.pipeline.downtime_correlator import (
    DowntimeCorrelationResult,
    correlate_downtime,
)
from backend.pipeline.signal_detector import SignalDetector
from backend.population_incidents import PopulationIncidentActivator
from backend.policies.recovery_policy import PolicyDecision, PolicyDecisionType, evaluate_policy
from backend.recovery.executor import AuditRecord, ExecutionResult, RecoveryExecutor
from backend.schemas import Downtime, Order, PaymentAttempt, PopulationIncidentContext, SignalContext


class RecoveryCaseError(RuntimeError):
    """Base error for durable recovery-case processing failures."""


class RecoveryCaseNotFoundError(RecoveryCaseError):
    """Raised when a recovery case does not exist."""


class RecoveryCaseStateError(RecoveryCaseError):
    """Raised when a case exists but its associated state is incomplete."""


class RecoveryCaseProcessingResult(BaseModel):
    case_id: str
    payment_id: str
    order_id: str
    signal: SignalContext | None = None
    population_signal_detected: bool = False
    population_incident: PopulationIncidentContext = PopulationIncidentContext()
    downtime: DowntimeCorrelationResult | None = None
    diagnosis: Diagnosis
    decision: PolicyDecision
    execution: ExecutionResult
    already_processed: bool = False


class RecoveryCaseProcessor:
    """Evaluate one persisted case and durably record its pipeline result."""

    def __init__(
        self,
        database: Database,
        signal_detector: SignalDetector,
        recovery_agent: RecoveryAgent,
        recovery_executor: RecoveryExecutor,
        downtimes: Iterable[Downtime] = (),
        population_incident_activator: PopulationIncidentActivator | None = None,
    ) -> None:
        self.database = database
        self.signal_detector = signal_detector
        self.recovery_agent = recovery_agent
        self.recovery_executor = recovery_executor
        self.downtimes = tuple(downtimes)
        self.population_incident_activator = population_incident_activator

    def process(self, case_id: str) -> RecoveryCaseProcessingResult:
        """Load durable state, run the pipeline, and persist its result once."""
        case = self.database.get_recovery_case(case_id)
        if case is None:
            raise RecoveryCaseNotFoundError(f"Recovery case not found: {case_id}")

        order, payment = self._load_case_state(case)
        existing = self.database.get_recovery_execution(case_id)
        if existing is not None:
            return self._existing_result(case_id, order, payment, existing, case)

        signal = self.signal_detector.detect(payment)
        population_incident = (
            self.population_incident_activator.active_context(payment)
            if self.population_incident_activator is not None
            else PopulationIncidentContext()
        )
        effective_signal = signal
        if population_incident.population_incident_active:
            # Keep the detector's instantaneous SignalContext unchanged in the
            # result. The policy/diagnosis boundary receives the effective
            # signal for this case, combining instantaneous and durable active
            # incident evidence without mutating detector or policy semantics.
            effective_signal = signal.model_copy(update={"is_cluster_candidate": True})
        downtime = correlate_downtime(payment, self.downtimes)
        diagnosis = self.recovery_agent.diagnose(
            DiagnosisEvidence(
                order=order,
                payment=payment,
                signal=effective_signal,
                downtime=downtime,
            )
        )
        decision = evaluate_policy(order, payment, effective_signal, downtime, diagnosis)

        # Case identity gives every processing attempt the same durable key.
        # The executor still performs its existing Redis claim for recoveries.
        idempotency_key = f"recovery-case:{case_id}"
        execution = self.recovery_executor.execute(
            payment.payment_id,
            order.order_id,
            decision,
            idempotency_key,
        )
        inserted = self.database.save_recovery_processing(
            case_id,
            diagnosis,
            decision,
            execution,
            idempotency_key,
            population_signal_detected=effective_signal.is_cluster_candidate,
            population_incident=population_incident,
        )
        if not inserted:
            existing = self.database.get_recovery_execution(case_id)
            if existing is None:
                raise RecoveryCaseError(
                    f"Recovery processing already claimed idempotency key: {idempotency_key}"
                )
            refreshed_case = self.database.get_recovery_case(case_id) or case
            return self._existing_result(case_id, order, payment, existing, refreshed_case)

        return RecoveryCaseProcessingResult(
            case_id=case_id,
            payment_id=payment.payment_id,
            order_id=order.order_id,
            signal=signal,
            population_signal_detected=effective_signal.is_cluster_candidate,
            population_incident=population_incident,
            downtime=downtime,
            diagnosis=diagnosis,
            decision=decision,
            execution=execution,
        )

    def _load_case_state(self, case: dict[str, object]) -> tuple[Order, PaymentAttempt]:
        payment_id = case.get("payment_id")
        order_id = case.get("order_id")
        if not isinstance(payment_id, str) or not isinstance(order_id, str):
            raise RecoveryCaseStateError("Recovery case has invalid payment or order identity.")

        order = self.database.get_order(order_id)
        payment = self.database.get_payment_attempt(payment_id)
        if order is None or payment is None:
            raise RecoveryCaseStateError("Recovery case is missing its associated payment or order.")
        if payment.order_id != order.order_id or payment.order_id != order_id:
            raise RecoveryCaseStateError("Recovery case payment and order do not match.")
        return order, payment

    def _existing_result(
        self,
        case_id: str,
        order: Order,
        payment: PaymentAttempt,
        execution_row: dict[str, object],
        case_row: dict[str, object],
    ) -> RecoveryCaseProcessingResult:
        diagnosis = self.database.get_recovery_diagnosis(case_id) or Diagnosis(
            category=DiagnosisCategory.unknown,
            confidence=0,
            reason="No diagnosis record was found for the existing execution.",
        )
        decision = self.database.get_recovery_decision(case_id) or PolicyDecision(
            decision=PolicyDecisionType.review,
            reason="No decision record was found for the existing execution.",
        )
        timestamp = execution_row.get("timestamp")
        if timestamp is None:
            raise RecoveryCaseError("Existing recovery execution has no timestamp.")
        action = execution_row.get("action")
        action = action if isinstance(action, str) else None
        audit = AuditRecord(
            payment_id=payment.payment_id,
            order_id=order.order_id,
            policy_decision=decision.decision,
            action=action,
            status=str(execution_row["status"]),
            reason=str(execution_row["reason"]),
            timestamp=timestamp,
        )
        execution = ExecutionResult(
            executed=bool(execution_row["executed"]),
            action=action,
            status=str(execution_row["status"]),
            reason=str(execution_row["reason"]),
            audit=audit,
        )
        population_incident = PopulationIncidentContext(
            population_incident_active=bool(case_row.get("population_incident_active", False)),
            population_incident_id=case_row.get("population_incident_id"),
            cohort_key=case_row.get("population_incident_cohort_key"),
            population_incident_activated_at=case_row.get("population_incident_activated_at"),
            population_incident_expires_at=case_row.get("population_incident_expires_at"),
        )
        return RecoveryCaseProcessingResult(
            case_id=case_id,
            payment_id=payment.payment_id,
            order_id=order.order_id,
            population_signal_detected=bool(case_row.get("population_signal_detected", False)),
            population_incident=population_incident,
            diagnosis=diagnosis,
            decision=decision,
            execution=execution,
            already_processed=True,
        )
