import json
from pathlib import Path
from urllib.parse import urlsplit

from urllib.error import URLError

import frontend.app as frontend_app


class FakeResponse:
    status = 200

    def __init__(self, payload):
        self.payload = payload

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        return False

    def read(self):
        return json.dumps(self.payload).encode("utf-8")


def _dashboard_data():
    metrics = {
        "payment_attempts": 2,
        "failed_payments": 2,
        "revenue_at_risk": 1500,
        "policy_eligible_revenue": 1000,
        "recovered_revenue": 700,
        "recovery_actions": 1,
        "blocked_actions": 1,
        "duplicate_actions_prevented": 0,
        "unsafe_actions": 0,
        "systemic_attempts": 1,
        "customer_attempts": 1,
    }
    executed_without_outcome = {
        "case_id": "case_executed",
        "payment_id": "pay_executed",
        "order_id": "order_executed",
        "amount": 1000,
        "payment_status": "failed",
        "order_status": "attempted",
        "case_status": "open",
        "diagnosis_category": "customer_issue",
        "diagnosis_confidence": 0.9,
        "diagnosis_reason": "Customer issue.",
        "decision": "recover",
        "decision_reason": "Safe to recover.",
        "execution_status": "executed",
        "execution_action": "retry_payment",
        "execution_reason": "Simulated execution only.",
        "execution_timestamp": "2026-01-01T00:00:00Z",
        "payment_outcome_status": None,
        "payment_outcome_amount": None,
        "payment_outcome_observed_at": None,
    }
    provider_recovered = {
        **executed_without_outcome,
        "case_id": "case_recovered",
        "payment_id": "pay_recovered",
        "order_id": "order_recovered",
        "amount": 700,
        "payment_status": "captured",
        "order_status": "paid",
        "case_status": "recovered",
        "execution_status": None,
        "execution_action": None,
        "execution_reason": None,
        "execution_timestamp": None,
        "payment_outcome_status": "recovered",
        "payment_outcome_amount": 700,
        "payment_outcome_observed_at": "2026-01-01T00:01:00Z",
    }
    return {
        "available": True,
        "api_base_url": "http://127.0.0.1:8000",
        "overview": {"metrics": metrics, "recent_cases": [executed_without_outcome]},
        "recoveries": [executed_without_outcome, provider_recovered],
        "signals": [
            {
                "issuer_bin": "411111",
                "error_code": "insufficient_funds",
                "failure_count": 2,
                "total_amount": 1500,
                "latest_failed_at": "2026-01-01T00:00:00Z",
            }
        ],
        "downtime": {"available": False, "items": []},
        "decisions": [],
        "audit": [],
    }


def test_frontend_uses_dashboard_api_and_configurable_base_url(monkeypatch):
    responses = {
        path: {"endpoint": path}
        for path in frontend_app.API_ENDPOINTS.values()
    }
    requested_urls = []

    def fake_urlopen(request, timeout):
        assert timeout == 5
        requested_urls.append(request.full_url)
        return FakeResponse(responses[urlsplit(request.full_url).path])

    monkeypatch.setenv("REVIVE_API_BASE_URL", "http://api.test/")
    monkeypatch.setattr(frontend_app, "urlopen", fake_urlopen)

    data = frontend_app.fetch_dashboard_data()

    assert data["available"] is True
    assert requested_urls == [
        f"http://api.test{path}" for path in frontend_app.API_ENDPOINTS.values()
    ]
    assert data["overview"] == {"endpoint": "/dashboard/overview"}
    assert data["audit"] == {"endpoint": "/dashboard/audit"}


def test_frontend_no_longer_contains_synthetic_dataset_dependencies():
    source = Path(frontend_app.__file__).read_text(encoding="utf-8")

    assert "generate_batch" not in source
    assert "build_dataset" not in source
    assert "backend.evaluation" not in source
    assert "backend.generator" not in source


def test_provider_confirmed_revenue_is_displayed_separately_from_execution():
    rendered = frontend_app.render_page(_dashboard_data()).decode("utf-8")

    assert '"recovered_revenue":700' in rendered
    assert '"execution_status":"executed"' in rendered
    assert '"payment_outcome_status":null' in rendered
    assert '"payment_outcome_status":"recovered"' in rendered
    assert "Execution success is not payment success." in rendered
    assert "provider-confirmed outcomes only" in rendered
    assert "Provider-confirmed payment outcome" in rendered


def test_unavailable_api_renders_graceful_error_state(monkeypatch):
    def unavailable(*_args, **_kwargs):
        raise URLError("connection refused")

    monkeypatch.setattr(frontend_app, "urlopen", unavailable)

    data = frontend_app.fetch_dashboard_data("http://api.test")
    rendered = frontend_app.render_page(data).decode("utf-8")

    assert data["available"] is False
    assert "API unavailable" in rendered
    assert "FastAPI/PostgreSQL dashboard is unavailable." in rendered


def test_unavailable_downtime_is_rendered_honestly():
    rendered = frontend_app.render_page(_dashboard_data()).decode("utf-8")

    assert "No persisted downtime source configured." in rendered
    assert "REVIVE is not fabricating downtime records." in rendered
