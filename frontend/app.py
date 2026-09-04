"""Thin, dependency-free REVIVE operations UI.

Run with ``python frontend/app.py`` and open http://127.0.0.1:8000.
The page is backed by the deterministic synthetic evaluation dataset; no
external payment provider or recovery service is contacted.
"""

from collections import defaultdict
from datetime import timedelta
import html
import json
from pathlib import Path
import sys
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from backend.agents.recovery_agent import DiagnosisCategory, DiagnosisEvidence, RecoveryAgent
from backend.cache import RedisFailureCache, RedisIdempotencyCache
from backend.evaluation.metrics import (
    EvaluationRecord,
    customer_side_recovery_attempts,
    number_of_blocked_actions,
    number_of_duplicate_actions_prevented,
    number_of_recovery_actions,
    systemic_cluster_recovery_attempts,
    total_amount_at_risk,
    total_amount_recovered,
    unsafe_recovery_actions,
)
from backend.generator import generate_batch
from backend.pipeline.downtime_correlator import DowntimeCorrelationResult, correlate_downtime
from backend.pipeline.signal_detector import SignalDetector
from backend.policies.recovery_policy import PolicyDecisionType, evaluate_policy
from backend.recovery.executor import RecoveryExecutor
from backend.schemas import Downtime, PaymentStatus


class MemoryRedis:
    """Small Redis-shaped store for this local synthetic product demo."""

    def __init__(self):
        self.sorted_sets = {}
        self.values = {}

    def zadd(self, key, members):
        self.sorted_sets.setdefault(key, {}).update(members)

    def zremrangebyscore(self, key, minimum, maximum):
        cutoff = float(str(maximum).lstrip("("))
        for member, score in list(self.sorted_sets.get(key, {}).items()):
            if score <= cutoff:
                del self.sorted_sets[key][member]

    def zcard(self, key):
        return len(self.sorted_sets.get(key, {}))

    def expire(self, key, seconds):
        return True

    def set(self, key, value, *, ex, nx):
        if nx and key in self.values:
            return False
        self.values[key] = (value, ex)
        return True


def _diagnosis_for(attempt, signal, downtime):
    category = (
        DiagnosisCategory.systemic_issue
        if signal.is_cluster_candidate or downtime.matched
        else DiagnosisCategory.customer_issue
    )
    reason = (
        "Synthetic evidence indicates a population signal or relevant downtime."
        if category is DiagnosisCategory.systemic_issue
        else "Synthetic customer-side error with no population signal or downtime."
    )
    return RecoveryAgent(
        lambda _prompt: {
            "category": category.value,
            "confidence": 0.94 if category is DiagnosisCategory.systemic_issue else 0.91,
            "reason": reason,
        }
    ).diagnose(
        DiagnosisEvidence(
            order=_CURRENT_ORDERS[attempt.order_id],
            payment=attempt,
            signal=signal,
            downtime=downtime,
        )
    )


def build_dataset():
    batch = generate_batch(num_attempts=100, seed=42)
    orders = {order.order_id: order for order in batch["orders"]}
    redis = MemoryRedis()
    detector = SignalDetector(RedisFailureCache(redis))
    executor = RecoveryExecutor(RedisIdempotencyCache(redis))

    downtimes = []
    for cluster_id, payment_ids in batch["ground_truth"]["clusters"].items():
        members = [
            attempt for attempt in batch["payment_attempts"] if attempt.payment_id in payment_ids
        ]
        if cluster_id == "synthetic_cluster_1":
            downtimes.append(
                Downtime(
                    downtime_id="downtime_synthetic_issuer_1",
                    entity="payments",
                    method="card",
                    begin=min(item.failed_at for item in members) - timedelta(minutes=1),
                    end=max(item.failed_at for item in members) + timedelta(minutes=1),
                    status="resolved",
                    scheduled=False,
                    severity="high",
                    instrument="issuer",
                )
            )

    global _CURRENT_ORDERS
    _CURRENT_ORDERS = orders
    records = []
    cases = []
    for attempt in batch["payment_attempts"]:
        signal = detector.detect(attempt)
        correlation = correlate_downtime(attempt, downtimes)
        diagnosis = _diagnosis_for(attempt, signal, correlation)
        order = orders[attempt.order_id]
        policy = evaluate_policy(
            order,
            attempt,
            signal,
            correlation,
            diagnosis,
        )
        execution = executor.execute(
            attempt.payment_id,
            attempt.order_id,
            policy,
            f"revive-demo:{attempt.payment_id}",
        )
        truth = batch["ground_truth"]["attempts"].get(attempt.payment_id, {})
        cases.append(
            {
                "payment_id": attempt.payment_id,
                "order_id": attempt.order_id,
                "amount": attempt.amount,
                "method": attempt.method.value,
                "issuer_bin": attempt.issuer_bin,
                "payment_status": attempt.status.value,
                "order_status": order.status.value,
                "attempts": order.attempts,
                "created_at": attempt.created_at.isoformat(),
                "failed_at": attempt.failed_at.isoformat() if attempt.failed_at else None,
                "error": attempt.error.model_dump(mode="json") if attempt.error else None,
                "signal": signal.model_dump(mode="json"),
                "downtime": correlation.model_dump(mode="json"),
                "diagnosis": diagnosis.model_dump(mode="json"),
                "policy": policy.model_dump(mode="json"),
                "execution": execution.model_dump(mode="json"),
                "cluster_id": truth.get("cluster_id"),
                "is_clustered": truth.get("is_clustered", False),
            }
        )
        records.append(
            EvaluationRecord(
                payment_id=attempt.payment_id,
                amount=attempt.amount,
                policy_decision=policy.decision,
                execution_status=execution.status,
                recovery_succeeded=False,
                payment_status=attempt.status,
                order_status=order.status.value,
                order_attempts=order.attempts,
                downtime_matched=correlation.matched,
                systemic_cluster=truth.get("is_clustered", False),
            )
        )

    failed = [case for case in cases if case["payment_status"] == "failed"]
    signal_rows = []
    grouped = defaultdict(list)
    for case in failed:
        if case["signal"]["matching_failure_count"]:
            key = (case["payment_id"], case["signal"]["matching_failure_count"])
            grouped[key].append(case)
    seen = set()
    for case in failed:
        if not case["signal"]["is_cluster_candidate"]:
            continue
        key = case["cluster_id"] or case["payment_id"]
        if key in seen:
            continue
        seen.add(key)
        signal_rows.append(
            {
                "signal_id": key,
                "issuer_bin": next(item["issuer_bin"] for item in failed if item["cluster_id"] == key),
                "error_code": next(item["error"]["code"] for item in failed if item["cluster_id"] == key),
                "matching_failure_count": case["signal"]["matching_failure_count"],
                "window_seconds": case["signal"]["window_seconds"],
                "cluster_status": "cluster candidate",
                "downtime": "active downtime detected" if case["downtime"]["matched"] else "none",
                "implication": "cooldown individual recovery",
                "case_id": case["payment_id"],
            }
        )

    return {
        "cases": cases,
        "downtimes": [item.model_dump(mode="json") for item in downtimes],
        "signals": signal_rows,
        "metrics": {
            "at_risk": total_amount_at_risk(records),
            "recovered": total_amount_recovered(records),
            "recovery_actions": number_of_recovery_actions(records),
            "blocked_actions": number_of_blocked_actions(records),
            "duplicates": number_of_duplicate_actions_prevented(records),
            "unsafe": unsafe_recovery_actions(records),
            "systemic_attempts": systemic_cluster_recovery_attempts(records),
            "customer_attempts": customer_side_recovery_attempts(records),
        },
        "dataset_note": "Synthetic evaluation dataset, seed 42. Execution is simulated.",
    }


DATASET = build_dataset()


def _money(value):
    return f"INR {value / 100:,.2f}"


def _safe(value):
    return html.escape(str(value))


PAGE = r"""<!doctype html>
<html lang="en"><head><meta charset="utf-8"><meta name="viewport" content="width=device-width, initial-scale=1">
<title>REVIVE / Revenue Operations</title>
<style>
:root{--wine:#7a1f3d;--wine-dark:#59152d;--blue:#2455d6;--ink:#0b0b0f;--muted:#6c707b;--line:#e7e8ec;--wash:#f7f7f8;--green:#176b4a;--amber:#916019;--red:#a33d49;--shadow:0 12px 35px rgba(11,11,15,.08)}
*{box-sizing:border-box}body{margin:0;background:#fff;color:var(--ink);font:14px/1.45 -apple-system,BlinkMacSystemFont,"Segoe UI",sans-serif}button,input{font:inherit}.shell{display:flex;min-height:100vh}.sidebar{width:238px;background:var(--ink);color:#fff;padding:25px 16px 18px;display:flex;flex-direction:column;flex-shrink:0}.brand{display:flex;align-items:center;gap:10px;padding:0 12px 30px}.brand-mark{width:30px;height:30px;border-radius:9px;background:var(--wine);display:grid;place-items:center;font-weight:800}.brand-name{font-size:18px;font-weight:750;letter-spacing:.08em}.workspace{margin:0 8px 25px;padding:12px;border:1px solid #2d2e35;border-radius:10px;color:#c9c9cf;font-size:11px}.workspace strong{display:block;color:#fff;font-size:13px;margin-top:3px}.nav-label{color:#777982;font-size:10px;font-weight:700;letter-spacing:.12em;padding:0 12px 9px;text-transform:uppercase}.nav button{width:100%;border:0;background:transparent;color:#a9aab1;border-radius:8px;text-align:left;padding:11px 12px;margin:2px 0;cursor:pointer}.nav button:hover,.nav button.active{background:#25252c;color:#fff}.nav button.active{box-shadow:inset 3px 0 var(--wine)}.nav-icon{display:inline-block;width:24px;color:#777982}.nav button.active .nav-icon{color:#d57693}.sidebar-foot{margin-top:auto;padding:12px 10px;color:#8e8f98;font-size:11px;border-top:1px solid #2d2e35}.main{flex:1;min-width:0}.topbar{height:72px;border-bottom:1px solid var(--line);display:flex;align-items:center;justify-content:space-between;padding:0 36px;background:#fff}.context{font-size:12px;color:var(--muted)}.context strong{display:block;color:var(--ink);font-size:15px}.top-actions{display:flex;align-items:center;gap:13px}.search{width:220px;border:1px solid var(--line);border-radius:8px;padding:9px 12px;outline:0}.search:focus{border-color:var(--blue);box-shadow:0 0 0 3px rgba(36,85,214,.1)}.pulse{width:8px;height:8px;background:#30a36e;border-radius:50%;display:inline-block}.avatar{width:32px;height:32px;border-radius:50%;background:#f0d9df;color:var(--wine);display:grid;place-items:center;font-weight:700}.content{padding:34px 36px 50px;max-width:1500px}.eyebrow{color:var(--wine);font-size:11px;font-weight:750;letter-spacing:.13em;text-transform:uppercase;margin-bottom:8px}.heading{display:flex;align-items:flex-end;justify-content:space-between;margin-bottom:25px}.heading h1{font-size:28px;letter-spacing:-.04em;margin:0 0 5px}.heading p{margin:0;color:var(--muted)}.dataset{font-size:11px;background:#fdf4f7;color:var(--wine);border:1px solid #f0d8df;border-radius:999px;padding:7px 11px}.metrics{display:grid;grid-template-columns:repeat(4,1fr);gap:14px;margin-bottom:30px}.metric{border:1px solid var(--line);border-radius:10px;padding:17px 18px;background:#fff}.metric .label{font-size:12px;color:var(--muted)}.metric .value{font-size:25px;font-weight:700;letter-spacing:-.04em;margin:7px 0}.metric .note{font-size:11px;color:var(--muted)}.metric.blue{border-top:3px solid var(--blue)}.metric.wine{border-top:3px solid var(--wine)}.metric.dark{border-top:3px solid var(--ink)}.section-head{display:flex;justify-content:space-between;align-items:center;margin:28px 0 12px}.section-head h2{font-size:16px;margin:0;letter-spacing:-.02em}.section-head span{font-size:12px;color:var(--muted)}.panel{border:1px solid var(--line);border-radius:10px;background:#fff;overflow:hidden;box-shadow:var(--shadow)}.table-wrap{overflow:auto}.table{width:100%;border-collapse:collapse;min-width:760px}.table th{text-align:left;font-size:10px;letter-spacing:.08em;text-transform:uppercase;color:#858891;background:var(--wash);padding:11px 15px;font-weight:700}.table td{padding:13px 15px;border-top:1px solid var(--line);font-size:12px;white-space:nowrap}.table tr.case-row{cursor:pointer}.table tr.case-row:hover{background:#fcf8f9}.mono{font-family:ui-monospace,SFMono-Regular,Menlo,monospace;font-size:11px}.sub{display:block;color:var(--muted);font-size:11px;margin-top:2px}.badge{display:inline-flex;align-items:center;border-radius:999px;padding:4px 8px;font-size:10px;font-weight:750;letter-spacing:.04em;text-transform:uppercase}.badge.recover{background:#f7e8ee;color:var(--wine)}.badge.cooldown{background:#e9efff;color:var(--blue)}.badge.review{background:#f5f0e5;color:var(--amber)}.badge.stop{background:#f1f1f2;color:#555861}.badge.executed{background:#e7f4ed;color:var(--green)}.badge.blocked{background:#f1f1f2;color:#555861}.badge.duplicate{background:#e9efff;color:var(--blue)}.badge.signal{background:#e9efff;color:var(--blue)}.badge.customer{background:#f7e8ee;color:var(--wine)}.filters{display:flex;gap:9px;flex-wrap:wrap}.filter{border:1px solid var(--line);background:#fff;border-radius:7px;padding:7px 10px;color:var(--muted);cursor:pointer}.filter.active,.filter:hover{border-color:var(--wine);color:var(--wine)}.empty{padding:35px;color:var(--muted);text-align:center}.drawer{position:fixed;inset:0;background:rgba(11,11,15,.32);z-index:5;display:none}.drawer.open{display:block}.drawer-card{position:absolute;right:0;top:0;height:100%;width:min(620px,100%);background:#fff;overflow:auto;box-shadow:-10px 0 35px rgba(0,0,0,.16);padding:28px}.drawer-close{float:right;border:0;background:#f3f3f4;border-radius:50%;width:30px;height:30px;cursor:pointer}.detail-title{margin:28px 0 5px;font-size:22px;letter-spacing:-.03em}.detail-sub{color:var(--muted);margin-bottom:22px}.detail-grid{display:grid;grid-template-columns:1fr 1fr;gap:10px;margin:16px 0 25px}.detail-item{background:var(--wash);border-radius:8px;padding:11px}.detail-item label{display:block;color:var(--muted);font-size:10px;text-transform:uppercase;letter-spacing:.08em}.detail-item strong{display:block;margin-top:4px;font-size:12px}.chain{border-left:2px solid #dfe3ef;margin:22px 0 0 9px;padding-left:19px}.step{position:relative;padding:0 0 22px}.step:before{content:"";position:absolute;left:-26px;top:3px;width:10px;height:10px;border-radius:50%;background:var(--blue);box-shadow:0 0 0 4px #e9efff}.step h4{margin:0 0 3px;font-size:12px}.step p{margin:0;color:var(--muted);font-size:12px}.notice{padding:10px 12px;background:#fdf4f7;color:var(--wine);border-radius:8px;font-size:12px;margin:15px 0}.kpi-line{display:flex;gap:20px;color:var(--muted);font-size:12px}.kpi-line strong{color:var(--ink)}@media(max-width:850px){.sidebar{width:70px;padding:20px 8px}.brand{padding:0 11px 28px}.brand-name,.workspace,.nav-label,.nav button:not(.active) span,.sidebar-foot{display:none}.nav button{padding:12px;text-align:center}.nav-icon{width:auto}.topbar{padding:0 18px}.search{width:150px}.content{padding:25px 18px}.metrics{grid-template-columns:repeat(2,1fr)}}@media(max-width:520px){.topbar{height:auto;padding:15px 18px;gap:12px;align-items:flex-start}.context{display:none}.top-actions{width:100%}.search{flex:1}.metrics{grid-template-columns:1fr 1fr;gap:8px}.metric{padding:13px}.metric .value{font-size:19px}.heading{display:block}.dataset{display:inline-block;margin-top:15px}.content{padding-top:23px}.drawer-card{padding:22px 18px}.detail-grid{grid-template-columns:1fr}}
</style></head><body><div class="shell"><aside class="sidebar"><div class="brand"><div class="brand-mark">R</div><div class="brand-name">REVIVE</div></div><div class="workspace">WORKSPACE<strong>Revenue Operations</strong><span>synthetic evaluation</span></div><div class="nav-label">Workspace</div><nav class="nav"><button data-page="overview" class="active"><span class="nav-icon">&#9632;</span><span>Overview</span></button><button data-page="recoveries"><span class="nav-icon">&#8635;</span><span>Recoveries</span></button><button data-page="signals"><span class="nav-icon">&#9670;</span><span>Signals</span></button><button data-page="downtime"><span class="nav-icon">&#9673;</span><span>Downtime</span></button><button data-page="decisions"><span class="nav-icon">&#10003;</span><span>Decisions</span></button><button data-page="audit"><span class="nav-icon">&#9776;</span><span>Audit</span></button></nav><div class="sidebar-foot">REVIVE v0.1<br>Decision infrastructure</div></aside><main class="main"><header class="topbar"><div class="context">WORKSPACE / <strong>Operational Overview</strong></div><div class="top-actions"><input class="search" id="search" placeholder="Search payment or order"><span class="pulse"></span><span style="color:#6c707b;font-size:12px">System nominal</span><div class="avatar">SR</div></div></header><section class="content" id="app"></section></main></div><div class="drawer" id="drawer"><div class="drawer-card"><button class="drawer-close" id="close">x</button><div id="drawer-content"></div></div></div><script>const DATA=__DATA__;let page='overview';let filter='all';const app=document.getElementById('app');const money=n=>'INR '+(n/100).toLocaleString('en-IN',{minimumFractionDigits:2});const short=s=>s?s.replace('pay_synthetic_','pay_').replace('order_synthetic_','order_'):'';const badge=(v,cls=v)=>`<span class="badge ${cls}">${v}</span>`;const date=s=>s?new Date(s).toLocaleString('en-IN',{day:'2-digit',month:'short',hour:'2-digit',minute:'2-digit'}):'—';function row(c,mode='queue'){let decision=c.policy.decision,status=c.execution.status;return `<tr class="case-row" data-case="${c.payment_id}"><td><span class="mono">${short(c.payment_id)}</span><span class="sub">${c.method} / ${c.payment_status}</span></td><td><span class="mono">${short(c.order_id)}</span></td><td><strong>${money(c.amount)}</strong></td><td>${c.error?c.error.code:'—'}</td><td>${c.signal.is_cluster_candidate?badge('cluster','signal'):'—'}</td><td>${badge(decision)}</td><td>${badge(status,status)}</td><td>${c.execution.action||'No action'}</td></tr>`}function table(cases){if(!cases.length)return '<div class="empty">No cases match the current view.</div>';return `<div class="panel table-wrap"><table class="table"><thead><tr><th>Payment</th><th>Order</th><th>Amount</th><th>Failure</th><th>Signal</th><th>Decision</th><th>Status</th><th>Action</th></tr></thead><tbody>${cases.map(row).join('')}</tbody></table></div>`}function head(eyebrow,title,desc,extra=''){return `<div class="heading"><div><div class="eyebrow">${eyebrow}</div><h1>${title}</h1><p>${desc}</p></div>${extra}</div>`}function metric(label,value,note,kind){return `<div class="metric ${kind}"><div class="label">${label}</div><div class="value">${value}</div><div class="note">${note}</div></div>`}function overview(){let m=DATA.metrics;let cases=DATA.cases.filter(c=>c.payment_status==='failed');return head('Revenue operations','Overview','A focused view of payment failures, recovery decisions, and evidence.',`<div class="dataset">${DATA.dataset_note}</div>`)+`<div class="metrics">${metric('Revenue at risk',money(m.at_risk),'failed payment amount','wine')}${metric('Recoverable',money(cases.filter(c=>c.policy.decision==='recover').reduce((a,c)=>a+c.amount,0)),'policy permits recovery','blue')}${metric('Recovered',money(m.recovered),'explicit simulation success','dark')}${metric('Active signals',DATA.signals.length,'population patterns','blue')}</div><div class="section-head"><h2>Recovery work queue</h2><span>${cases.length} failed payments</span></div>${table(cases)}`};function recoveries(){let cases=DATA.cases.filter(c=>c.execution.status!=='blocked'||c.policy.decision!=='review');return head('Execution','Recoveries','Simulated recovery outcomes, kept separate from policy decisions.',`<div class="filters">${['all','executed','blocked','duplicate'].map(x=>`<button class="filter ${filter===x?'active':''}" data-filter="${x}">${x}</button>`).join('')}</div>`)+`<div class="kpi-line"><span>Executed <strong>${DATA.metrics.recovery_actions}</strong></span><span>Blocked <strong>${DATA.metrics.blocked_actions}</strong></span><span>Duplicates prevented <strong>${DATA.metrics.duplicates}</strong></span></div><div style="height:14px"></div>${table(filter==='all'?cases:cases.filter(c=>c.execution.status===filter))}`};function signals(){return head('Population intelligence','Signals','Similar failures grouped by issuer BIN and error code.','<div class="dataset">10 min window / threshold 5</div>')+`<div class="panel table-wrap"><table class="table"><thead><tr><th>Signal</th><th>Issuer BIN</th><th>Error code</th><th>Matching failures</th><th>Window</th><th>Downtime</th><th>Implication</th></tr></thead><tbody>${DATA.signals.map(s=>`<tr class="case-row" data-case="${s.case_id}"><td>${badge('cluster','signal')}<span class="sub mono">${s.signal_id}</span></td><td class="mono">${s.issuer_bin}</td><td>${s.error_code}</td><td><strong>${s.matching_failure_count}</strong></td><td>${s.window_seconds/60} min</td><td>${s.downtime}</td><td>${s.implication}</td></tr>`).join('')}</tbody></table></div>`};function downtime(){return head('Operational context','Downtime','Relevant provider conditions correlated with payment failures.')+`<div class="panel table-wrap"><table class="table"><thead><tr><th>Status</th><th>Severity</th><th>Method</th><th>Begin</th><th>End</th><th>Affected cases</th></tr></thead><tbody>${DATA.downtimes.map(d=>{let count=DATA.cases.filter(c=>c.downtime.downtime_id===d.downtime_id).length;return `<tr><td>${badge(d.status,d.status==='resolved'?'blocked':'cooldown')}</td><td>${badge(d.severity,'cooldown')}</td><td>${d.method}</td><td>${date(d.begin)}</td><td>${date(d.end)}</td><td><strong>${count}</strong> <span class="sub">payment failures</span></td></tr>`}).join('')}</tbody></table></div>`};function decisions(){let cases=DATA.cases.filter(c=>c.payment_status==='failed');return head('Decision record','Decisions','The advisory diagnosis is evidence. The deterministic policy is authority.')+`<div class="panel table-wrap"><table class="table"><thead><tr><th>Payment</th><th>Diagnosis</th><th>Signal</th><th>Downtime</th><th>Policy</th><th>Reason</th><th>Time</th></tr></thead><tbody>${cases.map(c=>`<tr class="case-row" data-case="${c.payment_id}"><td class="mono">${short(c.payment_id)}</td><td>${badge(c.diagnosis.category,c.diagnosis.category==='systemic_issue'?'signal':'customer')}<span class="sub">${Math.round(c.diagnosis.confidence*100)}% confidence</span></td><td>${c.signal.is_cluster_candidate?c.signal.matching_failure_count+' similar':'No cluster'}</td><td>${c.downtime.matched?'Matched':'None'}</td><td>${badge(c.policy.decision)}</td><td>${c.policy.reason}</td><td>${date(c.failed_at)}</td></tr>`).join('')}</tbody></table></div>`};function audit(){return head('Traceability','Audit','A chronological record of what REVIVE decided and what the simulator did.')+`<div class="panel table-wrap"><table class="table"><thead><tr><th>Timestamp</th><th>Payment</th><th>Order</th><th>Decision</th><th>Action</th><th>Status</th><th>Reason</th></tr></thead><tbody>${DATA.cases.filter(c=>c.payment_status==='failed').sort((a,b)=>new Date(b.execution.audit.timestamp)-new Date(a.execution.audit.timestamp)).map(c=>`<tr class="case-row" data-case="${c.payment_id}"><td>${date(c.execution.audit.timestamp)}</td><td class="mono">${short(c.payment_id)}</td><td class="mono">${short(c.order_id)}</td><td>${badge(c.policy.decision)}</td><td>${c.execution.action||'—'}</td><td>${badge(c.execution.status,c.execution.status)}</td><td>${c.execution.audit.reason}</td></tr>`).join('')}</tbody></table></div>`};function render(){document.querySelectorAll('.nav button').forEach(b=>b.classList.toggle('active',b.dataset.page===page));app.innerHTML={overview,recoveries,signals,downtime,decisions,audit}[page]();}function openCase(id){let c=DATA.cases.find(x=>x.payment_id===id);if(!c)return;document.getElementById('drawer-content').innerHTML=`<div class="eyebrow">Case detail / ${c.is_clustered?'systemic failure':'customer-side failure'}</div><h2 class="detail-title">${short(c.payment_id)}</h2><div class="detail-sub">${c.error?c.error.description:'No failure detail'} · ${money(c.amount)}</div><div class="detail-grid"><div class="detail-item"><label>Order</label><strong>${short(c.order_id)}</strong></div><div class="detail-item"><label>Payment method</label><strong>${c.method}</strong></div><div class="detail-item"><label>Payment state</label><strong>${c.payment_status}</strong></div><div class="detail-item"><label>Order state</label><strong>${c.order_status}</strong></div><div class="detail-item"><label>Attempts</label><strong>${c.attempts}</strong></div><div class="detail-item"><label>Failure time</label><strong>${date(c.failed_at)}</strong></div></div><div class="notice">${c.execution.status==='executed'?'Simulated retry executed. No external payment call was made.':c.policy.reason}</div><div class="chain"><div class="step"><h4>Payment failure</h4><p>${c.error?c.error.code+' · '+c.error.description:'Not failed'}</p></div><div class="step"><h4>Population signal</h4><p>${c.signal.is_cluster_candidate?c.signal.matching_failure_count+' similar failures in '+c.signal.window_seconds/60+' min':'No matching population signal'}</p></div><div class="step"><h4>Downtime evidence</h4><p>${c.downtime.matched?'Matched '+c.downtime.downtime_id+' · '+c.downtime.severity:'No relevant downtime'}</p></div><div class="step"><h4>AI diagnosis</h4><p>${c.diagnosis.category} · ${Math.round(c.diagnosis.confidence*100)}%<br>${c.diagnosis.reason}</p></div><div class="step"><h4>Policy decision</h4><p>${badge(c.policy.decision)} ${c.policy.reason}</p></div><div class="step"><h4>Execution</h4><p>${c.execution.action||'No action'} · ${c.execution.status}<br>${c.execution.audit.timestamp?date(c.execution.audit.timestamp):''}</p></div></div>`;document.getElementById('drawer').classList.add('open')}document.addEventListener('click',e=>{let nav=e.target.closest('[data-page]');if(nav){page=nav.dataset.page;filter='all';render()}let f=e.target.closest('[data-filter]');if(f){filter=f.dataset.filter;render()}let r=e.target.closest('[data-case]');if(r)openCase(r.dataset.case);if(e.target.id==='close'||e.target.id==='drawer')document.getElementById('drawer').classList.remove('open')});document.getElementById('search').addEventListener('input',e=>{let q=e.target.value.toLowerCase();document.querySelectorAll('.case-row').forEach(r=>r.style.display=r.innerText.toLowerCase().includes(q)?'':'none')});render();</script></body></html>"""


class Handler(BaseHTTPRequestHandler):
    def do_GET(self):
        if self.path not in ("/", "/index.html"):
            self.send_error(404)
            return
        payload = json.dumps(DATASET).replace("<", "\\u003c")
        body = PAGE.replace("__DATA__", payload).encode("utf-8")
        self.send_response(200)
        self.send_header("Content-Type", "text/html; charset=utf-8")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def log_message(self, format, *args):
        return


if __name__ == "__main__":
    port = 8000
    print(f"REVIVE UI running at http://127.0.0.1:{port}")
    ThreadingHTTPServer(("127.0.0.1", port), Handler).serve_forever()
