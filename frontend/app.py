"""Thin, dependency-free REVIVE operations UI.

Run with ``python frontend/app.py`` and open the frontend port shown in the
terminal. The UI reads durable state from the FastAPI dashboard API and never
creates demo data in the browser.
"""

import json
import os
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from urllib.error import HTTPError, URLError
from urllib.request import Request, urlopen


DEFAULT_API_BASE_URL = "http://127.0.0.1:8000"
API_ENDPOINTS = {
    "overview": "/dashboard/overview",
    "recoveries": "/dashboard/recoveries",
    "incidents": "/dashboard/incidents",
    "signals": "/dashboard/signals",
    "downtime": "/dashboard/downtime",
    "decisions": "/dashboard/decisions",
    "audit": "/dashboard/audit",
}


class DashboardAPIError(RuntimeError):
    """Raised when the FastAPI dashboard cannot provide a valid response."""


def _api_base_url() -> str:
    return os.getenv("REVIVE_API_BASE_URL", DEFAULT_API_BASE_URL).rstrip("/")


def fetch_dashboard_data(base_url: str | None = None) -> dict[str, object]:
    """Fetch the complete read-only dashboard projection over HTTP."""
    api_base_url = (base_url or _api_base_url()).rstrip("/")
    data: dict[str, object] = {"available": True, "api_base_url": api_base_url}
    try:
        for name, path in API_ENDPOINTS.items():
            request = Request(
                f"{api_base_url}{path}",
                headers={"Accept": "application/json"},
            )
            with urlopen(request, timeout=5) as response:
                if getattr(response, "status", 200) >= 400:
                    raise DashboardAPIError(f"Dashboard API returned HTTP {response.status}.")
                data[name] = json.loads(response.read().decode("utf-8"))
    except (DashboardAPIError, HTTPError, URLError, TimeoutError, OSError, ValueError) as exc:
        return {
            "available": False,
            "api_base_url": api_base_url,
            "error": "FastAPI/PostgreSQL dashboard is unavailable.",
            "error_type": type(exc).__name__,
        }
    return data


def render_page(data: dict[str, object]) -> bytes:
    """Render the browser application with the latest API response embedded."""
    payload = json.dumps(data, separators=(",", ":")).replace("<", "\\u003c")
    return PAGE.replace("__DATA__", payload).encode("utf-8")


PAGE = r"""<!doctype html>
<html lang="en">
<head>
<meta charset="utf-8"><meta name="viewport" content="width=device-width, initial-scale=1">
<title>REVIVE / Revenue Operations</title>
<style>
:root{--ink:#11131a;--muted:#69707d;--line:#e5e7eb;--wash:#f7f8fa;--panel:#fff;--wine:#7a1f3d;--wine-soft:#f8edf1;--blue:#2655d8;--blue-soft:#edf2ff;--green:#18724d;--green-soft:#e8f5ee;--amber:#9a6516;--amber-soft:#fff6e4;--red:#a53f4c;--shadow:0 14px 40px rgba(17,19,26,.07)}
*{box-sizing:border-box}body{margin:0;background:#fbfbfc;color:var(--ink);font:14px/1.45 -apple-system,BlinkMacSystemFont,"Segoe UI",sans-serif}button,input{font:inherit}.shell{display:flex;min-height:100vh}.sidebar{width:244px;background:#11131a;color:#fff;padding:26px 16px 18px;display:flex;flex-direction:column;flex-shrink:0}.brand{display:flex;align-items:center;gap:10px;padding:0 12px 29px}.brand-mark{width:31px;height:31px;border-radius:9px;background:var(--wine);display:grid;place-items:center;font-weight:800}.brand-name{font-size:18px;font-weight:800;letter-spacing:.08em}.workspace{margin:0 8px 26px;padding:12px;border:1px solid #2c2f38;border-radius:10px;color:#b9bdc6;font-size:11px}.workspace strong{display:block;color:#fff;font-size:13px;margin-top:3px}.workspace span{display:block;margin-top:2px}.nav-label{color:#777d89;font-size:10px;font-weight:800;letter-spacing:.12em;padding:0 12px 9px;text-transform:uppercase}.nav button{width:100%;border:0;background:transparent;color:#aeb3be;border-radius:8px;text-align:left;padding:11px 12px;margin:2px 0;cursor:pointer}.nav button:hover,.nav button.active{background:#272a33;color:#fff}.nav button.active{box-shadow:inset 3px 0 var(--wine)}.nav-icon{display:inline-block;width:24px;color:#7f8693}.nav button.active .nav-icon{color:#df7897}.sidebar-foot{margin-top:auto;padding:12px 10px;color:#858b97;font-size:11px;border-top:1px solid #2c2f38}.main{flex:1;min-width:0}.topbar{height:72px;border-bottom:1px solid var(--line);display:flex;align-items:center;justify-content:space-between;padding:0 38px;background:#fff}.context{font-size:11px;color:var(--muted);letter-spacing:.08em;text-transform:uppercase}.context strong{display:block;color:var(--ink);font-size:15px;letter-spacing:0;text-transform:none}.top-actions{display:flex;align-items:center;gap:12px}.search{width:230px;border:1px solid var(--line);border-radius:8px;padding:9px 12px;outline:0;background:#fff}.search:focus{border-color:var(--blue);box-shadow:0 0 0 3px rgba(38,85,216,.1)}.pulse{width:8px;height:8px;background:#31a36e;border-radius:50%;display:inline-block}.pulse.offline{background:var(--red)}.api-status{color:var(--muted);font-size:12px}.avatar{width:32px;height:32px;border-radius:50%;background:#f0d9df;color:var(--wine);display:grid;place-items:center;font-weight:750}.content{padding:34px 38px 60px;max-width:1540px}.eyebrow{color:var(--wine);font-size:10px;font-weight:800;letter-spacing:.15em;text-transform:uppercase;margin-bottom:8px}.heading{display:flex;align-items:flex-end;justify-content:space-between;gap:20px;margin-bottom:26px}.heading h1{font-size:29px;letter-spacing:-.045em;margin:0 0 5px}.heading p{margin:0;color:var(--muted);max-width:720px}.dataset{font-size:11px;background:var(--wine-soft);color:var(--wine);border:1px solid #eed5de;border-radius:999px;padding:7px 11px;white-space:nowrap}.metrics{display:grid;grid-template-columns:repeat(4,1fr);gap:14px;margin-bottom:31px}.metric{border:1px solid var(--line);border-radius:11px;padding:17px 18px;background:var(--panel);box-shadow:0 3px 12px rgba(17,19,26,.025)}.metric .label{font-size:12px;color:var(--muted)}.metric .value{font-size:25px;font-weight:750;letter-spacing:-.045em;margin:7px 0}.metric .note{font-size:11px;color:var(--muted)}.metric.blue{border-top:3px solid var(--blue)}.metric.wine{border-top:3px solid var(--wine)}.metric.dark{border-top:3px solid var(--ink)}.metric.green{border-top:3px solid var(--green)}.section-head{display:flex;justify-content:space-between;align-items:center;gap:15px;margin:30px 0 12px}.section-head h2{font-size:16px;margin:0;letter-spacing:-.02em}.section-head span{font-size:12px;color:var(--muted)}.panel{border:1px solid var(--line);border-radius:11px;background:var(--panel);overflow:hidden;box-shadow:var(--shadow)}.table-wrap{overflow:auto}.table{width:100%;border-collapse:collapse;min-width:1080px}.table th{text-align:left;font-size:10px;letter-spacing:.08em;text-transform:uppercase;color:#858c98;background:var(--wash);padding:11px 15px;font-weight:800}.table td{padding:13px 15px;border-top:1px solid var(--line);font-size:12px;white-space:nowrap;vertical-align:top}.table tr.case-row{cursor:pointer}.table tr.case-row:hover{background:#fcf8f9}.mono{font-family:ui-monospace,SFMono-Regular,Menlo,monospace;font-size:11px}.sub{display:block;color:var(--muted);font-size:11px;margin-top:2px;white-space:normal}.badge{display:inline-flex;align-items:center;border-radius:999px;padding:4px 8px;font-size:10px;font-weight:800;letter-spacing:.04em;text-transform:uppercase}.badge.recover{background:var(--wine-soft);color:var(--wine)}.badge.cooldown,.badge.signal{background:var(--blue-soft);color:var(--blue)}.badge.review{background:var(--amber-soft);color:var(--amber)}.badge.stop,.badge.blocked{background:#f0f1f3;color:#555c68}.badge.executed,.badge.recovered,.badge.confirmed{background:var(--green-soft);color:var(--green)}.badge.duplicate{background:var(--blue-soft);color:var(--blue)}.badge.observed{background:var(--amber-soft);color:var(--amber)}.badge.none{background:#f0f1f3;color:#68707d}.filters{display:flex;gap:9px;flex-wrap:wrap}.filter{border:1px solid var(--line);background:#fff;border-radius:7px;padding:7px 10px;color:var(--muted);cursor:pointer}.filter.active,.filter:hover{border-color:var(--wine);color:var(--wine)}.empty{padding:35px;color:var(--muted);text-align:center}.notice{padding:14px 16px;background:var(--wine-soft);color:var(--wine);border-radius:9px;font-size:12px;margin:15px 0}.notice.ok{background:var(--green-soft);color:var(--green)}.notice.warning{background:var(--amber-soft);color:var(--amber)}.error-state{max-width:650px;margin:70px auto;text-align:center;border:1px solid #efd5dc;border-radius:13px;padding:38px;background:#fff;box-shadow:var(--shadow)}.error-state h2{margin:0 0 8px}.error-state p{color:var(--muted);margin:0}.incident-grid{display:grid;grid-template-columns:repeat(2,minmax(0,1fr));gap:14px}.incident-card{border:1px solid #dae2f7;border-left:4px solid var(--blue);border-radius:11px;background:#fff;padding:18px;box-shadow:var(--shadow)}.incident-top{display:flex;justify-content:space-between;gap:12px;align-items:flex-start}.incident-title{font-size:16px;font-weight:800;letter-spacing:-.02em}.incident-meta{font-size:12px;color:var(--muted);margin-top:3px}.status-active{display:inline-flex;border-radius:999px;padding:4px 8px;background:var(--blue-soft);color:var(--blue);font-size:10px;font-weight:800;letter-spacing:.06em}.incident-stats{display:flex;flex-wrap:wrap;gap:8px;margin:18px 0 14px}.incident-stat{background:var(--wash);border-radius:8px;padding:9px 11px;min-width:110px}.incident-stat label{display:block;color:var(--muted);font-size:10px;text-transform:uppercase;letter-spacing:.07em}.incident-stat strong{display:block;margin-top:3px;font-size:13px}.incident-times{display:grid;grid-template-columns:1fr 1fr;gap:10px;color:var(--muted);font-size:11px}.incident-times strong{display:block;color:var(--ink);font-size:12px;margin-top:2px}.incident-response{margin-top:16px;padding:11px 12px;border-radius:8px;background:var(--blue-soft);color:#2447a4;font-size:12px}.incident-response strong{display:block;font-size:10px;letter-spacing:.09em;text-transform:uppercase;margin-bottom:3px}.signal-pill{font-size:11px;color:var(--blue);background:var(--blue-soft);padding:4px 7px;border-radius:999px;font-weight:700}.no-signal{font-size:11px;color:var(--muted)}.chain{border-left:2px solid #dfe3ef;margin:22px 0 0 9px;padding-left:19px}.step{position:relative;padding:0 0 22px}.step:before{content:"";position:absolute;left:-26px;top:3px;width:10px;height:10px;border-radius:50%;background:var(--blue);box-shadow:0 0 0 4px var(--blue-soft)}.step h4{margin:0 0 3px;font-size:12px}.step p{margin:0;color:var(--muted);font-size:12px}.drawer{position:fixed;inset:0;background:rgba(11,13,19,.34);z-index:5;display:none}.drawer.open{display:block}.drawer-card{position:absolute;right:0;top:0;height:100%;width:min(650px,100%);background:#fff;overflow:auto;box-shadow:-10px 0 35px rgba(0,0,0,.16);padding:28px}.drawer-close{float:right;border:0;background:#f1f2f4;border-radius:50%;width:30px;height:30px;cursor:pointer}.detail-title{margin:28px 0 5px;font-size:22px;letter-spacing:-.03em}.detail-sub{color:var(--muted);margin-bottom:22px}.detail-grid{display:grid;grid-template-columns:1fr 1fr;gap:10px;margin:16px 0 25px}.detail-item{background:var(--wash);border-radius:8px;padding:11px}.detail-item label{display:block;color:var(--muted);font-size:10px;text-transform:uppercase;letter-spacing:.08em}.detail-item strong{display:block;margin-top:4px;font-size:12px;overflow-wrap:anywhere}.kpi-line{display:flex;gap:20px;color:var(--muted);font-size:12px;flex-wrap:wrap}.kpi-line strong{color:var(--ink)}
@media(max-width:1050px){.metrics{grid-template-columns:repeat(3,1fr)}.incident-grid{grid-template-columns:1fr}}
@media(max-width:850px){.sidebar{width:70px;padding:20px 8px}.brand{padding:0 11px 28px}.brand-name,.workspace,.nav-label,.nav button:not(.active) span,.sidebar-foot{display:none}.nav button{padding:12px;text-align:center}.nav-icon{width:auto}.topbar{padding:0 18px}.search{width:180px}.content{padding:25px 18px}.metrics{grid-template-columns:repeat(2,1fr)}}
@media(max-width:520px){.topbar{height:auto;padding:15px 18px;gap:12px;align-items:flex-start}.context{display:none}.top-actions{width:100%}.search{flex:1}.metrics{grid-template-columns:1fr 1fr;gap:8px}.metric{padding:13px}.metric .value{font-size:19px}.heading{display:block}.dataset{display:inline-block;margin-top:15px}.content{padding-top:23px}.drawer-card{padding:22px 18px}.detail-grid,.incident-times{grid-template-columns:1fr}}
</style>
</head>
<body>
<div class="shell">
<aside class="sidebar">
  <div class="brand"><div class="brand-mark">R</div><div class="brand-name">REVIVE</div></div>
  <div class="workspace">WORKSPACE<strong>Revenue Operations</strong><span>durable PostgreSQL state</span></div>
  <div class="nav-label">Workspace</div>
  <nav class="nav">
    <button data-page="overview" class="active"><span class="nav-icon">■</span><span>Overview</span></button>
    <button data-page="recoveries"><span class="nav-icon">↻</span><span>Recoveries</span></button>
    <button data-page="incidents"><span class="nav-icon">◆</span><span>Population incidents</span></button>
    <button data-page="signals"><span class="nav-icon">◇</span><span>Signals</span></button>
    <button data-page="downtime"><span class="nav-icon">◎</span><span>Downtime</span></button>
    <button data-page="decisions"><span class="nav-icon">✓</span><span>Decisions</span></button>
    <button data-page="audit"><span class="nav-icon">☷</span><span>Audit</span></button>
  </nav>
  <div class="sidebar-foot">REVIVE v0.1<br>Decision infrastructure</div>
</aside>
<main class="main">
  <header class="topbar"><div class="context">WORKSPACE / <strong id="page-context">Operational Overview</strong></div><div class="top-actions"><input class="search" id="search" placeholder="Search payment or order"><span class="pulse" id="pulse"></span><span class="api-status" id="api-status">Loading dashboard</span><div class="avatar">RO</div></div></header>
  <section class="content" id="app"></section>
</main>
</div>
<div class="drawer" id="drawer"><div class="drawer-card"><button class="drawer-close" id="close">×</button><div id="drawer-content"></div></div></div>
<script>
const DATA=__DATA__;
let page="overview";
let filter="all";
const app=document.getElementById("app");
const esc=function(value){return String(value==null?"—":value).replace(/[&<>"']/g,function(c){return {"&":"&amp;","<":"&lt;",">":"&gt;",'"':"&quot;",'\'':"&#39;"}[c]})};
const money=function(value){return "INR "+(Number(value||0)/100).toLocaleString("en-IN",{minimumFractionDigits:2})};
const percent=function(n,d){return d?((Number(n||0)/Number(d))*100).toFixed(2)+"%":"0.00%"};
const date=function(value){return value?new Date(value).toLocaleString("en-IN",{day:"2-digit",month:"short",year:"numeric",hour:"2-digit",minute:"2-digit"}):"—"};
const badge=function(value,cls){const safe=String(value||"none").toLowerCase().replace(/[^a-z0-9_-]/g,"");return '<span class="badge '+esc(cls||safe)+'">'+esc(value||"—")+'</span>'};
const overviewData=function(){return DATA.overview||{metrics:{},recent_cases:[]}};
const incidents=function(){return Array.isArray(DATA.incidents)?DATA.incidents:[]};
const cases=function(){return Array.isArray(DATA.recoveries)?DATA.recoveries:[]};
function head(eyebrow,title,description,extra){return '<div class="heading"><div><div class="eyebrow">'+esc(eyebrow)+'</div><h1>'+esc(title)+'</h1><p>'+esc(description)+'</p></div>'+(extra||"")+'</div>'}
function metric(label,value,note,kind){return '<div class="metric '+esc(kind)+'"><div class="label">'+esc(label)+'</div><div class="value">'+esc(value)+'</div><div class="note">'+esc(note)+'</div></div>'}
function outcomeText(c){if(c.payment_outcome_status==="recovered"){return "Provider-confirmed: recovered "+money(c.payment_outcome_amount)}if(c.payment_outcome_status){return "Provider outcome: "+c.payment_outcome_status}return "No provider-confirmed outcome"}
function signalCell(c){return c.population_signal_detected?'<span class="signal-pill">Systemic signal</span>':'<span class="no-signal">No population signal</span>'}
function incidentCell(c){if(!c.population_incident_active&&!c.population_incident_id){return '<span class="no-signal">No incident context</span>'}return '<span class="mono">'+esc(c.population_incident_id||"incident")+'</span><span class="sub">'+esc(c.population_incident_cohort_key||"Active at decision")+'</span>'}
function executionCell(c){if(!c.execution_status){return '<span class="no-signal">Not required</span>'}return badge(c.execution_status,c.execution_status)+'<span class="sub">Simulated recovery execution</span>'}
function caseRow(c){return '<tr class="case-row search-row" data-case="'+esc(c.case_id)+'"><td><span class="mono">'+esc(c.payment_id)+'</span><span class="sub">'+esc(c.payment_status)+' / '+esc(c.order_status)+'</span></td><td><span class="mono">'+esc(c.order_id)+'</span><span class="sub">'+esc(c.error_code||"Failure reason unavailable")+'</span></td><td><strong>'+money(c.amount)+'</strong></td><td>'+esc(c.diagnosis_category||"—")+'<span class="sub">'+(c.diagnosis_confidence==null?"—":Math.round(c.diagnosis_confidence*100)+"% confidence")+'</span></td><td>'+signalCell(c)+'</td><td>'+incidentCell(c)+'</td><td>'+badge(c.decision,c.decision)+'</td><td>'+executionCell(c)+'</td><td>'+esc(outcomeText(c))+'</td></tr>'}
function caseTable(items){if(!items.length){return '<div class="panel"><div class="empty">No persisted recovery cases match the current view.</div></div>'}return '<div class="panel table-wrap"><table class="table"><thead><tr><th>Payment</th><th>Order / failure</th><th>Amount</th><th>AI diagnosis</th><th>Systemic signal</th><th>Population incident</th><th>Policy</th><th>Execution</th><th>Provider outcome</th></tr></thead><tbody>'+items.map(caseRow).join("")+'</tbody></table></div>'}
function incidentCard(i){return '<article class="incident-card"><div class="incident-top"><div><div class="incident-title">'+esc((i.error_code||"Unknown failure").replace(/_/g," ").toUpperCase())+'</div><div class="incident-meta"><span class="mono">'+esc(i.issuer_bin)+'</span> / '+esc(i.error_code)+'</div></div><span class="status-active">'+esc(i.status)+'</span></div><div class="incident-stats"><div class="incident-stat"><label>Observed at activation</label><strong>'+esc(i.observed_count_at_activation)+' failures</strong></div><div class="incident-stat"><label>Threshold</label><strong>'+esc(i.threshold)+' unique</strong></div><div class="incident-stat"><label>Window</label><strong>'+esc(Math.round(i.window_seconds/60))+' minutes</strong></div></div><div class="incident-times"><div>Activated<strong>'+date(i.activated_at)+'</strong></div><div>Expires<strong>'+date(i.expires_at)+'</strong></div></div><div class="incident-meta" style="margin-top:12px">Trigger payment <span class="mono">'+esc(i.trigger_payment_id)+'</span></div><div class="incident-response"><strong>REVIVE response</strong>Systemic signal detected. Recovery is cooled down to prevent retry amplification.</div></article>'}
function incidentSection(title){const rows=incidents();return '<div class="section-head"><h2>'+esc(title)+'</h2><span>'+rows.length+' active incident'+(rows.length===1?"":"s")+'</span></div>'+(rows.length?'<div class="incident-grid">'+rows.map(incidentCard).join("")+'</div>':'<div class="panel"><div class="empty">No active population incidents. REVIVE is not fabricating incident data.</div></div>')}
function overview(){const m=overviewData().metrics||{};const recent=overviewData().recent_cases||[];const cards=[["Revenue at risk",money(m.revenue_at_risk),"failed amount on unpaid orders","wine"],["Eligible revenue",money(m.policy_eligible_revenue),"persisted recover decisions","blue"],["Revenue recovered",money(m.recovered_revenue),"provider-confirmed outcomes only","green"],["Recovery rate",percent(m.recovered_revenue,m.policy_eligible_revenue),"confirmed revenue / eligible revenue","green"],["Active incidents",incidents().length,"durable unexpired cohorts","blue"],["Recovery actions",m.recovery_actions,"simulated executions only","dark"],["Cooldown / blocked",m.blocked_actions,"systemic safety response","blue"],["Unsafe actions",m.unsafe_actions,"executed actions crossing guardrails","wine"]];return head("Revenue operations","Operational overview","REVIVE detects population-level failure signals, diagnoses context with AI, and lets deterministic policy control recovery.",'<div class="dataset">PostgreSQL source of truth</div>')+'<div class="metrics">'+cards.map(function(item){return metric.apply(null,item)}).join("")+'</div>'+incidentSection("Active population incidents")+'<div class="section-head"><h2>Recent recovery cases</h2><span>'+recent.length+' cases from API</span></div>'+caseTable(recent)}
function recoveries(){const all=cases();const selected=filter==="all"?all:all.filter(function(c){return c.execution_status===filter});return head("Recovery state","Recovery cases","AI diagnosis, deterministic policy, simulated execution, and provider-confirmed outcome remain separate.",'<div class="filters">'+["all","executed","blocked","duplicate"].map(function(x){return '<button class="filter '+(filter===x?"active":"")+'" data-filter="'+x+'">'+x+'</button>'}).join("")+'</div>')+'<div class="notice">Execution success is not payment success. Provider-confirmed payment outcome is the only source of recovered revenue.</div><div class="kpi-line"><span>Executed <strong>'+esc(overviewData().metrics.recovery_actions||0)+'</strong></span><span>Blocked <strong>'+esc(overviewData().metrics.blocked_actions||0)+'</strong></span><span>Duplicates prevented <strong>'+esc(overviewData().metrics.duplicate_actions_prevented||0)+'</strong></span><span>Recovered revenue <strong>'+money(overviewData().metrics.recovered_revenue)+'</strong></span></div><div style="height:14px"></div>'+caseTable(selected)}
function incidentsPage(){return head("Population intelligence","Population incidents","Incidents are activated from unique observed failures within the cohort window. No future failures or evaluator truth are used.")+incidentSection("Active population incidents")+'<div class="notice warning">Systemic failures are cooled down to prevent retry amplification. An ACTIVE incident is evidence at decision time; it is not a payment outcome.</div>'}
function signals(){const rows=Array.isArray(DATA.signals)?DATA.signals:[];if(!rows.length){return head("Population intelligence","Signals","Durable failed-payment groupings by issuer BIN and error code.")+'<div class="panel"><div class="empty">No persisted failed-payment signal groups.</div></div>'}return head("Population intelligence","Signals","Durable failed-payment groupings by issuer BIN and error code.")+'<div class="panel table-wrap"><table class="table"><thead><tr><th>Issuer BIN</th><th>Error code</th><th>Failures</th><th>Total amount</th><th>Latest failed</th></tr></thead><tbody>'+rows.map(function(s){return '<tr class="search-row"><td class="mono">'+esc(s.issuer_bin)+'</td><td>'+esc(s.error_code||"—")+'</td><td><strong>'+esc(s.failure_count)+'</strong></td><td>'+money(s.total_amount)+'</td><td>'+date(s.latest_failed_at)+'</td></tr>'}).join("")+'</tbody></table></div>'}
function downtime(){const source=DATA.downtime||{};if(!source.available){return head("Operational context","Downtime","No persisted downtime source is configured for this environment.")+'<div class="notice">No persisted downtime source configured. REVIVE is not fabricating downtime records.</div>'}const items=source.items||[];if(!items.length){return head("Operational context","Downtime","No downtime records are currently available.")+'<div class="panel"><div class="empty">No downtime records.</div></div>'}return head("Operational context","Downtime","Persisted downtime information available to the backend.")+'<div class="panel"><div class="empty">'+items.map(function(item){return esc(JSON.stringify(item))}).join("<br>")+'</div></div>'}
function decisions(){const rows=Array.isArray(DATA.decisions)?DATA.decisions:[];if(!rows.length){return head("Decision record","Decisions","Persisted deterministic recovery decisions.")+'<div class="panel"><div class="empty">No persisted decisions.</div></div>'}return head("Decision record","Decisions","The diagnosis is evidence. The persisted deterministic policy decision is authority.")+'<div class="panel table-wrap"><table class="table"><thead><tr><th>Decision</th><th>Payment / order</th><th>Amount</th><th>Diagnosis</th><th>Reason</th><th>Time</th></tr></thead><tbody>'+rows.map(function(d){return '<tr class="case-row search-row" data-case="'+esc(d.case_id)+'"><td>'+badge(d.decision,d.decision)+'</td><td><span class="mono">'+esc(d.payment_id)+'</span><span class="sub">'+esc(d.order_id)+' / '+esc(d.payment_status)+' / '+esc(d.order_status)+'</span></td><td>'+money(d.amount)+'</td><td>'+esc(d.diagnosis_category||"—")+'<span class="sub">'+(d.diagnosis_confidence==null?"—":Math.round(d.diagnosis_confidence*100)+"% confidence")+'</span></td><td>'+esc(d.decision_reason)+'</td><td>'+date(d.decision_created_at)+'</td></tr>'}).join("")+'</tbody></table></div>'}
function audit(){const rows=Array.isArray(DATA.audit)?DATA.audit:[];if(!rows.length){return head("Traceability","Audit","Persisted recovery and provider reconciliation events.")+'<div class="panel"><div class="empty">No persisted audit events.</div></div>'}return head("Traceability","Audit","A chronological record of policy, simulated execution, and provider reconciliation.")+'<div class="panel table-wrap"><table class="table"><thead><tr><th>Time</th><th>Case</th><th>Payment / order</th><th>Policy</th><th>Status</th><th>Action</th><th>Reason</th></tr></thead><tbody>'+rows.map(function(a){return '<tr class="search-row"><td>'+date(a.timestamp)+'</td><td class="mono">'+esc(a.case_id)+'</td><td><span class="mono">'+esc(a.payment_id)+'</span><span class="sub">'+esc(a.order_id)+' / '+money(a.amount)+'</span></td><td>'+badge(a.policy_decision,a.policy_decision)+'</td><td>'+badge(a.status,a.status)+'</td><td>'+esc(a.action||"No execution")+'</td><td>'+esc(a.reason)+'</td></tr>'}).join("")+'</tbody></table></div>'}
function render(){if(!DATA.available){app.innerHTML='<div class="error-state"><h2>API unavailable</h2><p>'+esc(DATA.error||"FastAPI/PostgreSQL dashboard is unavailable.")+'</p><p style="margin-top:14px">Start the FastAPI service and reload this page. REVIVE does not generate substitute dashboard data.</p></div>';return}const views={overview:overview,recoveries:recoveries,incidents:incidentsPage,signals:signals,downtime:downtime,decisions:decisions,audit:audit};app.innerHTML=(views[page]||overview)();document.querySelectorAll(".nav button").forEach(function(button){button.classList.toggle("active",button.dataset.page===page)});document.querySelectorAll(".filter").forEach(function(button){button.addEventListener("click",function(){filter=button.dataset.filter;render()})});document.querySelectorAll(".case-row[data-case]").forEach(function(row){row.addEventListener("click",function(){openCase(row.dataset.case)})});applySearch()}
function applySearch(){const term=(document.getElementById("search").value||"").trim().toLowerCase();document.querySelectorAll(".search-row").forEach(function(row){row.style.display=!term||row.textContent.toLowerCase().includes(term)?"":"none"})}
function openCase(caseId){const c=cases().find(function(item){return item.case_id===caseId});if(!c){return}const steps=[{title:"Payment failed",text:c.payment_id+" · "+(c.error_code||"failure reason unavailable")+" · "+money(c.amount)}];if(c.population_signal_detected){steps.push({title:"Systemic signal",text:c.population_incident_active?"Population signal detected with an ACTIVE incident at decision time.":"Observed systemic signal detected without an active incident context."})}if(c.population_incident_active||c.population_incident_id){steps.push({title:"Population incident",text:(c.population_incident_cohort_key||"Incident context")+" · "+(c.population_incident_id||"incident")+" · activated "+date(c.population_incident_activated_at)})}steps.push({title:"AI diagnosis",text:(c.diagnosis_category||"No diagnosis")+" · "+(c.diagnosis_reason||"No diagnosis reason persisted.")});steps.push({title:"Policy decision",text:(c.decision||"No decision")+" · "+(c.decision_reason||"No policy decision persisted.")});steps.push({title:"Recovery execution",text:c.execution_status?((c.execution_status==="executed"?"Simulated recovery execution recorded; this is not a payment outcome.":"No recovery execution; policy blocked or cooled down.")+(c.execution_reason?" "+c.execution_reason:"")):"No execution record."});steps.push({title:"Provider-confirmed outcome",text:c.payment_outcome_status==="recovered"?"Recovered "+money(c.payment_outcome_amount)+" after provider confirmation.":"No provider-confirmed outcome. Revenue remains unrecovered."});document.getElementById("drawer-content").innerHTML='<div class="eyebrow">Case trace</div><h2 class="detail-title">'+esc(c.case_id)+'</h2><div class="detail-sub">'+esc(c.payment_id)+' / '+esc(c.order_id)+'</div><div class="detail-grid"><div class="detail-item"><label>Amount</label><strong>'+money(c.amount)+'</strong></div><div class="detail-item"><label>Policy</label><strong>'+esc(c.decision||"—")+'</strong></div><div class="detail-item"><label>Signal</label><strong>'+esc(c.population_signal_detected?"Systemic signal detected":"No population signal")+'</strong></div><div class="detail-item"><label>Provider outcome</label><strong>'+esc(outcomeText(c))+'</strong></div></div><div class="chain">'+steps.map(function(step){return '<div class="step"><h4>'+esc(step.title)+'</h4><p>'+esc(step.text)+'</p></div>'}).join("")+'</div>';document.getElementById("drawer").classList.add("open")}
document.querySelectorAll(".nav button").forEach(function(button){button.addEventListener("click",function(){page=button.dataset.page;render()})});document.getElementById("search").addEventListener("input",applySearch);document.getElementById("close").addEventListener("click",function(){document.getElementById("drawer").classList.remove("open")});document.getElementById("drawer").addEventListener("click",function(event){if(event.target.id==="drawer"){event.currentTarget.classList.remove("open")}});document.getElementById("api-status").textContent=DATA.available?"Live durable state":"API unavailable";document.getElementById("pulse").classList.toggle("offline",!DATA.available);render();
</script>
</body></html>"""


class Handler(BaseHTTPRequestHandler):
    def do_GET(self):
        if self.path.split("?", 1)[0] not in ("/", "/index.html"):
            self.send_error(404)
            return
        body = render_page(fetch_dashboard_data())
        self.send_response(200)
        self.send_header("Content-Type", "text/html; charset=utf-8")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def log_message(self, format, *args):
        return


if __name__ == "__main__":
    frontend_port = int(os.getenv("REVIVE_FRONTEND_PORT", "3000"))
    print(f"REVIVE UI running at http://127.0.0.1:{frontend_port}")
    ThreadingHTTPServer(("127.0.0.1", frontend_port), Handler).serve_forever()
