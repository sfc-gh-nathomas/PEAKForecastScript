"""
PEAK Qualify & Commit — Streamlit App
Interactive version of the PEAK report with GVP and Sales Play filters.

Usage:
    streamlit run peak_app.py
"""

import sys
import os
import html as html_lib
from datetime import datetime
from collections import defaultdict
from concurrent.futures import ThreadPoolExecutor, as_completed

import streamlit as st

# Import data layer from existing report
from peak_report import (
    CONFIG, RISK_CATEGORIES,
    fmt_currency, fmt_pct, fmt_int, safe_str, html_escape,
    get_forecast_class, get_forecast_label, truncate_text, extract_latest_comment,
    get_connection, run_query,
    q_fiscal_calendar, q_qtd_revenue, q_forecast_calls, q_deployed_qtd,
    q_last7_deployed, q_open_pipeline, q_pipeline_risk, q_top5_use_cases,
    q_sales_play_summary, q_play_detail_metrics, q_bronze_tb_total,
    q_play_use_cases, q_prior_fy_pacing, q_play_risk_detail,
    q_high_risk_use_cases, q_consumption, q_si_theater_totals, q_si_usage,
    q_bronze_tb_by_account, q_play_targets, q_partner_sd_attach,
    q_use_case_velocity, q_bronze_created_qtd,
    q_current_pipeline_phases, q_historical_conversion_rates,
    q_backtest_models, compute_forecast_analysis, compute_weighted_ensemble,
    q_deployment_velocity, q_risk_adjusted_pipeline_detail,
    build_risk_narrative, build_use_case_row,
    update_risk_thresholds_from_velocity,
    _build_forecast_tab,
)

# ---------------------------------------------------------------------------
# Page config
# ---------------------------------------------------------------------------
st.set_page_config(
    page_title="PEAK Qualify & Commit",
    page_icon="📊",
    layout="wide",
    initial_sidebar_state="expanded",
)

# ---------------------------------------------------------------------------
# CSS — reuse styles from the HTML report, adapted for Streamlit
# ---------------------------------------------------------------------------
STREAMLIT_CSS = """
<style>
    body { font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, sans-serif; margin: 0; padding: 0; color: #333; }
    .forecast-table { border-collapse: collapse; margin: 10px 0; }
    .forecast-table th, .forecast-table td { padding: 10px 16px; text-align: left; border: 1px solid #ddd; }
    .forecast-table th { background: #29B5E8; color: white; }
    .forecast-table td { background: white; }
    .use-case-table { width: 100%; border-collapse: collapse; margin: 15px 0; }
    .use-case-table th { background: #1a1a2e; color: white; padding: 12px; text-align: left; }
    .use-case-table td { padding: 12px; border: 1px solid #ddd; background: white; vertical-align: top; }
    .use-case-table tr:nth-child(even) td { background: #fafafa; }
    .sales-play-table { width: 100%; border-collapse: collapse; margin: 15px 0; }
    .sales-play-table th { background: #1a1a2e; color: white; padding: 10px 12px; text-align: left; }
    .sales-play-table td { padding: 10px 12px; border: 1px solid #ddd; background: white; }
    .sales-play-table .number { text-align: right; font-family: 'SF Mono', Consolas, monospace; }
    a { color: #29B5E8; text-decoration: none; font-weight: 600; }
    a:hover { text-decoration: underline; }
    .acv { font-size: 1.1em; font-weight: bold; color: #1a1a2e; }
    .status-commit { background: #28a745; color: white; padding: 3px 8px; border-radius: 4px; font-size: 0.85em; }
    .status-likely { background: #ffc107; color: #1a1a2e; padding: 3px 8px; border-radius: 4px; font-size: 0.85em; }
    .status-stretch { background: #dc3545; color: white; padding: 3px 8px; border-radius: 4px; font-size: 0.85em; }
    .status-none { background: #6c757d; color: white; padding: 3px 8px; border-radius: 4px; font-size: 0.85em; }
    .label { font-weight: 600; color: #555; }
    .date { color: #666; }
    .stage { background: #e9ecef; color: #495057; padding: 3px 8px; border-radius: 4px; font-size: 0.85em; }
    .details-col .details-row { margin-bottom: 6px; line-height: 1.5; }
    .risk { color: #dc3545; font-weight: 500; }
    .next-steps { color: #0d6efd; font-style: italic; margin-top: 8px; }
    .se-comments { font-size: 0.9em; color: #2c3e50; margin-top: 8px; font-style: italic; }
    .ae-name { color: #6f42c1; }
    .summary { color: #444; font-size: 0.95em; line-height: 1.5; }
    .uc-number { font-size: 0.85em; color: #666; }
    .consumption { font-size: 0.8em; color: #17a2b8; font-weight: 500; }
    .bronze-tb { font-size: 0.8em; color: #cd7f32; font-weight: 500; }
    .si-metrics { font-size: 0.8em; color: #9b59b6; font-weight: 500; }
    .open-acv { color: #dc3545; font-weight: 600; }
    .deployed-acv { color: #28a745; font-weight: 600; }
    .implementer { font-size: 0.85em; color: #2c3e50; margin-top: 10px; }
    .partner { font-size: 0.85em; color: #8e44ad; }
    .play-section { margin-bottom: 30px; }
    .analysis { background: #e8f4f8; border-left: 4px solid #29B5E8; padding: 15px; margin: 15px 0; font-size: 0.95em; line-height: 1.6; }
    .risk-box { background: #fff5f5; border-left: 4px solid #dc3545; padding: 15px; margin: 15px 0; }
    .timeline-box { display: flex; align-items: center; justify-content: center; gap: 12px; padding: 10px 0; margin: 5px 0; }
    .timeline-item { text-align: center; background: #1a1a2e; border-radius: 8px; padding: 12px 20px; min-width: 120px; box-shadow: 0 3px 10px rgba(0,0,0,0.15); }
    .timeline-label { display: block; font-size: 0.8em; color: #aaa; margin-bottom: 6px; text-transform: uppercase; letter-spacing: 0.5px; }
    .timeline-days { display: block; font-size: 2em; font-weight: bold; color: #29B5E8; }
    .timeline-arrow { font-size: 3em; color: #29B5E8; font-weight: bold; }
    /* Forecast analysis styles */
    .fa-grid { display: grid; grid-template-columns: 1fr 1fr; gap: 20px; margin: 20px 0; }
    .fa-card { background: white; border-radius: 8px; padding: 20px; box-shadow: 0 2px 8px rgba(0,0,0,0.08); border-top: 4px solid #29B5E8; }
    .fa-card h4 { margin: 0 0 15px 0; color: #1a1a2e; }
    .fa-card.method1 { border-top-color: #6f42c1; }
    .fa-card.method2 { border-top-color: #28a745; }
    .fa-card.method3 { border-top-color: #17a2b8; }
    .fa-card.recommended { border-top-color: #29B5E8; grid-column: 1 / -1; }
    .fa-table { width: 100%; border-collapse: collapse; margin: 10px 0; }
    .fa-table th { background: #1a1a2e; color: white; padding: 10px 15px; text-align: left; font-size: 0.9em; }
    .fa-table td { padding: 10px 15px; border: 1px solid #eee; font-size: 0.95em; }
    .fa-table tr:nth-child(even) td { background: #fafafa; }
    .fa-table .number { text-align: right; font-family: 'SF Mono', Consolas, monospace; }
    .fa-highlight { font-size: 1.8em; font-weight: bold; color: #1a1a2e; }
    .fa-sublabel { font-size: 0.8em; color: #666; margin-top: 2px; }
    .fa-bar { height: 24px; border-radius: 4px; display: inline-block; vertical-align: middle; }
    .fa-vs { display: flex; gap: 30px; align-items: center; justify-content: center; margin: 15px 0; flex-wrap: wrap; }
    .fa-vs-item { text-align: center; min-width: 140px; }
    .fa-vs-label { font-size: 0.75em; text-transform: uppercase; color: #888; letter-spacing: 0.5px; }
    .fa-vs-value { font-size: 1.6em; font-weight: bold; }
    .fa-vs-value.commit { color: #28a745; }
    .fa-vs-value.likely { color: #ffc107; }
    .fa-vs-value.stretch { color: #dc3545; }
    .fa-note { background: #f8f9fa; border-left: 3px solid #6c757d; padding: 10px 15px; margin: 10px 0; font-size: 0.85em; color: #555; line-height: 1.5; }
</style>
"""

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------
GVP_OPTIONS = [
    "Dayne Turbitt",
    "Jennifer Chronis",
    "Jon Robertson",
    "Jonathan Beaulier",
    "Keegan Riley",
    "Mark Fleming",
]

PLAY_OPTIONS = {
    "Bronze (Make Your Data AI Ready)": "bronze",
    "Snowflake Intelligence (AI: Snowflake Intelligence & Agents)": "si",
    "SQL Server Migration (Modernize Your Data Estate)": "sqlserver",
}


# ---------------------------------------------------------------------------
# Snowflake connection (cached singleton + pool for concurrency)
# ---------------------------------------------------------------------------
@st.cache_resource
def get_sf_connection():
    """Create and cache a Snowflake connection."""
    return get_connection()


@st.cache_resource
def get_sf_connection_pool(_pool_size=8):
    """Create a pool of Snowflake connections for concurrent queries."""
    return [get_connection() for _ in range(_pool_size)]


# ---------------------------------------------------------------------------
# Data loading — all queries for a given GVP, with progress bar + concurrency
# ---------------------------------------------------------------------------
def _run_all_queries(gvp_name):
    """Run all queries with concurrent execution and a visible progress bar."""
    CONFIG["gvp_name"] = gvp_name
    conn = get_sf_connection()
    pool_conns = get_sf_connection_pool()

    total_steps = 30
    completed = [0]  # mutable for closure
    progress = st.progress(0, text="Connecting to Snowflake...")

    def _tick(label):
        completed[0] += 1
        progress.progress(min(completed[0] / total_steps, 1.0), text=label)

    # --- Phase 1: Fiscal calendar (must run first, populates CONFIG dates) ---
    _tick("Phase 1: Fiscal calendar...")
    fiscal = q_fiscal_calendar(conn)
    CONFIG["day_number"] = int(fiscal["DAY_NUMBER"])

    # --- Phase 1.5: Velocity (must run before risk queries to set thresholds) ---
    _tick("Phase 1.5: Use case velocity...")
    uc_velocity = q_use_case_velocity(conn)
    update_risk_thresholds_from_velocity(uc_velocity)

    # --- Phase 2: Run independent queries concurrently ---
    _tick("Phase 2: Running queries in parallel...")

    results = {}

    def _run(name, fn, *args):
        """Execute a query function and store the result."""
        results[name] = fn(*args)

    # Build (name, fn, args) tuples — each gets a connection from the pool
    phase2_queries = [
        ("revenue", q_qtd_revenue),
        ("forecasts", q_forecast_calls),
        ("deployed", q_deployed_qtd),
        ("last7", q_last7_deployed),
        ("pipeline", q_open_pipeline),
        ("risk_analysis", q_pipeline_risk),
        ("top5", q_top5_use_cases),
        ("play_summary", q_sales_play_summary),
        ("play_detail", q_play_detail_metrics),
        ("bronze_tb_total", q_bronze_tb_total),
        ("play_use_cases", q_play_use_cases),
        ("play_risk", q_play_risk_detail),
        ("high_risk_ucs", q_high_risk_use_cases),
        ("play_targets", q_play_targets),
        ("partner_sd", q_partner_sd_attach),
        ("bronze_created", q_bronze_created_qtd),
        ("si_theater", q_si_theater_totals),
        ("bronze_tb_acct", q_bronze_tb_by_account),
        ("velocity", q_deployment_velocity),
        ("pipeline_detail", q_risk_adjusted_pipeline_detail),
        ("pipeline_phases", q_current_pipeline_phases),
    ]
    # These need extra args beyond conn
    phase2_extra = [
        ("pacing", q_prior_fy_pacing, fiscal["DAY_NUMBER"], fiscal["WEEK_NUMBER"]),
        ("hist_conv_rates", q_historical_conversion_rates, fiscal["DAY_NUMBER"]),
    ]

    label_map = {
        "revenue": "QTD revenue", "forecasts": "Forecast calls",
        "deployed": "Deployed QTD", "last7": "Last 7 days",
        "pipeline": "Open pipeline", "risk_analysis": "Pipeline risk",
        "top5": "Top 5 UCs", "play_summary": "Play summary",
        "play_detail": "Play detail", "bronze_tb_total": "Bronze TB",
        "play_use_cases": "Play use cases", "play_risk": "Play risk detail",
        "high_risk_ucs": "High risk UCs (Cortex AI)", "play_targets": "Play targets",
        "partner_sd": "Partner/SD attach",
        "bronze_created": "Bronze created", "si_theater": "SI theater totals",
        "bronze_tb_acct": "Bronze TB by acct", "velocity": "Deployment velocity",
        "pipeline_detail": "Risk-adj pipeline", "pipeline_phases": "Pipeline phases",
        "pacing": "Pacing", "hist_conv_rates": "Historical conv rates",
    }

    with ThreadPoolExecutor(max_workers=8) as executor:
        futures = {}
        all_jobs = []
        for name, fn in phase2_queries:
            all_jobs.append((name, fn))
        for name, fn, *extra in phase2_extra:
            all_jobs.append((name, fn, *extra))

        for idx, job in enumerate(all_jobs):
            name = job[0]
            fn = job[1]
            extra_args = job[2:] if len(job) > 2 else ()
            c = pool_conns[idx % len(pool_conns)]
            fut = executor.submit(_run, name, fn, c, *extra_args)
            futures[fut] = label_map.get(name, name)

        for future in as_completed(futures):
            label = futures[future]
            try:
                future.result()
                _tick(f"Completed: {label}")
            except Exception as e:
                _tick(f"FAILED: {label}")
                raise e

    # --- Phase 3: Queries needing results from phase 2 ---
    _tick("Phase 3: Consumption & SI usage...")
    all_account_ids = set()
    for uc in results["top5"]:
        all_account_ids.add(safe_str(uc.get("ACCOUNT_ID")))
    for play in results["play_use_cases"].values():
        for uc in play:
            all_account_ids.add(safe_str(uc.get("ACCOUNT_ID")))
    all_account_ids.discard("")

    si_account_ids = set()
    for uc in results["play_use_cases"].get("si", []):
        si_account_ids.add(safe_str(uc.get("ACCOUNT_ID")))
    si_account_ids.discard("")

    with ThreadPoolExecutor(max_workers=2) as executor:
        f1 = executor.submit(_run, "consumption", q_consumption, pool_conns[0], all_account_ids)
        f2 = executor.submit(_run, "si_usage", q_si_usage, pool_conns[1], si_account_ids)
        f1.result()
        f2.result()
    _tick("Completed: Consumption & SI usage")

    # --- Phase 4: Forecast analysis (CPU-bound, fast) ---
    _tick("Phase 4: Forecast models...")
    forecast_analysis = compute_forecast_analysis(
        results["pipeline_phases"], results["hist_conv_rates"],
        results["deployed"], results["risk_analysis"],
        results["forecasts"]["most_likely"], results["pacing"]
    )

    _tick("Backtest models...")
    backtest_results = q_backtest_models(pool_conns[0], results["hist_conv_rates"])
    forecast_analysis["backtest"] = backtest_results

    _tick("Weighted ensemble...")
    compute_weighted_ensemble(forecast_analysis, backtest_results, day_number=int(fiscal["DAY_NUMBER"]))
    forecast_analysis["velocity"] = results["velocity"]
    forecast_analysis["pipeline_detail"] = results["pipeline_detail"]

    progress.progress(1.0, text="Done!")
    progress.empty()

    return {
        "fiscal": fiscal,
        "revenue": results["revenue"],
        "forecasts": results["forecasts"],
        "deployed": results["deployed"],
        "last7": results["last7"],
        "pipeline": results["pipeline"],
        "risk_analysis": results["risk_analysis"],
        "top5": results["top5"],
        "play_summary": results["play_summary"],
        "play_detail": results["play_detail"],
        "play_use_cases": results["play_use_cases"],
        "play_risk": results["play_risk"],
        "high_risk_ucs": results["high_risk_ucs"],
        "consumption": results["consumption"],
        "bronze_tb_total": results["bronze_tb_total"],
        "bronze_tb_acct": results["bronze_tb_acct"],
        "si_usage": results["si_usage"],
        "si_theater": results["si_theater"],
        "pacing": results["pacing"],
        "forecast_analysis": forecast_analysis,
        "play_targets": results["play_targets"],
        "partner_sd": results["partner_sd"],
        "uc_velocity": uc_velocity,
        "bronze_created": results["bronze_created"],
        # Snapshot of CONFIG values at query time (restored on cache hit for
        # functions like _build_forecast_tab that read CONFIG directly)
        "_config": {
            "gvp_name": CONFIG.get("gvp_name"),
            "quarter_start": CONFIG.get("quarter_start"),
            "quarter_end": CONFIG.get("quarter_end"),
            "fiscal_year": CONFIG.get("fiscal_year"),
            "fiscal_year_label": CONFIG.get("fiscal_year_label"),
            "prior_fy_label": CONFIG.get("prior_fy_label"),
            "prior_fy_avg_final": CONFIG.get("prior_fy_avg_final"),
            "prior_fy_quarters": CONFIG.get("prior_fy_quarters"),
            "day_number": CONFIG.get("day_number"),
            "days_to_tw": CONFIG.get("days_to_tw"),
            "days_to_imp": CONFIG.get("days_to_imp"),
            "days_to_deploy": CONFIG.get("days_to_deploy"),
            "risk_thresholds": CONFIG.get("risk_thresholds"),
            "output_dir": CONFIG.get("output_dir"),
        },
        "_loaded_at": datetime.now(),
    }


def load_all_data(gvp_name):
    """Load data with session_state caching (10-min TTL) and progress bar."""
    cache_key = f"peak_data_{gvp_name}"
    cached = st.session_state.get(cache_key)

    if cached is not None:
        loaded_at = cached.get("_loaded_at")
        if loaded_at and (datetime.now() - loaded_at).total_seconds() < 600:
            return cached

    data = _run_all_queries(gvp_name)
    st.session_state[cache_key] = data
    return data


# ---------------------------------------------------------------------------
# Helper: format WoW delta
# ---------------------------------------------------------------------------
def fmt_delta_html(delta):
    """Format forecast delta as small WoW change HTML."""
    if delta is None:
        return ""
    if delta == 0:
        return '<br><span style="font-size: 0.8em; color: #888;">Flat WoW</span>'
    sign = "+" if delta > 0 else ""
    color = "#28a745" if delta > 0 else "#dc3545"
    return f'<br><span style="font-size: 0.8em; color: {color};">{sign}{fmt_currency(delta)} WoW</span>'


def fmt_delta_text(delta):
    """Format forecast delta as plain text for script."""
    if delta is None:
        return ""
    if delta == 0:
        return " (flat WoW)"
    if delta > 0:
        return f" (+{fmt_currency(delta)} WoW)"
    else:
        return f" (-{fmt_currency(abs(delta))} WoW)"


# ---------------------------------------------------------------------------
# Helper: risk line for pipeline risk
# ---------------------------------------------------------------------------
def risk_line(risk_analysis, stage_group):
    r = risk_analysis.get(stage_group, {})
    if not r:
        return ""
    total = fmt_currency(r.get("TOTAL_ACV", 0), compact=True)
    total_count = int(r.get("TOTAL_COUNT", 0))
    at_risk = fmt_currency(r.get("AT_RISK_ACV", 0), compact=True)
    at_risk_count = int(r.get("AT_RISK_COUNT", 0))
    good = fmt_currency(r.get("GOOD_ACV", 0), compact=True)
    good_count = total_count - at_risk_count
    if stage_group == "Stage 6":
        return f"&bull; Stage 6: {total_count} total ({total}) &mdash; all good<br>"
    return f"&bull; {stage_group}: {total_count} total ({total}) &mdash; {at_risk_count} at risk ({at_risk}), {good_count} good ({good})<br>"


# ---------------------------------------------------------------------------
# Helper: play target formatting
# ---------------------------------------------------------------------------
def fmt_play_target(play_targets, play_key):
    t = play_targets.get(play_key, {})
    acv = t.get("acv")
    count = t.get("count")
    parts = []
    if acv is not None:
        parts.append(fmt_currency(acv))
    if count is not None:
        parts.append(f"{count} UCs")
    return " / ".join(parts) if parts else "&mdash;"


def fmt_play_gap(play_targets, play_key, deployed_acv, deployed_count):
    t = play_targets.get(play_key, {})
    target_acv = t.get("acv")
    target_count = t.get("count")
    parts = []
    if target_acv is not None:
        gap_acv = deployed_acv - target_acv
        parts.append(fmt_currency(gap_acv))
    if target_count is not None:
        gap_count = deployed_count - target_count
        parts.append(f"{gap_count} UCs")
    return " / ".join(parts) if parts else "&mdash;"


# ---------------------------------------------------------------------------
# Helper: build high-risk use case table HTML
# ---------------------------------------------------------------------------
def build_high_risk_table_html(high_risk_ucs):
    if not high_risk_ucs:
        return ""
    hr_rows_html = ""
    for i, uc in enumerate(high_risk_ucs):
        uc_id = safe_str(uc.get("USE_CASE_ID", ""))
        uc_num = html_escape(safe_str(uc.get("USE_CASE_NUMBER", "")))
        uc_name = html_escape(safe_str(uc.get("USE_CASE_NAME", "")))
        ae = html_escape(safe_str(uc.get("AE_NAME", "")))
        se = html_escape(safe_str(uc.get("SE_NAME", "")))
        acv = float(uc.get("USE_CASE_ACV", 0) or 0)
        risk_type = html_escape(safe_str(uc.get("RISK_TYPE", "")))
        raw_summary = safe_str(uc.get("RISK_SUMMARY", ""))
        for prefix in ["Here is a ", "Here's a ", "Here is the ", "Here's the "]:
            if raw_summary.startswith(prefix):
                colon_idx = raw_summary.find(":\n")
                if colon_idx != -1:
                    raw_summary = raw_summary[colon_idx + 1:].strip()
                break
        risk_summary = html_escape(raw_summary)
        sf_link = f"https://snowforce.lightning.force.com/lightning/r/{uc_id}/view" if uc_id else ""
        uc_num_cell = f'<a href="{sf_link}" target="_blank" style="color: #007bff; text-decoration: none;">{uc_num}</a>' if sf_link else uc_num
        row_bg = ' style="background: #f8f9fa;"' if i % 2 == 1 else ""
        hr_rows_html += f"""<tr{row_bg}>
<td style="padding: 5px 8px; border-bottom: 1px solid #eee;">{uc_num_cell}</td>
<td style="padding: 5px 8px; border-bottom: 1px solid #eee;">{uc_name}</td>
<td style="padding: 5px 8px; border-bottom: 1px solid #eee;">{ae}</td>
<td style="padding: 5px 8px; border-bottom: 1px solid #eee;">{se}</td>
<td style="padding: 5px 8px; border-bottom: 1px solid #eee; text-align: right;">{fmt_currency(acv)}</td>
<td style="padding: 5px 8px; border-bottom: 1px solid #eee;">{risk_type}</td>
<td style="padding: 5px 8px; border-bottom: 1px solid #eee; font-size: 0.85em;">{risk_summary}</td>
</tr>"""

    return f"""
<div style="margin-top: 12px;">
<strong>At-Risk Use Cases ({len(high_risk_ucs)}):</strong>
<table style="width: 100%; border-collapse: collapse; margin-top: 8px; font-size: 0.85em;">
<tr style="background: #e9ecef; font-weight: bold;">
<th style="padding: 6px 8px; text-align: left; border-bottom: 2px solid #dee2e6;">UC Number</th>
<th style="padding: 6px 8px; text-align: left; border-bottom: 2px solid #dee2e6;">Name</th>
<th style="padding: 6px 8px; text-align: left; border-bottom: 2px solid #dee2e6;">AE</th>
<th style="padding: 6px 8px; text-align: left; border-bottom: 2px solid #dee2e6;">SE</th>
<th style="padding: 6px 8px; text-align: right; border-bottom: 2px solid #dee2e6;">ACV</th>
<th style="padding: 6px 8px; text-align: left; border-bottom: 2px solid #dee2e6;">Risk Type</th>
<th style="padding: 6px 8px; text-align: left; border-bottom: 2px solid #dee2e6;">Risk Summary</th>
</tr>
{hr_rows_html}
</table>
</div>"""


# ===========================================================================
# TAB 1: SCRIPT
# ===========================================================================
def render_script_tab(data, selected_play_key):
    """Render the QC Script tab, showing only the selected play's detail."""
    fiscal = data["fiscal"]
    forecasts = data["forecasts"]
    deployed = data["deployed"]
    pipeline = data["pipeline"]
    risk_analysis = data["risk_analysis"]
    play_summary = data["play_summary"]
    play_detail = data["play_detail"]
    play_risk = data["play_risk"]
    high_risk_ucs = data["high_risk_ucs"]
    play_targets = data["play_targets"]
    partner_sd = data["partner_sd"]
    uc_velocity = data["uc_velocity"]
    bronze_created = data["bronze_created"]
    bronze_tb_total = data["bronze_tb_total"]
    si_theater = data["si_theater"]
    pacing = data["pacing"]
    cfg = data["_config"]

    most_likely = forecasts["most_likely"]
    gl_target = forecasts["target"]
    deployed_pct = (deployed["acv"] / most_likely * 100) if most_likely else 0
    deployed_pct_of_target = (deployed["acv"] / gl_target * 100) if gl_target else 0
    coverage_pct = ((pipeline["acv"] + deployed["acv"]) / most_likely * 100) if most_likely else 0

    total_good = sum(float(r.get("GOOD_ACV", 0) or 0) for r in risk_analysis.values())
    total_risk_acv = sum(float(r.get("AT_RISK_ACV", 0) or 0) for r in risk_analysis.values())
    good_coverage = ((total_good + deployed["acv"]) / most_likely * 100) if most_likely else 0

    ml_wow_text = fmt_delta_text(forecasts.get("ml_delta"))

    # Pacing
    day_number = int(fiscal["DAY_NUMBER"])
    week_number = int(fiscal["WEEK_NUMBER"])
    day_avg = pacing["day_avg"]
    day_pct_val = pacing["day_pct"]
    week_avg = pacing["week_avg"]
    week_pct_val = pacing["week_pct"]

    # Projections: if prior FY was X% deployed by day/week N, project current final
    day_projection = (deployed["acv"] / (day_pct_val / 100)) if day_pct_val > 0 else None
    week_projection = (deployed["acv"] / (week_pct_val / 100)) if week_pct_val > 0 else None

    # Partner/SD attach
    p_rate = partner_sd.get("partner_rate", 0)
    sd_rate = partner_sd.get("sd_rate", 0)
    p_acv = partner_sd.get("partner_acv", 0)
    p_cnt = partner_sd.get("partner_count", 0)
    sd_acv_val = partner_sd.get("sd_acv", 0)
    sd_cnt = partner_sd.get("sd_count", 0)
    unassisted_acv = partner_sd.get("unassisted_acv", 0)
    unassisted_cnt = partner_sd.get("unassisted_count", 0)
    unassisted_rate = (unassisted_acv / partner_sd.get("total_acv", 1) * 100) if partner_sd.get("total_acv") else 0

    # Velocity
    uc_vel_cur = uc_velocity.get("current", {})
    uc_vel_prior = uc_velocity.get("prior", {})
    v_tw = uc_vel_cur.get("time_to_tw")
    v_imp = uc_vel_cur.get("won_to_imp_start")
    v_dep = uc_vel_cur.get("won_to_deployed")
    v_tw_str = f"{v_tw:.0f}" if v_tw is not None else "N/A"
    v_imp_str = f"{v_imp:.0f}" if v_imp is not None else "N/A"
    v_dep_str = f"{v_dep:.0f}" if v_dep is not None else "N/A"
    pq_label = cfg.get("prior_fy_label", "Prior") + " Q4"

    def _vel_prior_sub(cur, prior_val):
        if prior_val is None:
            return ""
        return f'<span style="display: block; font-size: 0.45em; color: #888; margin-top: 4px;">{pq_label}: {prior_val:.0f}</span>'

    v_tw_prior = _vel_prior_sub(v_tw, uc_vel_prior.get("time_to_tw"))
    v_imp_prior = _vel_prior_sub(v_imp, uc_vel_prior.get("won_to_imp_start"))
    v_dep_prior = _vel_prior_sub(v_dep, uc_vel_prior.get("won_to_deployed"))

    # High risk table
    high_risk_table_html = build_high_risk_table_html(high_risk_ucs)

    # --- Build the common script sections ---
    script_html = f"""
<div style="background: #f8f9fa; border: 1px solid #dee2e6; border-radius: 8px; padding: 24px; font-family: Georgia, serif; font-size: 1.05em; line-height: 1.7;">

<h3 style="color: #333; border-bottom: 2px solid #007bff; padding-bottom: 8px;">Forecast Call</h3>
<p>For AMSExpansion, my Most Likely call for go-lives this quarter is <strong>{fmt_currency(most_likely)}</strong>{ml_wow_text}.
We have deployed <strong>{fmt_currency(deployed["acv"])}</strong> QTD against a target of <strong>{fmt_currency(gl_target)}</strong> (<strong>{fmt_pct(deployed_pct_of_target)}</strong> of target).
Our open pipeline is <strong>{fmt_currency(pipeline["acv"])}</strong>, giving us <strong>{fmt_pct(coverage_pct)}</strong> ML coverage (deployed + open pipeline vs Most Likely).</p>

<h3 style="color: #333; border-bottom: 2px solid #007bff; padding-bottom: 8px;">Risk</h3>
<p>Total pipeline risk stands at <strong>{fmt_currency(total_risk_acv)}</strong>, leaving
<strong>{fmt_currency(total_good)}</strong> in good pipeline for <strong>{fmt_pct(good_coverage)}</strong> good coverage vs Most Likely.
We are currently at <strong>{fmt_pct(deployed_pct_of_target)}</strong> of our go-live target.</p>
{high_risk_table_html}

<h3 style="color: #333; border-bottom: 2px solid #007bff; padding-bottom: 8px;">Pacing</h3>
<p>On Day <strong>{day_number}</strong> of the quarter, our current deployed ACV of <strong>{fmt_currency(deployed["acv"])}</strong>
compares to a prior FY average of <strong>{fmt_currency(day_avg)}</strong> deployed by this day
(<strong>{fmt_pct(day_pct_val)}</strong> of the prior FY average final of <strong>{fmt_currency(CONFIG["prior_fy_avg_final"] * 1e6)}</strong>).
On a weekly basis (Week <strong>{week_number}</strong>), the prior FY average deployed was <strong>{fmt_currency(week_avg)}</strong>
(<strong>{fmt_pct(week_pct_val)}</strong> of final).</p>
<p>If the current quarter follows the same deployment curve as the prior FY average,
our projected quarter-end deployed ACV would be{f" <strong>{fmt_currency(day_projection)}</strong> based on daily pacing" if day_projection else " unavailable (no prior FY daily data)"}{f" and <strong>{fmt_currency(week_projection)}</strong> based on weekly pacing" if week_projection else ""}.</p>

<h3 style="color: #333; border-bottom: 2px solid #007bff; padding-bottom: 8px;">Partner and SD Attach</h3>
<p>Partner attach rate on the open pipeline is <strong>{fmt_pct(p_rate)}</strong>
({p_cnt} use cases, {fmt_currency(p_acv)} ACV).
SD attach rate is <strong>{fmt_pct(sd_rate)}</strong>
({sd_cnt} use cases, {fmt_currency(sd_acv_val)} ACV).
The remaining <strong>{unassisted_cnt}</strong> use cases ({fmt_currency(unassisted_acv)} ACV, {fmt_pct(unassisted_rate)}) are unassisted (Customer Only, Unknown, or None).</p>

<h3 style="color: #333; border-bottom: 2px solid #007bff; padding-bottom: 8px;">Use Case Velocity</h3>
<p>For use cases that went live this quarter, our average velocity metrics are:</p>
<div class="timeline-box">
  <div class="timeline-item"><span class="timeline-label">Days to TW</span><span class="timeline-days">{v_tw_str}{v_tw_prior}</span></div>
  <div class="timeline-arrow">&rarr;</div>
  <div class="timeline-item"><span class="timeline-label">Days to Imp Start</span><span class="timeline-days">{v_imp_str}{v_imp_prior}</span></div>
  <div class="timeline-arrow">&rarr;</div>
  <div class="timeline-item"><span class="timeline-label">Days to Deployed</span><span class="timeline-days">{v_dep_str}{v_dep_prior}</span></div>
</div>

<h3 style="color: #333; border-bottom: 2px solid #007bff; padding-bottom: 8px;">Sales Play Detail</h3>
"""

    # --- Render only the selected play ---
    _ps = play_summary
    _empty_ps = {"acv": 0, "count": 0}

    if selected_play_key == "bronze":
        br_dep = _ps.get("bronze_deployed", _empty_ps)
        br_open = _ps.get("bronze_open", _empty_ps)
        bronze_gap = fmt_play_gap(play_targets, "bronze", br_dep["acv"], br_dep["count"])
        bronze_target = fmt_play_target(play_targets, "bronze")
        bronze_risk = build_risk_narrative(play_risk["bronze"])
        br_created = bronze_created.get("created", 0)
        br_create_target = bronze_created.get("target")
        br_create_target_str = str(br_create_target) if br_create_target is not None else "N/A"
        br_create_pct = f"{br_created / br_create_target * 100:.0f}%" if br_create_target else "N/A"

        script_html += f"""
<h4 style="color: #555;">Bronze (Make Your Data AI Ready)</h4>
<p>Bronze has deployed <strong>{fmt_currency(br_dep["acv"])}</strong> ({br_dep["count"]} UCs) QTD
with <strong>{fmt_currency(br_open["acv"])}</strong> ({br_open["count"]} UCs) in open pipeline.
Gap to deployed target: <strong>{bronze_gap}</strong>.
We have created <strong>{br_created}</strong> Bronze use cases this quarter against a creation target of <strong>{br_create_target_str}</strong> ({br_create_pct}).
QTD TB Ingested: <strong>{bronze_tb_total:.1f} TB</strong>.
Regional coverage: <strong>{play_detail["bronze"]["regions"]}</strong> of 8 AMSExpansion regions contributing.</p>
<div class="risk-box">
<strong>Risk Summary ({bronze_risk["at_risk"]} of {bronze_risk["total"]} use cases, {fmt_currency(bronze_risk["acv_at_risk"])} ACV at risk):</strong><br>
{bronze_risk["narrative_html"]}
</div>
"""

    elif selected_play_key == "si":
        si_dep = _ps.get("si_deployed", _empty_ps)
        si_open = _ps.get("si_open", _empty_ps)
        si_gap = fmt_play_gap(play_targets, "si", si_dep["acv"], si_dep["count"])
        si_target_str = fmt_play_target(play_targets, "si")
        si_risk = build_risk_narrative(play_risk["si"])

        script_html += f"""
<h4 style="color: #555;">Snowflake Intelligence (AI: Snowflake Intelligence &amp; Agents)</h4>
<p>SI has deployed <strong>{fmt_currency(si_dep["acv"])}</strong> ({si_dep["count"]} UCs) QTD
with <strong>{fmt_currency(si_open["acv"])}</strong> ({si_open["count"]} UCs) in open pipeline.
Deployed target: <strong>{si_target_str}</strong>. Gap to target: <strong>{si_gap}</strong>.
Regional coverage: <strong>{play_detail["si"]["regions"]}</strong> of 8 AMSExpansion regions contributing.</p>
<p>Theater SI Usage (Last 30 Days): {si_theater["accounts"]:,} Accounts | {si_theater["users"]:,} Users | {si_theater["credits"]:,} Credits | {fmt_currency(si_theater["revenue"])} Revenue.</p>
<div class="risk-box">
<strong>Risk Summary ({si_risk["at_risk"]} of {si_risk["total"]} use cases, {fmt_currency(si_risk["acv_at_risk"])} ACV at risk):</strong><br>
{si_risk["narrative_html"]}
</div>
"""

    elif selected_play_key == "sqlserver":
        sql_dep = _ps.get("sqlserver_deployed", _empty_ps)
        sql_open = _ps.get("sqlserver_open", _empty_ps)
        sql_gap = fmt_play_gap(play_targets, "sqlserver", sql_dep["acv"], sql_dep["count"])
        sqlserver_target = fmt_play_target(play_targets, "sqlserver")
        sql_risk = build_risk_narrative(play_risk["sqlserver"])

        script_html += f"""
<h4 style="color: #555;">SQL Server Migration (Modernize Your Data Estate)</h4>
<p>SQL Server has deployed <strong>{fmt_currency(sql_dep["acv"])}</strong> ({sql_dep["count"]} UCs) QTD
with <strong>{fmt_currency(sql_open["acv"])}</strong> ({sql_open["count"]} UCs) in open pipeline.
Deployed target: <strong>{sqlserver_target}</strong>. Gap to target: <strong>{sql_gap}</strong>.
Regional coverage: <strong>{play_detail["sqlserver"]["regions"]}</strong> of 8 AMSExpansion regions contributing.</p>
<div class="risk-box">
<strong>Risk Summary ({sql_risk["at_risk"]} of {sql_risk["total"]} use cases, {fmt_currency(sql_risk["acv_at_risk"])} ACV at risk):</strong><br>
{sql_risk["narrative_html"]}
</div>
"""

    script_html += "\n</div>"  # close the script body div

    st.html(STREAMLIT_CSS + script_html)


# ===========================================================================
# TAB 2: USE CASE GO-LIVES
# ===========================================================================
def render_golives_tab(data):
    """Render the Use Case Go-Lives tab (full content, all plays)."""
    fiscal = data["fiscal"]
    revenue = data["revenue"]
    forecasts = data["forecasts"]
    deployed = data["deployed"]
    last7 = data["last7"]
    pipeline = data["pipeline"]
    risk_analysis = data["risk_analysis"]
    top5 = data["top5"]
    play_summary = data["play_summary"]
    play_detail = data["play_detail"]
    play_use_cases = data["play_use_cases"]
    play_risk = data["play_risk"]
    consumption = data["consumption"]
    bronze_tb_total = data["bronze_tb_total"]
    bronze_tb_acct = data["bronze_tb_acct"]
    si_usage = data["si_usage"]
    si_theater = data["si_theater"]
    pacing = data["pacing"]
    play_targets = data["play_targets"]
    cfg = data["_config"]

    quarter = safe_str(fiscal["FISCAL_QUARTER"])
    day_number = int(fiscal["DAY_NUMBER"])
    week_number = int(fiscal["WEEK_NUMBER"])
    most_likely = forecasts["most_likely"]
    gl_target = forecasts["target"]

    # Deltas
    gl_commit_delta = fmt_delta_html(forecasts.get("commit_delta"))
    gl_ml_delta = fmt_delta_html(forecasts.get("ml_delta"))
    gl_stretch_delta = fmt_delta_html(forecasts.get("stretch_delta"))

    # Percentages
    deployed_pct = (deployed["acv"] / most_likely * 100) if most_likely else 0
    last7_pct = (last7["acv"] / most_likely * 100) if most_likely else 0
    deployed_pct_of_target = (deployed["acv"] / gl_target * 100) if gl_target else 0
    coverage_pct = ((pipeline["acv"] + deployed["acv"]) / most_likely * 100) if most_likely else 0

    # Pipeline breakdown bar
    gl_total_pipeline = deployed["acv"] + pipeline["acv"]
    gl_deployed_pct = (deployed["acv"] / gl_total_pipeline * 100) if gl_total_pipeline else 0
    gl_open_pct = (pipeline["acv"] / gl_total_pipeline * 100) if gl_total_pipeline else 0

    # Pipeline risk
    total_good = sum(float(r.get("GOOD_ACV", 0) or 0) for r in risk_analysis.values())
    good_coverage = ((total_good + deployed["acv"]) / most_likely * 100) if most_likely else 0

    # Build risk narratives
    bronze_risk = build_risk_narrative(play_risk["bronze"])
    si_risk = build_risk_narrative(play_risk["si"])
    sql_risk = build_risk_narrative(play_risk["sqlserver"])

    # Build use case rows
    top5_rows = "\n".join(build_use_case_row(uc, consumption) for uc in top5)
    bronze_rows = "\n".join(build_use_case_row(uc, consumption, bronze_tb=bronze_tb_acct, row_type="bronze") for uc in play_use_cases["bronze"])
    si_rows = "\n".join(build_use_case_row(uc, consumption, si_usage=si_usage, row_type="si") for uc in play_use_cases["si"])
    sql_rows = "\n".join(build_use_case_row(uc, consumption, row_type="standard") for uc in play_use_cases["sqlserver"])

    # Pacing
    day_avg = fmt_currency(pacing["day_avg"])
    day_pct = fmt_pct(pacing["day_pct"])
    week_avg = fmt_currency(pacing["week_avg"])
    week_pct = fmt_pct(pacing["week_pct"])

    # Play targets
    bronze_target = fmt_play_target(play_targets, "bronze")
    si_target = fmt_play_target(play_targets, "si")
    sqlserver_target = fmt_play_target(play_targets, "sqlserver")
    bronze_gap = fmt_play_gap(play_targets, "bronze", play_summary["bronze_deployed"]["acv"], play_summary["bronze_deployed"]["count"])
    si_gap = fmt_play_gap(play_targets, "si", play_summary["si_deployed"]["acv"], play_summary["si_deployed"]["count"])
    sqlserver_gap = fmt_play_gap(play_targets, "sqlserver", play_summary["sqlserver_deployed"]["acv"], play_summary["sqlserver_deployed"]["count"])

    fy_label = cfg.get("fiscal_year_label", "FY27")
    prior_fy_label = cfg.get("prior_fy_label", "FY26")

    html = f"""
<h3>Revenue &amp; Forecast</h3>
<div style="display: flex; gap: 20px; align-items: flex-start; flex-wrap: wrap;">
  <table class="forecast-table" style="width: 280px;">
    <tr><th colspan="2" style="background: #6f42c1;">QTD Revenue</th></tr>
    <tr><td><strong>Revenue</strong></td><td>{fmt_currency(revenue["revenue"])}</td></tr>
    <tr><td><strong>Target</strong></td><td>{revenue["target"]}</td></tr>
    <tr><td><strong>% of Target</strong></td><td>{revenue["pct_target"]}</td></tr>
    <tr><td><strong>Q1 Fcst</strong></td><td>{fmt_currency(revenue["q1_forecast"])}</td></tr>
    <tr><td><strong>{fy_label} Fcst</strong></td><td>{fmt_currency(revenue["fy_forecast"])}</td></tr>
  </table>
  <table class="forecast-table" style="width: 280px;">
    <tr><th>Forecast Call</th><th>Amount</th></tr>
    <tr><td><strong>Commit</strong></td><td>{fmt_currency(forecasts["commit"])}{gl_commit_delta}</td></tr>
    <tr><td><strong>Most Likely</strong></td><td>{fmt_currency(most_likely)}{gl_ml_delta}</td></tr>
    <tr><td><strong>Stretch</strong></td><td>{fmt_currency(forecasts["stretch"])}{gl_stretch_delta}</td></tr>
  </table>
  <table class="forecast-table" style="width: 280px;">
    <tr><th colspan="2" style="background: #28a745;">Deployed QTD</th></tr>
    <tr><td><strong>Total ACV</strong></td><td>{fmt_currency(deployed["acv"])}</td></tr>
    <tr><td><strong>Use Cases</strong></td><td>{deployed["count"]}</td></tr>
    <tr><td><strong>Target</strong></td><td>{fmt_currency(gl_target)}</td></tr>
    <tr><td><strong>% of Target</strong></td><td>{fmt_pct(deployed_pct_of_target)}</td></tr>
    <tr><td><strong>% of Most Likely</strong></td><td>{fmt_pct(deployed_pct)}</td></tr>
  </table>
  <table class="forecast-table" style="width: 280px;">
    <tr><th colspan="2" style="background: #17a2b8;">Last 7 Days</th></tr>
    <tr><td><strong>Total ACV</strong></td><td>{fmt_currency(last7["acv"])}</td></tr>
    <tr><td><strong>Use Cases</strong></td><td>{last7["count"]}</td></tr>
    <tr><td><strong>% of Most Likely</strong></td><td>{fmt_pct(last7_pct)}</td></tr>
  </table>
</div>

<div class="analysis" style="margin-top: 16px;">
  <strong>Pipeline Breakdown:</strong>
  Deployed {fmt_currency(deployed["acv"])} ({deployed["count"]} UCs) &nbsp;|&nbsp;
  Open Pipeline {fmt_currency(pipeline["acv"])} ({pipeline["count"]} UCs) &nbsp;|&nbsp;
  <strong>Total: {fmt_currency(gl_total_pipeline)}</strong>
</div>

<div style="display: flex; height: 28px; border-radius: 6px; overflow: hidden; background: #eee; margin: 12px 0 4px 0;">
  <div style="width: {gl_deployed_pct:.1f}%; background: #28a745; display: flex; align-items: center; justify-content: center; color: white; font-size: 0.8em; font-weight: 600; overflow: hidden; white-space: nowrap;">{"Deployed" if gl_deployed_pct >= 12 else ""}</div>
  <div style="width: {gl_open_pct:.1f}%; background: #ffc107; display: flex; align-items: center; justify-content: center; color: #333; font-size: 0.8em; font-weight: 600; overflow: hidden; white-space: nowrap;">{"Open Pipeline" if gl_open_pct >= 12 else ""}</div>
</div>
<div style="display: flex; flex-wrap: wrap; gap: 16px; margin-top: 8px; font-size: 0.85em;">
  <span><span style="display: inline-block; width: 12px; height: 12px; background: #28a745; border-radius: 2px; vertical-align: middle; margin-right: 4px;"></span><strong>Deployed:</strong> {fmt_currency(deployed["acv"])} ({fmt_pct(gl_deployed_pct)})</span>
  <span><span style="display: inline-block; width: 12px; height: 12px; background: #ffc107; border-radius: 2px; vertical-align: middle; margin-right: 4px;"></span><strong>Open Pipeline:</strong> {fmt_currency(pipeline["acv"])} ({fmt_pct(gl_open_pct)})</span>
</div>

<div class="timeline-box">
  <div class="timeline-item"><span class="timeline-label">Days to TW</span><span class="timeline-days">{cfg["days_to_tw"]}</span></div>
  <div class="timeline-arrow">&rarr;</div>
  <div class="timeline-item"><span class="timeline-label">Days to Imp Start</span><span class="timeline-days">{cfg["days_to_imp"]}</span></div>
  <div class="timeline-arrow">&rarr;</div>
  <div class="timeline-item"><span class="timeline-label">Days to Deployed</span><span class="timeline-days">{cfg["days_to_deploy"]}</span></div>
</div>

<div class="risk-box">
  <strong>Pipeline Risk Analysis:</strong><br>
  {risk_line(risk_analysis, "Stage 1-3")}
  {risk_line(risk_analysis, "Stage 4")}
  {risk_line(risk_analysis, "Stage 5")}
  {risk_line(risk_analysis, "Stage 6")}
  &bull; <strong>Total Good Pipeline: {fmt_currency(total_good)}</strong> ({fmt_pct(good_coverage)} coverage vs Most Likely)
</div>

<h3>Pacing vs {prior_fy_label}</h3>
<table class="forecast-table" style="width: 750px;">
  <tr>
    <th style="white-space: nowrap;">Period</th>
    <th>{prior_fy_label} Average</th>
    <th>{prior_fy_label} % of Final</th>
    <th>{fy_label} {quarter}</th>
    <th>% of Most Likely</th>
    <th>% of Target</th>
  </tr>
  <tr>
    <td style="white-space: nowrap;"><strong>Day {day_number}</strong></td>
    <td>{day_avg}</td>
    <td>{day_pct}</td>
    <td>{fmt_currency(deployed["acv"])}</td>
    <td>{fmt_pct(deployed_pct)}</td>
    <td>{fmt_pct(deployed_pct_of_target)}</td>
  </tr>
  <tr>
    <td style="white-space: nowrap;"><strong>Week {week_number}</strong></td>
    <td>{week_avg}</td>
    <td>{week_pct}</td>
    <td>{fmt_currency(deployed["acv"])}</td>
    <td>{fmt_pct(deployed_pct)}</td>
    <td>{fmt_pct(deployed_pct_of_target)}</td>
  </tr>
</table>
<p style="font-size: 0.85em; color: #666; margin-top: 5px;"><em>{prior_fy_label} Average Final: ${cfg["prior_fy_avg_final"]}M across 4 quarters | {fy_label} {quarter} Most Likely: {fmt_currency(most_likely)}</em></p>

<h3>Top 5 Use Cases Going Live This Quarter</h3>
<table class="use-case-table">
  <tr>
    <th style="width:25%">Account / Use Case</th>
    <th style="width:45%">Details</th>
    <th style="width:30%">Summary</th>
  </tr>
  {top5_rows}
</table>

<h3>Sales Play Performance</h3>
<table class="sales-play-table">
  <tr>
    <th>Sales Play</th>
    <th>Open ACV</th>
    <th>Open Count</th>
    <th>Deployed ACV</th>
    <th>Deployed Count</th>
    <th>Deployed Target</th>
    <th>Gap to Target</th>
  </tr>
  <tr>
    <td><strong>Bronze Ingest</strong></td>
    <td class="number open-acv">{fmt_currency(play_summary["bronze_open"]["acv"])}</td>
    <td class="number">{play_summary["bronze_open"]["count"]}</td>
    <td class="number deployed-acv">{fmt_currency(play_summary["bronze_deployed"]["acv"])}</td>
    <td class="number">{play_summary["bronze_deployed"]["count"]}</td>
    <td class="number">{bronze_target}</td>
    <td class="number">{bronze_gap}</td>
  </tr>
  <tr>
    <td><strong>Snowflake Intelligence</strong></td>
    <td class="number open-acv">{fmt_currency(play_summary["si_open"]["acv"])}</td>
    <td class="number">{play_summary["si_open"]["count"]}</td>
    <td class="number deployed-acv">{fmt_currency(play_summary["si_deployed"]["acv"])}</td>
    <td class="number">{play_summary["si_deployed"]["count"]}</td>
    <td class="number">{si_target}</td>
    <td class="number">{si_gap}</td>
  </tr>
  <tr>
    <td><strong>SQL Server Migration</strong></td>
    <td class="number open-acv">{fmt_currency(play_summary["sqlserver_open"]["acv"])}</td>
    <td class="number">{play_summary["sqlserver_open"]["count"]}</td>
    <td class="number deployed-acv">{fmt_currency(play_summary["sqlserver_deployed"]["acv"])}</td>
    <td class="number">{play_summary["sqlserver_deployed"]["count"]}</td>
    <td class="number">{sqlserver_target}</td>
    <td class="number">{sqlserver_gap}</td>
  </tr>
</table>

<div class="play-section">
<h3>Bronze Ingest - Use Cases &ge;$500K</h3>
<div class="analysis">
<strong>QTD TB Ingested:</strong> {bronze_tb_total:.1f} TB<br>
<strong>Average ACV:</strong> {fmt_currency(play_detail["bronze"]["avg_acv"])} | <strong>Median ACV:</strong> {fmt_currency(play_detail["bronze"]["median_acv"])} | across {play_detail["bronze"]["count"]} use cases.
<strong>Regional Coverage:</strong> {play_detail["bronze"]["regions"]} of 8 AMSExpansion regions contributing.
</div>
<div class="risk-box">
<strong>Risk Summary ({bronze_risk["at_risk"]} of {bronze_risk["total"]} use cases, {fmt_currency(bronze_risk["acv_at_risk"])} ACV at risk):</strong><br>
{bronze_risk["narrative_html"]}
</div>
<table class="use-case-table">
  <tr>
    <th style="width:25%">Account / Use Case</th>
    <th style="width:45%">Details</th>
    <th style="width:30%">Summary</th>
  </tr>
  {bronze_rows}
</table>
</div>

<div class="play-section">
<h3>Snowflake Intelligence - Use Cases &ge;$500K</h3>
<div class="analysis">
<strong>Theater SI Usage (Last 30 Days):</strong> {si_theater["accounts"]:,} Accounts | {si_theater["users"]:,} Users | {si_theater["credits"]:,} Credits | {fmt_currency(si_theater["revenue"])} Revenue<br>
<strong>Average ACV:</strong> {fmt_currency(play_detail["si"]["avg_acv"])} | <strong>Median ACV:</strong> {fmt_currency(play_detail["si"]["median_acv"])} | across {play_detail["si"]["count"]} use cases.
<strong>Regional Coverage:</strong> {play_detail["si"]["regions"]} of 8 AMSExpansion regions contributing.
</div>
<div class="risk-box">
<strong>Risk Summary ({si_risk["at_risk"]} of {si_risk["total"]} use cases, {fmt_currency(si_risk["acv_at_risk"])} ACV at risk):</strong><br>
{si_risk["narrative_html"]}
</div>
<table class="use-case-table">
  <tr>
    <th style="width:25%">Account / Use Case</th>
    <th style="width:45%">Details</th>
    <th style="width:30%">Summary</th>
  </tr>
  {si_rows}
</table>
</div>

<div class="play-section">
<h3>SQL Server Migration - Use Cases &ge;$500K</h3>
<div class="analysis">
<strong>Average ACV:</strong> {fmt_currency(play_detail["sqlserver"]["avg_acv"])} | <strong>Median ACV:</strong> {fmt_currency(play_detail["sqlserver"]["median_acv"])} | across {play_detail["sqlserver"]["count"]} use cases.
<strong>Regional Coverage:</strong> {play_detail["sqlserver"]["regions"]} of 8 AMSExpansion regions contributing.
</div>
<div class="risk-box">
<strong>Risk Summary ({sql_risk["at_risk"]} of {sql_risk["total"]} use cases, {fmt_currency(sql_risk["acv_at_risk"])} ACV at risk):</strong><br>
{sql_risk["narrative_html"]}
</div>
<table class="use-case-table">
  <tr>
    <th style="width:25%">Account / Use Case</th>
    <th style="width:45%">Details</th>
    <th style="width:30%">Summary</th>
  </tr>
  {sql_rows}
</table>
</div>
"""

    st.html(STREAMLIT_CSS + html)

# ===========================================================================
# TAB 3: FORECAST ANALYSIS
# ===========================================================================
def render_forecast_tab(data):
    """Render the Forecast Analysis tab using existing _build_forecast_tab."""
    forecast_analysis = data["forecast_analysis"]
    forecasts = data["forecasts"]
    deployed = data["deployed"]
    fiscal = data["fiscal"]

    day_number = int(fiscal["DAY_NUMBER"])
    week_number = int(fiscal["WEEK_NUMBER"])

    forecast_html = _build_forecast_tab(
        forecast_analysis, forecasts, deployed, day_number, week_number
    )
    st.html(STREAMLIT_CSS + forecast_html)


# ===========================================================================
# MAIN APP
# ===========================================================================
def main():
    # --- Sidebar ---
    with st.sidebar:
        st.title("PEAK QC Report")
        st.markdown("---")

        selected_gvp = st.selectbox(
            "GVP",
            options=GVP_OPTIONS,
            index=GVP_OPTIONS.index("Mark Fleming"),
            help="Select GVP to view. Changing GVP reloads all data.",
        )

        play_labels = list(PLAY_OPTIONS.keys())
        selected_play_label = st.selectbox(
            "Sales Play (Script tab only)",
            options=play_labels,
            index=0,
            key="play_selector",
            help="Select which Sales Play to feature in the Script tab. Does not affect Go-Lives or Forecast tabs.",
        )

        st.markdown("---")
        cache_key = f"peak_data_{selected_gvp}"
        cached = st.session_state.get(cache_key)
        if cached and cached.get("_loaded_at"):
            last_refresh = cached["_loaded_at"].strftime('%H:%M:%S')
        else:
            last_refresh = "not yet loaded"
        st.markdown(
            f"*Data cached for 10 min.*  \n"
            f"*Last refresh: {last_refresh}*"
        )
        if st.button("Refresh Data"):
            # Clear all cached data from session state
            for key in list(st.session_state.keys()):
                if key.startswith("peak_data_"):
                    del st.session_state[key]
            st.rerun()

    # --- Load data ---
    data = load_all_data(selected_gvp)

    # Restore CONFIG from snapshot (needed on cache hits for functions
    # like _build_forecast_tab that read CONFIG directly)
    for k, v in data["_config"].items():
        if v is not None:
            CONFIG[k] = v

    # --- Header ---
    fiscal = data["fiscal"]
    quarter = safe_str(fiscal["FISCAL_QUARTER"])
    qstart = safe_str(fiscal["FQ_START"])
    qend = safe_str(fiscal["FQ_END"])
    days_remaining = int(fiscal["DAYS_REMAINING"])

    st.markdown(f"## PEAK Forecasting — {selected_gvp}")
    st.markdown(f"**{quarter}** ({qstart} - {qend}) | **{days_remaining} days remaining**")

    # --- Tabs ---
    tab_script, tab_golives, tab_forecast = st.tabs([
        "Script",
        "Use Case Go-Lives",
        "Forecast Analysis",
    ])

    selected_play_key = PLAY_OPTIONS[st.session_state.get("play_selector", play_labels[0])]

    with tab_script:
        render_script_tab(data, selected_play_key)

    with tab_golives:
        render_golives_tab(data)

    with tab_forecast:
        render_forecast_tab(data)


if __name__ == "__main__":
    main()
