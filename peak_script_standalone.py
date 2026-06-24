#!/usr/bin/env python3
"""
PEAK QC Script — Standalone HTML Generator
Generates the same Script tab output as the PEAK QC Streamlit app.
Hardcoded for Mark Fleming / AMSExpansion.

Usage:
    python peak_script_standalone.py
    python peak_script_standalone.py --output my_report.html
"""

import os
import re
import time
import sys
import html as html_lib
from datetime import datetime, date
from collections import defaultdict
from concurrent.futures import ThreadPoolExecutor, as_completed

import pandas as pd
import snowflake.connector

# =============================================================================
# SNOWFLAKE CONNECTION
# =============================================================================
conn = snowflake.connector.connect(
    connection_name=os.getenv("SNOWFLAKE_CONNECTION_NAME", "MyConnection")
)


def run_query(sql, _retries=2, _delay=3):
    """Execute SQL and return results as list of dicts."""
    last_err = None
    for attempt in range(1 + _retries):
        try:
            cur = conn.cursor()
            cur.execute("USE WAREHOUSE SNOWADHOC")
            cur.execute(sql)
            cols = [desc[0] for desc in cur.description]
            df = pd.DataFrame(cur.fetchall(), columns=cols)
            cur.close()
            records = df.to_dict("records")
            for row in records:
                for k, v in row.items():
                    if hasattr(v, "item"):
                        row[k] = v.item()
            return records
        except Exception as e:
            last_err = e
            err_msg = str(e)
            if attempt < _retries and ("does not exist or not authorized" in err_msg
                                       or "Object does not exist" in err_msg):
                time.sleep(_delay)
                continue
            raise last_err


# =============================================================================
# CONFIGURATION
# =============================================================================

CONFIG = {
    "warehouse": "SNOWADHOC",
    "gvp_name": "Mark Fleming",
    "gvp_function": "GVP",
    "play_threshold": 500000,
    "top_n": 5,
    "salesforce_base_url": "https://snowforce.lightning.force.com/",
    "days_to_tw": 42,
    "days_to_imp": 25,
    "days_to_deploy": 79,
    "bronze_campaign": "%Bronze Activation - Make Your Data AI Ready%",
    "sqlserver_campaign": "%SQL Server Migration - Modernize Your Data Estate%",
    "si_technical_use_case": "%AI: Snowflake Intelligence & Agents%",
    "si_campaign_analyst": "%Cortex Analyst%",
    "si_campaign_search": "%Cortex Search%",
    "excluded_stages": ("Not In Pursuit", "Use Case Lost"),
    "dim_excluded_stages": ("0 - Not In Pursuit", "8 - Use Case Lost"),
    "pursuit_stages": ("Discovery", "Scoping", "Technical / Business Validation"),
    "won_stages": ("Use Case Won / Migration Plan", "Implementation In Progress",
                   "Implementation Complete", "Deployed"),
    "risk_thresholds": {"stage_123": 146, "stage_4": 104, "stage_5": 79},
    "raven_uc_table": "SALES.RAVEN.USE_CASE_EXPLORER_VH_DELIVERABLE_C",
    "raven_acct_table": "SALES.RAVEN.D_SALESFORCE_ACCOUNT_CUSTOMERS",
    "dim_uc_table": "(SELECT * FROM SALES.REPORTING.DIM_USE_CASE_HISTORY_DS WHERE DS = (SELECT MAX(DS) FROM SALES.REPORTING.DIM_USE_CASE_HISTORY_DS))",
}

RISK_CATEGORIES = [
    "Technical Fit", "Time / Resources", "Competitor",
    "Access to the Customer", "Performance", "Consumption",
]

GVP_THEATER_MAP = {
    "Mark Fleming": "AMSExpansion",
    "Jennifer Chronis": "USMajors",
    "Jonathan Beaulier": "USPubSec",
    "Keegan Riley": "AMSAcquisition",
    "Jon Robertson": "APJ",
    "Dayne Turbitt": "EMEA",
}


def _theater():
    return GVP_THEATER_MAP.get(CONFIG["gvp_name"], CONFIG["gvp_name"])


# =============================================================================
# FISCAL QUARTER HELPERS
# =============================================================================

def compute_fiscal_quarters():
    today = date.today()
    m, y = today.month, today.year
    if m >= 2:
        fy = y + 1
        if m <= 4:
            current_q = 1
        elif m <= 7:
            current_q = 2
        elif m <= 10:
            current_q = 3
        else:
            current_q = 4
    else:
        fy = y
        current_q = 4

    def _qtr_dates(fiscal_year, q):
        base_year = fiscal_year - 1
        if q == 1:
            return (date(base_year, 2, 1), date(base_year, 4, 30))
        elif q == 2:
            return (date(base_year, 5, 1), date(base_year, 7, 31))
        elif q == 3:
            return (date(base_year, 8, 1), date(base_year, 10, 31))
        else:
            return (date(base_year, 11, 1), date(base_year + 1, 1, 31))

    quarters = []
    prior_fy = fy - 1
    for q in range(1, 5):
        s, e = _qtr_dates(prior_fy, q)
        quarters.append({
            "label": f"FY{prior_fy % 100}-Q{q}",
            "start": s.strftime("%Y-%m-%d"),
            "end": e.strftime("%Y-%m-%d"),
            "fy": prior_fy,
            "q": q,
            "is_current": False,
            "fiscal_quarter_key": f"{prior_fy}-Q{q}",
        })
    for q in range(1, 5):
        s, e = _qtr_dates(fy, q)
        is_current = (q == current_q)
        quarters.append({
            "label": f"FY{fy % 100}-Q{q}",
            "start": s.strftime("%Y-%m-%d"),
            "end": e.strftime("%Y-%m-%d"),
            "fy": fy,
            "q": q,
            "is_current": is_current,
            "fiscal_quarter_key": f"{fy}-Q{q}",
        })
    return quarters


def get_current_quarter():
    quarters = compute_fiscal_quarters()
    for q in quarters:
        if q["is_current"]:
            return q
    return quarters[-1]


# =============================================================================
# FORMATTING HELPERS
# =============================================================================

def _is_nan(v):
    try:
        return v != v
    except Exception:
        return False


def safe_int(value, default=0):
    if value is None or _is_nan(value):
        return default
    return int(value)


def safe_float(value, default=0.0):
    if value is None or _is_nan(value):
        return default
    return float(value)


def fmt_currency(value, compact=True):
    if value is None or _is_nan(value):
        return "N/A"
    v = float(value)
    if compact:
        if abs(v) >= 1_000_000_000:
            return f"${v / 1_000_000_000:.2f}B"
        elif abs(v) >= 1_000_000:
            return f"${v / 1_000_000:.1f}M"
        elif abs(v) >= 1_000:
            return f"${v / 1_000:.0f}K"
        else:
            return f"${v:,.0f}"
    else:
        return f"${v:,.0f}"


def fmt_pct(value):
    if value is None or _is_nan(value):
        return "N/A"
    return f"{float(value):.1f}%"


def safe_str(value):
    if value is None:
        return ""
    return str(value).strip()


def html_escape(value):
    return html_lib.escape(safe_str(value))


def truncate_text(text, max_len=500):
    s = safe_str(text)
    if len(s) > max_len:
        return s[:max_len] + "..."
    return s


_DATE_LINE_RE = re.compile(
    r'^\s*(?:[A-Z]{1,4}[\s:\-]*)?(?:'
    r'\[?\*{0,2}\d{4}[-/]\d{2}[-/]\d{2}'
    r'|\[?\d{1,2}[-/]\d{1,2}[-/]\d{2,4}'
    r'|\d{4}\d{4}\s'
    r')',
    re.MULTILINE
)


def extract_latest_comment(text):
    s = safe_str(text).strip()
    if not s:
        return ""
    matches = list(_DATE_LINE_RE.finditer(s))
    if len(matches) <= 1:
        return s
    latest = s[matches[0].start():matches[1].start()].strip()
    return latest


def update_risk_thresholds_from_velocity(velocity):
    tw = velocity.get("time_to_tw")
    imp = velocity.get("tw_to_imp_start")
    dep = velocity.get("imp_to_deployed")
    tw = int(round(tw)) if tw is not None and not _is_nan(tw) else CONFIG["days_to_tw"]
    imp = int(round(imp)) if imp is not None and not _is_nan(imp) else CONFIG["days_to_imp"]
    dep = int(round(dep)) if dep is not None and not _is_nan(dep) else CONFIG["days_to_deploy"]
    CONFIG["days_to_tw"] = tw
    CONFIG["days_to_imp"] = imp
    CONFIG["days_to_deploy"] = dep
    CONFIG["risk_thresholds"] = {
        "stage_123": tw + imp + dep,
        "stage_4": imp + dep,
        "stage_5": dep,
    }


def fmt_delta_text(delta):
    if delta is None:
        return ""
    if delta == 0:
        return " (flat WoW)"
    if delta > 0:
        return f" (+{fmt_currency(delta)} WoW)"
    else:
        return f" (-{fmt_currency(abs(delta))} WoW)"


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
        parts.append(fmt_currency(deployed_acv - target_acv))
    if target_count is not None:
        parts.append(f"{deployed_count - target_count} UCs")
    return " / ".join(parts) if parts else "&mdash;"


# =============================================================================
# RISK NARRATIVE HELPERS
# =============================================================================

_THEME_PATTERNS = [
    (re.compile(r'migrat|snowconvert|code conver|stored proc', re.I), "migration complexity"),
    (re.compile(r'partner|si |system integrat|squadron|perficient|deloitte|ibm|kipi|proficient', re.I), "partner dependency"),
    (re.compile(r'timeline|go.?live|schedule|delay|slow|stall|on hold|paused|waiting|pending', re.I), "timeline uncertainty"),
    (re.compile(r'resource|bandwidth|capacity|availability|staff', re.I), "resource constraints"),
    (re.compile(r'connector|openflow|kafka|streaming|ingestion|snowpipe', re.I), "connector/ingestion readiness"),
    (re.compile(r'security|network|private.?link|firewall|permission|access', re.I), "security/access setup"),
    (re.compile(r'performance|latency|sla|p99|optim|slow.?quer|compil', re.I), "performance validation"),
    (re.compile(r'poc|proof of concept|test|pilot|evaluat', re.I), "POC/testing in progress"),
    (re.compile(r'compet|databricks|redshift|dbx|aerospike|clickhouse|mssql|sql server', re.I), "competitive displacement"),
    (re.compile(r'budget|funding|cost|pricing|contract|procurement|approv', re.I), "budget/procurement"),
    (re.compile(r'onboard|ramp|training|enablement', re.I), "onboarding/enablement"),
    (re.compile(r'no.?update|no.?change|no.?risk|on.?track|progressing|no.?blocker', re.I), "progressing - monitoring"),
]


def _detect_themes(text):
    themes = []
    for pattern, theme_label in _THEME_PATTERNS:
        if pattern.search(text):
            themes.append(theme_label)
    return themes if themes else ["details pending"]


def _synthesize_category(cat_name, use_cases):
    theme_counts = defaultdict(int)
    for uc in use_cases:
        for theme in uc["themes"]:
            theme_counts[theme] += 1
    sorted_themes = sorted(theme_counts.items(), key=lambda x: x[1], reverse=True)
    top_themes = [t[0] for t in sorted_themes[:3]]
    real_themes = [t for t in top_themes if t != "details pending"]
    if not real_themes:
        return "Risk flagged, monitoring for updates"
    n = len(use_cases)
    if n == 1:
        return "; ".join(real_themes)
    else:
        top_theme = real_themes[0]
        top_count = theme_counts[top_theme]
        if len(real_themes) == 1:
            if top_count == n:
                return f"{top_theme}"
            return f"{top_theme} ({top_count} of {n})"
        else:
            secondary = "; ".join(real_themes[1:])
            return f"{top_theme}; also {secondary}"


def build_risk_narrative(risk_data):
    risk_rows = risk_data["risk_rows"]
    total_count = risk_data["total_count"]
    if not risk_rows:
        return {"at_risk": 0, "total": total_count, "acv_at_risk": 0,
                "narrative_html": "No use cases flagged with risk this quarter."}
    category_data = defaultdict(lambda: {"use_cases": [], "total_acv": 0})
    for row in risk_rows:
        risk_str = safe_str(row.get("USE_CASE_RISK", ""))
        acv = float(row.get("USE_CASE_EACV", 0) or 0)
        account = safe_str(row.get("ACCOUNT_NAME", ""))
        uc_name = safe_str(row.get("USE_CASE_NAME", ""))
        se_comments = safe_str(row.get("SE_COMMENTS", ""))
        next_steps = safe_str(row.get("NEXT_STEPS", ""))
        stage = safe_str(row.get("USE_CASE_STAGE", ""))
        latest_comment = extract_latest_comment(se_comments).lower()
        latest_next = extract_latest_comment(next_steps).lower()
        combined_text = latest_comment + " " + latest_next
        themes = _detect_themes(combined_text)
        categories = [c.strip() for c in risk_str.split(";") if c.strip() and c.strip() != "None"]
        for cat in categories:
            category_data[cat]["use_cases"].append({"account": account, "uc_name": uc_name, "acv": acv, "stage": stage, "themes": themes})
            category_data[cat]["total_acv"] += acv
    sorted_cats = sorted(category_data.items(), key=lambda x: x[1]["total_acv"], reverse=True)
    at_risk_count = len(risk_rows)
    acv_at_risk = sum(safe_float(r.get("USE_CASE_EACV", 0) or 0) for r in risk_rows)
    bullets = []
    for cat_name, cat_info in sorted_cats:
        n_ucs = len(cat_info["use_cases"])
        cat_acv = cat_info["total_acv"]
        synthesis = _synthesize_category(cat_name, cat_info["use_cases"])
        account_names = list(dict.fromkeys(uc["account"] for uc in cat_info["use_cases"]))
        if len(account_names) <= 3:
            acct_str = ", ".join(html_escape(a) for a in account_names)
        else:
            acct_str = ", ".join(html_escape(a) for a in account_names[:3]) + f" +{len(account_names)-3} more"
        uc_word = "use case" if n_ucs == 1 else "use cases"
        bullets.append(f'&bull; <strong>{html_escape(cat_name)} ({n_ucs} {uc_word}, {fmt_currency(cat_acv)}):</strong> {synthesis} ({acct_str})')
    narrative_html = "<br>\n".join(bullets)
    return {"at_risk": at_risk_count, "total": total_count, "acv_at_risk": acv_at_risk, "narrative_html": narrative_html}


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


# =============================================================================
# SQL QUERIES
# =============================================================================

def q_fiscal_calendar(selected_quarter):
    fq_start = selected_quarter["start"]
    fq_end = selected_quarter["end"]
    fy = selected_quarter["fy"]
    q_num = selected_quarter["q"]
    fq_label = f"Q{q_num}"
    ref_date = CONFIG["reference_date"]

    CONFIG["quarter_start"] = fq_start
    CONFIG["quarter_end"] = fq_end
    CONFIG["fiscal_year"] = fy
    CONFIG["fiscal_year_label"] = f"FY{fy % 100}"
    CONFIG["prior_fy_label"] = f"FY{(fy - 1) % 100}"
    CONFIG["fiscal_quarter"] = f"FY{fy}-{fq_label}"

    fiscal = {
        "FISCAL_YEAR": fy,
        "FISCAL_QUARTER": fq_label,
        "FQ_START": fq_start,
        "FQ_END": fq_end,
    }

    day_rows = run_query(f"""
        SELECT DATEDIFF('day', '{fq_start}'::DATE, {ref_date}) + 1 AS DAY_NUMBER,
               CEIL((DATEDIFF('day', '{fq_start}'::DATE, {ref_date}) + 1) / 7.0) AS WEEK_NUMBER
    """)
    day_number = safe_int(day_rows[0].get("DAY_NUMBER", 0)) if day_rows else 0
    week_number = safe_int(day_rows[0].get("WEEK_NUMBER", 0)) if day_rows else 0
    fiscal["DAY_NUMBER"] = day_number
    fiscal["WEEK_NUMBER"] = week_number

    fiscal["DAYS_REMAINING"] = None
    dr_rows = run_query(f"SELECT DATEDIFF('day', {ref_date}, '{fq_end}'::DATE) + 1 AS DAYS_REMAINING")
    if dr_rows:
        fiscal["DAYS_REMAINING"] = safe_int(dr_rows[0].get("DAYS_REMAINING", 0))
        CONFIG["days_remaining"] = fiscal["DAYS_REMAINING"]

    prior_fy_start_year = fy - 2
    prior_quarters = [
        (f"{prior_fy_start_year}-02-01", f"{prior_fy_start_year}-04-30"),
        (f"{prior_fy_start_year}-05-01", f"{prior_fy_start_year}-07-31"),
        (f"{prior_fy_start_year}-08-01", f"{prior_fy_start_year}-10-31"),
        (f"{prior_fy_start_year}-11-01", f"{prior_fy_start_year + 1}-01-31"),
    ]
    CONFIG["prior_fy_quarters"] = prior_quarters

    if prior_quarters:
        unions = []
        for qs, qe in prior_quarters:
            unions.append(f"""
                SELECT COALESCE(SUM(u.USE_CASE_ACV), 0) as DEPLOYED_ACV
                FROM {CONFIG["raven_uc_table"]} u
                JOIN {CONFIG["raven_acct_table"]} a ON u.VH_ACCOUNT_C = a.SALESFORCE_ACCOUNT_ID
                WHERE a.GVP = '{CONFIG["gvp_name"]}'
                  AND u.USE_CASE_ACV > 0 AND u.IS_WENT_LIVE = TRUE
                  AND u.DEFAULT_DATE BETWEEN '{qs}' AND '{qe}'
            """)
        avg_rows = run_query(f"""
            SELECT ROUND(AVG(DEPLOYED_ACV) / 1000000, 1) as AVG_FINAL_M
            FROM ({' UNION ALL '.join(unions)})
        """)
        CONFIG["prior_fy_avg_final"] = float(avg_rows[0]["AVG_FINAL_M"] or 0)
    else:
        CONFIG["prior_fy_avg_final"] = 0
    return fiscal


def q_forecast_calls():
    fq_key = CONFIG["fiscal_quarter_key"]
    rows = run_query(f"""
        SELECT FORECAST_TYPE, FORECAST_AMOUNT, LATEST_DATE, PREVIOUS_WEEK
        FROM SALES.REPORTING.PEAK_FORECAST_CALLS_PIPELINE_TARGETS
        WHERE USER_NAME = '{CONFIG["gvp_name"]}'
          AND FUNCTION = '{CONFIG["gvp_function"]}'
          AND TYPE = 'Use Case Go-Lives'
          AND FORECAST_TYPE IN ('CommitForecast', 'MostLikelyForecast', 'BestCaseForecast', 'Target')
          AND FISCAL_QUARTER = '{fq_key}'
          AND (LATEST_DATE = TRUE OR PREVIOUS_WEEK = TRUE)
    """)
    current = {}
    prior = {}
    for r in rows:
        if r["LATEST_DATE"]:
            current[r["FORECAST_TYPE"]] = safe_float(r["FORECAST_AMOUNT"])
        if r["PREVIOUS_WEEK"]:
            prior[r["FORECAST_TYPE"]] = safe_float(r["FORECAST_AMOUNT"])
    commit = current.get("CommitForecast", 0)
    ml = current.get("MostLikelyForecast", 0)
    stretch = current.get("BestCaseForecast", 0)
    return {
        "commit": commit,
        "most_likely": ml,
        "stretch": stretch,
        "target": current.get("Target", 0),
        "commit_delta": commit - prior["CommitForecast"] if "CommitForecast" in prior else None,
        "ml_delta": ml - prior["MostLikelyForecast"] if "MostLikelyForecast" in prior else None,
        "stretch_delta": stretch - prior["BestCaseForecast"] if "BestCaseForecast" in prior else None,
    }


def q_deployed_qtd():
    rows = run_query(f"""
        SELECT SUM(u.USE_CASE_ACV) as DEPLOYED_ACV, COUNT(*) as DEPLOYED_COUNT
        FROM {CONFIG["raven_uc_table"]} u
        JOIN {CONFIG["raven_acct_table"]} a ON u.VH_ACCOUNT_C = a.SALESFORCE_ACCOUNT_ID
        WHERE a.GVP = '{CONFIG["gvp_name"]}'
          AND u.USE_CASE_ACV > 0
          AND u.IS_WENT_LIVE = TRUE
          AND u.DEFAULT_DATE BETWEEN '{CONFIG["quarter_start"]}' AND '{CONFIG["quarter_end"]}'
    """)
    r = rows[0]
    return {"acv": safe_float(r["DEPLOYED_ACV"] or 0), "count": safe_int(r["DEPLOYED_COUNT"] or 0)}


def q_open_pipeline():
    excluded = ", ".join(f"'{s}'" for s in CONFIG["excluded_stages"])
    rows = run_query(f"""
        SELECT SUM(u.USE_CASE_ACV) as OPEN_PIPELINE, COUNT(*) as OPEN_COUNT
        FROM {CONFIG["raven_uc_table"]} u
        JOIN {CONFIG["raven_acct_table"]} a ON u.VH_ACCOUNT_C = a.SALESFORCE_ACCOUNT_ID
        WHERE a.GVP = '{CONFIG["gvp_name"]}'
          AND u.USE_CASE_ACV > 0 AND u.IS_WENT_LIVE = FALSE AND u.IS_LOST = FALSE
          AND u.GO_LIVE_DATE BETWEEN '{CONFIG["quarter_start"]}' AND '{CONFIG["quarter_end"]}'
          AND u.USE_CASE_STAGE NOT IN ({excluded})
    """)
    r = rows[0]
    return {"acv": safe_float(r["OPEN_PIPELINE"] or 0), "count": safe_int(r["OPEN_COUNT"] or 0)}


def q_pipeline_risk():
    excluded = ", ".join(f"'{s}'" for s in CONFIG["excluded_stages"])
    ref_date = CONFIG["reference_date"]
    rows = run_query(f"""
        WITH fiscal_qtr AS (
            SELECT '{CONFIG["quarter_start"]}'::DATE AS FQ_START,
                   '{CONFIG["quarter_end"]}'::DATE AS FQ_END,
                   DATEDIFF('day', {ref_date}, '{CONFIG["quarter_end"]}'::DATE) + 1 AS DAYS_REMAINING
        )
        SELECT
            CASE
                WHEN u.STAGE_NUMBER IN (1,2,3) THEN 'Stage 1-3'
                WHEN u.STAGE_NUMBER = 4 THEN 'Stage 4'
                WHEN u.STAGE_NUMBER = 5 THEN 'Stage 5'
                WHEN u.STAGE_NUMBER = 6 THEN 'Stage 6'
            END AS STAGE_GROUP,
            COUNT(*) AS TOTAL_COUNT,
            SUM(r.USE_CASE_ACV) AS TOTAL_ACV,
            SUM(CASE
                WHEN u.STAGE_NUMBER IN (1,2,3) AND (u.DAYS_IN_STAGE + f.DAYS_REMAINING) < {CONFIG["risk_thresholds"]["stage_123"]} THEN 1
                WHEN u.STAGE_NUMBER = 4 AND (u.DAYS_IN_STAGE + f.DAYS_REMAINING) < {CONFIG["risk_thresholds"]["stage_4"]} THEN 1
                WHEN u.STAGE_NUMBER = 5 AND (u.DAYS_IN_STAGE + f.DAYS_REMAINING) < {CONFIG["risk_thresholds"]["stage_5"]} THEN 1
                ELSE 0
            END) AS AT_RISK_COUNT,
            SUM(CASE
                WHEN u.STAGE_NUMBER IN (1,2,3) AND (u.DAYS_IN_STAGE + f.DAYS_REMAINING) < {CONFIG["risk_thresholds"]["stage_123"]} THEN r.USE_CASE_ACV
                WHEN u.STAGE_NUMBER = 4 AND (u.DAYS_IN_STAGE + f.DAYS_REMAINING) < {CONFIG["risk_thresholds"]["stage_4"]} THEN r.USE_CASE_ACV
                WHEN u.STAGE_NUMBER = 5 AND (u.DAYS_IN_STAGE + f.DAYS_REMAINING) < {CONFIG["risk_thresholds"]["stage_5"]} THEN r.USE_CASE_ACV
                ELSE 0
            END) AS AT_RISK_ACV,
            SUM(CASE
                WHEN u.STAGE_NUMBER IN (1,2,3) AND (u.DAYS_IN_STAGE + f.DAYS_REMAINING) >= {CONFIG["risk_thresholds"]["stage_123"]} THEN r.USE_CASE_ACV
                WHEN u.STAGE_NUMBER = 4 AND (u.DAYS_IN_STAGE + f.DAYS_REMAINING) >= {CONFIG["risk_thresholds"]["stage_4"]} THEN r.USE_CASE_ACV
                WHEN u.STAGE_NUMBER = 5 AND (u.DAYS_IN_STAGE + f.DAYS_REMAINING) >= {CONFIG["risk_thresholds"]["stage_5"]} THEN r.USE_CASE_ACV
                WHEN u.STAGE_NUMBER = 6 THEN r.USE_CASE_ACV
                ELSE 0
            END) AS GOOD_ACV
        FROM {CONFIG["dim_uc_table"]} u
        CROSS JOIN fiscal_qtr f
        JOIN {CONFIG["raven_uc_table"]} r ON u.USE_CASE_ID = r.ID
        JOIN {CONFIG["raven_acct_table"]} a ON r.VH_ACCOUNT_C = a.SALESFORCE_ACCOUNT_ID
        WHERE a.GVP = '{CONFIG["gvp_name"]}'
            AND r.USE_CASE_ACV > 0 AND r.IS_WENT_LIVE = FALSE AND r.IS_LOST = FALSE
            AND u.STAGE_NUMBER BETWEEN 1 AND 6
            AND r.USE_CASE_STAGE NOT IN ({excluded})
            AND r.GO_LIVE_DATE BETWEEN f.FQ_START AND f.FQ_END
        GROUP BY STAGE_GROUP ORDER BY STAGE_GROUP
    """)
    return {r["STAGE_GROUP"]: r for r in rows}


def _play_summary_query(play_name, extra_join, filter_clause, is_deployed):
    excluded = ", ".join(f"'{s}'" for s in CONFIG["excluded_stages"])
    if is_deployed:
        deploy_filter = "u.IS_WENT_LIVE = TRUE"
        date_filter = f"u.DEFAULT_DATE BETWEEN '{CONFIG['quarter_start']}' AND '{CONFIG['quarter_end']}'"
        stage_filter = ""
    else:
        deploy_filter = "u.IS_WENT_LIVE = FALSE AND u.IS_LOST = FALSE"
        date_filter = f"u.GO_LIVE_DATE BETWEEN '{CONFIG['quarter_start']}' AND '{CONFIG['quarter_end']}'"
        stage_filter = f"AND u.USE_CASE_STAGE NOT IN ({excluded})"
    rows = run_query(f"""
        SELECT SUM(u.USE_CASE_ACV) as ACV, COUNT(*) as COUNT_
        FROM {CONFIG["raven_uc_table"]} u
        JOIN {CONFIG["raven_acct_table"]} a ON u.VH_ACCOUNT_C = a.SALESFORCE_ACCOUNT_ID
        {extra_join}
        WHERE a.GVP = '{CONFIG["gvp_name"]}'
          AND u.USE_CASE_ACV > 0 AND {deploy_filter} AND {date_filter}
          {stage_filter} AND {filter_clause}
    """)
    r = rows[0]
    return {"acv": safe_float(r["ACV"] or 0), "count": safe_int(r["COUNT_"] or 0)}


def q_sales_play_summary():
    dim_join = f"JOIN {CONFIG['dim_uc_table']} d ON u.ID = d.USE_CASE_ID"
    bronze_filter = f"u.TECHNICAL_CAMPAIGN_S_C ILIKE '{CONFIG['bronze_campaign']}'"
    sql_filter = f"u.TECHNICAL_CAMPAIGN_S_C ILIKE '{CONFIG['sqlserver_campaign']}'"
    si_filter = f"d.TECHNICAL_USE_CASE ILIKE '{CONFIG['si_technical_use_case']}'"
    result = {}
    for play, extra_join, filt in [
        ("bronze", "", bronze_filter),
        ("si", dim_join, si_filter),
        ("sqlserver", "", sql_filter),
    ]:
        result[f"{play}_open"] = _play_summary_query(play, extra_join, filt, False)
        result[f"{play}_deployed"] = _play_summary_query(play, extra_join, filt, True)
    return result


def q_play_detail_metrics():
    excluded = ", ".join(f"'{s}'" for s in CONFIG["excluded_stages"])
    dim_join = f"JOIN {CONFIG['dim_uc_table']} d ON u.ID = d.USE_CASE_ID"

    def _run(extra_join, filter_clause):
        rows = run_query(f"""
            SELECT COUNT(*) as OPEN_COUNT, AVG(u.USE_CASE_ACV) as AVG_ACV,
                   MEDIAN(u.USE_CASE_ACV) as MEDIAN_ACV,
                   COUNT(DISTINCT a.SALES_AREA) as REGION_COUNT
            FROM {CONFIG["raven_uc_table"]} u
            JOIN {CONFIG["raven_acct_table"]} a ON u.VH_ACCOUNT_C = a.SALESFORCE_ACCOUNT_ID
            {extra_join}
            WHERE a.GVP = '{CONFIG["gvp_name"]}'
              AND u.USE_CASE_ACV > 0 AND u.IS_WENT_LIVE = FALSE AND u.IS_LOST = FALSE
              AND u.GO_LIVE_DATE BETWEEN '{CONFIG["quarter_start"]}' AND '{CONFIG["quarter_end"]}'
              AND u.USE_CASE_STAGE NOT IN ({excluded}) AND {filter_clause}
        """)
        r = rows[0]
        return {
            "count": safe_int(r["OPEN_COUNT"] or 0), "avg_acv": safe_float(r["AVG_ACV"] or 0),
            "median_acv": safe_float(r["MEDIAN_ACV"] or 0), "regions": safe_int(r["REGION_COUNT"] or 0),
        }
    return {
        "bronze": _run("", f"u.TECHNICAL_CAMPAIGN_S_C ILIKE '{CONFIG['bronze_campaign']}'"),
        "si": _run(dim_join, f"d.TECHNICAL_USE_CASE ILIKE '{CONFIG['si_technical_use_case']}'"),
        "sqlserver": _run("", f"u.TECHNICAL_CAMPAIGN_S_C ILIKE '{CONFIG['sqlserver_campaign']}'"),
    }


def q_bronze_tb_total():
    rows = run_query(f"""
        SELECT SUM(TB_INGESTED) as BRONZE_TB
        FROM SALES.REPORTING.SALES_PROGRAMS_BRONZE_INGEST
        WHERE GVP = '{CONFIG["gvp_name"]}'
          AND IS_BRONZE = TRUE
          AND MONTH BETWEEN '{CONFIG["quarter_start"]}' AND '{CONFIG["quarter_end"]}'
    """)
    return float(rows[0]["BRONZE_TB"] or 0)


def _play_risk_detail_query(play_name, extra_join, filter_clause):
    excluded = ", ".join(f"'{s}'" for s in CONFIG["excluded_stages"])
    risk_rows = run_query(f"""
        SELECT a.SALESFORCE_ACCOUNT_NAME as ACCOUNT_NAME, u.VH_NAME_C as USE_CASE_NAME,
               u.USE_CASE_ACV as USE_CASE_EACV, u.USE_CASE_RISK_C as USE_CASE_RISK,
               u.USE_CASE_COMMENTS_C as SE_COMMENTS, u.NEXT_STEP_C as NEXT_STEPS, u.USE_CASE_STAGE
        FROM {CONFIG["raven_uc_table"]} u
        JOIN {CONFIG["raven_acct_table"]} a ON u.VH_ACCOUNT_C = a.SALESFORCE_ACCOUNT_ID
        {extra_join}
        WHERE a.GVP = '{CONFIG["gvp_name"]}'
          AND u.USE_CASE_ACV > 0 AND u.IS_WENT_LIVE = FALSE AND u.IS_LOST = FALSE
          AND u.GO_LIVE_DATE BETWEEN '{CONFIG["quarter_start"]}' AND '{CONFIG["quarter_end"]}'
          AND u.USE_CASE_STAGE NOT IN ({excluded})
          AND u.USE_CASE_RISK_C IS NOT NULL AND u.USE_CASE_RISK_C != '' AND u.USE_CASE_RISK_C != 'None'
          AND {filter_clause}
        ORDER BY u.USE_CASE_ACV DESC
    """)
    total_rows = run_query(f"""
        SELECT COUNT(*) as TOTAL_COUNT
        FROM {CONFIG["raven_uc_table"]} u
        JOIN {CONFIG["raven_acct_table"]} a ON u.VH_ACCOUNT_C = a.SALESFORCE_ACCOUNT_ID
        {extra_join}
        WHERE a.GVP = '{CONFIG["gvp_name"]}'
          AND u.USE_CASE_ACV > 0 AND u.IS_WENT_LIVE = FALSE AND u.IS_LOST = FALSE
          AND u.GO_LIVE_DATE BETWEEN '{CONFIG["quarter_start"]}' AND '{CONFIG["quarter_end"]}'
          AND u.USE_CASE_STAGE NOT IN ({excluded}) AND {filter_clause}
    """)
    total = safe_int(total_rows[0]["TOTAL_COUNT"])
    return {"risk_rows": risk_rows, "total_count": total}


def q_play_risk_detail():
    dim_join = f"JOIN {CONFIG['dim_uc_table']} d ON u.ID = d.USE_CASE_ID"
    return {
        "bronze": _play_risk_detail_query("Bronze", "", f"u.TECHNICAL_CAMPAIGN_S_C ILIKE '{CONFIG['bronze_campaign']}'"),
        "si": _play_risk_detail_query("SI", dim_join, f"d.TECHNICAL_USE_CASE ILIKE '{CONFIG['si_technical_use_case']}'"),
        "sqlserver": _play_risk_detail_query("SQL Server", "", f"u.TECHNICAL_CAMPAIGN_S_C ILIKE '{CONFIG['sqlserver_campaign']}'"),
    }


def q_high_risk_use_cases():
    excluded = ", ".join(f"'{s}'" for s in CONFIG["excluded_stages"])
    return run_query(f"""
        SELECT u.ID as USE_CASE_ID, u.NAME as USE_CASE_NUMBER, u.VH_NAME_C as USE_CASE_NAME,
               a.SALESFORCE_OWNER_NAME as AE_NAME, a.LEAD_SALES_ENGINEER_NAME as SE_NAME,
               u.USE_CASE_ACV, u.USE_CASE_RISK_C as RISK_TYPE,
               SNOWFLAKE.CORTEX.COMPLETE('llama3.1-70b',
                   CONCAT('Summarize this use case risk in 1-2 concise sentences for a sales leadership QC call. Focus on what the risk is and the current mitigation plan. Do not include any preamble or introductory text, just provide the summary directly. Risk type: ',
                       COALESCE(u.USE_CASE_RISK_C, 'Unknown'),
                       '. SE Comments: ', COALESCE(u.USE_CASE_COMMENTS_C, 'None'),
                       '. Next Steps: ', COALESCE(u.NEXT_STEP_C, 'None'))
               ) as RISK_SUMMARY
        FROM {CONFIG["raven_uc_table"]} u
        JOIN {CONFIG["raven_acct_table"]} a ON u.VH_ACCOUNT_C = a.SALESFORCE_ACCOUNT_ID
        WHERE a.GVP = '{CONFIG["gvp_name"]}'
          AND u.USE_CASE_ACV > 0 AND u.IS_WENT_LIVE = FALSE AND u.IS_LOST = FALSE
          AND u.GO_LIVE_DATE BETWEEN '{CONFIG["quarter_start"]}' AND '{CONFIG["quarter_end"]}'
          AND u.USE_CASE_STAGE NOT IN ({excluded})
          AND u.USE_CASE_RISK_C IS NOT NULL AND u.USE_CASE_RISK_C != '' AND u.USE_CASE_RISK_C != 'None'
          AND u.USE_CASE_ACV >= {CONFIG["play_threshold"]}
        ORDER BY u.USE_CASE_ACV DESC
    """)


def q_play_targets():
    rows = run_query(f"""
        SELECT PRIORITIZED_FEATURE_UC, TARGET_USE_CASE_EACV, TARGET_USE_CASE_COUNT, MOVEMENT_TYPE
        FROM SALES.REPORTING.SALES_PROGRAM_PRIORITIZED_FEATURES_TARGETS
        WHERE MAPPED_THEATER = '{_theater()}'
          AND FISCAL_QUARTER = '{CONFIG["fiscal_quarter"]}'
          AND MOVEMENT_TYPE IN ('Deployed', 'Created')
          AND PRIORITIZED_FEATURE_UC IN (
              'Make Your Data AI Ready', 'Modernize Your Data Estate',
              'AI: Snowflake Intelligence & Agents')
    """)
    mapping = {
        "Make Your Data AI Ready": "bronze",
        "Modernize Your Data Estate": "sqlserver",
        "AI: Snowflake Intelligence & Agents": "si",
    }
    targets = {}
    for r in rows:
        key = mapping.get(r["PRIORITIZED_FEATURE_UC"])
        movement = r["MOVEMENT_TYPE"].lower()
        if key:
            tgt = {
                "acv": safe_float(r["TARGET_USE_CASE_EACV"]) if r["TARGET_USE_CASE_EACV"] else None,
                "count": safe_int(r["TARGET_USE_CASE_COUNT"]) if r["TARGET_USE_CASE_COUNT"] else None,
            }
            targets[f"{key}_{movement}"] = tgt
    for k in ("bronze", "si", "sqlserver"):
        for m in ("deployed", "created"):
            if f"{k}_{m}" not in targets:
                targets[f"{k}_{m}"] = {"acv": None, "count": None}
    return targets


def q_partner_sd_attach():
    excluded = ", ".join(f"'{s}'" for s in CONFIG["excluded_stages"])
    rows = run_query(f"""
        SELECT u.IMPLEMENTER_C, COUNT(*) as CNT, SUM(u.USE_CASE_ACV) as ACV
        FROM {CONFIG["raven_uc_table"]} u
        JOIN {CONFIG["raven_acct_table"]} a ON u.VH_ACCOUNT_C = a.SALESFORCE_ACCOUNT_ID
        WHERE a.GVP = '{CONFIG["gvp_name"]}'
          AND u.USE_CASE_ACV > 0 AND u.IS_WENT_LIVE = FALSE AND u.IS_LOST = FALSE
          AND u.GO_LIVE_DATE BETWEEN '{CONFIG["quarter_start"]}' AND '{CONFIG["quarter_end"]}'
          AND u.USE_CASE_STAGE NOT IN ({excluded})
        GROUP BY u.IMPLEMENTER_C
    """)
    acct_rows = run_query(f"""
        SELECT DISTINCT u.VH_ACCOUNT_C as ACCOUNT_ID, u.IMPLEMENTER_C
        FROM {CONFIG["raven_uc_table"]} u
        JOIN {CONFIG["raven_acct_table"]} a ON u.VH_ACCOUNT_C = a.SALESFORCE_ACCOUNT_ID
        WHERE a.GVP = '{CONFIG["gvp_name"]}'
          AND u.USE_CASE_ACV > 0 AND u.IS_WENT_LIVE = FALSE AND u.IS_LOST = FALSE
          AND u.GO_LIVE_DATE BETWEEN '{CONFIG["quarter_start"]}' AND '{CONFIG["quarter_end"]}'
          AND u.USE_CASE_STAGE NOT IN ({excluded})
    """)
    total_acv = total_count = partner_acv = partner_count = sd_acv = sd_count = 0
    unassisted_acv = unassisted_count = 0
    partner_values = {"Partner Only", "Partner Prime + Snowflake SD", "Snowflake SD Prime + Partner"}
    sd_values = {"Snowflake SD Prime", "Partner Prime + Snowflake SD",
                 "Customer Prime + Snowflake SD", "Snowflake SD Prime + Partner"}
    unassisted_values = {"Customer Only", "Unknown", "None", "", None}
    for r in rows:
        acv = safe_float(r["ACV"] or 0)
        cnt = safe_int(r["CNT"] or 0)
        impl = r["IMPLEMENTER_C"] or ""
        total_acv += acv
        total_count += cnt
        if impl in partner_values:
            partner_acv += acv
            partner_count += cnt
        if impl in sd_values:
            sd_acv += acv
            sd_count += cnt
        if impl in unassisted_values:
            unassisted_acv += acv
            unassisted_count += cnt
    partner_rate = (partner_acv / total_acv * 100) if total_acv else 0
    sd_rate = (sd_acv / total_acv * 100) if total_acv else 0
    partner_accounts = set()
    sd_accounts = set()
    for r in acct_rows:
        impl = r.get("IMPLEMENTER_C") or ""
        aid = r.get("ACCOUNT_ID")
        if aid:
            if impl in partner_values:
                partner_accounts.add(aid)
            if impl in sd_values:
                sd_accounts.add(aid)
    partner_or_ps_accounts = partner_accounts | sd_accounts
    pps_cc_count = 0
    if partner_or_ps_accounts:
        pps_id_list = ", ".join(f"'{aid}'" for aid in partner_or_ps_accounts)
        try:
            cc_rows = run_query(f"""
                SELECT COUNT(DISTINCT SALESFORCE_ACCOUNT_ID) as CC_COUNT
                FROM SNOWPUBLIC.STREAMLIT.CC_USAGE_CACHE
                WHERE SALESFORCE_ACCOUNT_ID IN ({pps_id_list})
                  AND TOTAL_REQUESTS > 0
            """)
            pps_cc_count = safe_int(cc_rows[0]["CC_COUNT"]) if cc_rows else 0
        except Exception:
            pps_cc_count = 0
    return {
        "total_acv": total_acv, "total_count": total_count,
        "partner_acv": partner_acv, "partner_count": partner_count, "partner_rate": partner_rate,
        "sd_acv": sd_acv, "sd_count": sd_count, "sd_rate": sd_rate,
        "unassisted_acv": unassisted_acv, "unassisted_count": unassisted_count,
        "partner_or_ps_accounts": len(partner_or_ps_accounts),
        "partner_or_ps_cc_count": pps_cc_count,
    }


def q_pipeline_movements():
    if not CONFIG.get("is_current_quarter"):
        return {k: {"count": 0, "acv": 0} for k in ("won_to_imp", "won_to_lost", "pushed_out", "pulled_in", "imp_started", "new_pipeline")}
    gvp = CONFIG["gvp_name"]
    rows = run_query(f"""
        SELECT METRIC, CNT, ACV
        FROM SNOWPUBLIC.STREAMLIT.PIPELINE_MOVEMENTS_CACHE
        WHERE ACCOUNT_GVP = '{gvp}'
    """)
    result = {}
    for r in rows:
        m = r.get("METRIC", "")
        result[m] = {"count": safe_int(r.get("CNT", 0)), "acv": safe_float(r.get("ACV", 0))}
    for key in ("won_to_imp", "won_to_lost", "pushed_out", "pulled_in", "imp_started", "new_pipeline"):
        if key not in result:
            result[key] = {"count": 0, "acv": 0}
    return result


def q_use_case_velocity():
    if not CONFIG.get("is_current_quarter"):
        return {"time_to_tw": None, "tw_to_imp_start": None, "imp_to_deployed": None}
    gvp = CONFIG["gvp_name"]
    rows = run_query(f"""
        SELECT AVG_TW, AVG_TW_TO_IMP, AVG_IMP_TO_DEPLOYED
        FROM SNOWPUBLIC.STREAMLIT.VELOCITY_CACHE
        WHERE ACCOUNT_GVP = '{gvp}' AND METRIC_TYPE = 'stage_transition'
    """)
    r = rows[0] if rows else {}
    return {
        "time_to_tw": safe_float(r.get("AVG_TW")) if r.get("AVG_TW") is not None else None,
        "tw_to_imp_start": safe_float(r.get("AVG_TW_TO_IMP")) if r.get("AVG_TW_TO_IMP") is not None else None,
        "imp_to_deployed": safe_float(r.get("AVG_IMP_TO_DEPLOYED")) if r.get("AVG_IMP_TO_DEPLOYED") is not None else None,
    }


def q_bronze_created_qtd():
    created_rows = run_query(f"""
        SELECT COUNT(*) as CREATED_COUNT
        FROM {CONFIG["raven_uc_table"]} u
        JOIN {CONFIG["raven_acct_table"]} a ON u.VH_ACCOUNT_C = a.SALESFORCE_ACCOUNT_ID
        WHERE a.GVP = '{CONFIG["gvp_name"]}'
          AND u.TECHNICAL_CAMPAIGN_S_C ILIKE '{CONFIG["bronze_campaign"]}'
          AND u.CREATED_DATE BETWEEN '{CONFIG["quarter_start"]}' AND '{CONFIG["quarter_end"]}'
    """)
    created = safe_int(created_rows[0]["CREATED_COUNT"]) if created_rows else 0
    target_rows = run_query(f"""
        SELECT TARGET_USE_CASE_COUNT
        FROM SALES.REPORTING.SALES_PROGRAM_PRIORITIZED_FEATURES_TARGETS
        WHERE MAPPED_THEATER = '{_theater()}' AND FISCAL_QUARTER = '{CONFIG["fiscal_quarter"]}'
          AND MOVEMENT_TYPE = 'Created' AND PRIORITIZED_FEATURE_UC = 'Make Your Data AI Ready'
    """)
    target = safe_int(target_rows[0]["TARGET_USE_CASE_COUNT"]) if target_rows and target_rows[0]["TARGET_USE_CASE_COUNT"] else None
    return {"created": created, "target": target}


def q_si_theater_totals():
    rows = run_query(f"""
        SELECT COUNT(DISTINCT SALESFORCE_ACCOUNT_ID) as SI_ACCOUNTS,
               SUM(ACTIVE_USERS_LAST_30_DAYS) as SI_USERS_30D,
               ROUND(SUM(CREDITS_LAST_30_DAYS), 0) as SI_CREDITS_30D,
               ROUND(SUM(REVENUE_LAST_30_DAYS), 0) as SI_REVENUE_30D
        FROM SALES.REPORTING.BOB_SNOWFLAKE_INTELLIGENCE_USAGE_STREAMLIT_AGG
        WHERE GVP = '{CONFIG["gvp_name"]}' AND ACTIVE_ACCOUNT_LAST_30_DAYS = 1
    """)
    if rows:
        r = rows[0]
        return {
            "accounts": safe_int(r.get("SI_ACCOUNTS", 0) or 0),
            "users": safe_int(r.get("SI_USERS_30D", 0) or 0),
            "credits": safe_int(r.get("SI_CREDITS_30D", 0) or 0),
            "revenue": safe_float(r.get("SI_REVENUE_30D", 0) or 0),
        }
    return {"accounts": 0, "users": 0, "credits": 0, "revenue": 0}


def q_prior_fy_pacing(day_number, week_number):
    day_num = safe_int(day_number)
    week_days = safe_int(week_number) * 7
    quarters = CONFIG["prior_fy_quarters"]
    if not quarters:
        return {"day_avg": 0, "day_pct": 0, "week_avg": 0, "week_pct": 0}
    day_unions = []
    week_unions = []
    for qstart, qend in quarters:
        day_unions.append(f"""
            SELECT COALESCE(SUM(u.USE_CASE_ACV), 0) as DEPLOYED_ACV
            FROM {CONFIG["raven_uc_table"]} u
            JOIN {CONFIG["raven_acct_table"]} a ON u.VH_ACCOUNT_C = a.SALESFORCE_ACCOUNT_ID
            WHERE a.GVP = '{CONFIG["gvp_name"]}'
              AND u.USE_CASE_ACV > 0 AND u.IS_WENT_LIVE = TRUE
              AND u.DEFAULT_DATE BETWEEN '{qstart}' AND DATEADD('day', {day_num}-1, '{qstart}')
        """)
        week_unions.append(f"""
            SELECT COALESCE(SUM(u.USE_CASE_ACV), 0) as DEPLOYED_ACV
            FROM {CONFIG["raven_uc_table"]} u
            JOIN {CONFIG["raven_acct_table"]} a ON u.VH_ACCOUNT_C = a.SALESFORCE_ACCOUNT_ID
            WHERE a.GVP = '{CONFIG["gvp_name"]}'
              AND u.USE_CASE_ACV > 0 AND u.IS_WENT_LIVE = TRUE
              AND u.DEFAULT_DATE BETWEEN '{qstart}' AND DATEADD('day', {week_days}-1, '{qstart}')
        """)
    combined_sql = " UNION ALL ".join(day_unions)
    day_rows = run_query(f"SELECT AVG(DEPLOYED_ACV) as AVG_ACV FROM ({combined_sql})")
    combined_sql = " UNION ALL ".join(week_unions)
    week_rows = run_query(f"SELECT AVG(DEPLOYED_ACV) as AVG_ACV FROM ({combined_sql})")
    day_avg = float(day_rows[0]["AVG_ACV"] or 0)
    week_avg = float(week_rows[0]["AVG_ACV"] or 0)
    fy_final = CONFIG["prior_fy_avg_final"] * 1_000_000
    return {
        "day_avg": day_avg,
        "day_pct": (day_avg / fy_final * 100) if fy_final else 0,
        "week_avg": week_avg,
        "week_pct": (week_avg / fy_final * 100) if fy_final else 0,
    }


# =============================================================================
# MAIN: RUN ALL QUERIES AND GENERATE HTML
# =============================================================================

def run_all_queries():
    selected_quarter = get_current_quarter()
    CONFIG["is_current_quarter"] = selected_quarter["is_current"]
    if selected_quarter["is_current"]:
        CONFIG["reference_date"] = "CURRENT_DATE()"
    else:
        if date.fromisoformat(selected_quarter["end"]) < date.today():
            CONFIG["reference_date"] = f"'{selected_quarter['end']}'::DATE"
        else:
            CONFIG["reference_date"] = f"'{selected_quarter['start']}'::DATE"
    CONFIG["fiscal_quarter_key"] = selected_quarter["fiscal_quarter_key"]

    print(f"Quarter: {selected_quarter['label']}  |  GVP: {CONFIG['gvp_name']}  |  Theater: {_theater()}")

    # Phase 0: Sequential setup
    print("  [1/4] Fiscal calendar & velocity...")
    fiscal = q_fiscal_calendar(selected_quarter)
    CONFIG["day_number"] = safe_int(fiscal["DAY_NUMBER"])
    uc_velocity = q_use_case_velocity()
    update_risk_thresholds_from_velocity(uc_velocity)

    # Phase 1: Parallel queries
    print("  [2/4] Running queries (parallel)...")
    phase1_queries = {
        "forecasts": q_forecast_calls,
        "deployed": q_deployed_qtd,
        "pipeline": q_open_pipeline,
        "risk_analysis": q_pipeline_risk,
        "play_summary": q_sales_play_summary,
        "play_detail": q_play_detail_metrics,
        "bronze_tb_total": q_bronze_tb_total,
        "play_risk": q_play_risk_detail,
        "high_risk_ucs": q_high_risk_use_cases,
        "play_targets": q_play_targets,
        "partner_sd": q_partner_sd_attach,
        "pipeline_movements": q_pipeline_movements,
        "bronze_created": q_bronze_created_qtd,
        "si_theater": q_si_theater_totals,
    }
    phase1_with_args = {
        "pacing": lambda: q_prior_fy_pacing(fiscal["DAY_NUMBER"], fiscal["WEEK_NUMBER"]),
    }
    all_phase1 = {**phase1_queries, **phase1_with_args}
    results = {}
    errors = {}

    with ThreadPoolExecutor(max_workers=20) as executor:
        futures = {executor.submit(fn): name for name, fn in all_phase1.items()}
        for future in as_completed(futures):
            name = futures[future]
            try:
                results[name] = future.result()
            except Exception as e:
                errors[name] = e
                results[name] = {}

    if errors:
        for name, err in errors.items():
            print(f"  WARNING: Query '{name}' failed: {err}")

    return {
        "fiscal": fiscal,
        "forecasts": results["forecasts"],
        "deployed": results["deployed"],
        "pipeline": results["pipeline"],
        "risk_analysis": results["risk_analysis"],
        "play_summary": results["play_summary"],
        "play_detail": results["play_detail"],
        "play_risk": results["play_risk"],
        "high_risk_ucs": results["high_risk_ucs"],
        "play_targets": results["play_targets"],
        "partner_sd": results["partner_sd"],
        "uc_velocity": uc_velocity,
        "bronze_created": results["bronze_created"],
        "bronze_tb_total": results["bronze_tb_total"],
        "si_theater": results["si_theater"],
        "pacing": results["pacing"],
        "pipeline_movements": results.get("pipeline_movements", {}),
    }


def generate_script_html(data):
    """Generate the Script tab HTML — all three plays included."""
    print("  [3/4] Generating HTML...")
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
    pm = data.get("pipeline_movements", {})

    most_likely = forecasts.get("most_likely", 0)
    gl_target = forecasts.get("target", 0)
    deployed_pct = (deployed["acv"] / most_likely * 100) if most_likely else 0
    deployed_pct_of_target = (deployed["acv"] / gl_target * 100) if gl_target else 0
    coverage_pct = ((pipeline["acv"] + deployed["acv"]) / most_likely * 100) if most_likely else 0
    total_good = sum(safe_float(r.get("GOOD_ACV", 0) or 0) for r in risk_analysis.values())
    total_risk_acv = sum(safe_float(r.get("AT_RISK_ACV", 0) or 0) for r in risk_analysis.values())
    good_coverage = ((total_good + deployed["acv"]) / most_likely * 100) if most_likely else 0
    ml_wow_text = fmt_delta_text(forecasts.get("ml_delta"))
    day_number = safe_int(fiscal["DAY_NUMBER"])
    week_number = safe_int(fiscal["WEEK_NUMBER"])
    day_avg = pacing.get("day_avg", 0)
    day_pct_val = pacing.get("day_pct", 0)
    week_avg = pacing.get("week_avg", 0)
    week_pct_val = pacing.get("week_pct", 0)
    day_projection = (deployed["acv"] / (day_pct_val / 100)) if day_pct_val > 0 else None
    week_projection = (deployed["acv"] / (week_pct_val / 100)) if week_pct_val > 0 else None
    p_rate = partner_sd.get("partner_rate", 0)
    sd_rate = partner_sd.get("sd_rate", 0)
    p_acv = partner_sd.get("partner_acv", 0)
    p_cnt = partner_sd.get("partner_count", 0)
    sd_acv_val = partner_sd.get("sd_acv", 0)
    sd_cnt = partner_sd.get("sd_count", 0)
    unassisted_acv = partner_sd.get("unassisted_acv", 0)
    unassisted_cnt = partner_sd.get("unassisted_count", 0)
    unassisted_rate = (unassisted_acv / partner_sd.get("total_acv", 1) * 100) if partner_sd.get("total_acv") else 0
    partner_ps_total = partner_sd.get("partner_or_ps_accounts", 0)
    partner_ps_cc = partner_sd.get("partner_or_ps_cc_count", 0)
    partner_ps_cc_pct = round(partner_ps_cc / partner_ps_total * 100, 1) if partner_ps_total else 0
    uc_vel = uc_velocity
    v_tw = uc_vel.get("time_to_tw")
    v_imp = uc_vel.get("tw_to_imp_start")
    v_dep = uc_vel.get("imp_to_deployed")
    v_tw_str = f"{v_tw:.0f}" if v_tw is not None and not _is_nan(v_tw) else "N/A"
    v_imp_str = f"{v_imp:.0f}" if v_imp is not None and not _is_nan(v_imp) else "N/A"
    v_dep_str = f"{v_dep:.0f}" if v_dep is not None and not _is_nan(v_dep) else "N/A"
    high_risk_table_html = build_high_risk_table_html(high_risk_ucs)

    _pm_empty = {"count": 0, "acv": 0}
    pm_won_to_imp = pm.get("won_to_imp", _pm_empty)
    pm_won_to_lost = pm.get("won_to_lost", _pm_empty)
    pm_pushed_out = pm.get("pushed_out", _pm_empty)
    pm_pulled_in = pm.get("pulled_in", _pm_empty)
    pm_imp_started = pm.get("imp_started", _pm_empty)
    pm_new_pipeline = pm.get("new_pipeline", _pm_empty)

    script_html = f"""
<h3 style="color: #333; border-bottom: 2px solid #007bff; padding-bottom: 8px;">Forecast Call</h3>
<p>For {_theater()}, my Most Likely call for go-lives this quarter is <strong>{fmt_currency(most_likely)}</strong>{ml_wow_text}.
We have deployed <strong>{fmt_currency(deployed["acv"])}</strong> QTD against a target of <strong>{fmt_currency(gl_target)}</strong> (<strong>{fmt_pct(deployed_pct_of_target)}</strong> of target).
Our open pipeline is <strong>{fmt_currency(pipeline["acv"])}</strong>, giving us <strong>{fmt_pct(coverage_pct)}</strong> ML coverage (deployed + open pipeline vs Most Likely).</p>
<h3 style="color: #333; border-bottom: 2px solid #007bff; padding-bottom: 8px;">Pipeline (Last 7 Days)</h3>
<p>In the last 7 days, <strong>{pm_pushed_out["count"]}</strong> use cases (<strong>{fmt_currency(pm_pushed_out["acv"])}</strong>) were pushed out of the quarter
while <strong>{pm_pulled_in["count"]}</strong> (<strong>{fmt_currency(pm_pulled_in["acv"])}</strong>) were pulled in.
<strong>{pm_imp_started["count"]}</strong> use cases (<strong>{fmt_currency(pm_imp_started["acv"])}</strong>) started implementation with a go-live this quarter.
Net new pipeline (created in the last 7 days with a current-quarter go-live): <strong>{pm_new_pipeline["count"]}</strong> use cases, <strong>{fmt_currency(pm_new_pipeline["acv"])}</strong>.</p>
<h3 style="color: #333; border-bottom: 2px solid #007bff; padding-bottom: 8px;">Pacing</h3>
<p>On Day <strong>{day_number}</strong> of the quarter, our current deployed ACV of <strong>{fmt_currency(deployed["acv"])}</strong>
compares to a prior FY average of <strong>{fmt_currency(day_avg)}</strong> deployed by this day
(<strong>{fmt_pct(day_pct_val)}</strong> of the prior FY average final of <strong>{fmt_currency(CONFIG["prior_fy_avg_final"] * 1e6)}</strong>).
On a weekly basis (Week <strong>{week_number}</strong>), the prior FY average deployed was <strong>{fmt_currency(week_avg)}</strong>
(<strong>{fmt_pct(week_pct_val)}</strong> of final).</p>
<p>If the current quarter follows the same deployment curve as the prior FY average,
our projected quarter-end deployed ACV would be{f" <strong>{fmt_currency(day_projection)}</strong> based on daily pacing" if day_projection else " unavailable (no prior FY daily data)"}{f" and <strong>{fmt_currency(week_projection)}</strong> based on weekly pacing" if week_projection else ""}.</p>
<h3 style="color: #333; border-bottom: 2px solid #007bff; padding-bottom: 8px;">Sales Play Detail</h3>
"""

    _ps = play_summary
    _empty_ps = {"acv": 0, "count": 0}

    # Bronze
    br_dep = _ps.get("bronze_deployed", _empty_ps)
    br_open = _ps.get("bronze_open", _empty_ps)
    bronze_gap = fmt_play_gap(play_targets, "bronze_deployed", br_dep["acv"], br_dep["count"])
    bronze_target = fmt_play_target(play_targets, "bronze_deployed")
    bronze_risk = build_risk_narrative(play_risk.get("bronze", {"risk_rows": [], "total_count": 0}))
    br_created = bronze_created.get("created", 0) if isinstance(bronze_created, dict) else 0
    br_create_target = bronze_created.get("target") if isinstance(bronze_created, dict) else None
    br_create_target_str = str(br_create_target) if br_create_target is not None else "N/A"
    br_create_pct = f"{br_created / br_create_target * 100:.0f}%" if br_create_target else "N/A"
    script_html += f"""
<h4 style="color: #555;">Bronze (Make Your Data AI Ready)</h4>
<p>Bronze has deployed <strong>{fmt_currency(br_dep["acv"])}</strong> ({br_dep["count"]} UCs) QTD
with <strong>{fmt_currency(br_open["acv"])}</strong> ({br_open["count"]} UCs) in open pipeline.
Gap to deployed target: <strong>{bronze_gap}</strong>.
We have created <strong>{br_created}</strong> Bronze use cases this quarter against a creation target of <strong>{br_create_target_str}</strong> ({br_create_pct}).
QTD TB Ingested: <strong>{bronze_tb_total:.1f} TB</strong>.
Regional coverage: <strong>{play_detail.get("bronze", {}).get("regions", 0)}</strong> of 8 {_theater()} regions contributing.</p>
<div class="risk-box">
<strong>Risk Summary ({bronze_risk["at_risk"]} of {bronze_risk["total"]} use cases, {fmt_currency(bronze_risk["acv_at_risk"])} ACV at risk):</strong><br>
{bronze_risk["narrative_html"]}
</div>
"""

    # SI
    si_dep = _ps.get("si_deployed", _empty_ps)
    si_open = _ps.get("si_open", _empty_ps)
    si_gap = fmt_play_gap(play_targets, "si_deployed", si_dep["acv"], si_dep["count"])
    si_target_str = fmt_play_target(play_targets, "si_deployed")
    si_risk = build_risk_narrative(play_risk.get("si", {"risk_rows": [], "total_count": 0}))
    script_html += f"""
<h4 style="color: #555;">Snowflake Intelligence (AI: Snowflake Intelligence &amp; Agents)</h4>
<p>SI has deployed <strong>{fmt_currency(si_dep["acv"])}</strong> ({si_dep["count"]} UCs) QTD
with <strong>{fmt_currency(si_open["acv"])}</strong> ({si_open["count"]} UCs) in open pipeline.
Deployed target: <strong>{si_target_str}</strong>. Gap to target: <strong>{si_gap}</strong>.
Regional coverage: <strong>{play_detail.get("si", {}).get("regions", 0)}</strong> of 8 {_theater()} regions contributing.</p>
<p>Theater SI Usage (Last 30 Days): {si_theater.get("accounts", 0):,} Accounts | {si_theater.get("users", 0):,} Users | {si_theater.get("credits", 0):,} Credits | {fmt_currency(si_theater.get("revenue", 0))} Revenue.</p>
<div class="risk-box">
<strong>Risk Summary ({si_risk["at_risk"]} of {si_risk["total"]} use cases, {fmt_currency(si_risk["acv_at_risk"])} ACV at risk):</strong><br>
{si_risk["narrative_html"]}
</div>
"""

    # SQL Server
    sql_dep = _ps.get("sqlserver_deployed", _empty_ps)
    sql_open = _ps.get("sqlserver_open", _empty_ps)
    sql_gap = fmt_play_gap(play_targets, "sqlserver_deployed", sql_dep["acv"], sql_dep["count"])
    sqlserver_target = fmt_play_target(play_targets, "sqlserver_deployed")
    sql_risk = build_risk_narrative(play_risk.get("sqlserver", {"risk_rows": [], "total_count": 0}))
    script_html += f"""
<h4 style="color: #555;">SQL Server Migration (Modernize Your Data Estate)</h4>
<p>SQL Server has deployed <strong>{fmt_currency(sql_dep["acv"])}</strong> ({sql_dep["count"]} UCs) QTD
with <strong>{fmt_currency(sql_open["acv"])}</strong> ({sql_open["count"]} UCs) in open pipeline.
Deployed target: <strong>{sqlserver_target}</strong>. Gap to target: <strong>{sql_gap}</strong>.
Regional coverage: <strong>{play_detail.get("sqlserver", {}).get("regions", 0)}</strong> of 8 {_theater()} regions contributing.</p>
<div class="risk-box">
<strong>Risk Summary ({sql_risk["at_risk"]} of {sql_risk["total"]} use cases, {fmt_currency(sql_risk["acv_at_risk"])} ACV at risk):</strong><br>
{sql_risk["narrative_html"]}
</div>
"""

    script_html += f"""
<h3 style="color: #333; border-bottom: 2px solid #007bff; padding-bottom: 8px;">Risk</h3>
<p>Total pipeline risk stands at <strong>{fmt_currency(total_risk_acv)}</strong>, leaving
<strong>{fmt_currency(total_good)}</strong> in good pipeline for <strong>{fmt_pct(good_coverage)}</strong> good coverage vs Most Likely.
We are currently at <strong>{fmt_pct(deployed_pct_of_target)}</strong> of our go-live target.</p>
{high_risk_table_html}
<h3 style="color: #333; border-bottom: 2px solid #007bff; padding-bottom: 8px;">Partner and SD Attach</h3>
<p>Partner attach rate on the open pipeline is <strong>{fmt_pct(p_rate)}</strong>
({p_cnt} use cases, {fmt_currency(p_acv)} ACV).
SD attach rate is <strong>{fmt_pct(sd_rate)}</strong>
({sd_cnt} use cases, {fmt_currency(sd_acv_val)} ACV).
The remaining <strong>{unassisted_cnt}</strong> use cases ({fmt_currency(unassisted_acv)} ACV, {fmt_pct(unassisted_rate)}) are unassisted (Customer Only, Unknown, or None).
Of the <strong>{partner_ps_total}</strong> accounts with Partner or PS-attached use cases, <strong>{partner_ps_cc}</strong> (<strong>{partner_ps_cc_pct}%</strong>) are actively using Cortex Code CLI.</p>
<h3 style="color: #333; border-bottom: 2px solid #007bff; padding-bottom: 8px;">Use Case Velocity</h3>
<p>Average stage transition times for use cases created since FY26 Q1 (all stages):</p>
<div class="timeline-box">
  <div class="timeline-item"><span class="timeline-label">Created to TW</span><span class="timeline-days">{v_tw_str}</span></div>
  <div class="timeline-arrow">&rarr;</div>
  <div class="timeline-item"><span class="timeline-label">TW to Imp Start</span><span class="timeline-days">{v_imp_str}</span></div>
  <div class="timeline-arrow">&rarr;</div>
  <div class="timeline-item"><span class="timeline-label">Imp Start to Deployed</span><span class="timeline-days">{v_dep_str}</span></div>
</div>
"""

    # Wrap in full HTML document
    css = """
<style>
    body { font-family: Georgia, serif; margin: 40px auto; max-width: 900px; padding: 0 20px; color: #333; font-size: 1.05em; line-height: 1.7; }
    h3 { color: #333; border-bottom: 2px solid #007bff; padding-bottom: 8px; }
    h4 { color: #555; }
    a { color: #007bff; text-decoration: none; }
    .risk-box { background: #fff5f5; border-left: 4px solid #dc3545; padding: 15px; margin: 15px 0; }
    .timeline-box { display: flex; align-items: center; justify-content: center; gap: 12px; padding: 10px 0; margin: 5px 0; }
    .timeline-item { text-align: center; background: #1a1a2e; border-radius: 8px; padding: 12px 20px; min-width: 120px; box-shadow: 0 3px 10px rgba(0,0,0,0.15); }
    .timeline-label { display: block; font-size: 0.8em; color: #aaa; margin-bottom: 6px; text-transform: uppercase; letter-spacing: 0.5px; }
    .timeline-days { display: block; font-size: 2em; font-weight: bold; color: #29B5E8; }
    .timeline-arrow { font-size: 3em; color: #29B5E8; font-weight: bold; }
</style>
"""

    quarter_label = f"FY{fiscal['FISCAL_YEAR'] % 100}-{fiscal['FISCAL_QUARTER']}"
    full_html = f"""<!DOCTYPE html>
<html lang="en">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>PEAK QC Script — {_theater()} {quarter_label}</title>
    {css}
</head>
<body>
<h2>PEAK QC Script — {_theater()} {quarter_label}</h2>
<p style="color: #666; font-size: 0.9em;">Generated: {datetime.now().strftime("%Y-%m-%d %H:%M")}</p>
{script_html}
</body>
</html>"""

    return full_html


# =============================================================================
# ENTRY POINT
# =============================================================================

def main():
    output_file = None
    if len(sys.argv) > 1 and sys.argv[1] == "--output" and len(sys.argv) > 2:
        output_file = sys.argv[2]

    start = time.time()
    data = run_all_queries()
    html = generate_script_html(data)

    if output_file is None:
        quarter = get_current_quarter()
        output_file = f"PEAK_{_theater()}_{quarter['label']}.html"

    print(f"  [4/4] Writing {output_file}...")
    with open(output_file, "w") as f:
        f.write(html)

    elapsed = time.time() - start
    print(f"Done in {elapsed:.1f}s -> {output_file}")


if __name__ == "__main__":
    main()
