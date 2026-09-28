"""
PEAK Qualify & Commit — Streamlit in Snowflake (SiS) App
Single-file deployment for Snowflake Streamlit.

Combines all query functions, helpers, and rendering from peak_report.py + peak_app.py
into a single standalone file compatible with Snowpark session execution.
"""

import math
import os
import re
import statistics
import time
import html as html_lib
from datetime import datetime, date
from collections import defaultdict
from concurrent.futures import ThreadPoolExecutor, as_completed

import streamlit as st
import pandas as pd

# =============================================================================
# SNOWFLAKE SESSION (SiS + local dev fallback)
# =============================================================================
try:
    from snowflake.snowpark.context import get_active_session
    session = get_active_session()
    _use_snowpark = True
except Exception:
    import snowflake.connector
    session = snowflake.connector.connect(
        connection_name=os.getenv("SNOWFLAKE_CONNECTION_NAME", "MyConnection")
    )
    _use_snowpark = False

# Activate all viewer roles so the app can access data beyond the owner role
if _use_snowpark:
    try:
        session.sql("USE SECONDARY ROLES ALL").collect()
    except Exception:
        pass  # May not be supported in all contexts


def run_query(sql, _retries=2, _delay=3):
    """Execute SQL and return results as list of dicts.
    Retries on transient errors (e.g. view refresh, object not found)."""
    last_err = None
    for attempt in range(1 + _retries):
        try:
            if _use_snowpark:
                df = session.sql(sql).to_pandas()
            else:
                cur = session.cursor()
                cur.execute("USE WAREHOUSE SNOWADHOC")
                cur.execute(sql)
                cols = [desc[0] for desc in cur.description]
                df = pd.DataFrame(cur.fetchall(), columns=cols)
                cur.close()
            # Convert DataFrame to list of dicts for compatibility
            records = df.to_dict("records")
            # Ensure numeric types are Python native (not numpy)
            for row in records:
                for k, v in row.items():
                    if hasattr(v, "item"):
                        row[k] = v.item()
            return records
        except Exception as e:
            last_err = e
            err_msg = str(e)
            # Retry on transient object-not-found / auth errors (view being refreshed)
            if attempt < _retries and ("does not exist or not authorized" in err_msg
                                       or "Object does not exist" in err_msg):
                time.sleep(_delay)
                # Re-activate secondary roles in case session context was lost
                if _use_snowpark:
                    try:
                        session.sql("USE SECONDARY ROLES ALL").collect()
                    except Exception:
                        pass
                continue
            raise last_err


# =============================================================================
# CONFIGURATION
# =============================================================================

CONFIG = {
    "warehouse": "SNOWADHOC",
    # Scope is the THEATER. gvp_name / gvp_email are RESOLVED at load time by
    # _resolve_gvp() — never hard-code a person here (see _resolve_gvp docstring).
    "theater": "AMSExpansion",
    "gvp_name": "",
    "gvp_email": "",
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
    "dim_uc_table": "(SELECT * FROM SNOWPUBLIC.STREAMLIT.DIM_USE_CASE_HISTORY_DS_VW WHERE DS = (SELECT MAX(DS) FROM SNOWPUBLIC.STREAMLIT.DIM_USE_CASE_HISTORY_DS_VW))",
}

RISK_CATEGORIES = [
    "Technical Fit", "Time / Resources", "Competitor",
    "Access to the Customer", "Performance", "Consumption",
]

# Theaters are the stable scope. GVP names are NOT: on 2026-09 AMSExpansion's GVP
# became the placeholder "(TBH)  AMSExpansion GVP" (double space) and USMajors
# moved from Jonathan Beaulier to Josh Sullivan, silently zeroing every
# GVP-name filter. Scope on theater and resolve the person at runtime.
THEATER_OPTIONS = [
    "AMSExpansion", "AMSAcquisition", "APJ", "EMEA", "USMajors", "USPubSec",
]


def _theater():
    """Return the current theater scope."""
    return CONFIG["theater"]


def _resolve_gvp(theater):
    """
    Resolve the CURRENT GVP name and email for a theater from the account table.

    Needed only for the sources that have no theater column and key on the
    person: MaxIQ PEAK_FORECAST_CALLS_PIPELINE_TARGETS.USER_NAME and
    BOB_SNOWFLAKE_INTELLIGENCE_USAGE_STREAMLIT_AGG.GVP. Every other query scopes on
    theater directly. Because this is looked up per load, the next rename or
    backfill of a GVP heals itself with no code change.
    """
    try:
        rows = run_query(f"""
            SELECT GVP, GVP_EMAIL, COUNT(*) AS N
            FROM SALES.RAVEN.D_SALESFORCE_ACCOUNT_CUSTOMERS
            WHERE GEO = '{theater}' AND GVP IS NOT NULL
            GROUP BY GVP, GVP_EMAIL
            ORDER BY N DESC
            LIMIT 1
        """)
        if rows:
            return safe_str(rows[0].get("GVP", "")), safe_str(rows[0].get("GVP_EMAIL", ""))
    except Exception:
        pass
    return "", ""


def _quarter_label(qs):
    """Derive FY/Q label (e.g. 'FY27 Q1') from a quarter start date string."""
    d = date.fromisoformat(qs)
    fy = d.year + 1 if d.month >= 2 else d.year
    q = 1 if d.month in (2, 3, 4) else 2 if d.month in (5, 6, 7) else 3 if d.month in (8, 9, 10) else 4
    return f"FY{fy % 100} Q{q}"


def compute_fiscal_quarters():
    """Return list of fiscal quarter dicts for selector.
    Snowflake FY starts Feb 1: Q1=Feb-Apr, Q2=May-Jul, Q3=Aug-Oct, Q4=Nov-Jan.
    Includes all quarters in current FY + prior FY."""
    today = date.today()
    m, y = today.month, today.year

    # Determine current FY and quarter
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
    else:  # January
        fy = y
        current_q = 4

    # Quarter boundary helper: given FY and Q, return (start, end, cal_year_of_start)
    def _qtr_dates(fiscal_year, q):
        # FY starts in Feb of (fiscal_year - 1)
        base_year = fiscal_year - 1
        if q == 1:
            return (date(base_year, 2, 1), date(base_year, 4, 30))
        elif q == 2:
            return (date(base_year, 5, 1), date(base_year, 7, 31))
        elif q == 3:
            return (date(base_year, 8, 1), date(base_year, 10, 31))
        else:  # Q4
            return (date(base_year, 11, 1), date(base_year + 1, 1, 31))

    quarters = []
    # Prior FY (all 4 quarters)
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
    # Current FY (all 4 quarters)
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


# =============================================================================
# FORMATTING HELPERS
# =============================================================================

def _is_nan(v):
    try:
        return v != v  # NaN != NaN is True
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


def fmt_int(value):
    if value is None or _is_nan(value):
        return "0"
    return f"{int(value):,}"


def safe_str(value):
    if value is None:
        return ""
    return str(value).strip()


def html_escape(value):
    return html_lib.escape(safe_str(value))


def get_forecast_class(forecast_status):
    s = safe_str(forecast_status).lower()
    if s == "commit":
        return "commit"
    elif s in ("most likely", "mostlikely"):
        return "likely"
    elif s in ("stretch", "best case", "bestcase"):
        return "stretch"
    else:
        return "none"


def get_forecast_label(forecast_status):
    s = safe_str(forecast_status)
    if not s or s.lower() == "none":
        return "None"
    return s


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


def extract_recent_comments(text, n=3):
    """
    Return the n most recent dated comment blocks, newest first, as a list.

    SE_COMMENTS fields run to 4,000+ characters of dated history. extract_latest_comment()
    keeps only the newest block, which discards most of the narrative. This keeps the
    newest n so the summary carries real context without dumping the whole log.
    Falls back to the entire string when no date headers are detected.
    """
    s = safe_str(text).strip()
    if not s:
        return []
    matches = list(_DATE_LINE_RE.finditer(s))
    if len(matches) <= 1:
        return [s]
    blocks = []
    for i, m in enumerate(matches[:n]):
        end = matches[i + 1].start() if i + 1 < len(matches) else len(s)
        block = s[m.start():end].strip()
        if block:
            blocks.append(block)
    return blocks


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


# =============================================================================
# SQL QUERIES
# =============================================================================

def _gvp_filter(alias=None):
    """
    Account-scope filter for the current THEATER (name kept for its ~45 callers).

    Uses the raven account table's GEO, not a GVP name: GVP names get renamed
    (AMSExpansion became "(TBH)  AMSExpansion GVP" in 2026-09, which matched zero
    rows against the old 'Mark Fleming' literal). GEO covers the identical account
    set and still captures accounts whose MDM ACCOUNT_GVP is NULL.
    """
    theater = CONFIG["theater"]
    col = f"{alias}.ACCOUNT_ID" if alias else "ACCOUNT_ID"
    return (
        f"{col} IN ("
        f"SELECT SALESFORCE_ACCOUNT_ID "
        f"FROM SALES.RAVEN.D_SALESFORCE_ACCOUNT_CUSTOMERS "
        f"WHERE GEO = '{theater}')"
    )

def q_fiscal_calendar(selected_quarter):
    # Use the selected quarter's pre-computed dates instead of CURRENT_DATE()
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

    # Compute day/week number and days remaining in Python (no SQL needed)
    _fq_start_d = date.fromisoformat(fq_start)
    _fq_end_d = date.fromisoformat(fq_end)
    if selected_quarter["is_current"]:
        _ref_d = date.today()
    elif _fq_end_d < date.today():
        _ref_d = _fq_end_d
    else:
        _ref_d = _fq_start_d
    day_number = (_ref_d - _fq_start_d).days + 1
    week_number = math.ceil(day_number / 7.0)
    fiscal["DAY_NUMBER"] = day_number
    fiscal["WEEK_NUMBER"] = week_number
    fiscal["DAYS_REMAINING"] = (_fq_end_d - _ref_d).days + 1
    CONFIG["days_remaining"] = fiscal["DAYS_REMAINING"]

    # Prior FY quarters: FY starts Feb 1, so prior FY = fy-1
    prior_fy_start_year = fy - 2  # calendar year when prior FY starts (Feb)
    prior_quarters = [
        (f"{prior_fy_start_year}-02-01", f"{prior_fy_start_year}-04-30"),
        (f"{prior_fy_start_year}-05-01", f"{prior_fy_start_year}-07-31"),
        (f"{prior_fy_start_year}-08-01", f"{prior_fy_start_year}-10-31"),
        (f"{prior_fy_start_year}-11-01", f"{prior_fy_start_year + 1}-01-31"),
    ]
    # Also include any completed quarters from the current FY
    cur_fy_start_year = fy - 1
    cur_fy_quarters = [
        (f"{cur_fy_start_year}-02-01", f"{cur_fy_start_year}-04-30"),
        (f"{cur_fy_start_year}-05-01", f"{cur_fy_start_year}-07-31"),
        (f"{cur_fy_start_year}-08-01", f"{cur_fy_start_year}-10-31"),
        (f"{cur_fy_start_year}-11-01", f"{cur_fy_start_year + 1}-01-31"),
    ]
    today_str = date.today().isoformat()
    for qs, qe in cur_fy_quarters:
        if qe < today_str:
            prior_quarters.append((qs, qe))
    CONFIG["prior_fy_quarters"] = prior_quarters

    if prior_quarters:
        unions = []
        for qs, qe in prior_quarters:
            unions.append(f"""
                SELECT COALESCE(SUM(USE_CASE_EACV), 0) as DEPLOYED_ACV
                FROM SNOWPUBLIC.STREAMLIT.DIM_USE_CASE_MDM_CACHE
                WHERE {_gvp_filter()}
                  AND USE_CASE_EACV > 0 AND IS_DEPLOYED = TRUE
                  AND GO_LIVE_DATE BETWEEN '{qs}' AND '{qe}'
            """)
        avg_rows = run_query(f"""
            SELECT ROUND(AVG(DEPLOYED_ACV) / 1000000, 1) as AVG_FINAL_M
            FROM ({' UNION ALL '.join(unions)})
        """)
        CONFIG["prior_fy_avg_final"] = float(avg_rows[0]["AVG_FINAL_M"] or 0)
    else:
        CONFIG["prior_fy_avg_final"] = 0
    return fiscal


def q_regional_targets():
    """Fetch GVP-level targets from GVP_TARGET_CACHE."""
    fq_key = CONFIG["fiscal_quarter_key"]
    gvp = CONFIG["gvp_name"]
    rows = run_query(f"""
        -- Scoped on THEATER, not OWNER_NAME: a theater can carry two owner rows
        -- across a GVP change (USMajors has Beaulier + Sullivan), so MAX collapses them.
        SELECT TARGET_TYPE, MAX(TARGET_VALUE) AS TARGET_VALUE
        FROM SNOWPUBLIC.STREAMLIT.GVP_TARGET_CACHE
        WHERE THEATER = '{_theater()}'
          AND TARGET_LEVEL = 'GVP'
          AND FISCAL_QUARTER = '{fq_key}'
        GROUP BY TARGET_TYPE
    """)
    return {r["TARGET_TYPE"]: safe_float(r["TARGET_VALUE"]) for r in rows}


def q_qtd_revenue():
    fq_key = CONFIG["fiscal_quarter_key"]
    rows = run_query(f"""
        SELECT FORECAST_TYPE, FORECAST_AMOUNT
        FROM SALES.REPORTING.PEAK_FORECAST_CALLS_PIPELINE_TARGETS
        WHERE USER_NAME = '{CONFIG["gvp_name"]}'
          AND FUNCTION = '{CONFIG["gvp_function"]}'
          AND TYPE = 'Consumption'
          AND FISCAL_QUARTER = '{fq_key}'
          AND LATEST_DATE = TRUE
    """)
    assert len(rows) >= 3, f"Expected at least 3 revenue rows, got {len(rows)}"
    revenue_map = {r["FORECAST_TYPE"]: safe_float(r["FORECAST_AMOUNT"]) for r in rows}
    fy = CONFIG["fiscal_year"]
    fy_rows = run_query(f"""
        SELECT SUM(FORECAST_AMOUNT) as FY_FORECAST
        FROM SALES.REPORTING.PEAK_FORECAST_CALLS_PIPELINE_TARGETS
        WHERE USER_NAME = '{CONFIG["gvp_name"]}'
          AND FUNCTION = '{CONFIG["gvp_function"]}'
          AND TYPE = 'Consumption'
          AND FORECAST_TYPE = 'Total'
          AND LATEST_DATE = TRUE
          AND FISCAL_QUARTER LIKE '{fy}%'
    """)
    return {
        "revenue": revenue_map.get("Actual", 0),
        "q1_forecast": revenue_map.get("Total", 0),
        "target": revenue_map.get("Target", 0),
        "fy_forecast": float(fy_rows[0]["FY_FORECAST"]),
    }


def q_theater_consumption():
    """Fetch QTD, YTD, and FY consumption metrics from the pipeline targets table."""
    fy = CONFIG["fiscal_year"]
    fq_key = CONFIG["fiscal_quarter_key"]   # e.g. "2027-Q2"
    gvp = CONFIG["gvp_name"]
    function = CONFIG["gvp_function"]
    q_num = int(fq_key.split("-Q")[-1])     # current quarter number 1-4

    rows = run_query(f"""
        SELECT FISCAL_QUARTER, FORECAST_TYPE, FORECAST_AMOUNT
        FROM SALES.REPORTING.PEAK_FORECAST_CALLS_PIPELINE_TARGETS
        WHERE USER_NAME = '{gvp}'
          AND FUNCTION = '{function}'
          AND TYPE = 'Consumption'
          AND FORECAST_TYPE IN ('Actual', 'Total', 'Target')
          AND FISCAL_QUARTER LIKE '{fy}-%'
          AND LATEST_DATE = TRUE
        ORDER BY FISCAL_QUARTER
    """)

    by_q = {}
    for r in rows:
        q = r["FISCAL_QUARTER"]
        t = r["FORECAST_TYPE"]
        by_q.setdefault(q, {})[t] = safe_float(r["FORECAST_AMOUNT"])

    cq = by_q.get(fq_key, {})
    cq_actual   = cq.get("Actual", 0)
    cq_target   = cq.get("Target", 0)
    cq_forecast = cq.get("Total", 0)

    ytd_actual = ytd_target = ytd_forecast = 0
    for qi in range(1, q_num + 1):
        qk = f"{fy}-Q{qi}"
        qd = by_q.get(qk, {})
        ytd_target   += qd.get("Target", 0)
        ytd_actual   += qd.get("Actual", 0)
        ytd_forecast += qd.get("Actual", 0) if qi < q_num else qd.get("Total", 0)

    fy_target   = sum(by_q.get(f"{fy}-Q{qi}", {}).get("Target", 0) for qi in range(1, 5))
    fy_forecast = sum(
        by_q.get(f"{fy}-Q{qi}", {}).get("Actual" if qi < q_num else "Total", 0)
        for qi in range(1, 5)
    )

    q_labels = {1: "Feb–Apr", 2: "May–Jul", 3: "Aug–Oct", 4: "Nov–Jan"}
    fy_label = f"FY{fy % 100}"
    breakdown = []
    for qi in range(1, 5):
        qk = f"{fy}-Q{qi}"
        qd = by_q.get(qk, {})
        status = "actual" if qi < q_num else ("current" if qi == q_num else "future")
        breakdown.append({
            "label": f"{fy_label} Q{qi} ({q_labels[qi]})",
            "target": qd.get("Target", 0),
            "value": qd.get("Actual", 0) if qi < q_num else qd.get("Total", 0),
            "status": status,
        })

    return {
        "cq_actual": cq_actual, "cq_target": cq_target, "cq_forecast": cq_forecast,
        "ytd_actual": ytd_actual, "ytd_target": ytd_target, "ytd_forecast": ytd_forecast,
        "fy_target": fy_target, "fy_forecast": fy_forecast,
        "breakdown": breakdown, "q_num": q_num,
    }



def q_wins_qtd():
    """QTD wins ACV and count from MDM cache (IS_WON + DECISION_DATE = matches MaxIQ)."""
    qs, qe = CONFIG["quarter_start"], CONFIG["quarter_end"]
    rows = run_query(f"""
        SELECT SUM(USE_CASE_EACV) as WINS_ACV, COUNT(*) as WINS_COUNT
        FROM SNOWPUBLIC.STREAMLIT.DIM_USE_CASE_MDM_CACHE
        WHERE {_gvp_filter()}
          AND USE_CASE_EACV > 0 AND IS_WON = TRUE
          AND DECISION_DATE BETWEEN '{qs}' AND '{qe}'
    """)
    r = rows[0] if rows else {}
    return {"acv": safe_float(r.get("WINS_ACV") or 0), "count": safe_int(r.get("WINS_COUNT") or 0)}


def q_last7_wins():
    """Wins in the last 7 days."""
    ref_date = CONFIG["reference_date"]
    rows = run_query(f"""
        SELECT SUM(USE_CASE_EACV) as WINS_ACV, COUNT(*) as WINS_COUNT
        FROM SNOWPUBLIC.STREAMLIT.DIM_USE_CASE_MDM_CACHE
        WHERE {_gvp_filter()}
          AND USE_CASE_EACV > 0 AND IS_WON = TRUE
          AND DECISION_DATE >= DATEADD('day', -7, {ref_date})
          AND DECISION_DATE <= {ref_date}
    """)
    r = rows[0] if rows else {}
    return {"acv": safe_float(r.get("WINS_ACV") or 0), "count": safe_int(r.get("WINS_COUNT") or 0)}


def q_wins_forecast_calls():
    """Commit/ML/BestCase/Won/Open from pipeline targets + GVP target for wins."""
    fq_key = CONFIG["fiscal_quarter_key"]
    gvp = CONFIG["gvp_name"]
    function = CONFIG["gvp_function"]
    rows = run_query(f"""
        SELECT FORECAST_TYPE, FORECAST_AMOUNT, LATEST_DATE, PREVIOUS_WEEK
        FROM SALES.REPORTING.PEAK_FORECAST_CALLS_PIPELINE_TARGETS
        WHERE USER_NAME = '{gvp}'
          AND FUNCTION = '{function}'
          AND TYPE = 'Use Case Wins'
          AND FORECAST_TYPE IN ('CommitForecast', 'MostLikelyForecast', 'BestCaseForecast', 'Won', 'Open', 'Mature')
          AND FISCAL_QUARTER = '{fq_key}'
          AND (LATEST_DATE = TRUE OR PREVIOUS_WEEK = TRUE)
    """)
    latest = {r["FORECAST_TYPE"]: safe_float(r["FORECAST_AMOUNT"]) for r in rows if r["LATEST_DATE"]}
    prior  = {r["FORECAST_TYPE"]: safe_float(r["FORECAST_AMOUNT"]) for r in rows if r["PREVIOUS_WEEK"]}
    def _delta(key):
        cur = latest.get(key); prv = prior.get(key)
        return (cur - prv) if (cur is not None and prv is not None) else None
    # Fetch wins target from GVP target cache
    target_rows = run_query(f"""
        SELECT MAX(TARGET_VALUE) AS TARGET_VALUE FROM SNOWPUBLIC.STREAMLIT.GVP_TARGET_CACHE
        WHERE THEATER = '{_theater()}' AND TARGET_LEVEL = 'GVP'
          AND FISCAL_QUARTER = '{fq_key}' AND TARGET_TYPE = 'Use Case Won'
    """)
    wins_target = safe_float(target_rows[0]["TARGET_VALUE"]) if target_rows else 0
    return {
        "commit": latest.get("CommitForecast", 0),
        "most_likely": latest.get("MostLikelyForecast", 0),
        "stretch": latest.get("BestCaseForecast", 0),
        "won_actual": latest.get("Won", 0),
        "open_pipeline": latest.get("Open", 0),
        "mature": latest.get("Mature", 0),
        "target": wins_target,
        "commit_delta": _delta("CommitForecast"),
        "ml_delta": _delta("MostLikelyForecast"),
        "stretch_delta": _delta("BestCaseForecast"),
    }


def q_wins_open_pipeline():
    """
    Open pipeline for wins — not yet won, not lost, DECISION_DATE in quarter.

    Wins are DECISION_DATE events, not go-live events; gating this on
    GO_LIVE_DATE selected a different population entirely. Stage 0
    ("Not In Pursuit") is excluded — it is not real pipeline.
    Verified 2026-09-17: this ties to MaxIQ 'Open' for FY27-Q3 exactly
    ($196,724,457.41). Do NOT reintroduce a GO_LIVE_DATE gate here.
    """
    qs, qe = CONFIG["quarter_start"], CONFIG["quarter_end"]
    rows = run_query(f"""
        SELECT SUM(USE_CASE_EACV) as OPEN_ACV, COUNT(*) as OPEN_COUNT
        FROM SNOWPUBLIC.STREAMLIT.DIM_USE_CASE_MDM_CACHE
        WHERE {_gvp_filter()}
          AND USE_CASE_EACV > 0
          AND IS_WON = FALSE
          AND COALESCE(IS_LOST, FALSE) = FALSE
          AND STAGE_NUMBER BETWEEN 1 AND 6
          AND DECISION_DATE BETWEEN '{qs}' AND '{qe}'
    """)
    r = rows[0] if rows else {}
    return {"acv": safe_float(r.get("OPEN_ACV") or 0), "count": safe_int(r.get("OPEN_COUNT") or 0)}


def q_wins_top5():
    """
    Top open-pipeline UCs by ACV for the wins tab (returns up to 10).

    Population MUST match q_wins_open_pipeline: DECISION_DATE in quarter and
    STAGE_NUMBER 1-6. A GO_LIVE_DATE gate here previously drew the "top" list
    from ~5% of the real pipeline and buried the largest opportunities.
    """
    qs, qe = CONFIG["quarter_start"], CONFIG["quarter_end"]
    rows = run_query(f"""
        SELECT u.USE_CASE_ID, u.USE_CASE_NAME, u.USE_CASE_EACV, u.USE_CASE_STAGE,
               u.STAGE_NUMBER, u.DAYS_IN_STAGE, u.ACCOUNT_NAME, u.ACCOUNT_ID,
               u.REGION_NAME, u.USE_CASE_DESCRIPTION, u.IS_PARTNER_ATTACHED,
               u.TECHNICAL_WIN_DATE, u.GO_LIVE_DATE, u.USE_CASE_RISK,
               u.SPECIALIST_COMMENTS, u.IS_TECH_WON
        FROM SNOWPUBLIC.STREAMLIT.DIM_USE_CASE_MDM_CACHE u
        WHERE {_gvp_filter('u')}
          AND u.USE_CASE_EACV > 0
          AND u.IS_WON = FALSE
          AND COALESCE(u.IS_LOST, FALSE) = FALSE
          AND u.STAGE_NUMBER BETWEEN 1 AND 6
          AND u.DECISION_DATE BETWEEN '{qs}' AND '{qe}'
        ORDER BY u.USE_CASE_EACV DESC
        LIMIT 10
    """)
    return rows


def q_wins_risk_analysis():
    """Fetch open wins pipeline UCs with risk data for deep analysis."""
    gvp = CONFIG["gvp_name"]
    rows = run_query(f"""
        SELECT USE_CASE_EACV, ACCOUNT_NAME, USE_CASE_NAME, USE_CASE_RISK,
               RISK_DESCRIPTION, SPECIALIST_COMMENTS, STAGE_NUMBER, USE_CASE_STAGE
        FROM SNOWPUBLIC.STREAMLIT.DIM_USE_CASE_MDM_CACHE
        WHERE THEATER_NAME = '{_theater()}'
          AND USE_CASE_EACV > 0
          AND IS_WON = FALSE
          AND COALESCE(IS_LOST, FALSE) = FALSE
          AND (
              (USE_CASE_RISK IS NOT NULL AND UPPER(USE_CASE_RISK) != 'NONE')
              OR RISK_DESCRIPTION IS NOT NULL
              OR SPECIALIST_COMMENTS IS NOT NULL
          )
        ORDER BY USE_CASE_EACV DESC
        LIMIT 150
    """)
    return rows


def q_wins_pipeline_movements():
    """7-day wins pipeline movements: pushed out, pulled in, tech wins, new pipeline."""
    qs, qe = CONFIG["quarter_start"], CONFIG["quarter_end"]
    ref_date = CONFIG["reference_date"]
    snap = "SNOWPUBLIC.STREAMLIT.DIM_USE_CASE_HISTORY_DS_VW"
    try:
        rows = run_query(f"""
            WITH latest AS (
                SELECT USE_CASE_ID, GO_LIVE_DATE, IS_TECH_WON, USE_CASE_EACV, CREATED_DATE
                FROM {snap}
                WHERE DS = (SELECT MAX(DS) FROM {snap})
                  AND {_gvp_filter()} AND USE_CASE_EACV > 0
            ),
            prior AS (
                SELECT USE_CASE_ID, GO_LIVE_DATE, IS_TECH_WON
                FROM {snap}
                WHERE DS = (SELECT MAX(DS) FROM {snap} WHERE DS <= DATEADD('day', -7, {ref_date}))
                  AND {_gvp_filter()} AND USE_CASE_EACV > 0
            )
            SELECT
                SUM(CASE WHEN p.GO_LIVE_DATE BETWEEN '{qs}' AND '{qe}'
                         AND COALESCE(p.IS_TECH_WON, FALSE) = FALSE
                         AND l.USE_CASE_ID IS NOT NULL
                         AND (l.GO_LIVE_DATE > '{qe}' OR l.GO_LIVE_DATE < '{qs}')
                    THEN 1 ELSE 0 END) as PUSHED_OUT_COUNT,
                SUM(CASE WHEN p.GO_LIVE_DATE BETWEEN '{qs}' AND '{qe}'
                         AND COALESCE(p.IS_TECH_WON, FALSE) = FALSE
                         AND l.USE_CASE_ID IS NOT NULL
                         AND (l.GO_LIVE_DATE > '{qe}' OR l.GO_LIVE_DATE < '{qs}')
                    THEN l.USE_CASE_EACV ELSE 0 END) as PUSHED_OUT_ACV,
                SUM(CASE WHEN (p.USE_CASE_ID IS NULL OR p.GO_LIVE_DATE NOT BETWEEN '{qs}' AND '{qe}')
                         AND l.GO_LIVE_DATE BETWEEN '{qs}' AND '{qe}'
                         AND COALESCE(l.IS_TECH_WON, FALSE) = FALSE
                    THEN 1 ELSE 0 END) as PULLED_IN_COUNT,
                SUM(CASE WHEN (p.USE_CASE_ID IS NULL OR p.GO_LIVE_DATE NOT BETWEEN '{qs}' AND '{qe}')
                         AND l.GO_LIVE_DATE BETWEEN '{qs}' AND '{qe}'
                         AND COALESCE(l.IS_TECH_WON, FALSE) = FALSE
                    THEN l.USE_CASE_EACV ELSE 0 END) as PULLED_IN_ACV,
                SUM(CASE WHEN l.CREATED_DATE >= DATEADD('day', -7, {ref_date})
                         AND l.GO_LIVE_DATE BETWEEN '{qs}' AND '{qe}'
                         AND COALESCE(l.IS_TECH_WON, FALSE) = FALSE
                    THEN 1 ELSE 0 END) as NEW_PIPELINE_COUNT,
                SUM(CASE WHEN l.CREATED_DATE >= DATEADD('day', -7, {ref_date})
                         AND l.GO_LIVE_DATE BETWEEN '{qs}' AND '{qe}'
                         AND COALESCE(l.IS_TECH_WON, FALSE) = FALSE
                    THEN l.USE_CASE_EACV ELSE 0 END) as NEW_PIPELINE_ACV
            FROM latest l
            FULL OUTER JOIN prior p ON l.USE_CASE_ID = p.USE_CASE_ID
        """)
        r = rows[0] if rows else {}
        _e = {"count": 0, "acv": 0}
        return {
            "pushed_out":   {"count": safe_int(r.get("PUSHED_OUT_COUNT") or 0),   "acv": safe_float(r.get("PUSHED_OUT_ACV") or 0)},
            "pulled_in":    {"count": safe_int(r.get("PULLED_IN_COUNT") or 0),     "acv": safe_float(r.get("PULLED_IN_ACV") or 0)},
            "new_pipeline": {"count": safe_int(r.get("NEW_PIPELINE_COUNT") or 0),  "acv": safe_float(r.get("NEW_PIPELINE_ACV") or 0)},
        }
    except Exception:
        _e = {"count": 0, "acv": 0}
        return {"pushed_out": _e, "pulled_in": _e, "new_pipeline": _e}


def q_wins_pacing(day_number, week_number):
    """Prior FY win pacing using TECHNICAL_WIN_DATE, analogous to q_prior_fy_pacing."""
    day_num = safe_int(day_number)
    week_days = safe_int(week_number) * 7
    quarters = CONFIG.get("prior_fy_quarters", [])
    if not quarters:
        return {"day_avg": 0, "day_pct": 0, "week_avg": 0, "week_pct": 0, "prior_fy_final": 0}
    day_unions, week_unions, final_unions = [], [], []
    for qstart, qend in quarters:
        day_unions.append(f"""
            SELECT COALESCE(SUM(USE_CASE_EACV), 0) as WINS_ACV
            FROM SNOWPUBLIC.STREAMLIT.DIM_USE_CASE_MDM_CACHE
            WHERE {_gvp_filter()}
              AND USE_CASE_EACV > 0 AND IS_TECH_WON = TRUE
              AND TECHNICAL_WIN_DATE BETWEEN '{qstart}' AND DATEADD('day', {day_num}-1, '{qstart}')
        """)
        week_unions.append(f"""
            SELECT COALESCE(SUM(USE_CASE_EACV), 0) as WINS_ACV
            FROM SNOWPUBLIC.STREAMLIT.DIM_USE_CASE_MDM_CACHE
            WHERE {_gvp_filter()}
              AND USE_CASE_EACV > 0 AND IS_TECH_WON = TRUE
              AND TECHNICAL_WIN_DATE BETWEEN '{qstart}' AND DATEADD('day', {week_days}-1, '{qstart}')
        """)
        final_unions.append(f"""
            SELECT COALESCE(SUM(USE_CASE_EACV), 0) as WINS_ACV
            FROM SNOWPUBLIC.STREAMLIT.DIM_USE_CASE_MDM_CACHE
            WHERE {_gvp_filter()}
              AND USE_CASE_EACV > 0 AND IS_TECH_WON = TRUE
              AND TECHNICAL_WIN_DATE BETWEEN '{qstart}' AND '{qend}'
        """)
    day_avg = float(run_query(f"SELECT AVG(WINS_ACV) as AVG FROM ({' UNION ALL '.join(day_unions)})")[0]["AVG"] or 0)
    week_avg = float(run_query(f"SELECT AVG(WINS_ACV) as AVG FROM ({' UNION ALL '.join(week_unions)})")[0]["AVG"] or 0)
    fy_final = float(run_query(f"SELECT AVG(WINS_ACV) as AVG FROM ({' UNION ALL '.join(final_unions)})")[0]["AVG"] or 0)
    return {
        "day_avg": day_avg,
        "day_pct": (day_avg / fy_final * 100) if fy_final else 0,
        "week_avg": week_avg,
        "week_pct": (week_avg / fy_final * 100) if fy_final else 0,
        "prior_fy_final": fy_final,
    }


def q_forecast_calls():
    fq_key = CONFIG["fiscal_quarter_key"]
    rows = run_query(f"""
        SELECT FORECAST_TYPE, FORECAST_AMOUNT, LATEST_DATE, PREVIOUS_WEEK
        FROM SALES.REPORTING.PEAK_FORECAST_CALLS_PIPELINE_TARGETS
        WHERE USER_NAME = '{CONFIG["gvp_name"]}'
          AND FUNCTION = '{CONFIG["gvp_function"]}'
          AND TYPE = 'Use Case Go-Lives'
          AND FORECAST_TYPE IN ('CommitForecast', 'MostLikelyForecast', 'BestCaseForecast')
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
        "commit_delta": commit - prior["CommitForecast"] if "CommitForecast" in prior else None,
        "ml_delta": ml - prior["MostLikelyForecast"] if "MostLikelyForecast" in prior else None,
        "stretch_delta": stretch - prior["BestCaseForecast"] if "BestCaseForecast" in prior else None,
    }


def q_deployed_qtd():
    rows = run_query(f"""
        SELECT SUM(USE_CASE_EACV) as DEPLOYED_ACV, COUNT(*) as DEPLOYED_COUNT
        FROM SNOWPUBLIC.STREAMLIT.DIM_USE_CASE_MDM_CACHE
        WHERE {_gvp_filter()}
          AND USE_CASE_EACV > 0
          AND IS_DEPLOYED = TRUE
          AND GO_LIVE_DATE BETWEEN '{CONFIG["quarter_start"]}' AND '{CONFIG["quarter_end"]}'
    """)
    r = rows[0]
    return {"acv": safe_float(r["DEPLOYED_ACV"] or 0), "count": safe_int(r["DEPLOYED_COUNT"] or 0)}


def q_last7_deployed():
    ref_date = CONFIG["reference_date"]
    rows = run_query(f"""
        SELECT SUM(USE_CASE_EACV) as LAST7_ACV, COUNT(*) as LAST7_COUNT
        FROM SNOWPUBLIC.STREAMLIT.DIM_USE_CASE_MDM_CACHE
        WHERE {_gvp_filter()}
          AND USE_CASE_EACV > 0
          AND IS_DEPLOYED = TRUE
          AND GO_LIVE_DATE >= DATEADD('day', -7, {ref_date})
          AND GO_LIVE_DATE <= {ref_date}
    """)
    r = rows[0]
    return {"acv": safe_float(r["LAST7_ACV"] or 0), "count": safe_int(r["LAST7_COUNT"] or 0)}


def q_deployment_velocity():
    if not CONFIG.get("is_current_quarter"):
        return {"current": {"v7": 0, "v14": 0, "v30": 0}, "historical": []}
    gvp = CONFIG["gvp_name"]
    rows = run_query(f"""
        SELECT PERIOD, V7, V14, V30
        FROM SNOWPUBLIC.STREAMLIT.VELOCITY_CACHE
        WHERE THEATER_NAME = '{_theater()}' AND METRIC_TYPE = 'deployment'
        ORDER BY PERIOD
    """)
    current = {"v7": 0, "v14": 0, "v30": 0}
    hist_velocities = []
    period_labels = {"hist_q1": f"{CONFIG['prior_fy_label']} Q1",
                     "hist_q2": f"{CONFIG['prior_fy_label']} Q2",
                     "hist_q3": f"{CONFIG['prior_fy_label']} Q3",
                     "hist_q4": f"{CONFIG['prior_fy_label']} Q4"}
    for r in rows:
        period = r.get("PERIOD", "")
        v7 = float(r.get("V7", 0) or 0)
        v14 = float(r.get("V14", 0) or 0)
        v30 = float(r.get("V30", 0) or 0)
        if period == "current":
            current = {"v7": v7, "v14": v14, "v30": v30}
        elif period in period_labels and (v7 > 0 or v30 > 0):
            hist_velocities.append({"QTR": period_labels[period], "V7": v7, "V14": v14, "V30": v30})
    return {"current": current, "historical": hist_velocities}


def q_risk_adjusted_pipeline_detail():
    excluded = ", ".join(f"'{s}'" for s in CONFIG["dim_excluded_stages"])
    ref_date = CONFIG["reference_date"]
    return run_query(f"""
        WITH fiscal_qtr AS (
            SELECT '{CONFIG["quarter_start"]}'::DATE AS FQ_START,
                   '{CONFIG["quarter_end"]}'::DATE AS FQ_END,
                   DATEDIFF('day', {ref_date}, '{CONFIG["quarter_end"]}'::DATE) + 1 AS DAYS_REMAINING
        )
        SELECT u.RESOLVED_USE_CASE_ID as USE_CASE_ID, u.USE_CASE_NAME, u.ACCOUNT_NAME, u.USE_CASE_EACV,
               u.STAGE_NUMBER, u.USE_CASE_STAGE, u.DAYS_IN_STAGE, u.GO_LIVE_DATE,
               u.TECHNICAL_WIN_DATE, f.DAYS_REMAINING,
               CASE
                   WHEN u.STAGE_NUMBER = 6 THEN 'Good'
                   WHEN u.STAGE_NUMBER = 5 AND (u.DAYS_IN_STAGE + f.DAYS_REMAINING) >= {CONFIG["risk_thresholds"]["stage_5"]} THEN 'Good'
                   WHEN u.STAGE_NUMBER = 4 AND (u.DAYS_IN_STAGE + f.DAYS_REMAINING) >= {CONFIG["risk_thresholds"]["stage_4"]} THEN 'Good'
                   WHEN u.STAGE_NUMBER IN (1,2,3) AND (u.DAYS_IN_STAGE + f.DAYS_REMAINING) >= {CONFIG["risk_thresholds"]["stage_123"]} THEN 'Good'
                   ELSE 'At Risk'
               END AS RISK_STATUS
        FROM SNOWPUBLIC.STREAMLIT.DIM_USE_CASE_MDM_CACHE u
        CROSS JOIN fiscal_qtr f
        WHERE {_gvp_filter('u')}
            AND u.USE_CASE_EACV > 0 AND u.IS_DEPLOYED = FALSE AND u.IS_LOST = FALSE
            AND u.STAGE_NUMBER BETWEEN 1 AND 6
            AND u.USE_CASE_STAGE NOT IN ({excluded})
            AND u.GO_LIVE_DATE BETWEEN f.FQ_START AND f.FQ_END
        ORDER BY u.USE_CASE_EACV DESC LIMIT 15
    """)


def q_open_pipeline():
    excluded = ", ".join(f"'{s}'" for s in CONFIG["dim_excluded_stages"])
    rows = run_query(f"""
        SELECT SUM(USE_CASE_EACV) as OPEN_PIPELINE, COUNT(*) as OPEN_COUNT
        FROM SNOWPUBLIC.STREAMLIT.DIM_USE_CASE_MDM_CACHE
        WHERE {_gvp_filter()}
          AND USE_CASE_EACV > 0 AND IS_DEPLOYED = FALSE AND IS_LOST = FALSE
          AND GO_LIVE_DATE BETWEEN '{CONFIG["quarter_start"]}' AND '{CONFIG["quarter_end"]}'
          AND USE_CASE_STAGE NOT IN ({excluded})
    """)
    r = rows[0]
    return {"acv": safe_float(r["OPEN_PIPELINE"] or 0), "count": safe_int(r["OPEN_COUNT"] or 0)}


def q_pipeline_risk():
    excluded = ", ".join(f"'{s}'" for s in CONFIG["dim_excluded_stages"])
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
            SUM(u.USE_CASE_EACV) AS TOTAL_ACV,
            SUM(CASE
                WHEN u.STAGE_NUMBER IN (1,2,3) AND (u.DAYS_IN_STAGE + f.DAYS_REMAINING) < {CONFIG["risk_thresholds"]["stage_123"]} THEN 1
                WHEN u.STAGE_NUMBER = 4 AND (u.DAYS_IN_STAGE + f.DAYS_REMAINING) < {CONFIG["risk_thresholds"]["stage_4"]} THEN 1
                WHEN u.STAGE_NUMBER = 5 AND (u.DAYS_IN_STAGE + f.DAYS_REMAINING) < {CONFIG["risk_thresholds"]["stage_5"]} THEN 1
                ELSE 0
            END) AS AT_RISK_COUNT,
            SUM(CASE
                WHEN u.STAGE_NUMBER IN (1,2,3) AND (u.DAYS_IN_STAGE + f.DAYS_REMAINING) < {CONFIG["risk_thresholds"]["stage_123"]} THEN u.USE_CASE_EACV
                WHEN u.STAGE_NUMBER = 4 AND (u.DAYS_IN_STAGE + f.DAYS_REMAINING) < {CONFIG["risk_thresholds"]["stage_4"]} THEN u.USE_CASE_EACV
                WHEN u.STAGE_NUMBER = 5 AND (u.DAYS_IN_STAGE + f.DAYS_REMAINING) < {CONFIG["risk_thresholds"]["stage_5"]} THEN u.USE_CASE_EACV
                ELSE 0
            END) AS AT_RISK_ACV,
            SUM(CASE
                WHEN u.STAGE_NUMBER IN (1,2,3) AND (u.DAYS_IN_STAGE + f.DAYS_REMAINING) >= {CONFIG["risk_thresholds"]["stage_123"]} THEN u.USE_CASE_EACV
                WHEN u.STAGE_NUMBER = 4 AND (u.DAYS_IN_STAGE + f.DAYS_REMAINING) >= {CONFIG["risk_thresholds"]["stage_4"]} THEN u.USE_CASE_EACV
                WHEN u.STAGE_NUMBER = 5 AND (u.DAYS_IN_STAGE + f.DAYS_REMAINING) >= {CONFIG["risk_thresholds"]["stage_5"]} THEN u.USE_CASE_EACV
                WHEN u.STAGE_NUMBER = 6 THEN u.USE_CASE_EACV
                ELSE 0
            END) AS GOOD_ACV
        FROM SNOWPUBLIC.STREAMLIT.DIM_USE_CASE_MDM_CACHE u
        CROSS JOIN fiscal_qtr f
        WHERE {_gvp_filter('u')}
            AND u.USE_CASE_EACV > 0 AND u.IS_DEPLOYED = FALSE AND u.IS_LOST = FALSE
            AND u.STAGE_NUMBER BETWEEN 1 AND 6
            AND u.USE_CASE_STAGE NOT IN ({excluded})
            AND u.GO_LIVE_DATE BETWEEN f.FQ_START AND f.FQ_END
        GROUP BY STAGE_GROUP ORDER BY STAGE_GROUP
    """)
    return {r["STAGE_GROUP"]: r for r in rows}


def _use_case_select_cols():
    return """
        u.RESOLVED_USE_CASE_ID as USE_CASE_ID,
        u.ACCOUNT_ID,
        u.USE_CASE_NUMBER,
        u.ACCOUNT_NAME,
        u.USE_CASE_NAME,
        u.USE_CASE_EACV,
        u.GO_LIVE_FORECAST_STATUS as FORECAST_CATEGORY,
        u.GO_LIVE_DATE,
        u.USE_CASE_STAGE,
        u.SE_COMMENTS,
        u.NEXT_STEPS,
        u.USE_CASE_RISK,
        u.ACCOUNT_OWNER_NAME as ACCOUNT_EXECUTIVE_NAME,
        u.ACCOUNT_LEAD_SE_NAME as USE_CASE_LEAD_SE_NAME,
        u.SUB_REGION_NAME as REGION_NAME,
        u.USE_CASE_DESCRIPTION,
        u.IMPLEMENTER,
        u.PARTNER_NAME
    """


def q_top5_use_cases():
    excluded = ", ".join(f"'{s}'" for s in CONFIG["dim_excluded_stages"])
    return run_query(f"""
        SELECT {_use_case_select_cols()}
        FROM SNOWPUBLIC.STREAMLIT.DIM_USE_CASE_MDM_CACHE u
        WHERE {_gvp_filter('u')}
          AND u.USE_CASE_EACV > 0 AND u.IS_DEPLOYED = FALSE AND u.IS_LOST = FALSE
          AND u.GO_LIVE_DATE BETWEEN '{CONFIG["quarter_start"]}' AND '{CONFIG["quarter_end"]}'
          AND u.USE_CASE_STAGE NOT IN ({excluded})
        ORDER BY u.USE_CASE_EACV DESC LIMIT {CONFIG["top_n"]}
    """)


def q_sales_play_summary():
    excluded = ", ".join(f"'{s}'" for s in CONFIG["dim_excluded_stages"])
    bronze_camp = CONFIG['bronze_campaign']
    sql_camp = CONFIG['sqlserver_campaign']
    si_tuc = CONFIG['si_technical_use_case']
    # Open pipeline — 1 query for all 3 plays
    open_rows = run_query(f"""
        SELECT
            COALESCE(SUM(CASE WHEN u.PRIORITIZED_FEATURES ILIKE '{bronze_camp}' THEN u.USE_CASE_EACV END), 0) as BRONZE_ACV,
            COUNT(CASE WHEN u.PRIORITIZED_FEATURES ILIKE '{bronze_camp}' THEN 1 END) as BRONZE_COUNT,
            COALESCE(SUM(CASE WHEN u.TECHNICAL_USE_CASE ILIKE '{si_tuc}' THEN u.USE_CASE_EACV END), 0) as SI_ACV,
            COUNT(CASE WHEN u.TECHNICAL_USE_CASE ILIKE '{si_tuc}' THEN 1 END) as SI_COUNT,
            COALESCE(SUM(CASE WHEN u.PRIORITIZED_FEATURES ILIKE '{sql_camp}' THEN u.USE_CASE_EACV END), 0) as SQL_ACV,
            COUNT(CASE WHEN u.PRIORITIZED_FEATURES ILIKE '{sql_camp}' THEN 1 END) as SQL_COUNT
        FROM SNOWPUBLIC.STREAMLIT.DIM_USE_CASE_MDM_CACHE u
        WHERE {_gvp_filter('u')}
          AND u.USE_CASE_EACV > 0 AND u.IS_DEPLOYED = FALSE AND u.IS_LOST = FALSE
          AND u.GO_LIVE_DATE BETWEEN '{CONFIG["quarter_start"]}' AND '{CONFIG["quarter_end"]}'
          AND u.USE_CASE_STAGE NOT IN ({excluded})
    """)
    # Deployed — 1 query for all 3 plays
    dep_rows = run_query(f"""
        SELECT
            COALESCE(SUM(CASE WHEN u.PRIORITIZED_FEATURES ILIKE '{bronze_camp}' THEN u.USE_CASE_EACV END), 0) as BRONZE_ACV,
            COUNT(CASE WHEN u.PRIORITIZED_FEATURES ILIKE '{bronze_camp}' THEN 1 END) as BRONZE_COUNT,
            COALESCE(SUM(CASE WHEN u.TECHNICAL_USE_CASE ILIKE '{si_tuc}' THEN u.USE_CASE_EACV END), 0) as SI_ACV,
            COUNT(CASE WHEN u.TECHNICAL_USE_CASE ILIKE '{si_tuc}' THEN 1 END) as SI_COUNT,
            COALESCE(SUM(CASE WHEN u.PRIORITIZED_FEATURES ILIKE '{sql_camp}' THEN u.USE_CASE_EACV END), 0) as SQL_ACV,
            COUNT(CASE WHEN u.PRIORITIZED_FEATURES ILIKE '{sql_camp}' THEN 1 END) as SQL_COUNT
        FROM SNOWPUBLIC.STREAMLIT.DIM_USE_CASE_MDM_CACHE u
        WHERE {_gvp_filter('u')}
          AND u.USE_CASE_EACV > 0 AND u.IS_DEPLOYED = TRUE
          AND u.GO_LIVE_DATE BETWEEN '{CONFIG["quarter_start"]}' AND '{CONFIG["quarter_end"]}'
    """)
    o = open_rows[0]
    dp = dep_rows[0]
    return {
        "bronze_open": {"acv": safe_float(o["BRONZE_ACV"]), "count": safe_int(o["BRONZE_COUNT"])},
        "bronze_deployed": {"acv": safe_float(dp["BRONZE_ACV"]), "count": safe_int(dp["BRONZE_COUNT"])},
        "si_open": {"acv": safe_float(o["SI_ACV"]), "count": safe_int(o["SI_COUNT"])},
        "si_deployed": {"acv": safe_float(dp["SI_ACV"]), "count": safe_int(dp["SI_COUNT"])},
        "sqlserver_open": {"acv": safe_float(o["SQL_ACV"]), "count": safe_int(o["SQL_COUNT"])},
        "sqlserver_deployed": {"acv": safe_float(dp["SQL_ACV"]), "count": safe_int(dp["SQL_COUNT"])},
    }


def q_play_detail_metrics():
    excluded = ", ".join(f"'{s}'" for s in CONFIG["dim_excluded_stages"])
    bronze_camp = CONFIG['bronze_campaign']
    sql_camp = CONFIG['sqlserver_campaign']
    si_tuc = CONFIG['si_technical_use_case']
    # Single query: fetch per-UC rows with play labels, compute median in Python
    rows = run_query(f"""
        SELECT u.USE_CASE_EACV,
               u.SUB_REGION_NAME as SALES_AREA,
               CASE WHEN u.PRIORITIZED_FEATURES ILIKE '{bronze_camp}' THEN 1 ELSE 0 END AS IS_BRONZE,
               CASE WHEN u.TECHNICAL_USE_CASE ILIKE '{si_tuc}' THEN 1 ELSE 0 END AS IS_SI,
               CASE WHEN u.PRIORITIZED_FEATURES ILIKE '{sql_camp}' THEN 1 ELSE 0 END AS IS_SQL
        FROM SNOWPUBLIC.STREAMLIT.DIM_USE_CASE_MDM_CACHE u
        WHERE {_gvp_filter('u')}
          AND u.USE_CASE_EACV > 0 AND u.IS_DEPLOYED = FALSE AND u.IS_LOST = FALSE
          AND u.GO_LIVE_DATE BETWEEN '{CONFIG["quarter_start"]}' AND '{CONFIG["quarter_end"]}'
          AND u.USE_CASE_STAGE NOT IN ({excluded})
    """)
    def _calc(flag_col):
        filtered = [r for r in rows if r[flag_col] == 1]
        if not filtered:
            return {"count": 0, "avg_acv": 0, "median_acv": 0, "regions": 0}
        acvs = [safe_float(r["USE_CASE_EACV"]) for r in filtered]
        regions = len(set(safe_str(r["SALES_AREA"]) for r in filtered if r.get("SALES_AREA")))
        return {
            "count": len(acvs),
            "avg_acv": sum(acvs) / len(acvs),
            "median_acv": statistics.median(acvs),
            "regions": regions,
        }
    return {
        "bronze": _calc("IS_BRONZE"),
        "si": _calc("IS_SI"),
        "sqlserver": _calc("IS_SQL"),
    }


def q_bronze_tb_total():
    rows = run_query(f"""
        SELECT SUM(TB_INGESTED) as BRONZE_TB
        FROM SALES.REPORTING.SALES_PROGRAMS_BRONZE_INGEST
        WHERE GEO_NAME = '{_theater()}'
          AND IS_BRONZE = TRUE
          AND MONTH BETWEEN '{CONFIG["quarter_start"]}' AND '{CONFIG["quarter_end"]}'
    """)
    return float(rows[0]["BRONZE_TB"] or 0)


def q_tb_ingested_target():
    """TB Ingested target from SUCCESS_GOALS. Returns None if not accessible."""
    try:
        rows = run_query(f"""
            SELECT GOAL
            FROM SALES.SALES_BI.SALES_PROGRAMS_SUCCESS_GOALS
            WHERE GOAL_TYPE = 'TB Ingested'
              AND TAG_VALUE = 'Make Your Data AI Ready'
              AND THEATER = '{_theater()}'
              AND FISCAL_QUARTER = '{CONFIG["fiscal_quarter"]}'
        """, _retries=0)
        if rows and rows[0]["GOAL"] is not None:
            return float(rows[0]["GOAL"])
    except Exception:
        pass
    return None


def _play_use_cases_query(play_name, filter_clause):
    excluded = ", ".join(f"'{s}'" for s in CONFIG["dim_excluded_stages"])
    return run_query(f"""
        SELECT {_use_case_select_cols()}
        FROM SNOWPUBLIC.STREAMLIT.DIM_USE_CASE_MDM_CACHE u
        WHERE {_gvp_filter('u')}
          AND u.USE_CASE_EACV > 0
          AND u.IS_DEPLOYED = FALSE AND u.IS_LOST = FALSE
          AND u.GO_LIVE_DATE BETWEEN '{CONFIG["quarter_start"]}' AND '{CONFIG["quarter_end"]}'
          AND u.USE_CASE_STAGE NOT IN ({excluded}) AND {filter_clause}
        ORDER BY u.USE_CASE_EACV DESC
    """)


def q_play_use_cases():
    return {
        "bronze": _play_use_cases_query("Bronze", f"u.PRIORITIZED_FEATURES ILIKE '{CONFIG['bronze_campaign']}'"),
        "si": _play_use_cases_query("SI", f"u.TECHNICAL_USE_CASE ILIKE '{CONFIG['si_technical_use_case']}'"),
        "sqlserver": _play_use_cases_query("SQL Server", f"u.PRIORITIZED_FEATURES ILIKE '{CONFIG['sqlserver_campaign']}'"),
    }


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
            SELECT COALESCE(SUM(u.USE_CASE_EACV), 0) as DEPLOYED_ACV
            FROM SNOWPUBLIC.STREAMLIT.DIM_USE_CASE_MDM_CACHE u
            WHERE {_gvp_filter('u')}
              AND u.USE_CASE_EACV > 0 AND u.IS_DEPLOYED = TRUE
              AND u.GO_LIVE_DATE BETWEEN '{qstart}' AND DATEADD('day', {day_num}-1, '{qstart}')
        """)
        week_unions.append(f"""
            SELECT COALESCE(SUM(u.USE_CASE_EACV), 0) as DEPLOYED_ACV
            FROM SNOWPUBLIC.STREAMLIT.DIM_USE_CASE_MDM_CACHE u
            WHERE {_gvp_filter('u')}
              AND u.USE_CASE_EACV > 0 AND u.IS_DEPLOYED = TRUE
              AND u.GO_LIVE_DATE BETWEEN '{qstart}' AND DATEADD('day', {week_days}-1, '{qstart}')
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


def q_consumption(account_ids):
    if not account_ids:
        return {}
    id_list = ", ".join(f"'{aid}'" for aid in account_ids if aid)
    if not id_list:
        return {}
    rows = run_query(f"""
        SELECT ACCOUNT_ID, ACCOUNT_NAME,
               ROUND(REVENUE_TRAILING_90D, 0) as REV_90D,
               ROUND(GROWTH_RATE_90D * 100, 0) as GROWTH_90D_PCT,
               ROUND(REVENUE_LTM, 0) as RUN_RATE,
               ROUND(GROWTH_RATE_180D * 100, 0) as RUN_RATE_GROWTH_PCT
        FROM SALES.REPORTING.BOB_CONSUMPTION WHERE ACCOUNT_ID IN ({id_list})
    """)
    return {r["ACCOUNT_ID"]: r for r in rows}


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


def q_si_usage(account_ids):
    if not account_ids:
        return {}
    id_list = ", ".join(f"'{aid}'" for aid in account_ids if aid)
    if not id_list:
        return {}
    rows = run_query(f"""
        SELECT SALESFORCE_ACCOUNT_ID as ACCOUNT_ID, SALESFORCE_ACCOUNT_NAME as ACCOUNT_NAME,
               ROUND(CREDITS_LAST_30_DAYS, 0) as SI_CREDITS,
               ROUND(REVENUE_LAST_30_DAYS, 0) as SI_REVENUE,
               ACTIVE_USERS_LAST_30_DAYS as SI_USERS
        FROM SALES.REPORTING.BOB_SNOWFLAKE_INTELLIGENCE_USAGE_STREAMLIT_AGG
        WHERE SALESFORCE_ACCOUNT_ID IN ({id_list})
    """)
    return {r["ACCOUNT_ID"]: r for r in rows}


def _create_cc_temp_table():
    """No longer needed - using pre-populated CC_USAGE_CACHE table."""
    return True, -1, None


COCO_TIER_ORDER = ["Zero Usage", "Exploring", "Activated", "Expanded", "Deep"]

# Verbatim from the 2x2 dashboard so tier semantics stay identical across apps.
COCO_TIER_DEFINITIONS = (
    "Tiers are based on the last 28 days of activity. "
    "Zero Usage = no CoCo activity of any kind. "
    "Exploring = some usage, but no habitually engaged UI users (3+ active days & 10+ prompts). "
    "Activated = 1+ habitually engaged UI user, below Expanded/Deep CLI thresholds. "
    "Expanded = 10%+ of unblocked Snowflake users engaged on UI, plus 2+ engaged CLI/Desktop users. "
    "Deep = 20%+ engaged UI share, plus 5+ engaged CLI/Desktop users at 10%+ of unblocked users."
)


def q_coco_theater_tiers():
    """
    Theater-level CoCo adoption tier counts + Set Sail, ported from the 2x2 dashboard.

    Scope is GEO_NAME = theater (5,042 accounts for AMSExpansion), NOT the PEAK
    _gvp_filter() Raven lookup — these differ by ~600 accounts and the theater
    definition was chosen deliberately. COCO_ACCOUNT_COCO_USAGE already carries
    the full hierarchy, so no join to DIM_ACCOUNTS_SLIM_CACHE is needed.

    Uses IS_YESTERDAY = TRUE rather than MAX(DS): a correlated MAX(DS) subquery
    against this 4.6M-row table times out at 180s.

    Zero Usage is taken from the EXPLICIT 'Zero Usage' tier rows. The 2x2's
    sparkline instead derives it by subtraction, so our Zero Usage count will
    not tie exactly to the 2x2's — that is expected, not a defect.
    """
    theater = _theater()
    try:
        rows = run_query(f"""
            SELECT c.DS AS AS_OF,
                   COUNT(*) AS CAPACITY_ACCOUNTS,
                   COUNT(CASE WHEN COALESCE(c.ACCOUNT_TIER,'Zero Usage')='Zero Usage' THEN 1 END) AS ZERO_USAGE,
                   COUNT(CASE WHEN c.ACCOUNT_TIER='Exploring' THEN 1 END) AS EXPLORING,
                   COUNT(CASE WHEN c.ACCOUNT_TIER='Activated' THEN 1 END) AS ACTIVATED,
                   COUNT(CASE WHEN c.ACCOUNT_TIER='Expanded'  THEN 1 END) AS EXPANDED,
                   COUNT(CASE WHEN c.ACCOUNT_TIER='Deep'      THEN 1 END) AS DEEP
            FROM SALES.REPORTING.COCO_ACCOUNT_COCO_USAGE c
            WHERE c.IS_YESTERDAY = TRUE AND c.GEO_NAME = '{theater}'
            GROUP BY c.DS
        """)
        if not rows:
            return {}
        r = rows[0]
        out = {
            "as_of": safe_str(r.get("AS_OF", "")),
            "capacity": safe_int(r.get("CAPACITY_ACCOUNTS", 0)),
            "Zero Usage": safe_int(r.get("ZERO_USAGE", 0)),
            "Exploring": safe_int(r.get("EXPLORING", 0)),
            "Activated": safe_int(r.get("ACTIVATED", 0)),
            "Expanded": safe_int(r.get("EXPANDED", 0)),
            "Deep": safe_int(r.get("DEEP", 0)),
        }
        ss = run_query(f"""
            WITH cur AS (
              SELECT SALESFORCE_ACCOUNT_ID FROM SALES.REPORTING.COCO_ACCOUNT_COCO_USAGE
              WHERE IS_YESTERDAY = TRUE AND GEO_NAME = '{theater}'
            )
            SELECT COUNT(DISTINCT CASE WHEN a.ACTIVITY_DATE >= DATEADD(day,-28,CURRENT_DATE)
                                       THEN a.ACCOUNT_ID END) AS SETSAIL_L28,
                   COUNT(DISTINCT CASE WHEN a.ACTIVITY_DATE >= DATEADD(day,-56,CURRENT_DATE)
                                        AND a.ACTIVITY_DATE <  DATEADD(day,-28,CURRENT_DATE)
                                       THEN a.ACCOUNT_ID END) AS SETSAIL_PRIOR
            FROM SALES.REPORTING.INT_COCO_SETSAIL_ACTIVITY a
            JOIN cur ON cur.SALESFORCE_ACCOUNT_ID = a.ACCOUNT_ID
            WHERE a.ACTIVITY_TYPE = 'MEETING'
              AND (a.IS_RECURRING = TRUE OR a.IS_TECHNICAL_UPSKILL_EVENT = TRUE)
              AND a.ACTIVITY_DATE >= DATEADD(day,-56,CURRENT_DATE)
        """)
        s = ss[0] if ss else {}
        out["setsail_l28"] = safe_int(s.get("SETSAIL_L28", 0))
        out["setsail_prior"] = safe_int(s.get("SETSAIL_PRIOR", 0))
        return out
    except Exception as e:
        return {"error": str(e)}


def q_coco_tier_movement():
    """
    Accounts that moved INTO and OUT OF each CoCo tier over the last 7 days.

    Not present in the 2x2 — its trend query aggregates with COUNT(DISTINCT)
    per day and so cannot say WHICH accounts moved. This compares the latest
    snapshot against DS-7 at account grain.

    FULL OUTER JOIN is deliberate: the AMSExpansion universe happens to be
    stable week-over-week today (no '(new)'/'(gone)' rows), but an inner join
    would silently drop genuine entries/exits if that changes.
    """
    theater = _theater()
    try:
        rows = run_query(f"""
            WITH d AS (
              SELECT MAX(DS) AS MAXDS FROM SALES.REPORTING.COCO_ACCOUNT_COCO_USAGE
              WHERE GEO_NAME = '{theater}'
            ),
            cur AS (SELECT c.SALESFORCE_ACCOUNT_ID, c.ACCOUNT_TIER
                    FROM SALES.REPORTING.COCO_ACCOUNT_COCO_USAGE c JOIN d ON c.DS = d.MAXDS
                    WHERE c.GEO_NAME = '{theater}'),
            pri AS (SELECT c.SALESFORCE_ACCOUNT_ID, c.ACCOUNT_TIER
                    FROM SALES.REPORTING.COCO_ACCOUNT_COCO_USAGE c
                    JOIN d ON c.DS = DATEADD(day,-7,d.MAXDS)
                    WHERE c.GEO_NAME = '{theater}'),
            j AS (SELECT COALESCE(p.ACCOUNT_TIER,'(new)')  AS PREV_TIER,
                         COALESCE(c.ACCOUNT_TIER,'(gone)') AS CUR_TIER
                  FROM cur c FULL OUTER JOIN pri p
                    ON c.SALESFORCE_ACCOUNT_ID = p.SALESFORCE_ACCOUNT_ID),
            t AS (SELECT 'Zero Usage' AS TIER, 1 AS ORD UNION ALL SELECT 'Exploring',2
                  UNION ALL SELECT 'Activated',3 UNION ALL SELECT 'Expanded',4
                  UNION ALL SELECT 'Deep',5)
            SELECT t.TIER,
                   (SELECT COUNT(*) FROM j WHERE j.CUR_TIER = t.TIER AND j.PREV_TIER <> t.TIER) AS MOVED_IN,
                   (SELECT COUNT(*) FROM j WHERE j.PREV_TIER = t.TIER AND j.CUR_TIER <> t.TIER) AS MOVED_OUT
            FROM t ORDER BY t.ORD
        """)
        return {safe_str(r.get("TIER", "")): {
                    "in": safe_int(r.get("MOVED_IN", 0)),
                    "out": safe_int(r.get("MOVED_OUT", 0)),
                    "net": safe_int(r.get("MOVED_IN", 0)) - safe_int(r.get("MOVED_OUT", 0)),
                } for r in rows}
    except Exception:
        return {}


def _coco_skill_match_cte(alias="u"):
    """
    Shared SQL for the 2x2's CoCo skill match, ported verbatim in semantics.

    IMPORTANT — this is NOT semantic matching of a use case to a skill. It is a
    string-equality join of the use case's WORKLOADS tokens against
    CORTEX_CODE_SKILL_ACCT_CACHE.WORKLOAD_CATEGORY for the SAME ACCOUNT. It
    answers "has this ACCOUNT used CoCo skills in this use case's product
    categories", so two use cases at one account with equal WORKLOADS always
    score identically. Tier thresholds match the 2x2 exactly:
    n==0 -> None; n>=3 or sessions>=30 -> High; n>=2 or sessions>=10 -> Medium;
    else Low. Empty string when WORKLOADS is null/blank.

    WORKLOADS tokens must equal WORKLOAD_CATEGORY exactly, including the
    ampersand in 'Applications & Collaboration' — any normalisation drift
    silently yields 'None' rather than an error.
    """
    return f"""
        wl AS (
          SELECT {alias}.USE_CASE_ID, {alias}.ACCOUNT_ID, {alias}.WORKLOADS,
                 TRIM(s.VALUE) AS WORKLOAD_CATEGORY
          FROM uc {alias}, LATERAL SPLIT_TO_TABLE(COALESCE({alias}.WORKLOADS,''), ';') s
        ),
        m AS (
          SELECT wl.USE_CASE_ID, wl.WORKLOADS,
                 COUNT(DISTINCT c.SKILL_NAME) AS N_SKILLS,
                 COALESCE(SUM(c.SESSIONS_90D),0) AS TOT_SESS
          FROM wl
          LEFT JOIN SNOWPUBLIC.STREAMLIT.CORTEX_CODE_SKILL_ACCT_CACHE c
            ON c.SALESFORCE_ACCOUNT_ID = wl.ACCOUNT_ID
           AND c.WORKLOAD_CATEGORY = wl.WORKLOAD_CATEGORY
          GROUP BY 1, 2
        )"""


def q_coco_skill_match_by_uc():
    """Map USE_CASE_ID -> CoCo skill confidence tier for this quarter's go-lives."""
    excluded = ", ".join(f"'{s}'" for s in CONFIG["dim_excluded_stages"])
    try:
        rows = run_query(f"""
            WITH uc AS (
              SELECT u.RESOLVED_USE_CASE_ID AS USE_CASE_ID, u.ACCOUNT_ID, u.WORKLOADS
              FROM SNOWPUBLIC.STREAMLIT.DIM_USE_CASE_MDM_CACHE u
              WHERE {_gvp_filter('u')}
                AND u.USE_CASE_EACV > 0 AND u.IS_DEPLOYED = FALSE AND u.IS_LOST = FALSE
                AND u.GO_LIVE_DATE BETWEEN '{CONFIG["quarter_start"]}' AND '{CONFIG["quarter_end"]}'
                AND u.USE_CASE_STAGE NOT IN ({excluded})
            ),
            {_coco_skill_match_cte('u')}
            SELECT m.USE_CASE_ID, m.N_SKILLS, m.TOT_SESS,
                   CASE WHEN m.WORKLOADS IS NULL OR m.WORKLOADS = '' THEN ''
                        WHEN m.N_SKILLS = 0 THEN 'None'
                        WHEN m.N_SKILLS >= 3 OR m.TOT_SESS >= 30 THEN 'High'
                        WHEN m.N_SKILLS >= 2 OR m.TOT_SESS >= 10 THEN 'Medium'
                        ELSE 'Low' END AS CONFIDENCE
            FROM m
        """)
        return {safe_str(r.get("USE_CASE_ID", "")): {
                    "confidence": safe_str(r.get("CONFIDENCE", "")),
                    "n_skills": safe_int(r.get("N_SKILLS", 0)),
                    "sessions": safe_int(r.get("TOT_SESS", 0)),
                } for r in rows}
    except Exception:
        return {}


def q_coco_golive_insights():
    """
    Top-level CoCo insights for accounts with go-lives in the filtered quarter,
    plus the share of those use cases carrying a CoCo skill match.

    Account scope here is the PEAK _gvp_filter() go-live population (NOT the
    theater GEO_NAME scope used by the tier cards) because the question is
    specifically about accounts that have go-lives this quarter.
    """
    excluded = ", ".join(f"'{s}'" for s in CONFIG["dim_excluded_stages"])
    try:
        rows = run_query(f"""
            WITH uc AS (
              SELECT DISTINCT u.RESOLVED_USE_CASE_ID AS USE_CASE_ID, u.ACCOUNT_ID, u.WORKLOADS
              FROM SNOWPUBLIC.STREAMLIT.DIM_USE_CASE_MDM_CACHE u
              WHERE {_gvp_filter('u')}
                AND u.USE_CASE_EACV > 0 AND u.IS_DEPLOYED = FALSE AND u.IS_LOST = FALSE
                AND u.GO_LIVE_DATE BETWEEN '{CONFIG["quarter_start"]}' AND '{CONFIG["quarter_end"]}'
                AND u.USE_CASE_STAGE NOT IN ({excluded})
            ),
            accts AS (SELECT DISTINCT ACCOUNT_ID FROM uc),
            tiers AS (
              SELECT a.ACCOUNT_ID, COALESCE(c.ACCOUNT_TIER,'Zero Usage') AS ACCOUNT_TIER
              FROM accts a
              LEFT JOIN SALES.REPORTING.COCO_ACCOUNT_COCO_USAGE c
                ON c.SALESFORCE_ACCOUNT_ID = a.ACCOUNT_ID AND c.IS_YESTERDAY = TRUE
            ),
            {_coco_skill_match_cte('u')}
            SELECT (SELECT COUNT(*) FROM accts) AS GOLIVE_ACCOUNTS,
                   (SELECT COUNT(*) FROM tiers WHERE ACCOUNT_TIER <> 'Zero Usage') AS ACCTS_WITH_COCO,
                   (SELECT COUNT(*) FROM tiers
                     WHERE ACCOUNT_TIER IN ('Activated','Expanded','Deep')) AS ACCTS_ACTIVATED_PLUS,
                   (SELECT COUNT(*) FROM uc) AS GOLIVE_UCS,
                   (SELECT COUNT(*) FROM m WHERE N_SKILLS > 0) AS UCS_WITH_SKILL_MATCH
        """)
        if not rows:
            return {}
        r = rows[0]
        accts = safe_int(r.get("GOLIVE_ACCOUNTS", 0))
        ucs = safe_int(r.get("GOLIVE_UCS", 0))
        with_coco = safe_int(r.get("ACCTS_WITH_COCO", 0))
        act_plus = safe_int(r.get("ACCTS_ACTIVATED_PLUS", 0))
        matched = safe_int(r.get("UCS_WITH_SKILL_MATCH", 0))
        return {
            "golive_accounts": accts,
            "accts_with_coco": with_coco,
            "accts_with_coco_pct": round(with_coco / accts * 100, 1) if accts else 0,
            "accts_activated_plus": act_plus,
            "accts_activated_plus_pct": round(act_plus / accts * 100, 1) if accts else 0,
            "golive_ucs": ucs,
            "ucs_with_skill_match": matched,
            "ucs_matched_pct": round(matched / ucs * 100, 1) if ucs else 0,
        }
    except Exception as e:
        return {"error": str(e)}


def q_use_case_story():
    """
    Curated Problem / Solution / Impact narrative per use case, for the Summary column.

    Source: SALES.RAVEN.USE_CASE_QUALITY_STORIES. This is a STRONG match — it joins
    on a direct use-case key at use-case grain (one row per use case), not at account
    level, so a record cannot be misattributed to a sibling use case at the same account.

    KEY TRAP: join on the MDM `u.USE_CASE_ID`, NOT `RESOLVED_USE_CASE_ID`. Measured on
    the FY27-Q3 cohort: USE_CASE_ID matches 414/702 (59%), RESOLVED matches only 197
    (28%). This is the OPPOSITE of SALES.ACTIVITY.* tables, which key on RESOLVED.
    Neither errors on the wrong key — you just silently get fewer rows.

    Rejected alternatives (verified 2026-09-21, do not retry):
      - SALES.RAVEN.ALL_ENGAGEMENTS_PREPED — richest narrative but ACCOUNT-level only,
        no use-case key. Matches 99.3% of use cases, so it carries no signal about
        WHICH use case, and it is stale (max ACTIVITY_DATE 2025-11-10).
      - SALES.ACTIVITY.USECASE_FIELD_ACTIVITY_AGG — use-case grain, but it is SFDC
        field-change history: the long-text comment fields are 100% NULL, and it
        stopped updating 2026-04-06. 83.5% of its content just restates
        USE_CASE_DESCRIPTION.

    Content is refreshed by LAST_LOAD_DATE (2026-08-12 at time of writing), so it
    lags live SE comments — the renderer stamps the as-of date for that reason.
    """
    excluded = ", ".join(f"'{s}'" for s in CONFIG["dim_excluded_stages"])
    try:
        rows = run_query(f"""
            WITH coh AS (
              SELECT u.USE_CASE_ID AS MDM_ID, u.RESOLVED_USE_CASE_ID AS RID
              FROM SNOWPUBLIC.STREAMLIT.DIM_USE_CASE_MDM_CACHE u
              WHERE {_gvp_filter('u')}
                AND u.USE_CASE_EACV > 0 AND u.IS_DEPLOYED = FALSE AND u.IS_LOST = FALSE
                AND u.GO_LIVE_DATE BETWEEN '{CONFIG["quarter_start"]}' AND '{CONFIG["quarter_end"]}'
                AND u.USE_CASE_STAGE NOT IN ({excluded})
            )
            SELECT c.RID AS LOOKUP_KEY,
                   q.PROBLEM_CHALLENGE, q.SNOWFLAKE_SOLUTION, q.RESULT_OF_IMPACT,
                   q.LAST_LOAD_DATE
            FROM SALES.RAVEN.USE_CASE_QUALITY_STORIES q
            JOIN coh c ON c.MDM_ID = q.USE_CASE_ID
            WHERE q.PROBLEM_CHALLENGE IS NOT NULL
               OR q.SNOWFLAKE_SOLUTION IS NOT NULL
               OR q.RESULT_OF_IMPACT IS NOT NULL
        """)
        return {safe_str(r.get("LOOKUP_KEY", "")): {
                    "problem": safe_str(r.get("PROBLEM_CHALLENGE", "")),
                    "solution": safe_str(r.get("SNOWFLAKE_SOLUTION", "")),
                    "impact": safe_str(r.get("RESULT_OF_IMPACT", "")),
                    "as_of": safe_str(r.get("LAST_LOAD_DATE", "")),
                } for r in rows}
    except Exception:
        return {}


def q_cortex_code_by_account(account_ids):
    """Cortex Code usage per account for use case tables."""
    if not CONFIG.get("is_current_quarter"):
        return {}
    if not account_ids:
        return {}
    id_list = ", ".join(f"'{aid}'" for aid in account_ids if aid)
    if not id_list:
        return {}
    try:
        rows = run_query(f"""
            SELECT SALESFORCE_ACCOUNT_ID as ACCOUNT_ID,
                   AVG_DAILY_USERS as CC_USERS,
                   TOTAL_REQUESTS as CC_REQUESTS,
                   ROUND(ACTUAL_CREDITS, 1) as CC_CREDITS
            FROM SNOWPUBLIC.STREAMLIT.CC_USAGE_CACHE
            WHERE SALESFORCE_ACCOUNT_ID IN ({id_list})
              AND TOTAL_REQUESTS > 0
        """)
    except Exception:
        return {}
    return {r["ACCOUNT_ID"]: r for r in rows}


def q_bronze_tb_by_account():
    rows = run_query(f"""
        SELECT SALESFORCE_ACCOUNT_ID as ACCOUNT_ID, SALESFORCE_ACCOUNT_NAME as ACCOUNT_NAME,
               ROUND(SUM(TB_INGESTED), 1) as TB_INGESTED
        FROM SALES.REPORTING.SALES_PROGRAMS_BRONZE_INGEST
        WHERE GEO_NAME = '{_theater()}'
        GROUP BY SALESFORCE_ACCOUNT_ID, SALESFORCE_ACCOUNT_NAME
    """)
    return {r["ACCOUNT_ID"]: r for r in rows}


def _play_risk_detail_query(play_name, filter_clause):
    excluded = ", ".join(f"'{s}'" for s in CONFIG["dim_excluded_stages"])
    # Use CTE to get total count and risk rows in a single round trip
    rows = run_query(f"""
        WITH base AS (
            SELECT u.ACCOUNT_NAME, u.USE_CASE_NAME,
                   u.USE_CASE_EACV, u.USE_CASE_RISK,
                   u.SE_COMMENTS, u.NEXT_STEPS, u.USE_CASE_STAGE,
                   COUNT(*) OVER() as TOTAL_ALL
            FROM SNOWPUBLIC.STREAMLIT.DIM_USE_CASE_MDM_CACHE u
            WHERE {_gvp_filter('u')}
              AND u.USE_CASE_EACV > 0 AND u.IS_DEPLOYED = FALSE AND u.IS_LOST = FALSE
              AND u.GO_LIVE_DATE BETWEEN '{CONFIG["quarter_start"]}' AND '{CONFIG["quarter_end"]}'
              AND u.USE_CASE_STAGE NOT IN ({excluded})
              AND {filter_clause}
        )
        SELECT *, TOTAL_ALL as TOTAL_COUNT,
               CASE WHEN USE_CASE_RISK IS NOT NULL AND USE_CASE_RISK != '' AND USE_CASE_RISK != 'None'
                    THEN 1 ELSE 0 END as HAS_RISK
        FROM base
        ORDER BY USE_CASE_EACV DESC
    """)
    total = safe_int(rows[0]["TOTAL_COUNT"]) if rows else 0
    risk_rows = [r for r in rows if r["HAS_RISK"] == 1]
    return {"risk_rows": risk_rows, "total_count": total}


def q_play_risk_detail():
    return {
        "bronze": _play_risk_detail_query("Bronze", f"u.PRIORITIZED_FEATURES ILIKE '{CONFIG['bronze_campaign']}'"),
        "si": _play_risk_detail_query("SI", f"u.TECHNICAL_USE_CASE ILIKE '{CONFIG['si_technical_use_case']}'"),
        "sqlserver": _play_risk_detail_query("SQL Server", f"u.PRIORITIZED_FEATURES ILIKE '{CONFIG['sqlserver_campaign']}'"),
    }


def q_high_risk_use_cases():
    excluded = ", ".join(f"'{s}'" for s in CONFIG["dim_excluded_stages"])
    return run_query(f"""
        SELECT u.RESOLVED_USE_CASE_ID as USE_CASE_ID, u.USE_CASE_NUMBER, u.USE_CASE_NAME,
               u.ACCOUNT_OWNER_NAME as AE_NAME, u.ACCOUNT_LEAD_SE_NAME as SE_NAME,
               u.USE_CASE_EACV, u.USE_CASE_RISK as RISK_TYPE,
               SNOWFLAKE.CORTEX.COMPLETE('llama3.1-70b',
                   CONCAT('Summarize this use case risk in 1-2 concise sentences for a sales leadership QC call. Focus on what the risk is and the current mitigation plan. Do not include any preamble or introductory text, just provide the summary directly. Risk type: ',
                       COALESCE(u.USE_CASE_RISK, 'Unknown'),
                       '. SE Comments: ', COALESCE(u.SE_COMMENTS, 'None'),
                       '. Next Steps: ', COALESCE(u.NEXT_STEPS, 'None'))
               ) as RISK_SUMMARY
        FROM SNOWPUBLIC.STREAMLIT.DIM_USE_CASE_MDM_CACHE u
        WHERE {_gvp_filter('u')}
          AND u.USE_CASE_EACV > 0 AND u.IS_DEPLOYED = FALSE AND u.IS_LOST = FALSE
          AND u.GO_LIVE_DATE BETWEEN '{CONFIG["quarter_start"]}' AND '{CONFIG["quarter_end"]}'
          AND u.USE_CASE_STAGE NOT IN ({excluded})
          AND u.USE_CASE_RISK IS NOT NULL AND u.USE_CASE_RISK != '' AND u.USE_CASE_RISK != 'None'
          AND u.USE_CASE_EACV >= {CONFIG["play_threshold"]}
        ORDER BY u.USE_CASE_EACV DESC
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
        movement = r["MOVEMENT_TYPE"].lower()  # 'deployed' or 'created'
        if key:
            tgt = {
                "acv": safe_float(r["TARGET_USE_CASE_EACV"]) if r["TARGET_USE_CASE_EACV"] else None,
                "count": safe_int(r["TARGET_USE_CASE_COUNT"]) if r["TARGET_USE_CASE_COUNT"] else None,
            }
            targets[f"{key}_{movement}"] = tgt
    # Ensure all keys exist with defaults
    for k in ("bronze", "si", "sqlserver"):
        for m in ("deployed", "created"):
            if f"{k}_{m}" not in targets:
                targets[f"{k}_{m}"] = {"acv": None, "count": None}
    return targets


def q_partner_sd_attach():
    excluded = ", ".join(f"'{s}'" for s in CONFIG["dim_excluded_stages"])
    # Single query: per-UC rows with implementer + account ID (replaces 2 queries)
    uc_rows = run_query(f"""
        SELECT u.ACCOUNT_ID, u.IMPLEMENTER, u.USE_CASE_EACV
        FROM SNOWPUBLIC.STREAMLIT.DIM_USE_CASE_MDM_CACHE u
        WHERE {_gvp_filter('u')}
          AND u.USE_CASE_EACV > 0 AND u.IS_DEPLOYED = FALSE AND u.IS_LOST = FALSE
          AND u.GO_LIVE_DATE BETWEEN '{CONFIG["quarter_start"]}' AND '{CONFIG["quarter_end"]}'
          AND u.USE_CASE_STAGE NOT IN ({excluded})
    """)
    total_acv = total_count = partner_acv = partner_count = sd_acv = sd_count = 0
    unassisted_acv = unassisted_count = 0
    partner_values = {"Partner Only", "Partner Prime + Snowflake SD", "Snowflake SD Prime + Partner"}
    sd_values = {"Snowflake SD Prime", "Partner Prime + Snowflake SD",
                 "Customer Prime + Snowflake SD", "Snowflake SD Prime + Partner"}
    unassisted_values = {"Customer Only", "Unknown", "None", "", None}
    partner_accounts = set()
    sd_accounts = set()
    for r in uc_rows:
        acv = safe_float(r["USE_CASE_EACV"] or 0)
        impl = r["IMPLEMENTER"] or ""
        aid = r.get("ACCOUNT_ID")
        total_acv += acv
        total_count += 1
        if impl in partner_values:
            partner_acv += acv
            partner_count += 1
            if aid:
                partner_accounts.add(aid)
        if impl in sd_values:
            sd_acv += acv
            sd_count += 1
            if aid:
                sd_accounts.add(aid)
        if impl in unassisted_values:
            unassisted_acv += acv
            unassisted_count += 1
    partner_rate = (partner_acv / total_acv * 100) if total_acv else 0
    sd_rate = (sd_acv / total_acv * 100) if total_acv else 0
    partner_or_ps_accounts = partner_accounts | sd_accounts
    # Query CC cache directly for partner/PS accounts
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
    """7-day pipeline movement metrics from pre-computed cache table.
    Cache is built from MDM.MDM_INTERFACES.DIM_USE_CASE_DAILY using day-over-day
    LAG comparison to detect actual field-level changes.
    Pushed out  = go-live was in current FQ, now moved past FQ end.
    Pulled in   = go-live was outside current FQ, now moved into FQ.
    Won to lost = stage changed to lost/not-in-pursuit (had go-live in FQ).
    Imp started = stage moved into Implementation In Progress (go-live in FQ).
    Won to imp  = stage moved from Won to Implementation (go-live in FQ).
    Net new     = UC created in last 7 days with go-live in FQ."""
    if not CONFIG.get("is_current_quarter"):
        return {k: {"count": 0, "acv": 0} for k in ("won_to_imp", "won_to_lost", "pushed_out", "pulled_in", "imp_started", "new_pipeline")}
    gvp = CONFIG["gvp_name"]
    rows = run_query(f"""
        SELECT METRIC, CNT, ACV
        FROM SNOWPUBLIC.STREAMLIT.PIPELINE_MOVEMENTS_CACHE
        WHERE THEATER_NAME = '{_theater()}'
    """)
    result = {}
    for r in rows:
        m = r.get("METRIC", "")
        result[m] = {"count": safe_int(r.get("CNT", 0)), "acv": safe_float(r.get("ACV", 0))}
    # Ensure all keys exist
    for key in ("won_to_imp", "won_to_lost", "pushed_out", "pulled_in", "imp_started", "new_pipeline"):
        if key not in result:
            result[key] = {"count": 0, "acv": 0}
    return result


def q_use_case_velocity():
    """Stage transition velocity from pre-computed MDM cache.
    Self-calculated DATEDIFFs for UCs created >= 2025-02-01, all stages.
    Returns avg created-to-TW, TW-to-imp-start, imp-start-to-deployed."""
    if not CONFIG.get("is_current_quarter"):
        return {"time_to_tw": None, "tw_to_imp_start": None, "imp_to_deployed": None}
    gvp = CONFIG["gvp_name"]
    rows = run_query(f"""
        SELECT AVG_TW, AVG_TW_TO_IMP, AVG_IMP_TO_DEPLOYED
        FROM SNOWPUBLIC.STREAMLIT.VELOCITY_CACHE
        WHERE THEATER_NAME = '{_theater()}' AND METRIC_TYPE = 'stage_transition'
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
        FROM SNOWPUBLIC.STREAMLIT.DIM_USE_CASE_MDM_CACHE u
        WHERE {_gvp_filter('u')}
          AND u.PRIORITIZED_FEATURES ILIKE '{CONFIG["bronze_campaign"]}'
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


def q_si_created_qtd():
    """Count SI use cases created QTD using PRIORITIZED_FEATURES (same methodology as bronze)."""
    created_rows = run_query(f"""
        SELECT COUNT(*) as CREATED_COUNT
        FROM SNOWPUBLIC.STREAMLIT.DIM_USE_CASE_MDM_CACHE u
        WHERE {_gvp_filter('u')}
          AND (u.PRIORITIZED_FEATURES ILIKE '{CONFIG["si_campaign_analyst"]}'
               OR u.PRIORITIZED_FEATURES ILIKE '{CONFIG["si_campaign_search"]}')
          AND u.CREATED_DATE BETWEEN '{CONFIG["quarter_start"]}' AND '{CONFIG["quarter_end"]}'
    """)
    return safe_int(created_rows[0]["CREATED_COUNT"]) if created_rows else 0


# =============================================================================
# FORECAST ANALYSIS QUERIES
# =============================================================================

def q_current_pipeline_phases():
    excluded = ", ".join(f"'{s}'" for s in CONFIG["dim_excluded_stages"])
    ref_date = CONFIG["reference_date"]
    rows = run_query(f"""
        SELECT
            CASE
                WHEN u.IS_DEPLOYED = TRUE AND u.GO_LIVE_DATE BETWEEN '{CONFIG["quarter_start"]}' AND '{CONFIG["quarter_end"]}'
                    THEN 'Already Deployed'
                WHEN u.IMPLEMENTATION_START_DATE <= {ref_date}
                    AND u.IS_DEPLOYED = FALSE AND u.IS_LOST = FALSE THEN 'In Implementation'
                WHEN u.TECHNICAL_WIN_DATE <= {ref_date}
                    AND (u.IMPLEMENTATION_START_DATE > {ref_date} OR u.IMPLEMENTATION_START_DATE IS NULL)
                    AND u.IS_DEPLOYED = FALSE AND u.IS_LOST = FALSE THEN 'Post-TW / Pre-Imp'
                WHEN u.CREATED_DATE <= {ref_date}
                    AND (u.TECHNICAL_WIN_DATE > {ref_date} OR u.TECHNICAL_WIN_DATE IS NULL)
                    AND (u.IMPLEMENTATION_START_DATE > {ref_date} OR u.IMPLEMENTATION_START_DATE IS NULL)
                    AND u.IS_DEPLOYED = FALSE AND u.IS_LOST = FALSE THEN 'Pre-TW'
                ELSE 'Other'
            END as PIPELINE_PHASE,
            COUNT(*) as UC_COUNT,
            ROUND(SUM(u.USE_CASE_EACV), 0) as TOTAL_ACV
        FROM SNOWPUBLIC.STREAMLIT.DIM_USE_CASE_MDM_CACHE u
        WHERE {_gvp_filter('u')}
          AND u.USE_CASE_EACV > 0
          AND u.GO_LIVE_DATE BETWEEN '{CONFIG["quarter_start"]}' AND '{CONFIG["quarter_end"]}'
          AND u.USE_CASE_STAGE NOT IN ({excluded})
        GROUP BY PIPELINE_PHASE ORDER BY PIPELINE_PHASE
    """)
    return {r["PIPELINE_PHASE"]: r for r in rows}


def q_wins_pipeline_phases():
    """Current pipeline state for wins: Won QTD, Stage 4 (TW), Pre-TW (Stages 1-3)."""
    qs, qe = CONFIG["quarter_start"], CONFIG["quarter_end"]
    ref_date = CONFIG["reference_date"]
    rows = run_query(f"""
        SELECT
            CASE
                WHEN u.IS_WON = TRUE AND u.DECISION_DATE BETWEEN '{qs}' AND '{qe}'
                    THEN 'Won QTD'
                WHEN u.STAGE_NUMBER = 4
                    AND u.IS_WON = FALSE AND COALESCE(u.IS_LOST, FALSE) = FALSE
                    THEN 'Stage 4 (TW Pipeline)'
                WHEN u.STAGE_NUMBER BETWEEN 1 AND 3
                    AND u.IS_WON = FALSE AND COALESCE(u.IS_LOST, FALSE) = FALSE
                    THEN 'Pre-TW (Stages 1-3)'
                ELSE 'Other'
            END as WIN_PHASE,
            COUNT(*) as UC_COUNT,
            ROUND(SUM(u.USE_CASE_EACV), 0) as TOTAL_ACV
        FROM SNOWPUBLIC.STREAMLIT.DIM_USE_CASE_MDM_CACHE u
        WHERE {_gvp_filter('u')}
          AND u.USE_CASE_EACV > 0
          AND u.GO_LIVE_DATE BETWEEN '{qs}' AND '{qe}'
        GROUP BY WIN_PHASE ORDER BY WIN_PHASE
    """)
    return {r["WIN_PHASE"]: r for r in rows}


def q_historical_conversion_rates(day_number):
    quarters = CONFIG["prior_fy_quarters"]
    if not quarters:
        return []
    day_num = safe_int(day_number)
    snapshot_table = "SNOWPUBLIC.STREAMLIT.DIM_USE_CASE_HISTORY_DS_VW"
    unions = []
    for i, (qs, qe) in enumerate(quarters):
        q_label = _quarter_label(qs)
        snap_date = f"DATEADD('day', {day_num - 1}, '{qs}')::DATE"
        unions.append(f"""
            SELECT '{q_label}' as QTR, '{qs}' as QS, '{qe}' as QE,
                SUM(CASE WHEN h.IS_DEPLOYED = TRUE
                         AND h.GO_LIVE_DATE BETWEEN '{qs}' AND '{qe}'
                    THEN h.USE_CASE_EACV ELSE 0 END) as DEPLOYED_ACV,
                SUM(CASE WHEN h.IMPLEMENTATION_START_DATE <= {snap_date}
                         AND h.IS_DEPLOYED = FALSE AND COALESCE(h.IS_LOST, FALSE) = FALSE
                         AND h.GO_LIVE_DATE BETWEEN '{qs}' AND '{qe}'
                    THEN h.USE_CASE_EACV ELSE 0 END) as IMP_TOTAL,
                SUM(CASE WHEN h.IMPLEMENTATION_START_DATE <= {snap_date}
                         AND h.IS_DEPLOYED = FALSE AND COALESCE(h.IS_LOST, FALSE) = FALSE
                         AND h.GO_LIVE_DATE BETWEEN '{qs}' AND '{qe}'
                         AND o.IS_DEPLOYED = TRUE AND o.GO_LIVE_DATE BETWEEN '{qs}' AND '{qe}'
                    THEN h.USE_CASE_EACV ELSE 0 END) as IMP_CONVERTED,
                SUM(CASE WHEN h.TECHNICAL_WIN_DATE <= {snap_date}
                         AND (h.IMPLEMENTATION_START_DATE > {snap_date} OR h.IMPLEMENTATION_START_DATE IS NULL)
                         AND h.IS_DEPLOYED = FALSE AND COALESCE(h.IS_LOST, FALSE) = FALSE
                         AND h.GO_LIVE_DATE BETWEEN '{qs}' AND '{qe}'
                    THEN h.USE_CASE_EACV ELSE 0 END) as TW_TOTAL,
                SUM(CASE WHEN h.TECHNICAL_WIN_DATE <= {snap_date}
                         AND (h.IMPLEMENTATION_START_DATE > {snap_date} OR h.IMPLEMENTATION_START_DATE IS NULL)
                         AND h.IS_DEPLOYED = FALSE AND COALESCE(h.IS_LOST, FALSE) = FALSE
                         AND h.GO_LIVE_DATE BETWEEN '{qs}' AND '{qe}'
                         AND o.IS_DEPLOYED = TRUE AND o.GO_LIVE_DATE BETWEEN '{qs}' AND '{qe}'
                    THEN h.USE_CASE_EACV ELSE 0 END) as TW_CONVERTED,
                SUM(CASE WHEN h.CREATED_DATE <= {snap_date}
                         AND (h.TECHNICAL_WIN_DATE > {snap_date} OR h.TECHNICAL_WIN_DATE IS NULL)
                         AND (h.IMPLEMENTATION_START_DATE > {snap_date} OR h.IMPLEMENTATION_START_DATE IS NULL)
                         AND h.IS_DEPLOYED = FALSE AND COALESCE(h.IS_LOST, FALSE) = FALSE
                         AND h.GO_LIVE_DATE BETWEEN '{qs}' AND '{qe}'
                    THEN h.USE_CASE_EACV ELSE 0 END) as PRE_TW_TOTAL,
                SUM(CASE WHEN h.CREATED_DATE <= {snap_date}
                         AND (h.TECHNICAL_WIN_DATE > {snap_date} OR h.TECHNICAL_WIN_DATE IS NULL)
                         AND (h.IMPLEMENTATION_START_DATE > {snap_date} OR h.IMPLEMENTATION_START_DATE IS NULL)
                         AND h.IS_DEPLOYED = FALSE AND COALESCE(h.IS_LOST, FALSE) = FALSE
                         AND h.GO_LIVE_DATE BETWEEN '{qs}' AND '{qe}'
                         AND o.IS_DEPLOYED = TRUE AND o.GO_LIVE_DATE BETWEEN '{qs}' AND '{qe}'
                    THEN h.USE_CASE_EACV ELSE 0 END) as PRE_TW_CONVERTED,
                0 as NEW_PIPELINE_CONVERTED,
                0 as FINAL_DEPLOYED
            FROM {snapshot_table} h
            LEFT JOIN id_map m ON h.USE_CASE_ID = m.USE_CASE_ID
            LEFT JOIN SNOWPUBLIC.STREAMLIT.DIM_USE_CASE_MDM_CACHE o ON m.RESOLVED_USE_CASE_ID = o.RESOLVED_USE_CASE_ID
            WHERE h.DS = {snap_date}
              AND {_gvp_filter('h')}
              AND h.USE_CASE_EACV > 0
        """)

    # Combine new_pipeline and final_deployed into a single query
    np_fd_unions = []
    for i, (qs, qe) in enumerate(quarters):
        q_label = _quarter_label(qs)
        snap_date = f"DATEADD('day', {day_num - 1}, '{qs}')::DATE"
        np_fd_unions.append(f"""
            SELECT '{q_label}' as QTR,
                COALESCE((SELECT SUM(o2.USE_CASE_EACV)
                 FROM SNOWPUBLIC.STREAMLIT.DIM_USE_CASE_MDM_CACHE o2
                  LEFT JOIN id_map m2 ON m2.RESOLVED_USE_CASE_ID = o2.RESOLVED_USE_CASE_ID
                  LEFT JOIN {snapshot_table} h2 ON h2.USE_CASE_ID = m2.USE_CASE_ID AND h2.DS = {snap_date}
                 WHERE {_gvp_filter('o2')} AND o2.USE_CASE_EACV > 0
                   AND o2.IS_DEPLOYED = TRUE AND o2.GO_LIVE_DATE BETWEEN '{qs}' AND '{qe}'
                   AND (h2.USE_CASE_ID IS NULL OR h2.CREATED_DATE > {snap_date})
                ), 0) as NEW_PIPELINE_ACV,
                COALESCE((SELECT SUM(u2.USE_CASE_EACV)
                 FROM SNOWPUBLIC.STREAMLIT.DIM_USE_CASE_MDM_CACHE u2
                 WHERE {_gvp_filter('u2')} AND u2.IS_DEPLOYED = TRUE
                   AND u2.GO_LIVE_DATE BETWEEN '{qs}' AND '{qe}' AND u2.USE_CASE_EACV > 0
                ), 0) as FINAL_DEPLOYED
        """)

    id_map_cte = (
        f"WITH id_map AS ("
        f"SELECT DISTINCT USE_CASE_ID, RESOLVED_USE_CASE_ID "
        f"FROM {snapshot_table} "
        f"WHERE DS = (SELECT MAX(DS) FROM {snapshot_table}) "
        f"AND RESOLVED_USE_CASE_ID IS NOT NULL) "
    )
    rows = run_query(id_map_cte + " UNION ALL ".join(unions) + " ORDER BY QTR")
    if np_fd_unions:
        np_fd_rows = run_query(id_map_cte + " UNION ALL ".join(np_fd_unions) + " ORDER BY QTR")
        np_fd_map = {r["QTR"]: r for r in np_fd_rows}
        for r in rows:
            qtr_data = np_fd_map.get(r["QTR"], {})
            r["NEW_PIPELINE_CONVERTED"] = safe_float(qtr_data.get("NEW_PIPELINE_ACV", 0) or 0)
            r["FINAL_DEPLOYED"] = safe_float(qtr_data.get("FINAL_DEPLOYED", 0) or 0)

    rows = [r for r in rows if safe_float(r.get("IMP_TOTAL", 0) or 0) > 0
            or safe_float(r.get("DEPLOYED_ACV", 0) or 0) > 0]
    return rows


def q_wins_historical_conversion_rates(day_number):
    """Historical win conversion rates by stage at day N of prior quarters."""
    quarters = CONFIG.get("prior_fy_quarters", [])
    if not quarters:
        return []
    day_num = safe_int(day_number)
    snapshot_table = "SNOWPUBLIC.STREAMLIT.DIM_USE_CASE_HISTORY_DS_VW"
    unions = []
    for qs, qe in quarters:
        q_label = _quarter_label(qs)
        snap_date = f"DATEADD('day', {day_num - 1}, '{qs}')::DATE"
        unions.append(f"""
            SELECT '{q_label}' as QTR, '{qs}' as QS, '{qe}' as QE,
                SUM(CASE WHEN h.IS_TECH_WON = TRUE
                         AND h.TECHNICAL_WIN_DATE BETWEEN '{qs}' AND {snap_date}
                    THEN h.USE_CASE_EACV ELSE 0 END) as WON_AT_SNAP,
                SUM(CASE WHEN h.STAGE_NUMBER = 4
                         AND COALESCE(h.IS_LOST, FALSE) = FALSE
                         AND h.GO_LIVE_DATE BETWEEN '{qs}' AND '{qe}'
                    THEN h.USE_CASE_EACV ELSE 0 END) as STAGE4_TOTAL,
                SUM(CASE WHEN h.STAGE_NUMBER = 4
                         AND COALESCE(h.IS_LOST, FALSE) = FALSE
                         AND h.GO_LIVE_DATE BETWEEN '{qs}' AND '{qe}'
                         AND o.IS_TECH_WON = TRUE
                         AND o.TECHNICAL_WIN_DATE BETWEEN '{qs}' AND '{qe}'
                    THEN h.USE_CASE_EACV ELSE 0 END) as STAGE4_CONVERTED,
                SUM(CASE WHEN h.STAGE_NUMBER BETWEEN 1 AND 3
                         AND COALESCE(h.IS_LOST, FALSE) = FALSE
                         AND h.GO_LIVE_DATE BETWEEN '{qs}' AND '{qe}'
                    THEN h.USE_CASE_EACV ELSE 0 END) as PRE_TW_TOTAL,
                SUM(CASE WHEN h.STAGE_NUMBER BETWEEN 1 AND 3
                         AND COALESCE(h.IS_LOST, FALSE) = FALSE
                         AND h.GO_LIVE_DATE BETWEEN '{qs}' AND '{qe}'
                         AND o.IS_TECH_WON = TRUE
                         AND o.TECHNICAL_WIN_DATE BETWEEN '{qs}' AND '{qe}'
                    THEN h.USE_CASE_EACV ELSE 0 END) as PRE_TW_CONVERTED,
                0 as NEW_WINS_CONVERTED,
                0 as FINAL_WINS
            FROM {snapshot_table} h
            LEFT JOIN id_map m ON h.USE_CASE_ID = m.USE_CASE_ID
            LEFT JOIN SNOWPUBLIC.STREAMLIT.DIM_USE_CASE_MDM_CACHE o
                ON m.RESOLVED_USE_CASE_ID = o.RESOLVED_USE_CASE_ID
            WHERE h.DS = {snap_date}
              AND {_gvp_filter('h')}
              AND h.USE_CASE_EACV > 0
        """)

    # New wins + final wins per quarter (created after snap that won, and total)
    nw_unions = []
    for qs, qe in quarters:
        q_label = _quarter_label(qs)
        snap_date = f"DATEADD('day', {day_num - 1}, '{qs}')::DATE"
        nw_unions.append(f"""
            SELECT '{q_label}' as QTR,
                COALESCE((SELECT SUM(o2.USE_CASE_EACV)
                  FROM SNOWPUBLIC.STREAMLIT.DIM_USE_CASE_MDM_CACHE o2
                  LEFT JOIN id_map m2 ON m2.RESOLVED_USE_CASE_ID = o2.RESOLVED_USE_CASE_ID
                  LEFT JOIN {snapshot_table} h2 ON h2.USE_CASE_ID = m2.USE_CASE_ID AND h2.DS = {snap_date}
                  WHERE {_gvp_filter('o2')} AND o2.USE_CASE_EACV > 0
                    AND o2.IS_TECH_WON = TRUE
                    AND o2.TECHNICAL_WIN_DATE BETWEEN '{qs}' AND '{qe}'
                    AND (h2.USE_CASE_ID IS NULL OR h2.CREATED_DATE > {snap_date})
                ), 0) as NEW_WINS_ACV,
                COALESCE((SELECT SUM(u2.USE_CASE_EACV)
                  FROM SNOWPUBLIC.STREAMLIT.DIM_USE_CASE_MDM_CACHE u2
                  WHERE {_gvp_filter('u2')} AND u2.IS_TECH_WON = TRUE
                    AND u2.TECHNICAL_WIN_DATE BETWEEN '{qs}' AND '{qe}' AND u2.USE_CASE_EACV > 0
                ), 0) as FINAL_WINS
        """)

    id_map_cte = (
        f"WITH id_map AS (SELECT DISTINCT USE_CASE_ID, RESOLVED_USE_CASE_ID "
        f"FROM {snapshot_table} "
        f"WHERE DS = (SELECT MAX(DS) FROM {snapshot_table}) "
        f"AND RESOLVED_USE_CASE_ID IS NOT NULL) "
    )
    rows = run_query(id_map_cte + " UNION ALL ".join(unions) + " ORDER BY QTR")
    if nw_unions:
        nw_rows = run_query(id_map_cte + " UNION ALL ".join(nw_unions) + " ORDER BY QTR")
        nw_map = {r["QTR"]: r for r in nw_rows}
        for r in rows:
            qd = nw_map.get(r["QTR"], {})
            r["NEW_WINS_CONVERTED"] = safe_float(qd.get("NEW_WINS_ACV", 0) or 0)
            r["FINAL_WINS"] = safe_float(qd.get("FINAL_WINS", 0) or 0)
    rows = [r for r in rows if safe_float(r.get("STAGE4_TOTAL", 0) or 0) > 0
            or safe_float(r.get("WON_AT_SNAP", 0) or 0) > 0]
    return rows


def compute_forecast_analysis(pipeline_phases, hist_rates, deployed, pipeline_risk, most_likely, pacing):
    deployed_acv = float((pipeline_phases.get("Already Deployed") or {}).get("TOTAL_ACV", 0) or 0)
    imp_acv = float((pipeline_phases.get("In Implementation") or {}).get("TOTAL_ACV", 0) or 0)
    tw_acv = float((pipeline_phases.get("Post-TW / Pre-Imp") or {}).get("TOTAL_ACV", 0) or 0)
    pre_tw_acv = float((pipeline_phases.get("Pre-TW") or {}).get("TOTAL_ACV", 0) or 0)

    total_good = sum(safe_float(r.get("GOOD_ACV", 0) or 0) for r in pipeline_risk.values())
    total_pipeline_acv = sum(safe_float(r.get("TOTAL_ACV", 0) or 0) for r in pipeline_risk.values())
    total_at_risk = sum(safe_float(r.get("AT_RISK_ACV", 0) or 0) for r in pipeline_risk.values())
    stage6_acv = float((pipeline_risk.get("Stage 6") or {}).get("TOTAL_ACV", 0) or 0)
    stage5_good_acv = float((pipeline_risk.get("Stage 5") or {}).get("GOOD_ACV", 0) or 0)

    m1_commit = deployed["acv"] + stage6_acv + stage5_good_acv
    m1_most_likely = deployed["acv"] + total_good
    m1_stretch = deployed["acv"] + total_pipeline_acv

    if hist_rates:
        pacing_ratios = []
        for r in hist_rates:
            d26 = safe_float(r.get("DEPLOYED_ACV", 0) or 0)
            final = safe_float(r.get("FINAL_DEPLOYED", 0) or 0)
            if final > 0 and d26 > 0:
                pacing_ratios.append(d26 / final)
        if pacing_ratios:
            avg_ratio = sum(pacing_ratios) / len(pacing_ratios)
            min_ratio = max(pacing_ratios)
            max_ratio = min(pacing_ratios)
            m2_most_likely = deployed["acv"] / avg_ratio if avg_ratio > 0 else 0
            m2_commit = deployed["acv"] / min_ratio if min_ratio > 0 else 0
            m2_stretch = deployed["acv"] / max_ratio if max_ratio > 0 else 0
        else:
            m2_commit = m2_most_likely = m2_stretch = 0
    else:
        m2_commit = m2_most_likely = m2_stretch = 0
        pacing_ratios = []

    if hist_rates:
        imp_rates, tw_rates, pre_tw_rates, new_pcts = [], [], [], []
        for r in hist_rates:
            imp_t = safe_float(r.get("IMP_TOTAL", 0) or 0)
            imp_c = safe_float(r.get("IMP_CONVERTED", 0) or 0)
            tw_t = safe_float(r.get("TW_TOTAL", 0) or 0)
            tw_c = safe_float(r.get("TW_CONVERTED", 0) or 0)
            pt_t = safe_float(r.get("PRE_TW_TOTAL", 0) or 0)
            pt_c = safe_float(r.get("PRE_TW_CONVERTED", 0) or 0)
            new_c = safe_float(r.get("NEW_PIPELINE_CONVERTED", 0) or 0)
            final = safe_float(r.get("FINAL_DEPLOYED", 0) or 0)
            if imp_t > 0: imp_rates.append(imp_c / imp_t)
            if tw_t > 0: tw_rates.append(tw_c / tw_t)
            if pt_t > 0: pre_tw_rates.append(pt_c / pt_t)
            if final > 0: new_pcts.append(new_c / final)

        # Recency weighting: most recent quarter gets 2× weight (append last value again)
        if imp_rates: imp_rates.append(imp_rates[-1])
        if tw_rates: tw_rates.append(tw_rates[-1])
        if pre_tw_rates: pre_tw_rates.append(pre_tw_rates[-1])
        if new_pcts: new_pcts.append(new_pcts[-1])

        avg_imp_rate = sum(imp_rates) / len(imp_rates) if imp_rates else 0
        avg_tw_rate = sum(tw_rates) / len(tw_rates) if tw_rates else 0
        avg_pre_tw_rate = sum(pre_tw_rates) / len(pre_tw_rates) if pre_tw_rates else 0
        avg_new_pct = sum(new_pcts) / len(new_pcts) if new_pcts else 0
        min_imp = min(imp_rates) if imp_rates else 0
        min_tw = min(tw_rates) if tw_rates else 0
        min_pre_tw = min(pre_tw_rates) if pre_tw_rates else 0
        min_new = min(new_pcts) if new_pcts else 0
        max_new = max(new_pcts) if new_pcts else 0

        known_most_likely = deployed_acv + (imp_acv * avg_imp_rate) + (tw_acv * avg_tw_rate) + (pre_tw_acv * avg_pre_tw_rate)
        known_commit = deployed_acv + (imp_acv * min_imp) + (tw_acv * min_tw) + (pre_tw_acv * min_pre_tw)
        m3_most_likely = known_most_likely / (1 - avg_new_pct) if avg_new_pct < 1 else known_most_likely
        m3_commit = known_commit / (1 - min_new) if min_new < 1 else known_commit
        # Stretch capped at total pipeline — historical max never exceeded starting pipeline
        m3_stretch = deployed_acv + imp_acv + tw_acv + pre_tw_acv
    else:
        avg_imp_rate = avg_tw_rate = avg_pre_tw_rate = avg_new_pct = 0
        m3_commit = m3_most_likely = m3_stretch = 0

    rec_commit = (m1_commit + m2_commit + m3_commit) / 3
    rec_most_likely = (m1_most_likely + m2_most_likely + m3_most_likely) / 3
    rec_stretch = (m1_stretch + m2_stretch + m3_stretch) / 3

    return {
        "pipeline_phases": {"deployed": deployed_acv, "in_imp": imp_acv, "post_tw": tw_acv, "pre_tw": pre_tw_acv},
        "method1": {"commit": m1_commit, "most_likely": m1_most_likely, "stretch": m1_stretch, "label": "Pipeline Risk Model"},
        "method2": {"commit": m2_commit, "most_likely": m2_most_likely, "stretch": m2_stretch, "label": "Historical Pacing Model", "ratios": pacing_ratios},
        "method3": {"commit": m3_commit, "most_likely": m3_most_likely, "stretch": m3_stretch, "label": "Stage Conversion Model",
                     "rates": {"imp": avg_imp_rate, "tw": avg_tw_rate, "pre_tw": avg_pre_tw_rate, "new_pipeline": avg_new_pct}},
        "recommended": {"commit": rec_commit, "most_likely": rec_most_likely, "stretch": rec_stretch},
        "current_calls": {"commit": most_likely, "most_likely": most_likely, "stretch": most_likely},
        "hist_rates": hist_rates,
    }


def _compute_backtest_weights(bt_data):
    """Compute inverse-error weights for M1/M2/M3 from a list of backtest quarter results."""
    w = {}
    for call_key in ["commit", "most_likely", "stretch"]:
        errs = {"m1": [], "m2": [], "m3": []}
        for bt in bt_data:
            actual = bt["final_deployed"]
            if actual > 0:
                for mk in errs:
                    errs[mk].append(abs((bt[mk][call_key] - actual) / actual))
        avg_errs = {}
        for mk in errs:
            avg_errs[mk] = sum(errs[mk]) / len(errs[mk]) if errs[mk] else 1.0
        inv = {mk: 1.0 / max(e, 0.01) for mk, e in avg_errs.items()}
        total_inv = sum(inv.values())
        w[call_key] = {mk: v / total_inv for mk, v in inv.items()}
    return w


def _apply_day_adjustment(weights, day_n):
    """Adjust M2 weight based on how far into the quarter we are.
    Pre-quarter (day_n <= 0): M2 = 0, redistribute to M1/M3."""
    if day_n == 31:
        return weights
    adjusted = {}
    for call_key in weights:
        w = dict(weights[call_key])
        if day_n <= 0:
            # Pre-quarter: M2 cannot contribute (deployed = $0), redistribute its weight
            factor = 0.0
        elif day_n < 31:
            factor = 0.5 + 0.5 * (day_n / 31)
        else:
            factor = 1.0 + 0.3 * ((day_n - 31) / 59)
        factor = max(0.0, min(factor, 1.5))
        w["m2"] = w["m2"] * factor
        total = w["m1"] + w["m2"] + w["m3"]
        if total > 0:
            adjusted[call_key] = {mk: v / total for mk, v in w.items()}
        else:
            adjusted[call_key] = w
    return adjusted


def compute_wins_forecast_analysis(win_phases, hist_rates, wins_qtd, risk_analysis, wins_pacing):
    """Compute W1/W2/W3 wins forecast models parallel to go-lives M1/M2/M3."""
    won_acv = float(wins_qtd.get("acv", 0) or 0)
    stage4_acv = float((win_phases.get("Stage 4 (TW Pipeline)") or {}).get("TOTAL_ACV", 0) or 0)
    pre_tw_acv = float((win_phases.get("Pre-TW (Stages 1-3)") or {}).get("TOTAL_ACV", 0) or 0)

    # Stage 4 risk split (already computed by q_pipeline_risk)
    stage4_risk = risk_analysis.get("Stage 4") or {}
    stage4_good = float(stage4_risk.get("GOOD_ACV", 0) or 0)
    stage4_at_risk = float(stage4_risk.get("AT_RISK_ACV", 0) or 0)

    # W1: Pipeline Risk Model — mirrors M1 but for wins
    w1_commit = won_acv + stage4_good
    w1_most_likely = won_acv + stage4_good + (stage4_at_risk * 0.4)  # 40% of at-risk still wins
    w1_stretch = won_acv + stage4_acv + (pre_tw_acv * 0.15)  # all stage4 + some pre-TW

    # W2: Historical Pacing Model — mirrors M2
    if hist_rates:
        pacing_ratios = []
        for r in hist_rates:
            won_snap = safe_float(r.get("WON_AT_SNAP", 0) or 0)
            final = safe_float(r.get("FINAL_WINS", 0) or 0)
            if final > 0 and won_snap > 0:
                pacing_ratios.append(won_snap / final)
        if pacing_ratios:
            avg_ratio = sum(pacing_ratios) / len(pacing_ratios)
            min_ratio = max(pacing_ratios)  # conservative
            max_ratio = min(pacing_ratios)  # optimistic
            w2_most_likely = won_acv / avg_ratio if avg_ratio > 0 else 0
            w2_commit = won_acv / min_ratio if min_ratio > 0 else 0
            w2_stretch = won_acv / max_ratio if max_ratio > 0 else 0
        else:
            w2_commit = w2_most_likely = w2_stretch = 0
    else:
        w2_commit = w2_most_likely = w2_stretch = 0
        pacing_ratios = []

    # W3: Stage Conversion Model — mirrors M3
    if hist_rates:
        stage4_rates, pretw_rates, new_win_pcts = [], [], []
        for r in hist_rates:
            s4_t = safe_float(r.get("STAGE4_TOTAL", 0) or 0)
            s4_c = safe_float(r.get("STAGE4_CONVERTED", 0) or 0)
            pt_t = safe_float(r.get("PRE_TW_TOTAL", 0) or 0)
            pt_c = safe_float(r.get("PRE_TW_CONVERTED", 0) or 0)
            new_c = safe_float(r.get("NEW_WINS_CONVERTED", 0) or 0)
            final = safe_float(r.get("FINAL_WINS", 0) or 0)
            if s4_t > 0: stage4_rates.append(s4_c / s4_t)
            if pt_t > 0: pretw_rates.append(pt_c / pt_t)
            if final > 0: new_win_pcts.append(new_c / final)

        avg_s4_rate = sum(stage4_rates) / len(stage4_rates) if stage4_rates else 0.25
        avg_pt_rate = sum(pretw_rates) / len(pretw_rates) if pretw_rates else 0.15
        avg_new_pct = sum(new_win_pcts) / len(new_win_pcts) if new_win_pcts else 0.30
        min_s4 = min(stage4_rates) if stage4_rates else avg_s4_rate
        min_pt = min(pretw_rates) if pretw_rates else avg_pt_rate

        known_ml = won_acv + (stage4_acv * avg_s4_rate) + (pre_tw_acv * avg_pt_rate)
        known_commit = won_acv + (stage4_acv * min_s4) + (pre_tw_acv * min_pt)
        w3_most_likely = known_ml / (1 - avg_new_pct) if avg_new_pct < 1 else known_ml
        w3_commit = known_commit / (1 - min(new_win_pcts or [avg_new_pct]) * 0.8) if new_win_pcts else known_commit
        w3_stretch = (won_acv + stage4_acv + pre_tw_acv) / (1 - max(new_win_pcts or [avg_new_pct])) if new_win_pcts else (won_acv + stage4_acv + pre_tw_acv)
    else:
        avg_s4_rate = avg_pt_rate = avg_new_pct = 0
        min_s4 = min_pt = 0
        stage4_rates = pretw_rates = new_win_pcts = []
        w3_commit = w3_most_likely = w3_stretch = 0

    # W4: Simple average ensemble
    w4_commit = (w1_commit + w2_commit + w3_commit) / 3 if (w2_commit or w3_commit) else w1_commit
    w4_most_likely = (w1_most_likely + w2_most_likely + w3_most_likely) / 3 if (w2_most_likely or w3_most_likely) else w1_most_likely
    w4_stretch = (w1_stretch + w2_stretch + w3_stretch) / 3 if (w2_stretch or w3_stretch) else w1_stretch

    result = {
        "win_phases": {"won": won_acv, "stage4": stage4_acv, "pre_tw": pre_tw_acv,
                       "stage4_good": stage4_good, "stage4_at_risk": stage4_at_risk},
        "method1": {"commit": w1_commit, "most_likely": w1_most_likely, "stretch": w1_stretch,
                    "label": "Pipeline Risk Model"},
        "method2": {"commit": w2_commit, "most_likely": w2_most_likely, "stretch": w2_stretch,
                    "label": "Historical Pacing Model", "ratios": pacing_ratios},
        "method3": {"commit": w3_commit, "most_likely": w3_most_likely, "stretch": w3_stretch,
                    "label": "Stage Conversion Model",
                    "rates": {"stage4": avg_s4_rate, "pre_tw": avg_pt_rate, "new_wins": avg_new_pct}},
        "method4": {"commit": w4_commit, "most_likely": w4_most_likely, "stretch": w4_stretch,
                    "label": "Simple Ensemble (avg W1/W2/W3)"},
        "hist_rates": hist_rates,
        "recommended": {"commit": w4_commit, "most_likely": w4_most_likely, "stretch": w4_stretch},
    }

    # --- LOO Backtest: for each prior quarter, predict with the other quarters' rates ---
    backtest_rows = []
    for i, r in enumerate(hist_rates):
        others = [x for j, x in enumerate(hist_rates) if j != i]
        if not others:
            continue
        actual_final = safe_float(r.get("FINAL_WINS", 0) or 0)
        won_snap = safe_float(r.get("WON_AT_SNAP", 0) or 0)
        s4_t = safe_float(r.get("STAGE4_TOTAL", 0) or 0)
        pt_t = safe_float(r.get("PRE_TW_TOTAL", 0) or 0)
        new_c = safe_float(r.get("NEW_WINS_CONVERTED", 0) or 0)

        # W2 LOO
        loo_ratios = [safe_float(x.get("WON_AT_SNAP", 0) or 0) / safe_float(x.get("FINAL_WINS", 1) or 1)
                      for x in others if safe_float(x.get("FINAL_WINS", 0) or 0) > 0]
        loo_w2_ml = (won_snap / (sum(loo_ratios) / len(loo_ratios))) if loo_ratios else 0

        # W3 LOO
        loo_s4 = [safe_float(x.get("STAGE4_CONVERTED", 0) or 0) / safe_float(x.get("STAGE4_TOTAL", 1) or 1)
                  for x in others if safe_float(x.get("STAGE4_TOTAL", 0) or 0) > 0]
        loo_pt = [safe_float(x.get("PRE_TW_CONVERTED", 0) or 0) / safe_float(x.get("PRE_TW_TOTAL", 1) or 1)
                  for x in others if safe_float(x.get("PRE_TW_TOTAL", 0) or 0) > 0]
        loo_new = [safe_float(x.get("NEW_WINS_CONVERTED", 0) or 0) / safe_float(x.get("FINAL_WINS", 1) or 1)
                   for x in others if safe_float(x.get("FINAL_WINS", 0) or 0) > 0]
        loo_s4_r = sum(loo_s4) / len(loo_s4) if loo_s4 else avg_s4_rate
        loo_pt_r = sum(loo_pt) / len(loo_pt) if loo_pt else avg_pt_rate
        loo_new_r = sum(loo_new) / len(loo_new) if loo_new else avg_new_pct
        known_w3 = won_snap + (s4_t * loo_s4_r) + (pt_t * loo_pt_r)
        loo_w3_ml = known_w3 / (1 - loo_new_r) if loo_new_r < 1 else known_w3

        w2_err = (loo_w2_ml - actual_final) / actual_final * 100 if actual_final > 0 else 0
        w3_err = (loo_w3_ml - actual_final) / actual_final * 100 if actual_final > 0 else 0
        w4_bt = (loo_w2_ml + loo_w3_ml) / 2
        w4_err = (w4_bt - actual_final) / actual_final * 100 if actual_final > 0 else 0
        backtest_rows.append({
            "qtr": r.get("QTR", ""), "actual": actual_final,
            "w2_ml": loo_w2_ml, "w3_ml": loo_w3_ml, "w4_ml": w4_bt,
            "w2_err": w2_err, "w3_err": w3_err, "w4_err": w4_err,
        })

    result["backtest"] = backtest_rows
    result["backtest_w2_mae"] = (sum(abs(b["w2_err"]) for b in backtest_rows) / len(backtest_rows)) if backtest_rows else None
    result["backtest_w3_mae"] = (sum(abs(b["w3_err"]) for b in backtest_rows) / len(backtest_rows)) if backtest_rows else None
    result["backtest_w4_mae"] = (sum(abs(b["w4_err"]) for b in backtest_rows) / len(backtest_rows)) if backtest_rows else None
    return result


def compute_weighted_ensemble(forecast_analysis, backtest_results, day_number=31):
    fa = forecast_analysis
    m1 = fa["method1"]
    m2 = fa["method2"]
    m3 = fa["method3"]

    if backtest_results:
        base_weights = _compute_backtest_weights(backtest_results)
    else:
        base_weights = {ck: {"m1": 1/3, "m2": 1/3, "m3": 1/3} for ck in ["commit", "most_likely", "stretch"]}

    weights = _apply_day_adjustment(base_weights, day_number)
    m4 = {}
    for call_key in ["commit", "most_likely", "stretch"]:
        w = weights[call_key]
        m4[call_key] = w["m1"] * m1[call_key] + w["m2"] * m2[call_key] + w["m3"] * m3[call_key]

    fa["method4"] = {
        "commit": m4["commit"], "most_likely": m4["most_likely"], "stretch": m4["stretch"],
        "label": "Weighted Ensemble", "weights": weights, "base_weights": base_weights, "day_number": day_number,
    }
    fa["recommended"] = {"commit": m4["commit"], "most_likely": m4["most_likely"], "stretch": m4["stretch"]}

    if backtest_results:
        for i, bt in enumerate(backtest_results):
            others = [b for j, b in enumerate(backtest_results) if j != i]
            if others:
                loo_weights = _compute_backtest_weights(others)
            else:
                loo_weights = {ck: {"m1": 1/3, "m2": 1/3, "m3": 1/3} for ck in ["commit", "most_likely", "stretch"]}
            m4_bt = {}
            for call_key in ["commit", "most_likely", "stretch"]:
                w = loo_weights[call_key]
                m4_bt[call_key] = w["m1"] * bt["m1"][call_key] + w["m2"] * bt["m2"][call_key] + w["m3"] * bt["m3"][call_key]
            bt["m4"] = m4_bt

        confidence = {}
        for call_key in ["commit", "most_likely", "stretch"]:
            m4_val = fa["method4"][call_key]
            pct_errors = []
            for bt in backtest_results:
                actual = bt["final_deployed"]
                if actual > 0 and "m4" in bt:
                    pct_errors.append((bt["m4"][call_key] - actual) / actual)
            if pct_errors:
                mean_err = sum(pct_errors) / len(pct_errors)
                if len(pct_errors) > 1:
                    variance = sum((e - mean_err) ** 2 for e in pct_errors) / (len(pct_errors) - 1)
                    std_err = variance ** 0.5
                else:
                    std_err = abs(mean_err) if mean_err != 0 else 0.1
                min_err = min(pct_errors)
                max_err = max(pct_errors)
                confidence[call_key] = {
                    "mean_error": mean_err, "std_error": std_err,
                    "low_1sigma": m4_val * (1 + mean_err - std_err),
                    "high_1sigma": m4_val * (1 + mean_err + std_err),
                    "low_hist": m4_val * (1 + min_err),
                    "high_hist": m4_val * (1 + max_err),
                    "n_quarters": len(pct_errors),
                }
            else:
                confidence[call_key] = None
        fa["method4"]["confidence"] = confidence


def q_backtest_models(hist_rates):
    if not hist_rates:
        return []
    snapshot_table = "SNOWPUBLIC.STREAMLIT.DIM_USE_CASE_HISTORY_DS_VW"
    gvp = CONFIG["gvp_name"]
    thresholds = CONFIG["risk_thresholds"]

    imp_rates, tw_rates, pre_tw_rates, new_pcts, pacing_ratios = [], [], [], [], []
    for r in hist_rates:
        imp_t = safe_float(r.get("IMP_TOTAL", 0) or 0)
        imp_c = safe_float(r.get("IMP_CONVERTED", 0) or 0)
        tw_t = safe_float(r.get("TW_TOTAL", 0) or 0)
        tw_c = safe_float(r.get("TW_CONVERTED", 0) or 0)
        pt_t = safe_float(r.get("PRE_TW_TOTAL", 0) or 0)
        pt_c = safe_float(r.get("PRE_TW_CONVERTED", 0) or 0)
        new_c = safe_float(r.get("NEW_PIPELINE_CONVERTED", 0) or 0)
        final = safe_float(r.get("FINAL_DEPLOYED", 0) or 0)
        d_n = safe_float(r.get("DEPLOYED_ACV", 0) or 0)
        if imp_t > 0: imp_rates.append(imp_c / imp_t)
        if tw_t > 0: tw_rates.append(tw_c / tw_t)
        if pt_t > 0: pre_tw_rates.append(pt_c / pt_t)
        if final > 0: new_pcts.append(new_c / final)
        if final > 0 and d_n > 0: pacing_ratios.append(d_n / final)

    avg_imp = sum(imp_rates) / len(imp_rates) if imp_rates else 0
    avg_tw = sum(tw_rates) / len(tw_rates) if tw_rates else 0
    avg_pre_tw = sum(pre_tw_rates) / len(pre_tw_rates) if pre_tw_rates else 0
    avg_new = sum(new_pcts) / len(new_pcts) if new_pcts else 0
    avg_pacing = sum(pacing_ratios) / len(pacing_ratios) if pacing_ratios else 0
    min_pacing = max(pacing_ratios) if pacing_ratios else 0
    max_pacing = min(pacing_ratios) if pacing_ratios else 0

    quarters = CONFIG["prior_fy_quarters"]

    # Build a single UNION ALL query for all quarters instead of 4 separate queries
    unions = []
    quarter_meta = {}  # q_label -> (final_deployed, deployed_at_day_n)
    for i, (qs, qe) in enumerate(quarters):
        q_label = _quarter_label(qs)
        matching_hist = [r for r in hist_rates if r["QTR"] == q_label]
        if not matching_hist:
            continue
        hr = matching_hist[0]
        quarter_meta[q_label] = (
            float(hr.get("FINAL_DEPLOYED", 0) or 0),
            float(hr.get("DEPLOYED_ACV", 0) or 0),
        )
        snap_date_str = f"DATEADD('day', 30, '{qs}')::DATE"
        unions.append(f"""
            SELECT '{q_label}' as QTR,
                SUM(CASE WHEN h.IS_DEPLOYED = TRUE
                         AND h.GO_LIVE_DATE BETWEEN '{qs}' AND '{qe}'
                    THEN h.USE_CASE_EACV ELSE 0 END) as DEPLOYED_ACV,
                SUM(CASE WHEN h.IS_DEPLOYED = FALSE AND COALESCE(h.IS_LOST, FALSE) = FALSE
                         AND h.STAGE_NUMBER = 6 AND h.GO_LIVE_DATE BETWEEN '{qs}' AND '{qe}'
                    THEN h.USE_CASE_EACV ELSE 0 END) as STAGE6_ACV,
                SUM(CASE WHEN h.IS_DEPLOYED = FALSE AND COALESCE(h.IS_LOST, FALSE) = FALSE
                         AND h.STAGE_NUMBER = 5
                         AND (h.DAYS_IN_STAGE + DATEDIFF('day', {snap_date_str}, '{qe}')) >= {thresholds["stage_5"]}
                         AND h.GO_LIVE_DATE BETWEEN '{qs}' AND '{qe}'
                    THEN h.USE_CASE_EACV ELSE 0 END) as STAGE5_GOOD,
                SUM(CASE WHEN h.IS_DEPLOYED = FALSE AND COALESCE(h.IS_LOST, FALSE) = FALSE
                         AND h.STAGE_NUMBER BETWEEN 1 AND 6
                         AND h.GO_LIVE_DATE BETWEEN '{qs}' AND '{qe}'
                         AND (
                             (h.STAGE_NUMBER IN (1,2,3) AND (h.DAYS_IN_STAGE + DATEDIFF('day', {snap_date_str}, '{qe}')) >= {thresholds["stage_123"]})
                             OR (h.STAGE_NUMBER = 4 AND (h.DAYS_IN_STAGE + DATEDIFF('day', {snap_date_str}, '{qe}')) >= {thresholds["stage_4"]})
                             OR (h.STAGE_NUMBER = 5 AND (h.DAYS_IN_STAGE + DATEDIFF('day', {snap_date_str}, '{qe}')) >= {thresholds["stage_5"]})
                             OR h.STAGE_NUMBER = 6
                         )
                    THEN h.USE_CASE_EACV ELSE 0 END) as GOOD_PIPELINE,
                SUM(CASE WHEN h.IS_DEPLOYED = FALSE AND COALESCE(h.IS_LOST, FALSE) = FALSE
                         AND h.STAGE_NUMBER BETWEEN 1 AND 6
                         AND h.GO_LIVE_DATE BETWEEN '{qs}' AND '{qe}'
                    THEN h.USE_CASE_EACV ELSE 0 END) as TOTAL_PIPELINE,
                SUM(CASE WHEN h.IMPLEMENTATION_START_DATE <= {snap_date_str}
                         AND h.IS_DEPLOYED = FALSE AND COALESCE(h.IS_LOST, FALSE) = FALSE
                         AND h.GO_LIVE_DATE BETWEEN '{qs}' AND '{qe}'
                    THEN h.USE_CASE_EACV ELSE 0 END) as IMP_ACV,
                SUM(CASE WHEN h.TECHNICAL_WIN_DATE <= {snap_date_str}
                         AND (h.IMPLEMENTATION_START_DATE > {snap_date_str} OR h.IMPLEMENTATION_START_DATE IS NULL)
                         AND h.IS_DEPLOYED = FALSE AND COALESCE(h.IS_LOST, FALSE) = FALSE
                         AND h.GO_LIVE_DATE BETWEEN '{qs}' AND '{qe}'
                    THEN h.USE_CASE_EACV ELSE 0 END) as TW_ACV,
                SUM(CASE WHEN h.CREATED_DATE <= {snap_date_str}
                         AND (h.TECHNICAL_WIN_DATE > {snap_date_str} OR h.TECHNICAL_WIN_DATE IS NULL)
                         AND (h.IMPLEMENTATION_START_DATE > {snap_date_str} OR h.IMPLEMENTATION_START_DATE IS NULL)
                         AND h.IS_DEPLOYED = FALSE AND COALESCE(h.IS_LOST, FALSE) = FALSE
                         AND h.GO_LIVE_DATE BETWEEN '{qs}' AND '{qe}'
                    THEN h.USE_CASE_EACV ELSE 0 END) as PRE_TW_ACV
            FROM {snapshot_table} h
            WHERE h.DS = {snap_date_str}
              AND h.THEATER_NAME = '{_theater()}' AND h.USE_CASE_EACV > 0
        """)

    if not unions:
        return []

    all_rows = run_query(" UNION ALL ".join(unions) + " ORDER BY QTR")
    snap_map = {r["QTR"]: r for r in all_rows}

    results = []
    for q_label, (final_deployed, deployed_at_day_n) in quarter_meta.items():
        snap = snap_map.get(q_label)
        if not snap:
            continue
        dep = float(snap.get("DEPLOYED_ACV", 0) or 0)
        s6 = float(snap.get("STAGE6_ACV", 0) or 0)
        s5g = float(snap.get("STAGE5_GOOD", 0) or 0)
        good = float(snap.get("GOOD_PIPELINE", 0) or 0)
        total_pipe = float(snap.get("TOTAL_PIPELINE", 0) or 0)
        imp_a = float(snap.get("IMP_ACV", 0) or 0)
        tw_a = float(snap.get("TW_ACV", 0) or 0)
        pre_tw_a = float(snap.get("PRE_TW_ACV", 0) or 0)

        m1_commit = dep + s6 + s5g
        m1_ml = dep + good
        m1_stretch = dep + total_pipe

        m2_commit = deployed_at_day_n / min_pacing if min_pacing > 0 else 0
        m2_ml = deployed_at_day_n / avg_pacing if avg_pacing > 0 else 0
        m2_stretch = deployed_at_day_n / max_pacing if max_pacing > 0 else 0

        known_ml = dep + (imp_a * avg_imp) + (tw_a * avg_tw) + (pre_tw_a * avg_pre_tw)
        m3_ml = known_ml / (1 - avg_new) if avg_new < 1 else known_ml
        min_imp_r = min(imp_rates) if imp_rates else 0
        min_tw_r = min(tw_rates) if tw_rates else 0
        min_pre_tw_r = min(pre_tw_rates) if pre_tw_rates else 0
        min_new_r = min(new_pcts) if new_pcts else 0
        max_new_r = max(new_pcts) if new_pcts else 0
        known_commit = dep + (imp_a * min_imp_r) + (tw_a * min_tw_r) + (pre_tw_a * min_pre_tw_r)
        m3_commit = known_commit / (1 - min_new_r) if min_new_r < 1 else known_commit
        m3_stretch = (dep + imp_a + tw_a + pre_tw_a) / (1 - max_new_r) if max_new_r < 1 else (dep + imp_a + tw_a + pre_tw_a)

        bl_commit = (m1_commit + m2_commit + m3_commit) / 3
        bl_ml = (m1_ml + m2_ml + m3_ml) / 3
        bl_stretch = (m1_stretch + m2_stretch + m3_stretch) / 3

        results.append({
            "quarter": q_label, "final_deployed": final_deployed,
            "m1": {"commit": m1_commit, "most_likely": m1_ml, "stretch": m1_stretch},
            "m2": {"commit": m2_commit, "most_likely": m2_ml, "stretch": m2_stretch},
            "m3": {"commit": m3_commit, "most_likely": m3_ml, "stretch": m3_stretch},
            "blended": {"commit": bl_commit, "most_likely": bl_ml, "stretch": bl_stretch},
        })
    return results


def q_retrospective_snapshots(qs, qe, hist_rates, backtest_results):
    """
    For a completed quarter, compute M1/M2/M3/M4 Most Likely predictions at weekly
    snapshot intervals and compare against the actual final deployed ACV.
    Returns a list of dicts, one per snapshot day.
    """
    if not hist_rates:
        return []

    snapshot_table = "SNOWPUBLIC.STREAMLIT.DIM_USE_CASE_HISTORY_DS_VW"
    thresholds = CONFIG["risk_thresholds"]

    # --- Derive historical rate averages (same logic as q_backtest_models) ---
    imp_rates, tw_rates, pre_tw_rates, new_pcts, pacing_ratios = [], [], [], [], []
    for r in hist_rates:
        imp_t = safe_float(r.get("IMP_TOTAL", 0) or 0)
        imp_c = safe_float(r.get("IMP_CONVERTED", 0) or 0)
        tw_t = safe_float(r.get("TW_TOTAL", 0) or 0)
        tw_c = safe_float(r.get("TW_CONVERTED", 0) or 0)
        pt_t = safe_float(r.get("PRE_TW_TOTAL", 0) or 0)
        pt_c = safe_float(r.get("PRE_TW_CONVERTED", 0) or 0)
        new_c = safe_float(r.get("NEW_PIPELINE_CONVERTED", 0) or 0)
        final = safe_float(r.get("FINAL_DEPLOYED", 0) or 0)
        d_n = safe_float(r.get("DEPLOYED_ACV", 0) or 0)
        if imp_t > 0: imp_rates.append(imp_c / imp_t)
        if tw_t > 0: tw_rates.append(tw_c / tw_t)
        if pt_t > 0: pre_tw_rates.append(pt_c / pt_t)
        if final > 0: new_pcts.append(new_c / final)
        if final > 0 and d_n > 0: pacing_ratios.append(d_n / final)

    avg_imp = sum(imp_rates) / len(imp_rates) if imp_rates else 0
    avg_tw = sum(tw_rates) / len(tw_rates) if tw_rates else 0
    avg_pre_tw = sum(pre_tw_rates) / len(pre_tw_rates) if pre_tw_rates else 0
    avg_new = sum(new_pcts) / len(new_pcts) if new_pcts else 0
    avg_pacing = sum(pacing_ratios) / len(pacing_ratios) if pacing_ratios else 0
    min_imp_r = min(imp_rates) if imp_rates else 0
    min_tw_r = min(tw_rates) if tw_rates else 0
    min_pre_tw_r = min(pre_tw_rates) if pre_tw_rates else 0

    # --- M4 weights from backtest ---
    if backtest_results:
        base_weights = _compute_backtest_weights(backtest_results)
    else:
        base_weights = {ck: {"m1": 1/3, "m2": 1/3, "m3": 1/3} for ck in ["commit", "most_likely", "stretch"]}

    # --- Query actual final deployed from MDM cache ---
    actual_rows = run_query(f"""
        SELECT COALESCE(SUM(USE_CASE_EACV), 0) as ACTUAL_FINAL
        FROM SNOWPUBLIC.STREAMLIT.DIM_USE_CASE_MDM_CACHE
        WHERE {_gvp_filter()} AND IS_DEPLOYED = TRUE
          AND GO_LIVE_DATE BETWEEN '{qs}' AND '{qe}'
          AND USE_CASE_EACV > 0
    """)
    actual_final = float(actual_rows[0]["ACTUAL_FINAL"]) if actual_rows else 0

    # --- Weekly snapshot day numbers (day 7 through quarter end) ---
    q_start = date.fromisoformat(qs)
    q_end = date.fromisoformat(qe)
    total_days = (q_end - q_start).days + 1
    snap_day_numbers = list(range(7, total_days, 7))
    if snap_day_numbers[-1] != total_days:
        snap_day_numbers.append(total_days)

    # --- Build UNION ALL query for all snapshot days ---
    unions = []
    for day_n in snap_day_numbers:
        snap_expr = f"DATEADD('day', {day_n - 1}, '{qs}')::DATE"
        unions.append(f"""
            SELECT {day_n} as DAY_N,
                {snap_expr} as SNAP_DATE,
                SUM(CASE WHEN h.IS_DEPLOYED = TRUE
                         AND h.GO_LIVE_DATE BETWEEN '{qs}' AND '{qe}'
                    THEN h.USE_CASE_EACV ELSE 0 END) as DEPLOYED_ACV,
                SUM(CASE WHEN h.IS_DEPLOYED = FALSE AND COALESCE(h.IS_LOST, FALSE) = FALSE
                         AND h.STAGE_NUMBER = 6
                         AND h.GO_LIVE_DATE BETWEEN '{qs}' AND '{qe}'
                    THEN h.USE_CASE_EACV ELSE 0 END) as STAGE6_ACV,
                SUM(CASE WHEN h.IS_DEPLOYED = FALSE AND COALESCE(h.IS_LOST, FALSE) = FALSE
                         AND h.STAGE_NUMBER = 5
                         AND (h.DAYS_IN_STAGE + DATEDIFF('day', {snap_expr}, '{qe}')) >= {thresholds["stage_5"]}
                         AND h.GO_LIVE_DATE BETWEEN '{qs}' AND '{qe}'
                    THEN h.USE_CASE_EACV ELSE 0 END) as STAGE5_GOOD,
                SUM(CASE WHEN h.IS_DEPLOYED = FALSE AND COALESCE(h.IS_LOST, FALSE) = FALSE
                         AND h.STAGE_NUMBER BETWEEN 1 AND 6
                         AND h.GO_LIVE_DATE BETWEEN '{qs}' AND '{qe}'
                         AND (
                             (h.STAGE_NUMBER IN (1,2,3) AND (h.DAYS_IN_STAGE + DATEDIFF('day', {snap_expr}, '{qe}')) >= {thresholds["stage_123"]})
                             OR (h.STAGE_NUMBER = 4 AND (h.DAYS_IN_STAGE + DATEDIFF('day', {snap_expr}, '{qe}')) >= {thresholds["stage_4"]})
                             OR (h.STAGE_NUMBER = 5 AND (h.DAYS_IN_STAGE + DATEDIFF('day', {snap_expr}, '{qe}')) >= {thresholds["stage_5"]})
                             OR h.STAGE_NUMBER = 6
                         )
                    THEN h.USE_CASE_EACV ELSE 0 END) as GOOD_PIPELINE,
                SUM(CASE WHEN h.IS_DEPLOYED = FALSE AND COALESCE(h.IS_LOST, FALSE) = FALSE
                         AND h.STAGE_NUMBER BETWEEN 1 AND 6
                         AND h.GO_LIVE_DATE BETWEEN '{qs}' AND '{qe}'
                    THEN h.USE_CASE_EACV ELSE 0 END) as TOTAL_PIPELINE,
                SUM(CASE WHEN h.IMPLEMENTATION_START_DATE <= {snap_expr}
                         AND h.IS_DEPLOYED = FALSE AND COALESCE(h.IS_LOST, FALSE) = FALSE
                         AND h.GO_LIVE_DATE BETWEEN '{qs}' AND '{qe}'
                    THEN h.USE_CASE_EACV ELSE 0 END) as IMP_ACV,
                SUM(CASE WHEN h.TECHNICAL_WIN_DATE <= {snap_expr}
                         AND (h.IMPLEMENTATION_START_DATE > {snap_expr} OR h.IMPLEMENTATION_START_DATE IS NULL)
                         AND h.IS_DEPLOYED = FALSE AND COALESCE(h.IS_LOST, FALSE) = FALSE
                         AND h.GO_LIVE_DATE BETWEEN '{qs}' AND '{qe}'
                    THEN h.USE_CASE_EACV ELSE 0 END) as TW_ACV,
                SUM(CASE WHEN h.CREATED_DATE <= {snap_expr}
                         AND (h.TECHNICAL_WIN_DATE > {snap_expr} OR h.TECHNICAL_WIN_DATE IS NULL)
                         AND (h.IMPLEMENTATION_START_DATE > {snap_expr} OR h.IMPLEMENTATION_START_DATE IS NULL)
                         AND h.IS_DEPLOYED = FALSE AND COALESCE(h.IS_LOST, FALSE) = FALSE
                         AND h.GO_LIVE_DATE BETWEEN '{qs}' AND '{qe}'
                    THEN h.USE_CASE_EACV ELSE 0 END) as PRE_TW_ACV
            FROM {snapshot_table} h
            WHERE h.DS = {snap_expr}
              AND h.THEATER_NAME = '{_theater()}' AND h.USE_CASE_EACV > 0
        """)

    if not unions:
        return []

    rows = run_query(" UNION ALL ".join(unions) + " ORDER BY DAY_N")

    results = []
    for row in rows:
        day_n = int(row["DAY_N"])
        snap_date = str(row["SNAP_DATE"])
        dep = float(row.get("DEPLOYED_ACV", 0) or 0)
        s6 = float(row.get("STAGE6_ACV", 0) or 0)
        s5g = float(row.get("STAGE5_GOOD", 0) or 0)
        good = float(row.get("GOOD_PIPELINE", 0) or 0)
        total_pipe = float(row.get("TOTAL_PIPELINE", 0) or 0)
        imp_a = float(row.get("IMP_ACV", 0) or 0)
        tw_a = float(row.get("TW_ACV", 0) or 0)
        pre_tw_a = float(row.get("PRE_TW_ACV", 0) or 0)

        # M1
        m1_ml = dep + good

        # M2 (pacing ratio calibrated at day 31 from FY25 history)
        m2_ml = dep / avg_pacing if avg_pacing > 0 else 0

        # M3
        known_ml = dep + (imp_a * avg_imp) + (tw_a * avg_tw) + (pre_tw_a * avg_pre_tw)
        m3_ml = known_ml / (1 - avg_new) if avg_new < 1 else known_ml

        # M4 (day-adjusted ensemble)
        w = _apply_day_adjustment(base_weights, day_n)["most_likely"]
        m4_ml = w["m1"] * m1_ml + w["m2"] * m2_ml + w["m3"] * m3_ml

        def _err(pred):
            return (pred - actual_final) / actual_final * 100 if actual_final > 0 else 0

        results.append({
            "day": day_n,
            "date": snap_date,
            "m1_ml": m1_ml,
            "m2_ml": m2_ml,
            "m3_ml": m3_ml,
            "m4_ml": m4_ml,
            "actual_final": actual_final,
            "m1_err": _err(m1_ml),
            "m2_err": _err(m2_ml),
            "m3_err": _err(m3_ml),
            "m4_err": _err(m4_ml),
        })

    return results


# =============================================================================
# RISK NARRATIVE BUILDERS
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


# =============================================================================
# HTML ROW BUILDER
# =============================================================================

def build_use_case_row(uc, consumption, bronze_tb=None, si_usage=None, cc_by_account=None, row_type="standard",
                       skill_match=None, engagement=None):
    """
    Build one <tr> for a use case table.

    skill_match: optional dict {"confidence","n_skills","sessions"} from
    q_coco_skill_match_by_uc(). When supplied, a "CoCo Skill Match" line is added
    inside col1 next to the ACV/run-rate metrics. Table shape stays 3 columns, so
    callers need no header change. Blank confidence renders nothing.

    engagement: optional list of short strings from the Raven engagement record,
    appended to the Summary column. Pass ONLY records that are a verified strong
    match to this specific use case — an account-level match is not sufficient,
    since one account can carry many unrelated use cases.
    """
    uc_id = safe_str(uc.get("USE_CASE_ID", ""))
    account_id = safe_str(uc.get("ACCOUNT_ID", ""))
    account_name = html_escape(uc.get("ACCOUNT_NAME", ""))
    uc_name = html_escape(uc.get("USE_CASE_NAME", ""))
    uc_number = html_escape(uc.get("USE_CASE_NUMBER", ""))
    acv = float(uc.get("USE_CASE_EACV", 0) or 0)
    forecast_status = safe_str(uc.get("FORECAST_CATEGORY", uc.get("GO_LIVE_FORECAST_STATUS", "")))
    go_live = safe_str(uc.get("GO_LIVE_DATE", ""))
    stage = html_escape(uc.get("USE_CASE_STAGE", ""))
    ae = html_escape(uc.get("ACCOUNT_EXECUTIVE_NAME", uc.get("ACCOUNT_OWNER_NAME", "")))
    se = html_escape(uc.get("USE_CASE_LEAD_SE_NAME", ""))
    region = html_escape(uc.get("REGION_NAME", ""))
    risk = safe_str(uc.get("USE_CASE_RISK", ""))
    next_steps = html_escape(uc.get("NEXT_STEPS", ""))
    se_comments = html_escape(extract_latest_comment(uc.get("SE_COMMENTS", "")))
    # Summary column: full description, NOT truncated. The old 300-char cap was
    # discarding up to 1,859 of 2,159 characters on real records.
    _desc_raw = safe_str(uc.get("USE_CASE_DESCRIPTION", ""))
    description = html_escape(_desc_raw)
    implementer = html_escape(uc.get("IMPLEMENTER", ""))
    partner = html_escape(uc.get("PARTNER_NAME", ""))
    sf_url = f'{CONFIG["salesforce_base_url"]}{uc_id}'
    forecast_class = get_forecast_class(forecast_status)
    forecast_label = get_forecast_label(forecast_status)
    cons = consumption.get(account_id, {})
    if cons:
        rev_90d = fmt_currency(cons.get("REV_90D"), compact=False) if cons.get("REV_90D") is not None else "N/A"
        growth_90d = f'{safe_int(cons.get("GROWTH_90D_PCT", 0) or 0)}%'
        run_rate = fmt_currency(cons.get("RUN_RATE"), compact=False) if cons.get("RUN_RATE") is not None else "N/A"
        rr_growth = f'{safe_int(cons.get("RUN_RATE_GROWTH_PCT", 0) or 0)}%'
        cons_line = f'<span class="consumption">90D: {rev_90d} ({growth_90d}) | Run Rate: {run_rate} ({rr_growth})</span>'
    else:
        cons_line = '<span class="consumption">90D: N/A | Run Rate: N/A</span>'
    col1_lines = [
        f'<a href="{sf_url}" target="_blank">{account_name} - {uc_name}</a><br>',
        f'<span class="uc-number">{uc_number}</span><br>',
        f'<span class="acv">${acv:,.0f}</span><br>',
        cons_line,
    ]
    if row_type == "bronze" and bronze_tb:
        tb_data = bronze_tb.get(account_id, {})
        tb_val = tb_data.get("TB_INGESTED", "N/A") if tb_data else "N/A"
        col1_lines.append(f'<br>\n    <span class="bronze-tb">TB Ingested: {tb_val} TB</span>')
    elif row_type == "si" and si_usage:
        si_data = si_usage.get(account_id, {})
        if si_data:
            credits = safe_int(si_data.get("SI_CREDITS", 0) or 0)
            revenue = fmt_currency(si_data.get("SI_REVENUE"), compact=False)
            users = safe_int(si_data.get("SI_USERS", 0) or 0)
            col1_lines.append(f'<br>\n    <span class="si-metrics">SI 30D: {credits} Credits | {revenue} | {users} Users</span>')
    if cc_by_account:
        cc_data = cc_by_account.get(account_id, {})
        if cc_data:
            cc_users = safe_float(cc_data.get("CC_USERS", 0) or 0)
            cc_credits = safe_float(cc_data.get("CC_CREDITS", 0) or 0)
            col1_lines.append(f'<br>\n    <span class="cc-metrics" style="color: #6f42c1; font-size: 0.85em;">CC CLI 90D: {cc_users:.1f} Avg Daily Users | {cc_credits:,.0f} Credits</span>')
    # CoCo skill match — inline in col1 alongside the other per-account metrics.
    if skill_match is not None:
        _conf = safe_str(skill_match.get("confidence", ""))
        if _conf:
            _n = safe_int(skill_match.get("n_skills", 0))
            _sess = safe_int(skill_match.get("sessions", 0))
            _color = {"High": "#28a745", "Medium": "#ffc107", "Low": "#fd7e14",
                      "None": "#999"}.get(_conf, "#999")
            _detail = (f' ({_n} skill{"" if _n == 1 else "s"}, '
                       f'{_sess:,} session{"" if _sess == 1 else "s"})') if _n else ""
            col1_lines.append(
                f'<br>\n    <span class="coco-skill" style="font-size: 0.85em;">CoCo Skill Match: '
                f'<span style="color: {_color}; font-weight: 600;">{_conf}</span>{_detail}</span>'
            )
    risk_line_html = ""
    if risk and risk.lower() not in ("none", "", "-"):
        risk_line_html = f'<div class="details-row"><span class="risk"><strong>Risk:</strong> {html_escape(risk)}</span></div>'
    col2 = f"""
    <div class="details-row"><span class="label">Forecast:</span> <span class="status-{forecast_class}">{forecast_label}</span> &nbsp; <span class="label">Go-Live:</span> <span class="date">{go_live}</span></div>
    <div class="details-row"><span class="label">Stage:</span> <span class="stage">{stage}</span></div>
    <div class="details-row"><span class="label">AE:</span> <span class="ae-name">{ae}</span> &nbsp;|&nbsp; <span class="label">SE:</span> {se} &nbsp;|&nbsp; <span class="label">Region:</span> {region}</div>
    {risk_line_html}
    <div class="next-steps"><strong>Next Steps:</strong> {next_steps}</div>
    <div class="se-comments"><strong>SE Comments:</strong> {se_comments}</div>"""
    partner_line = f'<div class="partner"><strong>Partner:</strong> {partner}</div>' if partner else ""

    # ---- Summary column: layered context, nothing truncated away ----
    # Description in full, then the recent SE comment history (the newest entry is
    # already in col2, so this adds the ones behind it), then the complete raw log
    # inside <details> so long records are collapsed rather than cut off.
    _desc_block = (f'<div class="uc-desc">{description}</div>' if description
                   else '<div class="uc-desc" style="color:#999;"><em>No description recorded.</em></div>')

    _blocks = extract_recent_comments(uc.get("SE_COMMENTS", ""), n=4)
    _earlier = _blocks[1:] if len(_blocks) > 1 else []
    _earlier_html = ""
    if _earlier:
        _items = "".join(
            f'<div style="margin:3px 0; padding-left:8px; border-left:2px solid #dee2e6;">{html_escape(b)}</div>'
            for b in _earlier
        )
        _earlier_html = (
            '<div style="margin-top:8px; font-size:0.9em;">'
            '<strong style="color:#555;">Earlier SE activity:</strong>'
            f'{_items}</div>'
        )

    # Full untrimmed source text, collapsed. Guarantees nothing is lost from view.
    _full_se = safe_str(uc.get("SE_COMMENTS", ""))
    _full_next = safe_str(uc.get("NEXT_STEPS", ""))
    _full_parts = []
    if _full_next:
        _full_parts.append(f'<strong>Next Steps (full):</strong><br>{html_escape(_full_next)}')
    if _full_se:
        _full_parts.append(f'<strong>SE Comments (full history):</strong><br>{html_escape(_full_se)}')
    _details_html = ""
    if _full_parts:
        _joined = '<br><br>'.join(_full_parts).replace("\n", "<br>")
        _details_html = (
            '<details style="margin-top:8px;">'
            '<summary style="cursor:pointer; color:#0b6d94; font-size:0.85em; font-weight:600;">'
            'Full notes &amp; comment history</summary>'
            f'<div style="margin-top:6px; font-size:0.85em; color:#444; line-height:1.5; '
            f'max-height:340px; overflow-y:auto; background:#fafbfc; padding:8px 10px; '
            f'border-radius:4px;">{_joined}</div></details>'
        )

    # Curated use-case story — only present on a direct use-case-key match.
    _eng_html = ""
    if engagement:
        _parts = []
        for _lbl, _key in (("Problem", "problem"), ("Snowflake solution", "solution"),
                           ("Expected impact", "impact")):
            _v = safe_str(engagement.get(_key, "")).strip()
            if _v:
                _parts.append(
                    f'<div style="margin:4px 0;"><strong style="color:#0b6d94;">{_lbl}:</strong> '
                    f'{html_escape(_v)}</div>'
                )
        if _parts:
            _as_of = safe_str(engagement.get("as_of", ""))
            _stamp = (f' <span style="color:#888; font-weight:400; font-size:0.9em;">'
                      f'(as of {_as_of})</span>') if _as_of else ""
            _eng_html = (
                '<div style="margin:0 0 10px 0; padding:8px 10px; background:#f0f8ff; '
                'border-left:3px solid #29B5E8; border-radius:4px; font-size:0.88em; line-height:1.5;">'
                f'<div style="font-size:0.8em; font-weight:700; color:#0b6d94; '
                f'text-transform:uppercase; letter-spacing:0.04em; margin-bottom:2px;">'
                f'Use case story{_stamp}</div>'
                f'{"".join(_parts)}</div>'
            )

    col3 = f"""{_eng_html}
    {_desc_block}
    {_earlier_html}
    <div class="implementer"><strong>Implementer:</strong> {implementer if implementer else 'None'}</div>
    {partner_line}
    {_details_html}"""
    return f"""<tr>
  <td>{''.join(col1_lines)}</td>
  <td class="details-col">{col2}</td>
  <td class="summary">{col3}</td>
</tr>"""


# =============================================================================
# FORECAST TAB HTML BUILDER (from peak_report._build_forecast_tab)
# =============================================================================

def _build_wins_forecast_tab_html(wfa, wins_forecast, day_number, week_number):
    """HTML builder for wins forecast analysis — parallel to _build_forecast_tab."""
    if not wfa:
        return "<p>Wins forecast analysis data not available.</p>"
    wp = wfa["win_phases"]
    w1 = wfa["method1"]
    w2 = wfa["method2"]
    w3 = wfa["method3"]
    w4 = wfa["method4"]
    rec = wfa["recommended"]
    hist = wfa.get("hist_rates", [])
    rates = w3.get("rates", {})
    prior_fy = CONFIG["prior_fy_label"]

    total_win_pipeline = wp["won"] + wp["stage4"] + wp["pre_tw"]
    def bar_pct(val):
        return max(2, round(val / total_win_pipeline * 100)) if total_win_pipeline > 0 else 0
    def bar_label(val, label):
        return label if (val / total_win_pipeline * 100 if total_win_pipeline > 0 else 0) >= 8 else ""

    # Historical reference table rows
    hist_rows = ""
    for r in hist:
        qtr = r.get("QTR", "")
        won_snap = safe_float(r.get("WON_AT_SNAP", 0) or 0)
        final = safe_float(r.get("FINAL_WINS", 0) or 0)
        ratio = (won_snap / final * 100) if final > 0 else 0
        s4_t = safe_float(r.get("STAGE4_TOTAL", 0) or 0)
        s4_c = safe_float(r.get("STAGE4_CONVERTED", 0) or 0)
        s4_r = (s4_c / s4_t * 100) if s4_t > 0 else 0
        pt_t = safe_float(r.get("PRE_TW_TOTAL", 0) or 0)
        pt_c = safe_float(r.get("PRE_TW_CONVERTED", 0) or 0)
        pt_r = (pt_c / pt_t * 100) if pt_t > 0 else 0
        new_c = safe_float(r.get("NEW_WINS_CONVERTED", 0) or 0)
        new_pct = (new_c / final * 100) if final > 0 else 0
        hist_rows += f"""<tr>
            <td><strong>{qtr}</strong></td>
            <td class="number">{fmt_currency(won_snap)}</td>
            <td class="number">{fmt_currency(final)}</td>
            <td class="number">{ratio:.1f}%</td>
            <td class="number">{s4_r:.1f}%</td>
            <td class="number">{pt_r:.1f}%</td>
            <td class="number">{new_pct:.1f}%</td>
        </tr>"""

    _is_current_q = CONFIG.get("is_current_quarter", True)
    current_row = f"""<tr style="background: #e8f4f8; font-weight: 600;">
        <td><strong>{CONFIG["fiscal_year_label"]} {"(Current)" if _is_current_q else "(Complete)"}</strong></td>
        <td class="number">{fmt_currency(wp["won"])}</td>
        <td class="number">?</td>
        <td class="number">—</td>
        <td class="number" colspan="3" style="text-align:center;color:#29B5E8;">{"In progress — see projections" if _is_current_q else "Quarter Complete"}</td>
    </tr>"""

    w_commit = wins_forecast.get("commit", 0)
    w_ml = wins_forecast.get("most_likely", 0)
    w_stretch = wins_forecast.get("stretch", 0)

    backtest = wfa.get("backtest", [])
    w2_mae = wfa.get("backtest_w2_mae")
    w3_mae = wfa.get("backtest_w3_mae")
    w4_mae = wfa.get("backtest_w4_mae")

    # Backtest table HTML
    bt_rows = ""
    for b in backtest:
        def _err_fmt(e):
            color = "#28a745" if abs(e) <= 10 else "#dc3545"
            sign = "+" if e >= 0 else ""
            return f'<span style="color:{color};">{sign}{e:.1f}%</span>'
        bt_rows += f"""<tr>
            <td><strong>{b["qtr"]}</strong></td>
            <td class="number">{fmt_currency(b["actual"])}</td>
            <td class="number">{fmt_currency(b["w2_ml"])}</td>
            <td class="number">{_err_fmt(b["w2_err"])}</td>
            <td class="number">{fmt_currency(b["w3_ml"])}</td>
            <td class="number">{_err_fmt(b["w3_err"])}</td>
            <td class="number">{fmt_currency(b["w4_ml"])}</td>
            <td class="number">{_err_fmt(b["w4_err"])}</td>
        </tr>"""
    backtest_html = f"""
<h3>Model Backtest (Leave-One-Out)</h3>
<p class="fa-note">For each prior quarter, models are trained on all <em>other</em> quarters and tested on that quarter. Lower error = better calibration.</p>
<table class="fa-table">
  <tr>
    <th>Quarter</th><th>Actual Wins</th>
    <th>W2 ML</th><th>W2 Err%</th>
    <th>W3 ML</th><th>W3 Err%</th>
    <th>W4 ML</th><th>W4 Err%</th>
  </tr>
  {bt_rows}
  <tr style="background:#f0f0f0; font-weight:600;">
    <td>Avg Abs Error</td><td>—</td>
    <td>—</td><td class="number">{f"{w2_mae:.1f}%" if w2_mae is not None else "N/A"}</td>
    <td>—</td><td class="number">{f"{w3_mae:.1f}%" if w3_mae is not None else "N/A"}</td>
    <td>—</td><td class="number">{f"{w4_mae:.1f}%" if w4_mae is not None else "N/A"}</td>
  </tr>
</table>""" if backtest else ""

    return f"""
<h2>Wins Forecast Analysis</h2>
<p class="summary">Three independent models project Commit, Most Likely, and Best Case wins using pipeline risk,
historical pacing, and stage conversion rates. No external target exists for wins — compare to team forecast calls.</p>

<h3>Current Pipeline State (Day {day_number}, Week {week_number})</h3>
<div style="margin: 15px 0;">
  <div style="display: flex; height: 36px; border-radius: 6px; overflow: hidden; box-shadow: 0 2px 4px rgba(0,0,0,0.1);">
    <div style="width: {bar_pct(wp["won"])}%; background: #28a745; display: flex; align-items: center; justify-content: center; color: white; font-size: 0.8em; font-weight: 600; overflow: hidden; white-space: nowrap;">{bar_label(wp["won"], "Won QTD")}</div>
    <div style="width: {bar_pct(wp["stage4"])}%; background: #007bff; display: flex; align-items: center; justify-content: center; color: white; font-size: 0.8em; font-weight: 600; overflow: hidden; white-space: nowrap;">{bar_label(wp["stage4"], "Stage 4")}</div>
    <div style="width: {bar_pct(wp["pre_tw"])}%; background: #dc3545; display: flex; align-items: center; justify-content: center; color: white; font-size: 0.8em; font-weight: 600; overflow: hidden; white-space: nowrap;">{bar_label(wp["pre_tw"], "Pre-TW")}</div>
  </div>
  <div style="display: flex; flex-wrap: wrap; gap: 16px; margin-top: 8px; font-size: 0.85em;">
    <span><span style="display:inline-block;width:12px;height:12px;background:#28a745;border-radius:2px;vertical-align:middle;margin-right:4px;"></span><strong>Won QTD:</strong> {fmt_currency(wp["won"])}</span>
    <span><span style="display:inline-block;width:12px;height:12px;background:#007bff;border-radius:2px;vertical-align:middle;margin-right:4px;"></span><strong>Stage 4 (TW Pipeline):</strong> {fmt_currency(wp["stage4"])} ({fmt_currency(wp["stage4_good"])} good / {fmt_currency(wp["stage4_at_risk"])} at risk)</span>
    <span><span style="display:inline-block;width:12px;height:12px;background:#dc3545;border-radius:2px;vertical-align:middle;margin-right:4px;"></span><strong>Pre-TW (Stages 1-3):</strong> {fmt_currency(wp["pre_tw"])}</span>
  </div>
</div>

<h3>{prior_fy} Historical Reference (at Day {day_number})</h3>
<table class="fa-table">
  <tr><th>Quarter</th><th>Won at Day {day_number}</th><th>Final Wins</th><th>Day {day_number} / Final</th><th>Stage 4 Conv %</th><th>Pre-TW Conv %</th><th>New Wins %</th></tr>
  {hist_rows}
  {current_row}
</table>

<h3>Forecast Models</h3>
<div class="fa-grid">
  <div class="fa-card method1">
    <h4>W1: Pipeline Risk Model</h4>
    <p class="summary">Commit = Won QTD + Stage 4 "good" pipeline. ML adds 40% of at-risk Stage 4. Stretch adds Stage 4 + 15% Pre-TW.</p>
    <div class="fa-vs" style="flex-wrap:nowrap; gap:8px;">
      <div class="fa-vs-item" style="min-width:0; flex:1;"><div class="fa-vs-label">Commit</div><div class="fa-vs-value commit" style="font-size:1.3em;">{fmt_currency(w1["commit"])}</div></div>
      <div class="fa-vs-item" style="min-width:0; flex:1;"><div class="fa-vs-label">Most Likely</div><div class="fa-vs-value likely" style="font-size:1.3em;">{fmt_currency(w1["most_likely"])}</div></div>
      <div class="fa-vs-item" style="min-width:0; flex:1;"><div class="fa-vs-label">Stretch</div><div class="fa-vs-value stretch" style="font-size:1.3em;">{fmt_currency(w1["stretch"])}</div></div>
    </div>
  </div>
  <div class="fa-card method2">
    <h4>W2: Historical Pacing Model</h4>
    <p class="summary">Extrapolates from {prior_fy} win pacing curves at Day {day_number}. LOO error: {f"{w2_mae:.1f}%" if w2_mae is not None else "N/A"}</p>
    <div class="fa-vs" style="flex-wrap:nowrap; gap:8px;">
      <div class="fa-vs-item" style="min-width:0; flex:1;"><div class="fa-vs-label">Commit</div><div class="fa-vs-value commit" style="font-size:1.3em;">{fmt_currency(w2["commit"])}</div></div>
      <div class="fa-vs-item" style="min-width:0; flex:1;"><div class="fa-vs-label">Most Likely</div><div class="fa-vs-value likely" style="font-size:1.3em;">{fmt_currency(w2["most_likely"])}</div></div>
      <div class="fa-vs-item" style="min-width:0; flex:1;"><div class="fa-vs-label">Stretch</div><div class="fa-vs-value stretch" style="font-size:1.3em;">{fmt_currency(w2["stretch"])}</div></div>
    </div>
  </div>
  <div class="fa-card method3">
    <h4>W3: Stage Conversion Model</h4>
    <p class="summary">Applies {prior_fy} stage conversion rates + new pipeline factor. LOO error: {f"{w3_mae:.1f}%" if w3_mae is not None else "N/A"}</p>
    <div class="fa-vs" style="flex-wrap:nowrap; gap:8px;">
      <div class="fa-vs-item" style="min-width:0; flex:1;"><div class="fa-vs-label">Commit</div><div class="fa-vs-value commit" style="font-size:1.3em;">{fmt_currency(w3["commit"])}</div></div>
      <div class="fa-vs-item" style="min-width:0; flex:1;"><div class="fa-vs-label">Most Likely</div><div class="fa-vs-value likely" style="font-size:1.3em;">{fmt_currency(w3["most_likely"])}</div></div>
      <div class="fa-vs-item" style="min-width:0; flex:1;"><div class="fa-vs-label">Stretch</div><div class="fa-vs-value stretch" style="font-size:1.3em;">{fmt_currency(w3["stretch"])}</div></div>
    </div>
    <div style="margin-top:10px;font-size:0.82em;color:#555;">
      Stage 4 conv: {rates.get("stage4",0)*100:.1f}% &nbsp;|&nbsp; Pre-TW conv: {rates.get("pre_tw",0)*100:.1f}% &nbsp;|&nbsp; New wins: {rates.get("new_wins",0)*100:.1f}% of final
    </div>
  </div>
  <div class="fa-card recommended" style="grid-column: auto;">
    <h4>W4: Recommended (avg W2/W3)</h4>
    <p class="summary">Simple ensemble. LOO error: {f"{w4_mae:.1f}%" if w4_mae is not None else "N/A"}</p>
    <div class="fa-vs" style="flex-wrap:nowrap; gap:8px;">
      <div class="fa-vs-item" style="min-width:0; flex:1;"><div class="fa-vs-label">Commit</div><div class="fa-vs-value commit" style="font-size:1.3em;">{fmt_currency(rec["commit"])}</div></div>
      <div class="fa-vs-item" style="min-width:0; flex:1;"><div class="fa-vs-label">Most Likely</div><div class="fa-vs-value likely" style="font-size:1.3em;">{fmt_currency(rec["most_likely"])}</div></div>
      <div class="fa-vs-item" style="min-width:0; flex:1;"><div class="fa-vs-label">Stretch</div><div class="fa-vs-value stretch" style="font-size:1.3em;">{fmt_currency(rec["stretch"])}</div></div>
    </div>
    <div style="margin-top:12px;border-top:1px solid #ddd;padding-top:12px;font-size:0.9em;">
      <strong>Team Forecast Calls:</strong>
      <div class="fa-vs" style="flex-wrap:nowrap; gap:8px; margin-top:8px;">
        <div class="fa-vs-item" style="min-width:0; flex:1;"><div class="fa-vs-label">Commit</div><div class="fa-vs-value" style="color:#333; font-size:1.3em;">{fmt_currency(w_commit)}</div></div>
        <div class="fa-vs-item" style="min-width:0; flex:1;"><div class="fa-vs-label">Most Likely</div><div class="fa-vs-value" style="color:#333; font-size:1.3em;">{fmt_currency(w_ml)}</div></div>
        <div class="fa-vs-item" style="min-width:0; flex:1;"><div class="fa-vs-label">Best Case</div><div class="fa-vs-value" style="color:#333; font-size:1.3em;">{fmt_currency(w_stretch)}</div></div>
      </div>
    </div>
  </div>
</div>

{backtest_html}
"""


def _build_forecast_tab(fa, forecasts, deployed, day_number, week_number):
    if not fa:
        return "<p>Forecast analysis data not available.</p>"
    pp = fa["pipeline_phases"]
    m1 = fa["method1"]
    m2 = fa["method2"]
    m3 = fa["method3"]
    m4 = fa.get("method4", {})
    rec = fa["recommended"]
    hist = fa.get("hist_rates", [])
    rates = m3.get("rates", {})
    prior_fy = CONFIG["prior_fy_label"]
    total_pipeline = pp["deployed"] + pp["in_imp"] + pp["post_tw"] + pp["pre_tw"]

    def bar_pct(val):
        return max(2, round(val / total_pipeline * 100)) if total_pipeline > 0 else 0

    def bar_label(val, label):
        pct = (val / total_pipeline * 100) if total_pipeline > 0 else 0
        return label if pct >= 8 else ""

    hist_rows = ""
    for r in hist:
        qtr = r.get("QTR", "")
        d_acv = safe_float(r.get("DEPLOYED_ACV", 0) or 0)
        final = safe_float(r.get("FINAL_DEPLOYED", 0) or 0)
        ratio = (d_acv / final * 100) if final > 0 else 0
        imp_t = safe_float(r.get("IMP_TOTAL", 0) or 0)
        imp_c = safe_float(r.get("IMP_CONVERTED", 0) or 0)
        imp_r = (imp_c / imp_t * 100) if imp_t > 0 else 0
        tw_t = safe_float(r.get("TW_TOTAL", 0) or 0)
        tw_c = safe_float(r.get("TW_CONVERTED", 0) or 0)
        tw_r = (tw_c / tw_t * 100) if tw_t > 0 else 0
        pt_t = safe_float(r.get("PRE_TW_TOTAL", 0) or 0)
        pt_c = safe_float(r.get("PRE_TW_CONVERTED", 0) or 0)
        pt_r = (pt_c / pt_t * 100) if pt_t > 0 else 0
        new_c = safe_float(r.get("NEW_PIPELINE_CONVERTED", 0) or 0)
        new_pct = (new_c / final * 100) if final > 0 else 0
        hist_rows += f"""<tr>
            <td><strong>{qtr}</strong></td>
            <td class="number">{fmt_currency(d_acv)}</td>
            <td class="number">{fmt_currency(final)}</td>
            <td class="number">{ratio:.1f}%</td>
            <td class="number">{imp_r:.1f}%</td>
            <td class="number">{tw_r:.1f}%</td>
            <td class="number">{pt_r:.1f}%</td>
            <td class="number">{new_pct:.1f}%</td>
        </tr>"""

    _fq_num = CONFIG.get("fiscal_quarter", "FY?-Q?").split("-")[-1]  # "Q1", "Q2", etc.
    _is_current_q = CONFIG.get("is_current_quarter", True)
    _current_row_status = (
        "In progress &mdash; see projections below"
        if _is_current_q
        else "Quarter Complete"
    )
    current_row = f"""<tr style="background: #e8f4f8; font-weight: 600;">
        <td><strong>{CONFIG["fiscal_year_label"]} {_fq_num} {"(Current)" if _is_current_q else "(Complete)"}</strong></td>
        <td class="number">{fmt_currency(pp["deployed"])}</td>
        <td class="number">?</td>
        <td class="number">{(pp["deployed"] / deployed["acv"] * 100) if deployed["acv"] > 0 else 0:.1f}%</td>
        <td class="number" colspan="4" style="text-align: center; color: #29B5E8;">{_current_row_status}</td>
    </tr>"""

    parts = f"""
<h2>Forecast Analysis</h2>
<p class="summary">Three independent models project Commit, Most Likely, and Stretch calls based on current pipeline state,
historical pacing, and milestone-based conversion rates. All data is for {_theater()} / {CONFIG["gvp_name"]} only.</p>

<h3>Current Pipeline State (Day {day_number}, Week {week_number})</h3>
<div style="margin: 15px 0;">
  <div style="display: flex; height: 36px; border-radius: 6px; overflow: hidden; box-shadow: 0 2px 4px rgba(0,0,0,0.1);">
    <div style="width: {bar_pct(pp['deployed'])}%; background: #28a745; display: flex; align-items: center; justify-content: center; color: white; font-size: 0.8em; font-weight: 600; overflow: hidden; white-space: nowrap;">{bar_label(pp['deployed'], 'Deployed')}</div>
    <div style="width: {bar_pct(pp['in_imp'])}%; background: #17a2b8; display: flex; align-items: center; justify-content: center; color: white; font-size: 0.8em; font-weight: 600; overflow: hidden; white-space: nowrap;">{bar_label(pp['in_imp'], 'In Imp')}</div>
    <div style="width: {bar_pct(pp['post_tw'])}%; background: #ffc107; display: flex; align-items: center; justify-content: center; color: #333; font-size: 0.8em; font-weight: 600; overflow: hidden; white-space: nowrap;">{bar_label(pp['post_tw'], 'Post-TW')}</div>
    <div style="width: {bar_pct(pp['pre_tw'])}%; background: #dc3545; display: flex; align-items: center; justify-content: center; color: white; font-size: 0.8em; font-weight: 600; overflow: hidden; white-space: nowrap;">{bar_label(pp['pre_tw'], 'Pre-TW')}</div>
  </div>
  <div style="display: flex; flex-wrap: wrap; gap: 16px; margin-top: 8px; font-size: 0.85em;">
    <span><span style="display: inline-block; width: 12px; height: 12px; background: #28a745; border-radius: 2px; vertical-align: middle; margin-right: 4px;"></span><strong>Deployed:</strong> {fmt_currency(pp["deployed"])}</span>
    <span><span style="display: inline-block; width: 12px; height: 12px; background: #17a2b8; border-radius: 2px; vertical-align: middle; margin-right: 4px;"></span><strong>In Implementation:</strong> {fmt_currency(pp["in_imp"])}</span>
    <span><span style="display: inline-block; width: 12px; height: 12px; background: #ffc107; border-radius: 2px; vertical-align: middle; margin-right: 4px;"></span><strong>Post-TW:</strong> {fmt_currency(pp["post_tw"])}</span>
    <span><span style="display: inline-block; width: 12px; height: 12px; background: #dc3545; border-radius: 2px; vertical-align: middle; margin-right: 4px;"></span><strong>Pre-TW:</strong> {fmt_currency(pp["pre_tw"])}</span>
  </div>
</div>

<h3>{prior_fy} Historical Reference (at Day {day_number})</h3>
<table class="fa-table">
  <tr><th>Quarter</th><th>Deployed at Day {day_number}</th><th>Final Deployed</th><th>Day {day_number} / Final</th><th>Imp Conv %</th><th>TW Conv %</th><th>Pre-TW Conv %</th><th>New Pipeline %</th></tr>
  {hist_rows}
  {current_row}
</table>

<h3>Forecast Models</h3>
<div class="fa-grid">
  <div class="fa-card method1">
    <h4>Method 1: Pipeline Risk Model</h4>
    <p class="summary">Uses stage-based risk thresholds to classify pipeline as "good" or "at risk."</p>
    <div class="fa-vs">
      <div class="fa-vs-item"><div class="fa-vs-label">Commit</div><div class="fa-vs-value commit">{fmt_currency(m1["commit"])}</div></div>
      <div class="fa-vs-item"><div class="fa-vs-label">Most Likely</div><div class="fa-vs-value likely">{fmt_currency(m1["most_likely"])}</div></div>
      <div class="fa-vs-item"><div class="fa-vs-label">Stretch</div><div class="fa-vs-value stretch">{fmt_currency(m1["stretch"])}</div></div>
    </div>
  </div>
  <div class="fa-card method2">
    <h4>Method 2: Historical Pacing Model</h4>
    <p class="summary">Extrapolates from {prior_fy} deployment curves at the same day-in-quarter.</p>
    <div class="fa-vs">
      <div class="fa-vs-item"><div class="fa-vs-label">Commit</div><div class="fa-vs-value commit">{fmt_currency(m2["commit"])}</div></div>
      <div class="fa-vs-item"><div class="fa-vs-label">Most Likely</div><div class="fa-vs-value likely">{fmt_currency(m2["most_likely"])}</div></div>
      <div class="fa-vs-item"><div class="fa-vs-label">Stretch</div><div class="fa-vs-value stretch">{fmt_currency(m2["stretch"])}</div></div>
    </div>
  </div>
  <div class="fa-card method3">
    <h4>Method 3: Stage Conversion Model</h4>
    <p class="summary">Applies milestone-based historical conversion rates to current pipeline phases.</p>
    <div class="fa-vs">
      <div class="fa-vs-item"><div class="fa-vs-label">Commit</div><div class="fa-vs-value commit">{fmt_currency(m3["commit"])}</div></div>
      <div class="fa-vs-item"><div class="fa-vs-label">Most Likely</div><div class="fa-vs-value likely">{fmt_currency(m3["most_likely"])}</div></div>
      <div class="fa-vs-item"><div class="fa-vs-label">Stretch</div><div class="fa-vs-value stretch">{fmt_currency(m3["stretch"])}</div></div>
    </div>
    <div style="margin-top: 10px; font-size: 0.82em;">
      <table style="width:100%; border-collapse: collapse;">
        <tr style="border-bottom: 1px solid #dee2e6; color: #666;">
          <th style="text-align:left; padding: 3px 4px;">Stage</th>
          <th style="text-align:right; padding: 3px 4px;">Pipeline</th>
          <th style="text-align:right; padding: 3px 4px;">Conv %</th>
          <th style="text-align:right; padding: 3px 4px;">Expected</th>
        </tr>
        <tr>
          <td style="padding: 3px 4px;">In Implementation</td>
          <td style="text-align:right; padding: 3px 4px;">{fmt_currency(pp["in_imp"])}</td>
          <td style="text-align:right; padding: 3px 4px;">{rates.get('imp', 0)*100:.1f}%</td>
          <td style="text-align:right; padding: 3px 4px; font-weight:600;">{fmt_currency(pp["in_imp"] * rates.get('imp', 0))}</td>
        </tr>
        <tr style="background:#fafafa;">
          <td style="padding: 3px 4px;">Post-TW / Pre-Imp</td>
          <td style="text-align:right; padding: 3px 4px;">{fmt_currency(pp["post_tw"])}</td>
          <td style="text-align:right; padding: 3px 4px;">{rates.get('tw', 0)*100:.1f}%</td>
          <td style="text-align:right; padding: 3px 4px; font-weight:600;">{fmt_currency(pp["post_tw"] * rates.get('tw', 0))}</td>
        </tr>
        <tr>
          <td style="padding: 3px 4px;">Pre-TW</td>
          <td style="text-align:right; padding: 3px 4px;">{fmt_currency(pp["pre_tw"])}</td>
          <td style="text-align:right; padding: 3px 4px;">{rates.get('pre_tw', 0)*100:.1f}%</td>
          <td style="text-align:right; padding: 3px 4px; font-weight:600;">{fmt_currency(pp["pre_tw"] * rates.get('pre_tw', 0))}</td>
        </tr>
      </table>
      <div style="color:#888; margin-top:4px;">New Pipeline contribution: {rates.get('new_pipeline', 0)*100:.1f}% of final deployed</div>
    </div>
  </div>

"""

    # Pre-compute M4 weight values for the template
    _m4_weights = m4.get("weights", {}) or {}
    _m4_ml_w = _m4_weights.get("most_likely", {}) or {}
    _w_m1 = _m4_ml_w.get("m1", 0) * 100
    _w_m2 = _m4_ml_w.get("m2", 0) * 100
    _w_m3 = _m4_ml_w.get("m3", 0) * 100
    _m4_day = m4.get("day_number", 31)
    _day_adj_note = f", adjusted for day {_m4_day}" if _m4_day != 31 else ""

    parts += f"""
  <div class="fa-card method4" style="border-left: 4px solid #29B5E8;">
    <h4>Method 4: Weighted Ensemble</h4>
    <p class="summary">Weights M1/M2/M3 by inverse backtest error &mdash; models with better historical accuracy get more influence.</p>
    <div class="fa-vs">
      <div class="fa-vs-item"><div class="fa-vs-label">Commit</div><div class="fa-vs-value commit">{fmt_currency(m4.get("commit", 0))}</div><div class="fa-sublabel">Error-weighted blend</div></div>
      <div class="fa-vs-item"><div class="fa-vs-label">Most Likely</div><div class="fa-vs-value likely">{fmt_currency(m4.get("most_likely", 0))}</div><div class="fa-sublabel">Error-weighted blend</div></div>
      <div class="fa-vs-item"><div class="fa-vs-label">Stretch</div><div class="fa-vs-value stretch">{fmt_currency(m4.get("stretch", 0))}</div><div class="fa-sublabel">Error-weighted blend</div></div>
    </div>
    <div class="fa-note">
        <strong>Weights (Most Likely):</strong>
        M1: {_w_m1:.0f}% |
        M2: {_w_m2:.0f}% |
        M3: {_w_m3:.0f}%
        &mdash; derived from {prior_fy} backtest error{_day_adj_note}.
    </div>"""

    # M4 confidence intervals
    ci = m4.get("confidence", {})
    if ci:
        ci_ml = ci.get("most_likely")
        ci_co = ci.get("commit")
        ci_st = ci.get("stretch")
        if ci_ml:
            parts += f"""
    <div style="margin-top: 12px; padding: 10px; background: #f0f8ff; border-radius: 6px; border: 1px solid #d0e8f5;">
        <strong style="font-size: 0.9em;">Confidence Intervals</strong>
        <span style="font-size: 0.8em; color: #666;">(based on {ci_ml['n_quarters']} backtested quarters)</span>
        <table style="width: 100%; font-size: 0.85em; margin-top: 8px; border-collapse: collapse;">
          <tr style="border-bottom: 1px solid #d0e8f5;">
            <th style="text-align: left; padding: 4px;">Call</th>
            <th style="text-align: right; padding: 4px;">Point Estimate</th>
            <th style="text-align: right; padding: 4px;">1&sigma; Range (68%)</th>
            <th style="text-align: right; padding: 4px;">Historical Range</th>
          </tr>"""
            for ck, cl, ci_val in [("commit", "Commit", ci_co), ("most_likely", "Most Likely", ci_ml), ("stretch", "Stretch", ci_st)]:
                if ci_val:
                    parts += f"""
          <tr>
            <td style="padding: 4px;"><strong>{cl}</strong></td>
            <td style="text-align: right; padding: 4px; font-weight: bold;">{fmt_currency(m4.get(ck, 0))}</td>
            <td style="text-align: right; padding: 4px;">{fmt_currency(ci_val['low_1sigma'])} &ndash; {fmt_currency(ci_val['high_1sigma'])}</td>
            <td style="text-align: right; padding: 4px; color: #666;">{fmt_currency(ci_val['low_hist'])} &ndash; {fmt_currency(ci_val['high_hist'])}</td>
          </tr>"""
            parts += """
        </table>
        <div style="font-size: 0.78em; color: #888; margin-top: 6px;">
            1&sigma; range = point estimate adjusted by mean bias &plusmn; 1 standard deviation of backtest errors.
            Historical range = applying min/max observed errors from backtest.
        </div>
    </div>"""

    parts += f"""
  </div>

  <div class="fa-card recommended">
    <h4>Forecast Summary vs Current Calls</h4>
    <table class="fa-table">
      <tr>
        <th>Call</th>
        <th>M1: Risk</th>
        <th>M2: Pacing</th>
        <th>M3: Conversion</th>
        <th style="background: #29B5E8;">M4: Weighted</th>
        <th>Current Call</th>
        <th>Delta</th>
      </tr>
      <tr>
        <td><strong>Commit</strong></td>
        <td class="number">{fmt_currency(m1["commit"])}</td>
        <td class="number">{fmt_currency(m2["commit"])}</td>
        <td class="number">{fmt_currency(m3["commit"])}</td>
        <td class="number" style="font-weight: bold; color: #28a745;">{fmt_currency(rec["commit"])}</td>
        <td class="number">{fmt_currency(forecasts["commit"])}</td>
        <td class="number" style="color: {'#28a745' if rec['commit'] >= forecasts['commit'] else '#dc3545'};">{fmt_currency(rec["commit"] - forecasts["commit"])}</td>
      </tr>
      <tr>
        <td><strong>Most Likely</strong></td>
        <td class="number">{fmt_currency(m1["most_likely"])}</td>
        <td class="number">{fmt_currency(m2["most_likely"])}</td>
        <td class="number">{fmt_currency(m3["most_likely"])}</td>
        <td class="number" style="font-weight: bold; color: #b8860b;">{fmt_currency(rec["most_likely"])}</td>
        <td class="number">{fmt_currency(forecasts["most_likely"])}</td>
        <td class="number" style="color: {'#28a745' if rec['most_likely'] >= forecasts['most_likely'] else '#dc3545'};">{fmt_currency(rec["most_likely"] - forecasts["most_likely"])}</td>
      </tr>
      <tr>
        <td><strong>Stretch</strong></td>
        <td class="number">{fmt_currency(m1["stretch"])}</td>
        <td class="number">{fmt_currency(m2["stretch"])}</td>
        <td class="number">{fmt_currency(m3["stretch"])}</td>
        <td class="number" style="font-weight: bold; color: #dc3545;">{fmt_currency(rec["stretch"])}</td>
        <td class="number">{fmt_currency(forecasts["stretch"])}</td>
        <td class="number" style="color: {'#28a745' if rec['stretch'] >= forecasts['stretch'] else '#dc3545'};">{fmt_currency(rec["stretch"] - forecasts["stretch"])}</td>
      </tr>
    </table>
    <div class="fa-note">
        <strong>Methodology:</strong> M4 weights M1/M2/M3 by inverse historical backtest error.
        Pipeline Risk is bottom-up from current stage data.
        Historical Pacing extrapolates from {prior_fy} deployment velocity. Stage Conversion applies milestone-based conversion rates
        with a new-pipeline uplift factor. All data is {_theater()}-specific.
    </div>
  </div>

</div>
"""

    # Model accuracy summary from backtest
    backtest = fa.get("backtest", [])
    if backtest:
        def _avg_abs_err(backtest_data, model_key, call_key):
            errs = []
            for bt in backtest_data:
                actual = bt["final_deployed"]
                if actual > 0:
                    errs.append(abs((bt[model_key][call_key] - actual) / actual * 100))
            return sum(errs) / len(errs) if errs else 0

        parts += f"""
<h3>Model Accuracy Summary &mdash; {prior_fy} Backtest</h3>
<div class="fa-note" style="margin-bottom: 12px;">
    Average absolute error across all backtested quarters (point-in-time snapshots at day 31).
    Lower error = more accurate model. M4 applies inverse-error weights derived from backtest results.
    <span style="color: #28a745;">&le;10% error</span> |
    <span style="color: #b8860b;">10-20% error</span> |
    <span style="color: #dc3545;">&gt;20% error</span>
</div>
<h4>Average Absolute Error (across all backtested quarters)</h4>
<table class="fa-table">
  <tr>
    <th>Call</th>
    <th>M1: Pipeline Risk</th>
    <th>M2: Pacing</th>
    <th>M3: Conversion</th>
    <th>Equal Blend</th>
    <th style="background: #29B5E8;">M4: Weighted</th>
  </tr>
"""
        for call_key, call_label in [("commit", "Commit"), ("most_likely", "Most Likely"), ("stretch", "Stretch")]:
            m1_err = _avg_abs_err(backtest, "m1", call_key)
            m2_err = _avg_abs_err(backtest, "m2", call_key)
            m3_err = _avg_abs_err(backtest, "m3", call_key)
            bl_err = _avg_abs_err(backtest, "blended", call_key)
            m4_err = _avg_abs_err(backtest, "m4", call_key) if all("m4" in bt for bt in backtest) else 0
            parts += f"""  <tr>
    <td><strong>{call_label}</strong></td>
    <td class="number">{m1_err:.1f}%</td>
    <td class="number">{m2_err:.1f}%</td>
    <td class="number">{m3_err:.1f}%</td>
    <td class="number">{bl_err:.1f}%</td>
    <td class="number" style="font-weight: bold;">{m4_err:.1f}%</td>
  </tr>
"""
        parts += "</table>\n"

    return parts


# =============================================================================
# STREAMLIT CSS
# =============================================================================

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


# =============================================================================
# STREAMLIT APP CONSTANTS
# =============================================================================

PLAY_OPTIONS = {
    "Bronze (Make Your Data AI Ready)": "bronze",
    "Snowflake Intelligence (AI: Snowflake Intelligence & Agents)": "si",
    "SQL Server Migration (Modernize Your Data Estate)": "sqlserver",
}


# =============================================================================
# DATA LOADING (parallel with ThreadPoolExecutor)
# =============================================================================

def _run_all_queries(theater, selected_quarter):
    CONFIG["theater"] = theater
    # Resolve the current GVP for the few person-keyed sources (MaxIQ, SI agg).
    CONFIG["gvp_name"], CONFIG["gvp_email"] = _resolve_gvp(theater)
    CONFIG["is_current_quarter"] = selected_quarter["is_current"]
    # For current quarter, use CURRENT_DATE(); for past, use quarter end; for future, use quarter start
    if selected_quarter["is_current"]:
        CONFIG["reference_date"] = "CURRENT_DATE()"
    else:
        # If quarter end is in the past, use quarter end as reference
        if date.fromisoformat(selected_quarter["end"]) < date.today():
            CONFIG["reference_date"] = f"'{selected_quarter['end']}'::DATE"
        else:
            CONFIG["reference_date"] = f"'{selected_quarter['start']}'::DATE"
    CONFIG["fiscal_quarter_key"] = selected_quarter["fiscal_quarter_key"]
    total_steps = 10
    completed = [0]
    progress = st.progress(0, text="Connecting to Snowflake...")

    def _tick(label):
        completed[0] += 1
        progress.progress(min(completed[0] / total_steps, 1.0), text=label)

    # --- Phase 0: Sequential setup (sets CONFIG values needed by later queries) ---
    _tick("Fiscal calendar & velocity...")
    fiscal = q_fiscal_calendar(selected_quarter)
    CONFIG["day_number"] = safe_int(fiscal["DAY_NUMBER"])
    uc_velocity = q_use_case_velocity()
    update_risk_thresholds_from_velocity(uc_velocity)

    # Pin dim_uc_table to a concrete DS value so downstream queries skip MAX(DS) subquery
    try:
        _ds_rows = run_query("SELECT MAX(DS) AS MAX_DS FROM SNOWPUBLIC.STREAMLIT.DIM_USE_CASE_HISTORY_DS_VW")
        if _ds_rows and _ds_rows[0]["MAX_DS"] is not None:
            _max_ds = str(_ds_rows[0]["MAX_DS"])[:10]  # YYYY-MM-DD
            CONFIG["dim_uc_table"] = f"(SELECT * FROM SNOWPUBLIC.STREAMLIT.DIM_USE_CASE_HISTORY_DS_VW WHERE DS = '{_max_ds}')"
    except Exception:
        pass  # Keep the original subquery-based definition

    # --- Phase 1: Parallel independent queries ---
    _tick("Loading data (parallel)...")
    phase1_queries = {
        "revenue": q_qtd_revenue,
        "forecasts": q_forecast_calls,
        "deployed": q_deployed_qtd,
        "last7": q_last7_deployed,
        "pipeline": q_open_pipeline,
        "risk_analysis": q_pipeline_risk,
        "top5": q_top5_use_cases,
        "play_summary": q_sales_play_summary,
        "play_detail": q_play_detail_metrics,
        "bronze_tb_total": q_bronze_tb_total,
        "tb_ingested_target": q_tb_ingested_target,
        "play_use_cases": q_play_use_cases,
        "play_risk": q_play_risk_detail,
        "high_risk_ucs": q_high_risk_use_cases,
        "play_targets": q_play_targets,
        "partner_sd": q_partner_sd_attach,
        "pipeline_movements": q_pipeline_movements,
        "bronze_created": q_bronze_created_qtd,
        "si_created": q_si_created_qtd,
        "si_theater": q_si_theater_totals,
        "bronze_tb_acct": q_bronze_tb_by_account,
        "velocity": q_deployment_velocity,
        "pipeline_detail": q_risk_adjusted_pipeline_detail,
        "pipeline_phases": q_current_pipeline_phases,
        "coco_tiers": q_coco_theater_tiers,
        "coco_movement": q_coco_tier_movement,
        "coco_insights": q_coco_golive_insights,
        "coco_skill_uc": q_coco_skill_match_by_uc,
        "uc_story": q_use_case_story,
        "theater_consumption": q_theater_consumption,
        "wins_qtd": q_wins_qtd,
        "last7_wins": q_last7_wins,
        "wins_forecast": q_wins_forecast_calls,
        "wins_open_pipeline": q_wins_open_pipeline,
        "wins_top5": q_wins_top5,
        "wins_pipeline_phases": q_wins_pipeline_phases,
        "wins_pipeline_movements": q_wins_pipeline_movements,
        "wins_risk_analysis": q_wins_risk_analysis,
    }
    phase1_with_args = {
        "pacing": lambda: q_prior_fy_pacing(fiscal["DAY_NUMBER"], fiscal["WEEK_NUMBER"]),
        "wins_pacing": lambda: q_wins_pacing(fiscal["DAY_NUMBER"], fiscal["WEEK_NUMBER"]),
        "hist_conv_rates": lambda: q_historical_conversion_rates(fiscal["DAY_NUMBER"]),
        "wins_hist_conv": lambda: q_wins_historical_conversion_rates(fiscal["DAY_NUMBER"]),
    }
    all_phase1 = {**phase1_queries, **phase1_with_args}
    p1_results = {}
    p1_errors = {}

    with ThreadPoolExecutor(max_workers=34) as executor:
        futures = {executor.submit(fn): name for name, fn in all_phase1.items()}
        for future in as_completed(futures):
            name = futures[future]
            try:
                p1_results[name] = future.result()
            except Exception as e:
                p1_errors[name] = e
                p1_results[name] = {} if name not in ("top5", "play_use_cases") else ([] if name == "top5" else {})

    if p1_errors:
        for name, err in p1_errors.items():
            st.warning(f"Query '{name}' failed: {err}")

    _tick("Phase 1 complete...")

    # Unpack results
    revenue = p1_results["revenue"]
    forecasts = p1_results["forecasts"]
    deployed = p1_results["deployed"]
    last7 = p1_results["last7"]
    pipeline = p1_results["pipeline"]
    risk_analysis = p1_results["risk_analysis"]
    top5 = p1_results["top5"]
    play_summary = p1_results["play_summary"]
    play_detail = p1_results["play_detail"]
    bronze_tb_total = p1_results["bronze_tb_total"]
    tb_ingested_target = p1_results["tb_ingested_target"]
    play_use_cases = p1_results["play_use_cases"]
    play_risk = p1_results["play_risk"]
    high_risk_ucs = p1_results["high_risk_ucs"]
    play_targets = p1_results["play_targets"]
    partner_sd = p1_results["partner_sd"]
    pipeline_movements = p1_results["pipeline_movements"]
    bronze_created = p1_results["bronze_created"]
    si_created = p1_results["si_created"]
    si_theater = p1_results["si_theater"]
    bronze_tb_acct = p1_results["bronze_tb_acct"]
    velocity = p1_results["velocity"]
    pipeline_detail = p1_results["pipeline_detail"]
    pipeline_phases = p1_results["pipeline_phases"]
    coco_tiers = p1_results["coco_tiers"]
    coco_movement = p1_results["coco_movement"]
    coco_insights = p1_results["coco_insights"]
    coco_skill_uc = p1_results["coco_skill_uc"]
    uc_story = p1_results["uc_story"]
    theater_consumption = p1_results["theater_consumption"]
    wins_qtd = p1_results["wins_qtd"]
    last7_wins = p1_results["last7_wins"]
    wins_forecast = p1_results["wins_forecast"]
    wins_open_pipeline = p1_results["wins_open_pipeline"]
    wins_top5 = p1_results["wins_top5"]
    wins_pipeline_phases = p1_results["wins_pipeline_phases"]
    wins_pipeline_movements = p1_results["wins_pipeline_movements"]
    wins_risk_analysis = p1_results["wins_risk_analysis"]
    pacing = p1_results["pacing"]
    wins_pacing = p1_results["wins_pacing"]
    hist_conv_rates = p1_results["hist_conv_rates"]
    wins_hist_conv = p1_results["wins_hist_conv"]
    # Fetch regional targets synchronously (outside thread pool for SiS compatibility)
    try:
        regional_targets = q_regional_targets()
    except Exception as e:
        st.warning(f"Regional targets query failed: {e}")
        regional_targets = {}

    # Populate targets from VP_REGIONAL_TARGETS_VIEW (go-live) and PEAK_FORECAST (revenue/consumption)
    forecasts["target"] = regional_targets.get("Use Case Go Live", 0)
    consumption_target = revenue.get("target", 0)
    revenue["target"] = fmt_currency(consumption_target) if consumption_target else "TBD"
    revenue["pct_target"] = fmt_pct(revenue["revenue"] / consumption_target * 100) if consumption_target else "TBD"

    # --- Phase 2: Queries that depend on Phase 1 account IDs (parallel) ---
    _tick("Account-level queries...")
    all_account_ids = set()
    for uc in top5:
        all_account_ids.add(safe_str(uc.get("ACCOUNT_ID")))
    for play in play_use_cases.values():
        for uc in play:
            all_account_ids.add(safe_str(uc.get("ACCOUNT_ID")))
    all_account_ids.discard("")
    si_account_ids = set()
    for uc in play_use_cases.get("si", []):
        si_account_ids.add(safe_str(uc.get("ACCOUNT_ID")))
    si_account_ids.discard("")

    phase2_queries = {
        "consumption": lambda: q_consumption(all_account_ids),
        "si_usage": lambda: q_si_usage(si_account_ids),
        "cc_by_account": lambda: q_cortex_code_by_account(all_account_ids),
    }
    p2_results = {}
    with ThreadPoolExecutor(max_workers=3) as executor:
        futures = {executor.submit(fn): name for name, fn in phase2_queries.items()}
        for future in as_completed(futures):
            name = futures[future]
            try:
                p2_results[name] = future.result()
            except Exception as e:
                st.warning(f"Query '{name}' failed: {e}")
                p2_results[name] = {}

    consumption = p2_results["consumption"]
    si_usage = p2_results["si_usage"]
    cc_by_account = p2_results["cc_by_account"]

    # --- Phase 3: Forecast computation (local, no SQL) ---
    _tick("Forecast models...")
    forecast_analysis = compute_forecast_analysis(
        pipeline_phases, hist_conv_rates, deployed, risk_analysis,
        forecasts.get("most_likely", 0) if isinstance(forecasts, dict) else 0, pacing
    )
    _tick("Backtest models...")
    backtest_results = q_backtest_models(hist_conv_rates)
    forecast_analysis["backtest"] = backtest_results
    _tick("Weighted ensemble...")
    compute_weighted_ensemble(forecast_analysis, backtest_results, day_number=safe_int(fiscal["DAY_NUMBER"]))
    forecast_analysis["velocity"] = velocity
    forecast_analysis["pipeline_detail"] = pipeline_detail

    # --- Wins forecast analysis (parallel to go-lives Phase 3) ---
    wins_forecast_analysis = compute_wins_forecast_analysis(
        wins_pipeline_phases, wins_hist_conv, wins_qtd, risk_analysis, wins_pacing
    )

    # Retrospective: compute multi-snapshot model accuracy for completed quarters
    _tick("Retrospective analysis...")
    if date.fromisoformat(selected_quarter["end"]) < date.today():
        forecast_analysis["retrospective"] = q_retrospective_snapshots(
            selected_quarter["start"], selected_quarter["end"],
            hist_conv_rates, backtest_results
        )

    progress.progress(1.0, text="Done!")
    progress.empty()

    return {
        "fiscal": fiscal, "revenue": revenue, "forecasts": forecasts,
        "deployed": deployed, "last7": last7, "pipeline": pipeline,
        "risk_analysis": risk_analysis, "top5": top5,
        "play_summary": play_summary, "play_detail": play_detail,
        "play_use_cases": play_use_cases, "play_risk": play_risk,
        "high_risk_ucs": high_risk_ucs, "consumption": consumption,
        "bronze_tb_total": bronze_tb_total, "bronze_tb_acct": bronze_tb_acct,
        "tb_ingested_target": tb_ingested_target,
        "si_usage": si_usage, "si_theater": si_theater, "pacing": pacing,
        "forecast_analysis": forecast_analysis, "play_targets": play_targets,
        "regional_targets": regional_targets,
        "partner_sd": partner_sd, "uc_velocity": uc_velocity,
        "bronze_created": bronze_created, "si_created": si_created, "pipeline_movements": pipeline_movements,
        "coco_tiers": coco_tiers, "coco_movement": coco_movement,
        "coco_insights": coco_insights, "coco_skill_uc": coco_skill_uc,
        "uc_story": uc_story,
        "cc_by_account": cc_by_account,
        "theater_consumption": theater_consumption,
        "wins_qtd": wins_qtd, "last7_wins": last7_wins,
        "wins_forecast": wins_forecast, "wins_open_pipeline": wins_open_pipeline,
        "wins_top5": wins_top5, "wins_pacing": wins_pacing,
        "wins_pipeline_movements": wins_pipeline_movements,
        "wins_risk_analysis": wins_risk_analysis,
        "wins_forecast_analysis": wins_forecast_analysis,
        "_config": {
            "theater": CONFIG.get("theater"),
            "gvp_name": CONFIG.get("gvp_name"),
            "gvp_email": CONFIG.get("gvp_email"),
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
        },
        "_loaded_at": datetime.now(),
    }


def load_all_data(theater, selected_quarter):
    cache_key = f"peak_data_{theater}_{selected_quarter['label']}"
    cached = st.session_state.get(cache_key)
    if cached is not None:
        loaded_at = cached.get("_loaded_at")
        if loaded_at and (datetime.now() - loaded_at).total_seconds() < 600:
            return cached
    data = _run_all_queries(theater, selected_quarter)
    st.session_state[cache_key] = data
    return data


# =============================================================================
# RENDER HELPERS
# =============================================================================

def fmt_delta_html(delta):
    if delta is None:
        return ""
    if delta == 0:
        return '<br><span style="font-size: 0.8em; color: #888;">Flat WoW</span>'
    sign = "+" if delta > 0 else ""
    color = "#28a745" if delta > 0 else "#dc3545"
    return f'<br><span style="font-size: 0.8em; color: {color};">{sign}{fmt_currency(delta)} WoW</span>'


def fmt_delta_text(delta):
    if delta is None:
        return ""
    if delta == 0:
        return " (flat WoW)"
    if delta > 0:
        return f" (+{fmt_currency(delta)} WoW)"
    else:
        return f" (-{fmt_currency(abs(delta))} WoW)"


def risk_line(risk_analysis, stage_group):
    r = risk_analysis.get(stage_group, {})
    if not r:
        return ""
    total = fmt_currency(r.get("TOTAL_ACV", 0), compact=True)
    total_count = safe_int(r.get("TOTAL_COUNT", 0))
    at_risk = fmt_currency(r.get("AT_RISK_ACV", 0), compact=True)
    at_risk_count = safe_int(r.get("AT_RISK_COUNT", 0))
    good = fmt_currency(r.get("GOOD_ACV", 0), compact=True)
    good_count = total_count - at_risk_count
    if stage_group == "Stage 6":
        return f"&bull; Stage 6: {total_count} total ({total}) &mdash; all good<br>"
    return f"&bull; {stage_group}: {total_count} total ({total}) &mdash; {at_risk_count} at risk ({at_risk}), {good_count} good ({good})<br>"


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
        acv = float(uc.get("USE_CASE_EACV", 0) or 0)
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
# TAB RENDERERS (from peak_app.py)
# =============================================================================

def fmt_ci(value):
    """Integer-format currency for script talk track — 1 decimal for billions, whole numbers otherwise."""
    if value is None or _is_nan(value):
        return "N/A"
    v = float(value)
    if abs(v) >= 1_000_000_000:
        return f"${v / 1_000_000_000:.1f}B"
    elif abs(v) >= 1_000_000:
        return f"${round(v / 1_000_000)}M"
    elif abs(v) >= 1_000:
        return f"${round(v / 1_000)}K"
    return f"${round(v):,}"


def fmt_pi(value):
    """Integer-format percent for script talk track (no decimal places)."""
    if value is None or _is_nan(value):
        return "N/A"
    return f"{round(float(value))}%"


def _generate_script_summary(prompt):
    """Call Snowflake Cortex Complete and return a brief exec summary string."""
    try:
        safe = prompt.replace("'", "''").replace("\n", " ")
        rows = run_query(f"SELECT SNOWFLAKE.CORTEX.COMPLETE('mistral-large2', '{safe}') as SUMMARY")
        return rows[0]["SUMMARY"].strip() if rows else None
    except Exception:
        return None


def render_script_tab(data):
    fiscal = data["fiscal"]
    forecasts = data["forecasts"]
    deployed = data["deployed"]
    pipeline = data["pipeline"]
    risk_analysis = data["risk_analysis"]
    high_risk_ucs = data["high_risk_ucs"]
    partner_sd = data["partner_sd"]
    uc_velocity = data["uc_velocity"]
    pacing = data["pacing"]
    cfg = data["_config"]
    pm = data.get("pipeline_movements", {})
    last7 = data["last7"]
    theater_consumption = data.get("theater_consumption") or {}

    most_likely = forecasts["most_likely"]
    gl_target = forecasts["target"]
    deployed_pct = (deployed["acv"] / most_likely * 100) if most_likely else 0
    deployed_pct_of_target = (deployed["acv"] / gl_target * 100) if gl_target else 0
    ml_pct_of_target = (most_likely / gl_target * 100) if gl_target else 0
    coverage_pct = ((pipeline["acv"] + deployed["acv"]) / most_likely * 100) if most_likely else 0
    total_good = sum(safe_float(r.get("GOOD_ACV", 0) or 0) for r in risk_analysis.values())
    total_risk_acv = sum(safe_float(r.get("AT_RISK_ACV", 0) or 0) for r in risk_analysis.values())
    good_coverage = ((total_good + deployed["acv"]) / most_likely * 100) if most_likely else 0
    ml_wow_text = fmt_delta_text(forecasts.get("ml_delta"))
    day_number = safe_int(fiscal["DAY_NUMBER"])
    week_number = safe_int(fiscal["WEEK_NUMBER"])
    day_avg = pacing["day_avg"]
    day_pct_val = pacing["day_pct"]
    week_avg = pacing["week_avg"]
    week_pct_val = pacing["week_pct"]
    day_projection = (deployed["acv"] / (day_pct_val / 100)) if day_pct_val > 0 else None
    week_projection = (deployed["acv"] / (week_pct_val / 100)) if week_pct_val > 0 else None
    day_ahead = deployed["acv"] >= day_avg if day_avg else True
    week_ahead = deployed["acv"] >= week_avg if week_avg else True
    if day_ahead and week_ahead:
        pacing_direction = "ahead on both a daily and weekly basis"
    elif not day_ahead and not week_ahead:
        pacing_direction = "behind on both a daily and weekly basis"
    elif day_ahead:
        pacing_direction = "ahead on a daily basis but behind on a weekly basis"
    else:
        pacing_direction = "behind on a daily basis but ahead on a weekly basis"
    p_rate = partner_sd.get("partner_rate", 0)
    sd_rate = partner_sd.get("sd_rate", 0)
    p_acv = partner_sd.get("partner_acv", 0)
    p_cnt = partner_sd.get("partner_count", 0)
    sd_acv_val = partner_sd.get("sd_acv", 0)
    sd_cnt = partner_sd.get("sd_count", 0)
    unassisted_acv = partner_sd.get("unassisted_acv", 0)
    unassisted_cnt = partner_sd.get("unassisted_count", 0)
    unassisted_rate = (unassisted_acv / partner_sd.get("total_acv", 1) * 100) if partner_sd.get("total_acv") else 0
    # CC CLI adoption for partner/PS-attached accounts (pre-computed in query)
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

    # Pipeline movement variables (7-day)
    _pm_empty = {"count": 0, "acv": 0}
    pm_won_to_imp = pm.get("won_to_imp", _pm_empty)
    pm_won_to_lost = pm.get("won_to_lost", _pm_empty)
    pm_pushed_out = pm.get("pushed_out", _pm_empty)
    pm_pulled_in = pm.get("pulled_in", _pm_empty)
    pm_imp_started = pm.get("imp_started", _pm_empty)
    pm_new_pipeline = pm.get("new_pipeline", _pm_empty)

    # Consumption variables
    cq_actual   = theater_consumption.get("cq_actual", 0)
    cq_target   = theater_consumption.get("cq_target", 0)
    cq_forecast = theater_consumption.get("cq_forecast", 0)
    ytd_actual  = theater_consumption.get("ytd_actual", 0)
    ytd_target  = theater_consumption.get("ytd_target", 0)
    ytd_forecast = theater_consumption.get("ytd_forecast", 0)

    # Wins variables
    _wins_forecast = data.get("wins_forecast") or {}
    _wins_qtd = data.get("wins_qtd") or {}
    _wins_open = data.get("wins_open_pipeline") or {}
    _wins_pacing = data.get("wins_pacing") or {}
    _wins_pm = data.get("wins_pipeline_movements") or {}
    w_won_acv = _wins_qtd.get("acv", 0)
    w_won_count = _wins_qtd.get("count", 0)
    w_last7_acv = (data.get("last7_wins") or {}).get("acv", 0)
    w_last7_count = (data.get("last7_wins") or {}).get("count", 0)
    w_open_acv = _wins_forecast.get("open_pipeline", 0)   # authoritative from pipeline targets
    w_open_count = (data.get("wins_open_pipeline") or {}).get("count", 0)  # count only from MDM cache
    w_ml = _wins_forecast.get("most_likely", 0)
    w_commit = _wins_forecast.get("commit", 0)
    w_stretch = _wins_forecast.get("stretch", 0)
    w_target = _wins_forecast.get("target", 0)
    w_won_pct_target = (w_won_acv / w_target * 100) if w_target else 0
    w_ml_pct_target  = (w_ml / w_target * 100) if w_target else 0
    w_won_pct_ml = (w_won_acv / w_ml * 100) if w_ml else 0
    w_coverage = ((w_won_acv + w_open_acv) / w_ml * 100) if w_ml else 0
    wp_day_avg = _wins_pacing.get("day_avg", 0)
    wp_day_pct = _wins_pacing.get("day_pct", 0)
    wp_week_avg = _wins_pacing.get("week_avg", 0)
    wp_week_pct = _wins_pacing.get("week_pct", 0)
    wp_final = _wins_pacing.get("prior_fy_final", 0)
    # Pipeline movements
    w_pushed_out = _wins_pm.get("pushed_out", {"count": 0, "acv": 0})
    w_pulled_in  = _wins_pm.get("pulled_in",  {"count": 0, "acv": 0})
    w_new_pipe   = _wins_pm.get("new_pipeline",{"count": 0, "acv": 0})
    w_day_proj = (w_won_acv / (wp_day_pct / 100)) if wp_day_pct > 0 else None
    w_week_proj = (w_won_acv / (wp_week_pct / 100)) if wp_week_pct > 0 else None
    w_day_ahead = w_won_acv >= wp_day_avg if wp_day_avg else True
    w_week_ahead = w_won_acv >= wp_week_avg if wp_week_avg else True
    w_pacing_dir = (
        "ahead on both daily and weekly basis" if (w_day_ahead and w_week_ahead)
        else "behind on both daily and weekly basis" if (not w_day_ahead and not w_week_ahead)
        else "ahead daily, behind weekly" if w_day_ahead
        else "behind daily, ahead weekly"
    )
    fy_target   = theater_consumption.get("fy_target", 0)
    fy_forecast = theater_consumption.get("fy_forecast", 0)
    cq_pct      = cq_actual   / cq_target   * 100 if cq_target   > 0 else 0
    cq_fcst_pct = cq_forecast / cq_target   * 100 if cq_target   > 0 else 0
    ytd_pct     = ytd_actual  / ytd_target  * 100 if ytd_target  > 0 else 0
    fy_pct      = fy_forecast / fy_target   * 100 if fy_target   > 0 else 0
    fy_label    = CONFIG.get("fiscal_year_label", "FY??")
    fq_label    = safe_str(fiscal.get("FISCAL_QUARTER", ""))

    # --- Cortex AI summary ---
    # Build Cortex risk narrative from high-risk UCs (called after html is built, injected into Risk section)
    def _build_risk_narrative_prompt():
        if not high_risk_ucs:
            return None
        lines = []
        for uc in high_risk_ucs[:8]:
            acv = fmt_ci(safe_float(uc.get("USE_CASE_EACV", 0)))
            acct = safe_str(uc.get("ACCOUNT_NAME", "Unknown"))
            risk_type = safe_str(uc.get("RISK_TYPE", ""))
            risk_text = safe_str(uc.get("RISK_SUMMARY", uc.get("SPECIALIST_COMMENTS", "")))[:120]
            lines.append(f"- {acct} ({acv}): {risk_type} — {risk_text}")
        risk_list = "\n".join(lines)
        return (
            f"You are summarizing risk themes for a PEAK Forecast Call for {_theater()} in {fy_label} {fq_label}. "
            f"Based on these high-risk use cases, write exactly 2-3 sentences identifying the dominant risk patterns "
            f"and what actions are most needed. Be direct, factual, no bullet points.\n\nHigh-risk use cases:\n{risk_list}"
        )

    # Wins risk: use pipeline targets Mature vs Open (avoids GO_LIVE_DATE filter problem)
    w_mature = _wins_forecast.get("mature", 0)
    w_open_total = _wins_forecast.get("open_pipeline", 0)
    w_at_risk = max(0, w_open_total - w_mature)
    w_mature_pct = (w_mature / w_open_total * 100) if w_open_total else 0
    # If we win all mature pipeline, where does that put us?
    w_won_plus_mature = w_won_acv + w_mature
    w_mature_scenario_ml_pct  = (w_won_plus_mature / w_ml * 100) if w_ml else 0
    w_mature_scenario_tgt_pct = (w_won_plus_mature / w_target * 100) if w_target else 0

    # --- Deep wins risk analysis ---
    COMPETITOR_PATTERNS = [
        ("Databricks", ["databricks", "dbx"]),
        ("AWS/Redshift/EMR", ["redshift", "emr", "eks", " aws "]),
        ("GCP/BigQuery", ["bigquery", "bq", " gcp ", "google cloud"]),
        ("Azure/Synapse", ["synapse", "azure", "microsoft fabric", "fabric"]),
        ("Trino/Presto", ["trino", "presto"]),
        ("Druid", ["druid"]),
        ("Teradata", ["teradata"]),
        ("Oracle", ["oracle"]),
        ("Spark/Hadoop", ["spark", "hadoop", "hive"]),
    ]
    PRODUCT_PATTERNS = [
        ("Performance/Latency", ["performance", "latency", "slow", "faster", "speed"]),
        ("Cost/Pricing", ["cost", "pricing", "expensive", "price", "cheaper"]),
        ("Feature Gap", ["missing", "limitation", "gap", "can't", "cannot", "doesn't support", "not supported"]),
        ("Iceberg/Open Format", ["iceberg", "open format", "open table"]),
        ("Stability/Reliability", ["stability", "stable", "reliability", "outage", "downtime", "issues"]),
        ("Product POC/Evaluation", ["poc", "proof of concept", "evaluation", "testing", "benchmark"]),
    ]

    def compute_wins_risk_themes(rows):
        if not rows:
            return {}
        # Risk category breakdown
        risk_buckets = {}
        for r in rows:
            risk_str = safe_str(r.get("USE_CASE_RISK", "") or "")
            acv = safe_float(r.get("USE_CASE_EACV", 0) or 0)
            if not risk_str or risk_str.upper() in ("NONE", ""):
                continue
            for tag in [t.strip() for t in risk_str.split(";")]:
                if tag:
                    if tag not in risk_buckets:
                        risk_buckets[tag] = {"count": 0, "acv": 0}
                    risk_buckets[tag]["count"] += 1
                    risk_buckets[tag]["acv"] += acv

        # Competitor and product pattern extraction
        competitor_hits = {}
        product_hits = {}
        for r in rows:
            text = " ".join([
                safe_str(r.get("RISK_DESCRIPTION", "") or ""),
                safe_str(r.get("SPECIALIST_COMMENTS", "") or ""),
            ]).lower()
            acv = safe_float(r.get("USE_CASE_EACV", 0) or 0)
            for label, keywords in COMPETITOR_PATTERNS:
                if any(kw in text for kw in keywords):
                    if label not in competitor_hits:
                        competitor_hits[label] = {"count": 0, "acv": 0}
                    competitor_hits[label]["count"] += 1
                    competitor_hits[label]["acv"] += acv
            for label, keywords in PRODUCT_PATTERNS:
                if any(kw in text for kw in keywords):
                    if label not in product_hits:
                        product_hits[label] = {"count": 0, "acv": 0}
                    product_hits[label]["count"] += 1
                    product_hits[label]["acv"] += acv

        # Top risky UCs for context (prefer those with RISK_DESCRIPTION)
        top_risky = sorted(
            [r for r in rows if r.get("RISK_DESCRIPTION") or (r.get("USE_CASE_RISK") and safe_str(r.get("USE_CASE_RISK", "")).upper() not in ("NONE", ""))],
            key=lambda x: safe_float(x.get("USE_CASE_EACV", 0) or 0), reverse=True
        )[:10]

        return {
            "risk_buckets": risk_buckets,
            "competitor_hits": competitor_hits,
            "product_hits": product_hits,
            "top_risky": top_risky,
        }

    _wins_risk_rows = data.get("wins_risk_analysis") or []
    _risk_themes = compute_wins_risk_themes(_wins_risk_rows)

    def _build_wins_risk_prompt():
        if not _risk_themes:
            return None
        # Build structured stats block
        lines = [f"Deep analysis of {len(_wins_risk_rows)} open wins pipeline UCs for {_theater()} in {fy_label} {fq_label}.", ""]
        # Risk category breakdown
        rb = _risk_themes.get("risk_buckets", {})
        if rb:
            lines.append("RISK CATEGORY BREAKDOWN (by ACV):")
            for tag, d in sorted(rb.items(), key=lambda x: -x[1]["acv"]):
                lines.append(f"  {tag}: {d['count']} UCs, ${d['acv']/1e6:.1f}M")
            lines.append("")
        # Competitor breakdown
        ch = _risk_themes.get("competitor_hits", {})
        if ch:
            lines.append("COMPETITOR MENTIONS (across all text fields):")
            for comp, d in sorted(ch.items(), key=lambda x: -x[1]["acv"]):
                lines.append(f"  {comp}: {d['count']} UCs, ${d['acv']/1e6:.1f}M ACV mentioned")
            lines.append("")
        # Product/performance breakdown
        ph = _risk_themes.get("product_hits", {})
        if ph:
            lines.append("PRODUCT/PERFORMANCE PATTERNS:")
            for issue, d in sorted(ph.items(), key=lambda x: -x[1]["acv"]):
                lines.append(f"  {issue}: {d['count']} UCs, ${d['acv']/1e6:.1f}M ACV affected")
            lines.append("")
        # Top risky UCs with context
        top = _risk_themes.get("top_risky", [])
        if top:
            lines.append("TOP RISK-FLAGGED UCS (for context, do NOT just list these):")
            for uc in top[:8]:
                acv = f"${safe_float(uc.get('USE_CASE_EACV',0))/1e6:.1f}M"
                acct = safe_str(uc.get("ACCOUNT_NAME",""))
                risk = safe_str(uc.get("USE_CASE_RISK",""))
                desc = safe_str(uc.get("RISK_DESCRIPTION","") or uc.get("SPECIALIST_COMMENTS",""))[:200]
                lines.append(f"  {acct} ({acv}, {risk}): {desc}")
        lines.append("")
        lines.append(
            "Write 3-4 sentences for a PEAK Forecast Call risk summary:\n"
            "(1) Which competitor(s) are appearing most frequently and in what context — is this a pattern or isolated?\n"
            "(2) Are there recurring product or performance issues that could be blocking wins?\n"
            "(3) Any systemic risk trend worth flagging to ELT (e.g., a specific competitor making gains in a workload, a performance issue blocking multiple deals).\n"
            "Focus on PATTERNS not individual companies. Only mention a specific account if the same issue appears at 3+ accounts. "
            "Do NOT base escalation on company size or deal stage — those are already accounted for. Be direct, no bullet points."
        )
        return "\n".join(lines)

    with st.spinner("Generating wins risk summary..."):
        _wins_risk_narrative = _generate_script_summary(_build_wins_risk_prompt())
    w_shortfall = max(0, w_ml - w_won_acv - w_open_acv)
    wins_forecast = _wins_forecast  # alias for use in f-string

    # Pre-compute risk stat lines (no backslashes inside f-string allowed)
    _comp_hits = _risk_themes.get("competitor_hits", {})
    _prod_hits = _risk_themes.get("product_hits", {})
    _comp_line = ""
    _prod_line = ""
    if _comp_hits:
        parts = [f"{c}: {d['count']} UCs / ${d['acv']/1e6:.0f}M"
                 for c, d in sorted(_comp_hits.items(), key=lambda x: -x[1]["acv"])[:4]]
        _comp_line = (
            '<p style="font-size:0.9em; color:#555; margin:4px 0;">'
            '<strong>Competitive risk:</strong> ' + " | ".join(parts) + '</p>'
        )
    if _prod_hits:
        parts = [f"{i}: {d['count']} UCs / ${d['acv']/1e6:.0f}M"
                 for i, d in sorted(_prod_hits.items(), key=lambda x: -x[1]["acv"])[:4]]
        _prod_line = (
            '<p style="font-size:0.9em; color:#555; margin:4px 0;">'
            '<strong>Product/performance themes:</strong> ' + " | ".join(parts) + '</p>'
        )
    _wins_narrative_html = (
        f'<div class="analysis"><strong>Pipeline Risk Analysis (AI):</strong> '
        f'{html_escape(_wins_risk_narrative)}</div>'
        if _wins_risk_narrative else ""
    )
    # WoW phrasing for wins ML
    _ml_delta = wins_forecast.get("ml_delta")
    if _ml_delta is None:
        w_wow_phrase = ""
    elif _ml_delta == 0:
        w_wow_phrase = ", which is flat WoW,"
    elif _ml_delta > 0:
        w_wow_phrase = f", which is up {fmt_ci(_ml_delta)} WoW,"
    else:
        w_wow_phrase = f", which is down {fmt_ci(abs(_ml_delta))} WoW,"

    script_html = f"""
<div style="background: #f8f9fa; border: 1px solid #dee2e6; border-radius: 8px; padding: 24px; font-family: Georgia, serif; font-size: 1.05em; line-height: 1.7;">

<h2 style="color: #1a1a2e; border-bottom: 3px solid #28a745; padding-bottom: 8px; margin-bottom: 16px;">Use Case Wins</h2>
<h3 style="color: #333; border-bottom: 2px solid #28a745; padding-bottom: 8px;">Forecast Call</h3>
<p>For Use Case Wins, our Most Likely call is <strong>{fmt_ci(w_ml)}</strong>{w_wow_phrase} against our {fiscal["FISCAL_QUARTER"]} target of <strong>{fmt_ci(w_target)}</strong>.
This will bring us to <strong>{w_ml_pct_target:.0f}%</strong> of target.
QTD we have won <strong>{fmt_ci(w_won_acv)}</strong> ({w_won_count:,} use cases) — which is <strong>{w_won_pct_ml:.0f}%</strong> of our Most Likely call{f" and <strong>{w_won_pct_target:.0f}%</strong> of target" if w_target else ""}.
Our Commit is <strong>{fmt_ci(w_commit)}</strong> and Best Case is <strong>{fmt_ci(w_stretch)}</strong>.</p>
<h3 style="color: #333; border-bottom: 2px solid #28a745; padding-bottom: 8px;">Pacing</h3>
<p>Wins are pacing <strong>{w_pacing_dir}</strong>.
On Day <strong>{day_number}</strong> of the quarter, won ACV of <strong>{fmt_ci(w_won_acv)}</strong>
compares to a prior FY average of <strong>{fmt_ci(wp_day_avg)}</strong> at this point which was <strong>{wp_day_pct:.0f}%</strong> of prior {fiscal["FISCAL_QUARTER"]} final of <strong>{fmt_ci(wp_final)}</strong>.
On a Weekly basis (Week <strong>{week_number}</strong>): prior FY average was <strong>{fmt_ci(wp_week_avg)}</strong> which was <strong>{wp_week_pct:.0f}%</strong> of prior {fiscal["FISCAL_QUARTER"]} final.
So if we project these out through the end of the quarter we are Projected <strong>{fmt_ci(w_day_proj)}</strong> on the daily pacing and <strong>{fmt_ci(w_week_proj)}</strong> on the weekly pacing.</p>
<h3 style="color: #333; border-bottom: 2px solid #28a745; padding-bottom: 8px;">Pipeline (Last 7 Days)</h3>
<p>In the last 7 days, <strong>{w_pushed_out["count"]}</strong> use cases (<strong>{fmt_ci(w_pushed_out["acv"])}</strong>) were pushed out of the quarter based on decision date,
while <strong>{w_pulled_in["count"]}</strong> (<strong>{fmt_ci(w_pulled_in["acv"])}</strong>) were pulled in.
In the last 7 days, <strong>{w_last7_count}</strong> use cases for <strong>{fmt_ci(w_last7_acv)}</strong> were won.
<strong>{w_new_pipe["count"]}</strong> new use cases (<strong>{fmt_ci(w_new_pipe["acv"])}</strong>) were created with a decision date in {fiscal["FISCAL_QUARTER"]}.
Open wins pipeline stands at <strong>{fmt_ci(w_open_acv)}</strong> ({w_open_count:,} use cases) — Won+Open coverage is <strong>{fmt_pi(w_coverage)}</strong> vs Most Likely.
{f"We have a shortfall of <strong>{fmt_ci(w_shortfall)}</strong> between Won+Open and Most Likely." if w_shortfall > 1e6 else "Won+Open pipeline covers the Most Likely forecast."}</p>
<h3 style="color: #333; border-bottom: 2px solid #28a745; padding-bottom: 8px;">Risk</h3>
<p>Of the <strong>{fmt_ci(w_open_total)}</strong> in open wins pipeline, <strong>{fmt_ci(w_mature)}</strong> ({w_mature_pct:.0f}%) is mature/high-conviction pipeline,
leaving <strong>{fmt_ci(w_at_risk)}</strong> as less mature pipeline at risk of not converting this quarter.
If we win all of the mature pipeline, Won+Mature would be <strong>{fmt_ci(w_won_plus_mature)}</strong> — <strong>{w_mature_scenario_ml_pct:.0f}%</strong> of Most Likely and <strong>{w_mature_scenario_tgt_pct:.0f}%</strong> of target.
{f"We still have a shortfall of <strong>{fmt_ci(w_shortfall)}</strong> between Won+Open and Most Likely — additional pipeline needs to be created or accelerated." if w_shortfall > 1e6 else "Won+Open pipeline covers the Most Likely forecast."}</p>
{_comp_line}
{_prod_line}
{_wins_narrative_html}

<h2 style="color: #1a1a2e; border-bottom: 3px solid #007bff; padding-bottom: 8px; margin: 28px 0 16px 0;">Go-Lives &amp; Consumption</h2>
<h3 style="color: #333; border-bottom: 2px solid #007bff; padding-bottom: 8px;">Forecast Call</h3>
<p>For Go-Lives, our Most Likely call is <strong>{fmt_ci(most_likely)}</strong>{ml_wow_text}.
This will bring us to <strong>{fmt_pi(ml_pct_of_target)}</strong> of {fiscal["FISCAL_QUARTER"]} Target.
QTD we have deployed <strong>{fmt_ci(deployed["acv"])}</strong> against a target of <strong>{fmt_ci(gl_target)}</strong> which is <strong>{fmt_pi(deployed_pct_of_target)}</strong> of our target and <strong>{fmt_pi(deployed_pct)}</strong> of our Most Likely Call.
In the last 7 days, <strong>{last7["count"]}</strong> use cases for <strong>{fmt_ci(last7["acv"])}</strong> went live.
Our open pipeline is <strong>{fmt_ci(pipeline["acv"])}</strong>, giving us <strong>{fmt_pi(coverage_pct)}</strong> ML coverage.</p>
<h3 style="color: #333; border-bottom: 2px solid #007bff; padding-bottom: 8px;">Pacing</h3>
<p>Go-lives are pacing <strong>{pacing_direction}</strong>.
On Day <strong>{day_number}</strong> of the quarter, our current deployed ACV of <strong>{fmt_ci(deployed["acv"])}</strong>
compares to a prior {fiscal["FISCAL_QUARTER"]} average of <strong>{fmt_ci(day_avg)}</strong> deployed by this day which was <strong>{fmt_pi(day_pct_val)}</strong> of the prior FY average final of <strong>{fmt_ci(CONFIG["prior_fy_avg_final"] * 1e6)}</strong>.
On a weekly basis (Week <strong>{week_number}</strong>), the prior FY average deployed was <strong>{fmt_ci(week_avg)}</strong> which was <strong>{fmt_pi(week_pct_val)}</strong> of final.
If we continue to follow this pacing, our projected quarter-end deployed ACV would be{f" <strong>{fmt_ci(day_projection)}</strong> based on daily pacing" if day_projection else " unavailable (no prior FY daily data)"}{f" and <strong>{fmt_ci(week_projection)}</strong> based on weekly pacing" if week_projection else ""}.</p>
<h3 style="color: #333; border-bottom: 2px solid #007bff; padding-bottom: 8px;">Pipeline (Last 7 Days)</h3>
<p>In the last 7 days, <strong>{pm_pushed_out["count"]}</strong> use cases (<strong>{fmt_ci(pm_pushed_out["acv"])}</strong>) were pushed out of the quarter
while <strong>{pm_pulled_in["count"]}</strong> (<strong>{fmt_ci(pm_pulled_in["acv"])}</strong>) were pulled in.
<strong>{pm_imp_started["count"]}</strong> use cases (<strong>{fmt_ci(pm_imp_started["acv"])}</strong>) started implementation with a go-live this quarter.
In the last 7 days, <strong>{pm_new_pipeline["count"]}</strong> use cases were created with a go-live date in {fiscal["FISCAL_QUARTER"]} for <strong>{fmt_ci(pm_new_pipeline["acv"])}</strong>.</p>
<h3 style="color: #333; border-bottom: 2px solid #007bff; padding-bottom: 8px;">Consumption</h3>
<p>QTD consumption for {fy_label} {fq_label} is <strong>{fmt_ci(cq_actual)}</strong>, which is <strong>{cq_pct:.0f}%</strong> of our quarterly target of <strong>{fmt_ci(cq_target)}</strong>.
Our team forecast call for the quarter is <strong>{fmt_ci(cq_forecast)}</strong>, projecting <strong>{cq_fcst_pct:.0f}%</strong> attainment of target.
Year to date, we have consumed <strong>{fmt_ci(ytd_actual)}</strong> against a YTD target of <strong>{fmt_ci(ytd_target)}</strong> (<strong>{ytd_pct:.0f}%</strong> attainment).
Our {fy_label} full-year forecast of <strong>{fmt_ci(fy_forecast)}</strong> represents <strong>{fy_pct:.0f}%</strong> of the full-year target of <strong>{fmt_ci(fy_target)}</strong>.</p>
<h3 style="color: #333; border-bottom: 2px solid #007bff; padding-bottom: 8px;">Risk</h3>
<p>Total pipeline risk stands at <strong>{fmt_ci(total_risk_acv)}</strong>, leaving
<strong>{fmt_ci(total_good)}</strong> in good pipeline for <strong>{fmt_pi(good_coverage)}</strong> good coverage vs Most Likely.
We are currently at <strong>{fmt_pi(deployed_pct_of_target)}</strong> of our go-live target.</p>
{high_risk_table_html}
"""
    script_html += f"""
<h3 style="color: #333; border-bottom: 2px solid #007bff; padding-bottom: 8px;">Partner and SD Attach</h3>
<p>Partner attach rate on the open pipeline is <strong>{fmt_pi(p_rate)}</strong>
({p_cnt} use cases, {fmt_ci(p_acv)} ACV).
SD attach rate is <strong>{fmt_pi(sd_rate)}</strong>
({sd_cnt} use cases, {fmt_ci(sd_acv_val)} ACV).
The remaining <strong>{unassisted_cnt}</strong> use cases ({fmt_ci(unassisted_acv)} ACV, {fmt_pi(unassisted_rate)}) are unassisted (Customer Only, Unknown, or None).
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
</div>"""
    st.html(STREAMLIT_CSS + script_html)

    # Cortex risk narrative (rendered as native Streamlit after the HTML)
    _risk_prompt = _build_risk_narrative_prompt()
    if _risk_prompt:
        with st.spinner("Generating risk summary..."):
            _risk_narrative = _generate_script_summary(_risk_prompt)
        if _risk_narrative:
            st.markdown(
                f'<div style="background: #fff5f5; border-left: 4px solid #dc3545; border-radius: 6px; '
                f'padding: 14px 18px; margin: -8px 0 16px 0; font-family: Georgia, serif; font-size: 1em; line-height: 1.6;">'
                f'<p style="margin: 0 0 4px 0; font-size: 0.75em; font-weight: bold; color: #c0392b; '
                f'text-transform: uppercase; letter-spacing: 0.05em;">AI Risk Summary</p>'
                f'<p style="margin: 0;">{html_escape(_risk_narrative)}</p>'
                f'</div>',
                unsafe_allow_html=True,
            )




def render_wins_tab(data):
    """Render the Use Case Wins tab — QTD wins, forecast calls, open pipeline, top UCs."""
    fiscal = data["fiscal"]
    wins_qtd = data.get("wins_qtd") or {}
    last7_wins = data.get("last7_wins") or {}
    wins_forecast = data.get("wins_forecast") or {}
    wins_open = data.get("wins_open_pipeline") or {}
    wins_top5 = data.get("wins_top5") or []
    wins_pacing = data.get("wins_pacing") or {}
    cfg = data["_config"]

    quarter = safe_str(fiscal["FISCAL_QUARTER"])
    day_number = safe_int(fiscal["DAY_NUMBER"])
    week_number = safe_int(fiscal["WEEK_NUMBER"])
    fy_label = cfg.get("fiscal_year_label", "FY??")

    won_acv = wins_qtd.get("acv", 0)
    won_count = wins_qtd.get("count", 0)
    last7_acv = last7_wins.get("acv", 0)
    last7_count = last7_wins.get("count", 0)
    open_acv = wins_forecast.get("open_pipeline", 0)   # authoritative from pipeline targets
    open_count = wins_open.get("count", 0)

    w_commit = wins_forecast.get("commit", 0)
    w_ml = wins_forecast.get("most_likely", 0)
    w_stretch = wins_forecast.get("stretch", 0)
    w_won_snap = wins_forecast.get("won_actual", 0)   # pipeline targets snapshot
    w_open_snap = wins_forecast.get("open_pipeline", 0)

    won_pct_ml = (won_acv / w_ml * 100) if w_ml else 0
    coverage_pct = ((won_acv + open_acv) / w_ml * 100) if w_ml else 0

    w_commit_delta = wins_forecast.get("commit_delta")
    w_ml_delta = wins_forecast.get("ml_delta")
    w_stretch_delta = wins_forecast.get("stretch_delta")

    wp_day_avg = wins_pacing.get("day_avg", 0)
    wp_day_pct = wins_pacing.get("day_pct", 0)
    wp_week_avg = wins_pacing.get("week_avg", 0)
    wp_week_pct = wins_pacing.get("week_pct", 0)
    wp_final = wins_pacing.get("prior_fy_final", 0)

    day_proj = (won_acv / (wp_day_pct / 100)) if wp_day_pct > 0 else None
    week_proj = (won_acv / (wp_week_pct / 100)) if wp_week_pct > 0 else None
    day_ahead = won_acv >= wp_day_avg if wp_day_avg else True
    week_ahead = won_acv >= wp_week_avg if wp_week_avg else True

    gl_total = won_acv + open_acv
    won_bar_pct = int(won_acv / gl_total * 100) if gl_total else 0
    open_bar_pct = 100 - won_bar_pct

    pacing_label = (
        "ahead on both daily and weekly basis" if (day_ahead and week_ahead)
        else "behind on both daily and weekly basis" if (not day_ahead and not week_ahead)
        else "ahead daily, behind weekly" if day_ahead
        else "behind daily, ahead weekly"
    )

    wins_html = f"""
<div style="background: #f8f9fa; border: 1px solid #dee2e6; border-radius: 8px; padding: 24px; font-family: -apple-system, sans-serif;">

<h2 style="color: #1a1a2e; margin: 0 0 20px 0;">Use Case Wins — {fy_label} {quarter}</h2>

<!-- Summary metrics row -->
<div style="display: grid; grid-template-columns: repeat(4, 1fr); gap: 16px; margin-bottom: 24px;">
  <div style="background: white; border-radius: 8px; padding: 16px; box-shadow: 0 2px 6px rgba(0,0,0,0.08); border-top: 4px solid #28a745;">
    <div style="font-size: 0.75em; color: #666; text-transform: uppercase; letter-spacing: 0.5px;">Won QTD</div>
    <div style="font-size: 1.8em; font-weight: bold; color: #1a1a2e;">{fmt_currency(won_acv)}</div>
    <div style="font-size: 0.85em; color: #666;">{won_count:,} use cases</div>
  </div>
  <div style="background: white; border-radius: 8px; padding: 16px; box-shadow: 0 2px 6px rgba(0,0,0,0.08); border-top: 4px solid #007bff;">
    <div style="font-size: 0.75em; color: #666; text-transform: uppercase; letter-spacing: 0.5px;">% of ML Forecast</div>
    <div style="font-size: 1.8em; font-weight: bold; color: #{'28a745' if won_pct_ml >= 50 else '1a1a2e'};">{won_pct_ml:.0f}%</div>
    <div style="font-size: 0.85em; color: #666;">Day {day_number} of {92 if quarter == 'Q2' else 91}</div>
  </div>
  <div style="background: white; border-radius: 8px; padding: 16px; box-shadow: 0 2px 6px rgba(0,0,0,0.08); border-top: 4px solid #ffc107;">
    <div style="font-size: 0.75em; color: #666; text-transform: uppercase; letter-spacing: 0.5px;">Last 7 Days</div>
    <div style="font-size: 1.8em; font-weight: bold; color: #1a1a2e;">{fmt_currency(last7_acv)}</div>
    <div style="font-size: 0.85em; color: #666;">{last7_count:,} use cases</div>
  </div>
  <div style="background: white; border-radius: 8px; padding: 16px; box-shadow: 0 2px 6px rgba(0,0,0,0.08); border-top: 4px solid #17a2b8;">
    <div style="font-size: 0.75em; color: #666; text-transform: uppercase; letter-spacing: 0.5px;">Won + Open Coverage</div>
    <div style="font-size: 1.8em; font-weight: bold; color: #{'28a745' if coverage_pct >= 100 else '1a1a2e'};">{coverage_pct:.0f}%</div>
    <div style="font-size: 0.85em; color: #666;">vs ML Forecast</div>
  </div>
</div>

<!-- Forecast calls table -->
<h3 style="color: #333; border-bottom: 2px solid #007bff; padding-bottom: 8px;">Forecast Calls</h3>
<table class="forecast-table">
  <tr><th>Scenario</th><th>Amount</th><th>WoW</th></tr>
  <tr><td><span class="status-commit">Commit</span></td><td>{fmt_currency(w_commit)}</td><td>{fmt_delta_html(w_commit_delta)}</td></tr>
  <tr><td><span class="status-likely">Most Likely</span></td><td>{fmt_currency(w_ml)}</td><td>{fmt_delta_html(w_ml_delta)}</td></tr>
  <tr><td><span class="status-stretch">Best Case</span></td><td>{fmt_currency(w_stretch)}</td><td>{fmt_delta_html(w_stretch_delta)}</td></tr>
</table>

<!-- Won vs Open pipeline -->
<h3 style="color: #333; border-bottom: 2px solid #007bff; padding-bottom: 8px; margin-top: 24px;">Pipeline Status</h3>
<div style="background: white; border-radius: 8px; padding: 20px; box-shadow: 0 2px 6px rgba(0,0,0,0.08); margin-bottom: 20px;">
  <div style="display: flex; justify-content: space-between; margin-bottom: 8px;">
    <span><strong style="color: #28a745;">Won QTD:</strong> {fmt_currency(won_acv)} ({won_count:,} UCs)</span>
    <span><strong style="color: #007bff;">Open Pipeline:</strong> {fmt_currency(open_acv)} ({open_count:,} UCs)</span>
  </div>
  <div style="height: 24px; border-radius: 12px; overflow: hidden; background: #e9ecef;">
    <div style="height: 100%; width: {won_bar_pct}%; background: #28a745; display: inline-block; border-radius: 12px 0 0 12px;"></div>
    <div style="height: 100%; width: {open_bar_pct}%; background: #007bff; display: inline-block;"></div>
  </div>
  <div style="font-size: 0.85em; color: #666; margin-top: 8px;">
    Won+Open Total: {fmt_currency(gl_total)} &nbsp;|&nbsp; ML Forecast: {fmt_currency(w_ml)} &nbsp;|&nbsp; Coverage: {coverage_pct:.0f}%
  </div>
</div>

<!-- Pacing -->
<h3 style="color: #333; border-bottom: 2px solid #007bff; padding-bottom: 8px;">Pacing vs Prior FY</h3>
<div class="analysis">
Wins are pacing <strong>{pacing_label}</strong>.
On Day <strong>{day_number}</strong>, won ACV of <strong>{fmt_currency(won_acv)}</strong>
compares to prior FY average of <strong>{fmt_currency(wp_day_avg)}</strong> ({wp_day_pct:.0f}% of final avg {fmt_currency(wp_final)}).
Weekly (Wk {week_number}): prior FY avg = <strong>{fmt_currency(wp_week_avg)}</strong> ({wp_week_pct:.0f}% of final).
Projected quarter-end: {f'<strong>{fmt_currency(day_proj)}</strong> (daily) / <strong>{fmt_currency(week_proj)}</strong> (weekly)' if day_proj else 'N/A'}.
</div>
</div>"""

    st.html(STREAMLIT_CSS + wins_html)

    # Top open-pipeline use cases
    if wins_top5:
        st.markdown(f"#### Top Open-Pipeline Use Cases (Wins)")
        rows = []
        for uc in wins_top5:
            rows.append({
                "Account": safe_str(uc.get("ACCOUNT_NAME", "")),
                "Use Case": safe_str(uc.get("USE_CASE_NAME", "")),
                "ACV": fmt_currency(safe_float(uc.get("USE_CASE_EACV", 0))),
                "Stage": safe_str(uc.get("USE_CASE_STAGE", "")),
                "Days in Stage": safe_int(uc.get("DAYS_IN_STAGE", 0)),
                "Region": safe_str(uc.get("REGION_NAME", "")),
                "Risk": safe_str(uc.get("USE_CASE_RISK", "")),
            })
        st.dataframe(pd.DataFrame(rows), use_container_width=True, hide_index=True)


def render_golives_tab(data):
    fiscal = data["fiscal"]
    revenue = data["revenue"]
    forecasts = data["forecasts"]
    deployed = data["deployed"]
    last7 = data["last7"]
    pipeline = data["pipeline"]
    risk_analysis = data["risk_analysis"]
    top5 = data["top5"]
    consumption = data["consumption"]
    pacing = data["pacing"]
    cfg = data["_config"]
    coco_tiers = data.get("coco_tiers", {})
    coco_movement = data.get("coco_movement", {})
    coco_insights = data.get("coco_insights", {})
    coco_skill_uc = data.get("coco_skill_uc", {})
    uc_story = data.get("uc_story", {})
    cc_by_account = data.get("cc_by_account", {})

    quarter = safe_str(fiscal["FISCAL_QUARTER"])
    day_number = safe_int(fiscal["DAY_NUMBER"])
    week_number = safe_int(fiscal["WEEK_NUMBER"])
    most_likely = forecasts["most_likely"]
    gl_target = forecasts["target"]
    gl_commit_delta = fmt_delta_html(forecasts.get("commit_delta"))
    gl_ml_delta = fmt_delta_html(forecasts.get("ml_delta"))
    gl_stretch_delta = fmt_delta_html(forecasts.get("stretch_delta"))
    deployed_pct = (deployed["acv"] / most_likely * 100) if most_likely else 0
    last7_pct = (last7["acv"] / most_likely * 100) if most_likely else 0
    deployed_pct_of_target = (deployed["acv"] / gl_target * 100) if gl_target else 0
    coverage_pct = ((pipeline["acv"] + deployed["acv"]) / most_likely * 100) if most_likely else 0
    gl_total_pipeline = deployed["acv"] + pipeline["acv"]
    gl_deployed_pct = (deployed["acv"] / gl_total_pipeline * 100) if gl_total_pipeline else 0
    gl_open_pct = (pipeline["acv"] / gl_total_pipeline * 100) if gl_total_pipeline else 0
    total_good = sum(safe_float(r.get("GOOD_ACV", 0) or 0) for r in risk_analysis.values())
    good_coverage = ((total_good + deployed["acv"]) / most_likely * 100) if most_likely else 0
    top5_rows = "\n".join(
        build_use_case_row(uc, consumption, cc_by_account=cc_by_account,
                           skill_match=coco_skill_uc.get(safe_str(uc.get("USE_CASE_ID", "")), {}),
                           engagement=uc_story.get(safe_str(uc.get("USE_CASE_ID", ""))))
        for uc in top5
    )
    day_avg = fmt_currency(pacing["day_avg"])
    day_pct = fmt_pct(pacing["day_pct"])
    week_avg = fmt_currency(pacing["week_avg"])
    week_pct = fmt_pct(pacing["week_pct"])
    fy_label = cfg.get("fiscal_year_label", "FY27")
    prior_fy_label = cfg.get("prior_fy_label", "FY26")
    # ---- CoCo Adoption (theater level) — replaces the old Cortex Code CLI Usage
    # table, which read the CC_USAGE_CACHE that stopped refreshing 2026-07-26.
    _ct = coco_tiers or {}
    _cm = coco_movement or {}
    _ci = coco_insights or {}
    coco_error = safe_str(_ct.get("error", "")) or safe_str(_ci.get("error", ""))
    coco_capacity = safe_int(_ct.get("capacity", 0))
    coco_as_of = safe_str(_ct.get("as_of", ""))

    _tier_colors = {"Zero Usage": "#dc3545", "Exploring": "#fd7e14",
                    "Activated": "#007bff", "Expanded": "#17a2b8", "Deep": "#28a745"}
    _tier_cells = []
    for _tier in COCO_TIER_ORDER:
        _n = safe_int(_ct.get(_tier, 0))
        _pct = (_n / coco_capacity * 100) if coco_capacity else 0
        _mv = _cm.get(_tier, {})
        _in, _out = safe_int(_mv.get("in", 0)), safe_int(_mv.get("out", 0))
        _net = _in - _out
        _net_color = "#28a745" if _net > 0 else ("#dc3545" if _net < 0 else "#666")
        # For Zero Usage a NEGATIVE net is good (accounts leaving zero usage).
        if _tier == "Zero Usage":
            _net_color = "#28a745" if _net < 0 else ("#dc3545" if _net > 0 else "#666")
        _tier_cells.append(f"""
  <td style="text-align:center;padding:10px 12px;border:1px solid #ddd;background:white;">
    <div style="font-size:0.78em;font-weight:600;color:{_tier_colors[_tier]};text-transform:uppercase;letter-spacing:0.04em;">{_tier}</div>
    <div style="font-size:1.7em;font-weight:bold;color:#1a1a2e;">{_n:,}</div>
    <div style="font-size:0.78em;color:#666;">{_pct:.1f}% of book</div>
    <div style="font-size:0.78em;margin-top:4px;">
      <span style="color:#28a745;">&#9650;{_in}</span> &nbsp;
      <span style="color:#dc3545;">&#9660;{_out}</span> &nbsp;
      <span style="color:{_net_color};font-weight:600;">net {_net:+d}</span>
    </div>
  </td>""")
    coco_tier_cells = "".join(_tier_cells)

    _ss_now = safe_int(_ct.get("setsail_l28", 0))
    _ss_prior = safe_int(_ct.get("setsail_prior", 0))
    _ss_delta = _ss_now - _ss_prior
    coco_setsail_html = (
        f'<span><strong>CoCo Set Sail (L28):</strong> {_ss_now:,} accounts '
        f'<span style="color:{"#28a745" if _ss_delta >= 0 else "#dc3545"};">'
        f'({_ss_delta:+d} vs prior L28: {_ss_prior:,})</span></span>'
    )

    # Top-level insights for accounts with go-lives in the filtered quarter.
    coco_insight_html = ""
    if _ci and not _ci.get("error"):
        coco_insight_html = f"""
<div style="background:#f0f8ff;border-left:4px solid #29B5E8;border-radius:6px;padding:12px 16px;margin:10px 0 4px 0;font-size:0.92em;">
  <p style="margin:0 0 6px 0;font-size:0.75em;font-weight:bold;color:#0b6d94;text-transform:uppercase;letter-spacing:0.05em;">CoCo in this quarter's go-live accounts</p>
  <p style="margin:0;">Of the <strong>{_ci.get("golive_accounts", 0):,}</strong> accounts with go-lives in {quarter},
  <strong>{_ci.get("accts_with_coco", 0):,}</strong> (<strong>{_ci.get("accts_with_coco_pct", 0)}%</strong>) have some CoCo usage and
  <strong>{_ci.get("accts_activated_plus", 0):,}</strong> (<strong>{_ci.get("accts_activated_plus_pct", 0)}%</strong>) are Activated or better.
  <strong>{_ci.get("ucs_with_skill_match", 0):,}</strong> of <strong>{_ci.get("golive_ucs", 0):,}</strong> go-live use cases
  (<strong>{_ci.get("ucs_matched_pct", 0)}%</strong>) have a CoCo skill match.</p>
</div>"""

    html_content = f"""
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
  <tr><th style="white-space: nowrap;">Period</th><th>{prior_fy_label} Average</th><th>{prior_fy_label} % of Final</th><th>{fy_label} {quarter}</th><th>% of Most Likely</th><th>% of Target</th></tr>
  <tr><td style="white-space: nowrap;"><strong>Day {day_number}</strong></td><td>{day_avg}</td><td>{day_pct}</td><td>{fmt_currency(deployed["acv"])}</td><td>{fmt_pct(deployed_pct)}</td><td>{fmt_pct(deployed_pct_of_target)}</td></tr>
  <tr><td style="white-space: nowrap;"><strong>Week {week_number}</strong></td><td>{week_avg}</td><td>{week_pct}</td><td>{fmt_currency(deployed["acv"])}</td><td>{fmt_pct(deployed_pct)}</td><td>{fmt_pct(deployed_pct_of_target)}</td></tr>
</table>
<p style="font-size: 0.85em; color: #666; margin-top: 5px;"><em>{prior_fy_label} Average Final: ${cfg["prior_fy_avg_final"]}M across 4 quarters | {fy_label} {quarter} Most Likely: {fmt_currency(most_likely)}</em></p>

<h3>CoCo Adoption &mdash; {_theater()} (Theater)</h3>
{"<p style='color:red;'>CoCo Error: " + coco_error + "</p>" if coco_error else ""}
<p style="font-size: 0.85em; color: #666; margin: 0 0 8px 0;">
  {coco_capacity:,} capacity accounts &middot; tiers as of {coco_as_of} &middot;
  &#9650;/&#9660; = accounts moved in / out over the last 7 days. &nbsp; {coco_setsail_html}
</p>
<table style="border-collapse: collapse; margin: 4px 0 6px 0;">
  <tr>{coco_tier_cells}
  </tr>
</table>
<p style="font-size: 0.78em; color: #888; margin: 0 0 4px 0;"><em>{COCO_TIER_DEFINITIONS}</em></p>
{coco_insight_html}

<h3>Top 5 Use Cases Going Live This Quarter</h3>
<table class="use-case-table">
  <tr><th style="width:22%">Account / Use Case</th><th style="width:33%">Details</th><th style="width:45%">Summary</th></tr>
  {top5_rows}
</table>
"""
    st.html(STREAMLIT_CSS + html_content)


def _render_consumption(cons, fiscal):
    """Render the QTD / YTD consumption section at the top of the Forecast Analysis tab."""
    if not cons:
        return

    fy_label = CONFIG.get("fiscal_year_label", "FY??")
    fq = safe_str(fiscal.get("FISCAL_QUARTER", ""))          # e.g. "Q2"
    quarter = f"{fy_label} {fq}" if fq else fy_label

    cq_actual   = cons.get("cq_actual", 0)
    cq_target   = cons.get("cq_target", 0)
    cq_forecast = cons.get("cq_forecast", 0)
    ytd_actual  = cons.get("ytd_actual", 0)
    ytd_target  = cons.get("ytd_target", 0)
    ytd_forecast = cons.get("ytd_forecast", 0)
    fy_target   = cons.get("fy_target", 0)
    fy_forecast = cons.get("fy_forecast", 0)

    cq_pct_target  = cq_actual   / cq_target   * 100 if cq_target   > 0 else 0
    cq_fcst_pct    = cq_forecast / cq_target   * 100 if cq_target   > 0 else 0
    ytd_pct_target = ytd_actual  / ytd_target  * 100 if ytd_target  > 0 else 0
    ytd_fcst_pct   = ytd_forecast / ytd_target * 100 if ytd_target  > 0 else 0
    fy_pct         = fy_forecast / fy_target   * 100 if fy_target   > 0 else 0

    st.markdown(f"### Consumption — {quarter}")

    # ---- Current Quarter ----
    st.caption(f"**Current Quarter ({quarter})**")
    c1, c2, c3, c4 = st.columns(4)
    c1.metric("QTD Consumption",  fmt_currency(cq_actual))
    c2.metric("% of CQ Target",   f"{cq_pct_target:.1f}%",
              help=f"CQ Target: {fmt_currency(cq_target)}")
    c3.metric("CQ Forecast Call", fmt_currency(cq_forecast))
    c4.metric("Forecast vs Target", f"{cq_fcst_pct:.1f}%")

    # ---- YTD ----
    st.caption(f"**Year to Date ({fy_label})**")
    y1, y2, y3, y4 = st.columns(4)
    y1.metric("YTD Actual",        fmt_currency(ytd_actual))
    y2.metric("% of YTD Target",   f"{ytd_pct_target:.1f}%",
              help=f"YTD Target: {fmt_currency(ytd_target)}")
    y3.metric("YTD Forecast",      fmt_currency(ytd_forecast))
    y4.metric("YTD Forecast vs Target", f"{ytd_fcst_pct:.1f}%")

    # ---- FY Outlook row ----
    st.caption(f"**Full Year Outlook ({fy_label})**")
    f1, f2, _, _ = st.columns(4)
    f1.metric("FY Forecast", fmt_currency(fy_forecast))
    f2.metric("vs FY Target", f"{fy_pct:.1f}%",
              help=f"FY Target: {fmt_currency(fy_target)}")

    # ---- Quarterly breakdown ----
    breakdown = cons.get("breakdown", [])
    if breakdown:
        q_num = cons.get("q_num", 0)
        rows = []
        for i, b in enumerate(breakdown):
            qi = i + 1
            tgt = b.get("target", 0)
            val = b.get("value", 0)
            pct = val / tgt * 100 if tgt > 0 else 0
            status = b.get("status", "future")
            label = b.get("label", f"Q{qi}")
            rows.append({
                "Quarter": label,
                "Target": fmt_currency(tgt),
                "Actual / Forecast": fmt_currency(val),
                "% of Target": f"{pct:.1f}%",
                "Status": "Actual" if status == "actual" else ("In Progress" if status == "current" else "Forecast"),
            })
        st.dataframe(
            pd.DataFrame(rows),
            use_container_width=True,
            hide_index=True,
        )

    st.divider()


def _render_retrospective(retro_data, quarter_label):
    """Render the multi-snapshot forecast retrospective for a completed quarter."""
    if not retro_data:
        return

    actual_final = retro_data[0]["actual_final"]

    st.markdown("---")
    st.subheader(f"Forecast Retrospective — {quarter_label}")
    st.caption(
        "Model Most Likely predictions at weekly snapshots vs actual final deployed ACV. "
        "M2 (Pacing) is calibrated from day-31 FY25 history — early-quarter M2 estimates are directional only."
    )

    # --- Summary metrics ---
    day31 = next((s for s in retro_data if s["day"] >= 31), retro_data[0])
    models = {"M1": "m1_err", "M2": "m2_err", "M3": "m3_err", "M4": "m4_err"}
    best_d31 = min(models.items(), key=lambda kv: abs(day31[kv[1]]))
    best_final = next(iter(retro_data[-1:]), {})
    best_overall = min(models.items(), key=lambda kv: sum(abs(s[kv[1]]) for s in retro_data) / len(retro_data))

    cols = st.columns(3)
    with cols[0]:
        st.metric("Actual Final Deployed", f"${actual_final / 1e6:.1f}M")
    with cols[1]:
        err_d31 = day31[best_d31[1]]
        st.metric(f"Best Model @ Day 31", best_d31[0], f"{err_d31:+.1f}% vs actual")
    with cols[2]:
        avg_err = sum(abs(s[best_overall[1]]) for s in retro_data) / len(retro_data)
        st.metric(f"Most Accurate Overall", best_overall[0], f"avg {avg_err:.1f}% abs error")

    # --- Line chart ---
    chart_data = pd.DataFrame({
        "Day": [s["day"] for s in retro_data],
        "M1 Pipeline Risk": [s["m1_ml"] / 1e6 for s in retro_data],
        "M2 Pacing": [s["m2_ml"] / 1e6 for s in retro_data],
        "M3 Conversion": [s["m3_ml"] / 1e6 for s in retro_data],
        "M4 Ensemble": [s["m4_ml"] / 1e6 for s in retro_data],
        "Actual Final": [actual_final / 1e6 for s in retro_data],
    }).set_index("Day")
    st.line_chart(chart_data, height=300)

    # --- Detail table ---
    def _color_err(err):
        ae = abs(err)
        if ae <= 10:
            return f"+{err:.1f}%" if err >= 0 else f"{err:.1f}%"
        return f"+{err:.1f}%" if err >= 0 else f"{err:.1f}%"

    table_rows = []
    for s in retro_data:
        table_rows.append({
            "Day": s["day"],
            "Date": s["date"],
            "M1 $M": round(s["m1_ml"] / 1e6, 1),
            "M2 $M": round(s["m2_ml"] / 1e6, 1),
            "M3 $M": round(s["m3_ml"] / 1e6, 1),
            "M4 $M": round(s["m4_ml"] / 1e6, 1),
            "Actual $M": round(actual_final / 1e6, 1),
            "M1 Err%": round(s["m1_err"], 1),
            "M2 Err%": round(s["m2_err"], 1),
            "M3 Err%": round(s["m3_err"], 1),
            "M4 Err%": round(s["m4_err"], 1),
        })
    st.dataframe(
        pd.DataFrame(table_rows),
        use_container_width=True,
        hide_index=True,
        column_config={
            "M1 Err%": st.column_config.NumberColumn(format="%.1f%%"),
            "M2 Err%": st.column_config.NumberColumn(format="%.1f%%"),
            "M3 Err%": st.column_config.NumberColumn(format="%.1f%%"),
            "M4 Err%": st.column_config.NumberColumn(format="%.1f%%"),
        }
    )


def render_forecast_tab(data):
    fiscal = data["fiscal"]
    fa_golives_tab, fa_wins_tab = st.tabs(["Go-Lives Forecast", "Wins Forecast"])

    with fa_golives_tab:
        forecast_analysis = data["forecast_analysis"]
        forecasts = data["forecasts"]
        deployed = data["deployed"]
        day_number = safe_int(fiscal["DAY_NUMBER"])
        week_number = safe_int(fiscal["WEEK_NUMBER"])
        forecast_html = _build_forecast_tab(forecast_analysis, forecasts, deployed, day_number, week_number)
        st.html(STREAMLIT_CSS + forecast_html)
        retro = forecast_analysis.get("retrospective")
        if retro:
            fy = safe_int(fiscal["FISCAL_YEAR"])
            q = safe_str(fiscal["FISCAL_QUARTER"])
            _render_retrospective(retro, f"FY{fy % 100}-{q}")

    with fa_wins_tab:
        wfa = data.get("wins_forecast_analysis") or {}
        wins_forecast = data.get("wins_forecast") or {}
        day_number = safe_int(fiscal["DAY_NUMBER"])
        week_number = safe_int(fiscal["WEEK_NUMBER"])
        wins_html = _build_wins_forecast_tab_html(wfa, wins_forecast, day_number, week_number)
        st.html(STREAMLIT_CSS + wins_html)



# =============================================================================
# MAIN APP
# =============================================================================

st.set_page_config(
    page_title="PEAK Qualify & Commit",
    page_icon="📊",
    layout="wide",
    initial_sidebar_state="expanded",
)

# =============================================================================
# ACCESS CONTROL — restrict to approved users
# =============================================================================
ALLOWED_USERS = {"NATHOMAS", "SVANDAAL", "MMEREDITH", "NTSUI", "ADANNA", "JSMOLLETT", "JPATEL", "MMULDOON"}

def _check_access():
    """Verify the current user is in the approved access list."""
    allowed_emails = {
        "nate.thomas@snowflake.com",
        "saskia.vandaal@snowflake.com",
        "matt.meredith@snowflake.com",
        "nick.tsui@snowflake.com",
        "alex.danna@snowflake.com",
        "jocqui.smollett@snowflake.com",
        "jaime.patel@snowflake.com",
        "margaret.muldoon@snowflake.com",
        "josh.chacona@snowflake.com",
    }
    try:
        user_email = st.user.get("email", "").lower()
        if user_email:
            return user_email in allowed_emails
    except Exception:
        pass
    # Local dev fallback
    if not _use_snowpark:
        return True
    return False

if not _check_access():
    st.error("Access restricted. This app is currently limited to authorized users only.")
    st.info("Contact nate.thomas@snowflake.com to request access.")
    st.stop()


def main():
    with st.sidebar:
        st.title("PEAK QC Report")
        st.markdown("---")
        selected_theater = st.selectbox(
            "Theater", options=THEATER_OPTIONS,
            index=THEATER_OPTIONS.index("AMSExpansion"),
            help="Select theater to view. Changing theater reloads all data.",
        )
        all_quarters = compute_fiscal_quarters()
        quarter_labels = [q["label"] for q in all_quarters]
        current_idx = next((i for i, q in enumerate(all_quarters) if q["is_current"]), len(all_quarters) - 1)
        selected_quarter_label = st.selectbox(
            "Fiscal Quarter", options=quarter_labels,
            index=current_idx,
            help="Select fiscal quarter. Current quarter is the default.",
        )
        selected_quarter = all_quarters[quarter_labels.index(selected_quarter_label)]
        st.markdown("---")
        cache_key = f"peak_data_{selected_theater}_{selected_quarter_label}"
        cached = st.session_state.get(cache_key)
        if cached and cached.get("_loaded_at"):
            last_refresh = cached["_loaded_at"].strftime('%H:%M:%S')
        else:
            last_refresh = "not yet loaded"
        st.markdown(f"*Data cached for 10 min.*  \n*Last refresh: {last_refresh}*")
        if st.button("Refresh Data"):
            for key in list(st.session_state.keys()):
                if key.startswith("peak_data_"):
                    del st.session_state[key]
            st.rerun()

    data = load_all_data(selected_theater, selected_quarter)
    for k, v in data["_config"].items():
        if v is not None:
            CONFIG[k] = v

    fiscal = data["fiscal"]
    quarter = safe_str(fiscal["FISCAL_QUARTER"])
    qstart = safe_str(fiscal["FQ_START"])
    qend = safe_str(fiscal["FQ_END"])
    days_remaining = safe_int(fiscal["DAYS_REMAINING"])

    _gvp_disp = CONFIG.get("gvp_name") or "GVP unassigned"
    st.markdown(f"## PEAK Forecasting — {selected_theater}")
    st.caption(f"GVP: {_gvp_disp}")
    if not selected_quarter["is_current"]:
        st.info(f"Viewing **{selected_quarter_label}** (historical). Cache-based metrics (velocity, pipeline movements, Cortex Code usage) are only available for the current quarter.")
    if date.fromisoformat(qend) < date.today():
        st.markdown(f"**{selected_quarter_label} ({quarter})** ({qstart} - {qend}) | **Quarter Complete**")
    else:
        st.markdown(f"**{selected_quarter_label} ({quarter})** ({qstart} - {qend}) | **{days_remaining} days remaining**")

    tab_script, tab_wins, tab_golives, tab_forecast = st.tabs([
        "Script", "Use Case Wins", "Use Case Go-Lives", "Forecast Analysis",
    ])

    with tab_script:
        render_script_tab(data)
    with tab_wins:
        render_wins_tab(data)
    with tab_golives:
        render_golives_tab(data)
    with tab_forecast:
        render_forecast_tab(data)


main()
