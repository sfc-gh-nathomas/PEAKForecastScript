"""
PEAK Qualify & Commit — Streamlit in Snowflake (SiS) App
Single-file deployment for Snowflake Streamlit.

Combines all query functions, helpers, and rendering from peak_report.py + peak_app.py
into a single standalone file compatible with Snowpark session execution.
"""

import os
import re
import time
import html as html_lib
from datetime import datetime
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
    "dim_uc_direct_table": "MDM.MDM_INTERFACES.DIM_USE_CASE",
    "tb_ingest_table": "SALES.SALES_BI.SALES_PROGRAMS_BRONZE_INGEST",
    "tb_goals_table": "SALES.SALES_BI.SALES_PROGRAMS_SUCCESS_GOALS",
    "bronze_feature": "%Make Your Data AI Ready%",
    "sqlserver_feature": "%Modernize Your Data Estate%",
    "si_technical_uc": "%Snowflake Intelligence%",
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

# GVP to theater name mapping
GVP_THEATER_MAP = {
    "Mark Fleming": "AMSExpansion",
    "Jennifer Chronis": "USMajors",
    "Jonathan Beaulier": "USPubSec",
    "Keegan Riley": "AMSAcquisition",
    "Jon Robertson": "APJ",
    "Dayne Turbitt": "EMEA",
}


def _theater():
    """Return the theater name for the current GVP."""
    return GVP_THEATER_MAP.get(CONFIG["gvp_name"], CONFIG["gvp_name"])


# Snowflake fiscal year starts Feb 1.
# Q1=Feb-Apr, Q2=May-Jul, Q3=Aug-Oct, Q4=Nov-Jan
FISCAL_QUARTER_DEFS = [
    ("Q1", 2, 1, 4, 30),   # (label, start_month, start_day, end_month, end_day)
    ("Q2", 5, 1, 7, 31),
    ("Q3", 8, 1, 10, 31),
    ("Q4", 11, 1, 1, 31),  # end is Jan 31 of next calendar year
]


def _build_quarter_options():
    """Build a list of selectable fiscal quarters from FY26-Q1 through next FY's Q4.

    Returns list of dicts: {"label": "FY26-Q1", "fy": 2026, "q_idx": 0,
                            "start": "2025-02-01", "end": "2025-04-30"}
    """
    from datetime import date
    today = date.today()
    # Determine current FY
    if today.month >= 2:
        current_fy = today.year + 1
    else:
        current_fy = today.year

    options = []
    for fy in range(2026, current_fy + 2):  # FY26 through current+1
        cal_year = fy - 1  # calendar year when FY starts (Feb)
        for q_idx, (q_label, sm, sd, em, ed) in enumerate(FISCAL_QUARTER_DEFS):
            if q_label == "Q4":
                start = f"{cal_year}-{sm:02d}-{sd:02d}"
                end = f"{cal_year + 1}-{em:02d}-{ed:02d}"
            else:
                start = f"{cal_year}-{sm:02d}-{sd:02d}"
                end = f"{cal_year}-{em:02d}-{ed:02d}"
            options.append({
                "label": f"FY{fy % 100}-{q_label}",
                "fy": fy,
                "q_idx": q_idx,
                "start": start,
                "end": end,
            })
    return options


def _current_quarter_label():
    """Return the label for today's fiscal quarter, e.g. 'FY27-Q1'."""
    from datetime import date
    today = date.today()
    if today.month >= 2:
        fy = today.year + 1
    else:
        fy = today.year
    month = today.month
    if month in (2, 3, 4):
        q = "Q1"
    elif month in (5, 6, 7):
        q = "Q2"
    elif month in (8, 9, 10):
        q = "Q3"
    else:
        q = "Q4"
    return f"FY{fy % 100}-{q}"


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

def q_fiscal_calendar(quarter_override=None):
    """Compute fiscal calendar. If quarter_override is provided, use its dates instead of CURRENT_DATE().

    quarter_override: dict with keys "label", "fy", "start", "end" from _build_quarter_options(),
                      or None to auto-detect from CURRENT_DATE().
    """
    from datetime import date

    current_q_label = _current_quarter_label()

    if quarter_override is not None:
        # Use the override dates directly — no SQL needed for fiscal period
        fy = quarter_override["fy"]
        fq_label = quarter_override["label"].split("-")[1]  # e.g. "Q1" from "FY27-Q1"
        fq_start = quarter_override["start"]
        fq_end = quarter_override["end"]
        is_current = (quarter_override["label"] == current_q_label)
        fiscal = {
            "FISCAL_YEAR": fy,
            "FISCAL_QUARTER": fq_label,
            "FQ_START": fq_start,
            "FQ_END": fq_end,
        }
    else:
        # Original behavior: detect from CURRENT_DATE()
        rows = run_query("""
            SELECT
                CASE
                    WHEN MONTH(CURRENT_DATE()) >= 2 THEN YEAR(CURRENT_DATE()) + 1
                    ELSE YEAR(CURRENT_DATE())
                END AS FISCAL_YEAR,
                CASE
                    WHEN MONTH(CURRENT_DATE()) IN (2,3,4) THEN 'Q1'
                    WHEN MONTH(CURRENT_DATE()) IN (5,6,7) THEN 'Q2'
                    WHEN MONTH(CURRENT_DATE()) IN (8,9,10) THEN 'Q3'
                    ELSE 'Q4'
                END AS FISCAL_QUARTER,
                CASE
                    WHEN MONTH(CURRENT_DATE()) IN (2,3,4) THEN DATE_FROM_PARTS(YEAR(CURRENT_DATE()), 2, 1)
                    WHEN MONTH(CURRENT_DATE()) IN (5,6,7) THEN DATE_FROM_PARTS(YEAR(CURRENT_DATE()), 5, 1)
                    WHEN MONTH(CURRENT_DATE()) IN (8,9,10) THEN DATE_FROM_PARTS(YEAR(CURRENT_DATE()), 8, 1)
                    WHEN MONTH(CURRENT_DATE()) IN (11,12) THEN DATE_FROM_PARTS(YEAR(CURRENT_DATE()), 11, 1)
                    ELSE DATE_FROM_PARTS(YEAR(CURRENT_DATE()) - 1, 11, 1)
                END AS FQ_START,
                CASE
                    WHEN MONTH(CURRENT_DATE()) IN (2,3,4) THEN DATE_FROM_PARTS(YEAR(CURRENT_DATE()), 4, 30)
                    WHEN MONTH(CURRENT_DATE()) IN (5,6,7) THEN DATE_FROM_PARTS(YEAR(CURRENT_DATE()), 7, 31)
                    WHEN MONTH(CURRENT_DATE()) IN (8,9,10) THEN DATE_FROM_PARTS(YEAR(CURRENT_DATE()), 10, 31)
                    WHEN MONTH(CURRENT_DATE()) IN (11,12) THEN DATE_FROM_PARTS(YEAR(CURRENT_DATE()) + 1, 1, 31)
                    ELSE DATE_FROM_PARTS(YEAR(CURRENT_DATE()), 1, 31)
                END AS FQ_END
        """)
        assert len(rows) == 1, f"Expected 1 fiscal calendar row, got {len(rows)}"
        fiscal = rows[0]
        fy = safe_int(fiscal["FISCAL_YEAR"])
        fq_start = safe_str(fiscal["FQ_START"]).strip('"')
        fq_end = safe_str(fiscal["FQ_END"]).strip('"')
        fq_label = safe_str(fiscal.get("FISCAL_QUARTER", "Q1"))
        is_current = True

    CONFIG["quarter_start"] = fq_start
    CONFIG["quarter_end"] = fq_end
    CONFIG["fiscal_year"] = fy
    CONFIG["fiscal_year_label"] = f"FY{fy % 100}"
    CONFIG["prior_fy_label"] = f"FY{(fy - 1) % 100}"
    CONFIG["fiscal_quarter"] = f"FY{fy}-{fq_label}"
    CONFIG["is_current_quarter"] = is_current

    # Compute day/week number and reference_date
    if is_current:
        # Current quarter: use CURRENT_DATE() as reference
        day_rows = run_query(f"""
            SELECT DATEDIFF('day', '{fq_start}'::DATE, CURRENT_DATE()) + 1 AS DAY_NUMBER,
                   CEIL((DATEDIFF('day', '{fq_start}'::DATE, CURRENT_DATE()) + 1) / 7.0) AS WEEK_NUMBER
        """)
        day_number = safe_int(day_rows[0].get("DAY_NUMBER", 0)) if day_rows else 0
        week_number = safe_int(day_rows[0].get("WEEK_NUMBER", 0)) if day_rows else 0
        dr_rows = run_query(f"SELECT DATEDIFF('day', CURRENT_DATE(), '{fq_end}'::DATE) + 1 AS DAYS_REMAINING")
        days_remaining = safe_int(dr_rows[0].get("DAYS_REMAINING", 0)) if dr_rows else 0
        CONFIG["reference_date"] = "CURRENT_DATE()"
    else:
        # Non-current quarter: determine if past or future
        today = date.today()
        fq_end_date = date.fromisoformat(fq_end)
        fq_start_date = date.fromisoformat(fq_start)
        if today > fq_end_date:
            # Past quarter — fully elapsed
            total_days = (fq_end_date - fq_start_date).days + 1
            day_number = total_days
            week_number = -(-total_days // 7)  # ceil division
            days_remaining = 0
            CONFIG["reference_date"] = f"'{fq_end}'::DATE"
        else:
            # Future quarter — not yet started
            day_number = 0
            week_number = 0
            total_days = (fq_end_date - fq_start_date).days + 1
            days_remaining = total_days
            CONFIG["reference_date"] = f"'{fq_start}'::DATE"

    fiscal["DAY_NUMBER"] = day_number
    fiscal["WEEK_NUMBER"] = week_number
    fiscal["DAYS_REMAINING"] = days_remaining
    CONFIG["days_remaining"] = days_remaining

    # Prior FY quarters: FY starts Feb 1, so prior FY = fy-1
    prior_fy_start_year = fy - 2  # calendar year when prior FY starts (Feb)
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


def q_qtd_revenue():
    rows = run_query(f"""
        SELECT FORECAST_TYPE, FORECAST_AMOUNT
        FROM SALES.REPORTING.PEAK_FORECAST_CALLS_PIPELINE_TARGETS
        WHERE USER_NAME = '{CONFIG["gvp_name"]}'
          AND FUNCTION = '{CONFIG["gvp_function"]}'
          AND TYPE = 'Consumption'
          AND CURRENT_FISCAL_QUARTER = TRUE
          AND LATEST_DATE = TRUE
    """)
    assert len(rows) == 3, f"Expected 3 revenue rows, got {len(rows)}"
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
        "target": "TBD",
        "pct_target": "TBD",
        "q1_forecast": revenue_map.get("Total", 0),
        "fy_forecast": float(fy_rows[0]["FY_FORECAST"]),
    }


def q_forecast_calls():
    rows = run_query(f"""
        SELECT FORECAST_TYPE, FORECAST_AMOUNT, LATEST_DATE, PREVIOUS_WEEK
        FROM SALES.REPORTING.PEAK_FORECAST_CALLS_PIPELINE_TARGETS
        WHERE USER_NAME = '{CONFIG["gvp_name"]}'
          AND FUNCTION = '{CONFIG["gvp_function"]}'
          AND TYPE = 'Use Case Go-Lives'
          AND FORECAST_TYPE IN ('CommitForecast', 'MostLikelyForecast', 'BestCaseForecast', 'Target')
          AND CURRENT_FISCAL_QUARTER = TRUE
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


def q_last7_deployed():
    rows = run_query(f"""
        SELECT SUM(u.USE_CASE_ACV) as LAST7_ACV, COUNT(*) as LAST7_COUNT
        FROM {CONFIG["raven_uc_table"]} u
        JOIN {CONFIG["raven_acct_table"]} a ON u.VH_ACCOUNT_C = a.SALESFORCE_ACCOUNT_ID
        WHERE a.GVP = '{CONFIG["gvp_name"]}'
          AND u.USE_CASE_ACV > 0
          AND u.IS_WENT_LIVE = TRUE
          AND u.DEFAULT_DATE >= DATEADD('day', -7, {CONFIG["reference_date"]})
    """)
    r = rows[0]
    return {"acv": safe_float(r["LAST7_ACV"] or 0), "count": safe_int(r["LAST7_COUNT"] or 0)}


def q_deployment_velocity():
    # Velocity cache is a point-in-time snapshot — only meaningful for current quarter
    if not CONFIG.get("is_current_quarter", True):
        return {"current": {"v7": 0, "v14": 0, "v30": 0}, "historical": []}
    gvp = CONFIG["gvp_name"]
    rows = run_query(f"""
        SELECT PERIOD, V7, V14, V30
        FROM SNOWPUBLIC.STREAMLIT.VELOCITY_CACHE
        WHERE ACCOUNT_GVP = '{gvp}' AND METRIC_TYPE = 'deployment'
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
    excluded = ", ".join(f"'{s}'" for s in CONFIG["excluded_stages"])
    return run_query(f"""
        WITH fiscal_qtr AS (
            SELECT '{CONFIG["quarter_start"]}'::DATE AS FQ_START,
                   '{CONFIG["quarter_end"]}'::DATE AS FQ_END,
                   DATEDIFF('day', {CONFIG["reference_date"]}, '{CONFIG["quarter_end"]}'::DATE) + 1 AS DAYS_REMAINING
        )
        SELECT u.USE_CASE_ID, u.USE_CASE_NAME, u.ACCOUNT_NAME, r.USE_CASE_ACV as USE_CASE_EACV,
               u.STAGE_NUMBER, r.USE_CASE_STAGE, u.DAYS_IN_STAGE, r.GO_LIVE_DATE,
               u.TECHNICAL_WIN_DATE, f.DAYS_REMAINING,
               CASE
                   WHEN u.STAGE_NUMBER = 6 THEN 'Good'
                   WHEN u.STAGE_NUMBER = 5 AND (u.DAYS_IN_STAGE + f.DAYS_REMAINING) >= {CONFIG["risk_thresholds"]["stage_5"]} THEN 'Good'
                   WHEN u.STAGE_NUMBER = 4 AND (u.DAYS_IN_STAGE + f.DAYS_REMAINING) >= {CONFIG["risk_thresholds"]["stage_4"]} THEN 'Good'
                   WHEN u.STAGE_NUMBER IN (1,2,3) AND (u.DAYS_IN_STAGE + f.DAYS_REMAINING) >= {CONFIG["risk_thresholds"]["stage_123"]} THEN 'Good'
                   ELSE 'At Risk'
               END AS RISK_STATUS
        FROM {CONFIG["dim_uc_table"]} u
        CROSS JOIN fiscal_qtr f
        JOIN {CONFIG["raven_uc_table"]} r ON u.USE_CASE_ID = r.ID
        JOIN {CONFIG["raven_acct_table"]} a ON r.VH_ACCOUNT_C = a.SALESFORCE_ACCOUNT_ID
        WHERE a.GVP = '{CONFIG["gvp_name"]}'
            AND r.USE_CASE_ACV > 0 AND r.IS_WENT_LIVE = FALSE AND r.IS_LOST = FALSE
            AND u.STAGE_NUMBER BETWEEN 1 AND 6
            AND r.USE_CASE_STAGE NOT IN ({excluded})
            AND r.GO_LIVE_DATE BETWEEN f.FQ_START AND f.FQ_END
        ORDER BY r.USE_CASE_ACV DESC LIMIT 15
    """)


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
    rows = run_query(f"""
        WITH fiscal_qtr AS (
            SELECT '{CONFIG["quarter_start"]}'::DATE AS FQ_START,
                   '{CONFIG["quarter_end"]}'::DATE AS FQ_END,
                   DATEDIFF('day', {CONFIG["reference_date"]}, '{CONFIG["quarter_end"]}'::DATE) + 1 AS DAYS_REMAINING
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


def _use_case_select_cols():
    return """
        u.ID as USE_CASE_ID,
        u.VH_ACCOUNT_C as ACCOUNT_ID,
        u.NAME as USE_CASE_NUMBER,
        a.SALESFORCE_ACCOUNT_NAME as ACCOUNT_NAME,
        u.VH_NAME_C as USE_CASE_NAME,
        u.USE_CASE_ACV as USE_CASE_EACV,
        u.FORECAST_STATUS_C as FORECAST_CATEGORY,
        u.GO_LIVE_DATE,
        u.USE_CASE_STAGE,
        u.USE_CASE_COMMENTS_C as SE_COMMENTS,
        u.NEXT_STEP_C as NEXT_STEPS,
        u.USE_CASE_RISK_C as USE_CASE_RISK,
        a.SALESFORCE_OWNER_NAME as ACCOUNT_EXECUTIVE_NAME,
        a.LEAD_SALES_ENGINEER_NAME as USE_CASE_LEAD_SE_NAME,
        a.SALES_AREA as REGION_NAME,
        u.VH_DESCRIPTION_C as USE_CASE_DESCRIPTION,
        u.IMPLEMENTER_C as IMPLEMENTER,
        u.PARTNERS_C as PARTNER_NAME
    """


def _dim_use_case_select_cols():
    return """
        u.USE_CASE_ID,
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
        u.USE_CASE_LEAD_SE_NAME,
        u.REGION_NAME,
        u.USE_CASE_DESCRIPTION,
        u.IMPLEMENTER,
        u.PARTNER_NAME
    """


def q_top5_use_cases():
    excluded = ", ".join(f"'{s}'" for s in CONFIG["excluded_stages"])
    return run_query(f"""
        SELECT {_use_case_select_cols()}
        FROM {CONFIG["raven_uc_table"]} u
        JOIN {CONFIG["raven_acct_table"]} a ON u.VH_ACCOUNT_C = a.SALESFORCE_ACCOUNT_ID
        WHERE a.GVP = '{CONFIG["gvp_name"]}'
          AND u.USE_CASE_ACV > 0 AND u.IS_WENT_LIVE = FALSE AND u.IS_LOST = FALSE
          AND u.GO_LIVE_DATE BETWEEN '{CONFIG["quarter_start"]}' AND '{CONFIG["quarter_end"]}'
          AND u.USE_CASE_STAGE NOT IN ({excluded})
        ORDER BY u.USE_CASE_ACV DESC LIMIT {CONFIG["top_n"]}
    """)


def _play_summary_query(play_name, filter_clause, is_deployed):
    if is_deployed:
        deploy_filter = f"u.GO_LIVE_DATE BETWEEN '{CONFIG['quarter_start']}' AND '{CONFIG['quarter_end']}'"
    else:
        deploy_filter = f"u.USE_CASE_STATUS IN ('In Pursuit', 'Implementation')"
    rows = run_query(f"""
        SELECT COALESCE(SUM(u.USE_CASE_EACV), 0) as ACV, COUNT(*) as COUNT_
        FROM {CONFIG["dim_uc_direct_table"]} u
        WHERE u.THEATER_NAME = '{_theater()}'
          AND u.USE_CASE_EACV > 0 AND {deploy_filter}
          AND {filter_clause}
    """)
    r = rows[0]
    return {"acv": safe_float(r["ACV"] or 0), "count": safe_int(r["COUNT_"] or 0)}


def q_sales_play_summary():
    bronze_filter = f"u.PRIORITIZED_FEATURES ILIKE '{CONFIG['bronze_feature']}'"
    sql_filter = f"u.PRIORITIZED_FEATURES ILIKE '{CONFIG['sqlserver_feature']}'"
    si_filter = f"u.TECHNICAL_USE_CASE ILIKE '{CONFIG['si_technical_uc']}'"
    result = {}
    for play, filt in [
        ("bronze", bronze_filter),
        ("si", si_filter),
        ("sqlserver", sql_filter),
    ]:
        result[f"{play}_open"] = _play_summary_query(play, filt, False)
        result[f"{play}_deployed"] = _play_summary_query(play, filt, True)
    return result


def q_play_detail_metrics():
    def _run(filter_clause):
        rows = run_query(f"""
            SELECT COUNT(*) as OPEN_COUNT, AVG(u.USE_CASE_EACV) as AVG_ACV,
                   MEDIAN(u.USE_CASE_EACV) as MEDIAN_ACV,
                   COUNT(DISTINCT u.REGION_NAME) as REGION_COUNT
            FROM {CONFIG["dim_uc_direct_table"]} u
            WHERE u.THEATER_NAME = '{_theater()}'
              AND u.USE_CASE_EACV > 0
              AND u.USE_CASE_STATUS IN ('In Pursuit', 'Implementation')
              AND {filter_clause}
        """)
        r = rows[0]
        return {
            "count": safe_int(r["OPEN_COUNT"] or 0), "avg_acv": safe_float(r["AVG_ACV"] or 0),
            "median_acv": safe_float(r["MEDIAN_ACV"] or 0), "regions": safe_int(r["REGION_COUNT"] or 0),
        }
    return {
        "bronze": _run(f"u.PRIORITIZED_FEATURES ILIKE '{CONFIG['bronze_feature']}'"),
        "si": _run(f"u.TECHNICAL_USE_CASE ILIKE '{CONFIG['si_technical_uc']}'"),
        "sqlserver": _run(f"u.PRIORITIZED_FEATURES ILIKE '{CONFIG['sqlserver_feature']}'"),
    }


def q_bronze_tb_total():
    rows = run_query(f"""
        SELECT COALESCE(SUM(TB_INGESTED), 0) as BRONZE_TB
        FROM {CONFIG["tb_ingest_table"]}
        WHERE GEO_NAME = '{_theater()}'
          AND IS_BRONZE = true
          AND MONTH BETWEEN '{CONFIG["quarter_start"]}' AND '{CONFIG["quarter_end"]}'
    """)
    return float(rows[0]["BRONZE_TB"] or 0)


def q_bronze_tb_target():
    """Fetch TB Ingested target from SALES_PROGRAMS_SUCCESS_GOALS for current theater/quarter."""
    rows = run_query(f"""
        SELECT GOAL
        FROM {CONFIG["tb_goals_table"]}
        WHERE THEATER = '{_theater()}'
          AND GOAL_TYPE = 'TB Ingested'
          AND TAG_VALUE = 'Make Your Data AI Ready'
          AND FISCAL_QUARTER = '{CONFIG["fiscal_quarter"]}'
    """)
    return float(rows[0]["GOAL"]) if rows and rows[0]["GOAL"] else None


def _play_use_cases_query(play_name, filter_clause):
    return run_query(f"""
        SELECT {_dim_use_case_select_cols()}
        FROM {CONFIG["dim_uc_direct_table"]} u
        WHERE u.THEATER_NAME = '{_theater()}'
          AND u.USE_CASE_EACV >= {CONFIG["play_threshold"]}
          AND u.USE_CASE_STATUS IN ('In Pursuit', 'Implementation')
          AND {filter_clause}
        ORDER BY u.USE_CASE_EACV DESC
    """)


def q_play_use_cases():
    return {
        "bronze": _play_use_cases_query("Bronze", f"u.PRIORITIZED_FEATURES ILIKE '{CONFIG['bronze_feature']}'"),
        "si": _play_use_cases_query("SI", f"u.TECHNICAL_USE_CASE ILIKE '{CONFIG['si_technical_uc']}'"),
        "sqlserver": _play_use_cases_query("SQL Server", f"u.PRIORITIZED_FEATURES ILIKE '{CONFIG['sqlserver_feature']}'"),
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


def q_cortex_code_theater_usage():
    """Cortex Code usage across ALL accounts with open pipeline go-lives in the quarter."""
    excluded = ", ".join(f"'{s}'" for s in CONFIG["excluded_stages"])
    try:
        # Step 1: Get all go-live account IDs
        acct_rows = run_query(f"""
            SELECT DISTINCT u.VH_ACCOUNT_C as ACCOUNT_ID
            FROM {CONFIG["raven_uc_table"]} u
            JOIN {CONFIG["raven_acct_table"]} a ON u.VH_ACCOUNT_C = a.SALESFORCE_ACCOUNT_ID
            WHERE a.GVP = '{CONFIG["gvp_name"]}'
              AND u.USE_CASE_ACV > 0 AND u.IS_WENT_LIVE = FALSE AND u.IS_LOST = FALSE
              AND u.GO_LIVE_DATE BETWEEN '{CONFIG["quarter_start"]}' AND '{CONFIG["quarter_end"]}'
              AND u.USE_CASE_STAGE NOT IN ({excluded})
        """)
        go_live_ids = [str(r["ACCOUNT_ID"]) for r in acct_rows if r.get("ACCOUNT_ID")]
        total_accounts = len(go_live_ids)
        if not go_live_ids:
            return {"total_accounts": 0, "cc_accounts": 0, "pct": 0, "avg_users": 0, "requests": 0, "credits": 0}
        # Step 2: Query CC cache table
        id_list = ", ".join(f"'{aid}'" for aid in go_live_ids)
        rows = run_query(f"""
            SELECT
                COUNT(DISTINCT SALESFORCE_ACCOUNT_ID) as CC_ACCOUNTS,
                COALESCE(ROUND(AVG(AVG_DAILY_USERS), 1), 0) as AVG_USERS_PER_ACCT,
                COALESCE(SUM(TOTAL_REQUESTS), 0) as TOTAL_CC_REQUESTS,
                COALESCE(SUM(ACTUAL_CREDITS), 0) as TOTAL_CC_CREDITS
            FROM SNOWPUBLIC.STREAMLIT.CC_USAGE_CACHE
            WHERE SALESFORCE_ACCOUNT_ID IN ({id_list})
              AND TOTAL_REQUESTS > 0
        """)
        r = rows[0] if rows else {}
        cc_accounts = safe_int(r.get("CC_ACCOUNTS", 0))
        return {
            "total_accounts": total_accounts,
            "cc_accounts": cc_accounts,
            "pct": round(cc_accounts / total_accounts * 100, 1) if total_accounts else 0,
            "avg_users": safe_float(r.get("AVG_USERS_PER_ACCT", 0)),
            "requests": safe_int(r.get("TOTAL_CC_REQUESTS", 0)),
            "credits": round(safe_float(r.get("TOTAL_CC_CREDITS", 0)), 0),
        }
    except Exception as e:
        return {"total_accounts": 0, "cc_accounts": 0, "pct": 0, "avg_users": 0, "requests": 0, "credits": 0, "error": str(e)}


def q_cortex_code_by_account(account_ids):
    """Cortex Code usage per account for use case tables."""
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
        FROM {CONFIG["tb_ingest_table"]}
        WHERE GEO_NAME = '{_theater()}'
          AND IS_BRONZE = true
        GROUP BY SALESFORCE_ACCOUNT_ID, SALESFORCE_ACCOUNT_NAME
    """)
    return {r["ACCOUNT_ID"]: r for r in rows}


def _play_risk_detail_query(play_name, filter_clause):
    risk_rows = run_query(f"""
        SELECT u.ACCOUNT_NAME, u.USE_CASE_NAME,
               u.USE_CASE_EACV, u.USE_CASE_RISK,
               u.SE_COMMENTS, u.NEXT_STEPS, u.USE_CASE_STAGE
        FROM {CONFIG["dim_uc_direct_table"]} u
        WHERE u.THEATER_NAME = '{_theater()}'
          AND u.USE_CASE_EACV > 0
          AND u.USE_CASE_STATUS IN ('In Pursuit', 'Implementation')
          AND u.USE_CASE_RISK IS NOT NULL AND u.USE_CASE_RISK != '' AND u.USE_CASE_RISK != 'None'
          AND {filter_clause}
        ORDER BY u.USE_CASE_EACV DESC
    """)
    total_rows = run_query(f"""
        SELECT COUNT(*) as TOTAL_COUNT
        FROM {CONFIG["dim_uc_direct_table"]} u
        WHERE u.THEATER_NAME = '{_theater()}'
          AND u.USE_CASE_EACV > 0
          AND u.USE_CASE_STATUS IN ('In Pursuit', 'Implementation')
          AND {filter_clause}
    """)
    total = safe_int(total_rows[0]["TOTAL_COUNT"])
    return {"risk_rows": risk_rows, "total_count": total}


def q_high_risk_use_cases():
    return run_query(f"""
        SELECT u.USE_CASE_ID, u.USE_CASE_NUMBER, u.USE_CASE_NAME,
               u.ACCOUNT_OWNER_NAME as AE_NAME, u.USE_CASE_LEAD_SE_NAME as SE_NAME,
               u.USE_CASE_EACV as USE_CASE_ACV, u.USE_CASE_RISK as RISK_TYPE,
               SNOWFLAKE.CORTEX.COMPLETE('llama3.1-70b',
                   CONCAT('Summarize this use case risk in 1-2 concise sentences for a sales leadership QC call. Focus on what the risk is and the current mitigation plan. Do not include any preamble or introductory text, just provide the summary directly. Risk type: ',
                       COALESCE(u.USE_CASE_RISK, 'Unknown'),
                       '. SE Comments: ', COALESCE(u.SE_COMMENTS, 'None'),
                       '. Next Steps: ', COALESCE(u.NEXT_STEPS, 'None'))
               ) as RISK_SUMMARY
        FROM {CONFIG["dim_uc_direct_table"]} u
        WHERE u.THEATER_NAME = '{_theater()}'
          AND u.USE_CASE_EACV > 0
          AND u.USE_CASE_STATUS IN ('In Pursuit', 'Implementation')
          AND u.USE_CASE_RISK IS NOT NULL AND u.USE_CASE_RISK != '' AND u.USE_CASE_RISK != 'None'
          AND u.USE_CASE_EACV >= {CONFIG["play_threshold"]}
        ORDER BY u.USE_CASE_EACV DESC
    """)


def q_play_risk_detail():
    return {
        "bronze": _play_risk_detail_query("Bronze", f"u.PRIORITIZED_FEATURES ILIKE '{CONFIG['bronze_feature']}'"),
        "si": _play_risk_detail_query("SI", f"u.TECHNICAL_USE_CASE ILIKE '{CONFIG['si_technical_uc']}'"),
        "sqlserver": _play_risk_detail_query("SQL Server", f"u.PRIORITIZED_FEATURES ILIKE '{CONFIG['sqlserver_feature']}'"),
    }


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
    # Also get distinct account IDs per implementer category for CC cross-reference
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
    # Collect account IDs by implementer category
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
    # Pipeline movements cache is a rolling 7-day snapshot — only meaningful for current quarter
    empty = {k: {"count": 0, "acv": 0} for k in ("won_to_imp", "won_to_lost", "pushed_out", "pulled_in", "imp_started", "new_pipeline")}
    if not CONFIG.get("is_current_quarter", True):
        return empty
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
    # Ensure all keys exist
    for key in ("won_to_imp", "won_to_lost", "pushed_out", "pulled_in", "imp_started", "new_pipeline"):
        if key not in result:
            result[key] = {"count": 0, "acv": 0}
    return result


def q_use_case_velocity():
    """Stage transition velocity from pre-computed MDM cache.
    Self-calculated DATEDIFFs for UCs created >= 2025-02-01, all stages.
    Returns avg created-to-TW, TW-to-imp-start, imp-start-to-deployed."""
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
        FROM {CONFIG["dim_uc_direct_table"]} u
        WHERE u.THEATER_NAME = '{_theater()}'
          AND u.PRIORITIZED_FEATURES ILIKE '{CONFIG["bronze_feature"]}'
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
    """Count SI use cases created QTD using DIM_USE_CASE TECHNICAL_USE_CASE filter."""
    created_rows = run_query(f"""
        SELECT COUNT(*) as CREATED_COUNT
        FROM {CONFIG["dim_uc_direct_table"]} u
        WHERE u.THEATER_NAME = '{_theater()}'
          AND u.TECHNICAL_USE_CASE ILIKE '{CONFIG["si_technical_uc"]}'
          AND u.CREATED_DATE BETWEEN '{CONFIG["quarter_start"]}' AND '{CONFIG["quarter_end"]}'
    """)
    return safe_int(created_rows[0]["CREATED_COUNT"]) if created_rows else 0


# =============================================================================
# FORECAST ANALYSIS QUERIES
# =============================================================================

def q_current_pipeline_phases():
    excluded = ", ".join(f"'{s}'" for s in CONFIG["excluded_stages"])
    rows = run_query(f"""
        SELECT
            CASE
                WHEN u.IS_WENT_LIVE = TRUE AND u.DEFAULT_DATE BETWEEN '{CONFIG["quarter_start"]}' AND '{CONFIG["quarter_end"]}'
                    THEN 'Already Deployed'
                WHEN u.IMPLEMENTATION_START_DATE <= {CONFIG["reference_date"]}
                    AND u.IS_WENT_LIVE = FALSE AND u.IS_LOST = FALSE THEN 'In Implementation'
                WHEN u.TECHNICAL_WIN_DATE_FORECAST_C <= {CONFIG["reference_date"]}
                    AND (u.IMPLEMENTATION_START_DATE > {CONFIG["reference_date"]} OR u.IMPLEMENTATION_START_DATE IS NULL)
                    AND u.IS_WENT_LIVE = FALSE AND u.IS_LOST = FALSE THEN 'Post-TW / Pre-Imp'
                WHEN u.CREATED_DATE <= {CONFIG["reference_date"]}
                    AND (u.TECHNICAL_WIN_DATE_FORECAST_C > {CONFIG["reference_date"]} OR u.TECHNICAL_WIN_DATE_FORECAST_C IS NULL)
                    AND (u.IMPLEMENTATION_START_DATE > {CONFIG["reference_date"]} OR u.IMPLEMENTATION_START_DATE IS NULL)
                    AND u.IS_WENT_LIVE = FALSE AND u.IS_LOST = FALSE THEN 'Pre-TW'
                ELSE 'Other'
            END as PIPELINE_PHASE,
            COUNT(*) as UC_COUNT,
            ROUND(SUM(u.USE_CASE_ACV), 0) as TOTAL_ACV
        FROM {CONFIG["raven_uc_table"]} u
        JOIN {CONFIG["raven_acct_table"]} a ON u.VH_ACCOUNT_C = a.SALESFORCE_ACCOUNT_ID
        WHERE a.GVP = '{CONFIG["gvp_name"]}'
          AND u.USE_CASE_ACV > 0
          AND u.GO_LIVE_DATE BETWEEN '{CONFIG["quarter_start"]}' AND '{CONFIG["quarter_end"]}'
          AND u.USE_CASE_STAGE NOT IN ({excluded})
        GROUP BY PIPELINE_PHASE ORDER BY PIPELINE_PHASE
    """)
    return {r["PIPELINE_PHASE"]: r for r in rows}


def q_historical_conversion_rates(day_number):
    quarters = CONFIG["prior_fy_quarters"]
    if not quarters:
        return []
    day_num = safe_int(day_number)
    snapshot_table = "SALES.REPORTING.DIM_USE_CASE_HISTORY_DS"
    unions = []
    for i, (qs, qe) in enumerate(quarters):
        q_label = f"{CONFIG['prior_fy_label']} Q{i+1}"
        snap_date = f"DATEADD('day', {day_num - 1}, '{qs}')::DATE"
        unions.append(f"""
            SELECT '{q_label}' as QTR, '{qs}' as QS, '{qe}' as QE,
                SUM(CASE WHEN h.IS_DEPLOYED = TRUE AND h.ACTUAL_USE_CASE_DEPLOYMENT_DATE <= {snap_date}
                         AND h.ACTUAL_USE_CASE_DEPLOYMENT_DATE BETWEEN '{qs}' AND '{qe}'
                    THEN h.USE_CASE_EACV ELSE 0 END) as DEPLOYED_ACV,
                SUM(CASE WHEN h.IMPLEMENTATION_START_DATE <= {snap_date}
                         AND h.IS_DEPLOYED = FALSE AND h.IS_LOST = FALSE
                         AND h.GO_LIVE_DATE BETWEEN '{qs}' AND '{qe}'
                    THEN h.USE_CASE_EACV ELSE 0 END) as IMP_TOTAL,
                SUM(CASE WHEN h.IMPLEMENTATION_START_DATE <= {snap_date}
                         AND h.IS_DEPLOYED = FALSE AND h.IS_LOST = FALSE
                         AND h.GO_LIVE_DATE BETWEEN '{qs}' AND '{qe}'
                         AND o.IS_WENT_LIVE = TRUE AND o.DEFAULT_DATE BETWEEN '{qs}' AND '{qe}'
                    THEN h.USE_CASE_EACV ELSE 0 END) as IMP_CONVERTED,
                SUM(CASE WHEN h.TECHNICAL_WIN_DATE <= {snap_date}
                         AND (h.IMPLEMENTATION_START_DATE > {snap_date} OR h.IMPLEMENTATION_START_DATE IS NULL)
                         AND h.IS_DEPLOYED = FALSE AND h.IS_LOST = FALSE
                         AND h.GO_LIVE_DATE BETWEEN '{qs}' AND '{qe}'
                    THEN h.USE_CASE_EACV ELSE 0 END) as TW_TOTAL,
                SUM(CASE WHEN h.TECHNICAL_WIN_DATE <= {snap_date}
                         AND (h.IMPLEMENTATION_START_DATE > {snap_date} OR h.IMPLEMENTATION_START_DATE IS NULL)
                         AND h.IS_DEPLOYED = FALSE AND h.IS_LOST = FALSE
                         AND h.GO_LIVE_DATE BETWEEN '{qs}' AND '{qe}'
                         AND o.IS_WENT_LIVE = TRUE AND o.DEFAULT_DATE BETWEEN '{qs}' AND '{qe}'
                    THEN h.USE_CASE_EACV ELSE 0 END) as TW_CONVERTED,
                SUM(CASE WHEN h.CREATED_DATE <= {snap_date}
                         AND (h.TECHNICAL_WIN_DATE > {snap_date} OR h.TECHNICAL_WIN_DATE IS NULL)
                         AND (h.IMPLEMENTATION_START_DATE > {snap_date} OR h.IMPLEMENTATION_START_DATE IS NULL)
                         AND h.IS_DEPLOYED = FALSE AND h.IS_LOST = FALSE
                         AND h.GO_LIVE_DATE BETWEEN '{qs}' AND '{qe}'
                    THEN h.USE_CASE_EACV ELSE 0 END) as PRE_TW_TOTAL,
                SUM(CASE WHEN h.CREATED_DATE <= {snap_date}
                         AND (h.TECHNICAL_WIN_DATE > {snap_date} OR h.TECHNICAL_WIN_DATE IS NULL)
                         AND (h.IMPLEMENTATION_START_DATE > {snap_date} OR h.IMPLEMENTATION_START_DATE IS NULL)
                         AND h.IS_DEPLOYED = FALSE AND h.IS_LOST = FALSE
                         AND h.GO_LIVE_DATE BETWEEN '{qs}' AND '{qe}'
                         AND o.IS_WENT_LIVE = TRUE AND o.DEFAULT_DATE BETWEEN '{qs}' AND '{qe}'
                    THEN h.USE_CASE_EACV ELSE 0 END) as PRE_TW_CONVERTED,
                0 as NEW_PIPELINE_CONVERTED,
                0 as FINAL_DEPLOYED
            FROM {snapshot_table} h
            LEFT JOIN {CONFIG["raven_uc_table"]} o ON h.USE_CASE_ID = o.ID
            WHERE h.DS = {snap_date}
              AND h.THEATER_NAME = '{_theater()}'
              AND h.USE_CASE_EACV > 0
        """)

    # Combine new_pipeline and final_deployed into a single query
    np_fd_unions = []
    for i, (qs, qe) in enumerate(quarters):
        q_label = f"{CONFIG['prior_fy_label']} Q{i+1}"
        snap_date = f"DATEADD('day', {day_num - 1}, '{qs}')::DATE"
        np_fd_unions.append(f"""
            SELECT '{q_label}' as QTR,
                COALESCE((SELECT SUM(o2.USE_CASE_ACV)
                 FROM {CONFIG["raven_uc_table"]} o2
                 JOIN {CONFIG["raven_acct_table"]} a2 ON o2.VH_ACCOUNT_C = a2.SALESFORCE_ACCOUNT_ID
                 LEFT JOIN {snapshot_table} h2 ON h2.USE_CASE_ID = o2.ID AND h2.DS = {snap_date}
                 WHERE a2.GVP = '{CONFIG["gvp_name"]}' AND o2.USE_CASE_ACV > 0
                   AND o2.IS_WENT_LIVE = TRUE AND o2.DEFAULT_DATE BETWEEN '{qs}' AND '{qe}'
                   AND (h2.USE_CASE_ID IS NULL OR h2.CREATED_DATE > {snap_date})
                ), 0) as NEW_PIPELINE_ACV,
                COALESCE((SELECT SUM(u2.USE_CASE_ACV)
                 FROM {CONFIG["raven_uc_table"]} u2
                 JOIN {CONFIG["raven_acct_table"]} a2 ON u2.VH_ACCOUNT_C = a2.SALESFORCE_ACCOUNT_ID
                 WHERE a2.GVP = '{CONFIG["gvp_name"]}' AND u2.IS_WENT_LIVE = TRUE
                   AND u2.DEFAULT_DATE BETWEEN '{qs}' AND '{qe}' AND u2.USE_CASE_ACV > 0
                ), 0) as FINAL_DEPLOYED
        """)

    rows = run_query(" UNION ALL ".join(unions) + " ORDER BY QTR")
    if np_fd_unions:
        np_fd_rows = run_query(" UNION ALL ".join(np_fd_unions) + " ORDER BY QTR")
        np_fd_map = {r["QTR"]: r for r in np_fd_rows}
        for r in rows:
            qtr_data = np_fd_map.get(r["QTR"], {})
            r["NEW_PIPELINE_CONVERTED"] = safe_float(qtr_data.get("NEW_PIPELINE_ACV", 0) or 0)
            r["FINAL_DEPLOYED"] = safe_float(qtr_data.get("FINAL_DEPLOYED", 0) or 0)

    rows = [r for r in rows if safe_float(r.get("IMP_TOTAL", 0) or 0) > 0
            or safe_float(r.get("DEPLOYED_ACV", 0) or 0) > 0]
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
        m3_stretch = (deployed_acv + imp_acv + tw_acv + pre_tw_acv) / (1 - max_new) if max_new < 1 else (deployed_acv + imp_acv + tw_acv + pre_tw_acv)
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


def compute_weighted_ensemble(forecast_analysis, backtest_results, day_number=31):
    fa = forecast_analysis
    m1 = fa["method1"]
    m2 = fa["method2"]
    m3 = fa["method3"]

    def _apply_day_adjustment(weights, day_n):
        if day_n == 31:
            return weights
        adjusted = {}
        for call_key in weights:
            w = dict(weights[call_key])
            if day_n < 31:
                factor = 0.5 + 0.5 * (day_n / 31)
            else:
                factor = 1.0 + 0.3 * ((day_n - 31) / 59)
            factor = max(0.3, min(factor, 1.5))
            w["m2"] = w["m2"] * factor
            total = w["m1"] + w["m2"] + w["m3"]
            if total > 0:
                adjusted[call_key] = {mk: v / total for mk, v in w.items()}
            else:
                adjusted[call_key] = w
        return adjusted

    def _compute_weights(bt_data):
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

    if backtest_results:
        base_weights = _compute_weights(backtest_results)
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
                loo_weights = _compute_weights(others)
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
    snapshot_table = "SALES.REPORTING.DIM_USE_CASE_HISTORY_DS"
    raven_uc = CONFIG["raven_uc_table"]
    raven_acct = CONFIG["raven_acct_table"]
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
        q_label = f"{CONFIG['prior_fy_label']} Q{i+1}"
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
                SUM(CASE WHEN h.IS_DEPLOYED = TRUE AND h.ACTUAL_USE_CASE_DEPLOYMENT_DATE <= {snap_date_str}
                         AND h.ACTUAL_USE_CASE_DEPLOYMENT_DATE BETWEEN '{qs}' AND '{qe}'
                    THEN h.USE_CASE_EACV ELSE 0 END) as DEPLOYED_ACV,
                SUM(CASE WHEN h.IS_DEPLOYED = FALSE AND h.IS_LOST = FALSE
                         AND h.STAGE_NUMBER = 6 AND h.GO_LIVE_DATE BETWEEN '{qs}' AND '{qe}'
                    THEN h.USE_CASE_EACV ELSE 0 END) as STAGE6_ACV,
                SUM(CASE WHEN h.IS_DEPLOYED = FALSE AND h.IS_LOST = FALSE
                         AND h.STAGE_NUMBER = 5
                         AND (h.DAYS_IN_STAGE + DATEDIFF('day', {snap_date_str}, '{qe}')) >= {thresholds["stage_5"]}
                         AND h.GO_LIVE_DATE BETWEEN '{qs}' AND '{qe}'
                    THEN h.USE_CASE_EACV ELSE 0 END) as STAGE5_GOOD,
                SUM(CASE WHEN h.IS_DEPLOYED = FALSE AND h.IS_LOST = FALSE
                         AND h.STAGE_NUMBER BETWEEN 1 AND 6
                         AND h.GO_LIVE_DATE BETWEEN '{qs}' AND '{qe}'
                         AND (
                             (h.STAGE_NUMBER IN (1,2,3) AND (h.DAYS_IN_STAGE + DATEDIFF('day', {snap_date_str}, '{qe}')) >= {thresholds["stage_123"]})
                             OR (h.STAGE_NUMBER = 4 AND (h.DAYS_IN_STAGE + DATEDIFF('day', {snap_date_str}, '{qe}')) >= {thresholds["stage_4"]})
                             OR (h.STAGE_NUMBER = 5 AND (h.DAYS_IN_STAGE + DATEDIFF('day', {snap_date_str}, '{qe}')) >= {thresholds["stage_5"]})
                             OR h.STAGE_NUMBER = 6
                         )
                    THEN h.USE_CASE_EACV ELSE 0 END) as GOOD_PIPELINE,
                SUM(CASE WHEN h.IS_DEPLOYED = FALSE AND h.IS_LOST = FALSE
                         AND h.STAGE_NUMBER BETWEEN 1 AND 6
                         AND h.GO_LIVE_DATE BETWEEN '{qs}' AND '{qe}'
                    THEN h.USE_CASE_EACV ELSE 0 END) as TOTAL_PIPELINE,
                SUM(CASE WHEN h.IMPLEMENTATION_START_DATE <= {snap_date_str}
                         AND h.IS_DEPLOYED = FALSE AND h.IS_LOST = FALSE
                         AND h.GO_LIVE_DATE BETWEEN '{qs}' AND '{qe}'
                    THEN h.USE_CASE_EACV ELSE 0 END) as IMP_ACV,
                SUM(CASE WHEN h.TECHNICAL_WIN_DATE <= {snap_date_str}
                         AND (h.IMPLEMENTATION_START_DATE > {snap_date_str} OR h.IMPLEMENTATION_START_DATE IS NULL)
                         AND h.IS_DEPLOYED = FALSE AND h.IS_LOST = FALSE
                         AND h.GO_LIVE_DATE BETWEEN '{qs}' AND '{qe}'
                    THEN h.USE_CASE_EACV ELSE 0 END) as TW_ACV,
                SUM(CASE WHEN h.CREATED_DATE <= {snap_date_str}
                         AND (h.TECHNICAL_WIN_DATE > {snap_date_str} OR h.TECHNICAL_WIN_DATE IS NULL)
                         AND (h.IMPLEMENTATION_START_DATE > {snap_date_str} OR h.IMPLEMENTATION_START_DATE IS NULL)
                         AND h.IS_DEPLOYED = FALSE AND h.IS_LOST = FALSE
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

def build_use_case_row(uc, consumption, bronze_tb=None, si_usage=None, cc_by_account=None, row_type="standard"):
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
    description = html_escape(truncate_text(uc.get("USE_CASE_DESCRIPTION", ""), 300))
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
    col3 = f"""{description}
    <div class="implementer"><strong>Implementer:</strong> {implementer if implementer else 'None'}</div>
    {partner_line}"""
    return f"""<tr>
  <td>{''.join(col1_lines)}</td>
  <td class="details-col">{col2}</td>
  <td class="summary">{col3}</td>
</tr>"""


# =============================================================================
# FORECAST TAB HTML BUILDER (from peak_report._build_forecast_tab)
# =============================================================================

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

    current_row = f"""<tr style="background: #e8f4f8; font-weight: 600;">
        <td><strong>{CONFIG["fiscal_year_label"]} Q1 (Current)</strong></td>
        <td class="number">{fmt_currency(pp["deployed"])}</td>
        <td class="number">?</td>
        <td class="number">{(pp["deployed"] / deployed["acv"] * 100) if deployed["acv"] > 0 else 0:.1f}%</td>
        <td class="number" colspan="4" style="text-align: center; color: #29B5E8;">In progress &mdash; see projections below</td>
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
    <div class="fa-note">Rates: Imp {rates.get('imp', 0)*100:.1f}% | TW {rates.get('tw', 0)*100:.1f}% | Pre-TW {rates.get('pre_tw', 0)*100:.1f}% | New Pipeline {rates.get('new_pipeline', 0)*100:.1f}%</div>
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

GVP_OPTIONS = [
    "Dayne Turbitt", "Jennifer Chronis", "Jon Robertson",
    "Jonathan Beaulier", "Keegan Riley", "Mark Fleming",
]

PLAY_OPTIONS = {
    "Bronze (Make Your Data AI Ready)": "bronze",
    "Snowflake Intelligence (AI: Snowflake Intelligence & Agents)": "si",
    "SQL Server Migration (Modernize Your Data Estate)": "sqlserver",
}


# =============================================================================
# DATA LOADING (parallel with ThreadPoolExecutor)
# =============================================================================

def _run_all_queries(gvp_name, quarter=None):
    CONFIG["gvp_name"] = gvp_name
    total_steps = 10
    completed = [0]
    progress = st.progress(0, text="Connecting to Snowflake...")

    def _tick(label):
        completed[0] += 1
        progress.progress(min(completed[0] / total_steps, 1.0), text=label)

    # --- Phase 0: Sequential setup (sets CONFIG values needed by later queries) ---
    _tick("Fiscal calendar & velocity...")
    fiscal = q_fiscal_calendar(quarter_override=quarter)
    CONFIG["day_number"] = safe_int(fiscal["DAY_NUMBER"])
    uc_velocity = q_use_case_velocity()
    update_risk_thresholds_from_velocity(uc_velocity)

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
        "bronze_tb_target": q_bronze_tb_target,
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
        "cc_theater": q_cortex_code_theater_usage,
    }
    # Queries that need fiscal calendar results
    phase1_with_args = {
        "pacing": lambda: q_prior_fy_pacing(fiscal["DAY_NUMBER"], fiscal["WEEK_NUMBER"]),
        "hist_conv_rates": lambda: q_historical_conversion_rates(fiscal["DAY_NUMBER"]),
    }
    all_phase1 = {**phase1_queries, **phase1_with_args}
    p1_results = {}
    p1_errors = {}

    with ThreadPoolExecutor(max_workers=26) as executor:
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
    bronze_tb_target_val = p1_results["bronze_tb_target"]
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
    cc_theater = p1_results["cc_theater"]
    pacing = p1_results["pacing"]
    hist_conv_rates = p1_results["hist_conv_rates"]

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

    progress.progress(1.0, text="Done!")
    progress.empty()

    return {
        "fiscal": fiscal, "revenue": revenue, "forecasts": forecasts,
        "deployed": deployed, "last7": last7, "pipeline": pipeline,
        "risk_analysis": risk_analysis, "top5": top5,
        "play_summary": play_summary, "play_detail": play_detail,
        "play_use_cases": play_use_cases, "play_risk": play_risk,
        "high_risk_ucs": high_risk_ucs, "consumption": consumption,
        "bronze_tb_total": bronze_tb_total, "bronze_tb_target": bronze_tb_target_val, "bronze_tb_acct": bronze_tb_acct,
        "si_usage": si_usage, "si_theater": si_theater, "pacing": pacing,
        "forecast_analysis": forecast_analysis, "play_targets": play_targets,
        "partner_sd": partner_sd, "uc_velocity": uc_velocity,
        "bronze_created": bronze_created, "si_created": si_created, "pipeline_movements": pipeline_movements,
        "cc_theater": cc_theater, "cc_by_account": cc_by_account,
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
            "is_current_quarter": CONFIG.get("is_current_quarter"),
            "reference_date": CONFIG.get("reference_date"),
        },
        "_loaded_at": datetime.now(),
    }


def load_all_data(gvp_name, quarter=None):
    q_label = quarter["label"] if quarter else "current"
    cache_key = f"peak_data_{gvp_name}_{q_label}"
    cached = st.session_state.get(cache_key)
    if cached is not None:
        loaded_at = cached.get("_loaded_at")
        if loaded_at and (datetime.now() - loaded_at).total_seconds() < 600:
            return cached
    data = _run_all_queries(gvp_name, quarter=quarter)
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
# TAB RENDERERS (from peak_app.py)
# =============================================================================

def render_script_tab(data, selected_play_key):
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
    pm = data.get("pipeline_movements", {})

    most_likely = forecasts["most_likely"]
    gl_target = forecasts["target"]
    deployed_pct = (deployed["acv"] / most_likely * 100) if most_likely else 0
    deployed_pct_of_target = (deployed["acv"] / gl_target * 100) if gl_target else 0
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

    script_html = f"""
<div style="background: #f8f9fa; border: 1px solid #dee2e6; border-radius: 8px; padding: 24px; font-family: Georgia, serif; font-size: 1.05em; line-height: 1.7;">
<h3 style="color: #333; border-bottom: 2px solid #007bff; padding-bottom: 8px;">Forecast Call</h3>
<p>For {_theater()}, my Most Likely call for go-lives this quarter is <strong>{fmt_currency(most_likely)}</strong>{ml_wow_text}.
We have deployed <strong>{fmt_currency(deployed["acv"])}</strong> QTD against a target of <strong>{fmt_currency(gl_target)}</strong> (<strong>{fmt_pct(deployed_pct_of_target)}</strong> of target).
Our open pipeline is <strong>{fmt_currency(pipeline["acv"])}</strong>, giving us <strong>{fmt_pct(coverage_pct)}</strong> ML coverage (deployed + open pipeline vs Most Likely).</p>
<h3 style="color: #333; border-bottom: 2px solid #007bff; padding-bottom: 8px;">Pipeline (Last 7 Days)</h3>
<p>In the last 7 days, <strong>{pm_pushed_out["count"]}</strong> use cases (<strong>{fmt_currency(pm_pushed_out["acv"])}</strong>) were pushed out of the quarter
while <strong>{pm_pulled_in["count"]}</strong> (<strong>{fmt_currency(pm_pulled_in["acv"])}</strong>) were pulled in.
<strong>{pm_imp_started["count"]}</strong> use cases (<strong>{fmt_currency(pm_imp_started["acv"])}</strong>) started implementation with a go-live this quarter &mdash;
of those, <strong>{pm_won_to_imp["count"]}</strong> (<strong>{fmt_currency(pm_won_to_imp["acv"])}</strong>) were won in prior quarters.
<strong>{pm_won_to_lost["count"]}</strong> prior-quarter wins (<strong>{fmt_currency(pm_won_to_lost["acv"])}</strong>) were lost.
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
    if selected_play_key == "bronze":
        br_dep = _ps.get("bronze_deployed", _empty_ps)
        br_open = _ps.get("bronze_open", _empty_ps)
        bronze_gap = fmt_play_gap(play_targets, "bronze_deployed", br_dep["acv"], br_dep["count"])
        bronze_target = fmt_play_target(play_targets, "bronze_deployed")
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
Regional coverage: <strong>{play_detail["bronze"]["regions"]}</strong> of 8 {_theater()} regions contributing.</p>
<div class="risk-box">
<strong>Risk Summary ({bronze_risk["at_risk"]} of {bronze_risk["total"]} use cases, {fmt_currency(bronze_risk["acv_at_risk"])} ACV at risk):</strong><br>
{bronze_risk["narrative_html"]}
</div>
"""
    elif selected_play_key == "si":
        si_dep = _ps.get("si_deployed", _empty_ps)
        si_open = _ps.get("si_open", _empty_ps)
        si_gap = fmt_play_gap(play_targets, "si_deployed", si_dep["acv"], si_dep["count"])
        si_target_str = fmt_play_target(play_targets, "si_deployed")
        si_risk = build_risk_narrative(play_risk["si"])
        script_html += f"""
<h4 style="color: #555;">Snowflake Intelligence (AI: Snowflake Intelligence &amp; Agents)</h4>
<p>SI has deployed <strong>{fmt_currency(si_dep["acv"])}</strong> ({si_dep["count"]} UCs) QTD
with <strong>{fmt_currency(si_open["acv"])}</strong> ({si_open["count"]} UCs) in open pipeline.
Deployed target: <strong>{si_target_str}</strong>. Gap to target: <strong>{si_gap}</strong>.
Regional coverage: <strong>{play_detail["si"]["regions"]}</strong> of 8 {_theater()} regions contributing.</p>
<p>Theater SI Usage (Last 30 Days): {si_theater["accounts"]:,} Accounts | {si_theater["users"]:,} Users | {si_theater["credits"]:,} Credits | {fmt_currency(si_theater["revenue"])} Revenue.</p>
<div class="risk-box">
<strong>Risk Summary ({si_risk["at_risk"]} of {si_risk["total"]} use cases, {fmt_currency(si_risk["acv_at_risk"])} ACV at risk):</strong><br>
{si_risk["narrative_html"]}
</div>
"""
    elif selected_play_key == "sqlserver":
        sql_dep = _ps.get("sqlserver_deployed", _empty_ps)
        sql_open = _ps.get("sqlserver_open", _empty_ps)
        sql_gap = fmt_play_gap(play_targets, "sqlserver_deployed", sql_dep["acv"], sql_dep["count"])
        sqlserver_target = fmt_play_target(play_targets, "sqlserver_deployed")
        sql_risk = build_risk_narrative(play_risk["sqlserver"])
        script_html += f"""
<h4 style="color: #555;">SQL Server Migration (Modernize Your Data Estate)</h4>
<p>SQL Server has deployed <strong>{fmt_currency(sql_dep["acv"])}</strong> ({sql_dep["count"]} UCs) QTD
with <strong>{fmt_currency(sql_open["acv"])}</strong> ({sql_open["count"]} UCs) in open pipeline.
Deployed target: <strong>{sqlserver_target}</strong>. Gap to target: <strong>{sql_gap}</strong>.
Regional coverage: <strong>{play_detail["sqlserver"]["regions"]}</strong> of 8 {_theater()} regions contributing.</p>
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
</div>"""
    st.html(STREAMLIT_CSS + script_html)


def render_golives_tab(data):
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
    cc_theater = data.get("cc_theater", {})
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
    bronze_risk = build_risk_narrative(play_risk["bronze"])
    si_risk = build_risk_narrative(play_risk["si"])
    sql_risk = build_risk_narrative(play_risk["sqlserver"])
    top5_rows = "\n".join(build_use_case_row(uc, consumption, cc_by_account=cc_by_account) for uc in top5)
    bronze_rows = "\n".join(build_use_case_row(uc, consumption, bronze_tb=bronze_tb_acct, cc_by_account=cc_by_account, row_type="bronze") for uc in play_use_cases["bronze"])
    si_rows = "\n".join(build_use_case_row(uc, consumption, si_usage=si_usage, cc_by_account=cc_by_account, row_type="si") for uc in play_use_cases["si"])
    sql_rows = "\n".join(build_use_case_row(uc, consumption, cc_by_account=cc_by_account, row_type="standard") for uc in play_use_cases["sqlserver"])
    day_avg = fmt_currency(pacing["day_avg"])
    day_pct = fmt_pct(pacing["day_pct"])
    week_avg = fmt_currency(pacing["week_avg"])
    week_pct = fmt_pct(pacing["week_pct"])
    bronze_created = data.get("bronze_created", {})
    si_created = data.get("si_created", 0)
    bronze_tb_total = data["bronze_tb_total"]

    # --- Sales Play Performance table data ---
    _ps = play_summary
    _pt = play_targets
    _empty_ps = {"acv": 0, "count": 0}

    def _sp_attainment(actual, target):
        if target is None or target == 0:
            return "&mdash;"
        pct = actual / target * 100
        color = "#28a745" if pct >= 100 else "#dc3545"
        return f'<span style="color:{color}; font-weight:600;">{pct:.0f}%</span>'

    def _sp_coverage(actual, pipeline_val, target):
        if target is None or target == 0:
            return "&mdash;"
        pct = (actual + pipeline_val) / target * 100
        color = "#28a745" if pct >= 100 else "#dc3545"
        return f'<span style="color:{color}; font-weight:600;">{pct:.0f}%</span>'

    # Row 1: Bronze - Use Cases Created
    br_created_actual = bronze_created.get("created", 0)
    br_created_target = _pt.get("bronze_created", {}).get("count")
    br_created_target_str = f"{br_created_target:,}" if br_created_target is not None else "&mdash;"
    br_created_att = _sp_attainment(br_created_actual, br_created_target)

    # Row 2: Bronze - DCT TBs Ingested
    br_tb_actual = bronze_tb_total
    br_tb_target = data.get("bronze_tb_target")
    br_tb_target_str = f"{br_tb_target:,.0f} TB" if br_tb_target is not None else "&mdash;"
    br_tb_att = _sp_attainment(br_tb_actual, br_tb_target)

    # Row 3: SI - Use Cases Created
    si_created_actual = si_created
    si_created_target = _pt.get("si_created", {}).get("count")
    si_created_target_str = f"{si_created_target:,}" if si_created_target is not None else "&mdash;"
    si_created_att = _sp_attainment(si_created_actual, si_created_target)

    # Row 4: SI - Use Cases Deployed
    si_dep = _ps.get("si_deployed", _empty_ps)
    si_open = _ps.get("si_open", _empty_ps)
    si_dep_target = _pt.get("si_deployed", {}).get("count")
    si_dep_target_str = f"{si_dep_target:,}" if si_dep_target is not None else "&mdash;"
    si_dep_att = _sp_attainment(si_dep["count"], si_dep_target)
    si_dep_pipeline = si_open["count"]
    si_dep_coverage = _sp_coverage(si_dep["count"], si_dep_pipeline, si_dep_target)

    # Row 5: SQL Server - Deployed EACV
    sql_dep = _ps.get("sqlserver_deployed", _empty_ps)
    sql_open = _ps.get("sqlserver_open", _empty_ps)
    sql_dep_target = _pt.get("sqlserver_deployed", {}).get("acv")
    sql_dep_target_str = fmt_currency(sql_dep_target) if sql_dep_target is not None else "&mdash;"
    sql_dep_att = _sp_attainment(sql_dep["acv"], sql_dep_target)
    sql_dep_pipeline = sql_open["acv"]
    sql_dep_coverage = _sp_coverage(sql_dep["acv"], sql_dep_pipeline, sql_dep_target)
    fy_label = cfg.get("fiscal_year_label", "FY27")
    prior_fy_label = cfg.get("prior_fy_label", "FY26")
    cc_accounts = cc_theater.get("cc_accounts", 0)
    cc_total_accounts = cc_theater.get("total_accounts", 0)
    cc_pct = cc_theater.get("pct", 0)
    cc_avg_users = cc_theater.get("avg_users", 0)
    cc_requests = cc_theater.get("requests", 0)
    cc_credits = cc_theater.get("credits", 0)
    cc_error = cc_theater.get("error", "")

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

<h3>Cortex Code CLI Usage (Last 90 Days)</h3>
{"<p style='color:red;'>CC Error: " + cc_error + "</p>" if cc_error else ""}
<table class="forecast-table" style="width: 750px;">
  <tr><th>Open Pipeline Accounts</th><th>Using CC CLI</th><th>Adoption %</th><th>Avg Daily Users / Acct</th><th>Requests (90D)</th><th>Credits (90D)</th></tr>
  <tr><td>{cc_total_accounts}</td><td>{cc_accounts}</td><td>{cc_pct}%</td><td>{cc_avg_users}</td><td>{cc_requests:,}</td><td>{cc_credits:,.0f}</td></tr>
</table>

<h3>Top 5 Use Cases Going Live This Quarter</h3>
<table class="use-case-table">
  <tr><th style="width:25%">Account / Use Case</th><th style="width:45%">Details</th><th style="width:30%">Summary</th></tr>
  {top5_rows}
</table>

<h3>Sales Play Performance</h3>
<table class="sales-play-table">
  <tr><th>Sales Play</th><th>Metric</th><th>Q1 Target</th><th>QTD Actual</th><th>Attainment</th><th>Pipeline</th><th>Coverage</th></tr>
  <tr><td><strong>Make Your Data AI Ready</strong></td><td>Use Cases Created</td><td class="number">{br_created_target_str}</td><td class="number">{br_created_actual:,}</td><td class="number">{br_created_att}</td><td class="number">&mdash;</td><td class="number">&mdash;</td></tr>
  <tr><td><strong>Make Your Data AI Ready</strong></td><td>DCT TBs Ingested</td><td class="number">{br_tb_target_str}</td><td class="number">{br_tb_actual:,.1f} TB</td><td class="number">{br_tb_att}</td><td class="number">&mdash;</td><td class="number">&mdash;</td></tr>
  <tr><td><strong>Snowflake Intelligence</strong></td><td>Use Cases Created</td><td class="number">{si_created_target_str}</td><td class="number">{si_created_actual:,}</td><td class="number">{si_created_att}</td><td class="number">&mdash;</td><td class="number">&mdash;</td></tr>
  <tr><td><strong>Snowflake Intelligence</strong></td><td>Use Cases Deployed</td><td class="number">{si_dep_target_str}</td><td class="number">{si_dep["count"]:,}</td><td class="number">{si_dep_att}</td><td class="number">{si_dep_pipeline:,}</td><td class="number">{si_dep_coverage}</td></tr>
  <tr><td><strong>SQL Server Migration</strong></td><td>Deployed EACV</td><td class="number">{sql_dep_target_str}</td><td class="number">{fmt_currency(sql_dep["acv"])}</td><td class="number">{sql_dep_att}</td><td class="number">{fmt_currency(sql_dep_pipeline)}</td><td class="number">{sql_dep_coverage}</td></tr>
</table>

<div class="play-section">
<h3>Bronze Ingest - Use Cases &ge;$500K</h3>
<div class="analysis"><strong>QTD TB Ingested:</strong> {bronze_tb_total:.1f} TB<br>
<strong>Average ACV:</strong> {fmt_currency(play_detail["bronze"]["avg_acv"])} | <strong>Median ACV:</strong> {fmt_currency(play_detail["bronze"]["median_acv"])} | across {play_detail["bronze"]["count"]} use cases.
<strong>Regional Coverage:</strong> {play_detail["bronze"]["regions"]} of 8 {_theater()} regions contributing.</div>
<div class="risk-box"><strong>Risk Summary ({bronze_risk["at_risk"]} of {bronze_risk["total"]} use cases, {fmt_currency(bronze_risk["acv_at_risk"])} ACV at risk):</strong><br>{bronze_risk["narrative_html"]}</div>
<table class="use-case-table"><tr><th style="width:25%">Account / Use Case</th><th style="width:45%">Details</th><th style="width:30%">Summary</th></tr>{bronze_rows}</table>
</div>

<div class="play-section">
<h3>Snowflake Intelligence - Use Cases &ge;$500K</h3>
<div class="analysis"><strong>Theater SI Usage (Last 30 Days):</strong> {si_theater["accounts"]:,} Accounts | {si_theater["users"]:,} Users | {si_theater["credits"]:,} Credits | {fmt_currency(si_theater["revenue"])} Revenue<br>
<strong>Average ACV:</strong> {fmt_currency(play_detail["si"]["avg_acv"])} | <strong>Median ACV:</strong> {fmt_currency(play_detail["si"]["median_acv"])} | across {play_detail["si"]["count"]} use cases.
<strong>Regional Coverage:</strong> {play_detail["si"]["regions"]} of 8 {_theater()} regions contributing.</div>
<div class="risk-box"><strong>Risk Summary ({si_risk["at_risk"]} of {si_risk["total"]} use cases, {fmt_currency(si_risk["acv_at_risk"])} ACV at risk):</strong><br>{si_risk["narrative_html"]}</div>
<table class="use-case-table"><tr><th style="width:25%">Account / Use Case</th><th style="width:45%">Details</th><th style="width:30%">Summary</th></tr>{si_rows}</table>
</div>

<div class="play-section">
<h3>SQL Server Migration - Use Cases &ge;$500K</h3>
<div class="analysis"><strong>Average ACV:</strong> {fmt_currency(play_detail["sqlserver"]["avg_acv"])} | <strong>Median ACV:</strong> {fmt_currency(play_detail["sqlserver"]["median_acv"])} | across {play_detail["sqlserver"]["count"]} use cases.
<strong>Regional Coverage:</strong> {play_detail["sqlserver"]["regions"]} of 8 {_theater()} regions contributing.</div>
<div class="risk-box"><strong>Risk Summary ({sql_risk["at_risk"]} of {sql_risk["total"]} use cases, {fmt_currency(sql_risk["acv_at_risk"])} ACV at risk):</strong><br>{sql_risk["narrative_html"]}</div>
<table class="use-case-table"><tr><th style="width:25%">Account / Use Case</th><th style="width:45%">Details</th><th style="width:30%">Summary</th></tr>{sql_rows}</table>
</div>
"""
    st.html(STREAMLIT_CSS + html_content)


def render_forecast_tab(data):
    is_current = data["_config"].get("is_current_quarter", True)
    fiscal = data["fiscal"]
    day_number = safe_int(fiscal["DAY_NUMBER"])
    if not is_current and day_number == 0:
        st.info("Forecast analysis is not available for future quarters — no deployment data to extrapolate from.")
        return
    forecast_analysis = data["forecast_analysis"]
    forecasts = data["forecasts"]
    deployed = data["deployed"]
    week_number = safe_int(fiscal["WEEK_NUMBER"])
    forecast_html = _build_forecast_tab(forecast_analysis, forecasts, deployed, day_number, week_number)
    st.html(STREAMLIT_CSS + forecast_html)


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
ALLOWED_USERS = {"NATHOMAS", "SVANDAAL", "MMEREDITH", "NTSUI", "ADANNA"}

def _check_access():
    """Verify the current user is in the approved access list."""
    allowed_emails = {
        "nate.thomas@snowflake.com",
        "saskia.vandaal@snowflake.com",
        "matt.meredith@snowflake.com",
        "nick.tsui@snowflake.com",
        "alex.danna@snowflake.com",
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
        selected_gvp = st.selectbox(
            "GVP", options=GVP_OPTIONS,
            index=GVP_OPTIONS.index("Mark Fleming"),
            help="Select GVP to view. Changing GVP reloads all data.",
        )
        # Quarter selector
        quarter_opts = _build_quarter_options()
        quarter_labels = [q["label"] for q in quarter_opts]
        current_q = _current_quarter_label()
        default_idx = quarter_labels.index(current_q) if current_q in quarter_labels else 0
        selected_quarter_label = st.selectbox(
            "Fiscal Quarter", options=quarter_labels, index=default_idx,
            help="Select fiscal quarter. Defaults to current quarter.",
        )
        selected_quarter = next(q for q in quarter_opts if q["label"] == selected_quarter_label)
        play_labels = list(PLAY_OPTIONS.keys())
        selected_play_label = st.selectbox(
            "Sales Play (Script tab only)", options=play_labels, index=0,
            key="play_selector",
            help="Select which Sales Play to feature in the Script tab.",
        )
        st.markdown("---")
        cache_key = f"peak_data_{selected_gvp}_{selected_quarter_label}"
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

    data = load_all_data(selected_gvp, selected_quarter)
    for k, v in data["_config"].items():
        if v is not None:
            CONFIG[k] = v

    fiscal = data["fiscal"]
    quarter = safe_str(fiscal["FISCAL_QUARTER"])
    qstart = safe_str(fiscal["FQ_START"])
    qend = safe_str(fiscal["FQ_END"])
    days_remaining = safe_int(fiscal["DAYS_REMAINING"])
    is_current = data["_config"].get("is_current_quarter", True)

    st.markdown(f"## PEAK Forecasting — {selected_gvp}")
    if is_current:
        st.markdown(f"**{CONFIG['fiscal_quarter']}** ({qstart} - {qend}) | **{days_remaining} days remaining**")
    elif days_remaining == 0:
        st.markdown(f"**{CONFIG['fiscal_quarter']}** ({qstart} - {qend}) | *Quarter complete*")
    else:
        from datetime import date
        days_until = (date.fromisoformat(qstart) - date.today()).days
        st.markdown(f"**{CONFIG['fiscal_quarter']}** ({qstart} - {qend}) | *Starts in {days_until} days*")

    tab_script, tab_golives, tab_forecast = st.tabs([
        "Script", "Use Case Go-Lives", "Forecast Analysis",
    ])

    selected_play_key = PLAY_OPTIONS[st.session_state.get("play_selector", play_labels[0])]

    with tab_script:
        render_script_tab(data, selected_play_key)
    with tab_golives:
        render_golives_tab(data)
    with tab_forecast:
        render_forecast_tab(data)


main()
