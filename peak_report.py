#!/usr/bin/env python3
"""
PEAK Qualify & Commit Report Generator
Generates HTML reports for AMSExpansion theater.

Usage:
    python3 peak_report.py
    python3 peak_report.py --quarter 2027-Q1  # override quarter dates

Output:
    ~/Desktop/PEAK_AMSExpansion_{QUARTER}.html
"""

import sys
import os
import html
from datetime import datetime, date
from collections import defaultdict
from decimal import Decimal

import re
import csv
import snowflake.connector

# =============================================================================
# CONFIGURATION
# =============================================================================

CONFIG = {
    "connection_name": "MyConnection",
    "warehouse": "SNOWADHOC",
    "gvp_name": "Mark Fleming",
    "gvp_function": "GVP",
    # quarter_start, quarter_end, fiscal_year, prior_fy_quarters, prior_fy_avg_final
    # are populated dynamically after connecting — see q_fiscal_context()
    "play_threshold": 500000,
    "top_n": 5,
    "salesforce_base_url": "https://snowforce.lightning.force.com/",
    # Hardcoded timeline defaults (overridden by q_use_case_velocity when available)
    "days_to_tw": 42,
    "days_to_imp": 25,
    "days_to_deploy": 79,
    # Sales play identification
    "bronze_campaign": "%Bronze Activation - Make Your Data AI Ready%",
    "sqlserver_campaign": "%SQL Server Migration - Modernize Your Data Estate%",
    "si_technical_use_case": "%AI: Snowflake Intelligence & Agents%",
    # Non-active exclusion (RAVEN stage names — no numeric prefix)
    "excluded_stages": ("Not In Pursuit", "Use Case Lost"),
    # Pipeline risk thresholds (DAYS_IN_STAGE + DAYS_REMAINING)
    # Note: Query 7 still uses DIM_USE_CASE for STAGE_NUMBER/DAYS_IN_STAGE
    "dim_excluded_stages": ("0 - Not In Pursuit", "8 - Use Case Lost"),
    # Stage name mappings (RAVEN uses text names, not numbers)
    "pursuit_stages": ("Discovery", "Scoping", "Technical / Business Validation"),
    "won_stages": ("Use Case Won / Migration Plan", "Implementation In Progress", "Implementation Complete", "Deployed"),
    "risk_thresholds": {
        "stage_123": 146,
        "stage_4": 104,
        "stage_5": 79,
    },
    "output_dir": os.path.expanduser("~/Desktop"),
    # Table references
    "raven_uc_table": "SALES.RAVEN.USE_CASE_EXPLORER_VH_DELIVERABLE_C",
    "raven_acct_table": "SALES.RAVEN.D_SALESFORCE_ACCOUNT_CUSTOMERS",
    "dim_uc_table": "MDM.MDM_INTERFACES.DIM_USE_CASE",
}

# Risk categories (from USE_CASE_RISK field, semicolon-delimited)
RISK_CATEGORIES = [
    "Technical Fit",
    "Time / Resources",
    "Competitor",
    "Access to the Customer",
    "Performance",
    "Consumption",
]


# =============================================================================
# FORMATTING HELPERS
# =============================================================================

def fmt_currency(value, compact=True):
    """Format a number as currency. compact=True gives $1.2M/$2.1B style."""
    if value is None:
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
    """Format as percentage."""
    if value is None:
        return "N/A"
    return f"{float(value):.1f}%"


def fmt_int(value):
    """Format as integer with commas."""
    if value is None:
        return "0"
    return f"{int(value):,}"


def safe_str(value):
    """Convert to string safely, return empty string for None."""
    if value is None:
        return ""
    return str(value).strip()


def html_escape(value):
    """Escape HTML entities."""
    return html.escape(safe_str(value))


def get_forecast_class(forecast_status):
    """Map GO_LIVE_FORECAST_STATUS to CSS class."""
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
    """Get display label for forecast status."""
    s = safe_str(forecast_status)
    if not s or s.lower() == "none":
        return "None"
    return s


def truncate_text(text, max_len=500):
    """Truncate long text for display."""
    s = safe_str(text)
    if len(s) > max_len:
        return s[:max_len] + "..."
    return s


# Pattern to match date separators in SE_COMMENTS / NEXT_STEPS.
# Matches lines starting with common date formats:
#   2026-02-19, 02/19/26, 2/19/26, 12/8/25, 2026-01-12, etc.
# Also: "2/23/2026 -", "10/7/25 -", "**20250619", "[2026-02-06"
_DATE_LINE_RE = re.compile(
    r'^\s*(?:[A-Z]{1,4}[\s:\-]*)?(?:'
    r'\[?\*{0,2}\d{4}[-/]\d{2}[-/]\d{2}'   # 2026-02-19, **20250619
    r'|\[?\d{1,2}[-/]\d{1,2}[-/]\d{2,4}'    # 2/19/26, 12/8/25, 02/19/26
    r'|\d{4}\d{4}\s'                          # 20250827 NM
    r')',
    re.MULTILINE
)


def extract_latest_comment(text):
    """
    Extract only the latest (first) SE comment from a date-separated comment log.
    Comments are separated by date prefixes like "2026-02-19 [RC]: ..." or "2/19/26 ...".
    Returns the first comment block only.
    """
    s = safe_str(text).strip()
    if not s:
        return ""

    # Find all date-line positions
    matches = list(_DATE_LINE_RE.finditer(s))

    if len(matches) <= 1:
        # Zero or one date prefix — the whole thing is one comment
        return s

    # The first match is the start of the latest comment.
    # The second match is the start of the next (older) comment.
    latest = s[matches[0].start():matches[1].start()].strip()
    return latest


def update_risk_thresholds_from_velocity(velocity):
    """Update CONFIG risk_thresholds and days_to_* from velocity averages.

    Threshold logic:
      - Stage 1-3: need TW + imp start + deploy time
      - Stage 4:   need imp start + deploy time
      - Stage 5:   need deploy time only
    Falls back to existing CONFIG defaults when a velocity value is None.
    """
    tw = velocity["current"].get("time_to_tw")
    imp = velocity["current"].get("won_to_imp_start")
    dep = velocity["current"].get("won_to_deployed")

    tw = int(round(tw)) if tw is not None else CONFIG["days_to_tw"]
    imp = int(round(imp)) if imp is not None else CONFIG["days_to_imp"]
    dep = int(round(dep)) if dep is not None else CONFIG["days_to_deploy"]

    CONFIG["days_to_tw"] = tw
    CONFIG["days_to_imp"] = imp
    CONFIG["days_to_deploy"] = dep
    CONFIG["risk_thresholds"] = {
        "stage_123": tw + imp + dep,
        "stage_4": imp + dep,
        "stage_5": dep,
    }


# =============================================================================
# SNOWFLAKE CONNECTION
# =============================================================================

def get_connection():
    """Create Snowflake connection using connections.toml config."""
    print("Connecting to Snowflake (browser auth)...")
    conn = snowflake.connector.connect(
        connection_name=CONFIG["connection_name"],
    )
    cursor = conn.cursor()
    cursor.execute(f"USE WAREHOUSE {CONFIG['warehouse']}")
    cursor.close()
    print(f"  Connected. Warehouse: {CONFIG['warehouse']}")
    return conn


def run_query(conn, sql, description=""):
    """Execute a query and return results as list of dicts."""
    if description:
        print(f"  Running: {description}...")
    cursor = conn.cursor(snowflake.connector.DictCursor)
    cursor.execute(sql)
    results = cursor.fetchall()
    cursor.close()
    if description:
        print(f"    → {len(results)} rows")
    return results


# =============================================================================
# SQL QUERIES
# =============================================================================

def q_fiscal_calendar(conn):
    """Query 1: Fiscal Calendar + dynamic config derivation.
    
    Returns the fiscal calendar row for today AND populates CONFIG with:
      - quarter_start, quarter_end (current quarter boundaries)
      - fiscal_year (e.g., 2027)
      - fiscal_year_label (e.g., "FY27")
      - prior_fy_label (e.g., "FY26")
      - prior_fy_quarters (list of (start, end) tuples for prior FY)
      - prior_fy_avg_final (average quarterly deployed ACV for prior FY, in $M)
    """
    rows = run_query(conn, """
        SELECT FISCAL_QUARTER, FISCAL_YEAR, FQ_START, FQ_END,
               DATEDIFF('day', CURRENT_DATE(), FQ_END) + 1 as DAYS_REMAINING,
               DATEDIFF('day', FQ_START, CURRENT_DATE()) + 1 as DAY_NUMBER,
               WEEK_OF_FQ as WEEK_NUMBER
        FROM SALES.REPORTING.CORE_FISCAL_DATES
        WHERE DATE_ = CURRENT_DATE()
        LIMIT 1
    """, "Query 1: Fiscal Calendar")
    assert len(rows) == 1, f"Expected 1 fiscal calendar row, got {len(rows)}"
    fiscal = rows[0]

    fy = int(fiscal["FISCAL_YEAR"])
    fq_start = safe_str(fiscal["FQ_START"]).strip('"')
    fq_end = safe_str(fiscal["FQ_END"]).strip('"')

    # Populate current quarter dates
    CONFIG["quarter_start"] = fq_start
    CONFIG["quarter_end"] = fq_end
    CONFIG["fiscal_year"] = fy
    CONFIG["fiscal_year_label"] = f"FY{fy % 100}"       # e.g., "FY27"
    CONFIG["prior_fy_label"] = f"FY{(fy - 1) % 100}"    # e.g., "FY26"

    print(f"    → {fiscal['FISCAL_QUARTER']}: {fq_start} to {fq_end}, FY{fy % 100}")

    # Derive prior FY quarter date ranges
    prior_rows = run_query(conn, f"""
        SELECT DISTINCT FQ_START, FQ_END
        FROM SALES.REPORTING.CORE_FISCAL_DATES
        WHERE FISCAL_YEAR = {fy - 1}
        ORDER BY FQ_START
    """, "Query 1b: Prior FY Quarter Dates")

    prior_quarters = []
    for r in prior_rows:
        qs = safe_str(r["FQ_START"]).strip('"')
        qe = safe_str(r["FQ_END"]).strip('"')
        prior_quarters.append((qs, qe))
    CONFIG["prior_fy_quarters"] = prior_quarters

    # Compute prior FY average final deployed ACV
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
        avg_rows = run_query(conn, f"""
            SELECT ROUND(AVG(DEPLOYED_ACV) / 1000000, 1) as AVG_FINAL_M
            FROM ({' UNION ALL '.join(unions)})
        """, "Query 1c: Prior FY Avg Final")
        CONFIG["prior_fy_avg_final"] = float(avg_rows[0]["AVG_FINAL_M"] or 0)
    else:
        CONFIG["prior_fy_avg_final"] = 0

    print(f"    → Prior FY ({CONFIG['prior_fy_label']}): {len(prior_quarters)} quarters, avg final {CONFIG['prior_fy_avg_final']}M")

    return fiscal


def q_qtd_revenue(conn):
    """Query 2: QTD Revenue + Full FY Forecast"""
    # Current quarter metrics
    rows = run_query(conn, f"""
        SELECT FORECAST_TYPE, FORECAST_AMOUNT
        FROM SALES.REPORTING.PEAK_FORECAST_CALLS_PIPELINE_TARGETS
        WHERE USER_NAME = '{CONFIG["gvp_name"]}'
          AND FUNCTION = '{CONFIG["gvp_function"]}'
          AND TYPE = 'Consumption'
          AND CURRENT_FISCAL_QUARTER = TRUE
          AND LATEST_DATE = TRUE
    """, "Query 2a: QTD Revenue")
    assert len(rows) == 3, f"Expected 3 revenue rows (Actual/Predicted/Total), got {len(rows)}"

    revenue_map = {r["FORECAST_TYPE"]: float(r["FORECAST_AMOUNT"]) for r in rows}

    # Full FY forecast (dynamic — uses current fiscal year from calendar)
    fy = CONFIG["fiscal_year"]
    fy_label = CONFIG["fiscal_year_label"]
    fy_rows = run_query(conn, f"""
        SELECT SUM(FORECAST_AMOUNT) as FY_FORECAST
        FROM SALES.REPORTING.PEAK_FORECAST_CALLS_PIPELINE_TARGETS
        WHERE USER_NAME = '{CONFIG["gvp_name"]}'
          AND FUNCTION = '{CONFIG["gvp_function"]}'
          AND TYPE = 'Consumption'
          AND FORECAST_TYPE = 'Total'
          AND LATEST_DATE = TRUE
          AND FISCAL_QUARTER LIKE '{fy}%'
    """, f"Query 2b: {fy_label} Forecast")
    assert len(fy_rows) == 1 and fy_rows[0]["FY_FORECAST"], f"{fy_label} forecast query failed"

    return {
        "revenue": revenue_map.get("Actual", 0),
        "target": "TBD",
        "pct_target": "TBD",
        "q1_forecast": revenue_map.get("Total", 0),
        "fy_forecast": float(fy_rows[0]["FY_FORECAST"]),
    }


def q_forecast_calls(conn):
    """Query 3: Forecast Calls (Commit/Most Likely/Stretch) + Target"""
    rows = run_query(conn, f"""
        SELECT FORECAST_TYPE, FORECAST_AMOUNT, LATEST_DATE, PREVIOUS_WEEK
        FROM SALES.REPORTING.PEAK_FORECAST_CALLS_PIPELINE_TARGETS
        WHERE USER_NAME = '{CONFIG["gvp_name"]}'
          AND FUNCTION = '{CONFIG["gvp_function"]}'
          AND TYPE = 'Use Case Go-Lives'
          AND FORECAST_TYPE IN ('CommitForecast', 'MostLikelyForecast', 'BestCaseForecast', 'Target')
          AND CURRENT_FISCAL_QUARTER = TRUE
          AND (LATEST_DATE = TRUE OR PREVIOUS_WEEK = TRUE)
    """, "Query 3: Forecast Calls")
    current = {}
    prior = {}
    for r in rows:
        if r["LATEST_DATE"]:
            current[r["FORECAST_TYPE"]] = float(r["FORECAST_AMOUNT"])
        if r["PREVIOUS_WEEK"]:
            prior[r["FORECAST_TYPE"]] = float(r["FORECAST_AMOUNT"])
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


def q_wins_forecast_calls(conn):
    """Query 3b: Wins Forecast Calls (Commit/Most Likely/Stretch) + Target"""
    rows = run_query(conn, f"""
        SELECT FORECAST_TYPE, FORECAST_AMOUNT, LATEST_DATE, PREVIOUS_WEEK
        FROM SALES.REPORTING.PEAK_FORECAST_CALLS_PIPELINE_TARGETS
        WHERE USER_NAME = '{CONFIG["gvp_name"]}'
          AND FUNCTION = '{CONFIG["gvp_function"]}'
          AND TYPE = 'Use Case Wins'
          AND FORECAST_TYPE IN ('CommitForecast', 'MostLikelyForecast', 'BestCaseForecast')
          AND CURRENT_FISCAL_QUARTER = TRUE
          AND (LATEST_DATE = TRUE OR PREVIOUS_WEEK = TRUE)
    """, "Query 3b: Wins Forecast Calls")
    current = {}
    prior = {}
    for r in rows:
        if r["LATEST_DATE"]:
            current[r["FORECAST_TYPE"]] = float(r["FORECAST_AMOUNT"])
        if r["PREVIOUS_WEEK"]:
            prior[r["FORECAST_TYPE"]] = float(r["FORECAST_AMOUNT"])
    # Target is under TYPE = 'Use Case Won'
    target_rows = run_query(conn, f"""
        SELECT FORECAST_AMOUNT
        FROM SALES.REPORTING.PEAK_FORECAST_CALLS_PIPELINE_TARGETS
        WHERE USER_NAME = '{CONFIG["gvp_name"]}'
          AND FUNCTION = '{CONFIG["gvp_function"]}'
          AND TYPE = 'Use Case Won'
          AND FORECAST_TYPE = 'Target'
          AND CURRENT_FISCAL_QUARTER = TRUE
          AND LATEST_DATE = TRUE
    """, "Query 3b: Wins Target")
    target = float(target_rows[0]["FORECAST_AMOUNT"]) if target_rows else 0
    commit = current.get("CommitForecast", 0)
    ml = current.get("MostLikelyForecast", 0)
    stretch = current.get("BestCaseForecast", 0)
    return {
        "commit": commit,
        "most_likely": ml,
        "stretch": stretch,
        "target": target,
        "commit_delta": commit - prior["CommitForecast"] if "CommitForecast" in prior else None,
        "ml_delta": ml - prior["MostLikelyForecast"] if "MostLikelyForecast" in prior else None,
        "stretch_delta": stretch - prior["BestCaseForecast"] if "BestCaseForecast" in prior else None,
    }


def q_deployed_qtd(conn):
    """Query 4: Deployed QTD"""
    rows = run_query(conn, f"""
        SELECT SUM(u.USE_CASE_ACV) as DEPLOYED_ACV, COUNT(*) as DEPLOYED_COUNT
        FROM {CONFIG["raven_uc_table"]} u
        JOIN {CONFIG["raven_acct_table"]} a ON u.VH_ACCOUNT_C = a.SALESFORCE_ACCOUNT_ID
        WHERE a.GVP = '{CONFIG["gvp_name"]}'
          AND u.USE_CASE_ACV > 0
          AND u.IS_WENT_LIVE = TRUE
          AND u.DEFAULT_DATE BETWEEN '{CONFIG["quarter_start"]}' AND '{CONFIG["quarter_end"]}'
    """, "Query 4: Deployed QTD")
    r = rows[0]
    return {
        "acv": float(r["DEPLOYED_ACV"] or 0),
        "count": int(r["DEPLOYED_COUNT"] or 0),
    }


def q_last7_deployed(conn):
    """Query 5: Last 7 Days Deployed"""
    rows = run_query(conn, f"""
        SELECT SUM(u.USE_CASE_ACV) as LAST7_ACV, COUNT(*) as LAST7_COUNT
        FROM {CONFIG["raven_uc_table"]} u
        JOIN {CONFIG["raven_acct_table"]} a ON u.VH_ACCOUNT_C = a.SALESFORCE_ACCOUNT_ID
        WHERE a.GVP = '{CONFIG["gvp_name"]}'
          AND u.USE_CASE_ACV > 0
          AND u.IS_WENT_LIVE = TRUE
          AND u.DEFAULT_DATE >= DATEADD('day', -7, CURRENT_DATE())
    """, "Query 5: Last 7 Days")
    r = rows[0]
    return {
        "acv": float(r["LAST7_ACV"] or 0),
        "count": int(r["LAST7_COUNT"] or 0),
    }


def q_deployment_velocity(conn):
    """Query 5b: Deployment velocity — trailing 7/14/30 day windows + historical comparison."""
    snapshot_table = "SALES.DEV.DIM_USE_CASE_HISTORY_DS"
    quarters = CONFIG["prior_fy_quarters"]

    # Current velocity from RAVEN
    current_sql = f"""
        SELECT
            SUM(CASE WHEN u.DEFAULT_DATE >= DATEADD('day', -7, CURRENT_DATE()) THEN u.USE_CASE_ACV ELSE 0 END) as V7,
            SUM(CASE WHEN u.DEFAULT_DATE >= DATEADD('day', -14, CURRENT_DATE()) THEN u.USE_CASE_ACV ELSE 0 END) as V14,
            SUM(CASE WHEN u.DEFAULT_DATE >= DATEADD('day', -30, CURRENT_DATE()) THEN u.USE_CASE_ACV ELSE 0 END) as V30
        FROM {CONFIG["raven_uc_table"]} u
        JOIN {CONFIG["raven_acct_table"]} a ON u.VH_ACCOUNT_C = a.SALESFORCE_ACCOUNT_ID
        WHERE a.GVP = '{CONFIG["gvp_name"]}'
          AND u.USE_CASE_ACV > 0
          AND u.IS_WENT_LIVE = TRUE
          AND u.DEFAULT_DATE BETWEEN '{CONFIG["quarter_start"]}' AND '{CONFIG["quarter_end"]}'
    """
    cur = run_query(conn, current_sql, "Query 5b: Current Velocity")
    current = cur[0] if cur else {}

    # Historical velocity at same day-N from snapshot data
    day_num = int(CONFIG.get("day_number", 31))
    hist_velocities = []
    for i, (qs, qe) in enumerate(quarters):
        q_label = f"{CONFIG['prior_fy_label']} Q{i+1}"
        snap_date = f"DATEADD('day', {day_num - 1}, '{qs}')::DATE"
        snap7 = f"DATEADD('day', {day_num - 8}, '{qs}')::DATE"
        snap14 = f"DATEADD('day', {day_num - 15}, '{qs}')::DATE"
        snap30 = f"DATEADD('day', {day_num - 31}, '{qs}')::DATE"
        hist_sql = f"""
            SELECT
                '{q_label}' as QTR,
                SUM(CASE WHEN h.ACTUAL_USE_CASE_DEPLOYMENT_DATE > {snap7}
                         AND h.ACTUAL_USE_CASE_DEPLOYMENT_DATE <= {snap_date}
                         AND h.IS_DEPLOYED = TRUE
                    THEN h.USE_CASE_EACV ELSE 0 END) as V7,
                SUM(CASE WHEN h.ACTUAL_USE_CASE_DEPLOYMENT_DATE > {snap14}
                         AND h.ACTUAL_USE_CASE_DEPLOYMENT_DATE <= {snap_date}
                         AND h.IS_DEPLOYED = TRUE
                    THEN h.USE_CASE_EACV ELSE 0 END) as V14,
                SUM(CASE WHEN h.ACTUAL_USE_CASE_DEPLOYMENT_DATE > {snap30}
                         AND h.ACTUAL_USE_CASE_DEPLOYMENT_DATE <= {snap_date}
                         AND h.IS_DEPLOYED = TRUE
                    THEN h.USE_CASE_EACV ELSE 0 END) as V30
            FROM {snapshot_table} h
            WHERE h.DS = {snap_date}
              AND h.ACCOUNT_GVP = '{CONFIG["gvp_name"]}'
              AND h.USE_CASE_EACV > 0
        """
        rows = run_query(conn, hist_sql, f"Query 5b: {q_label} Velocity")
        if rows and (float(rows[0].get("V7", 0) or 0) > 0 or float(rows[0].get("V30", 0) or 0) > 0):
            rows[0]["QTR"] = q_label
            hist_velocities.append(rows[0])

    return {
        "current": {
            "v7": float(current.get("V7", 0) or 0),
            "v14": float(current.get("V14", 0) or 0),
            "v30": float(current.get("V30", 0) or 0),
        },
        "historical": hist_velocities,
    }


def q_risk_adjusted_pipeline_detail(conn):
    """Query 5c: Top 15 open use cases by ACV with risk assessment for forecast tab."""
    excluded = ", ".join(f"'{s}'" for s in CONFIG["dim_excluded_stages"])
    dim_theater = "(u.THEATER_NAME = 'AMSExpansion' OR u.ACCOUNT_GVP = 'Mark Fleming')"
    rows = run_query(conn, f"""
        WITH fiscal_qtr AS (
            SELECT FQ_START, FQ_END,
                   DATEDIFF('day', CURRENT_DATE(), FQ_END) + 1 as DAYS_REMAINING
            FROM SALES.REPORTING.CORE_FISCAL_DATES
            WHERE DATE_ = CURRENT_DATE()
        )
        SELECT
            u.USE_CASE_ID,
            u.USE_CASE_NAME,
            u.ACCOUNT_NAME,
            u.USE_CASE_EACV,
            u.STAGE_NUMBER,
            u.USE_CASE_STAGE,
            u.DAYS_IN_STAGE,
            u.GO_LIVE_DATE,
            u.TECHNICAL_WIN_DATE,
            f.DAYS_REMAINING,
            CASE
                WHEN u.STAGE_NUMBER = 6 THEN 'Good'
                WHEN u.STAGE_NUMBER = 5 AND (u.DAYS_IN_STAGE + f.DAYS_REMAINING) >= {CONFIG["risk_thresholds"]["stage_5"]} THEN 'Good'
                WHEN u.STAGE_NUMBER = 4 AND (u.DAYS_IN_STAGE + f.DAYS_REMAINING) >= {CONFIG["risk_thresholds"]["stage_4"]} THEN 'Good'
                WHEN u.STAGE_NUMBER IN (1,2,3) AND (u.DAYS_IN_STAGE + f.DAYS_REMAINING) >= {CONFIG["risk_thresholds"]["stage_123"]} THEN 'Good'
                ELSE 'At Risk'
            END AS RISK_STATUS
        FROM {CONFIG["dim_uc_table"]} u
        CROSS JOIN fiscal_qtr f
        WHERE {dim_theater}
            AND u.USE_CASE_EACV > 0
            AND u.IS_DEPLOYED = FALSE
            AND u.STAGE_NUMBER BETWEEN 1 AND 6
            AND u.USE_CASE_STAGE NOT IN ({excluded})
            AND u.GO_LIVE_DATE BETWEEN f.FQ_START AND f.FQ_END
        ORDER BY u.USE_CASE_EACV DESC
        LIMIT 15
    """, "Query 5c: Risk-Adjusted Pipeline Detail")
    return rows


# ---------------------------------------------------------------------------
# WINS TAB QUERIES (filtered by DECISION_DATE instead of GO_LIVE_DATE/DEFAULT_DATE)
# ---------------------------------------------------------------------------

def q_won_qtd(conn):
    """Won QTD: Use cases with decision date in quarter and stage 4+ (won)."""
    won_stages = ", ".join(f"'{s}'" for s in CONFIG["won_stages"])
    rows = run_query(conn, f"""
        SELECT COALESCE(SUM(u.USE_CASE_ACV), 0) as WON_ACV, COUNT(*) as WON_COUNT
        FROM {CONFIG["raven_uc_table"]} u
        JOIN {CONFIG["raven_acct_table"]} a ON u.VH_ACCOUNT_C = a.SALESFORCE_ACCOUNT_ID
        WHERE a.GVP = '{CONFIG["gvp_name"]}'
          AND u.USE_CASE_ACV > 0
          AND u.IS_LOST = FALSE
          AND u.DECISION_DATE BETWEEN '{CONFIG["quarter_start"]}' AND '{CONFIG["quarter_end"]}'
          AND u.USE_CASE_STAGE IN ({won_stages})
    """, "Query W1: Won QTD")
    r = rows[0]
    return {
        "acv": float(r["WON_ACV"] or 0),
        "count": int(r["WON_COUNT"] or 0),
    }


def q_won_last7(conn):
    """Won Last 7 Days: Use cases with decision date in last 7 days and stage 4+."""
    won_stages = ", ".join(f"'{s}'" for s in CONFIG["won_stages"])
    rows = run_query(conn, f"""
        SELECT COALESCE(SUM(u.USE_CASE_ACV), 0) as WON_ACV, COUNT(*) as WON_COUNT
        FROM {CONFIG["raven_uc_table"]} u
        JOIN {CONFIG["raven_acct_table"]} a ON u.VH_ACCOUNT_C = a.SALESFORCE_ACCOUNT_ID
        WHERE a.GVP = '{CONFIG["gvp_name"]}'
          AND u.USE_CASE_ACV > 0
          AND u.IS_LOST = FALSE
          AND u.DECISION_DATE >= DATEADD('day', -7, CURRENT_DATE())
          AND u.DECISION_DATE <= CURRENT_DATE()
          AND u.USE_CASE_STAGE IN ({won_stages})
    """, "Query W2: Won Last 7 Days")
    r = rows[0]
    return {
        "acv": float(r["WON_ACV"] or 0),
        "count": int(r["WON_COUNT"] or 0),
    }


def q_wins_in_pursuit(conn):
    """In-Pursuit Pipeline: Decision date in quarter, stage 1-3, not lost."""
    pursuit_stages = ", ".join(f"'{s}'" for s in CONFIG["pursuit_stages"])
    rows = run_query(conn, f"""
        SELECT COALESCE(SUM(u.USE_CASE_ACV), 0) as PURSUIT_ACV, COUNT(*) as PURSUIT_COUNT
        FROM {CONFIG["raven_uc_table"]} u
        JOIN {CONFIG["raven_acct_table"]} a ON u.VH_ACCOUNT_C = a.SALESFORCE_ACCOUNT_ID
        WHERE a.GVP = '{CONFIG["gvp_name"]}'
          AND u.USE_CASE_ACV > 0
          AND u.IS_LOST = FALSE
          AND u.IS_WENT_LIVE = FALSE
          AND u.DECISION_DATE BETWEEN '{CONFIG["quarter_start"]}' AND '{CONFIG["quarter_end"]}'
          AND u.USE_CASE_STAGE IN ({pursuit_stages})
    """, "Query W3: In-Pursuit Pipeline")
    r = rows[0]
    return {
        "acv": float(r["PURSUIT_ACV"] or 0),
        "count": int(r["PURSUIT_COUNT"] or 0),
    }


def q_wins_top5_pursuit(conn):
    """Top 5 In-Pursuit Use Cases by ACV (decision date in quarter, stage 1-3)."""
    pursuit_stages = ", ".join(f"'{s}'" for s in CONFIG["pursuit_stages"])
    rows = run_query(conn, f"""
        SELECT {_use_case_select_cols()},
               u.DECISION_DATE
        FROM {CONFIG["raven_uc_table"]} u
        JOIN {CONFIG["raven_acct_table"]} a ON u.VH_ACCOUNT_C = a.SALESFORCE_ACCOUNT_ID
        WHERE a.GVP = '{CONFIG["gvp_name"]}'
          AND u.USE_CASE_ACV > 0
          AND u.IS_LOST = FALSE
          AND u.IS_WENT_LIVE = FALSE
          AND u.DECISION_DATE BETWEEN '{CONFIG["quarter_start"]}' AND '{CONFIG["quarter_end"]}'
          AND u.USE_CASE_STAGE IN ({pursuit_stages})
        ORDER BY u.USE_CASE_ACV DESC
        LIMIT {CONFIG["top_n"]}
    """, "Query W4: Top 5 In-Pursuit Use Cases")
    return rows


def _wins_play_summary_query(conn, play_name, extra_join, filter_clause, is_won):
    """Helper for wins play summary: won (stage 4+) vs in-pursuit (stage 1-3)."""
    if is_won:
        won_stages = ", ".join(f"'{s}'" for s in CONFIG["won_stages"])
        stage_filter = f"AND u.USE_CASE_STAGE IN ({won_stages})"
    else:
        pursuit_stages = ", ".join(f"'{s}'" for s in CONFIG["pursuit_stages"])
        stage_filter = f"AND u.USE_CASE_STAGE IN ({pursuit_stages})"

    sql = f"""
        SELECT COALESCE(SUM(u.USE_CASE_ACV), 0) as ACV, COUNT(*) as COUNT_
        FROM {CONFIG["raven_uc_table"]} u
        JOIN {CONFIG["raven_acct_table"]} a ON u.VH_ACCOUNT_C = a.SALESFORCE_ACCOUNT_ID
        {extra_join}
        WHERE a.GVP = '{CONFIG["gvp_name"]}'
          AND u.USE_CASE_ACV > 0
          AND u.IS_LOST = FALSE
          AND u.DECISION_DATE BETWEEN '{CONFIG["quarter_start"]}' AND '{CONFIG["quarter_end"]}'
          {stage_filter}
          AND {filter_clause}
    """
    rows = run_query(conn, sql)
    r = rows[0]
    return {"acv": float(r["ACV"] or 0), "count": int(r["COUNT_"] or 0)}


def q_wins_play_summary(conn):
    """Sales Play Summary for Wins tab (won vs in-pursuit by decision date)."""
    print("  Running: Query W5: Wins Play Summary...")
    dim_join = f"JOIN {CONFIG['dim_uc_table']} d ON u.ID = d.USE_CASE_ID"

    bronze_filter = f"u.TECHNICAL_CAMPAIGN_S_C ILIKE '{CONFIG['bronze_campaign']}'"
    sql_filter = f"u.TECHNICAL_CAMPAIGN_S_C ILIKE '{CONFIG['sqlserver_campaign']}'"
    si_filter = f"d.TECHNICAL_USE_CASE ILIKE '{CONFIG['si_technical_use_case']}'"

    result = {}
    for play_key, play_name, extra, filt in [
        ("bronze", "Bronze", "", bronze_filter),
        ("si", "SI", dim_join, si_filter),
        ("sqlserver", "SQL Server", "", sql_filter),
    ]:
        result[f"{play_key}_won"] = _wins_play_summary_query(conn, play_name, extra, filt, is_won=True)
        result[f"{play_key}_pursuit"] = _wins_play_summary_query(conn, play_name, extra, filt, is_won=False)
        print(f"    → {play_name}: Won {result[f'{play_key}_won']['count']}/{fmt_currency(result[f'{play_key}_won']['acv'])}, "
              f"In Pursuit {result[f'{play_key}_pursuit']['count']}/{fmt_currency(result[f'{play_key}_pursuit']['acv'])}")
    return result


def _wins_play_use_cases_query(conn, play_name, extra_join, filter_clause):
    """Play use cases ≥$500K for wins tab (in-pursuit: decision date in quarter, stage 1-3)."""
    pursuit_stages = ", ".join(f"'{s}'" for s in CONFIG["pursuit_stages"])

    sql = f"""
        SELECT {_use_case_select_cols()},
               u.DECISION_DATE
        FROM {CONFIG["raven_uc_table"]} u
        JOIN {CONFIG["raven_acct_table"]} a ON u.VH_ACCOUNT_C = a.SALESFORCE_ACCOUNT_ID
        {extra_join}
        WHERE a.GVP = '{CONFIG["gvp_name"]}'
          AND u.USE_CASE_ACV >= {CONFIG["play_threshold"]}
          AND u.IS_LOST = FALSE
          AND u.IS_WENT_LIVE = FALSE
          AND u.DECISION_DATE BETWEEN '{CONFIG["quarter_start"]}' AND '{CONFIG["quarter_end"]}'
          AND u.USE_CASE_STAGE IN ({pursuit_stages})
          AND {filter_clause}
        ORDER BY u.USE_CASE_ACV DESC
    """
    return run_query(conn, sql, f"Query W6: {play_name} In-Pursuit ≥$500K")


def q_wins_play_use_cases(conn):
    """Use Cases by Sales Play for Wins tab (in-pursuit, ≥$500K)."""
    dim_join = f"JOIN {CONFIG['dim_uc_table']} d ON u.ID = d.USE_CASE_ID"
    return {
        "bronze": _wins_play_use_cases_query(conn, "Bronze", "", f"u.TECHNICAL_CAMPAIGN_S_C ILIKE '{CONFIG['bronze_campaign']}'"),
        "si": _wins_play_use_cases_query(conn, "SI", dim_join, f"d.TECHNICAL_USE_CASE ILIKE '{CONFIG['si_technical_use_case']}'"),
        "sqlserver": _wins_play_use_cases_query(conn, "SQL Server", "", f"u.TECHNICAL_CAMPAIGN_S_C ILIKE '{CONFIG['sqlserver_campaign']}'"),
    }


def q_play_targets(conn):
    """Query: Sales Play Deployed Targets from prioritized features table."""
    rows = run_query(conn, """
        SELECT PRIORITIZED_FEATURE_UC, TARGET_USE_CASE_EACV, TARGET_USE_CASE_COUNT
        FROM SALES.REPORTING.SALES_PROGRAM_PRIORITIZED_FEATURES_TARGETS
        WHERE MAPPED_THEATER = 'AMSExpansion'
          AND FISCAL_QUARTER = 'FY2027-Q1'
          AND MOVEMENT_TYPE = 'Deployed'
          AND PRIORITIZED_FEATURE_UC IN (
              'Make Your Data AI Ready',
              'Modernize Your Data Estate',
              'AI: Snowflake Intelligence & Agents'
          )
    """, "Query: Play Deployed Targets")
    mapping = {
        "Make Your Data AI Ready": "bronze",
        "Modernize Your Data Estate": "sqlserver",
        "AI: Snowflake Intelligence & Agents": "si",
    }
    targets = {}
    for r in rows:
        key = mapping.get(r["PRIORITIZED_FEATURE_UC"])
        if key:
            targets[key] = {
                "acv": float(r["TARGET_USE_CASE_EACV"]) if r["TARGET_USE_CASE_EACV"] else None,
                "count": int(r["TARGET_USE_CASE_COUNT"]) if r["TARGET_USE_CASE_COUNT"] else None,
            }
    # Ensure all plays have an entry
    for k in ("bronze", "si", "sqlserver"):
        if k not in targets:
            targets[k] = {"acv": None, "count": None}
    return targets


def q_partner_sd_attach(conn):
    """Query: Partner and SD attach rates on open go-live pipeline."""
    excluded = ", ".join(f"'{s}'" for s in CONFIG["excluded_stages"])
    rows = run_query(conn, f"""
        SELECT
            u.IMPLEMENTER_C,
            COUNT(*) as CNT,
            SUM(u.USE_CASE_ACV) as ACV
        FROM {CONFIG["raven_uc_table"]} u
        JOIN {CONFIG["raven_acct_table"]} a ON u.VH_ACCOUNT_C = a.SALESFORCE_ACCOUNT_ID
        WHERE a.GVP = '{CONFIG["gvp_name"]}'
          AND u.USE_CASE_ACV > 0
          AND u.IS_WENT_LIVE = FALSE
          AND u.IS_LOST = FALSE
          AND u.GO_LIVE_DATE BETWEEN '{CONFIG["quarter_start"]}' AND '{CONFIG["quarter_end"]}'
          AND u.USE_CASE_STAGE NOT IN ({excluded})
        GROUP BY u.IMPLEMENTER_C
    """, "Query: Partner/SD Attach")
    total_acv = 0
    total_count = 0
    partner_acv = 0
    partner_count = 0
    sd_acv = 0
    sd_count = 0
    unassisted_acv = 0
    unassisted_count = 0
    partner_values = {"Partner Only", "Partner Prime + Snowflake SD", "Snowflake SD Prime + Partner"}
    sd_values = {"Snowflake SD Prime", "Partner Prime + Snowflake SD",
                 "Customer Prime + Snowflake SD", "Snowflake SD Prime + Partner"}
    unassisted_values = {"Customer Only", "Unknown", "None", "", None}
    for r in rows:
        acv = float(r["ACV"] or 0)
        cnt = int(r["CNT"] or 0)
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
    return {
        "total_acv": total_acv, "total_count": total_count,
        "partner_acv": partner_acv, "partner_count": partner_count, "partner_rate": partner_rate,
        "sd_acv": sd_acv, "sd_count": sd_count, "sd_rate": sd_rate,
        "unassisted_acv": unassisted_acv, "unassisted_count": unassisted_count,
    }


def q_use_case_velocity(conn):
    """Query: Use case velocity averages for CQ go-lives + prior quarter."""
    def _query_velocity(start, end, label):
        rows = run_query(conn, f"""
            SELECT
                AVG(d.TIME_TO_TECH_WIN) as AVG_TW,
                AVG(CASE WHEN DATEDIFF('day', d.ACTUAL_USE_CASE_WON_DATE, d.IMPLEMENTATION_START_DATE) >= 0
                     THEN DATEDIFF('day', d.ACTUAL_USE_CASE_WON_DATE, d.IMPLEMENTATION_START_DATE) END) as AVG_WON_TO_IMP,
                AVG(d.TIME_WON_TO_DEPLOYED) as AVG_WON_TO_DEPLOYED
            FROM {CONFIG["raven_uc_table"]} u
            JOIN {CONFIG["raven_acct_table"]} a ON u.VH_ACCOUNT_C = a.SALESFORCE_ACCOUNT_ID
            JOIN {CONFIG["dim_uc_table"]} d ON u.ID = d.USE_CASE_ID
            WHERE a.GVP = '{CONFIG["gvp_name"]}'
              AND u.USE_CASE_ACV > 0
              AND u.IS_WENT_LIVE = TRUE
              AND u.DEFAULT_DATE BETWEEN '{start}' AND '{end}'
        """, f"Query: Use Case Velocity ({label})")
        if rows and rows[0]["AVG_TW"] is not None:
            r = rows[0]
            return {
                "time_to_tw": float(r["AVG_TW"]) if r["AVG_TW"] is not None else None,
                "won_to_imp_start": float(r["AVG_WON_TO_IMP"]) if r["AVG_WON_TO_IMP"] is not None else None,
                "won_to_deployed": float(r["AVG_WON_TO_DEPLOYED"]) if r["AVG_WON_TO_DEPLOYED"] is not None else None,
            }
        return {"time_to_tw": None, "won_to_imp_start": None, "won_to_deployed": None}

    current = _query_velocity(CONFIG["quarter_start"], CONFIG["quarter_end"], "Current")
    # Prior quarter = last quarter of prior FY
    prior_quarters = CONFIG.get("prior_fy_quarters", [])
    if prior_quarters:
        pq_start, pq_end = prior_quarters[-1]
        prior = _query_velocity(pq_start, pq_end, "Prior Q")
    else:
        prior = {"time_to_tw": None, "won_to_imp_start": None, "won_to_deployed": None}
    return {"current": current, "prior": prior}


def q_bronze_created_qtd(conn):
    """Query: Bronze UCs created in current quarter + creation target."""
    # Created count
    created_rows = run_query(conn, f"""
        SELECT COUNT(*) as CREATED_COUNT
        FROM {CONFIG["raven_uc_table"]} u
        JOIN {CONFIG["raven_acct_table"]} a ON u.VH_ACCOUNT_C = a.SALESFORCE_ACCOUNT_ID
        WHERE a.GVP = '{CONFIG["gvp_name"]}'
          AND u.TECHNICAL_CAMPAIGN_S_C ILIKE '{CONFIG["bronze_campaign"]}'
          AND u.CREATED_DATE BETWEEN '{CONFIG["quarter_start"]}' AND '{CONFIG["quarter_end"]}'
    """, "Query: Bronze Created QTD")
    created = int(created_rows[0]["CREATED_COUNT"]) if created_rows else 0
    # Creation target
    target_rows = run_query(conn, """
        SELECT TARGET_USE_CASE_COUNT
        FROM SALES.REPORTING.SALES_PROGRAM_PRIORITIZED_FEATURES_TARGETS
        WHERE MAPPED_THEATER = 'AMSExpansion'
          AND FISCAL_QUARTER = 'FY2027-Q1'
          AND MOVEMENT_TYPE = 'Created'
          AND PRIORITIZED_FEATURE_UC = 'Make Your Data AI Ready'
    """, "Query: Bronze Creation Target")
    target = int(target_rows[0]["TARGET_USE_CASE_COUNT"]) if target_rows and target_rows[0]["TARGET_USE_CASE_COUNT"] else None
    return {"created": created, "target": target}


def q_open_pipeline(conn):
    """Query 6: Open Pipeline"""
    excluded = ", ".join(f"'{s}'" for s in CONFIG["excluded_stages"])
    rows = run_query(conn, f"""
        SELECT SUM(u.USE_CASE_ACV) as OPEN_PIPELINE, COUNT(*) as OPEN_COUNT
        FROM {CONFIG["raven_uc_table"]} u
        JOIN {CONFIG["raven_acct_table"]} a ON u.VH_ACCOUNT_C = a.SALESFORCE_ACCOUNT_ID
        WHERE a.GVP = '{CONFIG["gvp_name"]}'
          AND u.USE_CASE_ACV > 0
          AND u.IS_WENT_LIVE = FALSE
          AND u.IS_LOST = FALSE
          AND u.GO_LIVE_DATE BETWEEN '{CONFIG["quarter_start"]}' AND '{CONFIG["quarter_end"]}'
          AND u.USE_CASE_STAGE NOT IN ({excluded})
    """, "Query 6: Open Pipeline")
    r = rows[0]
    return {
        "acv": float(r["OPEN_PIPELINE"] or 0),
        "count": int(r["OPEN_COUNT"] or 0),
    }


def q_pipeline_risk(conn):
    """Query 7: Pipeline Risk Analysis (uses DIM_USE_CASE for STAGE_NUMBER/DAYS_IN_STAGE)"""
    excluded = ", ".join(f"'{s}'" for s in CONFIG["dim_excluded_stages"])
    dim_theater = "(u.THEATER_NAME = 'AMSExpansion' OR u.ACCOUNT_GVP = 'Mark Fleming')"
    rows = run_query(conn, f"""
        WITH fiscal_qtr AS (
            SELECT FQ_START, FQ_END,
                   DATEDIFF('day', CURRENT_DATE(), FQ_END) + 1 as DAYS_REMAINING
            FROM SALES.REPORTING.CORE_FISCAL_DATES
            WHERE DATE_ = CURRENT_DATE()
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
        FROM {CONFIG["dim_uc_table"]} u
        CROSS JOIN fiscal_qtr f
        WHERE {dim_theater}
            AND u.USE_CASE_EACV > 0
            AND u.IS_DEPLOYED = FALSE
            AND u.STAGE_NUMBER BETWEEN 1 AND 6
            AND u.USE_CASE_STAGE NOT IN ({excluded})
            AND u.GO_LIVE_DATE BETWEEN f.FQ_START AND f.FQ_END
        GROUP BY STAGE_GROUP
        ORDER BY STAGE_GROUP
    """, "Query 7: Pipeline Risk")
    return {r["STAGE_GROUP"]: r for r in rows}


def _use_case_select_cols():
    """Common SELECT columns for use case queries (RAVEN-based).

    Expects: u = USE_CASE_EXPLORER_VH_DELIVERABLE_C, a = D_SALESFORCE_ACCOUNT_CUSTOMERS.
    Aliases output to same names the rest of the code expects.
    """
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


def q_top5_use_cases(conn):
    """Query 8: Top 5 Use Cases"""
    excluded = ", ".join(f"'{s}'" for s in CONFIG["excluded_stages"])
    rows = run_query(conn, f"""
        SELECT {_use_case_select_cols()}
        FROM {CONFIG["raven_uc_table"]} u
        JOIN {CONFIG["raven_acct_table"]} a ON u.VH_ACCOUNT_C = a.SALESFORCE_ACCOUNT_ID
        WHERE a.GVP = '{CONFIG["gvp_name"]}'
          AND u.USE_CASE_ACV > 0
          AND u.IS_WENT_LIVE = FALSE
          AND u.IS_LOST = FALSE
          AND u.GO_LIVE_DATE BETWEEN '{CONFIG["quarter_start"]}' AND '{CONFIG["quarter_end"]}'
          AND u.USE_CASE_STAGE NOT IN ({excluded})
        ORDER BY u.USE_CASE_ACV DESC
        LIMIT {CONFIG["top_n"]}
    """, "Query 8: Top 5 Use Cases")
    return rows


def _play_summary_query(conn, play_name, extra_join, filter_clause, is_deployed):
    """Helper for Query 9 sub-queries (RAVEN-based)."""
    excluded = ", ".join(f"'{s}'" for s in CONFIG["excluded_stages"])
    if is_deployed:
        deploy_filter = "u.IS_WENT_LIVE = TRUE"
        date_filter = f"u.DEFAULT_DATE BETWEEN '{CONFIG['quarter_start']}' AND '{CONFIG['quarter_end']}'"
        stage_filter = ""
    else:
        deploy_filter = "u.IS_WENT_LIVE = FALSE AND u.IS_LOST = FALSE"
        date_filter = f"u.GO_LIVE_DATE BETWEEN '{CONFIG['quarter_start']}' AND '{CONFIG['quarter_end']}'"
        stage_filter = f"AND u.USE_CASE_STAGE NOT IN ({excluded})"

    sql = f"""
        SELECT SUM(u.USE_CASE_ACV) as ACV, COUNT(*) as COUNT_
        FROM {CONFIG["raven_uc_table"]} u
        JOIN {CONFIG["raven_acct_table"]} a ON u.VH_ACCOUNT_C = a.SALESFORCE_ACCOUNT_ID
        {extra_join}
        WHERE a.GVP = '{CONFIG["gvp_name"]}'
          AND u.USE_CASE_ACV > 0
          AND {deploy_filter}
          AND {date_filter}
          {stage_filter}
          AND {filter_clause}
    """
    rows = run_query(conn, sql)
    r = rows[0]
    return {"acv": float(r["ACV"] or 0), "count": int(r["COUNT_"] or 0)}


def q_sales_play_summary(conn):
    """Query 9: Sales Play Summary Metrics (NO threshold)"""
    print("  Running: Query 9: Sales Play Summary...")
    # SI needs DIM join for TECHNICAL_USE_CASE (not available in RAVEN)
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
        result[f"{play}_open"] = _play_summary_query(conn, play, extra_join, filt, False)
        result[f"{play}_deployed"] = _play_summary_query(conn, play, extra_join, filt, True)

    print(f"    → Bronze: Open {result['bronze_open']['count']}/{fmt_currency(result['bronze_open']['acv'])}, Deployed {result['bronze_deployed']['count']}/{fmt_currency(result['bronze_deployed']['acv'])}")
    print(f"    → SI: Open {result['si_open']['count']}/{fmt_currency(result['si_open']['acv'])}, Deployed {result['si_deployed']['count']}/{fmt_currency(result['si_deployed']['acv'])}")
    print(f"    → SQL Server: Open {result['sqlserver_open']['count']}/{fmt_currency(result['sqlserver_open']['acv'])}, Deployed {result['sqlserver_deployed']['count']}/{fmt_currency(result['sqlserver_deployed']['acv'])}")
    return result


def q_play_detail_metrics(conn):
    """Query 10: Sales Play Detail Metrics (ALL use cases, NO threshold) for avg/median/regions."""
    excluded = ", ".join(f"'{s}'" for s in CONFIG["excluded_stages"])
    print("  Running: Query 10: Sales Play Detail Metrics...")

    # SI needs DIM join for TECHNICAL_USE_CASE
    dim_join = f"JOIN {CONFIG['dim_uc_table']} d ON u.ID = d.USE_CASE_ID"

    def _run(extra_join, filter_clause):
        sql = f"""
            SELECT COUNT(*) as OPEN_COUNT, AVG(u.USE_CASE_ACV) as AVG_ACV,
                   MEDIAN(u.USE_CASE_ACV) as MEDIAN_ACV,
                   COUNT(DISTINCT a.SALES_AREA) as REGION_COUNT
            FROM {CONFIG["raven_uc_table"]} u
            JOIN {CONFIG["raven_acct_table"]} a ON u.VH_ACCOUNT_C = a.SALESFORCE_ACCOUNT_ID
            {extra_join}
            WHERE a.GVP = '{CONFIG["gvp_name"]}'
              AND u.USE_CASE_ACV > 0
              AND u.IS_WENT_LIVE = FALSE
              AND u.IS_LOST = FALSE
              AND u.GO_LIVE_DATE BETWEEN '{CONFIG["quarter_start"]}' AND '{CONFIG["quarter_end"]}'
              AND u.USE_CASE_STAGE NOT IN ({excluded})
              AND {filter_clause}
        """
        rows = run_query(conn, sql)
        r = rows[0]
        return {
            "count": int(r["OPEN_COUNT"] or 0),
            "avg_acv": float(r["AVG_ACV"] or 0),
            "median_acv": float(r["MEDIAN_ACV"] or 0),
            "regions": int(r["REGION_COUNT"] or 0),
        }

    result = {
        "bronze": _run("", f"u.TECHNICAL_CAMPAIGN_S_C ILIKE '{CONFIG['bronze_campaign']}'"),
        "si": _run(dim_join, f"d.TECHNICAL_USE_CASE ILIKE '{CONFIG['si_technical_use_case']}'"),
        "sqlserver": _run("", f"u.TECHNICAL_CAMPAIGN_S_C ILIKE '{CONFIG['sqlserver_campaign']}'"),
    }
    for play, d in result.items():
        print(f"    → {play}: {d['count']} UCs, avg {fmt_currency(d['avg_acv'])}, median {fmt_currency(d['median_acv'])}, {d['regions']} regions")
    return result


def q_bronze_tb_total(conn):
    """Query 11: Bronze TB Ingested (total)"""
    rows = run_query(conn, f"""
        SELECT SUM(TB_INGESTED) as BRONZE_TB
        FROM SALES.SALES_BI.SALES_PROGRAMS_BRONZE_INGEST
        WHERE GVP = '{CONFIG["gvp_name"]}'
    """, "Query 11: Bronze TB Total")
    return float(rows[0]["BRONZE_TB"] or 0)


def _play_use_cases_query(conn, play_name, extra_join, filter_clause):
    """Helper for Query 12: Use Cases by Sales Play (≥$500K), RAVEN-based."""
    excluded = ", ".join(f"'{s}'" for s in CONFIG["excluded_stages"])

    sql = f"""
        SELECT {_use_case_select_cols()}
        FROM {CONFIG["raven_uc_table"]} u
        JOIN {CONFIG["raven_acct_table"]} a ON u.VH_ACCOUNT_C = a.SALESFORCE_ACCOUNT_ID
        {extra_join}
        WHERE a.GVP = '{CONFIG["gvp_name"]}'
          AND u.USE_CASE_ACV >= {CONFIG["play_threshold"]}
          AND u.IS_WENT_LIVE = FALSE
          AND u.IS_LOST = FALSE
          AND u.GO_LIVE_DATE BETWEEN '{CONFIG["quarter_start"]}' AND '{CONFIG["quarter_end"]}'
          AND u.USE_CASE_STAGE NOT IN ({excluded})
          AND {filter_clause}
        ORDER BY u.USE_CASE_ACV DESC
    """
    return run_query(conn, sql, f"Query 12: {play_name} Use Cases ≥$500K")


def q_play_use_cases(conn):
    """Query 12: Use Cases by Sales Play (≥$500K threshold for tables)."""
    dim_join = f"JOIN {CONFIG['dim_uc_table']} d ON u.ID = d.USE_CASE_ID"
    return {
        "bronze": _play_use_cases_query(conn, "Bronze", "", f"u.TECHNICAL_CAMPAIGN_S_C ILIKE '{CONFIG['bronze_campaign']}'"),
        "si": _play_use_cases_query(conn, "SI", dim_join, f"d.TECHNICAL_USE_CASE ILIKE '{CONFIG['si_technical_use_case']}'"),
        "sqlserver": _play_use_cases_query(conn, "SQL Server", "", f"u.TECHNICAL_CAMPAIGN_S_C ILIKE '{CONFIG['sqlserver_campaign']}'"),
    }


def q_prior_fy_pacing(conn, day_number, week_number):
    """Query 13: Prior FY Pacing Averages (dynamic)"""
    prior_label = CONFIG["prior_fy_label"]
    print(f"  Running: Query 13: {prior_label} Pacing...")
    day_num = int(day_number)
    week_days = int(week_number) * 7

    quarters = CONFIG["prior_fy_quarters"]
    if not quarters:
        print("    → No prior FY quarters found, skipping pacing")
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

    day_sql = f"SELECT AVG(DEPLOYED_ACV) as AVG_ACV FROM ({' UNION ALL '.join(day_unions)})"
    week_sql = f"SELECT AVG(DEPLOYED_ACV) as AVG_ACV FROM ({' UNION ALL '.join(week_unions)})"

    day_rows = run_query(conn, day_sql)
    week_rows = run_query(conn, week_sql)

    day_avg = float(day_rows[0]["AVG_ACV"] or 0)
    week_avg = float(week_rows[0]["AVG_ACV"] or 0)

    fy_final = CONFIG["prior_fy_avg_final"] * 1_000_000

    print(f"    → Day {day_num} avg: {fmt_currency(day_avg)}, Week {int(week_number)} avg: {fmt_currency(week_avg)}")
    return {
        "day_avg": day_avg,
        "day_pct": (day_avg / fy_final * 100) if fy_final else 0,
        "week_avg": week_avg,
        "week_pct": (week_avg / fy_final * 100) if fy_final else 0,
    }


def q_consumption(conn, account_ids):
    """Query 14: Account Consumption Data"""
    if not account_ids:
        return {}
    id_list = ", ".join(f"'{aid}'" for aid in account_ids if aid)
    if not id_list:
        return {}
    rows = run_query(conn, f"""
        SELECT
            ACCOUNT_ID,
            ACCOUNT_NAME,
            ROUND(REVENUE_TRAILING_90D, 0) as REV_90D,
            ROUND(GROWTH_RATE_90D * 100, 0) as GROWTH_90D_PCT,
            ROUND(REVENUE_LTM, 0) as RUN_RATE,
            ROUND(GROWTH_RATE_180D * 100, 0) as RUN_RATE_GROWTH_PCT
        FROM SALES.REPORTING.BOB_CONSUMPTION
        WHERE ACCOUNT_ID IN ({id_list})
    """, "Query 14: Consumption Data")
    return {r["ACCOUNT_ID"]: r for r in rows}


def q_si_theater_totals(conn):
    """Query 15a: Theater-wide SI Usage Totals"""
    rows = run_query(conn, f"""
        SELECT
            COUNT(DISTINCT SALESFORCE_ACCOUNT_ID) as SI_ACCOUNTS,
            SUM(ACTIVE_USERS_LAST_30_DAYS) as SI_USERS_30D,
            ROUND(SUM(CREDITS_LAST_30_DAYS), 0) as SI_CREDITS_30D,
            ROUND(SUM(REVENUE_LAST_30_DAYS), 0) as SI_REVENUE_30D
        FROM SALES.REPORTING.BOB_SNOWFLAKE_INTELLIGENCE_USAGE_STREAMLIT_AGG
        WHERE GVP = '{CONFIG["gvp_name"]}'
          AND ACTIVE_ACCOUNT_LAST_30_DAYS = 1
    """, "Query 15a: SI Theater Totals")
    if rows:
        r = rows[0]
        return {
            "accounts": int(r.get("SI_ACCOUNTS", 0) or 0),
            "users": int(r.get("SI_USERS_30D", 0) or 0),
            "credits": int(r.get("SI_CREDITS_30D", 0) or 0),
            "revenue": float(r.get("SI_REVENUE_30D", 0) or 0),
        }
    return {"accounts": 0, "users": 0, "credits": 0, "revenue": 0}


def q_si_usage(conn, account_ids):
    """Query 15: SI Usage Metrics (per-account)"""
    if not account_ids:
        return {}
    id_list = ", ".join(f"'{aid}'" for aid in account_ids if aid)
    if not id_list:
        return {}
    rows = run_query(conn, f"""
        SELECT
            SALESFORCE_ACCOUNT_ID as ACCOUNT_ID,
            SALESFORCE_ACCOUNT_NAME as ACCOUNT_NAME,
            ROUND(CREDITS_LAST_30_DAYS, 0) as SI_CREDITS,
            ROUND(REVENUE_LAST_30_DAYS, 0) as SI_REVENUE,
            ACTIVE_USERS_LAST_30_DAYS as SI_USERS
        FROM SALES.REPORTING.BOB_SNOWFLAKE_INTELLIGENCE_USAGE_STREAMLIT_AGG
        WHERE SALESFORCE_ACCOUNT_ID IN ({id_list})
    """, "Query 15: SI Usage")
    return {r["ACCOUNT_ID"]: r for r in rows}


def q_bronze_tb_by_account(conn):
    """Query 16: Bronze TB by Account"""
    rows = run_query(conn, f"""
        SELECT
            SALESFORCE_ACCOUNT_ID as ACCOUNT_ID,
            SALESFORCE_ACCOUNT_NAME as ACCOUNT_NAME,
            ROUND(SUM(TB_INGESTED), 1) as TB_INGESTED
        FROM SALES.SALES_BI.SALES_PROGRAMS_BRONZE_INGEST
        WHERE GVP = '{CONFIG["gvp_name"]}'
        GROUP BY SALESFORCE_ACCOUNT_ID, SALESFORCE_ACCOUNT_NAME
    """, "Query 16: Bronze TB by Account")
    return {r["ACCOUNT_ID"]: r for r in rows}


def _play_risk_detail_query(conn, play_name, extra_join, filter_clause):
    """Helper for Query 17: risk detail per play (RAVEN-based)."""
    excluded = ", ".join(f"'{s}'" for s in CONFIG["excluded_stages"])

    # Risk detail rows
    risk_sql = f"""
        SELECT a.SALESFORCE_ACCOUNT_NAME as ACCOUNT_NAME,
               u.VH_NAME_C as USE_CASE_NAME,
               u.USE_CASE_ACV as USE_CASE_EACV,
               u.USE_CASE_RISK_C as USE_CASE_RISK,
               u.USE_CASE_COMMENTS_C as SE_COMMENTS,
               u.NEXT_STEP_C as NEXT_STEPS,
               u.USE_CASE_STAGE
        FROM {CONFIG["raven_uc_table"]} u
        JOIN {CONFIG["raven_acct_table"]} a ON u.VH_ACCOUNT_C = a.SALESFORCE_ACCOUNT_ID
        {extra_join}
        WHERE a.GVP = '{CONFIG["gvp_name"]}'
          AND u.USE_CASE_ACV > 0
          AND u.IS_WENT_LIVE = FALSE
          AND u.IS_LOST = FALSE
          AND u.GO_LIVE_DATE BETWEEN '{CONFIG["quarter_start"]}' AND '{CONFIG["quarter_end"]}'
          AND u.USE_CASE_STAGE NOT IN ({excluded})
          AND u.USE_CASE_RISK_C IS NOT NULL AND u.USE_CASE_RISK_C != '' AND u.USE_CASE_RISK_C != 'None'
          AND {filter_clause}
        ORDER BY u.USE_CASE_ACV DESC
    """
    risk_rows = run_query(conn, risk_sql)

    # Total count for denominator
    total_sql = f"""
        SELECT COUNT(*) as TOTAL_COUNT
        FROM {CONFIG["raven_uc_table"]} u
        JOIN {CONFIG["raven_acct_table"]} a ON u.VH_ACCOUNT_C = a.SALESFORCE_ACCOUNT_ID
        {extra_join}
        WHERE a.GVP = '{CONFIG["gvp_name"]}'
          AND u.USE_CASE_ACV > 0
          AND u.IS_WENT_LIVE = FALSE
          AND u.IS_LOST = FALSE
          AND u.GO_LIVE_DATE BETWEEN '{CONFIG["quarter_start"]}' AND '{CONFIG["quarter_end"]}'
          AND u.USE_CASE_STAGE NOT IN ({excluded})
          AND {filter_clause}
    """
    total_rows = run_query(conn, total_sql)
    total = int(total_rows[0]["TOTAL_COUNT"] or 0)

    print(f"    → {play_name}: {len(risk_rows)} risk UCs out of {total} total")
    return {"risk_rows": risk_rows, "total_count": total}


def q_high_risk_use_cases(conn):
    """Query: All at-risk use cases in the quarter for the Script tab table."""
    excluded = ", ".join(f"'{s}'" for s in CONFIG["excluded_stages"])
    rows = run_query(conn, f"""
        SELECT
            u.ID as USE_CASE_ID,
            u.NAME as USE_CASE_NUMBER,
            u.VH_NAME_C as USE_CASE_NAME,
            a.SALESFORCE_OWNER_NAME as AE_NAME,
            a.LEAD_SALES_ENGINEER_NAME as SE_NAME,
            u.USE_CASE_ACV,
            u.USE_CASE_RISK_C as RISK_TYPE,
            SNOWFLAKE.CORTEX.COMPLETE(
                'llama3.1-70b',
                CONCAT(
                    'Summarize this use case risk in 1-2 concise sentences for a sales leadership QC call. Focus on what the risk is and the current mitigation plan. Do not include any preamble or introductory text, just provide the summary directly. Risk type: ',
                    COALESCE(u.USE_CASE_RISK_C, 'Unknown'),
                    '. SE Comments: ', COALESCE(u.USE_CASE_COMMENTS_C, 'None'),
                    '. Next Steps: ', COALESCE(u.NEXT_STEP_C, 'None')
                )
            ) as RISK_SUMMARY
        FROM {CONFIG["raven_uc_table"]} u
        JOIN {CONFIG["raven_acct_table"]} a ON u.VH_ACCOUNT_C = a.SALESFORCE_ACCOUNT_ID
        WHERE a.GVP = '{CONFIG["gvp_name"]}'
          AND u.USE_CASE_ACV > 0
          AND u.IS_WENT_LIVE = FALSE
          AND u.IS_LOST = FALSE
          AND u.GO_LIVE_DATE BETWEEN '{CONFIG["quarter_start"]}' AND '{CONFIG["quarter_end"]}'
          AND u.USE_CASE_STAGE NOT IN ({excluded})
          AND u.USE_CASE_RISK_C IS NOT NULL AND u.USE_CASE_RISK_C != '' AND u.USE_CASE_RISK_C != 'None'
          AND u.USE_CASE_ACV >= {CONFIG["play_threshold"]}
        ORDER BY u.USE_CASE_ACV DESC
    """, "Query: High Risk Use Cases (Script)")
    print(f"  Running: High Risk Use Cases → {len(rows)} at-risk UCs found")
    return rows


def q_play_risk_detail(conn):
    """Query 17: Sales Play Risk Detail (ALL use cases with risk, NO threshold)."""
    print("  Running: Query 17: Sales Play Risk Detail...")
    dim_join = f"JOIN {CONFIG['dim_uc_table']} d ON u.ID = d.USE_CASE_ID"
    return {
        "bronze": _play_risk_detail_query(conn, "Bronze", "", f"u.TECHNICAL_CAMPAIGN_S_C ILIKE '{CONFIG['bronze_campaign']}'"),
        "si": _play_risk_detail_query(conn, "SI", dim_join, f"d.TECHNICAL_USE_CASE ILIKE '{CONFIG['si_technical_use_case']}'"),
        "sqlserver": _play_risk_detail_query(conn, "SQL Server", "", f"u.TECHNICAL_CAMPAIGN_S_C ILIKE '{CONFIG['sqlserver_campaign']}'"),
    }


# =============================================================================
# FORECAST ANALYSIS QUERIES
# =============================================================================

def q_current_pipeline_phases(conn):
    """Query 18: Current pipeline by milestone phase (for forecast analysis)."""
    excluded = ", ".join(f"'{s}'" for s in CONFIG["excluded_stages"])
    rows = run_query(conn, f"""
        SELECT
            CASE
                WHEN u.IS_WENT_LIVE = TRUE AND u.DEFAULT_DATE BETWEEN '{CONFIG["quarter_start"]}' AND '{CONFIG["quarter_end"]}'
                    THEN 'Already Deployed'
                WHEN u.IMPLEMENTATION_START_DATE <= CURRENT_DATE()
                    AND u.IS_WENT_LIVE = FALSE AND u.IS_LOST = FALSE
                    THEN 'In Implementation'
                WHEN u.TECHNICAL_WIN_DATE_FORECAST_C <= CURRENT_DATE()
                    AND (u.IMPLEMENTATION_START_DATE > CURRENT_DATE() OR u.IMPLEMENTATION_START_DATE IS NULL)
                    AND u.IS_WENT_LIVE = FALSE AND u.IS_LOST = FALSE
                    THEN 'Post-TW / Pre-Imp'
                WHEN u.CREATED_DATE <= CURRENT_DATE()
                    AND (u.TECHNICAL_WIN_DATE_FORECAST_C > CURRENT_DATE() OR u.TECHNICAL_WIN_DATE_FORECAST_C IS NULL)
                    AND (u.IMPLEMENTATION_START_DATE > CURRENT_DATE() OR u.IMPLEMENTATION_START_DATE IS NULL)
                    AND u.IS_WENT_LIVE = FALSE AND u.IS_LOST = FALSE
                    THEN 'Pre-TW'
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
        GROUP BY PIPELINE_PHASE
        ORDER BY PIPELINE_PHASE
    """, "Query 18: Current Pipeline Phases")
    return {r["PIPELINE_PHASE"]: r for r in rows}


def q_historical_conversion_rates(conn, day_number):
    """Query 19: Historical conversion rates using point-in-time snapshots.

    Uses DIM_USE_CASE_HISTORY_DS to get pipeline state (including GO_LIVE_DATE)
    as it was at day N of each quarter — eliminates survivorship bias.
    Joins to current RAVEN table for actual deployment outcomes.
    Only includes pipeline where GO_LIVE_DATE (at day N) was within the quarter.
    Skips quarters where snapshot data is not available.
    """
    quarters = CONFIG["prior_fy_quarters"]
    if not quarters:
        return []

    day_num = int(day_number)
    snapshot_table = "SALES.DEV.DIM_USE_CASE_HISTORY_DS"
    unions = []
    for i, (qs, qe) in enumerate(quarters):
        q_label = f"{CONFIG['prior_fy_label']} Q{i+1}"
        # Calculate the snapshot date (day N of this quarter)
        # day_num is 1-based, so day 1 = quarter start
        snap_date = f"DATEADD('day', {day_num - 1}, '{qs}')::DATE"
        unions.append(f"""
            SELECT
                '{q_label}' as QTR,
                '{qs}' as QS, '{qe}' as QE,
                -- Already deployed by day N (within this quarter)
                SUM(CASE WHEN h.IS_DEPLOYED = TRUE
                         AND h.ACTUAL_USE_CASE_DEPLOYMENT_DATE <= {snap_date}
                         AND h.ACTUAL_USE_CASE_DEPLOYMENT_DATE BETWEEN '{qs}' AND '{qe}'
                    THEN h.USE_CASE_EACV ELSE 0 END) as DEPLOYED_ACV,
                -- In Implementation at day N, with GO_LIVE in quarter (snapshot-based)
                SUM(CASE WHEN h.IMPLEMENTATION_START_DATE <= {snap_date}
                         AND h.IS_DEPLOYED = FALSE AND h.IS_LOST = FALSE
                         AND h.GO_LIVE_DATE BETWEEN '{qs}' AND '{qe}'
                    THEN h.USE_CASE_EACV ELSE 0 END) as IMP_TOTAL,
                SUM(CASE WHEN h.IMPLEMENTATION_START_DATE <= {snap_date}
                         AND h.IS_DEPLOYED = FALSE AND h.IS_LOST = FALSE
                         AND h.GO_LIVE_DATE BETWEEN '{qs}' AND '{qe}'
                         AND o.IS_WENT_LIVE = TRUE AND o.DEFAULT_DATE BETWEEN '{qs}' AND '{qe}'
                    THEN h.USE_CASE_EACV ELSE 0 END) as IMP_CONVERTED,
                -- Post-TW / Pre-Imp at day N, with GO_LIVE in quarter (snapshot-based)
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
                -- Pre-TW at day N, with GO_LIVE in quarter (snapshot-based)
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
                -- New pipeline: deployed in quarter but created after day N
                -- (UCs not yet in the snapshot at day N that eventually deployed)
                0 as NEW_PIPELINE_CONVERTED,
                -- Final deployed placeholder (computed separately from RAVEN)
                0 as FINAL_DEPLOYED
            FROM {snapshot_table} h
            LEFT JOIN {CONFIG["raven_uc_table"]} o ON h.USE_CASE_ID = o.ID
            WHERE h.DS = {snap_date}
              AND h.ACCOUNT_GVP = '{CONFIG["gvp_name"]}'
              AND h.USE_CASE_EACV > 0
        """)

    # New pipeline query: UCs that deployed in quarter but weren't in snapshot at day N
    # Run as a separate UNION to avoid FULL OUTER JOIN complexity
    new_pipe_unions = []
    for i, (qs, qe) in enumerate(quarters):
        q_label = f"{CONFIG['prior_fy_label']} Q{i+1}"
        snap_date = f"DATEADD('day', {day_num - 1}, '{qs}')::DATE"
        new_pipe_unions.append(f"""
            SELECT '{q_label}' as QTR,
                   SUM(o.USE_CASE_ACV) as NEW_PIPELINE_ACV
            FROM {CONFIG["raven_uc_table"]} o
            JOIN {CONFIG["raven_acct_table"]} a ON o.VH_ACCOUNT_C = a.SALESFORCE_ACCOUNT_ID
            LEFT JOIN {snapshot_table} h
                ON h.USE_CASE_ID = o.ID AND h.DS = {snap_date}
            WHERE a.GVP = '{CONFIG["gvp_name"]}'
              AND o.USE_CASE_ACV > 0
              AND o.IS_WENT_LIVE = TRUE
              AND o.DEFAULT_DATE BETWEEN '{qs}' AND '{qe}'
              AND (h.USE_CASE_ID IS NULL OR h.CREATED_DATE > {snap_date})
        """)

    # Run main query
    sql = " UNION ALL ".join(unions) + " ORDER BY QTR"
    rows = run_query(conn, sql, "Query 19: Historical Conversion Rates")

    # Run new pipeline query and merge results
    if new_pipe_unions:
        np_sql = " UNION ALL ".join(new_pipe_unions) + " ORDER BY QTR"
        np_rows = run_query(conn, np_sql, "Query 19b: New Pipeline Contribution")
        np_map = {r["QTR"]: float(r["NEW_PIPELINE_ACV"] or 0) for r in np_rows}
        for r in rows:
            r["NEW_PIPELINE_CONVERTED"] = np_map.get(r["QTR"], 0)

    # Compute FINAL_DEPLOYED directly from RAVEN (not snapshot) for accuracy
    final_dep_unions = []
    for i, (qs, qe) in enumerate(quarters):
        q_label = f"{CONFIG['prior_fy_label']} Q{i+1}"
        final_dep_unions.append(f"""
            SELECT '{q_label}' as QTR,
                   SUM(u.USE_CASE_ACV) as FINAL_DEPLOYED
            FROM {CONFIG["raven_uc_table"]} u
            JOIN {CONFIG["raven_acct_table"]} a ON u.VH_ACCOUNT_C = a.SALESFORCE_ACCOUNT_ID
            WHERE a.GVP = '{CONFIG["gvp_name"]}'
              AND u.IS_WENT_LIVE = TRUE
              AND u.DEFAULT_DATE BETWEEN '{qs}' AND '{qe}'
              AND u.USE_CASE_ACV > 0
        """)
    fd_sql = " UNION ALL ".join(final_dep_unions) + " ORDER BY QTR"
    fd_rows = run_query(conn, fd_sql, "Query 19c: Actual Final Deployed")
    fd_map = {r["QTR"]: float(r["FINAL_DEPLOYED"] or 0) for r in fd_rows}
    for r in rows:
        r["FINAL_DEPLOYED"] = fd_map.get(r["QTR"], 0)

    # Filter out quarters with no snapshot data (e.g., Q1 if snapshots started late)
    rows = [r for r in rows if float(r.get("IMP_TOTAL", 0) or 0) > 0
            or float(r.get("DEPLOYED_ACV", 0) or 0) > 0]

    return rows


def compute_forecast_analysis(pipeline_phases, hist_rates, deployed, pipeline_risk, most_likely, pacing):
    """Combine pipeline phases, historical conversion rates, and risk data into forecast recommendations."""

    # --- Current pipeline state ---
    deployed_acv = float((pipeline_phases.get("Already Deployed") or {}).get("TOTAL_ACV", 0) or 0)
    imp_acv = float((pipeline_phases.get("In Implementation") or {}).get("TOTAL_ACV", 0) or 0)
    tw_acv = float((pipeline_phases.get("Post-TW / Pre-Imp") or {}).get("TOTAL_ACV", 0) or 0)
    pre_tw_acv = float((pipeline_phases.get("Pre-TW") or {}).get("TOTAL_ACV", 0) or 0)

    # --- Method 1: Pipeline Risk Model (from existing Query 7 stage-based risk) ---
    total_good = sum(float(r.get("GOOD_ACV", 0) or 0) for r in pipeline_risk.values())
    total_pipeline_acv = sum(float(r.get("TOTAL_ACV", 0) or 0) for r in pipeline_risk.values())
    total_at_risk = sum(float(r.get("AT_RISK_ACV", 0) or 0) for r in pipeline_risk.values())
    stage6_acv = float((pipeline_risk.get("Stage 6") or {}).get("TOTAL_ACV", 0) or 0)
    stage5_good_acv = float((pipeline_risk.get("Stage 5") or {}).get("GOOD_ACV", 0) or 0)

    m1_commit = deployed["acv"] + stage6_acv + stage5_good_acv
    m1_most_likely = deployed["acv"] + total_good
    m1_stretch = deployed["acv"] + total_pipeline_acv

    # --- Method 2: Historical Pacing Model ---
    if hist_rates:
        pacing_ratios = []
        for r in hist_rates:
            d26 = float(r.get("DEPLOYED_ACV", 0) or 0)
            final = float(r.get("FINAL_DEPLOYED", 0) or 0)
            if final > 0 and d26 > 0:
                pacing_ratios.append(d26 / final)

        if pacing_ratios:
            avg_ratio = sum(pacing_ratios) / len(pacing_ratios)
            min_ratio = max(pacing_ratios)  # highest ratio = lowest multiplier = commit
            max_ratio = min(pacing_ratios)  # lowest ratio = highest multiplier = stretch
            m2_most_likely = deployed["acv"] / avg_ratio if avg_ratio > 0 else 0
            m2_commit = deployed["acv"] / min_ratio if min_ratio > 0 else 0
            m2_stretch = deployed["acv"] / max_ratio if max_ratio > 0 else 0
        else:
            m2_commit = m2_most_likely = m2_stretch = 0
    else:
        m2_commit = m2_most_likely = m2_stretch = 0
        pacing_ratios = []

    # --- Method 3: Stage Conversion Model (milestone-based historical rates) ---
    if hist_rates:
        imp_rates, tw_rates, pre_tw_rates, new_pcts = [], [], [], []
        for r in hist_rates:
            imp_t = float(r.get("IMP_TOTAL", 0) or 0)
            imp_c = float(r.get("IMP_CONVERTED", 0) or 0)
            tw_t = float(r.get("TW_TOTAL", 0) or 0)
            tw_c = float(r.get("TW_CONVERTED", 0) or 0)
            pt_t = float(r.get("PRE_TW_TOTAL", 0) or 0)
            pt_c = float(r.get("PRE_TW_CONVERTED", 0) or 0)
            new_c = float(r.get("NEW_PIPELINE_CONVERTED", 0) or 0)
            final = float(r.get("FINAL_DEPLOYED", 0) or 0)

            if imp_t > 0:
                imp_rates.append(imp_c / imp_t)
            if tw_t > 0:
                tw_rates.append(tw_c / tw_t)
            if pt_t > 0:
                pre_tw_rates.append(pt_c / pt_t)
            if final > 0:
                new_pcts.append(new_c / final)

        avg_imp_rate = sum(imp_rates) / len(imp_rates) if imp_rates else 0
        avg_tw_rate = sum(tw_rates) / len(tw_rates) if tw_rates else 0
        avg_pre_tw_rate = sum(pre_tw_rates) / len(pre_tw_rates) if pre_tw_rates else 0
        avg_new_pct = sum(new_pcts) / len(new_pcts) if new_pcts else 0
        min_imp = min(imp_rates) if imp_rates else 0
        min_tw = min(tw_rates) if tw_rates else 0
        min_pre_tw = min(pre_tw_rates) if pre_tw_rates else 0
        min_new = min(new_pcts) if new_pcts else 0
        max_new = max(new_pcts) if new_pcts else 0

        # Known pipeline (already deployed + weighted open)
        known_most_likely = deployed_acv + (imp_acv * avg_imp_rate) + (tw_acv * avg_tw_rate) + (pre_tw_acv * avg_pre_tw_rate)
        known_commit = deployed_acv + (imp_acv * min_imp) + (tw_acv * min_tw) + (pre_tw_acv * min_pre_tw)

        # Add new pipeline uplift
        m3_most_likely = known_most_likely / (1 - avg_new_pct) if avg_new_pct < 1 else known_most_likely
        m3_commit = known_commit / (1 - min_new) if min_new < 1 else known_commit
        m3_stretch = (deployed_acv + imp_acv + tw_acv + pre_tw_acv) / (1 - max_new) if max_new < 1 else (deployed_acv + imp_acv + tw_acv + pre_tw_acv)
    else:
        avg_imp_rate = avg_tw_rate = avg_pre_tw_rate = avg_new_pct = 0
        min_imp = min_tw = min_pre_tw = min_new = max_new = 0
        m3_commit = m3_most_likely = m3_stretch = 0

    # --- Blended Recommendation ---
    # Average across the three methods
    rec_commit = (m1_commit + m2_commit + m3_commit) / 3
    rec_most_likely = (m1_most_likely + m2_most_likely + m3_most_likely) / 3
    rec_stretch = (m1_stretch + m2_stretch + m3_stretch) / 3

    return {
        "pipeline_phases": {
            "deployed": deployed_acv,
            "in_imp": imp_acv,
            "post_tw": tw_acv,
            "pre_tw": pre_tw_acv,
        },
        "method1": {"commit": m1_commit, "most_likely": m1_most_likely, "stretch": m1_stretch,
                     "label": "Pipeline Risk Model"},
        "method2": {"commit": m2_commit, "most_likely": m2_most_likely, "stretch": m2_stretch,
                     "label": "Historical Pacing Model",
                     "ratios": pacing_ratios},
        "method3": {"commit": m3_commit, "most_likely": m3_most_likely, "stretch": m3_stretch,
                     "label": "Stage Conversion Model",
                     "rates": {"imp": avg_imp_rate, "tw": avg_tw_rate, "pre_tw": avg_pre_tw_rate,
                               "new_pipeline": avg_new_pct}},
        "recommended": {"commit": rec_commit, "most_likely": rec_most_likely, "stretch": rec_stretch},
        "current_calls": {"commit": most_likely, "most_likely": most_likely, "stretch": most_likely},
        "hist_rates": hist_rates,
    }


def compute_weighted_ensemble(forecast_analysis, backtest_results, day_number=31):
    """Method 4: Inverse-error weighted ensemble of M1, M2, M3.

    Computes weights from backtest errors — models with lower historical error
    get higher weight. Falls back to equal weights if no backtest data.
    Uses leave-one-out cross-validation for backtest M4 predictions to avoid
    in-sample bias (each quarter's M4 uses weights from the other quarters only).
    Full-sample weights are used for the current quarter's forecast.

    Day-dependent adjustment: Backtest weights are calibrated at day 31. For different
    day-N values, we adjust: early in quarter (day < 31) M2 (pacing) has less data
    so we shift weight to M1/M3; later (day > 31) M2 becomes more reliable.
    """
    fa = forecast_analysis
    m1 = fa["method1"]
    m2 = fa["method2"]
    m3 = fa["method3"]

    def _apply_day_adjustment(weights, day_n):
        """Adjust weights based on day-in-quarter. At day 31 (calibration point),
        no adjustment. Earlier: dampen M2 (pacing has less signal). Later: boost M2."""
        if day_n == 31:
            return weights
        adjusted = {}
        for call_key in weights:
            w = dict(weights[call_key])
            # Day-dependent M2 scaling factor: linear ramp
            # day 1 -> factor 0.5 (halve M2), day 31 -> factor 1.0, day 90 -> factor 1.3
            if day_n < 31:
                factor = 0.5 + 0.5 * (day_n / 31)
            else:
                factor = 1.0 + 0.3 * ((day_n - 31) / 59)
            factor = max(0.3, min(factor, 1.5))
            w["m2"] = w["m2"] * factor
            # Renormalize
            total = w["m1"] + w["m2"] + w["m3"]
            if total > 0:
                adjusted[call_key] = {mk: v / total for mk, v in w.items()}
            else:
                adjusted[call_key] = w
        return adjusted

    def _compute_weights(bt_data):
        """Compute inverse-error weights from a list of backtest results."""
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

    # Full-sample weights for current quarter forecast
    if backtest_results:
        base_weights = _compute_weights(backtest_results)
    else:
        base_weights = {ck: {"m1": 1/3, "m2": 1/3, "m3": 1/3}
                   for ck in ["commit", "most_likely", "stretch"]}

    # Apply day-dependent adjustment to weights for current forecast
    weights = _apply_day_adjustment(base_weights, day_number)

    # Apply weights to current forecast
    m4 = {}
    for call_key in ["commit", "most_likely", "stretch"]:
        w = weights[call_key]
        m4[call_key] = w["m1"] * m1[call_key] + w["m2"] * m2[call_key] + w["m3"] * m3[call_key]

    fa["method4"] = {
        "commit": m4["commit"], "most_likely": m4["most_likely"], "stretch": m4["stretch"],
        "label": "Weighted Ensemble",
        "weights": weights,
        "base_weights": base_weights,
        "day_number": day_number,
    }
    # Update recommended to use M4
    fa["recommended"] = {"commit": m4["commit"], "most_likely": m4["most_likely"], "stretch": m4["stretch"]}

    # Leave-one-out: for each backtest quarter, compute M4 using weights
    # derived from the OTHER quarters only (avoids in-sample bias)
    if backtest_results:
        for i, bt in enumerate(backtest_results):
            others = [b for j, b in enumerate(backtest_results) if j != i]
            if others:
                loo_weights = _compute_weights(others)
            else:
                loo_weights = {ck: {"m1": 1/3, "m2": 1/3, "m3": 1/3}
                               for ck in ["commit", "most_likely", "stretch"]}
            m4_bt = {}
            for call_key in ["commit", "most_likely", "stretch"]:
                w = loo_weights[call_key]
                m4_bt[call_key] = (w["m1"] * bt["m1"][call_key]
                                   + w["m2"] * bt["m2"][call_key]
                                   + w["m3"] * bt["m3"][call_key])
            bt["m4"] = m4_bt

    # Compute confidence intervals from backtest error distribution
    if backtest_results:
        confidence = {}
        for call_key in ["commit", "most_likely", "stretch"]:
            m4_val = fa["method4"][call_key]
            # Collect signed percentage errors from M4 LOO backtest
            pct_errors = []
            for bt in backtest_results:
                actual = bt["final_deployed"]
                if actual > 0 and "m4" in bt:
                    pct_errors.append((bt["m4"][call_key] - actual) / actual)
            if pct_errors:
                mean_err = sum(pct_errors) / len(pct_errors)
                # Standard deviation of errors
                if len(pct_errors) > 1:
                    variance = sum((e - mean_err) ** 2 for e in pct_errors) / (len(pct_errors) - 1)
                    std_err = variance ** 0.5
                else:
                    std_err = abs(mean_err) if mean_err != 0 else 0.1
                # Use min/max of historical errors for range bounds
                min_err = min(pct_errors)
                max_err = max(pct_errors)
                # 1-sigma range (68% CI)
                low_1s = m4_val * (1 + mean_err - std_err)
                high_1s = m4_val * (1 + mean_err + std_err)
                # Historical range (min/max observed error)
                low_hist = m4_val * (1 + min_err)
                high_hist = m4_val * (1 + max_err)
                confidence[call_key] = {
                    "mean_error": mean_err,
                    "std_error": std_err,
                    "low_1sigma": low_1s,
                    "high_1sigma": high_1s,
                    "low_hist": low_hist,
                    "high_hist": high_hist,
                    "n_quarters": len(pct_errors),
                }
            else:
                confidence[call_key] = None
        fa["method4"]["confidence"] = confidence


def log_forecast_tracking(fiscal, forecast_analysis, deployed):
    """Append a row to the forecast tracking CSV for time-series analysis."""
    tracking_path = os.path.join(CONFIG["output_dir"], "PEAK_forecast_tracking.csv")
    headers = [
        "date", "day_number", "quarter",
        "m1_commit", "m1_most_likely", "m1_stretch",
        "m2_commit", "m2_most_likely", "m2_stretch",
        "m3_commit", "m3_most_likely", "m3_stretch",
        "m4_commit", "m4_most_likely", "m4_stretch",
        "deployed_to_date",
    ]
    fa = forecast_analysis
    m4 = fa.get("method4", {})
    row = {
        "date": date.today().isoformat(),
        "day_number": fiscal["DAY_NUMBER"],
        "quarter": f"{CONFIG['fiscal_year_label']} {fiscal['FISCAL_QUARTER']}",
        "m1_commit": round(fa["method1"]["commit"], 0),
        "m1_most_likely": round(fa["method1"]["most_likely"], 0),
        "m1_stretch": round(fa["method1"]["stretch"], 0),
        "m2_commit": round(fa["method2"]["commit"], 0),
        "m2_most_likely": round(fa["method2"]["most_likely"], 0),
        "m2_stretch": round(fa["method2"]["stretch"], 0),
        "m3_commit": round(fa["method3"]["commit"], 0),
        "m3_most_likely": round(fa["method3"]["most_likely"], 0),
        "m3_stretch": round(fa["method3"]["stretch"], 0),
        "m4_commit": round(m4.get("commit", 0), 0),
        "m4_most_likely": round(m4.get("most_likely", 0), 0),
        "m4_stretch": round(m4.get("stretch", 0), 0),
        "deployed_to_date": round(deployed["acv"], 0),
    }
    file_exists = os.path.exists(tracking_path)
    with open(tracking_path, "a", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=headers)
        if not file_exists:
            writer.writeheader()
        writer.writerow(row)
    print(f"  ✓ Forecast tracking logged: {tracking_path}")


def q_backtest_models(conn, hist_rates):
    """Backtest: Reconstruct what each forecast model would have predicted at day 31
    of each historical quarter, then compare to actual final deployed.

    Uses DIM_USE_CASE_HISTORY_DS snapshots for point-in-time pipeline state.
    Method 1 (Pipeline Risk): Reconstructs good/at-risk from snapshot STAGE_NUMBER + DAYS_IN_STAGE.
    Method 2 (Historical Pacing): Uses deployed-at-day-31 / final ratio from prior quarters.
    Method 3 (Stage Conversion): Applies average conversion rates to snapshot pipeline phases.
    """
    if not hist_rates:
        return []

    snapshot_table = "SALES.DEV.DIM_USE_CASE_HISTORY_DS"
    raven_uc = CONFIG["raven_uc_table"]
    raven_acct = CONFIG["raven_acct_table"]
    gvp = CONFIG["gvp_name"]
    thresholds = CONFIG["risk_thresholds"]

    # Compute average conversion rates across all available historical quarters
    imp_rates, tw_rates, pre_tw_rates, new_pcts = [], [], [], []
    pacing_ratios = []
    for r in hist_rates:
        imp_t = float(r.get("IMP_TOTAL", 0) or 0)
        imp_c = float(r.get("IMP_CONVERTED", 0) or 0)
        tw_t = float(r.get("TW_TOTAL", 0) or 0)
        tw_c = float(r.get("TW_CONVERTED", 0) or 0)
        pt_t = float(r.get("PRE_TW_TOTAL", 0) or 0)
        pt_c = float(r.get("PRE_TW_CONVERTED", 0) or 0)
        new_c = float(r.get("NEW_PIPELINE_CONVERTED", 0) or 0)
        final = float(r.get("FINAL_DEPLOYED", 0) or 0)
        d_n = float(r.get("DEPLOYED_ACV", 0) or 0)
        if imp_t > 0:
            imp_rates.append(imp_c / imp_t)
        if tw_t > 0:
            tw_rates.append(tw_c / tw_t)
        if pt_t > 0:
            pre_tw_rates.append(pt_c / pt_t)
        if final > 0:
            new_pcts.append(new_c / final)
        if final > 0 and d_n > 0:
            pacing_ratios.append(d_n / final)

    avg_imp = sum(imp_rates) / len(imp_rates) if imp_rates else 0
    avg_tw = sum(tw_rates) / len(tw_rates) if tw_rates else 0
    avg_pre_tw = sum(pre_tw_rates) / len(pre_tw_rates) if pre_tw_rates else 0
    avg_new = sum(new_pcts) / len(new_pcts) if new_pcts else 0
    avg_pacing = sum(pacing_ratios) / len(pacing_ratios) if pacing_ratios else 0
    min_pacing = max(pacing_ratios) if pacing_ratios else 0  # highest ratio = commit
    max_pacing = min(pacing_ratios) if pacing_ratios else 0  # lowest ratio = stretch

    quarters = CONFIG["prior_fy_quarters"]
    results = []

    for i, (qs, qe) in enumerate(quarters):
        q_label = f"{CONFIG['prior_fy_label']} Q{i+1}"
        # Day 31 snapshot date
        snap_date_str = f"DATEADD('day', 30, '{qs}')::DATE"

        # Check if this quarter has hist_rates data (skip if not, e.g. Q1)
        matching_hist = [r for r in hist_rates if r["QTR"] == q_label]
        if not matching_hist:
            continue

        hr = matching_hist[0]
        final_deployed = float(hr.get("FINAL_DEPLOYED", 0) or 0)
        deployed_at_day_n = float(hr.get("DEPLOYED_ACV", 0) or 0)

        # Query snapshot for Method 1: Pipeline Risk reconstruction
        # and Method 3: Pipeline phases with GO_LIVE scoping
        m1_sql = f"""
            SELECT
                -- Method 1: Pipeline Risk (stage-based)
                SUM(CASE WHEN h.IS_DEPLOYED = TRUE
                         AND h.ACTUAL_USE_CASE_DEPLOYMENT_DATE <= {snap_date_str}
                         AND h.ACTUAL_USE_CASE_DEPLOYMENT_DATE BETWEEN '{qs}' AND '{qe}'
                    THEN h.USE_CASE_EACV ELSE 0 END) as DEPLOYED_ACV,
                -- Stage 6 pipeline
                SUM(CASE WHEN h.STAGE_NUMBER = 6
                         AND h.IS_DEPLOYED = FALSE AND h.IS_LOST = FALSE
                         AND h.GO_LIVE_DATE BETWEEN '{qs}' AND '{qe}'
                    THEN h.USE_CASE_EACV ELSE 0 END) as STAGE6_ACV,
                -- Stage 5 Good pipeline
                SUM(CASE WHEN h.STAGE_NUMBER = 5
                         AND h.IS_DEPLOYED = FALSE AND h.IS_LOST = FALSE
                         AND h.GO_LIVE_DATE BETWEEN '{qs}' AND '{qe}'
                         AND (h.DAYS_IN_STAGE + DATEDIFF('day', {snap_date_str}, '{qe}'::DATE) + 1) >= {thresholds["stage_5"]}
                    THEN h.USE_CASE_EACV ELSE 0 END) as STAGE5_GOOD_ACV,
                -- All Good pipeline (stages 1-6)
                SUM(CASE WHEN h.IS_DEPLOYED = FALSE AND h.IS_LOST = FALSE
                         AND h.GO_LIVE_DATE BETWEEN '{qs}' AND '{qe}'
                         AND h.STAGE_NUMBER BETWEEN 1 AND 6
                         AND (
                             (h.STAGE_NUMBER IN (1,2,3) AND (h.DAYS_IN_STAGE + DATEDIFF('day', {snap_date_str}, '{qe}'::DATE) + 1) >= {thresholds["stage_123"]})
                             OR (h.STAGE_NUMBER = 4 AND (h.DAYS_IN_STAGE + DATEDIFF('day', {snap_date_str}, '{qe}'::DATE) + 1) >= {thresholds["stage_4"]})
                             OR (h.STAGE_NUMBER = 5 AND (h.DAYS_IN_STAGE + DATEDIFF('day', {snap_date_str}, '{qe}'::DATE) + 1) >= {thresholds["stage_5"]})
                             OR h.STAGE_NUMBER = 6
                         )
                    THEN h.USE_CASE_EACV ELSE 0 END) as GOOD_ACV,
                -- All pipeline ACV (stages 1-6)
                SUM(CASE WHEN h.IS_DEPLOYED = FALSE AND h.IS_LOST = FALSE
                         AND h.GO_LIVE_DATE BETWEEN '{qs}' AND '{qe}'
                         AND h.STAGE_NUMBER BETWEEN 1 AND 6
                    THEN h.USE_CASE_EACV ELSE 0 END) as ALL_PIPELINE_ACV,
                -- Method 3: Pipeline phases with GL in quarter (snapshot)
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
              AND h.ACCOUNT_GVP = '{gvp}'
              AND h.USE_CASE_EACV > 0
        """
        rows = run_query(conn, m1_sql, f"Backtest {q_label}: Snapshot Pipeline")
        if not rows:
            continue
        r = rows[0]

        dep = float(r["DEPLOYED_ACV"] or 0)
        s6 = float(r["STAGE6_ACV"] or 0)
        s5_good = float(r["STAGE5_GOOD_ACV"] or 0)
        good = float(r["GOOD_ACV"] or 0)
        all_pipe = float(r["ALL_PIPELINE_ACV"] or 0)
        imp_a = float(r["IMP_ACV"] or 0)
        tw_a = float(r["TW_ACV"] or 0)
        pre_tw_a = float(r["PRE_TW_ACV"] or 0)

        # Method 1: Pipeline Risk
        m1_commit = dep + s6 + s5_good
        m1_ml = dep + good
        m1_stretch = dep + all_pipe

        # Method 2: Historical Pacing
        m2_commit = dep / min_pacing if min_pacing > 0 else 0
        m2_ml = dep / avg_pacing if avg_pacing > 0 else 0
        m2_stretch = dep / max_pacing if max_pacing > 0 else 0

        # Method 3: Stage Conversion
        known_ml = dep + (imp_a * avg_imp) + (tw_a * avg_tw) + (pre_tw_a * avg_pre_tw)
        m3_ml = known_ml / (1 - avg_new) if avg_new < 1 else known_ml
        min_imp = min(imp_rates) if imp_rates else 0
        min_tw = min(tw_rates) if tw_rates else 0
        min_pre_tw = min(pre_tw_rates) if pre_tw_rates else 0
        min_new = min(new_pcts) if new_pcts else 0
        max_new = max(new_pcts) if new_pcts else 0
        known_commit = dep + (imp_a * min_imp) + (tw_a * min_tw) + (pre_tw_a * min_pre_tw)
        m3_commit = known_commit / (1 - min_new) if min_new < 1 else known_commit
        m3_stretch = (dep + imp_a + tw_a + pre_tw_a) / (1 - max_new) if max_new < 1 else (dep + imp_a + tw_a + pre_tw_a)

        # Blended
        bl_commit = (m1_commit + m2_commit + m3_commit) / 3
        bl_ml = (m1_ml + m2_ml + m3_ml) / 3
        bl_stretch = (m1_stretch + m2_stretch + m3_stretch) / 3

        results.append({
            "quarter": q_label,
            "final_deployed": final_deployed,
            "m1": {"commit": m1_commit, "most_likely": m1_ml, "stretch": m1_stretch},
            "m2": {"commit": m2_commit, "most_likely": m2_ml, "stretch": m2_stretch},
            "m3": {"commit": m3_commit, "most_likely": m3_ml, "stretch": m3_stretch},
            "blended": {"commit": bl_commit, "most_likely": bl_ml, "stretch": bl_stretch},
        })

    return results

def build_risk_narrative(risk_data):
    """
    Build a synthesized risk narrative from risk detail data.
    Parses semicolon-delimited USE_CASE_RISK categories, counts per category,
    sums ACV, and analyzes SE_COMMENTS/NEXT_STEPS to identify common themes
    rather than quoting raw text.
    """
    risk_rows = risk_data["risk_rows"]
    total_count = risk_data["total_count"]

    if not risk_rows:
        return {
            "at_risk": 0,
            "total": total_count,
            "acv_at_risk": 0,
            "narrative_html": "No use cases flagged with risk this quarter.",
        }

    # Parse categories and group use cases
    category_data = defaultdict(lambda: {"use_cases": [], "total_acv": 0})

    for row in risk_rows:
        risk_str = safe_str(row.get("USE_CASE_RISK", ""))
        acv = float(row.get("USE_CASE_EACV", 0) or 0)
        account = safe_str(row.get("ACCOUNT_NAME", ""))
        uc_name = safe_str(row.get("USE_CASE_NAME", ""))
        se_comments = safe_str(row.get("SE_COMMENTS", ""))
        next_steps = safe_str(row.get("NEXT_STEPS", ""))
        stage = safe_str(row.get("USE_CASE_STAGE", ""))

        # Get the latest comment only for theme analysis
        latest_comment = extract_latest_comment(se_comments).lower()
        latest_next = extract_latest_comment(next_steps).lower()
        combined_text = latest_comment + " " + latest_next

        # Detect themes from the text
        themes = _detect_themes(combined_text)

        categories = [c.strip() for c in risk_str.split(";") if c.strip() and c.strip() != "None"]

        for cat in categories:
            category_data[cat]["use_cases"].append({
                "account": account,
                "uc_name": uc_name,
                "acv": acv,
                "stage": stage,
                "themes": themes,
            })
            category_data[cat]["total_acv"] += acv

    # Build narrative bullets ordered by ACV descending
    sorted_cats = sorted(category_data.items(), key=lambda x: x[1]["total_acv"], reverse=True)

    at_risk_count = len(risk_rows)
    acv_at_risk = sum(float(r.get("USE_CASE_EACV", 0) or 0) for r in risk_rows)

    bullets = []
    for cat_name, cat_info in sorted_cats:
        n_ucs = len(cat_info["use_cases"])
        cat_acv = cat_info["total_acv"]

        # Synthesize themes across all use cases in this category
        synthesis = _synthesize_category(cat_name, cat_info["use_cases"])

        # Account names for context
        account_names = list(dict.fromkeys(uc["account"] for uc in cat_info["use_cases"]))  # unique, order preserved
        if len(account_names) <= 3:
            acct_str = ", ".join(html_escape(a) for a in account_names)
        else:
            acct_str = ", ".join(html_escape(a) for a in account_names[:3]) + f" +{len(account_names)-3} more"

        uc_word = "use case" if n_ucs == 1 else "use cases"
        bullets.append(
            f'&bull; <strong>{html_escape(cat_name)} ({n_ucs} {uc_word}, {fmt_currency(cat_acv)}):</strong> '
            f'{synthesis} ({acct_str})'
        )

    narrative_html = "<br>\n".join(bullets)

    return {
        "at_risk": at_risk_count,
        "total": total_count,
        "acv_at_risk": acv_at_risk,
        "narrative_html": narrative_html,
    }


# Theme detection keywords — maps keyword patterns to theme labels
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
    """Detect risk themes from comment text. Returns list of theme strings."""
    themes = []
    for pattern, theme_label in _THEME_PATTERNS:
        if pattern.search(text):
            themes.append(theme_label)
    return themes if themes else ["details pending"]


def _synthesize_category(cat_name, use_cases):
    """
    Synthesize a brief narrative for a risk category based on detected themes.
    Returns a concise sentence describing what's driving the risk.
    """
    # Count theme occurrences across all use cases
    theme_counts = defaultdict(int)
    for uc in use_cases:
        for theme in uc["themes"]:
            theme_counts[theme] += 1

    # Sort by frequency
    sorted_themes = sorted(theme_counts.items(), key=lambda x: x[1], reverse=True)

    # Take top 2-3 themes for the synthesis
    top_themes = [t[0] for t in sorted_themes[:3]]

    # Filter out "details pending" if we have real themes
    real_themes = [t for t in top_themes if t != "details pending"]
    if not real_themes:
        return "Risk flagged, monitoring for updates"

    # Build a readable synthesis
    n = len(use_cases)
    if n == 1:
        return "; ".join(real_themes)
    else:
        # Count how many UCs share the top theme
        top_theme = real_themes[0]
        top_count = theme_counts[top_theme]
        if len(real_themes) == 1:
            if top_count == n:
                return f"{top_theme}"
            return f"{top_theme} ({top_count} of {n})"
        else:
            secondary = "; ".join(real_themes[1:])
            return f"{top_theme}; also {secondary}"


# =============================================================================
# HTML ROW BUILDERS
# =============================================================================

def build_use_case_row(uc, consumption, bronze_tb=None, si_usage=None, row_type="standard"):
    """
    Build a single use case <tr> row.
    row_type: "standard" (Top5/SQL Server), "bronze", "si"
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
    description = html_escape(truncate_text(uc.get("USE_CASE_DESCRIPTION", ""), 300))
    implementer = html_escape(uc.get("IMPLEMENTER", ""))
    partner = html_escape(uc.get("PARTNER_NAME", ""))

    sf_url = f'{CONFIG["salesforce_base_url"]}{uc_id}'
    forecast_class = get_forecast_class(forecast_status)
    forecast_label = get_forecast_label(forecast_status)

    # Consumption data
    cons = consumption.get(account_id, {})
    if cons:
        rev_90d = fmt_currency(cons.get("REV_90D"), compact=False) if cons.get("REV_90D") is not None else "N/A"
        growth_90d = f'{int(cons.get("GROWTH_90D_PCT", 0) or 0)}%'
        run_rate = fmt_currency(cons.get("RUN_RATE"), compact=False) if cons.get("RUN_RATE") is not None else "N/A"
        rr_growth = f'{int(cons.get("RUN_RATE_GROWTH_PCT", 0) or 0)}%'
        cons_line = f'<span class="consumption">90D: {rev_90d} ({growth_90d}) | Run Rate: {run_rate} ({rr_growth})</span>'
    else:
        cons_line = '<span class="consumption">90D: N/A | Run Rate: N/A</span>'

    # Column 1: Account / Use Case
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
            credits = int(si_data.get("SI_CREDITS", 0) or 0)
            revenue = fmt_currency(si_data.get("SI_REVENUE"), compact=False)
            users = int(si_data.get("SI_USERS", 0) or 0)
            col1_lines.append(f'<br>\n    <span class="si-metrics">SI 30D: {credits} Credits | {revenue} | {users} Users</span>')

    # Column 2: Details
    risk_line = ""
    if risk and risk.lower() not in ("none", "", "-"):
        risk_line = f'<div class="details-row"><span class="risk"><strong>Risk:</strong> {html_escape(risk)}</span></div>'

    col2 = f"""
    <div class="details-row"><span class="label">Forecast:</span> <span class="status-{forecast_class}">{forecast_label}</span> &nbsp; <span class="label">Go-Live:</span> <span class="date">{go_live}</span></div>
    <div class="details-row"><span class="label">Stage:</span> <span class="stage">{stage}</span></div>
    <div class="details-row"><span class="label">AE:</span> <span class="ae-name">{ae}</span> &nbsp;|&nbsp; <span class="label">SE:</span> {se} &nbsp;|&nbsp; <span class="label">Region:</span> {region}</div>
    {risk_line}
    <div class="next-steps"><strong>Next Steps:</strong> {next_steps}</div>
    <div class="se-comments"><strong>SE Comments:</strong> {se_comments}</div>"""

    # Column 3: Summary
    partner_line = f'<div class="partner"><strong>Partner:</strong> {partner}</div>' if partner else ""
    col3 = f"""{description}
    <div class="implementer"><strong>Implementer:</strong> {implementer if implementer else 'None'}</div>
    {partner_line}"""

    return f"""<tr>
  <td>
    {''.join(col1_lines)}
  </td>
  <td class="details-col">
    {col2}
  </td>
  <td class="summary">
    {col3}
  </td>
</tr>"""


# =============================================================================
# HTML TEMPLATE
# =============================================================================

CSS = """
    body { font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, sans-serif; max-width: 1200px; margin: 0 auto; padding: 20px; background: #f5f5f5; }
    h1 { color: #29B5E8; border-bottom: 3px solid #29B5E8; padding-bottom: 10px; }
    h2 { color: #1a1a2e; margin-top: 30px; }
    h3 { color: #29B5E8; margin-top: 25px; border-left: 4px solid #29B5E8; padding-left: 10px; }
    .forecast-table { width: 300px; border-collapse: collapse; margin: 20px 0; }
    .forecast-table th, .forecast-table td { padding: 12px 20px; text-align: left; border: 1px solid #ddd; }
    .forecast-table th { background: #29B5E8; color: white; }
    .forecast-table td { background: white; }
    .use-case-table { width: 100%; border-collapse: collapse; margin: 20px 0; }
    .use-case-table th { background: #1a1a2e; color: white; padding: 15px; text-align: left; }
    .use-case-table td { padding: 15px; border: 1px solid #ddd; background: white; vertical-align: top; }
    .use-case-table tr:nth-child(even) td { background: #fafafa; }
    .sales-play-table { width: 100%; border-collapse: collapse; margin: 20px 0; }
    .sales-play-table th { background: #1a1a2e; color: white; padding: 12px 15px; text-align: left; }
    .sales-play-table td { padding: 12px 15px; border: 1px solid #ddd; background: white; }
    .sales-play-table tr:nth-child(even) td { background: #fafafa; }
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
    .footer { margin-top: 40px; padding-top: 20px; border-top: 1px solid #ddd; color: #888; font-size: 0.9em; }
    .uc-number { font-size: 0.85em; color: #666; }
    .consumption { font-size: 0.8em; color: #17a2b8; font-weight: 500; }
    .bronze-tb { font-size: 0.8em; color: #cd7f32; font-weight: 500; }
    .si-metrics { font-size: 0.8em; color: #9b59b6; font-weight: 500; }
    .open-acv { color: #dc3545; font-weight: 600; }
    .deployed-acv { color: #28a745; font-weight: 600; }
    .implementer { font-size: 0.85em; color: #2c3e50; margin-top: 10px; }
    .partner { font-size: 0.85em; color: #8e44ad; }
    .play-section { margin-bottom: 40px; }
    .analysis { background: #e8f4f8; border-left: 4px solid #29B5E8; padding: 15px; margin: 15px 0; font-size: 0.95em; line-height: 1.6; }
    .transcript-note { background: #fff8e6; border-left: 4px solid #ffc107; padding: 12px 15px; margin-top: 10px; font-size: 0.9em; }
    .transcript-note strong { color: #856404; }
    .risk-box { background: #fff5f5; border-left: 4px solid #dc3545; padding: 15px; margin: 15px 0; }
    .timeline-box { display: flex; align-items: center; justify-content: center; gap: 12px; padding: 10px 0; margin: 5px 0; }
    .timeline-item { text-align: center; background: #1a1a2e; border-radius: 8px; padding: 12px 20px; min-width: 120px; box-shadow: 0 3px 10px rgba(0,0,0,0.15); }
    .timeline-label { display: block; font-size: 0.8em; color: #aaa; margin-bottom: 6px; text-transform: uppercase; letter-spacing: 0.5px; }
    .timeline-days { display: block; font-size: 2em; font-weight: bold; color: #29B5E8; }
    .timeline-arrow { font-size: 3em; color: #29B5E8; font-weight: bold; }
    /* Tab navigation */
    .tab-nav { display: flex; gap: 0; margin-top: 20px; border-bottom: 3px solid #29B5E8; }
    .tab-btn { padding: 12px 30px; font-size: 1em; font-weight: 600; cursor: pointer; border: 1px solid #ddd; border-bottom: none;
               background: #e9ecef; color: #555; border-radius: 8px 8px 0 0; margin-bottom: -3px; transition: all 0.2s; }
    .tab-btn:hover { background: #d6e9f8; }
    .tab-btn.active { background: white; color: #29B5E8; border-color: #29B5E8; border-bottom: 3px solid white; }
    .tab-content { display: none; padding-top: 10px; }
    .tab-content.active { display: block; }
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
"""


def _build_forecast_tab(fa, forecasts, deployed, day_number, week_number):
    """Build the Forecast Analysis tab HTML content."""
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

    # Pipeline phase bar widths (relative to total)
    total_pipeline = pp["deployed"] + pp["in_imp"] + pp["post_tw"] + pp["pre_tw"]
    def bar_pct(val):
        return max(2, round(val / total_pipeline * 100)) if total_pipeline > 0 else 0
    def bar_label(val, label):
        """Only show text label if segment is wide enough (>=8%), otherwise empty."""
        pct = (val / total_pipeline * 100) if total_pipeline > 0 else 0
        return label if pct >= 8 else ""

    # Historical pacing table rows
    hist_rows = ""
    for r in hist:
        qtr = r.get("QTR", "")
        d_acv = float(r.get("DEPLOYED_ACV", 0) or 0)
        final = float(r.get("FINAL_DEPLOYED", 0) or 0)
        ratio = (d_acv / final * 100) if final > 0 else 0
        imp_t = float(r.get("IMP_TOTAL", 0) or 0)
        imp_c = float(r.get("IMP_CONVERTED", 0) or 0)
        imp_r = (imp_c / imp_t * 100) if imp_t > 0 else 0
        tw_t = float(r.get("TW_TOTAL", 0) or 0)
        tw_c = float(r.get("TW_CONVERTED", 0) or 0)
        tw_r = (tw_c / tw_t * 100) if tw_t > 0 else 0
        pt_t = float(r.get("PRE_TW_TOTAL", 0) or 0)
        pt_c = float(r.get("PRE_TW_CONVERTED", 0) or 0)
        pt_r = (pt_c / pt_t * 100) if pt_t > 0 else 0
        new_c = float(r.get("NEW_PIPELINE_CONVERTED", 0) or 0)
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

    # Current quarter row for comparison
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
historical pacing, and milestone-based conversion rates. All data is for AMSExpansion / {CONFIG["gvp_name"]} only.</p>

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
  <tr>
    <th>Quarter</th>
    <th>Deployed at Day {day_number}</th>
    <th>Final Deployed</th>
    <th>Day {day_number} / Final</th>
    <th>Imp Conv %</th>
    <th>TW Conv %</th>
    <th>Pre-TW Conv %</th>
    <th>New Pipeline %</th>
  </tr>
  {hist_rows}
  {current_row}
</table>
<div class="fa-note">
    <strong>Reading this table:</strong> "Imp Conv %" = % of ACV in Implementation at day {day_number} that eventually deployed.
    "New Pipeline %" = % of final deployed that came from use cases not yet created at day {day_number}.
    Conversion rates are specific to AMSExpansion at this point in the quarter.
</div>

<h3>Forecast Models</h3>
<div class="fa-grid">

  <div class="fa-card method1">
    <h4>Method 1: Pipeline Risk Model</h4>
    <p class="summary">Uses stage-based risk thresholds (days-in-stage vs days-remaining) to classify pipeline as "good" or "at risk."</p>
    <div class="fa-vs">
      <div class="fa-vs-item"><div class="fa-vs-label">Commit</div><div class="fa-vs-value commit">{fmt_currency(m1["commit"])}</div><div class="fa-sublabel">Deployed + Stage 6 + Stage 5 Good</div></div>
      <div class="fa-vs-item"><div class="fa-vs-label">Most Likely</div><div class="fa-vs-value likely">{fmt_currency(m1["most_likely"])}</div><div class="fa-sublabel">Deployed + Good Pipeline</div></div>
      <div class="fa-vs-item"><div class="fa-vs-label">Stretch</div><div class="fa-vs-value stretch">{fmt_currency(m1["stretch"])}</div><div class="fa-sublabel">Deployed + All Pipeline</div></div>
    </div>
    <div class="fa-note">Conservative model &mdash; does not account for new pipeline created after today or historical outperformance of "at risk" items.</div>
  </div>

  <div class="fa-card method2">
    <h4>Method 2: Historical Pacing Model</h4>
    <p class="summary">Projects final deployed by applying {prior_fy} deployed-at-day-{day_number} / final ratios to current deployed ACV.</p>
    <div class="fa-vs">
      <div class="fa-vs-item"><div class="fa-vs-label">Commit</div><div class="fa-vs-value commit">{fmt_currency(m2["commit"])}</div><div class="fa-sublabel">Best prior ratio</div></div>
      <div class="fa-vs-item"><div class="fa-vs-label">Most Likely</div><div class="fa-vs-value likely">{fmt_currency(m2["most_likely"])}</div><div class="fa-sublabel">Avg prior ratio</div></div>
      <div class="fa-vs-item"><div class="fa-vs-label">Stretch</div><div class="fa-vs-value stretch">{fmt_currency(m2["stretch"])}</div><div class="fa-sublabel">Lowest prior ratio</div></div>
    </div>
    <div class="fa-note">Trend-based model &mdash; assumes current quarter follows historical deployment velocity patterns. Sensitive to current deployed ACV.</div>
  </div>

  <div class="fa-card method3">
    <h4>Method 3: Stage Conversion Model</h4>
    <p class="summary">Applies historical milestone-to-deployed conversion rates to each current pipeline phase, plus a new-pipeline uplift factor.</p>
    <table class="fa-table" style="margin-bottom: 10px;">
      <tr><th>Phase</th><th>Current ACV</th><th>Avg Conv Rate</th><th>Expected ACV</th></tr>
      <tr><td>Already Deployed</td><td class="number">{fmt_currency(pp["deployed"])}</td><td class="number">100%</td><td class="number">{fmt_currency(pp["deployed"])}</td></tr>
      <tr><td>In Implementation</td><td class="number">{fmt_currency(pp["in_imp"])}</td><td class="number">{rates.get("imp", 0)*100:.1f}%</td><td class="number">{fmt_currency(pp["in_imp"] * rates.get("imp", 0))}</td></tr>
      <tr><td>Post-TW / Pre-Imp</td><td class="number">{fmt_currency(pp["post_tw"])}</td><td class="number">{rates.get("tw", 0)*100:.1f}%</td><td class="number">{fmt_currency(pp["post_tw"] * rates.get("tw", 0))}</td></tr>
      <tr><td>Pre-TW</td><td class="number">{fmt_currency(pp["pre_tw"])}</td><td class="number">{rates.get("pre_tw", 0)*100:.1f}%</td><td class="number">{fmt_currency(pp["pre_tw"] * rates.get("pre_tw", 0))}</td></tr>
      <tr style="background: #e8f4f8;"><td><strong>New Pipeline Uplift</strong></td><td class="number">&mdash;</td><td class="number">{rates.get("new_pipeline", 0)*100:.1f}% of final</td><td class="number">Applied to total</td></tr>
    </table>
    <div class="fa-vs">
      <div class="fa-vs-item"><div class="fa-vs-label">Commit</div><div class="fa-vs-value commit">{fmt_currency(m3["commit"])}</div><div class="fa-sublabel">Min conv rates</div></div>
      <div class="fa-vs-item"><div class="fa-vs-label">Most Likely</div><div class="fa-vs-value likely">{fmt_currency(m3["most_likely"])}</div><div class="fa-sublabel">Avg conv rates</div></div>
      <div class="fa-vs-item"><div class="fa-vs-label">Stretch</div><div class="fa-vs-value stretch">{fmt_currency(m3["stretch"])}</div><div class="fa-sublabel">Full pipeline + max uplift</div></div>
    </div>
  </div>

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
        M1: {m4.get("weights", {}).get("most_likely", {}).get("m1", 0)*100:.0f}% |
        M2: {m4.get("weights", {}).get("most_likely", {}).get("m2", 0)*100:.0f}% |
        M3: {m4.get("weights", {}).get("most_likely", {}).get("m3", 0)*100:.0f}%
        &mdash; derived from {prior_fy} backtest error{f', adjusted for day {m4.get("day_number", 31)}' if m4.get("day_number", 31) != 31 else ''}.
    </div>"""

    # Confidence intervals
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
        with a new-pipeline uplift factor. All data is AMSExpansion-specific.
    </div>
  </div>

</div><!-- end fa-grid -->
"""

    # --- Average Absolute Error Summary (moved above velocity/risk for quick reference) ---
    backtest = fa.get("backtest", [])
    if backtest:
        def _err_pct(predicted, actual):
            if actual == 0:
                return "N/A"
            pct = (predicted - actual) / actual * 100
            color = "#28a745" if abs(pct) <= 10 else "#b8860b" if abs(pct) <= 20 else "#dc3545"
            return f'<span style="color: {color};">{pct:+.1f}%</span>'

        def _err_color(predicted, actual):
            if actual == 0:
                return ""
            pct = abs((predicted - actual) / actual * 100)
            if pct <= 10:
                return "background: #d4edda;"
            elif pct <= 20:
                return "background: #fff3cd;"
            return "background: #f8d7da;"

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
"""
        parts += """
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
            m4_err = _avg_abs_err(backtest, "m4", call_key)
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

    # Pipeline Velocity section
    vel = fa.get("velocity", {})
    if vel:
        cur = vel.get("current", {})
        hist_vel = vel.get("historical", [])
        v7 = cur.get("v7", 0)
        v14 = cur.get("v14", 0)
        v30 = cur.get("v30", 0)

        # Compute daily rates
        rate7 = v7 / 7 if v7 else 0
        rate14 = v14 / 14 if v14 else 0
        rate30 = v30 / 30 if v30 else 0

        # Trend indicator: compare 7-day rate vs 30-day rate
        if rate30 > 0:
            accel = (rate7 - rate30) / rate30 * 100
            if accel > 10:
                trend_icon = '<span style="color: #28a745; font-weight: bold;">&#9650; Accelerating</span>'
            elif accel < -10:
                trend_icon = '<span style="color: #dc3545; font-weight: bold;">&#9660; Decelerating</span>'
            else:
                trend_icon = '<span style="color: #b8860b; font-weight: bold;">&#9654; Steady</span>'
        else:
            accel = 0
            trend_icon = '<span style="color: #999;">N/A</span>'

        # Historical velocity rows
        hist_vel_rows = ""
        for hv in hist_vel:
            hv7 = float(hv.get("V7", 0) or 0)
            hv14 = float(hv.get("V14", 0) or 0)
            hv30 = float(hv.get("V30", 0) or 0)
            hr7 = hv7 / 7 if hv7 else 0
            hr14 = hv14 / 14 if hv14 else 0
            hr30 = hv30 / 30 if hv30 else 0
            hist_vel_rows += f"""<tr>
    <td><strong>{hv.get('QTR', '')}</strong></td>
    <td class="number">{fmt_currency(hv7)}</td>
    <td class="number">{fmt_currency(hv14)}</td>
    <td class="number">{fmt_currency(hv30)}</td>
    <td class="number">{fmt_currency(hr7)}/day</td>
    <td class="number">{fmt_currency(hr30)}/day</td>
</tr>"""

        # Current vs historical average comparison
        if hist_vel:
            avg_hist_v30 = sum(float(hv.get("V30", 0) or 0) for hv in hist_vel) / len(hist_vel)
            vs_hist = ((v30 - avg_hist_v30) / avg_hist_v30 * 100) if avg_hist_v30 > 0 else 0
            vs_hist_label = f"{vs_hist:+.1f}% vs {prior_fy} avg"
        else:
            vs_hist_label = "No historical data"

        parts += f"""
<h3>Pipeline Velocity &amp; Acceleration</h3>
<div class="fa-note" style="margin-bottom: 12px;">
    Deployment velocity measures ACV deployed in trailing windows. Comparing current rates against the same
    day-{day_number} point in prior quarters reveals whether the pipeline is converting faster or slower than historical norms.
</div>

<div style="display: flex; gap: 20px; margin-bottom: 20px;">
  <div class="fa-card" style="flex: 1; border-left: 4px solid #28a745; padding: 15px;">
    <h4 style="margin: 0 0 10px 0;">Current Quarter Velocity</h4>
    <table style="width: 100%; font-size: 0.9em;">
      <tr><td>Last 7 days</td><td class="number" style="font-weight: bold;">{fmt_currency(v7)}</td><td class="number" style="color: #666;">{fmt_currency(rate7)}/day</td></tr>
      <tr><td>Last 14 days</td><td class="number" style="font-weight: bold;">{fmt_currency(v14)}</td><td class="number" style="color: #666;">{fmt_currency(rate14)}/day</td></tr>
      <tr><td>Last 30 days</td><td class="number" style="font-weight: bold;">{fmt_currency(v30)}</td><td class="number" style="color: #666;">{fmt_currency(rate30)}/day</td></tr>
    </table>
  </div>
  <div class="fa-card" style="flex: 1; border-left: 4px solid #29B5E8; padding: 15px;">
    <h4 style="margin: 0 0 10px 0;">Trend</h4>
    <div style="font-size: 1.4em; margin: 10px 0;">{trend_icon}</div>
    <div style="font-size: 0.85em; color: #666;">7-day rate vs 30-day rate: {accel:+.1f}%</div>
    <div style="font-size: 0.85em; color: #666; margin-top: 5px;">30-day total: {vs_hist_label}</div>
  </div>
</div>

<h4>Historical Velocity at Day {day_number}</h4>
<table class="fa-table">
  <tr>
    <th>Quarter</th>
    <th>7-Day Window</th>
    <th>14-Day Window</th>
    <th>30-Day Window</th>
    <th>7-Day Rate</th>
    <th>30-Day Rate</th>
  </tr>
  <tr style="background: #e8f4f8; font-weight: 600;">
    <td><strong>{CONFIG["fiscal_year_label"]} Q1 (Current)</strong></td>
    <td class="number">{fmt_currency(v7)}</td>
    <td class="number">{fmt_currency(v14)}</td>
    <td class="number">{fmt_currency(v30)}</td>
    <td class="number">{fmt_currency(rate7)}/day</td>
    <td class="number">{fmt_currency(rate30)}/day</td>
  </tr>
  {hist_vel_rows}
</table>
<div class="fa-note">
    <strong>Reading this table:</strong> Each row shows how much ACV was deployed in the 7/14/30 days leading up to day {day_number}
    of that quarter. Higher velocity at the same day-N suggests stronger conversion momentum.
    {trend_icon} indicates whether recent deployment pace (7-day) is faster or slower than the trailing 30-day rate.
</div>
"""

    # Risk-Adjusted Pipeline Detail table
    pipe_detail = fa.get("pipeline_detail", [])
    if pipe_detail:
        detail_rows = ""
        total_acv = 0
        good_acv = 0
        risk_acv = 0
        for uc in pipe_detail:
            acv = float(uc.get("USE_CASE_EACV", 0) or 0)
            total_acv += acv
            risk = uc.get("RISK_STATUS", "")
            if risk == "Good":
                good_acv += acv
                risk_badge = '<span style="background: #d4edda; color: #155724; padding: 2px 8px; border-radius: 4px; font-size: 0.8em;">Good</span>'
            else:
                risk_acv += acv
                risk_badge = '<span style="background: #f8d7da; color: #721c24; padding: 2px 8px; border-radius: 4px; font-size: 0.8em;">At Risk</span>'
            stage = uc.get("USE_CASE_STAGE", "")
            days = int(uc.get("DAYS_IN_STAGE", 0) or 0)
            go_live = safe_str(uc.get("GO_LIVE_DATE", ""))[:10]
            tw_date = safe_str(uc.get("TECHNICAL_WIN_DATE", ""))[:10]
            detail_rows += f"""<tr>
    <td style="max-width: 180px; overflow: hidden; text-overflow: ellipsis; white-space: nowrap;" title="{safe_str(uc.get('USE_CASE_NAME', ''))}">{safe_str(uc.get('USE_CASE_NAME', ''))[:40]}</td>
    <td style="max-width: 120px; overflow: hidden; text-overflow: ellipsis; white-space: nowrap;">{safe_str(uc.get('ACCOUNT_NAME', ''))[:25]}</td>
    <td class="number">{fmt_currency(acv)}</td>
    <td>{stage}</td>
    <td class="number">{days}d</td>
    <td>{tw_date or '&mdash;'}</td>
    <td>{go_live}</td>
    <td style="text-align: center;">{risk_badge}</td>
</tr>"""

        good_pct = (good_acv / total_acv * 100) if total_acv > 0 else 0
        parts += f"""
<h3>Risk-Adjusted Pipeline Detail &mdash; Top 15 Open Use Cases</h3>
<div class="fa-note" style="margin-bottom: 12px;">
    Largest open use cases by ACV with risk assessment based on stage and days-in-stage thresholds.
    <strong>Total shown:</strong> {fmt_currency(total_acv)} &mdash;
    <span style="color: #28a745;">{fmt_currency(good_acv)} Good ({good_pct:.0f}%)</span> |
    <span style="color: #dc3545;">{fmt_currency(risk_acv)} At Risk ({100-good_pct:.0f}%)</span>
</div>
<table class="fa-table" style="font-size: 0.88em;">
  <tr>
    <th>Use Case</th>
    <th>Account</th>
    <th>ACV</th>
    <th>Stage</th>
    <th>Days in Stage</th>
    <th>TW Date</th>
    <th>Go-Live</th>
    <th>Risk</th>
  </tr>
  {detail_rows}
</table>
"""

    # --- Model Accuracy Detail (Per-Call Backtest) Section ---
    if backtest:
        parts += f"""
<h3>Model Accuracy Detail &mdash; {prior_fy} Backtest</h3>
<div class="fa-note" style="margin-bottom: 12px;">
    Per-quarter breakdown of each model's predictions vs actual deployed.
    Predictions are reconstructed from point-in-time snapshots using the same methodology as current forecasts.
    <span style="color: #28a745;">&le;10% error</span> |
    <span style="color: #b8860b;">10-20% error</span> |
    <span style="color: #dc3545;">&gt;20% error</span>
</div>
"""

        for call_key, call_label in [("commit", "Commit"), ("most_likely", "Most Likely"), ("stretch", "Stretch")]:
            parts += f"""
<h4>{call_label} Accuracy</h4>
<table class="fa-table">
  <tr>
    <th>Quarter</th>
    <th>Actual Deployed</th>
    <th>M1: Pipeline Risk</th>
    <th>Error</th>
    <th>M2: Pacing</th>
    <th>Error</th>
    <th>M3: Conversion</th>
    <th>Error</th>
    <th>Equal Blend</th>
    <th>Error</th>
    <th style="background: #29B5E8;">M4: Weighted</th>
    <th>Error</th>
  </tr>
"""
            for bt in backtest:
                actual = bt["final_deployed"]
                m4_val = bt.get("m4", {}).get(call_key, 0)
                parts += f"""  <tr>
    <td><strong>{bt['quarter']}</strong></td>
    <td class="number">{fmt_currency(actual)}</td>
    <td class="number" style="{_err_color(bt['m1'][call_key], actual)}">{fmt_currency(bt['m1'][call_key])}</td>
    <td class="number">{_err_pct(bt['m1'][call_key], actual)}</td>
    <td class="number" style="{_err_color(bt['m2'][call_key], actual)}">{fmt_currency(bt['m2'][call_key])}</td>
    <td class="number">{_err_pct(bt['m2'][call_key], actual)}</td>
    <td class="number" style="{_err_color(bt['m3'][call_key], actual)}">{fmt_currency(bt['m3'][call_key])}</td>
    <td class="number">{_err_pct(bt['m3'][call_key], actual)}</td>
    <td class="number" style="{_err_color(bt['blended'][call_key], actual)}">{fmt_currency(bt['blended'][call_key])}</td>
    <td class="number">{_err_pct(bt['blended'][call_key], actual)}</td>
    <td class="number" style="font-weight: bold; {_err_color(m4_val, actual)}">{fmt_currency(m4_val)}</td>
    <td class="number" style="font-weight: bold;">{_err_pct(m4_val, actual)}</td>
  </tr>
"""
            parts += "</table>\n"

    # Convergence chart from CSV tracking history
    tracking_path = os.path.join(CONFIG["output_dir"], "PEAK_forecast_tracking.csv")
    if os.path.exists(tracking_path):
        with open(tracking_path, "r") as f:
            reader = csv.DictReader(f)
            track_rows = list(reader)
        # Filter to current quarter
        current_fy = CONFIG.get("fiscal_year_label", "")
        # Find the most recent quarter in the data that matches current FY
        all_qtrs = sorted(set(r.get("quarter", "") for r in track_rows if r.get("quarter", "").startswith(current_fy)))
        current_qtr = all_qtrs[-1] if all_qtrs else ""
        qtr_rows = [r for r in track_rows if r.get("quarter", "") == current_qtr]
        if len(qtr_rows) >= 2:
            # Build SVG convergence chart
            svg_w, svg_h = 700, 300
            pad_l, pad_r, pad_t, pad_b = 70, 20, 30, 50

            # Extract data points
            dates = [r["date"] for r in qtr_rows]
            series = {
                "M1": [float(r.get("m1_most_likely", 0) or 0) for r in qtr_rows],
                "M2": [float(r.get("m2_most_likely", 0) or 0) for r in qtr_rows],
                "M3": [float(r.get("m3_most_likely", 0) or 0) for r in qtr_rows],
                "M4": [float(r.get("m4_most_likely", 0) or 0) for r in qtr_rows],
            }
            colors = {"M1": "#dc3545", "M2": "#ffc107", "M3": "#17a2b8", "M4": "#29B5E8"}

            # Scale
            all_vals = [v for s in series.values() for v in s if v > 0]
            if all_vals:
                y_min = min(all_vals) * 0.9
                y_max = max(all_vals) * 1.1
            else:
                y_min, y_max = 0, 200000000

            n = len(dates)
            def sx(i):
                return pad_l + i * (svg_w - pad_l - pad_r) / max(n - 1, 1)
            def sy(v):
                if y_max == y_min:
                    return pad_t + (svg_h - pad_t - pad_b) / 2
                return pad_t + (1 - (v - y_min) / (y_max - y_min)) * (svg_h - pad_t - pad_b)

            # Build paths
            paths_svg = ""
            for label, vals in series.items():
                points = []
                for i, v in enumerate(vals):
                    if v > 0:
                        points.append(f"{sx(i):.1f},{sy(v):.1f}")
                if points:
                    paths_svg += f'<polyline points="{" ".join(points)}" fill="none" stroke="{colors[label]}" stroke-width="2" />\n'
                    # End dot
                    last_i = len(vals) - 1
                    for i in range(len(vals) - 1, -1, -1):
                        if vals[i] > 0:
                            last_i = i
                            break
                    paths_svg += f'<circle cx="{sx(last_i):.1f}" cy="{sy(vals[last_i]):.1f}" r="4" fill="{colors[label]}" />\n'
                    paths_svg += f'<text x="{sx(last_i) + 6:.1f}" y="{sy(vals[last_i]) + 4:.1f}" font-size="11" fill="{colors[label]}">{label}</text>\n'

            # Y-axis labels (5 ticks)
            y_labels_svg = ""
            for i in range(5):
                val = y_min + i * (y_max - y_min) / 4
                y = sy(val)
                y_labels_svg += f'<text x="{pad_l - 5}" y="{y + 4}" font-size="10" fill="#666" text-anchor="end">{fmt_currency(val, compact=True)}</text>\n'
                y_labels_svg += f'<line x1="{pad_l}" y1="{y}" x2="{svg_w - pad_r}" y2="{y}" stroke="#eee" stroke-width="1" />\n'

            # X-axis labels (show first, last, and middle dates)
            x_labels_svg = ""
            label_indices = [0, n // 2, n - 1] if n >= 3 else list(range(n))
            for i in sorted(set(label_indices)):
                x_labels_svg += f'<text x="{sx(i)}" y="{svg_h - pad_b + 18}" font-size="10" fill="#666" text-anchor="middle">{dates[i]}</text>\n'

            parts += f"""
<h3>Forecast Convergence (Most Likely)</h3>
<div class="fa-note" style="margin-bottom: 12px;">
    Tracks how each model's Most Likely forecast evolves over the quarter. Converging lines indicate
    increasing model agreement. Data from {len(qtr_rows)} report runs.
</div>
<div style="text-align: center;">
<svg width="{svg_w}" height="{svg_h}" style="background: white; border: 1px solid #eee; border-radius: 6px;">
  <!-- Grid -->
  {y_labels_svg}
  <!-- Axes -->
  <line x1="{pad_l}" y1="{pad_t}" x2="{pad_l}" y2="{svg_h - pad_b}" stroke="#ccc" stroke-width="1" />
  <line x1="{pad_l}" y1="{svg_h - pad_b}" x2="{svg_w - pad_r}" y2="{svg_h - pad_b}" stroke="#ccc" stroke-width="1" />
  <!-- Series -->
  {paths_svg}
  <!-- X labels -->
  {x_labels_svg}
</svg>
</div>
<div style="text-align: center; margin-top: 8px; font-size: 0.82em;">
  <span style="color: #dc3545;">&#9632; M1: Risk</span> &nbsp;
  <span style="color: #ffc107;">&#9632; M2: Pacing</span> &nbsp;
  <span style="color: #17a2b8;">&#9632; M3: Conversion</span> &nbsp;
  <span style="color: #29B5E8; font-weight: bold;">&#9632; M4: Weighted</span>
</div>
"""

    return parts


def build_html(data):
    """Assemble the full HTML report from collected data."""

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
    high_risk_ucs = data.get("high_risk_ucs", [])
    consumption = data["consumption"]
    bronze_tb_total = data["bronze_tb_total"]
    bronze_tb_acct = data["bronze_tb_acct"]
    si_usage = data["si_usage"]
    si_theater = data["si_theater"]
    pacing = data["pacing"]
    forecast_analysis = data.get("forecast_analysis", {})

    # Wins tab data
    wins_forecasts = data.get("wins_forecasts", {"commit": 0, "most_likely": 0, "stretch": 0})
    wins_won_qtd = data.get("wins_won_qtd", {"acv": 0, "count": 0})
    wins_won_last7 = data.get("wins_won_last7", {"acv": 0, "count": 0})
    wins_in_pursuit = data.get("wins_in_pursuit", {"acv": 0, "count": 0})
    wins_top5_pursuit = data.get("wins_top5_pursuit", [])
    wins_play_summary = data.get("wins_play_summary", {})
    wins_play_use_cases = data.get("wins_play_use_cases", {})
    play_targets = data.get("play_targets", {})
    partner_sd = data.get("partner_sd", {})
    uc_velocity = data.get("uc_velocity", {})
    bronze_created = data.get("bronze_created", {})

    quarter = safe_str(fiscal["FISCAL_QUARTER"])
    qstart = safe_str(fiscal["FQ_START"])
    qend = safe_str(fiscal["FQ_END"])
    days_remaining = int(fiscal["DAYS_REMAINING"])
    day_number = int(fiscal["DAY_NUMBER"])
    week_number = int(fiscal["WEEK_NUMBER"])

    most_likely = forecasts["most_likely"]

    # Format forecast delta as small WoW change text
    def fmt_delta(delta):
        if delta is None:
            return ""
        if delta == 0:
            return '<br><span style="font-size: 0.8em; color: #888;">Flat WoW</span>'
        sign = "+" if delta > 0 else ""
        color = "#28a745" if delta > 0 else "#dc3545"
        return f'<br><span style="font-size: 0.8em; color: {color};">{sign}{fmt_currency(delta)} WoW</span>'

    # Go-Lives forecast deltas
    gl_commit_delta = fmt_delta(forecasts.get("commit_delta"))
    gl_ml_delta = fmt_delta(forecasts.get("ml_delta"))
    gl_stretch_delta = fmt_delta(forecasts.get("stretch_delta"))

    # Wins forecast deltas
    w_commit_delta = fmt_delta(wins_forecasts.get("commit_delta"))
    w_ml_delta = fmt_delta(wins_forecasts.get("ml_delta"))
    w_stretch_delta = fmt_delta(wins_forecasts.get("stretch_delta"))

    # Deployed % of Most Likely
    deployed_pct = (deployed["acv"] / most_likely * 100) if most_likely else 0
    last7_pct = (last7["acv"] / most_likely * 100) if most_likely else 0

    # Deployed % of Target (Go-Lives)
    gl_target = forecasts["target"]
    deployed_pct_of_target = (deployed["acv"] / gl_target * 100) if gl_target else 0

    # Open pipeline coverage = (Open Pipeline + Deployed) / Most Likely
    coverage_pct = ((pipeline["acv"] + deployed["acv"]) / most_likely * 100) if most_likely else 0

    # Go-Lives pipeline breakdown (Deployed + Open)
    gl_total_pipeline = deployed["acv"] + pipeline["acv"]
    gl_deployed_pct = (deployed["acv"] / gl_total_pipeline * 100) if gl_total_pipeline else 0
    gl_open_pct = (pipeline["acv"] / gl_total_pipeline * 100) if gl_total_pipeline else 0

    # Pipeline risk analysis
    def risk_line(stage_group):
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

    # Total good pipeline
    total_good = sum(float(r.get("GOOD_ACV", 0) or 0) for r in risk_analysis.values())
    # Good pipeline coverage = (Total Good Pipeline + Deployed) / Most Likely
    good_coverage = ((total_good + deployed["acv"]) / most_likely * 100) if most_likely else 0

    # Build risk narrative for each play
    bronze_risk = build_risk_narrative(play_risk["bronze"])
    si_risk = build_risk_narrative(play_risk["si"])
    sql_risk = build_risk_narrative(play_risk["sqlserver"])

    # Build use case rows
    top5_rows = "\n".join(build_use_case_row(uc, consumption) for uc in top5)
    bronze_rows = "\n".join(build_use_case_row(uc, consumption, bronze_tb=bronze_tb_acct, row_type="bronze") for uc in play_use_cases["bronze"])
    si_rows = "\n".join(build_use_case_row(uc, consumption, si_usage=si_usage, row_type="si") for uc in play_use_cases["si"])
    sql_rows = "\n".join(build_use_case_row(uc, consumption, row_type="standard") for uc in play_use_cases["sqlserver"])

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

    # Play deployed targets
    def fmt_play_target(play_key):
        t = play_targets.get(play_key, {})
        acv = t.get("acv")
        count = t.get("count")
        parts = []
        if acv is not None:
            parts.append(fmt_currency(acv))
        if count is not None:
            parts.append(f"{count} UCs")
        return " / ".join(parts) if parts else "&mdash;"

    def fmt_play_gap(play_key, deployed_acv, deployed_count):
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
    
    bronze_target = fmt_play_target("bronze")
    si_target = fmt_play_target("si")
    sqlserver_target = fmt_play_target("sqlserver")
    bronze_gap = fmt_play_gap("bronze", play_summary["bronze_deployed"]["acv"], play_summary["bronze_deployed"]["count"])
    si_gap = fmt_play_gap("si", play_summary["si_deployed"]["acv"], play_summary["si_deployed"]["count"])
    sqlserver_gap = fmt_play_gap("sqlserver", play_summary["sqlserver_deployed"]["acv"], play_summary["sqlserver_deployed"]["count"])

    # --- Wins tab rows ---
    wins_top5_rows = "\n".join(build_use_case_row(uc, consumption) for uc in wins_top5_pursuit)
    wins_bronze_rows = "\n".join(build_use_case_row(uc, consumption, bronze_tb=bronze_tb_acct, row_type="bronze") for uc in wins_play_use_cases.get("bronze", []))
    wins_si_rows = "\n".join(build_use_case_row(uc, consumption, si_usage=si_usage, row_type="si") for uc in wins_play_use_cases.get("si", []))
    wins_sql_rows = "\n".join(build_use_case_row(uc, consumption, row_type="standard") for uc in wins_play_use_cases.get("sqlserver", []))

    # Wins total pipeline = won + in pursuit
    wins_total_pipeline = wins_won_qtd["acv"] + wins_in_pursuit["acv"]
    wins_won_pct = (wins_won_qtd["acv"] / wins_total_pipeline * 100) if wins_total_pipeline else 0
    wins_pursuit_pct = (wins_in_pursuit["acv"] / wins_total_pipeline * 100) if wins_total_pipeline else 0
    wins_most_likely = wins_forecasts["most_likely"]
    wins_target = wins_forecasts["target"]
    wins_won_pct_of_ml = (wins_won_qtd["acv"] / wins_most_likely * 100) if wins_most_likely else 0
    wins_last7_pct_of_ml = (wins_won_last7["acv"] / wins_most_likely * 100) if wins_most_likely else 0
    wins_won_pct_of_target = (wins_won_qtd["acv"] / wins_target * 100) if wins_target else 0

    # Pre-compute wins play summary values for safe f-string usage
    _wps = wins_play_summary
    _empty = {"acv": 0, "count": 0}
    w_bronze_pursuit = _wps.get("bronze_pursuit", _empty)
    w_bronze_won = _wps.get("bronze_won", _empty)
    w_si_pursuit = _wps.get("si_pursuit", _empty)
    w_si_won = _wps.get("si_won", _empty)
    w_sqlserver_pursuit = _wps.get("sqlserver_pursuit", _empty)
    w_sqlserver_won = _wps.get("sqlserver_won", _empty)

    today_str = datetime.now().strftime("%B %d, %Y")

    # --- Build Script Tab narrative ---
    # Total pipeline risk
    total_risk_acv = sum(float(r.get("AT_RISK_ACV", 0) or 0) for r in risk_analysis.values())

    # ML WoW delta as plain text for script
    ml_delta_val = forecasts.get("ml_delta")
    if ml_delta_val is None:
        ml_wow_text = ""
    elif ml_delta_val == 0:
        ml_wow_text = " (flat WoW)"
    else:
        if ml_delta_val > 0:
            ml_wow_text = f" (+{fmt_currency(ml_delta_val)} WoW)"
        else:
            ml_wow_text = f" (-{fmt_currency(abs(ml_delta_val))} WoW)"

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
    # Prior quarter subtitles
    pq_label = CONFIG.get("prior_fy_label", "Prior") + " Q4"
    def _vel_prior_sub(cur, prior_val):
        if prior_val is None:
            return ""
        return f'<span style="display: block; font-size: 0.45em; color: #888; margin-top: 4px;">{pq_label}: {prior_val:.0f}</span>'
    v_tw_prior = _vel_prior_sub(v_tw, uc_vel_prior.get("time_to_tw"))
    v_imp_prior = _vel_prior_sub(v_imp, uc_vel_prior.get("won_to_imp_start"))
    v_dep_prior = _vel_prior_sub(v_dep, uc_vel_prior.get("won_to_deployed"))

    # Bronze created
    br_created = bronze_created.get("created", 0)
    br_create_target = bronze_created.get("target")
    br_create_target_str = str(br_create_target) if br_create_target is not None else "N/A"
    br_create_pct = f"{br_created / br_create_target * 100:.0f}%" if br_create_target else "N/A"

    # Play summary shortcuts for script
    _ps = play_summary
    _empty_ps = {"acv": 0, "count": 0}
    br_dep = _ps.get("bronze_deployed", _empty_ps)
    br_open = _ps.get("bronze_open", _empty_ps)
    si_dep = _ps.get("si_deployed", _empty_ps)
    si_open = _ps.get("si_open", _empty_ps)
    sql_dep = _ps.get("sqlserver_deployed", _empty_ps)
    sql_open = _ps.get("sqlserver_open", _empty_ps)

    # Build high-risk use case table HTML
    if high_risk_ucs:
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
            # Strip common LLM preambles
            for prefix in ["Here is a ", "Here's a ", "Here is the ", "Here's the "]:
                if raw_summary.startswith(prefix):
                    # Find the end of the preamble line (first colon + newline)
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
</tr>
"""
        high_risk_table_html = f"""
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
    else:
        high_risk_table_html = ""

    script_html = f"""
<h2>QC Script</h2>
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

<h3 style="color: #333; border-bottom: 2px solid #007bff; padding-bottom: 8px;">Sales Plays</h3>

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

<h4 style="color: #555;">Snowflake Intelligence (AI: Snowflake Intelligence &amp; Agents)</h4>
<p>SI has deployed <strong>{fmt_currency(si_dep["acv"])}</strong> ({si_dep["count"]} UCs) QTD
with <strong>{fmt_currency(si_open["acv"])}</strong> ({si_open["count"]} UCs) in open pipeline.
Deployed target: <strong>{si_target}</strong>. Gap to target: <strong>{si_gap}</strong>.
Regional coverage: <strong>{play_detail["si"]["regions"]}</strong> of 8 AMSExpansion regions contributing.</p>
<p>Theater SI Usage (Last 30 Days): {si_theater["accounts"]:,} Accounts | {si_theater["users"]:,} Users | {si_theater["credits"]:,} Credits | {fmt_currency(si_theater["revenue"])} Revenue.</p>
<div class="risk-box">
<strong>Risk Summary ({si_risk["at_risk"]} of {si_risk["total"]} use cases, {fmt_currency(si_risk["acv_at_risk"])} ACV at risk):</strong><br>
{si_risk["narrative_html"]}
</div>

<h4 style="color: #555;">SQL Server Migration (Modernize Your Data Estate)</h4>
<p>SQL Server has deployed <strong>{fmt_currency(sql_dep["acv"])}</strong> ({sql_dep["count"]} UCs) QTD
with <strong>{fmt_currency(sql_open["acv"])}</strong> ({sql_open["count"]} UCs) in open pipeline.
Deployed target: <strong>{sqlserver_target}</strong>. Gap to target: <strong>{sqlserver_gap}</strong>.
Regional coverage: <strong>{play_detail["sqlserver"]["regions"]}</strong> of 8 AMSExpansion regions contributing.</p>
<div class="risk-box">
<strong>Risk Summary ({sql_risk["at_risk"]} of {sql_risk["total"]} use cases, {fmt_currency(sql_risk["acv_at_risk"])} ACV at risk):</strong><br>
{sql_risk["narrative_html"]}
</div>

</div>

<div style="margin-top: 16px; text-align: right;">
  <button onclick="copyScript()" style="padding: 8px 16px; background: #007bff; color: white; border: none; border-radius: 4px; cursor: pointer; font-size: 0.9em;">Copy Script to Clipboard</button>
</div>
"""

    html_out = f"""<!DOCTYPE html>
<html>
<head>
  <title>AMSExpansion PEAK - {quarter}</title>
  <style>
{CSS}
  </style>
</head>
<body>

<h1>AMSExpansion PEAK Report</h1>
<h2>{quarter} ({qstart} - {qend}) | {days_remaining} days remaining</h2>

<div class="tab-nav">
  <button class="tab-btn active" onclick="switchTab('tab-wins', this)">Use Case Wins</button>
  <button class="tab-btn" onclick="switchTab('tab-qc', this)">Use Case Go-Lives</button>
  <button class="tab-btn" onclick="switchTab('tab-forecast', this)">Forecast Analysis</button>
  <button class="tab-btn" onclick="switchTab('tab-script', this)">Script</button>
</div>

<div class="tab-content active" id="tab-wins">

<h2>Use Case Wins Summary</h2>
<div style="display: flex; gap: 20px; align-items: flex-start; flex-wrap: wrap;">
  <table class="forecast-table" style="width: 280px;">
    <tr><th>Forecast Call</th><th>Amount</th></tr>
    <tr><td><strong>Commit</strong></td><td>{fmt_currency(wins_forecasts["commit"])}{w_commit_delta}</td></tr>
    <tr><td><strong>Most Likely</strong></td><td>{fmt_currency(wins_most_likely)}{w_ml_delta}</td></tr>
    <tr><td><strong>Stretch</strong></td><td>{fmt_currency(wins_forecasts["stretch"])}{w_stretch_delta}</td></tr>
  </table>
  <table class="forecast-table" style="width: 280px;">
    <tr><th colspan="2" style="background: #17a2b8;">Won Last 7 Days</th></tr>
    <tr><td><strong>Total ACV</strong></td><td>{fmt_currency(wins_won_last7["acv"])}</td></tr>
    <tr><td><strong>Use Cases</strong></td><td>{wins_won_last7["count"]}</td></tr>
    <tr><td><strong>% of Most Likely</strong></td><td>{fmt_pct(wins_last7_pct_of_ml)}</td></tr>
  </table>
  <table class="forecast-table" style="width: 280px;">
    <tr><th colspan="2" style="background: #28a745;">Won QTD</th></tr>
    <tr><td><strong>Total ACV</strong></td><td>{fmt_currency(wins_won_qtd["acv"])}</td></tr>
    <tr><td><strong>Use Cases</strong></td><td>{wins_won_qtd["count"]}</td></tr>
    <tr><td><strong>Target</strong></td><td>{fmt_currency(wins_target)}</td></tr>
    <tr><td><strong>% of Target</strong></td><td>{fmt_pct(wins_won_pct_of_target)}</td></tr>
    <tr><td><strong>% of Most Likely</strong></td><td>{fmt_pct(wins_won_pct_of_ml)}</td></tr>
  </table>
</div>

<div class="analysis" style="margin-top: 16px;">
  <strong>Pipeline Breakdown:</strong>
  Won {fmt_currency(wins_won_qtd["acv"])} ({wins_won_qtd["count"]} UCs) &nbsp;|&nbsp;
  In Pursuit {fmt_currency(wins_in_pursuit["acv"])} ({wins_in_pursuit["count"]} UCs) &nbsp;|&nbsp;
  <strong>Total: {fmt_currency(wins_total_pipeline)}</strong>
</div>

<div style="display: flex; height: 28px; border-radius: 6px; overflow: hidden; background: #eee; margin: 12px 0 4px 0;">
  <div style="width: {wins_won_pct:.1f}%; background: #28a745; display: flex; align-items: center; justify-content: center; color: white; font-size: 0.8em; font-weight: 600; overflow: hidden; white-space: nowrap;">{"Won" if wins_won_pct >= 12 else ""}</div>
  <div style="width: {wins_pursuit_pct:.1f}%; background: #ffc107; display: flex; align-items: center; justify-content: center; color: #333; font-size: 0.8em; font-weight: 600; overflow: hidden; white-space: nowrap;">{"In Pursuit" if wins_pursuit_pct >= 12 else ""}</div>
</div>
<div style="display: flex; flex-wrap: wrap; gap: 16px; margin-top: 8px; font-size: 0.85em;">
  <span><span style="display: inline-block; width: 12px; height: 12px; background: #28a745; border-radius: 2px; vertical-align: middle; margin-right: 4px;"></span><strong>Won:</strong> {fmt_currency(wins_won_qtd["acv"])} ({fmt_pct(wins_won_pct)})</span>
  <span><span style="display: inline-block; width: 12px; height: 12px; background: #ffc107; border-radius: 2px; vertical-align: middle; margin-right: 4px;"></span><strong>In Pursuit:</strong> {fmt_currency(wins_in_pursuit["acv"])} ({fmt_pct(wins_pursuit_pct)})</span>
</div>

<h2>Top 5 In-Pursuit Use Cases by ACV</h2>
<table class="use-case-table">
  <tr>
    <th style="width:25%">Account / Use Case</th>
    <th style="width:45%">Details</th>
    <th style="width:30%">Summary</th>
  </tr>
  {wins_top5_rows}
</table>

<h2>Sales Play Performance (Wins)</h2>
<table class="sales-play-table">
  <tr>
    <th>Sales Play</th>
    <th>In Pursuit ACV</th>
    <th>In Pursuit Count</th>
    <th>Won ACV</th>
    <th>Won Count</th>
  </tr>
  <tr>
    <td><strong>Bronze Ingest</strong></td>
    <td class="number open-acv">{fmt_currency(w_bronze_pursuit["acv"])}</td>
    <td class="number">{w_bronze_pursuit["count"]}</td>
    <td class="number deployed-acv">{fmt_currency(w_bronze_won["acv"])}</td>
    <td class="number">{w_bronze_won["count"]}</td>
  </tr>
  <tr>
    <td><strong>Snowflake Intelligence</strong></td>
    <td class="number open-acv">{fmt_currency(w_si_pursuit["acv"])}</td>
    <td class="number">{w_si_pursuit["count"]}</td>
    <td class="number deployed-acv">{fmt_currency(w_si_won["acv"])}</td>
    <td class="number">{w_si_won["count"]}</td>
  </tr>
  <tr>
    <td><strong>SQL Server Migration</strong></td>
    <td class="number open-acv">{fmt_currency(w_sqlserver_pursuit["acv"])}</td>
    <td class="number">{w_sqlserver_pursuit["count"]}</td>
    <td class="number deployed-acv">{fmt_currency(w_sqlserver_won["acv"])}</td>
    <td class="number">{w_sqlserver_won["count"]}</td>
  </tr>
</table>

<div class="play-section">
<h3>Bronze Ingest - In-Pursuit Use Cases &ge;$500K</h3>
<table class="use-case-table">
  <tr>
    <th style="width:25%">Account / Use Case</th>
    <th style="width:45%">Details</th>
    <th style="width:30%">Summary</th>
  </tr>
  {wins_bronze_rows}
</table>
</div>

<div class="play-section">
<h3>Snowflake Intelligence - In-Pursuit Use Cases &ge;$500K</h3>
<table class="use-case-table">
  <tr>
    <th style="width:25%">Account / Use Case</th>
    <th style="width:45%">Details</th>
    <th style="width:30%">Summary</th>
  </tr>
  {wins_si_rows}
</table>
</div>

<div class="play-section">
<h3>SQL Server Migration - In-Pursuit Use Cases &ge;$500K</h3>
<table class="use-case-table">
  <tr>
    <th style="width:25%">Account / Use Case</th>
    <th style="width:45%">Details</th>
    <th style="width:30%">Summary</th>
  </tr>
  {wins_sql_rows}
</table>
</div>

</div><!-- end tab-wins -->

<div class="tab-content" id="tab-qc">

<h2>Revenue &amp; Forecast</h2>
<div style="display: flex; gap: 20px; align-items: flex-start; flex-wrap: wrap;">
  <table class="forecast-table" style="width: 280px;">
    <tr><th colspan="2" style="background: #6f42c1;">QTD Revenue</th></tr>
    <tr><td><strong>Revenue</strong></td><td>{fmt_currency(revenue["revenue"])}</td></tr>
    <tr><td><strong>Target</strong></td><td>{revenue["target"]}</td></tr>
    <tr><td><strong>% of Target</strong></td><td>{revenue["pct_target"]}</td></tr>
    <tr><td><strong>Q1 Fcst</strong></td><td>{fmt_currency(revenue["q1_forecast"])}</td></tr>
    <tr><td><strong>{CONFIG["fiscal_year_label"]} Fcst</strong></td><td>{fmt_currency(revenue["fy_forecast"])}</td></tr>
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
  <div class="timeline-item"><span class="timeline-label">Days to TW</span><span class="timeline-days">{CONFIG["days_to_tw"]}</span></div>
  <div class="timeline-arrow">&rarr;</div>
  <div class="timeline-item"><span class="timeline-label">Days to Imp Start</span><span class="timeline-days">{CONFIG["days_to_imp"]}</span></div>
  <div class="timeline-arrow">&rarr;</div>
  <div class="timeline-item"><span class="timeline-label">Days to Deployed</span><span class="timeline-days">{CONFIG["days_to_deploy"]}</span></div>
</div>

<div class="risk-box">
  <strong>Pipeline Risk Analysis:</strong><br>
  {risk_line("Stage 1-3")}
  {risk_line("Stage 4")}
  {risk_line("Stage 5")}
  {risk_line("Stage 6")}
  &bull; <strong>Total Good Pipeline: {fmt_currency(total_good)}</strong> ({fmt_pct(good_coverage)} coverage vs Most Likely)
</div>

<h3>Pacing vs {CONFIG["prior_fy_label"]}</h3>
<table class="forecast-table" style="width: 750px;">
  <tr>
    <th style="white-space: nowrap;">Period</th>
    <th>{CONFIG["prior_fy_label"]} Average</th>
    <th>{CONFIG["prior_fy_label"]} % of Final</th>
    <th>{CONFIG["fiscal_year_label"]} {quarter}</th>
    <th>% of Most Likely</th>
    <th>% of Target</th>
  </tr>
  <tr>
    <td style="white-space: nowrap;"><strong>Day {day_number}</strong></td>
    <td>{fmt_currency(day_avg)}</td>
    <td>{fmt_pct(day_pct_val)}</td>
    <td>{fmt_currency(deployed["acv"])}</td>
    <td>{fmt_pct(deployed_pct)}</td>
    <td>{fmt_pct(deployed_pct_of_target)}</td>
  </tr>
  <tr>
    <td style="white-space: nowrap;"><strong>Week {week_number}</strong></td>
    <td>{fmt_currency(week_avg)}</td>
    <td>{fmt_pct(week_pct_val)}</td>
    <td>{fmt_currency(deployed["acv"])}</td>
    <td>{fmt_pct(deployed_pct)}</td>
    <td>{fmt_pct(deployed_pct_of_target)}</td>
  </tr>
</table>
<p style="font-size: 0.85em; color: #666; margin-top: 5px;"><em>{CONFIG["prior_fy_label"]} Average Final: ${CONFIG["prior_fy_avg_final"]}M across 4 quarters | {CONFIG["fiscal_year_label"]} {quarter} Most Likely: {fmt_currency(most_likely)}</em></p>

<h2>Top 5 Use Cases Going Live This Quarter</h2>
<table class="use-case-table">
  <tr>
    <th style="width:25%">Account / Use Case</th>
    <th style="width:45%">Details</th>
    <th style="width:30%">Summary</th>
  </tr>
  {top5_rows}
</table>

<h2>Sales Play Performance</h2>
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

</div><!-- end tab-qc -->

<div class="tab-content" id="tab-forecast">
{_build_forecast_tab(forecast_analysis, forecasts, deployed, day_number, week_number)}
</div><!-- end tab-forecast -->

<div class="tab-content" id="tab-script">
{script_html}
</div><!-- end tab-script -->

<div class="footer">
  Generated: {today_str} | Data as of: {today_str}
</div>

<script>
function switchTab(tabId, btn) {{
  document.querySelectorAll('.tab-content').forEach(t => t.classList.remove('active'));
  document.querySelectorAll('.tab-btn').forEach(b => b.classList.remove('active'));
  document.getElementById(tabId).classList.add('active');
  btn.classList.add('active');
}}
function copyScript() {{
  var el = document.querySelector('#tab-script .qc-script-body');
  if (!el) el = document.querySelector('#tab-script div[style]');
  var range = document.createRange();
  range.selectNodeContents(el);
  var sel = window.getSelection();
  sel.removeAllRanges();
  sel.addRange(range);
  document.execCommand('copy');
  sel.removeAllRanges();
  var btn = event.target;
  btn.textContent = 'Copied!';
  setTimeout(function() {{ btn.textContent = 'Copy Script to Clipboard'; }}, 2000);
}}
</script>

</body>
</html>"""

    return html_out, quarter


# =============================================================================
# MAIN ORCHESTRATION
# =============================================================================

def main():
    print("=" * 60)
    print("PEAK Report Generator")
    print("=" * 60)

    # Phase 1: Data Collection
    print("\n--- PHASE 1: DATA COLLECTION ---")
    conn = get_connection()

    try:
        fiscal = q_fiscal_calendar(conn)
        uc_velocity = q_use_case_velocity(conn)
        update_risk_thresholds_from_velocity(uc_velocity)
        revenue = q_qtd_revenue(conn)
        forecasts = q_forecast_calls(conn)
        deployed = q_deployed_qtd(conn)
        last7 = q_last7_deployed(conn)
        pipeline = q_open_pipeline(conn)
        risk_analysis = q_pipeline_risk(conn)
        top5 = q_top5_use_cases(conn)
        play_summary = q_sales_play_summary(conn)
        play_detail = q_play_detail_metrics(conn)
        bronze_tb_total = q_bronze_tb_total(conn)
        play_use_cases = q_play_use_cases(conn)
        pacing = q_prior_fy_pacing(conn, fiscal["DAY_NUMBER"], fiscal["WEEK_NUMBER"])
        play_risk = q_play_risk_detail(conn)
        high_risk_ucs = q_high_risk_use_cases(conn)

        # --- Wins tab queries ---
        wins_forecasts = q_wins_forecast_calls(conn)
        wins_won_qtd = q_won_qtd(conn)
        wins_won_last7 = q_won_last7(conn)
        wins_in_pursuit = q_wins_in_pursuit(conn)
        wins_top5_pursuit = q_wins_top5_pursuit(conn)
        wins_play_summary = q_wins_play_summary(conn)
        wins_play_use_cases = q_wins_play_use_cases(conn)
        play_targets = q_play_targets(conn)
        partner_sd = q_partner_sd_attach(conn)
        bronze_created = q_bronze_created_qtd(conn)

        # Set day_number in CONFIG for velocity query
        CONFIG["day_number"] = int(fiscal["DAY_NUMBER"])
        velocity = q_deployment_velocity(conn)
        pipeline_detail = q_risk_adjusted_pipeline_detail(conn)

        # Collect all account IDs for consumption/SI/bronze lookups
        all_account_ids = set()
        for uc in top5:
            all_account_ids.add(safe_str(uc.get("ACCOUNT_ID")))
        for play in play_use_cases.values():
            for uc in play:
                all_account_ids.add(safe_str(uc.get("ACCOUNT_ID")))
        # Include wins tab account IDs
        for uc in wins_top5_pursuit:
            all_account_ids.add(safe_str(uc.get("ACCOUNT_ID")))
        for play in wins_play_use_cases.values():
            for uc in play:
                all_account_ids.add(safe_str(uc.get("ACCOUNT_ID")))
        all_account_ids.discard("")

        si_account_ids = set()
        for uc in play_use_cases.get("si", []):
            si_account_ids.add(safe_str(uc.get("ACCOUNT_ID")))
        si_account_ids.discard("")

        consumption = q_consumption(conn, all_account_ids)
        si_theater = q_si_theater_totals(conn)
        si_usage = q_si_usage(conn, si_account_ids)
        bronze_tb_acct = q_bronze_tb_by_account(conn)

        # Forecast analysis queries
        pipeline_phases = q_current_pipeline_phases(conn)
        hist_conv_rates = q_historical_conversion_rates(conn, fiscal["DAY_NUMBER"])
        forecast_analysis = compute_forecast_analysis(
            pipeline_phases, hist_conv_rates, deployed, risk_analysis,
            forecasts["most_likely"], pacing
        )
        print(f"  Running: Forecast Analysis...")
        print(f"    → Pipeline phases: {len(pipeline_phases)} groups")
        print(f"    → Historical quarters: {len(hist_conv_rates)}")
        print(f"    → Blended Commit: {fmt_currency(forecast_analysis['recommended']['commit'])}")
        print(f"    → Blended Most Likely: {fmt_currency(forecast_analysis['recommended']['most_likely'])}")
        print(f"    → Blended Stretch: {fmt_currency(forecast_analysis['recommended']['stretch'])}")

        # Backtest models against historical quarters
        backtest_results = q_backtest_models(conn, hist_conv_rates)
        if backtest_results:
            print(f"  Running: Backtest...")
            for bt in backtest_results:
                err = (bt["blended"]["most_likely"] - bt["final_deployed"]) / bt["final_deployed"] * 100 if bt["final_deployed"] else 0
                print(f"    → {bt['quarter']}: Blended ML {fmt_currency(bt['blended']['most_likely'])} vs Actual {fmt_currency(bt['final_deployed'])} ({err:+.1f}%)")
        forecast_analysis["backtest"] = backtest_results

        # Method 4: Weighted ensemble (uses backtest errors to weight M1/M2/M3)
        compute_weighted_ensemble(forecast_analysis, backtest_results, day_number=int(fiscal["DAY_NUMBER"]))
        m4 = forecast_analysis.get("method4", {})
        if m4:
            w = m4.get("weights", {}).get("most_likely", {})
            print(f"  Running: Weighted Ensemble (M4)...")
            print(f"    → Weights (ML): M1={w.get('m1',0):.0%}, M2={w.get('m2',0):.0%}, M3={w.get('m3',0):.0%}")
            print(f"    → M4 Commit: {fmt_currency(m4['commit'])}")
            print(f"    → M4 Most Likely: {fmt_currency(m4['most_likely'])}")
            print(f"    → M4 Stretch: {fmt_currency(m4['stretch'])}")

        # Pipeline velocity
        if velocity:
            v = velocity["current"]
            print(f"  Running: Pipeline Velocity...")
            print(f"    → Last 7 days: {fmt_currency(v['v7'])}")
            print(f"    → Last 14 days: {fmt_currency(v['v14'])}")
            print(f"    → Last 30 days: {fmt_currency(v['v30'])}")

        # Store velocity in forecast_analysis for HTML rendering
        forecast_analysis["velocity"] = velocity
        forecast_analysis["pipeline_detail"] = pipeline_detail

        # Log forecast tracking (after M4 so it captures weighted ensemble)
        log_forecast_tracking(fiscal, forecast_analysis, deployed)

        # Phase 2: Validation
        print("\n--- PHASE 2: DATA VALIDATION ---")
        assert revenue["revenue"] > 0, f"QTD Revenue is {revenue['revenue']} - expected > 0"
        print(f"  ✓ QTD Revenue: {fmt_currency(revenue['revenue'])}")
        print(f"  ✓ {CONFIG['fiscal_year_label']} Forecast: {fmt_currency(revenue['fy_forecast'])}")
        print(f"  ✓ Open Pipeline: {fmt_currency(pipeline['acv'])} ({pipeline['count']} UCs)")
        print(f"  ✓ Deployed QTD: {fmt_currency(deployed['acv'])} ({deployed['count']} UCs)")

        for play_name, ucs in play_use_cases.items():
            for uc in ucs:
                acv = float(uc.get("USE_CASE_EACV", 0) or 0)
                assert acv >= CONFIG["play_threshold"], f"{play_name} use case has ACV {acv} < {CONFIG['play_threshold']}"
            print(f"  ✓ {play_name}: {len(ucs)} use cases (all ≥$500K)")

        # Phase 3: HTML Generation
        print("\n--- PHASE 3: HTML GENERATION ---")
        data = {
            "fiscal": fiscal,
            "revenue": revenue,
            "forecasts": forecasts,
            "deployed": deployed,
            "last7": last7,
            "pipeline": pipeline,
            "risk_analysis": risk_analysis,
            "top5": top5,
            "play_summary": play_summary,
            "play_detail": play_detail,
            "play_use_cases": play_use_cases,
            "play_risk": play_risk,
            "high_risk_ucs": high_risk_ucs,
            "consumption": consumption,
            "bronze_tb_total": bronze_tb_total,
            "bronze_tb_acct": bronze_tb_acct,
            "si_usage": si_usage,
            "si_theater": si_theater,
            "pacing": pacing,
            "forecast_analysis": forecast_analysis,
            # Wins tab data
            "wins_forecasts": wins_forecasts,
            "wins_won_qtd": wins_won_qtd,
            "wins_won_last7": wins_won_last7,
            "wins_in_pursuit": wins_in_pursuit,
            "wins_top5_pursuit": wins_top5_pursuit,
            "wins_play_summary": wins_play_summary,
            "wins_play_use_cases": wins_play_use_cases,
            "play_targets": play_targets,
            "partner_sd": partner_sd,
            "uc_velocity": uc_velocity,
            "bronze_created": bronze_created,
        }

        html_content, quarter = build_html(data)

        # Write output
        output_path = os.path.join(CONFIG["output_dir"], f"PEAK_AMSExpansion_{quarter}.html")
        with open(output_path, "w", encoding="utf-8") as f:
            f.write(html_content)

        print(f"  ✓ HTML generated: {output_path}")

        # Phase 4: Post-Generation Verification
        print("\n--- PHASE 4: VERIFICATION ---")

        # Check QTD Revenue is not TBD
        assert "Revenue</strong></td><td>$" in html_content, "Revenue shows TBD instead of dollar amount"
        print("  ✓ QTD Revenue shows dollar amount")

        # Check FY Fcst
        fy_label = CONFIG["fiscal_year_label"]
        assert f"{fy_label} Fcst</strong></td><td>$" in html_content, f"{fy_label} Fcst is missing"
        print(f"  ✓ {fy_label} Fcst shows dollar amount")

        # Check Sales Play headers use ≥$500K
        assert "&ge;$500K" in html_content, "Sales Play headers missing ≥$500K"
        assert ">= $500K" not in html_content, "Sales Play headers use wrong >= format"
        print("  ✓ Sales Play headers use &ge;$500K")

        # Check risk summaries present
        assert "Risk Summary (" in html_content, "Risk summaries missing"
        print("  ✓ Risk summaries present")

        # Count Salesforce links
        link_count = html_content.count("snowforce.lightning.force.com/")
        expected_links = len(top5) + sum(len(ucs) for ucs in play_use_cases.values())
        print(f"  ✓ Salesforce links: {link_count} (expected {expected_links})")

        # Count consumption lines
        cons_count = html_content.count("90D: $") + html_content.count("90D: N/A")
        print(f"  ✓ Consumption data lines: {cons_count}")

        # Count SE Comments
        se_count = html_content.count("<strong>SE Comments:</strong>")
        print(f"  ✓ SE Comments: {se_count}")

        print("\n" + "=" * 60)
        print(f"Report generated successfully: {output_path}")
        print("=" * 60)

    finally:
        conn.close()


if __name__ == "__main__":
    main()
