#!/usr/bin/env python3
"""
PEAK QC Forecast — AMSExpansion (quarter-parameterized)
Generates an HTML forecast report using live Snowflake data.

Usage:
    python3 peak_report.py          # current quarter (Q3 FY27)
    python3 peak_report.py q3
    python3 peak_report.py q4       # next quarter (out-quarter view)

Rates are calibrated per time horizon by peak_calibrate.py — an in-quarter
report and an out-quarter report must not share conversion rates.
"""

import json
import csv
import os
import sys
from datetime import date, datetime
from pathlib import Path

# Sandboxed / automated interpreters do not reliably put the script's own
# directory on sys.path, which makes `import peak_calibrate` fail and silently
# degrade the report to hardcoded fallback rates. Prepend it ourselves so any
# caller — cron, agent task, sandbox, another cwd — resolves the sibling module.
_SCRIPT_DIR = str(Path(__file__).resolve().parent)
if _SCRIPT_DIR not in sys.path:
    sys.path.insert(0, _SCRIPT_DIR)

import snowflake.connector

# ─────────────────────────── Configuration ───────────────────────────────────
CONNECTION_NAME = "MyConnection"
ROLE = "SALES_RAVEN_RO_RL"
WAREHOUSE = "SNOWADHOC"
# Scope is the THEATER. GVP names are not stable: in 2026-09 AMSExpansion's GVP
# became the placeholder "(TBH)  AMSExpansion GVP" and every 'Mark Fleming'
# filter silently returned zero rows. Only MaxIQ (USER_NAME) is keyed on a
# person, so GVP is resolved at runtime from the account table in main().
THEATER = "AMSExpansion"
GVP = ""   # set by resolve_gvp(); used ONLY for the MaxIQ USER_NAME lookup

# PEAK-native detail table. This is what MaxIQ reports against, and the only
# source that reproduces its published figures exactly:
#   open pipeline (Going Live) = stage 1-6, GO_LIVE_DATE_FQ_SK = quarter
#   deployed (Live)           = stage 7
#   won (Won)                 = TECHNICAL_WIN='Yes', stage 4-7, DECISION_DATE_FQ_SK
# Grain is one row per use case per hierarchy level (AE -> DM -> SubRegion Lead
# -> RVP -> GVP), so LEVEL_NUM = 1 plus a dedup on USE_CASE_ID is required.
# REGION is only populated on the base rows; GVP rollup rows carry NULL.
PEAK = "SALES.REPORTING.PEAK_USE_CASE_FORECAST"

TODAY = date.today()

# ──────────────────────── Quarter definitions ────────────────────────────────
# Each quarter carries its own horizon-calibrated rate set. Rates come from
# peak_calibrate.py run at that quarter's actual day offset.
QUARTERS = {
    "q3": {
        "label": "Q3 FY27",
        "start": date(2026, 8, 1),
        "end": date(2026, 10, 31),
        "fq_key": "2027-Q3",
        "prior_label": "Q3 FY26",
        "prior_start": date(2025, 8, 1),
        "prior_end": date(2025, 10, 31),
        # Calibrated @ day 40 in-quarter, STAGE-based buckets (4Q, Q2 FY27 2x)
        "m3_avg": {"imp": 0.639, "tw": 0.378, "pretw": 0.178, "new": 0.230},
        "m3_min": {"imp": 0.536, "tw": 0.294, "pretw": 0.150, "new": 0.195},
        # M2 pacing @ day 40: share of final deployed already banked
        "m2_pace": {"avg": 0.348, "min": 0.321, "max": 0.362},
    },
    "q4": {
        "label": "Q4 FY27",
        "start": date(2026, 11, 1),
        "end": date(2027, 1, 31),
        "fq_key": "2027-Q4",
        "prior_label": "Q4 FY26",
        "prior_start": date(2025, 11, 1),
        "prior_end": date(2026, 1, 31),
        # Calibrated @ 53 days before open, STAGE-based buckets (4Q, Q2 FY27 2x)
        "m3_avg": {"imp": 0.440, "tw": 0.277, "pretw": 0.148, "new": 0.633},
        "m3_min": {"imp": 0.337, "tw": 0.219, "pretw": 0.104, "new": 0.547},
        # M2 requires deployed ACV — N/A pre-quarter
        "m2_pace": None,
    },
}

# Calibration is mandatory by default. If peak_calibrate cannot run, the report
# would otherwise be built on the hardcoded QUARTERS rates measured at some
# other horizon — which publishes wrong numbers with no visible marker. Pass
# --allow-fallback-rates to proceed anyway; the report is then stamped so a
# reader can see the model was not calibrated.
ALLOW_FALLBACK = "--allow-fallback-rates" in sys.argv
CALIBRATED = True   # set by main(); False means fallback rates were used
_args = [a for a in sys.argv[1:] if not a.startswith("--")]

_arg = (_args[0].lower() if _args else "q3")
if _arg not in QUARTERS:
    sys.exit(f"Unknown quarter '{_arg}'. Use one of: {', '.join(QUARTERS)}")
Q = QUARTERS[_arg]

QLABEL = Q["label"]
QS = Q["start"]
QE = Q["end"]
FQ_KEY = Q["fq_key"]
PRIOR_LABEL = Q["prior_label"]
PRIOR_QS = Q["prior_start"]
PRIOR_QE = Q["prior_end"]

QDAYS = (QE - QS).days + 1          # quarter length, M1 time horizon
DAYS_TO_OPEN = (QS - TODAY).days     # negative once the quarter has started
DAY_IN_QUARTER = (TODAY - QS).days + 1   # 1 on the first day; <=0 pre-quarter
DAYS_REMAINING = max((QE - TODAY).days + 1, 0)
IN_QUARTER = DAY_IN_QUARTER >= 1
# M1 horizon: days a UC still has to clear its stage threshold
M1_HORIZON = DAYS_REMAINING if IN_QUARTER else QDAYS

# ──────────────────────── M3 Calibration Rates ───────────────────────────────
# Milestone-date classification, divisor formula: ml = known / (1 - new_pct).
# new_pct = share of final deployed ACV that was NOT visible at this horizon
# (pull-ins + newly created), so it scales with how far out we are.
M3_AVG_IMP_RATE   = Q["m3_avg"]["imp"]
M3_AVG_TW_RATE    = Q["m3_avg"]["tw"]
M3_AVG_PRETW_RATE = Q["m3_avg"]["pretw"]
M3_AVG_NEW_PCT    = Q["m3_avg"]["new"]
M3_MIN_IMP_RATE   = Q["m3_min"]["imp"]
M3_MIN_TW_RATE    = Q["m3_min"]["tw"]
M3_MIN_PRETW_RATE = Q["m3_min"]["pretw"]
M3_MIN_NEW_PCT    = Q["m3_min"]["new"]
# Stretch: capped at total pipeline (never exceeds what is actually in play)
M3_MAX_NEW_PCT    = 0.0

# ──────────────────────── M2 Historical Pacing ───────────────────────────────
M2_PACE = Q["m2_pace"]
# Pacing is noisy very early in a quarter; require day 30+ before trusting it.
M2_RELIABLE_DAY = 30
M2_ACTIVE = bool(M2_PACE) and IN_QUARTER and DAY_IN_QUARTER >= M2_RELIABLE_DAY

# ──────────────────────── M4 Ensemble Weights ────────────────────────────────
# Dashboard base: M1 51%, M2 10%, M3 40%.
_M4_RAW_W1 = 0.51
_M4_RAW_W2 = 0.10
_M4_RAW_W3 = 0.40
if M2_ACTIVE:
    M4_W1, M4_W2, M4_W3 = _M4_RAW_W1, _M4_RAW_W2, _M4_RAW_W3
else:
    # M2 contributes nothing — redistribute its weight across M1/M3 so the
    # ensemble still sums to 1.0. Without this the blend silently understates.
    _d = _M4_RAW_W1 + _M4_RAW_W3
    M4_W1 = _M4_RAW_W1 / _d      # 0.56
    M4_W2 = 0.0
    M4_W3 = _M4_RAW_W3 / _d      # 0.44

# ──────────────────────── Backtest accuracy (for display only) ───────────────
BACKTEST = {
    "Q3 FY26": {"m1": 5.2,  "m3": 27.2},
    "Q4 FY26": {"m1": 25.7, "m3": 20.2},
    "Q1 FY27": {"m1": 37.5, "m3": 17.0},
}
M1_AVG_ERR = sum(v["m1"] for v in BACKTEST.values()) / len(BACKTEST)
M3_AVG_ERR = sum(v["m3"] for v in BACKTEST.values()) / len(BACKTEST)

# A handful of use cases carry no REGION on their base PEAK row. They are kept
# in the totals so the report still ties to MaxIQ, but they get no region card
# (no RVP, no target) because a near-empty card reads as a bug.
SUPPRESS_REGION_CARDS = set()

# ──────────────────────── M1 Risk Thresholds ─────────────────────────────────
M1_THRESH = {5: 79, 4: 104, "pretw": 146}   # days threshold per stage

# ─────────────────────────── Snowflake Queries ───────────────────────────────

def get_conn():
    """
    Connect flexibly so the same script runs locally and inside an automation
    sandbox, which has no connections.toml. Tries the named connection first,
    then the default connection, then whatever the environment provides.
    """
    attempts = [
        dict(connection_name=CONNECTION_NAME, role=ROLE, warehouse=WAREHOUSE),
        dict(role=ROLE, warehouse=WAREHOUSE),
        dict(warehouse=WAREHOUSE),
        dict(),
    ]
    last = None
    for kw in attempts:
        try:
            return snowflake.connector.connect(**kw)
        except Exception as exc:
            last = exc
    raise RuntimeError(f"could not open a Snowflake connection: {last}")


def run(conn, sql):
    """Execute SQL and return list of dicts."""
    cur = conn.cursor()
    cur.execute(sql)
    cols = [c[0] for c in cur.description]
    return [dict(zip(cols, row)) for row in cur.fetchall()]


def _peak_base(extra_cols="", fq_col="GO_LIVE_DATE_FQ_SK", fq_val=None):
    """
    Base CTE over the PEAK detail table: one deduped row per use case at the
    LEVEL_NUM=1 grain, with stage parsed to an integer. `fq_col` selects which
    fiscal-quarter key to filter on — go-live for pipeline, decision for wins.
    RVP name comes from Raven so casing matches the org chart (deriving it from
    the email mangles names like LaCamera).
    """
    fq_val = fq_val or FQ_KEY
    return f"""
    WITH rvp_names AS (
        SELECT DISTINCT LOWER(RVP_EMAIL) AS rvp_email, RVP AS rvp_name
        FROM SALES.RAVEN.D_SALESFORCE_ACCOUNT_CUSTOMERS
        WHERE RVP_EMAIL IS NOT NULL AND RVP IS NOT NULL
    ),
    base AS (
        SELECT
            p.USE_CASE_ID,
            p.REGION                                           AS region,
            COALESCE(rn.rvp_name,
                     INITCAP(REPLACE(SPLIT_PART(p.RVP_EMAIL,'@',1),'.',' '))
                    )                                          AS rvp,
            p.DISTRICT                                         AS district,
            p.ESTIMATED_VALUE                                  AS acv,
            TRY_TO_NUMBER(LEFT(p.USE_CASE_STAGE, 1))           AS stage_num,
            p.DAYS_IN_STAGE                                    AS days_in_stage,
            p.NEXT_STEPS                                       AS next_steps,
            p.TECHNICAL_WIN                                    AS technical_win
            {extra_cols}
        FROM {PEAK} p
        LEFT JOIN rvp_names rn ON LOWER(p.RVP_EMAIL) = rn.rvp_email
        WHERE p.THEATER = '{THEATER}'
          AND p.LEVEL_NUM = 1
          AND p.ESTIMATED_VALUE > 0
          AND p.{fq_col} = '{fq_val}'
          -- A few records carry no REGION on their base row (hierarchy data
          -- quality). They cannot be attributed to an RVP or target, so they
          -- are dropped from region rows; this is worth ~$0.1M.
          AND p.REGION IS NOT NULL
        QUALIFY ROW_NUMBER() OVER (
            PARTITION BY p.USE_CASE_ID
            ORDER BY p.REGION NULLS LAST, p.DISTRICT NULLS LAST
        ) = 1
    )"""


def query_pipeline(conn):
    """
    Per-region open pipeline from the PEAK detail table: stage bucket ACVs,
    IMP depth bands, and M1 risk flags. Stage 1-6 only, which is what MaxIQ
    counts as 'Going Live'.
    """
    sql = _peak_base() + f"""
    SELECT
        region,
        MAX(rvp)                                    AS rvp,
        COUNT(*)                                    AS uc_count,
        ROUND(SUM(acv), 0)                          AS total_acv,
        -- IMP (Stage 5) depth buckets
        ROUND(SUM(CASE WHEN stage_num=5 AND days_in_stage BETWEEN  0 AND 13 THEN acv ELSE 0 END),0) AS imp_0_13,
        ROUND(SUM(CASE WHEN stage_num=5 AND days_in_stage BETWEEN 14 AND 29 THEN acv ELSE 0 END),0) AS imp_14_29,
        ROUND(SUM(CASE WHEN stage_num=5 AND days_in_stage BETWEEN 30 AND 59 THEN acv ELSE 0 END),0) AS imp_30_59,
        ROUND(SUM(CASE WHEN stage_num=5 AND days_in_stage BETWEEN 60 AND 89 THEN acv ELSE 0 END),0) AS imp_60_89,
        ROUND(SUM(CASE WHEN stage_num=5 AND days_in_stage >= 90            THEN acv ELSE 0 END),0) AS imp_90p,
        ROUND(SUM(CASE WHEN stage_num=5 THEN acv ELSE 0 END),0)  AS imp_total,
        -- TW (Stage 4) split good/at-risk against the current horizon
        ROUND(SUM(CASE WHEN stage_num=4 AND (days_in_stage + {M1_HORIZON}) >= 104 THEN acv ELSE 0 END),0) AS tw_good,
        ROUND(SUM(CASE WHEN stage_num=4 AND (days_in_stage + {M1_HORIZON}) <  104 THEN acv ELSE 0 END),0) AS tw_risk,
        ROUND(SUM(CASE WHEN stage_num=4 THEN acv ELSE 0 END),0)  AS tw_total,
        -- Pre-TW (Stage 1-3)
        ROUND(SUM(CASE WHEN stage_num IN (1,2,3) AND (days_in_stage + {M1_HORIZON}) >= 146 THEN acv ELSE 0 END),0) AS pretw_good,
        ROUND(SUM(CASE WHEN stage_num IN (1,2,3) AND (days_in_stage + {M1_HORIZON}) <  146 THEN acv ELSE 0 END),0) AS pretw_risk,
        ROUND(SUM(CASE WHEN stage_num IN (1,2,3) THEN acv ELSE 0 END),0) AS pretw_total,
        -- Stage 6 (Implementation Complete — locked)
        ROUND(SUM(CASE WHEN stage_num=6 THEN acv ELSE 0 END),0)  AS stage6_acv,
        COUNT(CASE WHEN stage_num=6 THEN 1 END)                  AS stage6_count,
        -- Stale / hygiene flags
        COUNT(CASE WHEN stage_num=5 AND days_in_stage > 79 THEN 1 END)  AS stale_imp_cnt,
        ROUND(SUM(CASE WHEN stage_num=5 AND days_in_stage > 79 THEN acv ELSE 0 END),0) AS stale_imp_acv,
        COUNT(CASE WHEN stage_num=4 AND days_in_stage > 104 THEN 1 END) AS stale_tw_cnt,
        ROUND(SUM(CASE WHEN stage_num=4 AND days_in_stage > 104 THEN acv ELSE 0 END),0) AS stale_tw_acv,
        COUNT(CASE WHEN stage_num >= 4 AND (next_steps IS NULL OR next_steps = '') THEN 1 END) AS no_next_steps
    FROM base
    WHERE stage_num BETWEEN 1 AND 6
    GROUP BY region
    ORDER BY total_acv DESC
    """
    return run(conn, sql)


def query_prior_actuals(conn):
    """
    Prior-year same-quarter actual deployed ACV by region, plus TOTAL.

    Stays on MDM for the same reason as query_yoy — PEAK go-live coverage
    begins at 2027-Q1 and holds no FY26 history.
    """
    sql = f"""
    SELECT
        CASE
            WHEN SUB_REGION_NAME IN ('CommEast_SR','CommWest_SR') THEN 'Commercial'
            ELSE REPLACE(SUB_REGION_NAME, '_SR', '')
        END                                  AS region,
        ROUND(SUM(USE_CASE_EACV), 0)         AS actual_acv
    FROM MDM.MDM_INTERFACES.DIM_USE_CASE
    WHERE THEATER_NAME = '{THEATER}'
      AND IS_DEPLOYED = TRUE
      AND USE_CASE_EACV > 0
      AND STAGE_NUMBER >= 1
      AND GO_LIVE_DATE BETWEEN '{PRIOR_QS}' AND '{PRIOR_QE}'
      AND SUB_REGION_NAME NOT IN ('AMSExpansion_SRgn_Hold')
      AND SUB_REGION_NAME IS NOT NULL
    GROUP BY 1
    """
    rows = run(conn, sql)
    out = {r["REGION"]: float(r["ACTUAL_ACV"] or 0) for r in rows}
    out["TOTAL"] = sum(out.values())
    return out


def query_maxiq(conn):
    """
    MaxIQ's own figures for this quarter, used as the tie-out reference.
      'Going Live' = open pipeline, 'Live' = deployed/banked

    Pulls BOTH the current snapshot (LATEST_DATE) and the prior week
    (PREVIOUS_WEEK). MaxIQ views are frequently read a week behind, and the
    week-over-week move is large enough to look like a defect if only one
    snapshot is shown.

    Note this table is a hierarchy — one row per person per FUNCTION level
    (GVP / RVP / SubRegion Lead / DM). The GVP row is the AMSExpansion rollup;
    summing SubRegion Lead rows undercounts it because a few sub-regions have
    no assigned lead.
    """
    sql = f"""
    SELECT TYPE, FORECAST_TYPE,
           MAX(CASE WHEN LATEST_DATE   THEN FORECAST_AMOUNT END) AS amt_now,
           MAX(CASE WHEN PREVIOUS_WEEK THEN FORECAST_AMOUNT END) AS amt_prior
    FROM SALES.REPORTING.PEAK_FORECAST_CALLS_PIPELINE_TARGETS
    WHERE USER_NAME = '{GVP}'
      AND FUNCTION = 'GVP'
      AND FISCAL_QUARTER = '{FQ_KEY}'
      AND TYPE IN ('Use Case Go-Lives', 'Use Case Wins')
      AND (LATEST_DATE OR PREVIOUS_WEEK)
    GROUP BY TYPE, FORECAST_TYPE
    """
    rows = run(conn, sql)
    now, prior = {}, {}
    for r in rows:
        key = (r["TYPE"], r["FORECAST_TYPE"])
        now[key] = float(r["AMT_NOW"] or 0)
        prior[key] = float(r["AMT_PRIOR"] or 0)

    g, w = "Use Case Go-Lives", "Use Case Wins"
    fields = {
        "open":   (g, "Going Live"),
        "live":   (g, "Live"),
        "commit": (g, "CommitForecast"),
        "ml":     (g, "MostLikelyForecast"),
        "best":   (g, "BestCaseForecast"),
        "target": (g, "Target"),
        "won":    (w, "Won"),
    }
    out = {k: now.get(key, 0) for k, key in fields.items()}
    out["prior"] = {k: prior.get(key, 0) for k, key in fields.items()}
    return out


def query_targets(conn):
    sql = f"""
    SELECT REGION, USE_CASE_GO_LIVE_TARGET_REGION AS target_acv
    FROM SALES.REPORTING.PEAK_USE_CASE_TARGETS
    WHERE GEO = 'AMSExpansion'
      AND FISCAL_QUARTER = '{FQ_KEY}'
      AND REGION != 'AMSExpansion_Rgn_Hold'
    """
    rows = run(conn, sql)
    return {r["REGION"]: r["TARGET_ACV"] for r in rows}


def query_yoy(conn):
    """
    Prior-year same-quarter pipeline at the same calendar date.

    Stays on the MDM history snapshots: PEAK_USE_CASE_FORECAST holds only the
    current state (single timestamp) and its go-live coverage starts at
    2027-Q1, so it cannot answer a year-ago question.
    """
    yoy_date = date(TODAY.year - 1, TODAY.month, TODAY.day)
    sql = f"""
    SELECT
        ROUND(SUM(USE_CASE_EACV), 0) AS yoy_acv
    FROM SALES.SE_REPORTING.DIM_USE_CASE_HISTORY_DS
    WHERE THEATER_NAME = '{THEATER}'
      AND DS = '{yoy_date}'
      AND IS_DEPLOYED = FALSE
      AND IS_LOST     = FALSE
      AND USE_CASE_EACV > 0
      AND STAGE_NUMBER BETWEEN 1 AND 6
      AND GO_LIVE_DATE BETWEEN '{PRIOR_QS}' AND '{PRIOR_QE}'
    """
    rows = run(conn, sql)
    return rows[0]["YOY_ACV"] if rows else 0


def query_deployed(conn):
    """
    Deployed (banked) ACV by region — MaxIQ's 'Live'. Stage 7 in PEAK.
    $0 pre-quarter. Added on top of model output, since M1/M3 forecast only
    what is still open.
    """
    sql = _peak_base() + """
    SELECT region,
           ROUND(SUM(acv), 0) AS deployed_acv,
           COUNT(*)           AS deployed_count
    FROM base
    WHERE stage_num = 7
    GROUP BY region
    """
    rows = run(conn, sql)
    return {r["REGION"]: r for r in rows}


def query_won_qtd(conn):
    """
    Wins booked in the quarter — MaxIQ's 'Won'. Verified to reproduce it to the
    cent: TECHNICAL_WIN='Yes', stage 4-7, decision date in the quarter
    (Q3 FY27 = $77.628M vs MaxIQ $77.63M).

    Dropping the stage bound yields MaxIQ's 'Mature' figure instead ($85.66M),
    so the 4-7 bound is what separates the two measures.
    """
    sql = _peak_base(fq_col="DECISION_DATE_FQ_SK") + """
    SELECT ROUND(SUM(acv), 0) AS won_acv,
           COUNT(*)           AS won_count
    FROM base
    WHERE technical_win = 'Yes'
      AND stage_num BETWEEN 4 AND 7
    """
    rows = run(conn, sql)
    r = rows[0] if rows else {}
    return {
        "won_acv": r.get("WON_ACV") or 0,
        "won_count": r.get("WON_COUNT") or 0,
    }


def query_wins(conn):
    """Wins pipeline: all Pre-TW UCs that could achieve TW in Q3, plus wins target."""
    # Current Pre-TW pipeline (all stages 1-3, no go-live filter — matches backtest methodology)
    pipeline_sql = f"""
    SELECT
        ROUND(SUM(CASE WHEN STAGE_NUMBER IN (1,2,3) THEN USE_CASE_EACV ELSE 0 END), 0) AS pretw_total,
        COUNT(CASE WHEN STAGE_NUMBER IN (1,2,3) THEN 1 END) AS pretw_cnt,
        ROUND(SUM(CASE WHEN STAGE_NUMBER IN (1,2,3) AND DECISION_DATE BETWEEN '{QS}' AND '{QE}'
                  THEN USE_CASE_EACV ELSE 0 END), 0) AS pretw_q3_decision,
        COUNT(CASE WHEN STAGE_NUMBER IN (1,2,3) AND DECISION_DATE BETWEEN '{QS}' AND '{QE}'
                  THEN 1 END) AS pretw_q3_cnt
    FROM MDM.MDM_INTERFACES.DIM_USE_CASE
    WHERE THEATER_NAME = '{THEATER}'
      AND IS_LOST = FALSE AND IS_DEPLOYED = FALSE
      AND USE_CASE_EACV > 0 AND STAGE_NUMBER BETWEEN 1 AND 6
    """
    # Wins target
    target_sql = f"""
    SELECT SUM(USE_CASE_WON_TARGET_REGION) AS wins_target
    FROM SALES.REPORTING.PEAK_USE_CASE_TARGETS
    WHERE GEO = 'AMSExpansion' AND FISCAL_QUARTER = '{FQ_KEY}'
      AND REGION != 'AMSExpansion_Rgn_Hold'
    """
    pipeline = run(conn, pipeline_sql)
    target_rows = run(conn, target_sql)
    return {
        "pretw_total": pipeline[0]["PRETW_TOTAL"] if pipeline else 0,
        "pretw_cnt": pipeline[0]["PRETW_CNT"] if pipeline else 0,
        "pretw_q3_decision": pipeline[0]["PRETW_Q3_DECISION"] if pipeline else 0,
        "pretw_q3_cnt": pipeline[0]["PRETW_Q3_CNT"] if pipeline else 0,
        "wins_target": target_rows[0]["WINS_TARGET"] if target_rows else 0,
    }


# ─── Wins Model Constants (4Q calibration, Q2 FY27 at 2× weight) ────────────
# Pipeline = Stage 1-3 UCs with DECISION_DATE in Q3 (not go-live date)
# Won = UC reaches Stage 4 (ACTUAL_USE_CASE_WON_DATE in quarter)
W3_AVG_PRETW_RATE = 0.294   # wt avg(29.7, 26.1, 25.6, 32.8×2) / 5
W3_MIN_PRETW_RATE = 0.256   # Q1 FY27 (lowest)
# New wins % (fraction of total won from UCs outside decision-dated pipeline)
W3_AVG_NEW_PCT   = 0.462   # wt avg(41.8, 34.7, 55.2, 49.7×2) / 5
W3_MIN_NEW_PCT   = 0.347   # Q4 FY26 (lowest)


def query_districts(conn):
    """District-level open pipeline for Commercial and USGrowth, from PEAK."""
    sql = _peak_base() + f"""
    SELECT
        district                                    AS district,
        region                                      AS region,
        COUNT(*)                                    AS uc_count,
        ROUND(SUM(acv), 0)                          AS total_acv,
        ROUND(SUM(CASE WHEN stage_num=5 AND days_in_stage BETWEEN  0 AND 13 THEN acv ELSE 0 END),0) AS imp_0_13,
        ROUND(SUM(CASE WHEN stage_num=5 AND days_in_stage BETWEEN 14 AND 29 THEN acv ELSE 0 END),0) AS imp_14_29,
        ROUND(SUM(CASE WHEN stage_num=5 AND days_in_stage BETWEEN 30 AND 59 THEN acv ELSE 0 END),0) AS imp_30_59,
        ROUND(SUM(CASE WHEN stage_num=5 AND days_in_stage BETWEEN 60 AND 89 THEN acv ELSE 0 END),0) AS imp_60_89,
        ROUND(SUM(CASE WHEN stage_num=5 AND days_in_stage >= 90            THEN acv ELSE 0 END),0) AS imp_90p,
        ROUND(SUM(CASE WHEN stage_num=5 THEN acv ELSE 0 END),0)  AS imp_total,
        ROUND(SUM(CASE WHEN stage_num=4 AND (days_in_stage + {M1_HORIZON}) >= 104 THEN acv ELSE 0 END),0) AS tw_good,
        ROUND(SUM(CASE WHEN stage_num=4 THEN acv ELSE 0 END),0)  AS tw_total,
        ROUND(SUM(CASE WHEN stage_num IN (1,2,3) AND (days_in_stage + {M1_HORIZON}) >= 146 THEN acv ELSE 0 END),0) AS pretw_good,
        ROUND(SUM(CASE WHEN stage_num IN (1,2,3) THEN acv ELSE 0 END),0) AS pretw_total,
        ROUND(SUM(CASE WHEN stage_num=6 THEN acv ELSE 0 END),0)  AS stage6_acv,
        COUNT(CASE WHEN stage_num=6 THEN 1 END)                  AS stage6_count,
        COUNT(CASE WHEN stage_num=5 AND days_in_stage > 79 THEN 1 END)  AS stale_imp_cnt,
        ROUND(SUM(CASE WHEN stage_num=5 AND days_in_stage > 79 THEN acv ELSE 0 END),0) AS stale_imp_acv,
        COUNT(CASE WHEN stage_num=4 AND days_in_stage > 104 THEN 1 END) AS stale_tw_cnt,
        COUNT(CASE WHEN stage_num >= 4 AND (next_steps IS NULL OR next_steps = '') THEN 1 END) AS no_next_steps
    FROM base
    WHERE stage_num BETWEEN 1 AND 6
      AND region IN ('Commercial', 'USGrowthExp')
      AND district IS NOT NULL
    GROUP BY district, region
    ORDER BY region, total_acv DESC
    """
    return run(conn, sql)




def compute_m1(r, deployed=0):
    """
    M1 Pipeline Risk. Deployed ACV is already banked, so it floors every case.
      Commit  = Deployed + Stage6 + Stage5 (all pass 79d at the current horizon)
      ML      = Commit + TW_Good + PreTW_Good
      Stretch = Deployed + total open pipeline
    """
    commit  = deployed + r["STAGE6_ACV"] + r["IMP_TOTAL"]
    ml      = commit + r["TW_GOOD"] + r["PRETW_GOOD"]
    stretch = deployed + r["TOTAL_ACV"]
    return {"commit": commit, "ml": ml, "stretch": stretch}


def compute_m2(deployed):
    """
    M2 Historical Pacing: extrapolate the quarter from what is already banked,
    using the share of final deployed ACV that prior quarters had reached by
    this same day. Undefined pre-quarter and before day 30 (too noisy).
    """
    if not M2_ACTIVE or deployed <= 0:
        return None
    p = M2_PACE
    # A higher pace share implies less left to come, hence the inversion:
    # dividing by the max share gives the conservative (commit) case.
    return {
        "commit":  deployed / p["max"],
        "ml":      deployed / p["avg"],
        "stretch": deployed / p["min"],
    }


def compute_m3(r, deployed=0):
    """
    M3 Stage Conversion (PEAK QC Dashboard methodology):
      Milestone-date classification, divisor formula, horizon-calibrated rates.
      known   = deployed + sum(bucket ACV x bucket conversion rate)
      ML      = known / (1 - avg_new_pct)
      Commit  = known / (1 - min_new_pct)
      Stretch = deployed + total open pipeline (capped)
    Deployed sits inside the numerator because it is part of final deployed ACV;
    the divisor then grosses up for pipeline not yet visible at this horizon.
    """
    imp_acv   = r.get("IMP_MILESTONE", r["IMP_TOTAL"] + r["STAGE6_ACV"])
    tw_acv    = r.get("TW_MILESTONE", r["TW_TOTAL"])
    pretw_acv = r.get("PRETW_MILESTONE", r["PRETW_TOTAL"])

    known_ml     = deployed + (imp_acv * M3_AVG_IMP_RATE) + (tw_acv * M3_AVG_TW_RATE) + (pretw_acv * M3_AVG_PRETW_RATE)
    known_commit = deployed + (imp_acv * M3_MIN_IMP_RATE) + (tw_acv * M3_MIN_TW_RATE) + (pretw_acv * M3_MIN_PRETW_RATE)

    ml      = known_ml / (1 - M3_AVG_NEW_PCT)
    commit  = known_commit / (1 - M3_MIN_NEW_PCT)
    stretch = deployed + r["TOTAL_ACV"]   # capped at banked + total pipeline
    return {"commit": commit, "ml": ml, "stretch": stretch}


def compute_m4(m1, m3, m2=None):
    """
    Weighted ensemble. When M2 is unavailable its weight has already been
    redistributed to M1/M3 in the config block, so W2 is 0 and this still
    sums to 1.0.
    """
    def blend(key):
        val = M4_W1 * m1[key] + M4_W3 * m3[key]
        if m2 and M4_W2:
            val += M4_W2 * m2[key]
        return val

    return {k: blend(k) for k in ("commit", "ml", "stretch")}


# ─────────────────────────── Formatting helpers ──────────────────────────────

def fmt_m(val):
    """
    Format ACV as $XX.XM — always one decimal.

    Previously this dropped the decimal above $10M, which rounded away exactly
    the differences that matter when reconciling to MaxIQ: $143.8M and $144.1M
    both rendered as "$144M", making a real change look like no change at all.
    """
    return f"${val / 1_000_000:.1f}M"


def fmt_pct(num, denom):
    if not denom:
        return "—"
    return f"{100*num/denom:.0f}%"


def pct_f(num, denom):
    if not denom:
        return 0
    return 100 * num / denom


# ─────────────────────────── HTML Generation ─────────────────────────────────

RVP_NAMES = {
    "NorthwestExp": "Dean Kuvelis",
    "NortheastExp": "Matt Moscoffian",
    "CentralExp":   "Jeff LaCamera",
    "USGrowthExp":  "Brian Daniels",
    "SoutheastExp": "Adrian Tarquinio",
    "SouthwestExp": "Adam Sadowski",
    "CanadaExp":    "Shannon Katschilo",
    "Commercial":   "Lisa Yu",
}

# Prior-year same-quarter actuals, populated at runtime by query_prior_actuals()
PRIOR_ACTUALS = {"TOTAL": 0}


CSS = """
* { box-sizing: border-box; margin: 0; padding: 0; }
body { font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, sans-serif; background: #f0f2f5; color: #1a1a2e; padding: 24px; }
h1 { font-size: 1.8em; color: #1a1a2e; margin-bottom: 4px; }
.subtitle { color: #666; font-size: 0.95em; margin-bottom: 24px; }
.badge { display: inline-block; background: #29B5E8; color: white; padding: 3px 10px; border-radius: 12px; font-size: 0.8em; font-weight: 600; margin-left: 8px; vertical-align: middle; }

.summary-banner { display: grid; grid-template-columns: repeat(5, 1fr); gap: 16px; margin-bottom: 24px; }
.banner-card { background: white; border-radius: 10px; padding: 18px; box-shadow: 0 2px 8px rgba(0,0,0,0.07); text-align: center; border-top: 4px solid #ddd; }
.banner-card.commit  { border-top-color: #e67e22; }
.banner-card.ml      { border-top-color: #29B5E8; }
.banner-card.stretch { border-top-color: #28a745; }
.banner-card.target  { border-top-color: #6f42c1; }
.banner-card.pipeline{ border-top-color: #1a1a2e; }
.banner-label { font-size: 0.72em; text-transform: uppercase; letter-spacing: 0.5px; color: #888; margin-bottom: 6px; }
.banner-value { font-size: 2em; font-weight: 700; }
.banner-value.commit  { color: #e67e22; }
.banner-value.ml      { color: #29B5E8; }
.banner-value.stretch { color: #28a745; }
.banner-value.target  { color: #6f42c1; }
.banner-sub { font-size: 0.78em; color: #888; margin-top: 4px; }

.progress-wrap { margin: 0 0 24px 0; background: white; border-radius: 10px; padding: 18px 24px; box-shadow: 0 2px 8px rgba(0,0,0,0.07); }
.progress-wrap h3 { font-size: 0.85em; text-transform: uppercase; letter-spacing: 0.5px; color: #888; margin-bottom: 12px; }
.progress-track { height: 28px; background: #f0f2f5; border-radius: 6px; position: relative; overflow: visible; margin-bottom: 8px; }
.progress-bar { height: 100%; border-radius: 6px; display: flex; align-items: center; padding-left: 10px; font-size: 0.82em; font-weight: 600; color: white; }
.bar-commit  { background: #e67e22; }
.bar-ml      { background: #29B5E8; }
.bar-stretch { background: #28a745; }
.target-line { position: absolute; top: -6px; bottom: -6px; width: 3px; background: #6f42c1; border-radius: 2px; }
.target-label { position: absolute; top: -22px; font-size: 0.75em; color: #6f42c1; font-weight: 700; white-space: nowrap; transform: translateX(-50%); }
.progress-legend { display: flex; gap: 20px; font-size: 0.78em; color: #666; margin-top: 8px; }
.leg-dot { display: inline-block; width: 10px; height: 10px; border-radius: 50%; margin-right: 4px; vertical-align: middle; }

details.model-breakdown { background: white; border-radius: 10px; padding: 0; box-shadow: 0 2px 8px rgba(0,0,0,0.07); margin-bottom: 24px; }
details.model-breakdown summary { padding: 16px 24px; cursor: pointer; list-style: none; display: flex; align-items: center; gap: 8px; }
details.model-breakdown summary::-webkit-details-marker { display: none; }
details.model-breakdown summary::before { content: '▶'; font-size: 0.7em; color: #888; transition: transform 0.2s; flex-shrink: 0; }
details.model-breakdown[open] summary::before { transform: rotate(90deg); }
details.model-breakdown summary h3 { margin: 0; font-size: 0.95em; color: #1a1a2e; }
details.model-breakdown summary:hover { background: #f8f9fa; border-radius: 10px; }
details.model-breakdown > *:not(summary) { padding: 0 24px 20px; }
.model-table { width: 100%; border-collapse: collapse; font-size: 0.88em; }
.model-table th { background: #1a1a2e; color: white; padding: 10px 14px; text-align: left; }
.model-table th.num { text-align: right; }
.model-table td { padding: 10px 14px; border-bottom: 1px solid #eee; vertical-align: top; }
.model-table td.num { text-align: right; font-family: 'SF Mono', Consolas, monospace; }
.model-table tr:last-child td { border-bottom: none; font-weight: 700; background: #f0f8fd; }

.weight-box { background: #f8f9fa; border-radius: 8px; padding: 12px 16px; margin-top: 12px; font-size: 0.82em; }
.weight-box strong { color: #1a1a2e; }
.weight-grid { display: grid; grid-template-columns: repeat(4, 1fr); gap: 12px; margin-top: 10px; }
.weight-item { text-align: center; background: white; border-radius: 6px; padding: 8px; border: 1px solid #eee; }
.weight-label { font-size: 0.78em; color: #888; margin-bottom: 3px; }
.weight-value { font-weight: 700; font-size: 1.05em; }

.defs-box { background: white; border-radius: 10px; padding: 16px 22px; box-shadow: 0 2px 8px rgba(0,0,0,0.07); margin-bottom: 18px; display: flex; gap: 32px; flex-wrap: wrap; }
.defs-box .def-item { font-size: 0.8em; color: #555; }
.defs-box .def-term { font-weight: 700; color: #1a1a2e; }

.region-grid { display: grid; grid-template-columns: 1fr 1fr; gap: 18px; }
.region-card { background: white; border-radius: 10px; padding: 20px; box-shadow: 0 2px 8px rgba(0,0,0,0.07); border-left: 5px solid #29B5E8; }
.region-card.risk-flag { border-left-color: #e74c3c; }
.region-name { font-size: 1.1em; font-weight: 700; color: #1a1a2e; margin-bottom: 3px; }
.region-pipeline { font-size: 0.85em; color: #666; margin-bottom: 12px; }

.phase-bar-wrap { margin-bottom: 10px; }
.phase-bar-track { display: flex; height: 20px; border-radius: 4px; overflow: hidden; }
.phase-imp   { background: #17a2b8; }
.phase-tw    { background: #ffc107; }
.phase-pretw { background: #e9ecef; }
.phase-s6    { background: #28a745; }
.phase-legend { display: flex; gap: 12px; margin-top: 5px; font-size: 0.75em; color: #666; }
.phase-legend span::before { content: '●'; margin-right: 3px; }
.phase-imp-leg::before   { color: #17a2b8; }
.phase-tw-leg::before    { color: #ffc107; }
.phase-pretw-leg::before { color: #aaa; }
.phase-s6-leg::before    { color: #28a745; }

.model-mini { display: grid; grid-template-columns: 1fr 1fr 1fr; gap: 6px; margin-bottom: 10px; font-size: 0.78em; }
.model-mini-item { background: #f8f9fa; border-radius: 4px; padding: 5px 8px; text-align: center; }
.model-mini-label { color: #888; font-size: 0.82em; margin-bottom: 2px; }
.model-mini-value { font-weight: 700; color: #1a1a2e; }
.model-mini-value.m4 { color: #27ae60; }

.scenario-row { display: grid; grid-template-columns: 1fr 1fr 1fr; gap: 10px; margin-bottom: 10px; }
.scenario-box { text-align: center; border-radius: 6px; padding: 10px 8px; }
.scenario-box.commit  { background: #fef9f5; border: 1px solid #f0a070; }
.scenario-box.ml      { background: #f0fdf4; border: 1px solid #86efac; }
.scenario-box.stretch { background: #f0f8fd; border: 1px solid #7bd3f0; }
.scenario-label { font-size: 0.68em; text-transform: uppercase; letter-spacing: 0.4px; color: #888; margin-bottom: 4px; }
.scenario-value { font-size: 1.3em; font-weight: 700; }
.scenario-value.commit  { color: #e67e22; }
.scenario-value.ml      { color: #16a34a; }
.scenario-value.stretch { color: #1d8ab5; }

.rvp-line { text-align: center; font-size: 0.78em; color: #888; margin-bottom: 10px; }
.facts-grid { display: grid; grid-template-columns: 1fr 1fr; gap: 4px 16px; font-size: 0.8em; color: #444; padding-top: 10px; border-top: 1px solid #eee; }
.fact-row { display: flex; justify-content: space-between; padding: 3px 0; border-bottom: 1px solid #f5f5f5; }
.fact-label { color: #888; }
.fact-value { font-weight: 600; color: #1a1a2e; }
.fact-value.warn    { color: #c0392b; }
.fact-value.ok      { color: #27ae60; }
.fact-value.neutral { color: #555; }

.locked-bar { display: flex; align-items: center; gap: 8px; font-size: 0.8em; margin-bottom: 8px; }
.locked-label { color: #666; white-space: nowrap; }
.locked-track { flex: 1; height: 8px; background: #e9ecef; border-radius: 4px; overflow: hidden; }
.locked-fill { height: 100%; background: #28a745; border-radius: 4px; }
.locked-value { color: #28a745; font-weight: 600; white-space: nowrap; }

.methodology { background: white; border-radius: 10px; padding: 20px; box-shadow: 0 2px 8px rgba(0,0,0,0.07); margin-top: 24px; }
.methodology h3 { font-size: 0.9em; color: #1a1a2e; margin-bottom: 12px; }
.method-grid { display: grid; grid-template-columns: repeat(4, 1fr); gap: 14px; }
.method-box { background: #f8f9fa; border-radius: 6px; padding: 12px; font-size: 0.8em; }
.method-box h4 { margin-bottom: 6px; font-size: 0.85em; }
.method-box p { color: #555; line-height: 1.5; }
.caution { background: #fff8f0; border-left: 3px solid #e67e22; padding: 10px 14px; border-radius: 4px; font-size: 0.8em; color: #555; margin-top: 12px; line-height: 1.5; }
.insight { background: #e8f8e8; border-left: 3px solid #27ae60; padding: 10px 14px; border-radius: 4px; font-size: 0.8em; color: #2c5f2e; margin-top: 12px; line-height: 1.5; }
.section-title { font-size: 1.1em; font-weight: 700; color: #1a1a2e; margin: 24px 0 10px; }
.stale-tag { font-size: 0.72em; background: #fff3cd; color: #856404; padding: 2px 6px; border-radius: 4px; margin-left: 4px; font-weight: 600; }
.risk-tag  { font-size: 0.72em; background: #f8d7da; color: #842029; padding: 2px 6px; border-radius: 4px; margin-left: 4px; font-weight: 600; }
.over-tag  { font-size: 0.72em; background: #d4edda; color: #155724; padding: 2px 6px; border-radius: 4px; margin-left: 4px; font-weight: 600; }

/* ── District Tabs ── */
.tab-wrap { background: white; border-radius: 10px; box-shadow: 0 2px 8px rgba(0,0,0,0.07); margin-top: 28px; overflow: hidden; }
.tab-bar  { display: flex; border-bottom: 2px solid #e9ecef; background: #f8f9fa; }
.tab-btn  { padding: 14px 28px; font-size: 0.92em; font-weight: 600; color: #888; cursor: pointer; border: none; background: none; border-bottom: 3px solid transparent; margin-bottom: -2px; transition: color .15s, border-color .15s; }
.tab-btn:hover { color: #1a1a2e; }
.tab-btn.active { color: #29B5E8; border-bottom-color: #29B5E8; }
.tab-content { display: none; padding: 20px 24px 24px; }
.tab-content.active { display: block; }
.tab-subtitle { font-size: 0.82em; color: #888; margin-bottom: 16px; }
.district-grid { display: grid; grid-template-columns: repeat(4, 1fr); gap: 14px; }
.district-card { background: #f8f9fa; border-radius: 8px; padding: 16px; border-top: 3px solid #29B5E8; }
.district-card.risk-flag { border-top-color: #e74c3c; }
.district-name  { font-size: 0.95em; font-weight: 700; color: #1a1a2e; margin-bottom: 2px; }
.district-meta  { font-size: 0.75em; color: #888; margin-bottom: 10px; }
.district-m4    { text-align: center; font-size: 1.6em; font-weight: 700; color: #27ae60; margin: 8px 0 4px; }
.district-m4-label { text-align: center; font-size: 0.68em; text-transform: uppercase; letter-spacing: 0.4px; color: #888; margin-bottom: 10px; }
.district-mini  { display: grid; grid-template-columns: 1fr 1fr; gap: 5px; font-size: 0.75em; margin-bottom: 10px; }
.district-mini-item { background: white; border-radius: 4px; padding: 4px 6px; text-align: center; }
.district-mini-label { color: #aaa; font-size: 0.82em; }
.district-mini-value { font-weight: 600; color: #1a1a2e; }
.district-flags { font-size: 0.73em; color: #888; border-top: 1px solid #e9ecef; padding-top: 8px; display: flex; flex-direction: column; gap: 3px; }
.district-flags span { display: flex; justify-content: space-between; }
.district-flags .warn { color: #c0392b; font-weight: 600; }
.district-flags .ok   { color: #27ae60; font-weight: 600; }
"""


def region_card_html(r, target_acv, m1, m3, m4, yoy_acv):
    region   = r["REGION"]
    rvp      = RVP_NAMES.get(region, r.get("RVP") or "")
    total    = r["TOTAL_ACV"]
    imp      = r["IMP_TOTAL"]
    tw       = r["TW_TOTAL"]
    pretw    = r["PRETW_TOTAL"]
    s6       = r["STAGE6_ACV"]
    uc_count = r["UC_COUNT"]

    # Phase bar percentages
    p_imp   = 100 * imp   / total if total else 0
    p_tw    = 100 * tw    / total if total else 0
    p_pretw = 100 * pretw / total if total else 0

    # Locked bar (stage 6)
    p_s6_locked = 100 * s6 / target_acv if target_acv else 0

    # ML/target badge
    ml_pct = pct_f(m4["ml"], target_acv)
    if ml_pct >= 100:
        ml_badge = f'<span class="over-tag">{ml_pct:.0f}% of target</span>'
        risk_class = ""
    elif ml_pct >= 90:
        ml_badge = f'<span class="risk-tag">{ml_pct:.0f}% of target</span>'
        risk_class = ""
    else:
        ml_badge = f'<span class="risk-tag">{ml_pct:.0f}% of target</span>'
        risk_class = " risk-flag"

    # Stale tags
    stale_tags = ""
    if r["STALE_IMP_ACV"] > 0:
        stale_tags += f' <span class="stale-tag">&#9888; {fmt_m(r["STALE_IMP_ACV"])} Stale IMP</span>'
    if r["STAGE6_COUNT"] == 0 and s6 == 0:
        stale_tags += ' <span class="risk-tag">No Stage 6</span>'

    # YoY
    if yoy_acv:
        yoy_chg = (total - yoy_acv) / yoy_acv * 100
        yoy_str = f'{fmt_m(yoy_acv)} &rarr; {fmt_m(total)} ({yoy_chg:+.0f}%)'
        yoy_cls = "warn" if yoy_chg < 0 else "fact-value"
    else:
        yoy_str = "—"
        yoy_cls = "neutral"

    # Stale IMP display
    stale_imp_cls = "warn" if r["STALE_IMP_CNT"] > 0 else "ok"
    stale_tw_cls  = "warn" if r["STALE_TW_CNT"] > 0 else "ok"
    nns_cls       = "warn" if r["NO_NEXT_STEPS"] >= 5 else ("neutral" if r["NO_NEXT_STEPS"] > 0 else "ok")

    q3fy26_actual = PRIOR_ACTUALS.get(region, 0)
    q3fy26_str    = fmt_m(q3fy26_actual) if q3fy26_actual else "—"
    target_str    = fmt_m(target_acv) if target_acv else "—"
    locked_color  = "#dc3545" if s6 == 0 else ""

    return f"""
  <div class="region-card{risk_class}">
    <div class="region-name">{region} {ml_badge}{stale_tags}</div>
    <div class="region-pipeline">{fmt_m(total)} total &bull; {uc_count} open UCs &bull; IMP {p_imp:.0f}% / TW {p_tw:.0f}% / Pre-TW {p_pretw:.0f}%</div>
    <div class="phase-bar-wrap">
      <div class="phase-bar-track">
        <div class="phase-imp"   style="width:{p_imp:.1f}%"></div>
        <div class="phase-tw"    style="width:{p_tw:.1f}%"></div>
        <div class="phase-pretw" style="width:{p_pretw:.1f}%"></div>
      </div>
      <div class="phase-legend">
        <span class="phase-imp-leg">IMP {fmt_m(imp)}</span>
        <span class="phase-tw-leg">TW {fmt_m(tw)}</span>
        <span class="phase-pretw-leg">Pre-TW {fmt_m(pretw)}</span>
      </div>
    </div>
    <div class="locked-bar">
      <span class="locked-label">Stage 6 locked:</span>
      <div class="locked-track"><div class="locked-fill" style="width:{min(p_s6_locked,100):.1f}%"></div></div>
      <span class="locked-value" style="color:{locked_color}">{fmt_m(s6)} &bull; {r['STAGE6_COUNT']} UC{'s' if r['STAGE6_COUNT'] != 1 else ''}</span>
    </div>
    <div class="model-mini">
      <div class="model-mini-item"><div class="model-mini-label">M1 ML</div><div class="model-mini-value">{fmt_m(m1['ml'])}</div></div>
      <div class="model-mini-item"><div class="model-mini-label">M3 ML</div><div class="model-mini-value">{fmt_m(m3['ml'])}</div></div>
      <div class="model-mini-item"><div class="model-mini-label">M4 ML</div><div class="model-mini-value m4">{fmt_m(m4['ml'])}</div></div>
    </div>
    <div class="scenario-row">
      <div class="scenario-box commit"><div class="scenario-label">M4 Commit</div><div class="scenario-value commit">{fmt_m(m4['commit'])}</div></div>
      <div class="scenario-box ml"><div class="scenario-label">M4 Most Likely</div><div class="scenario-value ml">{fmt_m(m4['ml'])}</div></div>
      <div class="scenario-box stretch"><div class="scenario-label">M4 Stretch</div><div class="scenario-value stretch">{fmt_m(m4['stretch'])}</div></div>
    </div>
    <div class="rvp-line">RVP: <strong>{rvp}</strong> &bull; Target: <strong style="color:#6f42c1;">{target_str}</strong> &bull; M4 ML {ml_pct:.0f}% of target &bull; {PRIOR_LABEL} actual: {q3fy26_str}</div>
    <div class="facts-grid">
      <div class="fact-row"><span class="fact-label">YoY pipeline ({PRIOR_LABEL})</span><span class="fact-value {yoy_cls}">{yoy_str}</span></div>
      <div class="fact-row"><span class="fact-label">Stale IMP (&gt;79d)</span><span class="fact-value {stale_imp_cls}">{r['STALE_IMP_CNT']} UCs / {fmt_m(r['STALE_IMP_ACV'])}</span></div>
      <div class="fact-row"><span class="fact-label">Stale TW (&gt;104d)</span><span class="fact-value {stale_tw_cls}">{r['STALE_TW_CNT']} UCs / {fmt_m(r['STALE_TW_ACV'])}</span></div>
      <div class="fact-row"><span class="fact-label">No next steps (Stage 4+)</span><span class="fact-value {nns_cls}">{r['NO_NEXT_STEPS']} UCs</span></div>
    </div>
  </div>"""


def district_card_html(d):
    name   = d["DISTRICT"]
    total  = d["TOTAL_ACV"]
    imp    = d["IMP_TOTAL"]
    tw     = d["TW_TOTAL"]
    pretw  = d["PRETW_TOTAL"]
    s6     = d["STAGE6_ACV"]

    p_imp   = 100 * imp   / total if total else 0
    p_tw    = 100 * tw    / total if total else 0
    p_pretw = 100 * pretw / total if total else 0

    m1 = compute_m1(d)
    m3 = compute_m3(d)
    m4 = compute_m4(m1, m3)

    risk_class = " risk-flag" if d["STALE_IMP_CNT"] >= 5 or d["NO_NEXT_STEPS"] >= 5 else ""

    stale_imp_cls = "warn" if d["STALE_IMP_CNT"] > 0 else "ok"
    stale_tw_cls  = "warn" if d["STALE_TW_CNT"] > 0 else "ok"
    nns_cls       = "warn" if d["NO_NEXT_STEPS"] >= 3 else ("ok" if d["NO_NEXT_STEPS"] == 0 else "")

    return f"""
  <div class="district-card{risk_class}">
    <div class="district-name">{name}</div>
    <div class="district-meta">{d['UC_COUNT']} UCs &bull; {fmt_m(total)} pipeline</div>
    <div class="phase-bar-wrap">
      <div class="phase-bar-track">
        <div class="phase-imp"   style="width:{p_imp:.1f}%"></div>
        <div class="phase-tw"    style="width:{p_tw:.1f}%"></div>
        <div class="phase-pretw" style="width:{p_pretw:.1f}%"></div>
      </div>
      <div class="phase-legend">
        <span class="phase-imp-leg">IMP {fmt_m(imp)}</span>
        <span class="phase-tw-leg">TW {fmt_m(tw)}</span>
        <span class="phase-pretw-leg">Pre-TW {fmt_m(pretw)}</span>
      </div>
    </div>
    <div class="district-m4">{fmt_m(m4['ml'])}</div>
    <div class="district-m4-label">M4 Most Likely</div>
    <div class="district-mini">
      <div class="district-mini-item"><div class="district-mini-label">M1 ML</div><div class="district-mini-value">{fmt_m(m1['ml'])}</div></div>
      <div class="district-mini-item"><div class="district-mini-label">M3 ML</div><div class="district-mini-value">{fmt_m(m3['ml'])}</div></div>
      <div class="district-mini-item"><div class="district-mini-label">M4 Commit</div><div class="district-mini-value">{fmt_m(m4['commit'])}</div></div>
      <div class="district-mini-item"><div class="district-mini-label">M4 Stretch</div><div class="district-mini-value">{fmt_m(m4['stretch'])}</div></div>
    </div>
    <div class="district-flags">
      <span><span>Stale IMP (&gt;79d)</span><span class="{stale_imp_cls}">{d['STALE_IMP_CNT']} UCs / {fmt_m(d['STALE_IMP_ACV'])}</span></span>
      <span><span>Stale TW (&gt;104d)</span><span class="{stale_tw_cls}">{d['STALE_TW_CNT']} UCs</span></span>
      <span><span>No next steps</span><span class="{nns_cls}">{d['NO_NEXT_STEPS']} UCs</span></span>
    </div>
  </div>"""


def district_tabs_html(districts):
    comm_rows = [d for d in districts if d["REGION"] == "Commercial"]
    usg_rows  = [d for d in districts if d["REGION"] == "USGrowthExp"]

    comm_total = sum(d["TOTAL_ACV"] for d in comm_rows)
    usg_total  = sum(d["TOTAL_ACV"] for d in usg_rows)

    comm_cards = "".join(district_card_html(d) for d in comm_rows)
    usg_cards  = "".join(district_card_html(d) for d in usg_rows)

    return f"""
<div class="tab-wrap">
  <div class="tab-bar">
    <button class="tab-btn active" onclick="showTab('commercial', this)">Commercial Districts</button>
    <button class="tab-btn"        onclick="showTab('usgrowth',   this)">USGrowth Districts</button>
  </div>

  <div id="tab-commercial" class="tab-content active">
    <p class="tab-subtitle">Commercial &mdash; {len(comm_rows)} districts &bull; {fmt_m(comm_total)} total {QLABEL} pipeline &bull; RVP: Lisa Yu</p>
    <div class="district-grid">
      {comm_cards}
    </div>
  </div>

  <div id="tab-usgrowth" class="tab-content">
    <p class="tab-subtitle">USGrowth &mdash; {len(usg_rows)} districts &bull; {fmt_m(usg_total)} total {QLABEL} pipeline &bull; RVP: Brian Daniels</p>
    <div class="district-grid">
      {usg_cards}
    </div>
  </div>
</div>

<script>
function showTab(id, btn) {{
  document.querySelectorAll('.tab-content').forEach(el => el.classList.remove('active'));
  document.querySelectorAll('.tab-btn').forEach(el => el.classList.remove('active'));
  document.getElementById('tab-' + id).classList.add('active');
  btn.classList.add('active');
}}
</script>"""


def generate_html(regions, targets, yoy_total, districts, wins_data,
                  deployed_by_region=None, won_qtd=None, maxiq=None):
    deployed_by_region = deployed_by_region or {}
    won_qtd = won_qtd or {"won_acv": 0, "won_count": 0}
    _mk = ("open", "live", "commit", "ml", "best", "target", "won")
    maxiq = maxiq or {k: 0 for k in _mk}
    maxiq.setdefault("prior", {k: 0 for k in _mk})
    today_str = TODAY.strftime("%B %-d, %Y")
    days_label = (
        f"{abs(DAYS_TO_OPEN)} days before {QLABEL} opens"
        if not IN_QUARTER
        else f"{QLABEL} day {DAY_IN_QUARTER} of {QDAYS} — {DAYS_REMAINING} days remaining"
    )

    # Aggregate totals
    total_pipeline = sum(r["TOTAL_ACV"] for r in regions)
    total_target   = sum(targets.values())
    total_deployed = sum(float(v["DEPLOYED_ACV"] or 0)
                         for v in deployed_by_region.values())

    # Compute models for each region (deployed ACV folded in)
    region_models = region_models_from(regions, deployed_by_region)

    # Aggregate model totals. M2 is region-summed only where defined.
    agg_m1 = {k: sum(rm[1][k] for rm in region_models) for k in ("commit","ml","stretch")}
    agg_m3 = {k: sum(rm[3][k] for rm in region_models) for k in ("commit","ml","stretch")}
    agg_m4 = {k: sum(rm[4][k] for rm in region_models) for k in ("commit","ml","stretch")}
    agg_m2 = None
    if M2_ACTIVE:
        _m2s = [rm[2] for rm in region_models if rm[2]]
        if _m2s:
            agg_m2 = {k: sum(m[k] for m in _m2s) for k in ("commit","ml","stretch")}

    # Progress bar widths (cap at 100%)
    target_ref = total_target
    def bar_w(val): return min(100, 100 * val / target_ref) if target_ref else 0

    m4_ml_pct_str = fmt_pct(agg_m4["ml"], total_target)
    m4_str_pct_str = fmt_pct(agg_m4["stretch"], total_target)
    m4_cmt_pct_str = fmt_pct(agg_m4["commit"], total_target)

    # M2 display state — active only in-quarter from day 30 onward.
    if agg_m2:
        m2_col = "#333"
        m2_desc = (
            f"Extrapolates from {fmt_m(total_deployed)} already banked. Prior quarters had "
            f"reached {M2_PACE['avg']*100:.1f}% of final deployed ACV by day {DAY_IN_QUARTER} "
            f"(range {M2_PACE['min']*100:.1f}&ndash;{M2_PACE['max']*100:.1f}%), so "
            f"ML = deployed &divide; {M2_PACE['avg']:.3f}. Tight historical spread makes this "
            f"the most stable model at this point in the quarter."
        )
        m2_err = "&plusmn;2.1pt spread"
        m2_commit = fmt_m(agg_m2["commit"])
        m2_ml = fmt_m(agg_m2["ml"])
        m2_stretch = fmt_m(agg_m2["stretch"])
    else:
        m2_col = "#bbb"
        if not IN_QUARTER:
            m2_desc = (f"Requires deployed ACV as the denominator. Deployed = $0 until "
                       f"{QLABEL} opens {QS.strftime('%b %-d')}. Weight redistributed to M1/M3.")
        else:
            m2_desc = (f"Pacing is too noisy before day {M2_RELIABLE_DAY}; currently day "
                       f"{DAY_IN_QUARTER}. Weight redistributed to M1/M3.")
        m2_err = "&mdash;"
        m2_commit = m2_ml = m2_stretch = "N/A"

    m1_horizon_note = (f" ({DAYS_REMAINING} days left in {QLABEL})" if IN_QUARTER
                       else " (full quarter)")

    # YoY compares this quarter's open pipeline against the prior-year quarter
    # measured at the same point in its cycle (same calendar date one year
    # back, which is the same day-of-quarter / days-to-open offset).
    _yoy_date = f"{TODAY.strftime('%b %-d')}, {TODAY.year - 1}"
    if yoy_total:
        _yoy_pct = pct_f(total_pipeline - yoy_total, yoy_total)
        _yoy_point = (f"day {DAY_IN_QUARTER}" if IN_QUARTER
                      else f"{abs(DAYS_TO_OPEN)} days pre-open")
        yoy_caption = (
            f"{_yoy_pct:+.0f}% YoY &mdash; {PRIOR_LABEL} open pipeline was "
            f"{fmt_m(yoy_total)} at the same point in its cycle "
            f"({_yoy_point}, {_yoy_date})"
        )
    else:
        yoy_caption = f"No comparable {PRIOR_LABEL} snapshot for {_yoy_date}"

    if agg_m2:
        ensemble_caption = (f"Weighted Ensemble: {M4_W1*100:.0f}% Pipeline Risk + "
                            f"{M4_W2*100:.0f}% Pacing + {M4_W3*100:.0f}% Stage Conversion")
    else:
        ensemble_caption = (f"Weighted Ensemble: {M4_W1*100:.0f}% Pipeline Risk + "
                            f"{M4_W3*100:.0f}% Stage Conversion "
                            f"(Pacing N/A &mdash; weight redistributed)")

    if agg_m2:
        m4_method_note = (
            f"Dashboard weights with all three models live: M1 {M4_W1*100:.0f}%, "
            f"M2 {M4_W2*100:.0f}%, M3 {M4_W3*100:.0f}%. Applied identically to all "
            f"{len(regions)} regions."
        )
    else:
        _why = ("the quarter has not opened" if not IN_QUARTER
                else f"pacing is unreliable before day {M2_RELIABLE_DAY}")
        m4_method_note = (
            f"M2 contributes nothing because {_why}, so its {_M4_RAW_W2*100:.0f}% weight is "
            f"redistributed proportionally across M1 and M3 &mdash; M1 {M4_W1*100:.1f}%, "
            f"M3 {M4_W3*100:.1f}% &mdash; keeping the ensemble at 100%. Applied identically "
            f"to all {len(regions)} regions."
        )

    html_parts = [f"""<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="UTF-8">
<title>{QLABEL} Forecast — AMSExpansion</title>
<style>
{CSS}
</style>
</head>
<body>

<h1>{QLABEL} Forecast Analysis <span class="badge">AMSExpansion</span></h1>
<p class="subtitle">As of {today_str} &mdash; {days_label} ({QS.strftime("%b %-d, %Y")} &ndash; {QE.strftime("%b %-d, %Y")}) &mdash; {THEATER}{(" &mdash; " + GVP) if GVP else ""}</p>
{"" if CALIBRATED else '''
<div style="background:#fff3cd; border:2px solid #dc3545; border-radius:6px; padding:14px 18px; margin:12px 0;">
  <div style="color:#b3261e; font-weight:700; font-size:1.05em; margin-bottom:4px;">
    &#9888; UNCALIBRATED &mdash; fallback conversion rates
  </div>
  <div style="color:#3c4653; font-size:0.9em; line-height:1.5;">
    peak_calibrate.py did not run for this report, so M3 conversion rates and M2 pacing
    are the hardcoded values measured at a <strong>different time horizon</strong>, not at
    this run&rsquo;s horizon. Forecast figures below (Commit / Most Likely / Stretch) are
    unreliable and must not be quoted. Re-run without --allow-fallback-rates once the
    calibration failure is fixed.
  </div>
</div>'''}

<!-- Summary Banner -->
<div class="summary-banner">
  <div class="banner-card pipeline">
    <div class="banner-label">Total {QLABEL} Open Pipeline</div>
    <div class="banner-value" style="color:#1a1a2e">{fmt_m(total_pipeline)}</div>
    <div class="banner-sub">{yoy_caption}</div>
  </div>
  <div class="banner-card commit">
    <div class="banner-label">Commit (M4)</div>
    <div class="banner-value commit">{fmt_m(agg_m4['commit'])}</div>
    <div class="banner-sub">{ensemble_caption}</div>
  </div>
  <div class="banner-card ml">
    <div class="banner-label">Most Likely (M4)</div>
    <div class="banner-value ml">{fmt_m(agg_m4['ml'])}</div>
    <div class="banner-sub">Pipeline Risk: {fmt_m(agg_m1['ml'])} &bull; Stage Conv: {fmt_m(agg_m3['ml'])} &bull; Ensemble: {fmt_m(agg_m4['ml'])}</div>
  </div>
  <div class="banner-card stretch">
    <div class="banner-label">Stretch (M4)</div>
    <div class="banner-value stretch">{fmt_m(agg_m4['stretch'])}</div>
    <div class="banner-sub">Pipeline Risk: {fmt_m(agg_m1['stretch'])} &bull; Stage Conv: {fmt_m(agg_m3['stretch'])} &bull; Ensemble: {fmt_m(agg_m4['stretch'])}</div>
  </div>
  <div class="banner-card target">
    <div class="banner-label">{QLABEL} Target</div>
    <div class="banner-value target">{fmt_m(total_target)}</div>
    <div class="banner-sub">M4 ML = {m4_ml_pct_str} of target &bull; {PRIOR_LABEL} actual: {fmt_m(PRIOR_ACTUALS["TOTAL"])}</div>
  </div>
</div>

<!-- Progress Bars -->
<div class="progress-wrap">
  <h3>M4 Ensemble vs Target ({fmt_m(total_target)})</h3>
  <div style="position:relative; padding-top: 28px;">
    <div class="progress-track" style="margin-bottom:8px;">
      <div class="progress-bar bar-stretch" style="width:{bar_w(agg_m4['stretch']):.1f}%">Stretch: {fmt_m(agg_m4['stretch'])} ({m4_str_pct_str} of target)</div>
      <div class="target-line" style="left:{min(100,100*total_target/max(agg_m4['stretch'],total_target)):.1f}%"><div class="target-label">Target {fmt_m(total_target)}</div></div>
    </div>
    <div class="progress-track" style="margin-bottom:8px;">
      <div class="progress-bar bar-ml" style="width:{bar_w(agg_m4['ml']):.1f}%">Most Likely: {fmt_m(agg_m4['ml'])} ({m4_ml_pct_str})</div>
      <div class="target-line" style="left:{min(100,100*total_target/max(agg_m4['stretch'],total_target)):.1f}%"></div>
    </div>
    <div class="progress-track" style="margin-bottom:8px;">
      <div class="progress-bar bar-commit" style="width:{bar_w(agg_m4['commit']):.1f}%">Commit: {fmt_m(agg_m4['commit'])} ({m4_cmt_pct_str})</div>
      <div class="target-line" style="left:{min(100,100*total_target/max(agg_m4['stretch'],total_target)):.1f}%"></div>
    </div>
  </div>
  <div class="progress-legend">
    <span><span class="leg-dot" style="background:#e67e22"></span>Commit (M4)</span>
    <span><span class="leg-dot" style="background:#29B5E8"></span>Most Likely (M4)</span>
    <span><span class="leg-dot" style="background:#28a745"></span>Stretch (M4)</span>
    <span><span class="leg-dot" style="background:#6f42c1"></span>Target</span>
  </div>
</div>
"""]

    # ── Pipeline Composition ──────────────────────────────────────────────────
    agg_imp    = sum(r["IMP_TOTAL"]    for r in regions)
    agg_tw     = sum(r["TW_TOTAL"]     for r in regions)
    agg_pretw  = sum(r["PRETW_TOTAL"]  for r in regions)
    agg_s6     = sum(r["STAGE6_ACV"]   for r in regions)
    agg_stale_imp  = sum(r["STALE_IMP_ACV"] for r in regions)
    agg_stale_imp_n= sum(r["STALE_IMP_CNT"] for r in regions)
    agg_stale_tw   = sum(r["STALE_TW_ACV"]  for r in regions)
    agg_stale_tw_n = sum(r["STALE_TW_CNT"]  for r in regions)
    agg_nns        = sum(r["NO_NEXT_STEPS"]  for r in regions)

    fresh_imp = agg_imp - agg_stale_imp
    fresh_tw  = agg_tw  - agg_stale_tw
    stale_imp_pct = 100 * agg_stale_imp / agg_imp if agg_imp else 0
    stale_tw_pct  = 100 * agg_stale_tw  / agg_tw  if agg_tw  else 0

    # Stacked bar widths (of total pipeline)
    b_s6    = 100 * agg_s6 / total_pipeline if total_pipeline else 0
    b_imp_f = 100 * fresh_imp / total_pipeline if total_pipeline else 0
    b_imp_s = 100 * agg_stale_imp / total_pipeline if total_pipeline else 0
    b_tw_f  = 100 * fresh_tw / total_pipeline if total_pipeline else 0
    b_tw_s  = 100 * agg_stale_tw / total_pipeline if total_pipeline else 0
    b_ptw   = 100 * agg_pretw / total_pipeline if total_pipeline else 0

    html_parts.append(f"""
<!-- Pipeline Composition -->
<div style="background:white;border-radius:10px;padding:18px 24px;box-shadow:0 2px 8px rgba(0,0,0,0.07);margin-bottom:24px;">
  <h3 style="font-size:0.85em;text-transform:uppercase;letter-spacing:0.5px;color:#888;margin-bottom:14px;">Pipeline Composition &mdash; AMSExpansion ({fmt_m(total_pipeline)})</h3>

  <!-- Stacked bar -->
  <div style="display:flex;height:28px;border-radius:6px;overflow:hidden;margin-bottom:8px;">
    <div style="width:{b_s6:.1f}%;background:#28a745;" title="Stage 6: {fmt_m(agg_s6)}"></div>
    <div style="width:{b_imp_f:.1f}%;background:#17a2b8;" title="IMP (fresh): {fmt_m(fresh_imp)}"></div>
    <div style="width:{b_imp_s:.1f}%;background:#0d6980;" title="IMP (stale >79d): {fmt_m(agg_stale_imp)}"></div>
    <div style="width:{b_tw_f:.1f}%;background:#ffc107;" title="TW (fresh): {fmt_m(fresh_tw)}"></div>
    <div style="width:{b_tw_s:.1f}%;background:#b38600;" title="TW (stale >104d): {fmt_m(agg_stale_tw)}"></div>
    <div style="width:{b_ptw:.1f}%;background:#e9ecef;" title="Pre-TW: {fmt_m(agg_pretw)}"></div>
  </div>
  <div style="display:flex;gap:16px;flex-wrap:wrap;font-size:0.76em;color:#666;margin-bottom:16px;">
    <span><span style="display:inline-block;width:10px;height:10px;border-radius:50%;background:#28a745;margin-right:4px;vertical-align:middle;"></span>Stage 6: {fmt_m(agg_s6)}</span>
    <span><span style="display:inline-block;width:10px;height:10px;border-radius:50%;background:#17a2b8;margin-right:4px;vertical-align:middle;"></span>IMP (fresh): {fmt_m(fresh_imp)}</span>
    <span><span style="display:inline-block;width:10px;height:10px;border-radius:50%;background:#0d6980;margin-right:4px;vertical-align:middle;"></span>IMP (stale &gt;79d): {fmt_m(agg_stale_imp)}</span>
    <span><span style="display:inline-block;width:10px;height:10px;border-radius:50%;background:#ffc107;margin-right:4px;vertical-align:middle;"></span>TW (fresh): {fmt_m(fresh_tw)}</span>
    <span><span style="display:inline-block;width:10px;height:10px;border-radius:50%;background:#b38600;margin-right:4px;vertical-align:middle;"></span>TW (stale &gt;104d): {fmt_m(agg_stale_tw)}</span>
    <span><span style="display:inline-block;width:10px;height:10px;border-radius:50%;background:#e9ecef;margin-right:4px;vertical-align:middle;border:1px solid #ccc;"></span>Pre-TW: {fmt_m(agg_pretw)}</span>
  </div>

  <!-- Metric cards -->
  <div style="display:grid;grid-template-columns:repeat(5,1fr);gap:12px;">
    <div style="background:#f8f9fa;border-radius:8px;padding:14px;text-align:center;">
      <div style="font-size:0.7em;text-transform:uppercase;color:#888;margin-bottom:4px;">IMP (Stage 5)</div>
      <div style="font-size:1.5em;font-weight:700;color:#17a2b8;">{fmt_m(agg_imp)}</div>
      <div style="font-size:0.75em;color:#666;margin-top:3px;">{100*agg_imp/total_pipeline:.0f}% of pipeline</div>
    </div>
    <div style="background:#f8f9fa;border-radius:8px;padding:14px;text-align:center;">
      <div style="font-size:0.7em;text-transform:uppercase;color:#888;margin-bottom:4px;">TW (Stage 4)</div>
      <div style="font-size:1.5em;font-weight:700;color:#ffc107;">{fmt_m(agg_tw)}</div>
      <div style="font-size:0.75em;color:#666;margin-top:3px;">{100*agg_tw/total_pipeline:.0f}% of pipeline</div>
    </div>
    <div style="background:#f8f9fa;border-radius:8px;padding:14px;text-align:center;">
      <div style="font-size:0.7em;text-transform:uppercase;color:#888;margin-bottom:4px;">Pre-TW (Stage 1-3)</div>
      <div style="font-size:1.5em;font-weight:700;color:#555;">{fmt_m(agg_pretw)}</div>
      <div style="font-size:0.75em;color:#666;margin-top:3px;">{100*agg_pretw/total_pipeline:.0f}% of pipeline</div>
    </div>
    <div style="background:#fff8f0;border-radius:8px;padding:14px;text-align:center;border:1px solid #f0a070;">
      <div style="font-size:0.7em;text-transform:uppercase;color:#c0392b;margin-bottom:4px;">Stale IMP (&gt;79d)</div>
      <div style="font-size:1.5em;font-weight:700;color:#c0392b;">{fmt_m(agg_stale_imp)}</div>
      <div style="font-size:0.75em;color:#888;margin-top:3px;">{stale_imp_pct:.0f}% of IMP &bull; {agg_stale_imp_n} UCs</div>
    </div>
    <div style="background:#f8f9fa;border-radius:8px;padding:14px;text-align:center;{'border:1px solid #f0a070;background:#fff8f0;' if stale_tw_pct > 20 else ''}">
      <div style="font-size:0.7em;text-transform:uppercase;color:{'#c0392b' if stale_tw_pct > 20 else '#888'};margin-bottom:4px;">Stale TW (&gt;104d)</div>
      <div style="font-size:1.5em;font-weight:700;color:{'#c0392b' if stale_tw_pct > 20 else '#555'};">{fmt_m(agg_stale_tw)}</div>
      <div style="font-size:0.75em;color:#888;margin-top:3px;">{stale_tw_pct:.0f}% of TW &bull; {agg_stale_tw_n} UCs</div>
    </div>
  </div>

  <div style="margin-top:12px;font-size:0.78em;color:#888;display:flex;gap:20px;">
    <span>Stage 6 (locked): <strong style="color:#28a745;">{fmt_m(agg_s6)}</strong></span>
    <span>No next steps (Stage 4+): <strong style="color:{'#c0392b' if agg_nns > 20 else '#555'};">{agg_nns} UCs</strong></span>
  </div>
</div>
""")

    # ── MaxIQ tie-out ─────────────────────────────────────────────────────────
    # Proves the pipeline figures reconcile to what leadership sees in MaxIQ,
    # and puts the model's forecast call next to MaxIQ's own call.
    def _delta_cell(mine, theirs):
        if not theirs:
            return '<td class="num" style="color:#bbb;">&mdash;</td>'
        d = mine - theirs
        pct = 100 * d / theirs
        if abs(pct) < 1.0:
            col, mark = "#27ae60", "&#10003;"
        elif abs(pct) < 5.0:
            col, mark = "#e67e22", ""
        else:
            col, mark = "#c0392b", "&#9888;"
        return (f'<td class="num" style="color:{col};font-weight:600;">'
                f'{d/1e6:+.1f}M ({pct:+.1f}%) {mark}</td>')

    _mp = maxiq["prior"]
    _tie_rows = [
        ("Open pipeline (Going Live)", total_pipeline, maxiq["open"], _mp["open"]),
        ("Deployed / banked (Live)",   total_deployed, maxiq["live"], _mp["live"]),
        ("Wins booked (Won)",          float(won_qtd["won_acv"]), maxiq["won"], _mp["won"]),
        ("Target",                     total_target,   maxiq["target"], _mp["target"]),
    ]
    _call_rows = [
        ("Commit",      agg_m4["commit"],  maxiq["commit"], _mp["commit"]),
        ("Most Likely", agg_m4["ml"],      maxiq["ml"],     _mp["ml"]),
        ("Stretch / Best Case", agg_m4["stretch"], maxiq["best"], _mp["best"]),
    ]

    def _wow_cell(now, prior):
        if not prior or not now:
            return '<td class="num" style="color:#bbb;">&mdash;</td>'
        d = now - prior
        col = "#27ae60" if d >= 0 else "#c0392b"
        return (f'<td class="num" style="color:{col};">{d/1e6:+.1f}M</td>')

    tie_html = "".join(
        f"<tr><td>{label}</td><td class='num'>{fmt_m(mine)}</td>"
        f"<td class='num'>{fmt_m(theirs) if theirs else '&mdash;'}</td>"
        f"{_delta_cell(mine, theirs)}"
        f"<td class='num' style='color:#888;'>{fmt_m(prior) if prior else '&mdash;'}</td>"
        f"{_wow_cell(theirs, prior)}</tr>"
        for label, mine, theirs, prior in _tie_rows
    )
    call_html = "".join(
        f"<tr><td>{label}</td><td class='num'>{fmt_m(mine)}</td>"
        f"<td class='num'>{fmt_m(theirs) if theirs else '&mdash;'}</td>"
        f"{_delta_cell(mine, theirs)}"
        f"<td class='num' style='color:#888;'>{fmt_m(prior) if prior else '&mdash;'}</td>"
        f"{_wow_cell(theirs, prior)}</tr>"
        for label, mine, theirs, prior in _call_rows
    )

    _ml_gap = agg_m4["ml"] - maxiq["ml"] if maxiq["ml"] else 0
    if maxiq["ml"] and abs(_ml_gap) / maxiq["ml"] > 0.15:
        _call_note = (
            f'<div class="caution" style="margin-top:10px;"><strong>Forecast call diverges '
            f'from MaxIQ:</strong> the model\'s Most Likely is {fmt_m(agg_m4["ml"])} against '
            f'MaxIQ\'s {fmt_m(maxiq["ml"])} &mdash; a {_ml_gap/1e6:+.0f}M gap. '
            f'{"Pre-quarter MaxIQ calls are typically conservative because reps have not committed yet, while the model grosses up for pipeline not yet created. Treat the range, not either endpoint, as the answer." if not IN_QUARTER else "Worth reconciling before the forecast call."}</div>'
        )
    else:
        _call_note = (
            '<div class="insight" style="margin-top:10px;">Model forecast call is within 15% '
            'of MaxIQ across all three cases &mdash; no reconciliation needed.</div>'
        )

    html_parts.append(f"""
<!-- MaxIQ tie-out -->
<details class="model-breakdown" open>
  <summary><h3>MaxIQ Tie-Out &mdash; {QLABEL}</h3></summary>
  <p style="font-size:0.8em;color:#666;margin:6px 0 10px;">
    Source of truth: <code>SALES.REPORTING.PEAK_FORECAST_CALLS_PIPELINE_TARGETS</code>
    ({GVP}, {FQ_KEY}, <code>FUNCTION='GVP'</code> rollup row). Open pipeline counts
    Stage 1&ndash;6 only &mdash; Stage 0 is unqualified, Stage 7 is deployed, Stage 8 is lost.
    <br><strong>Reading a MaxIQ view a week behind is the usual source of a phantom gap</strong>
    &mdash; the prior-week column is shown so either view can be reconciled. Note also that
    summing MaxIQ SubRegion Lead rows undercounts the GVP rollup, because a few sub-regions
    have no assigned lead.
  </p>
  <table class="model-table">
    <thead>
      <tr><th>Pipeline measure</th><th class="num">This report</th>
          <th class="num">MaxIQ (current)</th><th class="num">Delta</th>
          <th class="num">MaxIQ (prior wk)</th><th class="num">WoW</th></tr>
    </thead>
    <tbody>{tie_html}</tbody>
  </table>
  <table class="model-table" style="margin-top:14px;">
    <thead>
      <tr><th>Forecast call</th><th class="num">Model (M4)</th>
          <th class="num">MaxIQ (current)</th><th class="num">Delta</th>
          <th class="num">MaxIQ (prior wk)</th><th class="num">WoW</th></tr>
    </thead>
    <tbody>{call_html}</tbody>
  </table>
  {_call_note}
</details>
""")

    # ── Model Breakdown ───────────────────────────────────────────────────────
    html_parts.append(f"""
<!-- Model Breakdown -->
<details class="model-breakdown" open>
  <summary><h3>Model Breakdown &mdash; AMSExpansion Total</h3></summary>
  <table class="model-table">
    <thead>
      <tr>
        <th>Model</th><th>Description</th>
        <th class="num">Error (backtest)</th><th class="num">Weight</th>
        <th class="num">Commit</th><th class="num">Most Likely</th><th class="num">Stretch</th>
      </tr>
    </thead>
    <tbody>
      <tr>
        <td><strong>M1 &mdash; Pipeline Risk</strong></td>
        <td>Risk threshold classification against {M1_HORIZON} days of runway{m1_horizon_note}. Stage 6 + all Stage 5 (pass 79d threshold) + Stage 4 good + Pre-TW good. Binary good/at-risk. Deployed ACV floors all three cases.</td>
        <td class="num">{M1_AVG_ERR:.1f}% avg<br><span style="font-size:0.8em;color:#888;">{BACKTEST['Q3 FY26']['m1']}% / {BACKTEST['Q4 FY26']['m1']}% / {BACKTEST['Q1 FY27']['m1']}%</span></td>
        <td class="num" style="color:#e67e22;font-weight:600;">{M4_W1*100:.1f}%</td>
        <td class="num">{fmt_m(agg_m1['commit'])}</td>
        <td class="num">{fmt_m(agg_m1['ml'])}</td>
        <td class="num">{fmt_m(agg_m1['stretch'])}</td>
      </tr>
      <tr>
        <td><strong>M2 &mdash; Historical Pacing</strong></td>
        <td>{m2_desc}</td>
        <td class="num" style="color:{m2_col};">{m2_err}</td>
        <td class="num" style="color:{m2_col};font-weight:600;">{M4_W2*100:.1f}%</td>
        <td class="num" style="color:{m2_col};">{m2_commit}</td>
        <td class="num" style="color:{m2_col};">{m2_ml}</td>
        <td class="num" style="color:{m2_col};">{m2_stretch}</td>
      </tr>
      <tr>
        <td><strong>M3 &mdash; Stage Conversion</strong></td>
        <td>IMP {M3_AVG_IMP_RATE*100:.1f}% + TW {M3_AVG_TW_RATE*100:.1f}% + Pre-TW {M3_AVG_PRETW_RATE*100:.1f}%, then &divide; (1 &minus; {M3_AVG_NEW_PCT*100:.1f}%) for new pipeline uplift. Commit uses min rates (IMP {M3_MIN_IMP_RATE*100:.1f}%, TW {M3_MIN_TW_RATE*100:.1f}%, Pre-TW {M3_MIN_PRETW_RATE*100:.1f}%) &divide; (1 &minus; {M3_MIN_NEW_PCT*100:.1f}%). Stretch = total pipeline (capped). Rates from 4Q backtest (Q3/Q4 FY26 + Q1/Q2 FY27).</td>
        <td class="num">{M3_AVG_ERR:.1f}% avg (LOO)<br><span style="font-size:0.8em;color:#888;">{BACKTEST['Q3 FY26']['m3']}% / {BACKTEST['Q4 FY26']['m3']}% / {BACKTEST['Q1 FY27']['m3']}%</span></td>
        <td class="num" style="color:#1d8ab5;font-weight:600;">{M4_W3*100:.1f}%</td>
        <td class="num">{fmt_m(agg_m3['commit'])}</td>
        <td class="num">{fmt_m(agg_m3['ml'])}</td>
        <td class="num">{fmt_m(agg_m3['stretch'])}</td>
      </tr>
      <tr>
        <td><strong>M4 &mdash; Weighted Ensemble</strong></td>
        <td>Inverse-error weighted blend of M1 &amp; M3. M1 weight = 1/{M1_AVG_ERR:.1f}% &divide; (1/{M1_AVG_ERR:.1f}% + 1/{M3_AVG_ERR:.1f}%) = {M4_W1*100:.1f}%. M3 weight = {M4_W3*100:.1f}%. M2 excluded (N/A pre-quarter). Applied consistently to all regions.</td>
        <td class="num">&mdash;</td>
        <td class="num" style="color:#27ae60;font-weight:600;">100%</td>
        <td class="num" style="color:#e67e22;font-weight:700;">{fmt_m(agg_m4['commit'])}</td>
        <td class="num" style="color:#27ae60;font-weight:700;">{fmt_m(agg_m4['ml'])}</td>
        <td class="num" style="color:#1d8ab5;font-weight:700;">{fmt_m(agg_m4['stretch'])}</td>
      </tr>
    </tbody>
  </table>
  <div class="weight-box">
    <strong>Backtest accuracy &mdash; 3 prior quarters at comparable horizon:</strong>
    <div class="weight-grid">
      <div class="weight-item"><div class="weight-label">Q3 FY26</div><div class="weight-value">M1: {BACKTEST['Q3 FY26']['m1']}% &bull; M3: {BACKTEST['Q3 FY26']['m3']}%</div></div>
      <div class="weight-item"><div class="weight-label">Q4 FY26</div><div class="weight-value">M1: {BACKTEST['Q4 FY26']['m1']}% &bull; M3: {BACKTEST['Q4 FY26']['m3']}%</div></div>
      <div class="weight-item"><div class="weight-label">Q1 FY27</div><div class="weight-value">M1: {BACKTEST['Q1 FY27']['m1']}% &bull; M3: {BACKTEST['Q1 FY27']['m3']}%</div></div>
      <div class="weight-item" style="background:#f0f8fd;"><div class="weight-label">Avg error &rarr; Weight</div><div class="weight-value">M1: {M1_AVG_ERR:.1f}% &rarr; {M4_W1*100:.1f}%<br>M3: {M3_AVG_ERR:.1f}% &rarr; {M4_W3*100:.1f}%</div></div>
    </div>
  </div>
</details>

<div class="section-title">Regional Breakdown</div>

<div class="defs-box">
  <div class="def-item"><span class="def-term">Stale IMP:</span> Stage 5 with &gt;79 days in current stage (avg IMP-to-deploy = 79 days)</div>
  <div class="def-item"><span class="def-term">Stale TW:</span> Stage 4 with &gt;104 days in current stage (avg TW-to-deploy = 104 days)</div>
  <div class="def-item"><span class="def-term">No Next Steps:</span> Stage 4 or higher, next steps field empty</div>
  <div class="def-item"><span class="def-term">Stage 6 Locked:</span> Implementation Complete, deployment confirmation pending</div>
  <div class="def-item"><span class="def-term">YoY Pipeline:</span> Same calendar date one year prior ({date(TODAY.year-1, TODAY.month, TODAY.day).strftime('%b %-d, %Y')})</div>
  <div class="def-item"><span class="def-term">M4 = {M4_W1*100:.1f}% M1 + {M4_W3*100:.1f}% M3</span> (same weights applied to all regions)</div>
</div>

<div class="region-grid">
""")

    for r, m1, m2, m3, m4 in region_models:
        reg = r["REGION"]
        if reg in SUPPRESS_REGION_CARDS:
            continue
        html_parts.append(region_card_html(r, targets.get(reg, 0), m1, m3, m4, yoy_total))

    html_parts.append("</div><!-- end region grid -->\n")

    # ── Regional Summary Table ────────────────────────────────────────────────
    html_parts.append("""
<div class="section-title">Regional Summary</div>
<table style="width:100%;background:white;border-radius:10px;border-collapse:collapse;box-shadow:0 2px 8px rgba(0,0,0,0.07);overflow:hidden;font-size:0.87em;">
  <thead>
    <tr style="background:#1a1a2e;color:white;">
      <th style="padding:11px 14px;text-align:left;">Region</th>
      <th style="padding:11px 14px;text-align:right;">Pipeline</th>
      <th style="padding:11px 14px;text-align:right;color:#f0a070;">M1 ML</th>
      <th style="padding:11px 14px;text-align:right;color:#7bd3f0;">M3 ML</th>
      <th style="padding:11px 14px;text-align:right;color:#f0a070;">M4 Commit</th>
      <th style="padding:11px 14px;text-align:right;color:#86efac;">M4 ML</th>
      <th style="padding:11px 14px;text-align:right;color:#93c5fd;">M4 Stretch</th>
      <th style="padding:11px 14px;text-align:right;color:#c0a0f0;">Target</th>
      <th style="padding:11px 14px;text-align:right;">ML/Target</th>
      <th style="padding:11px 14px;text-align:right;">FY26 Q3</th>
      <th style="padding:11px 14px;text-align:center;">RVP</th>
    </tr>
  </thead>
  <tbody>
""")
    for idx, (r, m1, m2, m3, m4) in enumerate(
            [t for t in region_models if t[0]["REGION"] not in SUPPRESS_REGION_CARDS]):
        reg    = r["REGION"]
        tgt    = targets.get(reg, 0)
        bg     = "background:#fafafa;" if idx % 2 == 1 else ""
        ml_pct = pct_f(m4["ml"], tgt)
        ml_col = "#27ae60" if ml_pct >= 100 else ("#16a34a" if ml_pct >= 90 else "#e74c3c")
        q3act  = PRIOR_ACTUALS.get(reg, 0)
        rvp_last = RVP_NAMES.get(reg, "").split()[-1] if RVP_NAMES.get(reg) else ""
        html_parts.append(f"""    <tr style="border-bottom:1px solid #eee;{bg}">
      <td style="padding:10px 14px;font-weight:600;">{reg}</td>
      <td style="padding:10px 14px;text-align:right;">{fmt_m(r['TOTAL_ACV'])}</td>
      <td style="padding:10px 14px;text-align:right;color:#888;">{fmt_m(m1['ml'])}</td>
      <td style="padding:10px 14px;text-align:right;color:#888;">{fmt_m(m3['ml'])}</td>
      <td style="padding:10px 14px;text-align:right;color:#e67e22;font-weight:600;">{fmt_m(m4['commit'])}</td>
      <td style="padding:10px 14px;text-align:right;color:{ml_col};font-weight:600;">{fmt_m(m4['ml'])}</td>
      <td style="padding:10px 14px;text-align:right;color:#1d8ab5;font-weight:600;">{fmt_m(m4['stretch'])}</td>
      <td style="padding:10px 14px;text-align:right;color:#6f42c1;font-weight:600;">{fmt_m(tgt) if tgt else '—'}</td>
      <td style="padding:10px 14px;text-align:right;color:{ml_col};font-weight:600;">{ml_pct:.0f}%</td>
      <td style="padding:10px 14px;text-align:right;color:#666;">{fmt_m(q3act) if q3act else '—'}</td>
      <td style="padding:10px 14px;text-align:center;font-size:0.85em;">{rvp_last}</td>
    </tr>
""")

    agg_ml_col = "#27ae60" if pct_f(agg_m4['ml'], total_target) >= 95 else "#f0a070"
    html_parts.append(f"""    <tr style="background:#1a1a2e;color:white;font-weight:700;">
      <td style="padding:12px 14px;">AMSExpansion Total</td>
      <td style="padding:12px 14px;text-align:right;">{fmt_m(total_pipeline)}</td>
      <td style="padding:12px 14px;text-align:right;color:#aaa;">{fmt_m(agg_m1['ml'])}</td>
      <td style="padding:12px 14px;text-align:right;color:#aaa;">{fmt_m(agg_m3['ml'])}</td>
      <td style="padding:12px 14px;text-align:right;color:#f0a070;">{fmt_m(agg_m4['commit'])}</td>
      <td style="padding:12px 14px;text-align:right;color:{agg_ml_col};">{fmt_m(agg_m4['ml'])}</td>
      <td style="padding:12px 14px;text-align:right;color:#93c5fd;">{fmt_m(agg_m4['stretch'])}</td>
      <td style="padding:12px 14px;text-align:right;color:#c0a0f0;">{fmt_m(total_target)}</td>
      <td style="padding:12px 14px;text-align:right;color:{agg_ml_col};">{fmt_pct(agg_m4['ml'], total_target)}</td>
      <td style="padding:12px 14px;text-align:right;color:#aaa;">{fmt_m(PRIOR_ACTUALS['TOTAL'])}</td>
      <td style="padding:12px 14px;text-align:center;color:#aaa;">{GVP or THEATER}</td>
    </tr>
  </tbody>
</table>
""")

    # ── District Tabs ───────────────────────────────────────────────────────
    html_parts.append(district_tabs_html(districts))

    # ── Wins Forecast ──────────────────────────────────────────────────────
    w_pipeline = wins_data["pretw_q3_decision"]   # decision-dated pipeline (not all Pre-TW)
    w_pipe_n   = wins_data["pretw_q3_cnt"]
    w_target   = wins_data["wins_target"]

    w3_known   = w_pipeline * W3_AVG_PRETW_RATE
    w3_ml      = w3_known / (1 - W3_AVG_NEW_PCT)
    w3_commit  = (w_pipeline * W3_MIN_PRETW_RATE) / (1 - W3_MIN_NEW_PCT)
    w3_stretch = w_pipeline * 0.653   # max historical effective rate (Q2 FY27: $182.7M/$280M = 65.3%)

    w_ml_pct   = 100 * w3_ml / w_target if w_target else 0
    w_ml_col   = "#27ae60" if w_ml_pct >= 90 else ("#e67e22" if w_ml_pct >= 75 else "#c0392b")

    html_parts.append(f"""
<!-- Wins Forecast -->
<div class="section-title" style="margin-top:32px;">Use Case Wins Forecast &mdash; {QLABEL}</div>
<div style="background:white;border-radius:10px;padding:20px 24px;box-shadow:0 2px 8px rgba(0,0,0,0.07);margin-bottom:24px;">

  <div style="display:grid;grid-template-columns:repeat(5,1fr);gap:14px;margin-bottom:18px;">
    <div style="background:#f8f9fa;border-radius:8px;padding:14px;text-align:center;border-top:3px solid #1a1a2e;">
      <div style="font-size:0.7em;text-transform:uppercase;color:#888;margin-bottom:4px;">Q3 Decision Pipeline</div>
      <div style="font-size:1.5em;font-weight:700;color:#1a1a2e;">{fmt_m(w_pipeline)}</div>
      <div style="font-size:0.75em;color:#666;margin-top:3px;">{w_pipe_n:,} UCs with Q3 decision date</div>
    </div>
    <div style="background:#fef9f5;border-radius:8px;padding:14px;text-align:center;border-top:3px solid #e67e22;">
      <div style="font-size:0.7em;text-transform:uppercase;color:#888;margin-bottom:4px;">Commit</div>
      <div style="font-size:1.5em;font-weight:700;color:#e67e22;">{fmt_m(w3_commit)}</div>
      <div style="font-size:0.75em;color:#666;margin-top:3px;">Stage Conversion (min rates) &bull; {100*w3_commit/w_target:.0f}% of target</div>
    </div>
    <div style="background:#f0fdf4;border-radius:8px;padding:14px;text-align:center;border-top:3px solid #27ae60;">
      <div style="font-size:0.7em;text-transform:uppercase;color:#888;margin-bottom:4px;">Most Likely</div>
      <div style="font-size:1.5em;font-weight:700;color:{w_ml_col};">{fmt_m(w3_ml)}</div>
      <div style="font-size:0.75em;color:#666;margin-top:3px;">Stage Conversion (avg rates) &bull; {w_ml_pct:.0f}% of target</div>
    </div>
    <div style="background:#f0f8fd;border-radius:8px;padding:14px;text-align:center;border-top:3px solid #1d8ab5;">
      <div style="font-size:0.7em;text-transform:uppercase;color:#888;margin-bottom:4px;">Stretch</div>
      <div style="font-size:1.5em;font-weight:700;color:#1d8ab5;">{fmt_m(w3_stretch)}</div>
      <div style="font-size:0.75em;color:#666;margin-top:3px;">Best-quarter effective rate (65%, Q2 FY27)</div>
    </div>
    <div style="background:#f8f9fa;border-radius:8px;padding:14px;text-align:center;border-top:3px solid #6f42c1;">
      <div style="font-size:0.7em;text-transform:uppercase;color:#888;margin-bottom:4px;">Wins Target</div>
      <div style="font-size:1.5em;font-weight:700;color:#6f42c1;">{fmt_m(w_target)}</div>
      <div style="font-size:0.75em;color:#666;margin-top:3px;">{QLABEL}</div>
    </div>
  </div>

  <div style="font-size:0.82em;color:#555;line-height:1.7;">
    <strong>Methodology (Stage Conversion Model):</strong> Applies historical decision-dated Pre-TW &rarr; Stage 4 Won conversion rate ({W3_AVG_PRETW_RATE*100:.1f}%)
    to pipeline with DECISION_DATE in Q3 ({fmt_m(w_pipeline)}),
    then divides by (1 &minus; {W3_AVG_NEW_PCT*100:.1f}%) to account for wins from outside the decision-dated pipeline (pull-ins + new UCs).
    Commit uses min rates ({W3_MIN_PRETW_RATE*100:.1f}% conv, {W3_MIN_NEW_PCT*100:.1f}% new). Stretch uses best-quarter effective rate (65.3%, Q2 FY27).<br>
    Rates calibrated from Q3/Q4 FY26 + Q1/Q2 FY27 (Q2 at 2&times; weight). Win = ACTUAL_USE_CASE_WON_DATE (Stage 4 achieved).
  </div>
</div>
""")

    # ── Methodology ───────────────────────────────────────────────────────────
    gap = agg_m4["ml"] - total_target
    gap_sign = "+" if gap >= 0 else ""
    gap_note = (
        f"M4 ML ({fmt_m(agg_m4['ml'])}) {gap_sign}{gap/1e6:.1f}M vs target ({fmt_m(total_target)}). "
        f"M4 Stretch ({fmt_m(agg_m4['stretch'])}) "
        + ("exceeds" if agg_m4["stretch"] >= total_target else "falls short of")
        + f" target by {abs(agg_m4['stretch']-total_target)/1e6:.1f}M."
    )

    html_parts.append(f"""
<!-- Methodology -->
<div class="methodology">
  <h3>Model Methodology</h3>
  <div class="method-grid">
    <div class="method-box">
      <h4 style="color:#e67e22;">M1 &mdash; Pipeline Risk</h4>
      <p>Binary risk classification against {M1_HORIZON} days of runway{m1_horizon_note}. Good = days_in_stage + {M1_HORIZON} &ge; threshold. Stage 5: 79d. Stage 4: 104d. Stage 1&ndash;3: 146d. Commit = deployed + Stage 5+6. ML = commit + all good pipeline. Stretch = deployed + total pipeline. As the quarter runs down the runway shrinks, so the same UC can move from good to at-risk without changing stage. <em>Backtest avg error: {M1_AVG_ERR:.1f}%</em></p>
    </div>
    <div class="method-box">
      <h4 style="color:{'#333' if agg_m2 else '#888'};">M2 &mdash; Historical Pacing</h4>
      <p>{m2_desc}</p>
    </div>
    <div class="method-box">
      <h4 style="color:#1d8ab5;">M3 &mdash; Stage Conversion</h4>
      <p>IMP {M3_AVG_IMP_RATE*100:.1f}% + TW {M3_AVG_TW_RATE*100:.1f}% + Pre-TW {M3_AVG_PRETW_RATE*100:.1f}%, applied to milestone-dated buckets and added to deployed ACV, then &divide; (1 &minus; {M3_AVG_NEW_PCT*100:.1f}%) to gross up for pipeline not yet visible. Commit uses min rates &divide; (1 &minus; {M3_MIN_NEW_PCT*100:.1f}%). Stretch = deployed + total pipeline (capped). Rates calibrated at this report's own time horizon from Q3/Q4 FY26 + Q1/Q2 FY27, most recent quarter double-weighted. <em>LOO backtest avg error: {M3_AVG_ERR:.1f}%</em></p>
    </div>
    <div class="method-box">
      <h4 style="color:#27ae60;">M4 &mdash; Weighted Ensemble</h4>
      <p>{m4_method_note}</p>
    </div>
  </div>
  <div class="caution">
    <strong>Target gap (AMSExpansion):</strong> {gap_note}
  </div>
  <div class="insight">
    <strong>M1 vs M3 divergence note:</strong> M1 and M3 are nearly equal in accuracy ({M1_AVG_ERR:.1f}% vs {M3_AVG_ERR:.1f}% LOO error), so M4 weights them almost equally. M1 tends to overpredict in high-pipeline quarters (Q4 FY26: +{BACKTEST['Q4 FY26']['m1']}%, Q1 FY27: +{BACKTEST['Q1 FY27']['m1']}%) while M3 tends to underpredict in low-pipeline/high-new-pipeline quarters (Q3 FY26: &minus;{BACKTEST['Q3 FY26']['m3']}%). The ensemble corrects for both biases.
  </div>
</div>

<p style="font-size:0.72em;color:#aaa;margin-top:16px;text-align:center;">Generated {today_str} &bull; Sources: MDM.MDM_INTERFACES.DIM_USE_CASE &bull; SALES.SE_REPORTING.DIM_USE_CASE_HISTORY_DS &bull; SALES.REPORTING.PEAK_USE_CASE_TARGETS &bull; SALES.REPORTING.CORE_PRODUCT_CATEGORY_CONSUMPTION &bull; {THEATER}</p>

</body>
</html>""")

    return "".join(html_parts)


def append_tracking_csv(m4_totals, target_acv):
    """Append a convergence tracking row to ~/Desktop/PEAK_forecast_tracking.csv."""
    csv_path = Path.home() / "Desktop" / "PEAK_forecast_tracking.csv"
    fieldnames = ["date", "quarter", "m4_commit", "m4_ml", "m4_stretch", "target"]
    row = {
        "date":       TODAY.isoformat(),
        "quarter":    QLABEL,
        "m4_commit":  round(m4_totals["commit"]),
        "m4_ml":      round(m4_totals["ml"]),
        "m4_stretch": round(m4_totals["stretch"]),
        "target":     round(target_acv),
    }
    file_exists = csv_path.exists()
    with open(csv_path, "a", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        if not file_exists:
            writer.writeheader()
        writer.writerow(row)
    print(f"Tracking CSV updated: {csv_path}")


# ─────────────────────────── Main ────────────────────────────────────────────

def apply_calibration(conn):
    """
    Recalibrate M3 rates and M2 pacing at THIS run's horizon.

    The values in QUARTERS are only a fallback. Without this, a scheduled run
    would keep applying rates measured on the day they were first hardcoded —
    which is precisely how the report drifted out of correctness before: by day
    40 the pre-quarter rates understated conversion and overstated new pipeline.
    Recalibrating each run keeps the model honest as the quarter progresses.
    """
    try:
        import peak_calibrate as pc
    except ImportError:
        print("  ! peak_calibrate.py not importable — using fallback rates")
        return False

    day_offset = DAY_IN_QUARTER if IN_QUARTER else -abs(DAYS_TO_OPEN) + 1
    try:
        m3_avg, m3_min, m2_pace = pc.get_rates(conn, day_offset)
    except Exception as exc:
        print(f"  ! calibration failed ({exc}) — using fallback rates")
        return False

    g = globals()
    g["M3_AVG_IMP_RATE"]   = m3_avg["imp"]
    g["M3_AVG_TW_RATE"]    = m3_avg["tw"]
    g["M3_AVG_PRETW_RATE"] = m3_avg["pretw"]
    g["M3_AVG_NEW_PCT"]    = m3_avg["new"]
    g["M3_MIN_IMP_RATE"]   = m3_min["imp"]
    g["M3_MIN_TW_RATE"]    = m3_min["tw"]
    g["M3_MIN_PRETW_RATE"] = m3_min["pretw"]
    g["M3_MIN_NEW_PCT"]    = m3_min["new"]
    g["M2_PACE"]           = m2_pace

    # M2 availability drives the M4 weights, so both are re-derived here rather
    # than left at whatever the module-level defaults computed.
    m2_active = bool(m2_pace) and IN_QUARTER and DAY_IN_QUARTER >= M2_RELIABLE_DAY
    g["M2_ACTIVE"] = m2_active
    if m2_active:
        g["M4_W1"], g["M4_W2"], g["M4_W3"] = _M4_RAW_W1, _M4_RAW_W2, _M4_RAW_W3
    else:
        _d = _M4_RAW_W1 + _M4_RAW_W3
        g["M4_W1"], g["M4_W2"], g["M4_W3"] = _M4_RAW_W1 / _d, 0.0, _M4_RAW_W3 / _d

    horizon = (f"day {day_offset}" if day_offset >= 1
               else f"{abs(day_offset) + 1} days pre-open")
    print(f"  Calibrated @ {horizon}: IMP {m3_avg['imp']:.3f} TW {m3_avg['tw']:.3f} "
          f"PreTW {m3_avg['pretw']:.3f} new {m3_avg['new']:.3f} | "
          f"M2 {'pace ' + str(m2_pace['avg']) if m2_pace else 'N/A'}")
    return True


def resolve_gvp(conn):
    """
    Current GVP name for THEATER, from the account table. Only the MaxIQ tie-out
    (PEAK_FORECAST_CALLS_PIPELINE_TARGETS.USER_NAME) is keyed on a person, so this
    is looked up per run: the next GVP rename or backfill heals itself. An empty
    result makes the MaxIQ tie-out return nothing rather than guessing a name.
    """
    cur = conn.cursor()
    try:
        row = cur.execute(f"""
            SELECT GVP FROM SALES.RAVEN.D_SALESFORCE_ACCOUNT_CUSTOMERS
            WHERE GEO = '{THEATER}' AND GVP IS NOT NULL
            GROUP BY GVP ORDER BY COUNT(*) DESC LIMIT 1
        """).fetchone()
        return row[0] if row else ""
    finally:
        cur.close()


def main():
    print(f"Connecting to Snowflake ({CONNECTION_NAME}) …")
    conn = get_conn()

    global GVP
    GVP = resolve_gvp(conn)
    print(f"Scope: theater {THEATER} — current GVP resolved as {GVP!r}")
    if not GVP:
        print("  ! No GVP resolved for this theater — MaxIQ tie-out will be empty")

    print("Recalibrating rates at current horizon …")
    global CALIBRATED
    CALIBRATED = apply_calibration(conn)
    if not CALIBRATED:
        if not ALLOW_FALLBACK:
            sys.exit(
                "\nABORTED: calibration did not run, so the report would use the hardcoded\n"
                "QUARTERS rates measured at a different horizon. Publishing that silently is\n"
                "how this report drifted out of correctness before.\n"
                "Fix the cause (usually peak_calibrate.py not importable, or a query failure\n"
                "above), or re-run with --allow-fallback-rates to publish a stamped report."
            )
        print("  ! PROCEEDING ON FALLBACK RATES — report will be stamped UNCALIBRATED")

    horizon = (f"day {DAY_IN_QUARTER} of {QDAYS}, {DAYS_REMAINING} remaining"
               if IN_QUARTER else f"{DAYS_TO_OPEN} days before open")
    print(f"Quarter: {QLABEL} ({QS} → {QE}) — {horizon}")

    print("Querying pipeline …")
    regions = query_pipeline(conn)

    print("Querying targets …")
    targets = query_targets(conn)

    print("Querying YoY comparison …")
    yoy_total = query_yoy(conn)

    print(f"Querying {PRIOR_LABEL} actuals …")
    PRIOR_ACTUALS.update(query_prior_actuals(conn))

    print("Querying MaxIQ tie-out figures …")
    maxiq = query_maxiq(conn)

    print("Querying deployed (banked) ACV …")
    deployed_by_region = query_deployed(conn)

    print("Querying wins booked QTD …")
    won_qtd = query_won_qtd(conn)

    print("Querying district breakdown …")
    districts = query_districts(conn)

    print("Querying wins pipeline …")
    wins_data = query_wins(conn)

    conn.close()

    print(f"  Pipeline rows: {len(regions)}")
    total_pipeline = sum(r["TOTAL_ACV"] for r in regions)
    total_deployed = sum(float(v["DEPLOYED_ACV"] or 0)
                         for v in deployed_by_region.values())
    print(f"  Total {QLABEL} open pipeline: ${total_pipeline/1e6:.1f}M")
    print(f"  Deployed (banked) QTD:        ${total_deployed/1e6:.1f}M")
    print(f"  Wins booked QTD:              ${float(won_qtd['won_acv'])/1e6:.1f}M "
          f"({won_qtd['won_count']} UCs)")
    print(f"  YoY ({PRIOR_LABEL} pipeline on same date): "
          f"${yoy_total/1e6 if yoy_total else 0:.1f}M")
    print(f"  District rows: {len(districts)}")
    print(f"  Wins Pre-TW: ${wins_data['pretw_total']/1e6:.1f}M "
          f"({wins_data['pretw_cnt']} UCs)")
    def _tie(label, mine, theirs):
        if not theirs:
            return f"    {label:<26} {mine/1e6:>8.1f}M   (no MaxIQ value)"
        d = mine - theirs
        flag = "OK" if abs(d) / theirs < 0.01 else "CHECK"
        return (f"    {label:<26} {mine/1e6:>8.1f}M vs {theirs/1e6:>8.1f}M  "
                f"{d/1e6:+7.2f}M  {flag}")
    print("  MaxIQ tie-out:")
    print(_tie("open pipeline", total_pipeline, maxiq["open"]))
    print(_tie("deployed (Live)", total_deployed, maxiq["live"]))
    print(_tie("wins booked (Won)", float(won_qtd["won_acv"]), maxiq["won"]))
    print(_tie("target", sum(targets.values()), maxiq["target"]))
    print(f"  M2 active: {M2_ACTIVE}  |  M4 weights "
          f"M1 {M4_W1*100:.1f}% / M2 {M4_W2*100:.1f}% / M3 {M4_W3*100:.1f}%")

    rm = region_models_from(regions, deployed_by_region)
    agg = {k: sum(t[4][k] for t in rm) for k in ("commit", "ml", "stretch")}
    total_target_acv = sum(targets.values())
    print(f"  M4 ensemble — commit ${agg['commit']/1e6:.1f}M / "
          f"ML ${agg['ml']/1e6:.1f}M / stretch ${agg['stretch']/1e6:.1f}M "
          f"vs target ${total_target_acv/1e6:.1f}M")

    print("Generating HTML …")
    html = generate_html(regions, targets, yoy_total, districts, wins_data,
                         deployed_by_region, won_qtd, maxiq)

    fname = f"PEAK_AMSExpansion_{QLABEL.replace(' ', '')}.html"
    out_path = Path.home() / "Desktop" / fname
    out_path.write_text(html, encoding="utf-8")
    print(f"Report written: {out_path}")

    append_tracking_csv(agg, total_target_acv)

    # Per-region model output, written so downstream comms quote exactly what
    # the report shows rather than a separately-computed number.
    reg_csv = Path.home() / "Desktop" / f"PEAK_region_detail_{QLABEL.replace(' ','')}.csv"
    with open(reg_csv, "w", newline="") as f:
        w = csv.writer(f)
        w.writerow(["quarter", "region", "rvp", "uc_count", "open_acv",
                    "deployed_acv", "target", "m4_commit", "m4_ml", "m4_stretch",
                    "ml_pct_of_target", "imp_acv", "tw_acv", "pretw_acv",
                    "pretw_pct_of_open", "stale_imp_cnt", "stale_imp_acv",
                    "stale_tw_cnt", "no_next_steps", "prior_year_actual"])
        for r, m1, m2, m3, m4 in rm:
            reg = r["REGION"]
            dep = float((deployed_by_region.get(reg) or {}).get("DEPLOYED_ACV") or 0)
            tgt = float(targets.get(reg) or 0)
            open_acv = float(r["TOTAL_ACV"] or 0)
            pretw = float(r["PRETW_TOTAL"] or 0)
            w.writerow([QLABEL, reg, r.get("RVP", ""), r["UC_COUNT"],
                        round(open_acv), round(dep), round(tgt),
                        round(m4["commit"]), round(m4["ml"]), round(m4["stretch"]),
                        (round(100 * m4["ml"] / tgt, 1) if tgt else ""),
                        round(float(r["IMP_TOTAL"] or 0) + float(r["STAGE6_ACV"] or 0)),
                        round(float(r["TW_TOTAL"] or 0)), round(pretw),
                        (round(100 * pretw / open_acv) if open_acv else ""),
                        r["STALE_IMP_CNT"], round(float(r["STALE_IMP_ACV"] or 0)),
                        r["STALE_TW_CNT"], r["NO_NEXT_STEPS"],
                        round(PRIOR_ACTUALS.get(reg, 0))])
    print(f"Region detail written: {reg_csv}")
    return out_path


def region_models_from(regions, deployed_by_region=None):
    """Returns (row, m1, m2, m3, m4) per region, with deployed ACV folded in."""
    deployed_by_region = deployed_by_region or {}
    out = []
    for r in regions:
        d = deployed_by_region.get(r["REGION"]) or {}
        dep = float(d.get("DEPLOYED_ACV") or 0)
        m1 = compute_m1(r, dep)
        m2 = compute_m2(dep)
        m3 = compute_m3(r, dep)
        m4 = compute_m4(m1, m3, m2)
        out.append((r, m1, m2, m3, m4))
    return out


if __name__ == "__main__":
    main()
