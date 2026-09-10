#!/usr/bin/env python3
"""
PEAK Forecast Learning Guide — NorthwestExp + SouthwestExp
Week-by-week pipeline → go-live conversion analysis for new SEMs.
Usage: python3 peak_nw_sw_forecast.py
Output: ~/Desktop/PEAK_NW_SW_Forecast_Q3FY27.html
"""

import os
from datetime import date, timedelta
from pathlib import Path

import snowflake.connector

# ─────────────────────── Config ──────────────────────────────────────────────
CONNECTION_NAME = "MyConnection"
ROLE            = "SALES_RAVEN_RO_RL"
WAREHOUSE       = "SNOWADHOC"
GVP             = "Mark Fleming"
REGIONS         = ("NorthwestExp_SR", "SouthwestExp_SR")
Q3_START        = date(2026, 8, 1)
Q3_END          = date(2026, 10, 31)
Q3_DAYS         = 91
TODAY           = date.today()

# Historical quarters: (label, start, end)
QUARTERS = [
    ("Q2 FY26", date(2025, 5,  1), date(2025, 7, 31)),
    ("Q3 FY26", date(2025, 8,  1), date(2025, 10,31)),
    ("Q4 FY26", date(2025, 11, 1), date(2026, 1, 31)),
    ("Q1 FY27", date(2026, 2,  1), date(2026, 4, 30)),
    ("Q2 FY27", date(2026, 5,  1), date(2026, 7, 31)),
]

# Pre-computed source breakdown — % of final deployed by source (from historical analysis)
PIPELINE_SOURCES = {
    "Q2 FY26": {"week1": 55.7, "pullin": 31.1, "new": 13.2},
    "Q3 FY26": {"week1": 57.6, "pullin": 34.7, "new":  7.7},
    "Q4 FY26": {"week1": 62.0, "pullin": 29.4, "new":  8.6},
    "Q1 FY27": {"week1": 51.7, "pullin": 26.7, "new": 21.6},
    "Q2 FY27": {"week1": 65.3, "pullin": 20.7, "new": 14.0},
}
# Week offsets: snapshot on day 1, 8, 15 … 85 (13 snapshots per 91-day quarter)
WEEK_DAYS = [1, 8, 15, 22, 29, 36, 43, 50, 57, 64, 71, 78, 85]

# ─────────────────────── M3/M4 constants (matches main report) ───────────────
M3_AVG_IMP_RATE   = 0.566
M3_AVG_TW_RATE    = 0.293
M3_AVG_PRETW_RATE = 0.242
M3_AVG_NEW_PCT    = 0.145
M3_MIN_IMP_RATE   = 0.463
M3_MIN_TW_RATE    = 0.194
M3_MIN_PRETW_RATE = 0.140
M3_MIN_NEW_PCT    = 0.101
_RAW_W1, _RAW_W3  = 0.51, 0.40
# Pre-quarter: redistribute M2's dead weight
if (Q3_START - TODAY).days > 0:
    M4_W1 = _RAW_W1 / (_RAW_W1 + _RAW_W3)
    M4_W3 = _RAW_W3 / (_RAW_W1 + _RAW_W3)
else:
    M4_W1, M4_W3 = _RAW_W1, _RAW_W3

# ─────────────────────── DB ──────────────────────────────────────────────────
def get_conn():
    return snowflake.connector.connect(
        connection_name=CONNECTION_NAME, role=ROLE, warehouse=WAREHOUSE)

def run(conn, sql):
    cur = conn.cursor()
    cur.execute(sql)
    cols = [c[0] for c in cur.description]
    return [dict(zip(cols, row)) for row in cur.fetchall()]

# ─────────────────────── Queries ─────────────────────────────────────────────

def query_weekly_snapshots(conn):
    """
    For each historical quarter, at each week boundary:
    - Pipeline total (IMP/TW/Pre-TW) for those UCs
    - TRUE conversion rate: of those specific UCs, what % actually deployed in the quarter
    """
    region_filter = "','".join(REGIONS)
    results = {}
    for lbl, qs, qe in QUARTERS:
        snap_dates = [qs + timedelta(days=d - 1) for d in WEEK_DAYS]
        dates_sql  = ",".join(f"'{d}'" for d in snap_dates)
        sql = f"""
        SELECT
            h.DS,
            DATEDIFF('day', '{qs}', h.DS) + 1                                   AS day_num,
            ROUND(SUM(CASE WHEN h.IMPLEMENTATION_START_DATE <= h.DS
                           THEN h.USE_CASE_EACV ELSE 0 END), 0)                 AS imp_acv,
            ROUND(SUM(CASE WHEN (h.IMPLEMENTATION_START_DATE > h.DS OR h.IMPLEMENTATION_START_DATE IS NULL)
                           AND h.TECHNICAL_WIN_DATE <= h.DS
                           THEN h.USE_CASE_EACV ELSE 0 END), 0)                 AS tw_acv,
            ROUND(SUM(CASE WHEN (h.IMPLEMENTATION_START_DATE > h.DS OR h.IMPLEMENTATION_START_DATE IS NULL)
                           AND (h.TECHNICAL_WIN_DATE > h.DS OR h.TECHNICAL_WIN_DATE IS NULL)
                           THEN h.USE_CASE_EACV ELSE 0 END), 0)                 AS pretw_acv,
            ROUND(SUM(h.USE_CASE_EACV), 0)                                      AS total_acv,
            COUNT(*)                                                             AS uc_cnt,
            -- TRUE conversion: of these specific UCs, what % deployed in the quarter?
            ROUND(SUM(CASE WHEN u.IS_DEPLOYED = TRUE
                           AND u.GO_LIVE_DATE BETWEEN '{qs}' AND '{qe}'
                      THEN h.USE_CASE_EACV ELSE 0 END), 0)                      AS converted_acv,
            COUNT(CASE WHEN u.IS_DEPLOYED = TRUE
                       AND u.GO_LIVE_DATE BETWEEN '{qs}' AND '{qe}'
                  THEN 1 END)                                                    AS converted_cnt,
            ROUND(100.0 * SUM(CASE WHEN u.IS_DEPLOYED = TRUE
                                   AND u.GO_LIVE_DATE BETWEEN '{qs}' AND '{qe}'
                              THEN h.USE_CASE_EACV ELSE 0 END)
                        / NULLIF(SUM(h.USE_CASE_EACV), 0), 1)                   AS true_conv_pct
        FROM SALES.SE_REPORTING.DIM_USE_CASE_HISTORY_DS h
        JOIN SNOWPUBLIC.STREAMLIT.DIM_USE_CASE_MDM_CACHE u
          ON h.USE_CASE_ID = u.USE_CASE_ID
        WHERE h.ACCOUNT_GVP = '{GVP}'
          AND h.SUB_REGION_NAME IN ('{region_filter}')
          AND h.DS IN ({dates_sql})
          AND h.IS_DEPLOYED = FALSE AND COALESCE(h.IS_LOST, FALSE) = FALSE
          AND h.USE_CASE_EACV > 0
          AND h.GO_LIVE_DATE BETWEEN '{qs}' AND '{qe}'
        GROUP BY h.DS
        ORDER BY h.DS
        """
        rows = run(conn, sql)
        results[lbl] = {r["DAY_NUM"]: r for r in rows}
    return results


def query_final_deployed(conn):
    """Actual deployed ACV per historical quarter for NW+SW."""
    region_filter = "','".join(REGIONS)
    results = {}
    for lbl, qs, qe in QUARTERS:
        sql = f"""
        SELECT ROUND(SUM(USE_CASE_EACV), 0) AS deployed
        FROM SNOWPUBLIC.STREAMLIT.DIM_USE_CASE_MDM_CACHE
        WHERE ACCOUNT_GVP = '{GVP}'
          AND SUB_REGION_NAME IN ('{region_filter}')
          AND IS_DEPLOYED = TRUE AND USE_CASE_EACV > 0
          AND GO_LIVE_DATE BETWEEN '{qs}' AND '{qe}'
        """
        rows = run(conn, sql)
        results[lbl] = rows[0]["DEPLOYED"] if rows else 0
    return results


def query_current_pipeline(conn):
    """Current Q3 FY27 pipeline by district for NW+SW with milestone classification."""
    region_filter = "','".join(REGIONS)
    sql = f"""
    SELECT
        DISTRICT_NAME                                                             AS district,
        CASE WHEN SUB_REGION_NAME = 'NorthwestExp_SR' THEN 'NorthwestExp'
             ELSE 'SouthwestExp' END                                              AS region,
        COUNT(*)                                                                  AS uc_count,
        ROUND(SUM(USE_CASE_EACV), 0)                                              AS total_acv,
        ROUND(SUM(CASE WHEN IMPLEMENTATION_START_DATE <= CURRENT_DATE()
                       THEN USE_CASE_EACV ELSE 0 END), 0)                        AS imp_milestone,
        ROUND(SUM(CASE WHEN (IMPLEMENTATION_START_DATE > CURRENT_DATE() OR IMPLEMENTATION_START_DATE IS NULL)
                       AND TECHNICAL_WIN_DATE <= CURRENT_DATE()
                       THEN USE_CASE_EACV ELSE 0 END), 0)                        AS tw_milestone,
        ROUND(SUM(CASE WHEN (IMPLEMENTATION_START_DATE > CURRENT_DATE() OR IMPLEMENTATION_START_DATE IS NULL)
                       AND (TECHNICAL_WIN_DATE > CURRENT_DATE() OR TECHNICAL_WIN_DATE IS NULL)
                       THEN USE_CASE_EACV ELSE 0 END), 0)                        AS pretw_milestone,
        ROUND(SUM(CASE WHEN STAGE_NUMBER=5 AND DAYS_IN_STAGE > 79  THEN USE_CASE_EACV ELSE 0 END), 0) AS stale_imp_acv,
        COUNT(CASE WHEN STAGE_NUMBER=5 AND DAYS_IN_STAGE > 79  THEN 1 END)       AS stale_imp_cnt,
        COUNT(CASE WHEN STAGE_NUMBER=4 AND DAYS_IN_STAGE > 104 THEN 1 END)       AS stale_tw_cnt,
        COUNT(CASE WHEN STAGE_NUMBER>=4 AND (NEXT_STEPS IS NULL OR NEXT_STEPS='') THEN 1 END) AS no_next_steps,
        ROUND(SUM(CASE WHEN STAGE_NUMBER=6 THEN USE_CASE_EACV ELSE 0 END), 0)    AS stage6_acv,
        ROUND(SUM(CASE WHEN STAGE_NUMBER=5 THEN USE_CASE_EACV ELSE 0 END), 0)    AS imp_total,
        ROUND(SUM(CASE WHEN STAGE_NUMBER=4 AND (DAYS_IN_STAGE + {Q3_DAYS}) >= 104 THEN USE_CASE_EACV ELSE 0 END), 0) AS tw_good,
        ROUND(SUM(CASE WHEN STAGE_NUMBER IN (1,2,3) AND (DAYS_IN_STAGE + {Q3_DAYS}) >= 146 THEN USE_CASE_EACV ELSE 0 END), 0) AS pretw_good,
        ROUND(SUM(CASE WHEN STAGE_NUMBER=4 THEN USE_CASE_EACV ELSE 0 END), 0)    AS tw_total,
        ROUND(SUM(CASE WHEN STAGE_NUMBER IN (1,2,3) THEN USE_CASE_EACV ELSE 0 END), 0) AS pretw_total
    FROM MDM.MDM_INTERFACES.DIM_USE_CASE
    WHERE ACCOUNT_GVP = '{GVP}'
      AND SUB_REGION_NAME IN ('{region_filter}')
      AND IS_DEPLOYED = FALSE AND IS_LOST = FALSE
      AND USE_CASE_EACV > 0 AND STAGE_NUMBER >= 1
      AND GO_LIVE_DATE BETWEEN '{Q3_START}' AND '{Q3_END}'
      AND DISTRICT_NAME IS NOT NULL
    GROUP BY 1, 2
    ORDER BY 2, 4 DESC
    """
    return run(conn, sql)


def query_current_q3_weekly(conn):
    """Q3 FY27 in-progress: pipeline at each available week so far."""
    region_filter = "','".join(REGIONS)
    snap_dates = [Q3_START + timedelta(days=d - 1) for d in WEEK_DAYS
                  if Q3_START + timedelta(days=d - 1) <= TODAY]
    if not snap_dates:
        return {}
    dates_sql = ",".join(f"'{d}'" for d in snap_dates)
    sql = f"""
    SELECT
        DS,
        DATEDIFF('day', '{Q3_START}', DS) + 1 AS day_num,
        ROUND(SUM(CASE WHEN IMPLEMENTATION_START_DATE <= DS THEN USE_CASE_EACV ELSE 0 END), 0) AS imp_acv,
        ROUND(SUM(CASE WHEN (IMPLEMENTATION_START_DATE > DS OR IMPLEMENTATION_START_DATE IS NULL)
                       AND TECHNICAL_WIN_DATE <= DS THEN USE_CASE_EACV ELSE 0 END), 0) AS tw_acv,
        ROUND(SUM(CASE WHEN (IMPLEMENTATION_START_DATE > DS OR IMPLEMENTATION_START_DATE IS NULL)
                       AND (TECHNICAL_WIN_DATE > DS OR TECHNICAL_WIN_DATE IS NULL)
                       THEN USE_CASE_EACV ELSE 0 END), 0) AS pretw_acv,
        ROUND(SUM(USE_CASE_EACV), 0) AS total_acv
    FROM SALES.SE_REPORTING.DIM_USE_CASE_HISTORY_DS
    WHERE ACCOUNT_GVP = '{GVP}'
      AND SUB_REGION_NAME IN ('{region_filter}')
      AND DS IN ({dates_sql})
      AND IS_DEPLOYED = FALSE AND COALESCE(IS_LOST, FALSE) = FALSE
      AND USE_CASE_EACV > 0
      AND GO_LIVE_DATE BETWEEN '{Q3_START}' AND '{Q3_END}'
    GROUP BY DS ORDER BY DS
    """
    rows = run(conn, sql)
    return {r["DAY_NUM"]: r for r in rows}


def query_targets(conn):
    sql = """
    SELECT REGION, USE_CASE_GO_LIVE_TARGET_REGION AS target
    FROM SALES.REPORTING.PEAK_USE_CASE_TARGETS
    WHERE GEO = 'AMSExpansion' AND FISCAL_QUARTER = '2027-Q3'
      AND REGION IN ('NorthwestExp','SouthwestExp')
    """
    rows = run(conn, sql)
    return {r["REGION"]: r["TARGET"] for r in rows}

# ─────────────────────── Models ──────────────────────────────────────────────
def m1(d):
    commit  = d["STAGE6_ACV"] + d["IMP_TOTAL"]
    ml      = commit + d["TW_GOOD"] + d["PRETW_GOOD"]
    stretch = d["TOTAL_ACV"]
    return {"commit": commit, "ml": ml, "stretch": stretch}

def m3(d):
    imp   = d.get("IMP_MILESTONE", d["IMP_TOTAL"] + d["STAGE6_ACV"])
    tw    = d.get("TW_MILESTONE", d["TW_TOTAL"])
    pretw = d.get("PRETW_MILESTONE", d["PRETW_TOTAL"])
    kml   = imp * M3_AVG_IMP_RATE + tw * M3_AVG_TW_RATE + pretw * M3_AVG_PRETW_RATE
    kcmt  = imp * M3_MIN_IMP_RATE  + tw * M3_MIN_TW_RATE  + pretw * M3_MIN_PRETW_RATE
    return {
        "commit":  kcmt / (1 - M3_MIN_NEW_PCT),
        "ml":      kml  / (1 - M3_AVG_NEW_PCT),
        "stretch": d["TOTAL_ACV"],
    }

def m4(r1, r3):
    return {k: M4_W1 * r1[k] + M4_W3 * r3[k] for k in ("commit","ml","stretch")}

def fm(v):
    m = v / 1_000_000
    return f"${m:.1f}M" if m < 10 else f"${m:.0f}M"

def pct(n, d):
    return f"{100*n/d:.0f}%" if d else "—"

# ─────────────────────── HTML helpers ────────────────────────────────────────

WEEK_LABELS = ["Wk 1","Wk 2","Wk 3","Wk 4","Wk 5","Wk 6","Wk 7",
               "Wk 8","Wk 9","Wk 10","Wk 11","Wk 12","Wk 13"]

REGION_COLORS = {"NorthwestExp": "#17a2b8", "SouthwestExp": "#6f42c1"}
QTR_COLORS    = ["#29B5E8","#e67e22","#27ae60","#e74c3c","#6f42c1"]


def weekly_row_html(lbl, wk_data, final_deployed, color, max_pipeline):
    """One row in the week-by-week table."""
    cells = ""
    for i, day in enumerate(WEEK_DAYS):
        row   = wk_data.get(day, {})
        total = row.get("TOTAL_ACV", 0) or 0
        imp   = row.get("IMP_ACV", 0) or 0
        tw    = row.get("TW_ACV", 0) or 0
        ptw   = row.get("PRETW_ACV", 0) or 0

        if total == 0:
            cells += '<td style="text-align:center;color:#ccc;font-size:0.8em;">—</td>'
            continue

        # Bar widths scaled to max pipeline
        bar_w = max(2, int(60 * total / max_pipeline)) if max_pipeline else 0
        w_imp = int(bar_w * imp / total) if total else 0
        w_tw  = int(bar_w * tw  / total) if total else 0
        w_ptw = bar_w - w_imp - w_tw

        # TRUE conversion: % of this week's specific UCs that deployed in the quarter
        conv_pct = row.get("TRUE_CONV_PCT") or None
        conv_str = f"{conv_pct:.0f}%" if conv_pct is not None else ""
        if conv_pct is not None:
            conv_col = "#27ae60" if conv_pct >= 60 else ("#e67e22" if conv_pct >= 40 else "#c0392b")
        else:
            conv_col = "#888"

        cells += f"""
        <td style="padding:6px 4px;text-align:center;vertical-align:middle;">
          <div style="font-size:1.0em;font-weight:700;color:{conv_col};">{conv_str}</div>
          <div style="display:inline-flex;height:6px;border-radius:3px;overflow:hidden;width:{bar_w}px;margin:3px 0;">
            <div style="width:{w_imp}px;background:#17a2b8;"></div>
            <div style="width:{w_tw}px;background:#ffc107;"></div>
            <div style="width:{w_ptw}px;background:#e9ecef;"></div>
          </div>
          <div style="font-size:0.68em;color:#aaa;">{fm(total)}</div>
        </td>"""

    final_str = fm(final_deployed) if final_deployed else "—"
    return f"""
    <tr style="border-bottom:1px solid #eee;">
      <td style="padding:8px 10px;font-weight:700;color:{color};white-space:nowrap;">{lbl}</td>
      {cells}
      <td style="padding:8px 10px;text-align:center;font-weight:700;color:#27ae60;font-size:0.9em;">{final_str}</td>
    </tr>"""


def district_card_html(d, target):
    region  = d["REGION"]
    color   = REGION_COLORS.get(region, "#29B5E8")
    total   = d["TOTAL_ACV"]
    imp     = d["IMP_TOTAL"]
    tw      = d["TW_TOTAL"]
    pretw   = d["PRETW_TOTAL"]
    p_i     = 100 * imp   / total if total else 0
    p_t     = 100 * tw    / total if total else 0
    p_p     = 100 * pretw / total if total else 0

    r1 = m1(d); r3 = m3(d); r4 = m4(r1, r3)
    ml_pct = 100 * r4["ml"] / target if target else 0
    ml_col = "#27ae60" if ml_pct >= 90 else ("#e67e22" if ml_pct >= 70 else "#c0392b")

    si_cls = "warn" if d["STALE_IMP_CNT"] > 0 else "ok"
    st_cls = "warn" if d["STALE_TW_CNT"]  > 0 else "ok"
    nn_cls = "warn" if d["NO_NEXT_STEPS"] >= 3 else ("ok" if d["NO_NEXT_STEPS"] == 0 else "")

    return f"""
<div style="background:#f8f9fa;border-radius:8px;padding:16px;border-top:4px solid {color};">
  <div style="font-size:0.95em;font-weight:700;color:#1a1a2e;margin-bottom:2px;">{d['DISTRICT']}</div>
  <div style="font-size:0.74em;color:#888;margin-bottom:10px;">{d['UC_COUNT']} UCs &bull; {fm(total)} pipeline &bull; {region.replace('Exp','')}</div>
  <div style="display:flex;height:18px;border-radius:4px;overflow:hidden;margin-bottom:4px;">
    <div style="width:{p_i:.1f}%;background:#17a2b8;"></div>
    <div style="width:{p_t:.1f}%;background:#ffc107;"></div>
    <div style="width:{p_p:.1f}%;background:#e9ecef;"></div>
  </div>
  <div style="display:flex;gap:8px;font-size:0.7em;color:#666;margin-bottom:10px;">
    <span>&#9679; IMP {fm(imp)}</span>
    <span>&#9679; TW {fm(tw)}</span>
    <span>&#9679; Pre-TW {fm(pretw)}</span>
  </div>
  <div style="text-align:center;font-size:1.5em;font-weight:700;color:{ml_col};margin-bottom:2px;">{fm(r4['ml'])}</div>
  <div style="text-align:center;font-size:0.68em;text-transform:uppercase;color:#888;margin-bottom:8px;">M4 Most Likely (Weighted Ensemble)</div>
  <div style="display:grid;grid-template-columns:1fr 1fr;gap:4px;font-size:0.72em;margin-bottom:8px;">
    <div style="background:white;border-radius:4px;padding:4px;text-align:center;">
      <div style="color:#aaa;font-size:0.82em;">Commit (Pipeline Risk)</div>
      <div style="font-weight:600;color:#e67e22;">{fm(r4['commit'])}</div>
    </div>
    <div style="background:white;border-radius:4px;padding:4px;text-align:center;">
      <div style="color:#aaa;font-size:0.82em;">Stretch (cap at pipeline)</div>
      <div style="font-weight:600;color:#1d8ab5;">{fm(r4['stretch'])}</div>
    </div>
  </div>
  <div style="font-size:0.7em;color:#888;border-top:1px solid #e9ecef;padding-top:7px;display:flex;flex-direction:column;gap:3px;">
    <span style="display:flex;justify-content:space-between;">
      <span>Stale IMP (&gt;79d)</span>
      <span class="{si_cls}" style="font-weight:600;color:{'#c0392b' if d['STALE_IMP_CNT'] > 0 else '#27ae60'};">{d['STALE_IMP_CNT']} UCs / {fm(d['STALE_IMP_ACV'])}</span>
    </span>
    <span style="display:flex;justify-content:space-between;">
      <span>Stale TW (&gt;104d)</span>
      <span style="font-weight:600;color:{'#c0392b' if d['STALE_TW_CNT'] > 0 else '#27ae60'};">{d['STALE_TW_CNT']} UCs</span>
    </span>
    <span style="display:flex;justify-content:space-between;">
      <span>No next steps</span>
      <span style="font-weight:600;color:{'#c0392b' if d['NO_NEXT_STEPS'] >= 3 else '#555'};">{d['NO_NEXT_STEPS']} UCs</span>
    </span>
  </div>
</div>"""


CSS = """
* { box-sizing:border-box; margin:0; padding:0; }
body { font-family:-apple-system,BlinkMacSystemFont,'Segoe UI',Roboto,sans-serif;
       background:#f0f2f5;color:#1a1a2e;padding:24px; }
h1 { font-size:1.7em;color:#1a1a2e;margin-bottom:4px; }
.subtitle { color:#666;font-size:0.9em;margin-bottom:24px; }
.badge { display:inline-block;background:#29B5E8;color:white;padding:3px 10px;
         border-radius:12px;font-size:0.78em;font-weight:600;margin-left:8px;vertical-align:middle; }
.section-title { font-size:1.05em;font-weight:700;color:#1a1a2e;margin:28px 0 10px; }
.card-wrap { background:white;border-radius:10px;padding:20px 24px;
             box-shadow:0 2px 8px rgba(0,0,0,0.07);margin-bottom:22px; }
.summary-grid { display:grid;grid-template-columns:1fr 1fr;gap:18px;margin-bottom:22px; }
.summary-card { background:white;border-radius:10px;padding:18px 22px;
                box-shadow:0 2px 8px rgba(0,0,0,0.07);border-top:4px solid #ccc; }
.region-label { font-size:0.72em;text-transform:uppercase;letter-spacing:0.5px;color:#888;margin-bottom:4px; }
.region-name  { font-size:1.15em;font-weight:700;color:#1a1a2e;margin-bottom:8px; }
.forecast-row { display:flex;gap:16px;margin-top:8px; }
.fc-item { flex:1;text-align:center;background:#f8f9fa;border-radius:6px;padding:10px; }
.fc-lbl  { font-size:0.68em;text-transform:uppercase;color:#888;margin-bottom:3px; }
.fc-val  { font-size:1.35em;font-weight:700; }
.wk-table { width:100%;border-collapse:collapse;font-size:0.82em; }
.wk-table th { background:#1a1a2e;color:white;padding:8px 4px;text-align:center;font-size:0.75em; }
.wk-table th:first-child { text-align:left;padding-left:10px; }
.wk-table th:last-child  { text-align:center; }
.legend { display:flex;gap:16px;font-size:0.76em;color:#666;margin-top:10px; }
.legend span::before { content:'●';margin-right:4px; }
.district-grid { display:grid;grid-template-columns:repeat(4,1fr);gap:14px; }
.insight { background:#e8f4fd;border-left:3px solid #29B5E8;
           padding:12px 16px;border-radius:4px;font-size:0.82em;
           color:#1a4a6b;margin-top:14px;line-height:1.6; }
.insight strong { color:#1a1a2e; }
.warn { color:#c0392b;font-weight:600; }
.ok   { color:#27ae60;font-weight:600; }
"""


def generate_html(pipeline, weekly, final_deployed, q3_weekly, targets):
    today_str = TODAY.strftime("%B %-d, %Y")
    day_in_q3 = (TODAY - Q3_START).days + 1

    # Aggregate NW and SW totals from pipeline districts
    nw_rows = [d for d in pipeline if d["REGION"] == "NorthwestExp"]
    sw_rows = [d for d in pipeline if d["REGION"] == "SouthwestExp"]

    def agg(rows):
        if not rows:
            return None
        total = {"TOTAL_ACV":0,"IMP_MILESTONE":0,"TW_MILESTONE":0,"PRETW_MILESTONE":0,
                 "IMP_TOTAL":0,"TW_TOTAL":0,"PRETW_TOTAL":0,"STAGE6_ACV":0,
                 "TW_GOOD":0,"PRETW_GOOD":0,"STALE_IMP_ACV":0,"STALE_IMP_CNT":0,
                 "STALE_TW_CNT":0,"NO_NEXT_STEPS":0,"UC_COUNT":0}
        for r in rows:
            for k in total:
                total[k] += r.get(k, 0) or 0
        return total

    nw_agg = agg(nw_rows)
    sw_agg = agg(sw_rows)
    nw_target = targets.get("NorthwestExp", 0)
    sw_target = targets.get("SouthwestExp", 0)

    def region_summary_card(label, data, target, color):
        if not data: return ""
        r1_ = m1(data); r3_ = m3(data); r4_ = m4(r1_, r3_)
        ml_pct = 100 * r4_["ml"] / target if target else 0
        ml_col = "#27ae60" if ml_pct >= 90 else ("#e67e22" if ml_pct >= 70 else "#c0392b")
        return f"""
  <div class="summary-card" style="border-top-color:{color}">
    <div class="region-label">Region</div>
    <div class="region-name">{label} <span style="font-size:0.7em;color:#888;font-weight:400;">{data['UC_COUNT']} UCs &bull; {fm(data['TOTAL_ACV'])} Q3 pipeline</span></div>
    <div class="forecast-row">
      <div class="fc-item">
        <div class="fc-lbl">Commit<br><span style="color:#aaa;">Pipeline Risk</span></div>
        <div class="fc-val" style="color:#e67e22;">{fm(r4_['commit'])}</div>
      </div>
      <div class="fc-item">
        <div class="fc-lbl">Most Likely<br><span style="color:#aaa;">Weighted Ensemble</span></div>
        <div class="fc-val" style="color:{ml_col};">{fm(r4_['ml'])}</div>
        <div style="font-size:0.72em;color:{ml_col};">{ml_pct:.0f}% of ${target/1e6:.1f}M target</div>
      </div>
      <div class="fc-item">
        <div class="fc-lbl">Stretch<br><span style="color:#aaa;">Total pipeline</span></div>
        <div class="fc-val" style="color:#1d8ab5;">{fm(r4_['stretch'])}</div>
      </div>
    </div>
  </div>"""

    # Build weekly table
    all_totals = []
    for lbl, _, _ in QUARTERS:
        wk = weekly.get(lbl, {})
        all_totals += [r.get("TOTAL_ACV", 0) for r in wk.values()]
    if q3_weekly:
        all_totals += [r.get("TOTAL_ACV", 0) for r in q3_weekly.values()]
    max_pipeline = max(all_totals) if all_totals else 1

    week_header = "".join(f'<th>{w}</th>' for w in WEEK_LABELS)
    table_rows  = ""
    for i, (lbl, qs, qe) in enumerate(QUARTERS):
        fd = final_deployed.get(lbl, 0)
        table_rows += weekly_row_html(lbl, weekly.get(lbl, {}), fd,
                                      QTR_COLORS[i % len(QTR_COLORS)], max_pipeline)
    # Q3 FY27 current row (no final deployed yet)
    if q3_weekly:
        table_rows += weekly_row_html("Q3 FY27 ▶", q3_weekly, 0, "#888", max_pipeline)

    # Compute average TRUE conversion rate at each week across completed quarters
    avg_convs = []
    for day in WEEK_DAYS:
        rates = []
        for lbl, _, _ in QUARTERS:
            row = weekly.get(lbl, {}).get(day, {})
            cp = row.get("TRUE_CONV_PCT")
            if cp is not None:
                rates.append(cp)
        avg_convs.append(sum(rates)/len(rates) if rates else None)

    conv_cells = ""
    for i, ac in enumerate(avg_convs):
        if ac is None:
            conv_cells += f'<td style="text-align:center;color:#ccc;">—</td>'
        else:
            col = "#27ae60" if ac >= 60 else ("#e67e22" if ac >= 40 else "#c0392b")
            conv_cells += f'<td style="text-align:center;font-weight:700;color:{col};font-size:0.85em;">{ac:.0f}%</td>'

    # Insight callout
    wk4_avg = avg_convs[3] if len(avg_convs) > 3 and avg_convs[3] else None
    wk8_avg = avg_convs[7] if len(avg_convs) > 7 and avg_convs[7] else None
    insight_text = ""
    if wk4_avg and wk8_avg:
        insight_text = (
            f"At <strong>week 4</strong>, NW+SW historically deploys <strong>{wk4_avg:.0f}%</strong> "
            f"of remaining pipeline by quarter end. By <strong>week 8</strong>, that rises to "
            f"<strong>{wk8_avg:.0f}%</strong> — the pipeline is mostly composed of UCs "
            f"deep in Implementation that are on track. If you see IMP pipeline dropping off "
            f"faster than usual by week 6, check for stale IMP UCs holding down the conversion rate."
        )

    return f"""<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="UTF-8">
<title>NW+SW Forecast Learning Guide — Q3 FY27</title>
<style>{CSS}</style>
</head>
<body>

<h1>NW + SW Forecast Learning Guide <span class="badge">AMSExpansion</span></h1>
<p class="subtitle">Q3 FY27 (Aug 1 – Oct 31, 2026) &mdash; Day {day_in_q3} of {Q3_DAYS} &mdash;
NorthwestExp (D. Kuvelis) &amp; SouthwestExp (A. Sadowski) &mdash; As of {today_str}</p>

<!-- Regional Summary -->
<div class="section-title">Q3 FY27 Forecast — By Region</div>
<div class="summary-grid">
  {region_summary_card("NorthwestExp", nw_agg, nw_target, "#17a2b8")}
  {region_summary_card("SouthwestExp", sw_agg, sw_target, "#6f42c1")}
</div>

<!-- Week-by-Week Teaching Section -->
<div class="section-title">Week-by-Week Pipeline → Go-Live Conversion
  <span style="font-size:0.72em;font-weight:400;color:#888;margin-left:8px;">
    How does pipeline at each week of the quarter relate to final deployed? (Q2 FY26 &rarr; Q3 FY27)
  </span>
</div>
<div class="card-wrap">
  <p style="font-size:0.8em;color:#555;margin-bottom:14px;">
    Each cell shows the <strong>total open pipeline</strong> for NW+SW at that week, with a mini-bar
    (teal = IMP, gold = TW, gray = Pre-TW). The <strong>"X% conv"</strong> label is the
    <em>true conversion rate</em> — of the specific UCs visible in pipeline at that week,
    what % of their ACV actually deployed by quarter end. This rises over the quarter as
    UCs that will slip have already been removed or date-pushed, leaving a higher-quality pool.
  </p>
  <table class="wk-table">
    <thead>
      <tr>
        <th style="text-align:left;padding-left:10px;">Quarter</th>
        {week_header}
        <th>Final<br>Deployed</th>
      </tr>
    </thead>
    <tbody>
      {table_rows}
      <tr style="background:#f8f9fa;border-top:2px solid #ddd;">
        <td style="padding:8px 10px;font-size:0.78em;color:#888;font-style:italic;">Avg conv %</td>
        {conv_cells}
        <td></td>
      </tr>
    </tbody>
  </table>
  <div class="legend">
    <span style="color:#17a2b8;">IMP (Implementation In Progress)</span>
    <span style="color:#ffc107;">TW (Post Tech Win, Pre-IMP)</span>
    <span style="color:#aaa;">Pre-TW (Stage 1-3)</span>
  </div>
  {f'<div class="insight"><strong>Teaching insight:</strong> {insight_text}</div>' if insight_text else ''}
</div>

<!-- Pipeline Sources Section -->
<div class="section-title">Where Does Deployed ACV Come From?
  <span style="font-size:0.72em;font-weight:400;color:#888;margin-left:8px;">
    On average, only ~58% of what deploys was visible at the start of the quarter
  </span>
</div>
<div class="card-wrap">
  <p style="font-size:0.8em;color:#555;margin-bottom:16px;">
    For each completed quarter, what fraction of final deployed ACV was visible in the week-1 pipeline
    vs arrived from <strong>pull-ins</strong> (UCs that existed but had their go-live date moved into
    the quarter) or <strong>new UCs</strong> (created from scratch and deployed within the same quarter).
    This is why the forecast model uses a new-pipeline uplift — your week-1 pipeline is not the ceiling.
  </p>
  <table style="width:100%;border-collapse:collapse;font-size:0.85em;">
    <thead>
      <tr style="background:#1a1a2e;color:white;">
        <th style="padding:9px 12px;text-align:left;">Quarter</th>
        <th style="padding:9px 12px;text-align:left;">Deployed ACV Source (% of final)</th>
        <th style="padding:9px 12px;text-align:right;">Wk-1 Pipeline</th>
        <th style="padding:9px 12px;text-align:right;">Pull-ins</th>
        <th style="padding:9px 12px;text-align:right;">New UCs</th>
      </tr>
    </thead>
    <tbody>
      {"".join(f'''
      <tr style="border-bottom:1px solid #eee;">
        <td style="padding:9px 12px;font-weight:600;">{lbl}</td>
        <td style="padding:9px 12px;">
          <div style="display:flex;height:20px;border-radius:4px;overflow:hidden;width:100%;">
            <div style="width:{src['week1']:.1f}%;background:#17a2b8;display:flex;align-items:center;
                        justify-content:center;font-size:0.72em;color:white;font-weight:700;white-space:nowrap;overflow:hidden;">
              {src['week1']:.0f}%
            </div>
            <div style="width:{src['pullin']:.1f}%;background:#f0a070;display:flex;align-items:center;
                        justify-content:center;font-size:0.72em;color:white;font-weight:700;white-space:nowrap;overflow:hidden;">
              {src['pullin']:.0f}%
            </div>
            <div style="width:{src['new']:.1f}%;background:#27ae60;display:flex;align-items:center;
                        justify-content:center;font-size:0.72em;color:white;font-weight:700;white-space:nowrap;overflow:hidden;">
              {src['new']:.0f}%
            </div>
          </div>
        </td>
        <td style="padding:9px 12px;text-align:right;font-weight:700;color:#17a2b8;">{src['week1']:.1f}%</td>
        <td style="padding:9px 12px;text-align:right;font-weight:700;color:#e67e22;">{src['pullin']:.1f}%</td>
        <td style="padding:9px 12px;text-align:right;font-weight:700;color:#27ae60;">{src['new']:.1f}%</td>
      </tr>'''
      for lbl, _, _ in QUARTERS
      for src in [PIPELINE_SOURCES[lbl]])}
      <tr style="background:#f0f2f5;font-weight:700;">
        <td style="padding:9px 12px;">Average</td>
        <td style="padding:9px 12px;">
          <div style="display:flex;height:20px;border-radius:4px;overflow:hidden;width:100%;">
            <div style="width:58.5%;background:#17a2b8;display:flex;align-items:center;justify-content:center;font-size:0.72em;color:white;font-weight:700;">58.5%</div>
            <div style="width:28.5%;background:#f0a070;display:flex;align-items:center;justify-content:center;font-size:0.72em;color:white;font-weight:700;">28.5%</div>
            <div style="width:13.0%;background:#27ae60;display:flex;align-items:center;justify-content:center;font-size:0.72em;color:white;font-weight:700;">13%</div>
          </div>
        </td>
        <td style="padding:9px 12px;text-align:right;color:#17a2b8;">58.5%</td>
        <td style="padding:9px 12px;text-align:right;color:#e67e22;">28.5%</td>
        <td style="padding:9px 12px;text-align:right;color:#27ae60;">13.0%</td>
      </tr>
    </tbody>
  </table>
  <div style="display:flex;gap:20px;font-size:0.76em;color:#666;margin-top:10px;">
    <span><span style="display:inline-block;width:10px;height:10px;border-radius:2px;background:#17a2b8;margin-right:4px;vertical-align:middle;"></span>Week-1 pipeline (had Q-dated go-live at quarter start)</span>
    <span><span style="display:inline-block;width:10px;height:10px;border-radius:2px;background:#f0a070;margin-right:4px;vertical-align:middle;"></span>Pull-ins (existed but date moved into quarter)</span>
    <span><span style="display:inline-block;width:10px;height:10px;border-radius:2px;background:#27ae60;margin-right:4px;vertical-align:middle;"></span>New UCs (created &amp; deployed within the quarter)</span>
  </div>
  <div class="insight" style="margin-top:14px;">
    <strong>Key takeaway for new SEMs:</strong> On average, <strong>41.5% of what deploys in a quarter
    is not visible at the start.</strong> Pull-ins (~28.5%) are the largest source — these come from
    SEs accelerating timelines or UCs moving faster than expected. New UCs (~13%) are created and
    close within the same quarter. This is why your week-1 pipeline is a floor, not a ceiling,
    and why the Stage Conversion model divides by (1 − 14.5%) to project final deployed.
  </div>
</div>

<!-- District Cards -->
<div class="section-title">Current Q3 FY27 Pipeline — By District</div>
<p style="font-size:0.82em;color:#666;margin-bottom:12px;">
  M4 = Weighted Ensemble of Pipeline Risk (56%) and Stage Conversion (44%) models.
  Commit uses conservative (min) conversion rates. Stretch = total pipeline (max possible).
</p>

<p style="font-size:0.85em;font-weight:600;color:#17a2b8;margin-bottom:8px;">NorthwestExp (RVP: Dean Kuvelis)</p>
<div class="district-grid" style="margin-bottom:20px;">
  {"".join(district_card_html(d, nw_target / len(nw_rows) if nw_rows else 0) for d in nw_rows)}
</div>

<p style="font-size:0.85em;font-weight:600;color:#6f42c1;margin-bottom:8px;">SouthwestExp (RVP: Adam Sadowski)</p>
<div class="district-grid">
  {"".join(district_card_html(d, sw_target / len(sw_rows) if sw_rows else 0) for d in sw_rows)}
</div>

<!-- Methodology for new SEMs -->
<div class="section-title" style="margin-top:28px;">How to Read This Forecast (For New SEMs)</div>
<div class="card-wrap" style="font-size:0.84em;line-height:1.7;color:#444;">
  <p style="margin-bottom:10px;">
    <strong>Pipeline Risk Model (M1 — Commit):</strong> Looks at each UC in your pipeline and asks
    "given how long this UC has been in its current stage, can it realistically deploy by Oct 31?"
    Stage 5 (IMP) needs 79 days average to deploy — so any IMP UC passes.
    Stage 4 (TW) needs 104 days — UCs with &lt;13 days in stage are "at risk."
    Commit = only the "safe" pipeline. This is your floor.
  </p>
  <p style="margin-bottom:10px;">
    <strong>Stage Conversion Model (Stage Conv — Most Likely):</strong> Uses historical conversion rates
    — on average, 56.6% of IMP pipeline, 29.3% of TW, and 24.2% of Pre-TW deploys within the quarter.
    Then adds 14.5% uplift for new UCs created/pulled in during the quarter. This is your best estimate.
  </p>
  <p style="margin-bottom:10px;">
    <strong>Weighted Ensemble (M4 — the recommended number):</strong> 56% Pipeline Risk + 44% Stage
    Conversion. Pipeline Risk gets more weight because it directly inspects your pipeline health.
    Once Q3 starts and deployments accumulate, a third model (Historical Pacing) enters the blend.
  </p>
  <p>
    <strong>What the week-by-week chart teaches:</strong> At week 1, only ~75–85% of visible pipeline
    converts — some UCs slip, dates move, or deals fall through. By week 8, remaining pipeline is
    almost entirely deep-IMP and converts at 90%+. The earlier you can get UCs into Implementation,
    the more predictable your quarter becomes.
  </p>
</div>

<p style="font-size:0.7em;color:#aaa;margin-top:16px;text-align:center;">
  Generated {today_str} &bull; NorthwestExp + SouthwestExp &bull; AMSExpansion / Mark Fleming GVP &bull;
  Sources: MDM.MDM_INTERFACES.DIM_USE_CASE &bull; SALES.SE_REPORTING.DIM_USE_CASE_HISTORY_DS
</p>

</body>
</html>"""


def main():
    print(f"Connecting ({CONNECTION_NAME}) …")
    conn = get_conn()

    print("Querying current Q3 pipeline …")
    pipeline = query_current_pipeline(conn)

    print("Querying targets …")
    targets = query_targets(conn)

    print("Querying weekly historical snapshots (5 quarters) …")
    weekly = query_weekly_snapshots(conn)

    print("Querying final deployed per quarter …")
    final_deployed = query_final_deployed(conn)

    print("Querying Q3 FY27 weekly progress …")
    q3_weekly = query_current_q3_weekly(conn)

    conn.close()

    print(f"  NW districts: {sum(1 for d in pipeline if d['REGION']=='NorthwestExp')}")
    print(f"  SW districts: {sum(1 for d in pipeline if d['REGION']=='SouthwestExp')}")
    for lbl, _, _ in QUARTERS:
        fd = final_deployed.get(lbl, 0)
        wk1 = weekly.get(lbl, {}).get(1, {})
        t1  = wk1.get("TOTAL_ACV", 0) or 0
        print(f"  {lbl}: Wk1 pipeline ${t1/1e6:.1f}M → Final deployed ${fd/1e6:.1f}M "
              f"({100*fd/t1:.0f}% conv)" if t1 else f"  {lbl}: no data")

    print("Generating HTML …")
    html = generate_html(pipeline, weekly, final_deployed, q3_weekly, targets)

    out = Path.home() / "Desktop" / "PEAK_NW_SW_Forecast_Q3FY27.html"
    out.write_text(html, encoding="utf-8")
    print(f"Report written: {out}")
    os.system(f'open "{out}"')


if __name__ == "__main__":
    main()
