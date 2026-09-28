#!/usr/bin/env python3
"""
PEAK District Breakdown — Q3 FY27
Commercial and USGrowth districts with M1/M3/M4 forecasts.
Usage: python3 peak_districts.py
Output: ~/Desktop/PEAK_Districts_Q3FY27.html
"""

import os
from datetime import date
from pathlib import Path

import snowflake.connector

# ─────────────────────── Config ──────────────────────────────────────────────
CONNECTION_NAME = "MyConnection"
ROLE            = "SALES_RAVEN_RO_RL"
WAREHOUSE       = "SNOWADHOC"
# Scope on THEATER, not a GVP name: in 2026-09 AMSExpansion's GVP was renamed
# "(TBH)  AMSExpansion GVP" and ACCOUNT_GVP went NULL in MDM, so the old
# 'Mark Fleming' filter matched nothing. THEATER_NAME is a verified superset of
# the old scope in every history snapshot. See CORTEX.md "Scope is THEATER".
THEATER         = "AMSExpansion"
Q3_START        = date(2026, 8, 1)
Q3_END          = date(2026, 10, 31)
Q3_DAYS         = 91
TODAY           = date.today()
DAYS_TO_Q3_OPEN = (Q3_START - TODAY).days

# ─────────────────────── M3 Rates (calibrated Q3/Q4 FY26, Q1 FY27) ──────────
M3_IMP_RATES = {"14_29": 0.579, "30_59": 0.497, "60_89": 0.386, "90p": 0.393}
M3_TW_RATE   = 0.209
M3_PRETW_RATE= 0.123
M3_NEW_RATE  = 0.431
M3_COMMIT_R  = 0.554
M3_STRETCH_R = 1.121
M4_W1, M4_W3 = 0.485, 0.515

# ─────────────────────── DB ──────────────────────────────────────────────────
def get_conn():
    return snowflake.connector.connect(
        connection_name=CONNECTION_NAME, role=ROLE, warehouse=WAREHOUSE)

def run(conn, sql):
    cur = conn.cursor()
    cur.execute(sql)
    cols = [c[0] for c in cur.description]
    return [dict(zip(cols, row)) for row in cur.fetchall()]

def query_districts(conn):
    sql = f"""
    SELECT
        DISTRICT_NAME AS district,
        CASE WHEN SUB_REGION_NAME IN ('CommEast_SR','CommWest_SR') THEN 'Commercial'
             ELSE 'USGrowthExp' END AS region,
        COUNT(*) AS uc_count,
        ROUND(SUM(USE_CASE_EACV), 0) AS total_acv,
        ROUND(SUM(CASE WHEN STAGE_NUMBER=5 AND DAYS_IN_STAGE BETWEEN  0 AND 13 THEN USE_CASE_EACV ELSE 0 END),0) AS imp_0_13,
        ROUND(SUM(CASE WHEN STAGE_NUMBER=5 AND DAYS_IN_STAGE BETWEEN 14 AND 29 THEN USE_CASE_EACV ELSE 0 END),0) AS imp_14_29,
        ROUND(SUM(CASE WHEN STAGE_NUMBER=5 AND DAYS_IN_STAGE BETWEEN 30 AND 59 THEN USE_CASE_EACV ELSE 0 END),0) AS imp_30_59,
        ROUND(SUM(CASE WHEN STAGE_NUMBER=5 AND DAYS_IN_STAGE BETWEEN 60 AND 89 THEN USE_CASE_EACV ELSE 0 END),0) AS imp_60_89,
        ROUND(SUM(CASE WHEN STAGE_NUMBER=5 AND DAYS_IN_STAGE >= 90            THEN USE_CASE_EACV ELSE 0 END),0) AS imp_90p,
        ROUND(SUM(CASE WHEN STAGE_NUMBER=5 THEN USE_CASE_EACV ELSE 0 END),0) AS imp_total,
        ROUND(SUM(CASE WHEN STAGE_NUMBER=4 AND (DAYS_IN_STAGE + {Q3_DAYS}) >= 104 THEN USE_CASE_EACV ELSE 0 END),0) AS tw_good,
        ROUND(SUM(CASE WHEN STAGE_NUMBER=4 THEN USE_CASE_EACV ELSE 0 END),0) AS tw_total,
        ROUND(SUM(CASE WHEN STAGE_NUMBER IN (1,2,3) AND (DAYS_IN_STAGE + {Q3_DAYS}) >= 146 THEN USE_CASE_EACV ELSE 0 END),0) AS pretw_good,
        ROUND(SUM(CASE WHEN STAGE_NUMBER IN (1,2,3) THEN USE_CASE_EACV ELSE 0 END),0) AS pretw_total,
        ROUND(SUM(CASE WHEN STAGE_NUMBER=6 THEN USE_CASE_EACV ELSE 0 END),0) AS stage6_acv,
        COUNT(CASE WHEN STAGE_NUMBER=5 AND DAYS_IN_STAGE > 79  THEN 1 END) AS stale_imp_cnt,
        ROUND(SUM(CASE WHEN STAGE_NUMBER=5 AND DAYS_IN_STAGE > 79 THEN USE_CASE_EACV ELSE 0 END),0) AS stale_imp_acv,
        COUNT(CASE WHEN STAGE_NUMBER=4 AND DAYS_IN_STAGE > 104 THEN 1 END) AS stale_tw_cnt,
        ROUND(SUM(CASE WHEN STAGE_NUMBER=4 AND DAYS_IN_STAGE > 104 THEN USE_CASE_EACV ELSE 0 END),0) AS stale_tw_acv,
        COUNT(CASE WHEN STAGE_NUMBER >= 4 AND (NEXT_STEPS IS NULL OR NEXT_STEPS = '') THEN 1 END) AS no_next_steps
    FROM MDM.MDM_INTERFACES.DIM_USE_CASE
    WHERE THEATER_NAME = '{THEATER}'
      AND IS_DEPLOYED = FALSE AND IS_LOST = FALSE
      AND USE_CASE_EACV > 0 AND STAGE_NUMBER >= 1
      AND GO_LIVE_DATE BETWEEN '{Q3_START}' AND '{Q3_END}'
      AND SUB_REGION_NAME IN ('CommEast_SR','CommWest_SR','USGrowthExp_SR')
      AND DISTRICT_NAME IS NOT NULL
    GROUP BY 1, 2
    ORDER BY 2, 4 DESC
    """
    return run(conn, sql)

# ─────────────────────── Models ──────────────────────────────────────────────
def m1(d):
    commit  = d["STAGE6_ACV"] + d["IMP_TOTAL"]
    ml      = commit + d["TW_GOOD"] + d["PRETW_GOOD"]
    stretch = d["TOTAL_ACV"]
    return {"commit": commit, "ml": ml, "stretch": stretch}

def m3(d):
    phases = (
        d["IMP_0_13"]  * M3_IMP_RATES["14_29"]
        + d["IMP_14_29"] * M3_IMP_RATES["14_29"]
        + d["IMP_30_59"] * M3_IMP_RATES["30_59"]
        + d["IMP_60_89"] * M3_IMP_RATES["60_89"]
        + d["IMP_90P"]   * M3_IMP_RATES["90p"]
        + d["TW_TOTAL"]   * M3_TW_RATE
        + d["PRETW_TOTAL"]* M3_PRETW_RATE
        + d["STAGE6_ACV"] * 1.0
    )
    new    = M3_NEW_RATE * d["TOTAL_ACV"]
    ml_val = phases + new
    return {"commit": ml_val * M3_COMMIT_R, "ml": ml_val, "stretch": ml_val * M3_STRETCH_R}

def m4(r1, r3):
    return {k: M4_W1 * r1[k] + M4_W3 * r3[k] for k in ("commit", "ml", "stretch")}

# ─────────────────────── Formatting ──────────────────────────────────────────
def fm(val):
    m = val / 1_000_000
    return f"${m:.1f}M" if m < 10 else f"${m:.0f}M"

# ─────────────────────── HTML ────────────────────────────────────────────────
CSS = """
* { box-sizing: border-box; margin: 0; padding: 0; }
body { font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, sans-serif;
       background: #f0f2f5; color: #1a1a2e; padding: 24px; }
h1 { font-size: 1.6em; color: #1a1a2e; margin-bottom: 4px; }
.subtitle { color: #666; font-size: 0.9em; margin-bottom: 24px; }
.badge { display:inline-block; background:#29B5E8; color:white; padding:3px 10px;
         border-radius:12px; font-size:0.78em; font-weight:600; margin-left:8px; vertical-align:middle; }

/* Summary row */
.summary-row { display:grid; grid-template-columns:1fr 1fr; gap:16px; margin-bottom:20px; }
.summary-card { background:white; border-radius:10px; padding:16px 20px;
                box-shadow:0 2px 8px rgba(0,0,0,0.07); display:flex; align-items:center; gap:20px; }
.summary-accent { width:4px; border-radius:2px; align-self:stretch; }
.summary-body { flex:1; }
.summary-label { font-size:0.72em; text-transform:uppercase; letter-spacing:0.5px; color:#888; margin-bottom:4px; }
.summary-name  { font-size:1.1em; font-weight:700; color:#1a1a2e; margin-bottom:2px; }
.summary-stats { font-size:0.78em; color:#666; }
.summary-m4    { font-size:1.8em; font-weight:700; color:#27ae60; white-space:nowrap; }
.summary-m4-lbl{ font-size:0.65em; text-transform:uppercase; letter-spacing:0.4px; color:#888; text-align:right; }

/* Tabs */
.tab-wrap { background:white; border-radius:10px; box-shadow:0 2px 8px rgba(0,0,0,0.07); overflow:hidden; }
.tab-bar  { display:flex; border-bottom:2px solid #e9ecef; background:#f8f9fa; }
.tab-btn  { padding:14px 28px; font-size:0.92em; font-weight:600; color:#888; cursor:pointer;
            border:none; background:none; border-bottom:3px solid transparent;
            margin-bottom:-2px; transition:color .15s,border-color .15s; }
.tab-btn:hover { color:#1a1a2e; }
.tab-btn.active { color:#29B5E8; border-bottom-color:#29B5E8; }
.tab-content { display:none; padding:20px 24px 24px; }
.tab-content.active { display:block; }
.tab-subtitle { font-size:0.8em; color:#888; margin-bottom:16px; }

/* District grid */
.district-grid { display:grid; grid-template-columns:repeat(4,1fr); gap:14px; }
.district-card { background:#f8f9fa; border-radius:8px; padding:16px;
                 border-top:3px solid #29B5E8; }
.district-card.risk-flag { border-top-color:#e74c3c; }
.district-name { font-size:0.93em; font-weight:700; color:#1a1a2e; margin-bottom:2px; }
.district-meta { font-size:0.74em; color:#888; margin-bottom:10px; }

/* Phase bar */
.phase-bar-track { display:flex; height:18px; border-radius:4px; overflow:hidden; margin-bottom:4px; }
.phase-imp   { background:#17a2b8; }
.phase-tw    { background:#ffc107; }
.phase-pretw { background:#e9ecef; }
.phase-legend { display:flex; gap:10px; font-size:0.72em; color:#666; margin-bottom:10px; }
.phase-legend span::before { content:'●'; margin-right:3px; }
.ph-imp::before  { color:#17a2b8; }
.ph-tw::before   { color:#ffc107; }
.ph-ptw::before  { color:#aaa; }

/* M4 big number */
.d-m4     { text-align:center; font-size:1.55em; font-weight:700; color:#27ae60; margin:6px 0 2px; }
.d-m4-lbl { text-align:center; font-size:0.67em; text-transform:uppercase;
            letter-spacing:0.4px; color:#888; margin-bottom:10px; }

/* Mini grid */
.d-mini { display:grid; grid-template-columns:1fr 1fr; gap:5px; font-size:0.74em; margin-bottom:10px; }
.d-mini-item  { background:white; border-radius:4px; padding:4px 6px; text-align:center; }
.d-mini-lbl   { color:#aaa; font-size:0.8em; }
.d-mini-val   { font-weight:600; color:#1a1a2e; }
.d-mini-val.commit  { color:#e67e22; }
.d-mini-val.stretch { color:#1d8ab5; }

/* Flags */
.d-flags { font-size:0.72em; color:#888; border-top:1px solid #e9ecef;
           padding-top:8px; display:flex; flex-direction:column; gap:3px; }
.d-flags .row { display:flex; justify-content:space-between; }
.warn { color:#c0392b; font-weight:600; }
.ok   { color:#27ae60; font-weight:600; }

/* Model breakdown */
details.mb { background:#f8f9fa; border-radius:8px; margin-bottom:18px; overflow:hidden; }
details.mb summary { padding:12px 16px; cursor:pointer; list-style:none; display:flex;
                     align-items:center; gap:8px; font-size:0.88em; font-weight:600; color:#1a1a2e;
                     background:white; border-bottom:1px solid #eee; }
details.mb summary::-webkit-details-marker { display:none; }
details.mb summary::before { content:'▶'; font-size:0.65em; color:#888; transition:transform .2s; }
details.mb[open] summary::before { transform:rotate(90deg); }
details.mb > *:not(summary) { padding:0 16px 16px; }
.mb-table { width:100%; border-collapse:collapse; font-size:0.84em; margin-top:12px; }
.mb-table th { background:#1a1a2e; color:white; padding:9px 12px; text-align:left; }
.mb-table th.num { text-align:right; }
.mb-table td { padding:9px 12px; border-bottom:1px solid #eee; vertical-align:top; }
.mb-table td.num { text-align:right; font-family:'SF Mono',Consolas,monospace; }
.mb-table tr:last-child td { border-bottom:none; font-weight:700; background:#f0f8fd; }
.mb-weight { background:white; border-radius:6px; padding:10px 14px; margin-top:10px; font-size:0.8em; }
.mb-weight strong { color:#1a1a2e; }
.wgrid { display:grid; grid-template-columns:repeat(4,1fr); gap:10px; margin-top:8px; }
.witem { text-align:center; background:#f8f9fa; border-radius:5px; padding:7px; border:1px solid #eee; }
.wlbl  { font-size:0.76em; color:#888; margin-bottom:2px; }
.wval  { font-weight:700; font-size:0.95em; }"""


BACKTEST = {
    "Q3 FY26": {"m1": 5.2,  "m3": 27.2},
    "Q4 FY26": {"m1": 25.7, "m3": 20.2},
    "Q1 FY27": {"m1": 37.5, "m3": 17.0},
}
M1_AVG_ERR = sum(v["m1"] for v in BACKTEST.values()) / len(BACKTEST)
M3_AVG_ERR = sum(v["m3"] for v in BACKTEST.values()) / len(BACKTEST)


def model_breakdown_html(rows, label):
    """Collapsible M1/M2/M3/M4 breakdown table for a set of district rows."""
    r1 = {k: sum(m1(d)[k] for d in rows) for k in ("commit","ml","stretch")}
    r3 = {k: sum(m3(d)[k] for d in rows) for k in ("commit","ml","stretch")}
    r4 = m4(r1, r3)

    return f"""
<details class="mb" open>
  <summary>Model Breakdown &mdash; {label} Total</summary>
  <table class="mb-table">
    <thead>
      <tr>
        <th>Model</th><th>Description</th>
        <th class="num">Backtest error</th><th class="num">Weight</th>
        <th class="num">Commit</th><th class="num">Most Likely</th><th class="num">Stretch</th>
      </tr>
    </thead>
    <tbody>
      <tr>
        <td><strong>M1 &mdash; Pipeline Risk</strong></td>
        <td>Stage 6 + Stage 5 all pass 79d threshold at {Q3_DAYS}d remaining.
            Commit = Stage 5+6. ML = Commit + TW good (&gt;13d) + Pre-TW good (&gt;55d).
            Stretch = total pipeline.</td>
        <td class="num">{M1_AVG_ERR:.1f}% avg<br>
            <span style="font-size:0.8em;color:#888;">
              {BACKTEST['Q3 FY26']['m1']}% / {BACKTEST['Q4 FY26']['m1']}% / {BACKTEST['Q1 FY27']['m1']}%
            </span></td>
        <td class="num" style="color:#e67e22;font-weight:600;">{M4_W1*100:.1f}%</td>
        <td class="num">{fm(r1['commit'])}</td>
        <td class="num">{fm(r1['ml'])}</td>
        <td class="num">{fm(r1['stretch'])}</td>
      </tr>
      <tr>
        <td><strong>M2 &mdash; Historical Pacing</strong></td>
        <td>Requires deployed ACV. Pre-quarter deployed = $0. Reactivates Aug 1.</td>
        <td class="num" style="color:#bbb;">&mdash;</td>
        <td class="num" style="color:#bbb;">0%</td>
        <td class="num" style="color:#bbb;">N/A</td>
        <td class="num" style="color:#bbb;">N/A</td>
        <td class="num" style="color:#bbb;">N/A</td>
      </tr>
      <tr>
        <td><strong>M3 &mdash; Stage Conversion</strong></td>
        <td>Depth-adjusted IMP (14&ndash;29d: {M3_IMP_RATES['14_29']*100:.1f}%,
            30&ndash;59d: {M3_IMP_RATES['30_59']*100:.1f}%,
            60&ndash;89d: {M3_IMP_RATES['60_89']*100:.1f}%,
            90+d: {M3_IMP_RATES['90p']*100:.1f}%) +
            TW {M3_TW_RATE*100:.1f}% + Pre-TW {M3_PRETW_RATE*100:.1f}% +
            new pipeline uplift ({M3_NEW_RATE*100:.1f}% of known pipeline).
            Rates calibrated from Q3/Q4 FY26, Q1 FY27.</td>
        <td class="num">{M3_AVG_ERR:.1f}% avg (LOO)<br>
            <span style="font-size:0.8em;color:#888;">
              {BACKTEST['Q3 FY26']['m3']}% / {BACKTEST['Q4 FY26']['m3']}% / {BACKTEST['Q1 FY27']['m3']}%
            </span></td>
        <td class="num" style="color:#1d8ab5;font-weight:600;">{M4_W3*100:.1f}%</td>
        <td class="num">{fm(r3['commit'])}</td>
        <td class="num">{fm(r3['ml'])}</td>
        <td class="num">{fm(r3['stretch'])}</td>
      </tr>
      <tr>
        <td><strong>M4 &mdash; Weighted Ensemble</strong></td>
        <td>Inverse-error weighted blend: M1 {M4_W1*100:.1f}% + M3 {M4_W3*100:.1f}%.
            M2 excluded (N/A pre-quarter). Same weights applied to all districts.</td>
        <td class="num">&mdash;</td>
        <td class="num" style="color:#27ae60;font-weight:600;">100%</td>
        <td class="num" style="color:#e67e22;font-weight:700;">{fm(r4['commit'])}</td>
        <td class="num" style="color:#27ae60;font-weight:700;">{fm(r4['ml'])}</td>
        <td class="num" style="color:#1d8ab5;font-weight:700;">{fm(r4['stretch'])}</td>
      </tr>
    </tbody>
  </table>
  <div class="mb-weight">
    <strong>Backtest accuracy &mdash; pre-quarter snapshot ({Q3_DAYS} days remaining, 3 prior quarters):</strong>
    <div class="wgrid">
      <div class="witem"><div class="wlbl">Q3 FY26</div>
        <div class="wval">M1: {BACKTEST['Q3 FY26']['m1']}% &bull; M3: {BACKTEST['Q3 FY26']['m3']}%</div></div>
      <div class="witem"><div class="wlbl">Q4 FY26</div>
        <div class="wval">M1: {BACKTEST['Q4 FY26']['m1']}% &bull; M3: {BACKTEST['Q4 FY26']['m3']}%</div></div>
      <div class="witem"><div class="wlbl">Q1 FY27</div>
        <div class="wval">M1: {BACKTEST['Q1 FY27']['m1']}% &bull; M3: {BACKTEST['Q1 FY27']['m3']}%</div></div>
      <div class="witem" style="background:#f0f8fd;">
        <div class="wlbl">Avg error &rarr; Weight</div>
        <div class="wval">M1: {M1_AVG_ERR:.1f}% &rarr; {M4_W1*100:.1f}%<br>
                          M3: {M3_AVG_ERR:.1f}% &rarr; {M4_W3*100:.1f}%</div></div>
    </div>
  </div>
</details>"""


def district_card(d):
    total  = d["TOTAL_ACV"]
    imp    = d["IMP_TOTAL"]
    tw     = d["TW_TOTAL"]
    pretw  = d["PRETW_TOTAL"]
    p_i = 100 * imp   / total if total else 0
    p_t = 100 * tw    / total if total else 0
    p_p = 100 * pretw / total if total else 0

    r1 = m1(d); r3 = m3(d); r4 = m4(r1, r3)

    risk = " risk-flag" if d["STALE_IMP_CNT"] >= 5 or d["NO_NEXT_STEPS"] >= 5 else ""

    si_cls = "warn" if d["STALE_IMP_CNT"] > 0 else "ok"
    st_cls = "warn" if d["STALE_TW_CNT"]  > 0 else "ok"
    nn_cls = "warn" if d["NO_NEXT_STEPS"] >= 3 else ("ok" if d["NO_NEXT_STEPS"] == 0 else "")

    return f"""
  <div class="district-card{risk}">
    <div class="district-name">{d['DISTRICT']}</div>
    <div class="district-meta">{d['UC_COUNT']} UCs &bull; {fm(total)} pipeline</div>
    <div class="phase-bar-track">
      <div class="phase-imp"   style="width:{p_i:.1f}%"></div>
      <div class="phase-tw"    style="width:{p_t:.1f}%"></div>
      <div class="phase-pretw" style="width:{p_p:.1f}%"></div>
    </div>
    <div class="phase-legend">
      <span class="ph-imp">IMP {fm(imp)}</span>
      <span class="ph-tw">TW {fm(tw)}</span>
      <span class="ph-ptw">Pre-TW {fm(pretw)}</span>
    </div>
    <div class="d-m4">{fm(r4['ml'])}</div>
    <div class="d-m4-lbl">M4 Most Likely</div>
    <div class="d-mini">
      <div class="d-mini-item"><div class="d-mini-lbl">M1 ML</div><div class="d-mini-val">{fm(r1['ml'])}</div></div>
      <div class="d-mini-item"><div class="d-mini-lbl">M3 ML</div><div class="d-mini-val">{fm(r3['ml'])}</div></div>
      <div class="d-mini-item"><div class="d-mini-lbl">Commit</div><div class="d-mini-val commit">{fm(r4['commit'])}</div></div>
      <div class="d-mini-item"><div class="d-mini-lbl">Stretch</div><div class="d-mini-val stretch">{fm(r4['stretch'])}</div></div>
    </div>
    <div class="d-flags">
      <div class="row"><span>Stale IMP (&gt;79d)</span><span class="{si_cls}">{d['STALE_IMP_CNT']} UCs / {fm(d['STALE_IMP_ACV'])}</span></div>
      <div class="row"><span>Stale TW (&gt;104d)</span><span class="{st_cls}">{d['STALE_TW_CNT']} UCs / {fm(d['STALE_TW_ACV'])}</span></div>
      <div class="row"><span>No next steps</span><span class="{nn_cls}">{d['NO_NEXT_STEPS']} UCs</span></div>
    </div>
  </div>"""


def summary_card(label, rvp, rows, color):
    total  = sum(d["TOTAL_ACV"] for d in rows)
    ucs    = sum(d["UC_COUNT"]  for d in rows)
    r1_tot = {k: sum(m1(d)[k] for d in rows) for k in ("commit","ml","stretch")}
    r3_tot = {k: sum(m3(d)[k] for d in rows) for k in ("commit","ml","stretch")}
    r4_tot = m4(r1_tot, r3_tot)
    return f"""
  <div class="summary-card">
    <div class="summary-accent" style="background:{color}"></div>
    <div class="summary-body">
      <div class="summary-label">Region</div>
      <div class="summary-name">{label}</div>
      <div class="summary-stats">{ucs} open UCs &bull; {fm(total)} Q3 pipeline &bull; RVP: {rvp}</div>
    </div>
    <div>
      <div class="summary-m4">{fm(r4_tot['ml'])}</div>
      <div class="summary-m4-lbl">M4 ML</div>
    </div>
  </div>"""


def generate_html(districts):
    today_str  = TODAY.strftime("%B %-d, %Y")
    days_label = (
        f"{DAYS_TO_Q3_OPEN} days before Q3 opens"
        if DAYS_TO_Q3_OPEN > 0 else
        f"Q3 day {abs(DAYS_TO_Q3_OPEN) + 1} of {Q3_DAYS}"
    )

    comm = [d for d in districts if d["REGION"] == "Commercial"]
    usg  = [d for d in districts if d["REGION"] == "USGrowthExp"]

    comm_cards = "".join(district_card(d) for d in comm)
    usg_cards  = "".join(district_card(d) for d in usg)

    comm_sum = summary_card("Commercial", "Lisa Yu",     comm, "#e74c3c")
    usg_sum  = summary_card("USGrowthExp","Brian Daniels", usg, "#29B5E8")

    return f"""<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="UTF-8">
<title>Q3 FY27 District Breakdown — AMSExpansion</title>
<style>{CSS}</style>
</head>
<body>

<h1>Q3 FY27 District Breakdown <span class="badge">AMSExpansion</span></h1>
<p class="subtitle">As of {today_str} &mdash; {days_label} (Aug 1 &ndash; Oct 31, 2026) &mdash; Commercial &amp; USGrowth only</p>

<div class="summary-row">
  {comm_sum}
  {usg_sum}
</div>

<div class="tab-wrap">
  <div class="tab-bar">
    <button class="tab-btn active" onclick="showTab('comm',this)">Commercial Districts</button>
    <button class="tab-btn"        onclick="showTab('usg', this)">USGrowth Districts</button>
  </div>

  <div id="tab-comm" class="tab-content active">
    <p class="tab-subtitle">
      {len(comm)} districts &bull; {fm(sum(d['TOTAL_ACV'] for d in comm))} total Q3 pipeline &bull; RVP: Lisa Yu
    </p>
    {model_breakdown_html(comm, 'Commercial')}
    <div class="district-grid">{comm_cards}</div>
  </div>

  <div id="tab-usg" class="tab-content">
    <p class="tab-subtitle">
      {len(usg)} districts &bull; {fm(sum(d['TOTAL_ACV'] for d in usg))} total Q3 pipeline &bull; RVP: Brian Daniels
    </p>
    {model_breakdown_html(usg, 'USGrowthExp')}
    <div class="district-grid">{usg_cards}</div>
  </div>
</div>

<p style="font-size:0.7em;color:#aaa;margin-top:16px;text-align:center;">
  Generated {today_str} &bull; Source: MDM.MDM_INTERFACES.DIM_USE_CASE &bull; {THEATER}
</p>

<script>
function showTab(id, btn) {{
  document.querySelectorAll('.tab-content').forEach(e => e.classList.remove('active'));
  document.querySelectorAll('.tab-btn').forEach(e => e.classList.remove('active'));
  document.getElementById('tab-' + id).classList.add('active');
  btn.classList.add('active');
}}
</script>
</body>
</html>"""


def main():
    print(f"Connecting ({CONNECTION_NAME}) …")
    conn = get_conn()
    print("Querying districts …")
    districts = query_districts(conn)
    conn.close()
    print(f"  {len(districts)} districts returned")

    html     = generate_html(districts)
    out_path = Path.home() / "Desktop" / "PEAK_Districts_Q3FY27.html"
    out_path.write_text(html, encoding="utf-8")
    print(f"Report written: {out_path}")
    os.system(f'open "{out_path}"')


if __name__ == "__main__":
    main()
