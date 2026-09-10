#!/usr/bin/env python3
"""
PEAK rate calibration — computes M3 stage-conversion rates at an arbitrary
day offset relative to quarter start, across prior completed quarters.

Used to feed both the in-quarter Q3 report and the out-quarter Q4 report,
so each uses rates measured at its own actual time horizon rather than
rates borrowed from a different point in the quarter.

Usage:
    python3 peak_calibrate.py            # both Q3 (day 14) and Q4 (day -78)
    python3 peak_calibrate.py 14         # single offset
"""

import sys
from datetime import date

import snowflake.connector

CONNECTION_NAME = "MyConnection"
ROLE = "SALES_RAVEN_RO_RL"
WAREHOUSE = "SNOWADHOC"
GVP = "Mark Fleming"

SNAP = "SALES.SE_REPORTING.DIM_USE_CASE_HISTORY_DS"
FINAL = "SNOWPUBLIC.STREAMLIT.DIM_USE_CASE_MDM_CACHE"

# Completed quarters used for calibration, oldest first.
# Most recent gets 2x weight (established recency-weighting methodology).
QUARTERS = [
    ("Q3 FY26", date(2025, 8, 1),  date(2025, 10, 31)),
    ("Q4 FY26", date(2025, 11, 1), date(2026, 1, 31)),
    ("Q1 FY27", date(2026, 2, 1),  date(2026, 4, 30)),
    ("Q2 FY27", date(2026, 5, 1),  date(2026, 7, 31)),
]


def get_conn():
    """
    Connect flexibly so the same script runs locally and inside an automation
    sandbox, which has no connections.toml.
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
    cur = conn.cursor()
    cur.execute(sql)
    cols = [c[0] for c in cur.description]
    return [dict(zip(cols, row)) for row in cur.fetchall()]


def calibrate_quarter(conn, label, qs, qe, day_offset):
    """
    day_offset semantics:
      >= 1  -> in-quarter. snapshot = qs + (day_offset - 1)
      <= 0  -> pre-quarter. snapshot = qs + (day_offset - 1), i.e. before qs

    Buckets are STAGE-based (IMP = stage 5-6, TW = stage 4, Pre-TW = stage 1-3)
    to match SALES.REPORTING.PEAK_USE_CASE_FORECAST, which the report now reads.
    PEAK's IMPLEMENTATION_START_DATE is a *planned* date populated on ~92% of
    use cases, so milestone-date bucketing is not comparable across the two
    sources — stage is unambiguous in both.
    """
    snap_sql = f"DATEADD('day', {day_offset - 1}, '{qs}'::DATE)"

    sql = f"""
    WITH snap AS (
        SELECT {snap_sql} AS d
    ),
    -- open pipeline state as of the snapshot date
    pipe AS (
        SELECT h.USE_CASE_ID, h.USE_CASE_EACV, h.STAGE_NUMBER
        FROM {SNAP} h
        CROSS JOIN snap s
        WHERE h.DS = s.d
          AND h.ACCOUNT_GVP = '{GVP}'
          AND h.USE_CASE_EACV > 0
          AND h.IS_DEPLOYED = FALSE
          AND COALESCE(h.IS_LOST, FALSE) = FALSE
          AND h.GO_LIVE_DATE BETWEEN '{qs}' AND '{qe}'
          AND h.STAGE_NUMBER BETWEEN 1 AND 6
    ),
    -- what actually deployed in the quarter (final state)
    final AS (
        SELECT u.USE_CASE_ID
        FROM {FINAL} u
        WHERE u.ACCOUNT_GVP = '{GVP}'
          AND u.USE_CASE_EACV > 0
          AND u.IS_DEPLOYED = TRUE
          AND u.GO_LIVE_DATE BETWEEN '{qs}' AND '{qe}'
    )
    SELECT
        ROUND(SUM(CASE WHEN p.STAGE_NUMBER IN (5,6)
                  THEN p.USE_CASE_EACV ELSE 0 END), 0) AS imp_total,
        ROUND(SUM(CASE WHEN p.STAGE_NUMBER IN (5,6) AND f.USE_CASE_ID IS NOT NULL
                  THEN p.USE_CASE_EACV ELSE 0 END), 0) AS imp_conv,
        ROUND(SUM(CASE WHEN p.STAGE_NUMBER = 4
                  THEN p.USE_CASE_EACV ELSE 0 END), 0) AS tw_total,
        ROUND(SUM(CASE WHEN p.STAGE_NUMBER = 4 AND f.USE_CASE_ID IS NOT NULL
                  THEN p.USE_CASE_EACV ELSE 0 END), 0) AS tw_conv,
        ROUND(SUM(CASE WHEN p.STAGE_NUMBER BETWEEN 1 AND 3
                  THEN p.USE_CASE_EACV ELSE 0 END), 0) AS pretw_total,
        ROUND(SUM(CASE WHEN p.STAGE_NUMBER BETWEEN 1 AND 3 AND f.USE_CASE_ID IS NOT NULL
                  THEN p.USE_CASE_EACV ELSE 0 END), 0) AS pretw_conv
    FROM pipe p
    LEFT JOIN final f ON p.USE_CASE_ID = f.USE_CASE_ID
    """

    # Final deployed total, and the share of it that was genuinely NOT knowable
    # at the snapshot. "Knowable" = present in the snapshot with a go-live date
    # in the quarter, whether still open OR already deployed by then. Excluding
    # already-deployed UCs here would wrongly inflate new% later in the quarter.
    new_sql = f"""
    WITH snap AS (SELECT DATEADD('day', {day_offset - 1}, '{qs}'::DATE) AS d),
    known AS (
        SELECT DISTINCT h.USE_CASE_ID
        FROM {SNAP} h CROSS JOIN snap s
        WHERE h.DS = s.d
          AND h.ACCOUNT_GVP = '{GVP}'
          AND h.USE_CASE_EACV > 0
          AND COALESCE(h.IS_LOST, FALSE) = FALSE
          AND h.GO_LIVE_DATE BETWEEN '{qs}' AND '{qe}'
          AND h.STAGE_NUMBER BETWEEN 1 AND 7
    )
    SELECT
        ROUND(SUM(u.USE_CASE_EACV), 0) AS final_deployed,
        ROUND(SUM(CASE WHEN k.USE_CASE_ID IS NULL
                  THEN u.USE_CASE_EACV ELSE 0 END), 0) AS not_in_pipeline
    FROM {FINAL} u
    LEFT JOIN known k ON u.USE_CASE_ID = k.USE_CASE_ID
    WHERE u.ACCOUNT_GVP = '{GVP}'
      AND u.USE_CASE_EACV > 0
      AND u.IS_DEPLOYED = TRUE
      AND u.GO_LIVE_DATE BETWEEN '{qs}' AND '{qe}'
    """

    r = run(conn, sql)[0]
    n = run(conn, new_sql)[0]

    def rate(conv, total):
        return (float(conv) / float(total)) if total else 0.0

    final_dep = float(n["FINAL_DEPLOYED"] or 0)
    not_in = float(n["NOT_IN_PIPELINE"] or 0)

    return {
        "label": label,
        "imp_rate": rate(r["IMP_CONV"], r["IMP_TOTAL"]),
        "tw_rate": rate(r["TW_CONV"], r["TW_TOTAL"]),
        "pretw_rate": rate(r["PRETW_CONV"], r["PRETW_TOTAL"]),
        "new_pct": (not_in / final_dep) if final_dep else 0.0,
        "imp_total": float(r["IMP_TOTAL"] or 0),
        "tw_total": float(r["TW_TOTAL"] or 0),
        "pretw_total": float(r["PRETW_TOTAL"] or 0),
        "final_deployed": final_dep,
    }


def summarize(results, day_offset):
    """Recency-weighted average (most recent quarter 2x) and min across quarters."""
    keys = ("imp_rate", "tw_rate", "pretw_rate", "new_pct")
    weights = [1.0] * len(results)
    weights[-1] = 2.0
    wsum = sum(weights)

    avg = {k: sum(r[k] * w for r, w in zip(results, weights)) / wsum for k in keys}
    mins = {k: min(r[k] for r in results) for k in keys}

    horizon = (
        f"day {day_offset} (in-quarter)"
        if day_offset >= 1
        else f"{abs(day_offset) + 1} days before quarter open"
    )

    print(f"\n{'=' * 78}")
    print(f"CALIBRATION @ {horizon}")
    print(f"{'=' * 78}")
    print(f"{'Quarter':<10} {'IMP':>8} {'TW':>8} {'Pre-TW':>8} {'New%':>8} "
          f"{'Pipe($M)':>10} {'Final($M)':>10}")
    print("-" * 78)
    for r, w in zip(results, weights):
        pipe_m = (r["imp_total"] + r["tw_total"] + r["pretw_total"]) / 1e6
        star = " *2x" if w > 1 else ""
        print(f"{r['label']:<10} {r['imp_rate']*100:>7.1f}% {r['tw_rate']*100:>7.1f}% "
              f"{r['pretw_rate']*100:>7.1f}% {r['new_pct']*100:>7.1f}% "
              f"{pipe_m:>10.1f} {r['final_deployed']/1e6:>10.1f}{star}")
    print("-" * 78)
    print(f"{'WTD AVG':<10} {avg['imp_rate']*100:>7.1f}% {avg['tw_rate']*100:>7.1f}% "
          f"{avg['pretw_rate']*100:>7.1f}% {avg['new_pct']*100:>7.1f}%")
    print(f"{'MIN':<10} {mins['imp_rate']*100:>7.1f}% {mins['tw_rate']*100:>7.1f}% "
          f"{mins['pretw_rate']*100:>7.1f}% {mins['new_pct']*100:>7.1f}%")

    print("\n  # paste into report config")
    print(f"  M3_AVG_IMP_RATE   = {avg['imp_rate']:.3f}")
    print(f"  M3_AVG_TW_RATE    = {avg['tw_rate']:.3f}")
    print(f"  M3_AVG_PRETW_RATE = {avg['pretw_rate']:.3f}")
    print(f"  M3_AVG_NEW_PCT    = {avg['new_pct']:.3f}")
    print(f"  M3_MIN_IMP_RATE   = {mins['imp_rate']:.3f}")
    print(f"  M3_MIN_TW_RATE    = {mins['tw_rate']:.3f}")
    print(f"  M3_MIN_PRETW_RATE = {mins['pretw_rate']:.3f}")
    print(f"  M3_MIN_NEW_PCT    = {mins['new_pct']:.3f}")
    return avg, mins


def get_rates(conn, day_offset):
    """
    Programmatic API used by peak_report.py so a scheduled run recalibrates at
    its own horizon instead of carrying rates frozen at the day they were first
    measured. Returns (m3_avg, m3_min, m2_pace) shaped for the report config.

    m2_pace is None pre-quarter or before M2_RELIABLE_DAY, since pacing needs
    banked ACV and is too noisy very early.
    """
    results = [calibrate_quarter(conn, label, qs, qe, day_offset)
               for label, qs, qe in QUARTERS]

    keys = ("imp_rate", "tw_rate", "pretw_rate", "new_pct")
    weights = [1.0] * len(results)
    weights[-1] = 2.0            # recency weighting: most recent quarter counts double
    wsum = sum(weights)

    avg = {k: sum(r[k] * w for r, w in zip(results, weights)) / wsum for k in keys}
    mins = {k: min(r[k] for r in results) for k in keys}

    m3_avg = {"imp": round(avg["imp_rate"], 3), "tw": round(avg["tw_rate"], 3),
              "pretw": round(avg["pretw_rate"], 3), "new": round(avg["new_pct"], 3)}
    m3_min = {"imp": round(mins["imp_rate"], 3), "tw": round(mins["tw_rate"], 3),
              "pretw": round(mins["pretw_rate"], 3), "new": round(mins["new_pct"], 3)}

    m2_pace = None
    if day_offset >= 1:
        pace = [pacing_quarter(conn, label, qs, qe, day_offset)
                for label, qs, qe in QUARTERS]
        pcts = [p["pct_by_day"] for p in pace]
        if any(pcts):
            pavg = sum(p * w for p, w in zip(pcts, weights)) / wsum
            m2_pace = {"avg": round(pavg, 3),
                       "min": round(min(pcts), 3),
                       "max": round(max(pcts), 3)}

    return m3_avg, m3_min, m2_pace


def pacing_quarter(conn, label, qs, qe, day_offset):
    """
    M2 Historical Pacing input: what share of the quarter's final deployed ACV
    had already deployed by day `day_offset`. Only meaningful in-quarter.
    """
    if day_offset < 1:
        return {"label": label, "pct_by_day": 0.0, "by_day": 0.0, "final": 0.0}

    snap = f"DATEADD('day', {day_offset - 1}, '{qs}'::DATE)"
    sql = f"""
    SELECT
        ROUND(SUM(CASE WHEN u.GO_LIVE_DATE <= {snap}
                  THEN u.USE_CASE_EACV ELSE 0 END), 0) AS by_day,
        ROUND(SUM(u.USE_CASE_EACV), 0) AS final_deployed
    FROM {FINAL} u
    WHERE u.ACCOUNT_GVP = '{GVP}'
      AND u.USE_CASE_EACV > 0
      AND u.IS_DEPLOYED = TRUE
      AND u.GO_LIVE_DATE BETWEEN '{qs}' AND '{qe}'
    """
    r = run(conn, sql)[0]
    by_day = float(r["BY_DAY"] or 0)
    final = float(r["FINAL_DEPLOYED"] or 0)
    return {
        "label": label,
        "pct_by_day": (by_day / final) if final else 0.0,
        "by_day": by_day,
        "final": final,
    }


def summarize_pacing(results, day_offset):
    if day_offset < 1:
        print(f"\n  M2 Historical Pacing: N/A pre-quarter (deployed = $0)")
        return None

    weights = [1.0] * len(results)
    weights[-1] = 2.0
    wsum = sum(weights)
    pcts = [r["pct_by_day"] for r in results]
    avg = sum(p * w for p, w in zip(pcts, weights)) / wsum

    print(f"\n  M2 Historical Pacing — share of final deployed banked by day {day_offset}:")
    for r, w in zip(results, weights):
        star = " *2x" if w > 1 else ""
        print(f"    {r['label']:<10} {r['pct_by_day']*100:>6.1f}%  "
              f"(${r['by_day']/1e6:.1f}M of ${r['final']/1e6:.1f}M){star}")
    print(f"    {'WTD AVG':<10} {avg*100:>6.1f}%   -> M2 ML = deployed_qtd / {avg:.3f}")
    print(f"    {'MIN':<10} {min(pcts)*100:>6.1f}%   -> M2 commit basis")
    print(f"    {'MAX':<10} {max(pcts)*100:>6.1f}%   -> M2 stretch basis")
    print(f"\n  M2_AVG_PACE = {avg:.3f}")
    print(f"  M2_MIN_PACE = {min(pcts):.3f}")
    print(f"  M2_MAX_PACE = {max(pcts):.3f}")
    return avg


def main():
    if len(sys.argv) > 1:
        offsets = [int(a) for a in sys.argv[1:]]
    else:
        today = date.today()
        q3_start = date(2026, 8, 1)
        q4_start = date(2026, 11, 1)
        offsets = [
            (today - q3_start).days + 1,
            -((q4_start - today).days) + 1,
        ]

    conn = get_conn()
    try:
        for off in offsets:
            results = []
            for label, qs, qe in QUARTERS:
                results.append(calibrate_quarter(conn, label, qs, qe, off))
            summarize(results, off)

            pace = [pacing_quarter(conn, label, qs, qe, off)
                    for label, qs, qe in QUARTERS]
            summarize_pacing(pace, off)
    finally:
        conn.close()


if __name__ == "__main__":
    main()
