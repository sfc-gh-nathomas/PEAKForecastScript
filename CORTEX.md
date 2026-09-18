# PEAK Qualify & Commit Forecast

Streamlit-in-Snowflake app: `SNOWPUBLIC.STREAMLIT.PEAK_QC_FORECAST`
Main file: `peak_app_sis.py` (local variant: `peak_app.py`)

## Scripts in this project
- `peak_report.py` — "PEAK QC Forecast — AMSExpansion (quarter-parameterized)".
  Generates the HTML forecast from live Snowflake data.
  `python3 peak_report.py` (current quarter) | `q3` | `q4` (out-quarter view).
  Uses role `SALES_RAVEN_RO_RL`, warehouse `SNOWADHOC`, connection `MyConnection`,
  GVP "Mark Fleming".
  NOTE: as of 2026-09-10 this replaced an older, larger 193KB
  "PEAK Qualify & Commit Report Generator". That version is recoverable at
  commit `99731ef` if anything is missing from the rewrite.
- `peak_calibrate.py` — computes M3 stage-conversion rates at an arbitrary day
  offset relative to quarter start, across prior completed quarters.
  `python3 peak_calibrate.py` (both Q3 day 14 and Q4 day -78) or pass an offset.
- `peak_districts.py` — Commercial and USGrowth district breakdown with M1/M3/M4.
- `peak_nw_sw_forecast.py` — NW/SW region forecast.
- `peak_script_standalone.py`, `deploy.py` — standalone script variant and deploy.
- `archive/` — superseded `peak_report.py.bak-mdm`, `.bak-prequarter`, `.bak-stage0`.

## Calibration rule — do not share rates across horizons
An in-quarter report and an out-quarter report MUST NOT share conversion rates.
`peak_calibrate.py` measures rates at each report's own actual time horizon
(Q3 at day 14, Q4 at day -78). Borrowing rates from a different point in the
quarter invalidates the forecast.

## KNOWN ISSUE: scripts write output to ~/Desktop, not this folder
Three scripts hardcode `Path.home() / "Desktop"` as their output directory:
- `peak_districts.py:456` -> `~/Desktop/PEAK_Districts_Q3FY27.html`
- `peak_nw_sw_forecast.py:740` -> `~/Desktop/PEAK_NW_SW_Forecast_Q3FY27.html`
- `peak_report.py:1585` -> `~/Desktop/PEAK_forecast_tracking.csv`
- `peak_report.py:1738` -> `~/Desktop/<report>.html`
- `peak_report.py:1746` -> `~/Desktop/PEAK_region_detail_<Q>.csv`

The existing HTML/CSV outputs were moved into this folder on 2026-09-10, but the
scripts were NOT changed. Re-running any of them will write a fresh copy to the
Desktop instead of here, leaving this folder stale. Either redirect these paths
to the script directory, or move outputs in manually after each run.

## Deployment — verify role at each step
The role can silently fall back to PUBLIC between calls, which breaks all view
access. Verify, do not assume.

1. `USE ROLE SALES_STREAMLIT_RL`
2. `SELECT CURRENT_ROLE()` and CONFIRM it returns `SALES_STREAMLIT_RL`.
   If not, re-run step 1 before continuing.
3. PUT file to `@SNOWPUBLIC.STREAMLIT.PEAK_QC_FORECAST_STAGE`
4. `CREATE OR REPLACE STREAMLIT SNOWPUBLIC.STREAMLIT.PEAK_QC_FORECAST`
   `FROM` stage, `MAIN_FILE='peak_app_sis.py'`,
   `TITLE='PEAK QC Forecast'`, `QUERY_WAREHOUSE='SNOWADHOC'`
5. `GRANT USAGE` to `SALES_RAVEN_RO_RL`
6. `ALTER STREAMLIT ... ADD LIVE VERSION FROM LAST`
7. `SHOW STREAMLITS` and CONFIRM `owner=SALES_STREAMLIT_RL`.
   If owner is wrong (e.g. PUBLIC), redo from step 1.

## Cache tables — grants are dropped on replace
CRITICAL: after every `CREATE OR REPLACE TABLE` on `PIPELINE_MOVEMENTS_CACHE`
or `CC_USAGE_CACHE`, re-run `GRANT SELECT` to `SALES_STREAMLIT_RL`.
`CREATE OR REPLACE` drops all grants silently.

## VELOCITY_CACHE is stale and not on the hourly pipeline
`SNOWPUBLIC.STREAMLIT.VELOCITY_CACHE` is refreshed ONLY by manually running
`velocity_cache_refresh.sql` in this folder. It is NOT wired into the hourly
SYSTEM refresh that keeps the other caches current.

Verified 2026-09-10 via `SNOWPUBLIC.INFORMATION_SCHEMA.TABLES`:
- `VELOCITY_CACHE` last altered 2026-08-07 — 34 days stale, 35 rows
- `CC_USAGE_CACHE` last altered 2026-07-26 — 46 days stale, also NOT refreshing
- `PIPELINE_MOVEMENTS_CACHE` last altered today — this one IS current

(Note: the comment header inside `velocity_cache_refresh.sql` claims the last
write was 2026-06-05. That was true when the file was written; it has been
refreshed once since. Trust INFORMATION_SCHEMA over the header.)

Consumed by `peak_app_sis.py`:
- `q_use_case_velocity()` -> `METRIC_TYPE='stage_transition'`
- `q_deployment_velocity()` -> `METRIC_TYPE='deployment'`

KNOWN ISSUE: the V7/V14/V30 and `hist_q*` windows bake in `CURRENT_DATE()` at
BUILD time, so they are frozen as of the last refresh. A "trailing 7-day
deployed ACV" read today actually reflects the 7 days before the last refresh
date, not the last 7 days. The `stage_transition` averages degrade more slowly
(long-run averages over everything created since 2025-02-01).

Before trusting any velocity number from this app, check staleness:
```sql
SELECT TABLE_NAME, LAST_ALTERED,
       DATEDIFF(day, LAST_ALTERED, CURRENT_TIMESTAMP()) AS DAYS_STALE
FROM SNOWPUBLIC.INFORMATION_SCHEMA.TABLES
WHERE TABLE_SCHEMA='STREAMLIT'
  AND TABLE_NAME IN ('VELOCITY_CACHE','CC_USAGE_CACHE','PIPELINE_MOVEMENTS_CACHE');
```

## Wins tab — gate on DECISION_DATE, exclude Stage 0
Wins are DECISION_DATE events. Any wins-tab population filter MUST use
`DECISION_DATE BETWEEN quarter_start AND quarter_end` and
`STAGE_NUMBER BETWEEN 1 AND 6`. Never `GO_LIVE_DATE` — that is a different
milestone and selects a different population.

Fixed 2026-09-17 in `q_wins_open_pipeline()` and `q_wins_top5()`, which both
gated on `GO_LIVE_DATE`. Effect was severe: the open-pipeline headline read
$10.49M against a true $196.72M (74 UCs instead of 1,338, only 47 overlapping),
and the "top open wins" list was drawn from that 5% pool — showing Securonix
$1.0M as #1 when the real #1 was Fanatics Holdings at $5.0M. Nine of ten
displayed rows did not belong.

Cross-check after ANY change to these filters — it ties exactly:
```sql
SELECT SUM(FORECAST_AMOUNT) FROM SALES.REPORTING.PEAK_FORECAST_CALLS_PIPELINE_TARGETS
WHERE USER_NAME='Mark Fleming' AND FUNCTION='GVP' AND TYPE='Use Case Wins'
  AND FORECAST_TYPE='Open' AND FISCAL_QUARTER='2027-Q3' AND LATEST_DATE=TRUE;
-- 196,724,457.41 == patched q_wins_open_pipeline for FY27-Q3
```
Stage 0 ("Not In Pursuit") is worth $9.89M here — including it is what breaks
the tie to MaxIQ.

STILL OPEN: `q_wins_risk_analysis()` has no quarter filter at all and uses raw
`ACCOUNT_GVP = '{gvp}'` instead of `_gvp_filter()`, so it misses accounts with
NULL `ACCOUNT_GVP`. Not yet fixed.

## CoCo Adoption on the Go-Lives tab (replaced Cortex Code CLI Usage)
The old "Cortex Code CLI Usage (Last 90 Days)" table was removed 2026-09-17. It
read `SNOWPUBLIC.STREAMLIT.CC_USAGE_CACHE`, which stopped refreshing 2026-07-26
(53 days stale at removal) — it had been showing dead numbers.

Replaced by a theater-level CoCo Adoption block ported from the 2x2 dashboard
(`/Users/nathomas/Cortex Code Projects/SalesDashboard/sales_dashboard.py`),
plus two things the 2x2 does NOT have: weekly tier movement and go-live insights.

### There is no "CoCo budget"
The bucketing dimension is `ACCOUNT_TIER` — a 5-level ENGAGEMENT ladder
(`Zero Usage, Exploring, Activated, Expanded, Deep`) precomputed upstream in
`SALES.REPORTING.COCO_ACCOUNT_COCO_USAGE`. Thresholds are engaged-user counts and
their share of `UNBLOCKED_SF_USERS` over 28 days. **Credits are never involved.**
Do not describe tiers as spend or budget bands.

### Scope decision — GEO_NAME, not the PEAK GVP filter
Tier cards use `GEO_NAME = _theater()` (5,042 accounts for AMSExpansion), NOT
`_gvp_filter()`. The 2x2 scopes by GVP person name and gets 5,659 — a ~600
account difference. GEO_NAME was chosen deliberately as the true theater
definition. `q_coco_golive_insights()` DOES use `_gvp_filter()`, because that
question is about accounts with go-lives, not the theater book. Two different
scopes on one tab is intentional; do not "fix" one to match the other.

Verified 2026-09-17 (as of DS 2026-09-16): 5,042 capacity accounts —
Zero Usage 638, Exploring 1,330, Activated 2,587, Expanded 439, Deep 48
(sums to 5,042). Set Sail L28 77 vs prior 58.
Weekly movement nets to zero across tiers (−287 +182 +84 +19 +2 = 0).

Two query gotchas:
- Use `IS_YESTERDAY = TRUE`, NOT a correlated `MAX(DS)` subquery. `MAX(DS)`
  against this 4.6M-row table times out at 180s.
- Take Zero Usage from the EXPLICIT `'Zero Usage'` rows. The 2x2 derives it by
  subtraction instead, so our count will not tie to its sparkline. Expected.

### CoCo Skill Match is NOT semantic matching
`_coco_skill_match_cte()` ports the 2x2 exactly: a string-equality join of the
use case's `WORKLOADS` tokens against `CORTEX_CODE_SKILL_ACCT_CACHE.WORKLOAD_CATEGORY`
for the SAME ACCOUNT. No AI, no embeddings, no keyword match, no use-case→skill
mapping table. It answers "has this ACCOUNT used CoCo skills in this use case's
product categories" — so two use cases at one account with equal `WORKLOADS`
always score identically. Tiers: n==0 None; n>=3 or sessions>=30 High;
n>=2 or sessions>=10 Medium; else Low.

`WORKLOADS` tokens must equal `WORKLOAD_CATEGORY` exactly, including the
ampersand in `Applications & Collaboration`. Normalisation drift silently
yields 'None' rather than erroring.

Verified distribution over 733 FY27-Q3 go-live use cases: High 444, None 118,
Medium 89, Low 82 — 615/733 = 83.9% have at least one match. It discriminates
here, but on the DEPLOYED population it saturates (top 20 all High), so do not
reuse it as a ranking key.

### CORTEX_CODE_SKILL_ACCT_CACHE staleness — STILL OPEN
`LAST_ALTERED 2026-08-10` understates it: the data was built with a 90-day
window ending 2026-07-10, so it is ~69 days stale, not 38. There is NO refresh
task; the similarly-named `CORTEX_CODE_ACCT_COCO_CACHE_REFRESH_TASK` targets a
DIFFERENT table. Both this app and the 2x2 silently inherit the staleness.

A refresh procedure + task is drafted and compile-verified but NOT yet deployed.
Blockers and cautions recorded before running it:
- True upstream is `SNOWSCIENCE.LLM.CORTEX_CODE_SKILL_DAY_FACT` (NOT
  `CORTEX_CODE_ACCOUNT_DAY_FACT`, which has no skill column). Semantics proven
  to 99.15% exact match on `SESSIONS_90D` when reconstructed at the 2026-07-10 window.
- `WORKLOAD_CATEGORY` is NOT derivable — no mapping table exists. The 29-skill
  catalog is a human curation over 28,832 upstream skill names and must stay hardcoded.
- The table is owned by `ROLE PUBLIC`, unlike every other cache here
  (`SALES_ENGINEER`). Refreshing requires `GRANT OWNERSHIP ... COPY CURRENT GRANTS` first.
- It carries a manual `SELECT` grant to `PUBLIC` that `CREATE OR REPLACE` would drop.
- Refreshing WILL move numbers in both dashboards: four skills have effectively
  died, almost certainly renames — `cortex-ai-functions` 2,660→18 accounts,
  `snowsight-performance-summary` 2,441→19, `streamlit` 601→6,
  `developing-with-streamlit` 679→168. Because the tier scores on distinct
  matched skill COUNT, some accounts drop a tier even though total sessions grew.
  Back the table up first — current contents are not reproducible once replaced.

### Join key trap — RESOLVED_USE_CASE_ID, not USE_CASE_ID
`_use_case_select_cols()` aliases `u.RESOLVED_USE_CASE_ID AS USE_CASE_ID`, so every
row dict reaching `build_use_case_row()` is keyed on the RESOLVED id. Anything that
joins back to those rows by id MUST also select `RESOLVED_USE_CASE_ID AS USE_CASE_ID`.

They differ on **364 of 744** FY27-Q3 go-live rows (~49%). Caught 2026-09-17 when
the skill match keyed on the raw `USE_CASE_ID` and 3 of the top 5 rows silently
rendered no skill line — no error, just a missing line. Both
`q_coco_skill_match_by_uc()` and `q_coco_golive_insights()` now use RESOLVED.

The skill match renders INLINE in col1 next to ACV / run rate / CC metrics, not as
its own table column — the Top 5 table stays 3 columns.

Verified after the fix: 5/5 top rows carry the line; 725 use cases scored;
608/725 = 83.9% matched across 564 go-live accounts.

## Forecast methods
M1 Pipeline Risk, M2 Historical Pacing, M3 Stage Conversion,
M4 Weighted Ensemble (inverse error weighting).
See the `peak-forecast` and `peak-forecast-methods` skills before changing
model logic — they hold the methodology and backtest conventions.
