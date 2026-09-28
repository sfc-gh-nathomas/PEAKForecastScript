# PEAK Qualify & Commit Forecast

Streamlit-in-Snowflake app: `SNOWPUBLIC.STREAMLIT.PEAK_QC_FORECAST`
Main file: `peak_app_sis.py` (also run locally; `peak_app.py` is dead — see below)

## Scripts in this project
- `peak_report.py` — "PEAK QC Forecast — AMSExpansion (quarter-parameterized)".
  Generates the HTML forecast from live Snowflake data.
  `python3 peak_report.py` (current quarter) | `q3` | `q4` (out-quarter view).
  Uses role `SALES_RAVEN_RO_RL`, warehouse `SNOWADHOC`, connection `MyConnection`,
  scoped on THEATER "AMSExpansion" (see "Scope is THEATER, never a GVP name").
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

## Scope is THEATER, never a GVP name (refactored 2026-09-27)
WHY: between 2026-09-15 and 2026-09-27 AMSExpansion's GVP was renamed from
"Mark Fleming" to the placeholder `(TBH)  AMSExpansion GVP` — note the DOUBLE
SPACE — and MDM `ACCOUNT_GVP` went NULL for every AMSExpansion row. Every
`GVP = 'Mark Fleming'` filter silently returned zero rows and the app broke.
USMajors had the same latent break (Jonathan Beaulier -> Josh Sullivan).

RULE: scope every query on theater. Column per source:
| Source | Theater column |
|---|---|
| `SALES.RAVEN.D_SALESFORCE_ACCOUNT_CUSTOMERS` (`_gvp_filter`) | `GEO` |
| `DIM_USE_CASE_MDM_CACHE`, `DIM_USE_CASE_HISTORY_DS_VW`, `SE_REPORTING.DIM_USE_CASE_HISTORY_DS`, `MDM...DIM_USE_CASE(_DAILY)` | `THEATER_NAME` |
| `SALES.REPORTING.PEAK_USE_CASE_FORECAST` | `THEATER` |
| `SALES_PROGRAMS_BRONZE_INGEST` | `GEO_NAME` |
| `GVP_TARGET_CACHE` | `THEATER` (use `MAX()` — a theater can have 2 owner rows across a GVP change) |
| `VELOCITY_CACHE`, `PIPELINE_MOVEMENTS_CACHE` | `THEATER_NAME` (added 2026-09-27) |

Only TWO sources have no theater column and are keyed on a person: MaxIQ
`PEAK_FORECAST_CALLS_PIPELINE_TARGETS.USER_NAME` and
`BOB_SNOWFLAKE_INTELLIGENCE_USAGE_STREAMLIT_AGG.GVP`. For those the GVP name is
RESOLVED AT RUNTIME from the account table by GEO (`_resolve_gvp()` in the app,
`resolve_gvp()` in peak_report.py). Never hard-code the placeholder — it will
change again when the seat is filled, and runtime resolution heals that.

VERIFIED SAFE FOR HISTORY: in both history tables, `THEATER_NAME='AMSExpansion'`
contains every former `ACCOUNT_GVP='Mark Fleming'` row with ZERO exceptions in
every snapshot sampled 2025-02 -> 2026-09 (plus ~29 NULL-GVP rows the old
`_gvp_filter` was already meant to include). Calibration populations unchanged:
post-refactor Q3 run calibrated @ day 58 and the MaxIQ target tied exactly.

Cache builders changed to group on THEATER_NAME, keeping ACCOUNT_GVP as MAX():
- `velocity_cache_refresh.sql` (manual). Backup: `VELOCITY_CACHE_BAK_20260927`.
- `SNOWPUBLIC.STREAMLIT.REFRESH_PIPELINE_MOVEMENTS_CACHE()` — SHARED, owned by
  `SALES_ENGINEER`, called daily 6am ET by task `REFRESH_PEAK_PIPELINE_CACHE`.
  Altered with the user's approval. Only consumers are this app, by column name.

Also refactored 2026-09-27 and run end to end: `peak_districts.py`,
`peak_nw_sw_forecast.py`, `peak_script_standalone.py` (the last resolves the GVP
at runtime for its MaxIQ and SI-agg queries, like the app).

`peak_app.py` is NOT refactored and cannot run at all: 46 of its 47
`from peak_report import ...` names were removed when peak_report.py was
rewritten on 2026-09-10 (last touched at commit 99731ef). It is superseded —
`peak_app_sis.py` runs locally via `streamlit run` and is the maintained app.
Candidate for retirement rather than repair.

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

REFRESHED 2026-09-23 (was 47 days stale, last built 2026-08-07). Because the
script is `CREATE OR REPLACE TABLE`, all grants were dropped and had to be
re-issued — `SELECT` to `SALES_STREAMLIT_RL`, `NORMALYZEROLE`, and `PUBLIC`.
ALWAYS re-grant after running it or the app loses access to the table.

Refresh moved the numbers materially, which is the cost of the frozen windows
below. Mark Fleming, before -> after:
- `stage_transition`: AVG_TW 82.69 -> 81.71, TW_TO_IMP 51.20 -> 46.99,
  IMP_TO_DEPLOYED 87.56 -> 89.11
- `deployment` current: V7 $9.72M -> $8.85M, V14 $21.80M -> $30.18M (+38%),
  V30 $44.42M -> $61.50M (+38%)

That shifts `CONFIG["risk_thresholds"]` via `update_risk_thresholds_from_velocity()`,
since they are sums of the rounded averages: stage_123 222 -> 218, stage_4
139 -> 136, stage_5 88 -> 89. Use-case risk classifications move with them, so
expect the risk counts on the dashboard to change after any velocity refresh.

Row count went 35 -> 34. The dropped row is in some GVP other than Mark Fleming,
who still has the full 4 deployment periods + 1 stage_transition. It could not be
identified precisely: `CREATE OR REPLACE` makes the prior version unreachable via
Time Travel ("Time travel data is not available"). If you need a before/after
diff on a future refresh, snapshot the table to a scratch copy FIRST.
`hist_q1` is absent for every GVP, which predates this refresh.

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
WHERE USER_NAME='(TBH)  AMSExpansion GVP' AND FUNCTION='GVP' AND TYPE='Use Case Wins'
  AND FORECAST_TYPE='Open' AND FISCAL_QUARTER='2027-Q3' AND LATEST_DATE=TRUE;
-- USER_NAME is whatever _resolve_gvp() returns TODAY — do not copy a literal.
-- 2026-09-17: 196,724,457.41 == app, exact (then keyed 'Mark Fleming').
-- 2026-09-27: MaxIQ 163,442,237.65 vs app 165,342,237.65. The $1.9M gap is
--   exactly 18 AMSExpansion use cases (mostly created 2026-09-26/27) that are
--   in MDM but not yet in PEAK_USE_CASE_FORECAST — source lag, not scoping.
--   Re-check after the PEAK source refreshes; it should close.
```
Stage 0 ("Not In Pursuit") is worth $9.89M here — including it is what breaks
the tie to MaxIQ.

STILL OPEN: `q_wins_risk_analysis()` has no quarter filter at all. (Its old
raw `ACCOUNT_GVP` filter is now `THEATER_NAME`, which fixes the NULL-GVP miss.)

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

## Calibration failure is now FATAL (fixed 2026-09-21)
`apply_calibration()` used to return False on failure and the caller at the old
line 1663 DISCARDED that return, so a failed calibration published a report built
on the hardcoded `QUARTERS` fallback rates with no marker. That is a silent
wrong-numbers publication. Measured cost on Q3 FY27 day 52: fallback rates gave
ML $202.6M vs the calibrated $192.2M — a $10.4M overstatement.

Now:
- `main()` captures the result and `sys.exit(1)` with an explanatory message if
  calibration did not run. Verified: exit code 1 on stderr, so automations catch it.
- `--allow-fallback-rates` overrides the abort and stamps a red "UNCALIBRATED"
  banner into the HTML naming the risk. Verified both paths.
- `sys.path` now prepends the script's own directory (derived from `__file__`)
  at import time. The sandbox/automation interpreter does NOT do this, which is
  what made `import peak_calibrate` fail. Verified calibrating from a foreign cwd.

## The automation runs the WORKSPACE copy, not your local file
The refresh task fetches both scripts from `TEMP.NATHOMAS.SHARED_REPORTS:/peak/`.
Editing `peak_report.py` locally does NOTHING for the automation until you upload
it. On 2026-09-21 the workspace copy was still the Sep 9 version (94,416 bytes)
while local was 96,804 — so the two fixes above would never have reached the
scheduled run. ALWAYS upload after changing either script:

```bash
cortex ws cp peak_report.py TEMP.NATHOMAS.SHARED_REPORTS:/peak/
```

`SHARED_REPORTS` is a SHARED workspace: uploads land in `live` and are invisible
to other users until you publish. Follow every upload with:
```sql
ALTER WORKSPACE TEMP.NATHOMAS.SHARED_REPORTS COMMIT;
```
(Published as `version$13` on 2026-09-21 with the fixed script, both region CSVs,
and refreshed Q3/Q4 HTML.)

## RSS blocks the automation's workspace writes — STILL OPEN
The agent task runs under `USER$NATHOMAS.RSS.PEAK_REPORT_REFRESH`, which denies
WRITE on `WORKSPACE TEMP.NATHOMAS.SHARED_REPORTS`. That is why the 2026-09-21 run
could publish the report artifacts but could NOT upload the CSVs or refresh the
workspace HTML copies. An interactive session is unaffected — the uploads above
were done manually. Until the scope grants WRITE, every scheduled run will leave
the workspace copies stale. Update the scope via `/guardrails` (CoCo panel).

## Summary column context (2026-09-21)
The Summary column of the Top 5 table now carries: full `USE_CASE_DESCRIPTION`
(the old 300-char cap was discarding up to 1,859 of 2,159 chars on real records),
the 3 SE comment entries BEHIND the newest one (`extract_recent_comments()`, since
`extract_latest_comment()` shows only the newest of a 4,000+ char history), a
curated use-case story, and a `<details>` block holding the complete raw
`NEXT_STEPS` + `SE_COMMENTS`. Nothing is truncated — long text is collapsed
instead of cut. Column widths rebalanced to 22/33/45.

### The use-case story source, and what was rejected
`SALES.RAVEN.USE_CASE_QUALITY_STORIES` — `PROBLEM_CHALLENGE`, `SNOWFLAKE_SOLUTION`,
`RESULT_OF_IMPACT`. Use-case grain with a direct use-case key, so it is a genuine
STRONG match, 100% populated, 414/702 (59%) FY27-Q3 coverage, `LAST_LOAD_DATE`
2026-08-12. Content verified distinct from `USE_CASE_DESCRIPTION`, not a restatement.
The renderer stamps the as-of date because it lags live SE comments.

Rejected after measurement — do NOT retry either:
- `SALES.RAVEN.ALL_ENGAGEMENTS_PREPED` (2.8M rows) is the real engagement record
  and has the best narrative (`TAKEAWAYS`, `RAW_CONTENT`), but it is ACCOUNT-level
  with NO use-case key. It matches 99.3% of use cases, so it cannot say WHICH use
  case, and it is stale (max `ACTIVITY_DATE` 2025-11-10). A name-in-subject
  heuristic fires on only 1.7% of use cases.
- `SALES.ACTIVITY.USECASE_FIELD_ACTIVITY_AGG` has a use-case key but is SFDC
  field-change history: `Use_Case_Comments__c` / `Implementation_Comments__c` /
  `Specialist_Comments__c` are 100% NULL, it froze 2026-04-06, and 83.5% of its
  text just restates `USE_CASE_DESCRIPTION`.

### THIRD instance of the use-case ID trap — read this before joining anything
There are TWO use-case ids and the correct one DEPENDS ON THE TABLE. Every row dict
from `_use_case_select_cols()` is keyed on RESOLVED (it aliases
`u.RESOLVED_USE_CASE_ID AS USE_CASE_ID`), so any lookup map consumed by
`build_use_case_row()` must ALSO be keyed on RESOLVED — but the join to the source
table may need the other one:

| Source table | JOIN on | Measured |
|---|---|---|
| `CORTEX_CODE_SKILL_ACCT_CACHE` (via workloads) | RESOLVED | — |
| `SALES.RAVEN.USE_CASE_QUALITY_STORIES` | **MDM `USE_CASE_ID`** | 414/702 vs 197/702 on RESOLVED |
| `SALES.ACTIVITY.USECASE_FIELD_*` | RESOLVED | 528/1498 vs 0/1498 on MDM id |
| `DIM_USE_CASE_HISTORY_DS_VW` (old snapshots) | **`USE_CASE_ID`** | RESOLVED is NULL on 64,272/64,280 rows |

So `q_use_case_story()` joins on MDM `USE_CASE_ID` and returns the map keyed on
RESOLVED. Getting this wrong NEVER errors — you silently get fewer rows or none.
It cost three separate debugging rounds; always verify match counts after a join.

## Forecast methods
M1 Pipeline Risk, M2 Historical Pacing, M3 Stage Conversion,
M4 Weighted Ensemble (inverse error weighting).
See the `peak-forecast` and `peak-forecast-methods` skills before changing
model logic — they hold the methodology and backtest conventions.
