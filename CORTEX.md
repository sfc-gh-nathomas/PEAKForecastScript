# PEAK Qualify & Commit Forecast

Streamlit-in-Snowflake app: `SNOWPUBLIC.STREAMLIT.PEAK_QC_FORECAST`
Main file: `peak_app_sis.py` (local variant: `peak_app.py`)

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
`velocity_cache_refresh.sql` in this folder. Unlike every other cache the app
reads, it is NOT wired into the hourly SYSTEM refresh. Last write was
2026-06-05 (confirmed via ACCESS_HISTORY: 6 writes total, all manual).

Consumed by `peak_app_sis.py`:
- `q_use_case_velocity()` -> `METRIC_TYPE='stage_transition'`
- `q_deployment_velocity()` -> `METRIC_TYPE='deployment'`

KNOWN ISSUE: the V7/V14/V30 and `hist_q*` windows bake in `CURRENT_DATE()` at
BUILD time, so they are frozen as of the last refresh. A "trailing 7-day
deployed ACV" read today actually reflects the 7 days before the last refresh
date, not the last 7 days. The `stage_transition` averages degrade more slowly
(long-run averages over everything created since 2025-02-01).

Before trusting any velocity number from this app, check when VELOCITY_CACHE was
last written and re-run the refresh if needed.

## Forecast methods
M1 Pipeline Risk, M2 Historical Pacing, M3 Stage Conversion,
M4 Weighted Ensemble (inverse error weighting).
See the `peak-forecast` and `peak-forecast-methods` skills before changing
model logic — they hold the methodology and backtest conventions.
