-- =====================================================================
-- VELOCITY_CACHE refresh — the ONLY statement that has ever populated
-- SNOWPUBLIC.STREAMLIT.VELOCITY_CACHE.
--
-- Retrieved verbatim from SNOWFLAKE.ACCOUNT_USAGE.QUERY_HISTORY
--   QUERY_ID : 01c4da86-0816-7ee7-0001-dd784a3da917
--   RUN BY   : NATHOMAS (role PUBLIC)
--   RUN AT   : 2026-06-05 20:54:04 UTC
--   LENGTH   : 10,921 characters
--
-- ACCESS_HISTORY confirms this is the sole write to the table in the last
-- 180 days (6 writes total, all by NATHOMAS, none since 2026-06-05).
-- Every other cache the PEAK QC Forecast app reads is refreshed hourly by
-- SYSTEM. This one is not wired into that pipeline.
--
-- CONSUMED BY (peak_app_sis.py):
--   line 1502  q_use_case_velocity()   -> METRIC_TYPE='stage_transition'
--   line  845  q_deployment_velocity() -> METRIC_TYPE='deployment'
--
-- KNOWN ISSUE: V7/V14/V30 and the hist_q* windows bake in CURRENT_DATE()
-- at BUILD time, so they are frozen as of the last refresh. A "trailing
-- 7-day deployed ACV" read today reflects the 7 days before 2026-06-05.
-- The stage_transition averages degrade more slowly (long-run averages
-- over everything created since 2025-02-01).
-- =====================================================================

-- 2026-09-27: KEYED ON THEATER_NAME. ACCOUNT_GVP is now NULL in MDM for all of
-- AMSExpansion (GVP renamed to "(TBH)  AMSExpansion GVP"), so grouping on it
-- merged the theater into an anonymous NULL bucket. ACCOUNT_GVP is kept as
-- MAX() for backward compatibility only; the app filters on THEATER_NAME.
-- After running: re-GRANT SELECT to SALES_STREAMLIT_RL, NORMALYZEROLE, PUBLIC.
CREATE OR REPLACE TABLE SNOWPUBLIC.STREAMLIT.VELOCITY_CACHE AS

    WITH fiscal AS (
        SELECT 
            CASE
                WHEN MONTH(CURRENT_DATE()) >= 2 THEN YEAR(CURRENT_DATE()) + 1
                ELSE YEAR(CURRENT_DATE())
            END AS FY,
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
    ),
    day_num AS (
        SELECT DATEDIFF('day', f.FQ_START, CURRENT_DATE()) + 1 AS DAY_NUMBER
        FROM fiscal f
    ),
    prior_fy AS (
        SELECT f.FY, f.FY - 2 AS PRIOR_START_YEAR
        FROM fiscal f
    ),
    prior_quarters AS (
        SELECT p.PRIOR_START_YEAR AS SY, 
               DATE_FROM_PARTS(p.PRIOR_START_YEAR, 2, 1) AS Q1S, DATE_FROM_PARTS(p.PRIOR_START_YEAR, 4, 30) AS Q1E,
               DATE_FROM_PARTS(p.PRIOR_START_YEAR, 5, 1) AS Q2S, DATE_FROM_PARTS(p.PRIOR_START_YEAR, 7, 31) AS Q2E,
               DATE_FROM_PARTS(p.PRIOR_START_YEAR, 8, 1) AS Q3S, DATE_FROM_PARTS(p.PRIOR_START_YEAR, 10, 31) AS Q3E,
               DATE_FROM_PARTS(p.PRIOR_START_YEAR, 11, 1) AS Q4S, DATE_FROM_PARTS(p.PRIOR_START_YEAR + 1, 1, 31) AS Q4E
        FROM prior_fy p
    ),
    latest_ds AS (
        SELECT MAX(DS) AS MAX_DS FROM MDM.MDM_INTERFACES.DIM_USE_CASE_DAILY
    ),

    -- STAGE TRANSITION VELOCITY: self-calculated DATEDIFFs
    -- Population: all UCs created >= 2025-02-01, all stages, no eACV filter
    -- Metrics: Created->TW, TW->ImpStart, ImpStart->Deployed (only where both dates exist and >= 0)
    stage_velocity AS (
        SELECT 
            d.THEATER_NAME,
            MAX(d.ACCOUNT_GVP) AS ACCOUNT_GVP,
            'stage_transition' AS METRIC_TYPE,
            'current' AS PERIOD,
            AVG(CASE WHEN d.TECHNICAL_WIN_DATE IS NOT NULL
                      AND DATEDIFF('day', d.CREATED_DATE, d.TECHNICAL_WIN_DATE) >= 0
                 THEN DATEDIFF('day', d.CREATED_DATE, d.TECHNICAL_WIN_DATE) END) AS AVG_TW,
            AVG(CASE WHEN d.TECHNICAL_WIN_DATE IS NOT NULL
                      AND d.IMPLEMENTATION_START_DATE IS NOT NULL
                      AND DATEDIFF('day', d.TECHNICAL_WIN_DATE, d.IMPLEMENTATION_START_DATE) >= 0
                 THEN DATEDIFF('day', d.TECHNICAL_WIN_DATE, d.IMPLEMENTATION_START_DATE) END) AS AVG_TW_TO_IMP,
            AVG(CASE WHEN d.IMPLEMENTATION_START_DATE IS NOT NULL
                      AND d.ACTUAL_USE_CASE_DEPLOYMENT_DATE IS NOT NULL
                      AND DATEDIFF('day', d.IMPLEMENTATION_START_DATE, d.ACTUAL_USE_CASE_DEPLOYMENT_DATE) >= 0
                 THEN DATEDIFF('day', d.IMPLEMENTATION_START_DATE, d.ACTUAL_USE_CASE_DEPLOYMENT_DATE) END) AS AVG_IMP_TO_DEPLOYED,
            NULL AS V7, NULL AS V14, NULL AS V30
        FROM MDM.MDM_INTERFACES.DIM_USE_CASE_DAILY d
        CROSS JOIN latest_ds l
        WHERE d.DS = l.MAX_DS
          AND d.CREATED_DATE >= '2025-02-01'
        GROUP BY d.THEATER_NAME
    ),

    -- DEPLOYMENT VELOCITY: unchanged methodology
    deploy_current AS (
        SELECT 
            d.THEATER_NAME,
            MAX(d.ACCOUNT_GVP) AS ACCOUNT_GVP,
            'deployment' AS METRIC_TYPE,
            'current' AS PERIOD,
            NULL AS AVG_TW, NULL AS AVG_TW_TO_IMP, NULL AS AVG_IMP_TO_DEPLOYED,
            SUM(CASE WHEN d.ACTUAL_USE_CASE_DEPLOYMENT_DATE > DATEADD('day', -7, CURRENT_DATE())
                      AND d.ACTUAL_USE_CASE_DEPLOYMENT_DATE <= CURRENT_DATE()
                 THEN d.USE_CASE_EACV ELSE 0 END) AS V7,
            SUM(CASE WHEN d.ACTUAL_USE_CASE_DEPLOYMENT_DATE > DATEADD('day', -14, CURRENT_DATE())
                      AND d.ACTUAL_USE_CASE_DEPLOYMENT_DATE <= CURRENT_DATE()
                 THEN d.USE_CASE_EACV ELSE 0 END) AS V14,
            SUM(CASE WHEN d.ACTUAL_USE_CASE_DEPLOYMENT_DATE > DATEADD('day', -30, CURRENT_DATE())
                      AND d.ACTUAL_USE_CASE_DEPLOYMENT_DATE <= CURRENT_DATE()
                 THEN d.USE_CASE_EACV ELSE 0 END) AS V30
        FROM MDM.MDM_INTERFACES.DIM_USE_CASE_DAILY d
        CROSS JOIN latest_ds l CROSS JOIN fiscal f
        WHERE d.DS = l.MAX_DS
          AND d.IS_DEPLOYED = TRUE AND d.USE_CASE_EACV > 0
          AND d.ACTUAL_USE_CASE_DEPLOYMENT_DATE BETWEEN f.FQ_START AND f.FQ_END
        GROUP BY d.THEATER_NAME
    ),
    deploy_hist_q1 AS (
        SELECT d.THEATER_NAME, MAX(d.ACCOUNT_GVP) AS ACCOUNT_GVP, 'deployment' AS METRIC_TYPE, 'hist_q1' AS PERIOD,
               NULL AS AVG_TW, NULL AS AVG_TW_TO_IMP, NULL AS AVG_IMP_TO_DEPLOYED,
               SUM(CASE WHEN d.ACTUAL_USE_CASE_DEPLOYMENT_DATE > DATEADD('day', (SELECT DAY_NUMBER FROM day_num) - 8, pq.Q1S)
                         AND d.ACTUAL_USE_CASE_DEPLOYMENT_DATE <= DATEADD('day', (SELECT DAY_NUMBER FROM day_num) - 1, pq.Q1S)
                    THEN d.USE_CASE_EACV ELSE 0 END) AS V7,
               SUM(CASE WHEN d.ACTUAL_USE_CASE_DEPLOYMENT_DATE > DATEADD('day', (SELECT DAY_NUMBER FROM day_num) - 15, pq.Q1S)
                         AND d.ACTUAL_USE_CASE_DEPLOYMENT_DATE <= DATEADD('day', (SELECT DAY_NUMBER FROM day_num) - 1, pq.Q1S)
                    THEN d.USE_CASE_EACV ELSE 0 END) AS V14,
               SUM(CASE WHEN d.ACTUAL_USE_CASE_DEPLOYMENT_DATE > DATEADD('day', (SELECT DAY_NUMBER FROM day_num) - 31, pq.Q1S)
                         AND d.ACTUAL_USE_CASE_DEPLOYMENT_DATE <= DATEADD('day', (SELECT DAY_NUMBER FROM day_num) - 1, pq.Q1S)
                    THEN d.USE_CASE_EACV ELSE 0 END) AS V30
        FROM MDM.MDM_INTERFACES.DIM_USE_CASE_DAILY d
        CROSS JOIN prior_quarters pq
        WHERE d.DS = DATEADD('day', (SELECT DAY_NUMBER FROM day_num) - 1, pq.Q1S)
          AND d.IS_DEPLOYED = TRUE AND d.USE_CASE_EACV > 0
        GROUP BY d.THEATER_NAME
    ),
    deploy_hist_q2 AS (
        SELECT d.THEATER_NAME, MAX(d.ACCOUNT_GVP) AS ACCOUNT_GVP, 'deployment' AS METRIC_TYPE, 'hist_q2' AS PERIOD,
               NULL AS AVG_TW, NULL AS AVG_TW_TO_IMP, NULL AS AVG_IMP_TO_DEPLOYED,
               SUM(CASE WHEN d.ACTUAL_USE_CASE_DEPLOYMENT_DATE > DATEADD('day', (SELECT DAY_NUMBER FROM day_num) - 8, pq.Q2S)
                         AND d.ACTUAL_USE_CASE_DEPLOYMENT_DATE <= DATEADD('day', (SELECT DAY_NUMBER FROM day_num) - 1, pq.Q2S)
                    THEN d.USE_CASE_EACV ELSE 0 END) AS V7,
               SUM(CASE WHEN d.ACTUAL_USE_CASE_DEPLOYMENT_DATE > DATEADD('day', (SELECT DAY_NUMBER FROM day_num) - 15, pq.Q2S)
                         AND d.ACTUAL_USE_CASE_DEPLOYMENT_DATE <= DATEADD('day', (SELECT DAY_NUMBER FROM day_num) - 1, pq.Q2S)
                    THEN d.USE_CASE_EACV ELSE 0 END) AS V14,
               SUM(CASE WHEN d.ACTUAL_USE_CASE_DEPLOYMENT_DATE > DATEADD('day', (SELECT DAY_NUMBER FROM day_num) - 31, pq.Q2S)
                         AND d.ACTUAL_USE_CASE_DEPLOYMENT_DATE <= DATEADD('day', (SELECT DAY_NUMBER FROM day_num) - 1, pq.Q2S)
                    THEN d.USE_CASE_EACV ELSE 0 END) AS V30
        FROM MDM.MDM_INTERFACES.DIM_USE_CASE_DAILY d
        CROSS JOIN prior_quarters pq
        WHERE d.DS = DATEADD('day', (SELECT DAY_NUMBER FROM day_num) - 1, pq.Q2S)
          AND d.IS_DEPLOYED = TRUE AND d.USE_CASE_EACV > 0
        GROUP BY d.THEATER_NAME
    ),
    deploy_hist_q3 AS (
        SELECT d.THEATER_NAME, MAX(d.ACCOUNT_GVP) AS ACCOUNT_GVP, 'deployment' AS METRIC_TYPE, 'hist_q3' AS PERIOD,
               NULL AS AVG_TW, NULL AS AVG_TW_TO_IMP, NULL AS AVG_IMP_TO_DEPLOYED,
               SUM(CASE WHEN d.ACTUAL_USE_CASE_DEPLOYMENT_DATE > DATEADD('day', (SELECT DAY_NUMBER FROM day_num) - 8, pq.Q3S)
                         AND d.ACTUAL_USE_CASE_DEPLOYMENT_DATE <= DATEADD('day', (SELECT DAY_NUMBER FROM day_num) - 1, pq.Q3S)
                    THEN d.USE_CASE_EACV ELSE 0 END) AS V7,
               SUM(CASE WHEN d.ACTUAL_USE_CASE_DEPLOYMENT_DATE > DATEADD('day', (SELECT DAY_NUMBER FROM day_num) - 15, pq.Q3S)
                         AND d.ACTUAL_USE_CASE_DEPLOYMENT_DATE <= DATEADD('day', (SELECT DAY_NUMBER FROM day_num) - 1, pq.Q3S)
                    THEN d.USE_CASE_EACV ELSE 0 END) AS V14,
               SUM(CASE WHEN d.ACTUAL_USE_CASE_DEPLOYMENT_DATE > DATEADD('day', (SELECT DAY_NUMBER FROM day_num) - 31, pq.Q3S)
                         AND d.ACTUAL_USE_CASE_DEPLOYMENT_DATE <= DATEADD('day', (SELECT DAY_NUMBER FROM day_num) - 1, pq.Q3S)
                    THEN d.USE_CASE_EACV ELSE 0 END) AS V30
        FROM MDM.MDM_INTERFACES.DIM_USE_CASE_DAILY d
        CROSS JOIN prior_quarters pq
        WHERE d.DS = DATEADD('day', (SELECT DAY_NUMBER FROM day_num) - 1, pq.Q3S)
          AND d.IS_DEPLOYED = TRUE AND d.USE_CASE_EACV > 0
        GROUP BY d.THEATER_NAME
    ),
    deploy_hist_q4 AS (
        SELECT d.THEATER_NAME, MAX(d.ACCOUNT_GVP) AS ACCOUNT_GVP, 'deployment' AS METRIC_TYPE, 'hist_q4' AS PERIOD,
               NULL AS AVG_TW, NULL AS AVG_TW_TO_IMP, NULL AS AVG_IMP_TO_DEPLOYED,
               SUM(CASE WHEN d.ACTUAL_USE_CASE_DEPLOYMENT_DATE > DATEADD('day', (SELECT DAY_NUMBER FROM day_num) - 8, pq.Q4S)
                         AND d.ACTUAL_USE_CASE_DEPLOYMENT_DATE <= DATEADD('day', (SELECT DAY_NUMBER FROM day_num) - 1, pq.Q4S)
                    THEN d.USE_CASE_EACV ELSE 0 END) AS V7,
               SUM(CASE WHEN d.ACTUAL_USE_CASE_DEPLOYMENT_DATE > DATEADD('day', (SELECT DAY_NUMBER FROM day_num) - 15, pq.Q4S)
                         AND d.ACTUAL_USE_CASE_DEPLOYMENT_DATE <= DATEADD('day', (SELECT DAY_NUMBER FROM day_num) - 1, pq.Q4S)
                    THEN d.USE_CASE_EACV ELSE 0 END) AS V14,
               SUM(CASE WHEN d.ACTUAL_USE_CASE_DEPLOYMENT_DATE > DATEADD('day', (SELECT DAY_NUMBER FROM day_num) - 31, pq.Q4S)
                         AND d.ACTUAL_USE_CASE_DEPLOYMENT_DATE <= DATEADD('day', (SELECT DAY_NUMBER FROM day_num) - 1, pq.Q4S)
                    THEN d.USE_CASE_EACV ELSE 0 END) AS V30
        FROM MDM.MDM_INTERFACES.DIM_USE_CASE_DAILY d
        CROSS JOIN prior_quarters pq
        WHERE d.DS = DATEADD('day', (SELECT DAY_NUMBER FROM day_num) - 1, pq.Q4S)
          AND d.IS_DEPLOYED = TRUE AND d.USE_CASE_EACV > 0
        GROUP BY d.THEATER_NAME
    )

    SELECT * FROM stage_velocity
    UNION ALL SELECT * FROM deploy_current
    UNION ALL SELECT * FROM deploy_hist_q1
    UNION ALL SELECT * FROM deploy_hist_q2
    UNION ALL SELECT * FROM deploy_hist_q3
    UNION ALL SELECT * FROM deploy_hist_q4;
