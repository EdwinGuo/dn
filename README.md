# deepnative

## Option 1 — Simple & Practical (most common)

### ✅ List all tables + show 5 sample rows per table

```
WITH params AS (
  SELECT
    to_date('2024-01-01') AS start_dt,
    to_date('2024-12-31') AS end_dt
),

-- 1) Scope to TDAF customers (LOB membership / ability to use service)
tdaf_scope AS (
  SELECT DISTINCT
    CAST(cust_cust_id AS STRING)      AS cust_no,
    CAST(cust_cust_type_mn AS STRING) AS cust_type_mn
  FROM ra_fy_2025.tdaf_full_gen
  WHERE cust_cust_id IS NOT NULL
    AND cust_cust_type_mn IS NOT NULL
),

-- 2) Customer snapshot at start date (SCD slicing)
cust_start AS (
  SELECT DISTINCT
    cm.cust_no,
    cm.cust_type_mn
  FROM rafy2025_centralized.customer_master_cde cm
  CROSS JOIN params p
  WHERE cm.cust_no IS NOT NULL
    AND cm.cust_type_mn IS NOT NULL
    AND cm.etl_record_start_ts <= p.start_dt
    AND (cm.etl_record_end_ts IS NULL OR cm.etl_record_end_ts > p.start_dt)
    AND cm.etl_active_flag = true
    AND cm.death_dt IS NULL
),

-- 3) Customer snapshot at end date (SCD slicing)
cust_end AS (
  SELECT DISTINCT
    cm.cust_no,
    cm.cust_type_mn
  FROM rafy2025_centralized.customer_master_cde cm
  CROSS JOIN params p
  WHERE cm.cust_no IS NOT NULL
    AND cm.cust_type_mn IS NOT NULL
    AND cm.etl_record_start_ts <= p.end_dt
    AND (cm.etl_record_end_ts IS NULL OR cm.etl_record_end_ts > p.end_dt)
    AND cm.etl_active_flag = true
    AND cm.death_dt IS NULL
),

-- 4) Restrict both snapshots to TDAF scope
tdaf_start AS (
  SELECT s.cust_no, s.cust_type_mn
  FROM cust_start s
  INNER JOIN tdaf_scope t
    ON s.cust_no = t.cust_no
   AND s.cust_type_mn = t.cust_type_mn
),

tdaf_end AS (
  SELECT e.cust_no, e.cust_type_mn
  FROM cust_end e
  INNER JOIN tdaf_scope t
    ON e.cust_no = t.cust_no
   AND e.cust_type_mn = t.cust_type_mn
),

counts AS (
  SELECT
    (SELECT COUNT(*) FROM tdaf_start) AS cust_start_cnt,
    (SELECT COUNT(*) FROM tdaf_end)   AS cust_end_cnt
)

SELECT
  cust_start_cnt,
  cust_end_cnt,
  (cust_end_cnt - cust_start_cnt) AS delta_customers,
  CASE
    WHEN cust_start_cnt = 0 THEN NULL
    ELSE ROUND( (cust_end_cnt - cust_start_cnt) * 100.0 / cust_start_cnt, 4)
  END AS customer_growth_rate_pct
FROM counts;

```
