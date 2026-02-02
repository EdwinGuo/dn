# deepnative

## Option 1 — Simple & Practical (most common)

### ✅ List all tables + show 5 sample rows per table

```python
db_name = "hive_metastore.your_database_name"
sample_rows = 5

tables = spark.sql(f"SHOW TABLES IN {db_name}").collect()

for t in tables:
    table_name = f"{db_name}.{t.tableName}"
    print(f"\n===== TABLE: {table_name} =====")

    try:
        spark.sql(f"SELECT * FROM {table_name} LIMIT {sample_rows}").show(truncate=False)
    except Exception as e:
        print(f"⚠️ Failed to read table {table_name}: {e}")
```

### Why this works well

* Compatible with **legacy Hive Metastore**
* Uses `SHOW TABLES`
* Avoids scanning full tables
* Handles failures gracefully

---

## Option 2 — Include schema + row count (still safe)

```python
db_name = "hive_metastore.your_database_name"
sample_rows = 5

tables = spark.sql(f"SHOW TABLES IN {db_name}").collect()

for t in tables:
    table_name = f"{db_name}.{t.tableName}"
    print(f"\n===== TABLE: {table_name} =====")

    try:
        print("Schema:")
        spark.sql(f"DESCRIBE {table_name}").show(truncate=False)

        print("Sample rows:")
        spark.sql(f"SELECT * FROM {table_name} LIMIT {sample_rows}").show(truncate=False)

    except Exception as e:
        print(f"⚠️ Error reading {table_name}: {e}")
```

---

## Option 3 — PySpark DataFrame API (no SQL strings)

Useful if you prefer **pure PySpark**:

```python
db_name = "hive_metastore.your_database_name"
sample_rows = 5

tables = spark.catalog.listTables(db_name)

for t in tables:
    table_name = f"{db_name}.{t.name}"
    print(f"\n===== TABLE: {table_name} =====")

    try:
        df = spark.table(table_name)
        df.show(sample_rows, truncate=False)
    except Exception as e:
        print(f"⚠️ Failed to read {table_name}: {e}")
```

---

## Option 4 — Filter only managed / external tables

```python
tables = spark.catalog.listTables("hive_metastore.your_database_name")

for t in tables:
    if t.tableType in ("MANAGED", "EXTERNAL"):
        print(f"{t.name} ({t.tableType})")
```

---

## ⚠️ Important safety notes (very relevant in Databricks)

### 1️⃣ Do NOT do `count(*)` blindly

```sql
SELECT COUNT(*) FROM big_table  -- ❌ expensive
```

Use:

```sql
DESCRIBE DETAIL table_name
```

### 2️⃣ Some tables may fail

Common causes:

* Corrupted metadata
* External table path deleted
* Permission issue

Your `try/except` prevents notebook failure.

---

## Optional — Pretty output for large databases

If you want **structured output**:

```python
from pyspark.sql import Row

results = []

for t in spark.catalog.listTables("hive_metastore.your_database_name"):
    try:
        df = spark.table(f"hive_metastore.your_database_name.{t.name}")
        sample = df.limit(3).toJSON().collect()
        results.append(Row(table=t.name, sample=sample))
    except:
        results.append(Row(table=t.name, sample=["<error>"]))

display(spark.createDataFrame(results))
```


1.1
```
Yep — below is a **Databricks / Spark SQL** template that matches what your RA sheet describes:

* **Population** = “customers within unit” (from your RESL/customer base table)
* **Rated anchor** = `rafy2025_centralized.haha_rated_cust_cde_1_1_2025`
* **Unrated/unscored** = customers in population **missing** from rated anchor (LEFT ANTI join)

I’ll give you two versions:

1. **Recommended** (LEFT ANTI join)
2. **Debug-friendly** (LEFT JOIN + IS NULL)

> Replace table/column names in the “TODO” spots if your base table uses `cas_cust_id` / `cbs_cust_id` instead of `cust_intrl_id`.

---

## Version A (recommended): LEFT ANTI join

```sql
-- Metric: Total number of unscored/unrated customers in the unit
-- Method: customers in population NOT present in rated-customer view

WITH population AS (
  SELECT DISTINCT
         -- TODO: use the customer ID you can join with rated table
         cust_intrl_id
  FROM ra_fy_2025.resl_full_gen
  WHERE 1=1
    -- TODO: apply "within unit" filters used by your org (examples below)
    -- AND segment = 'Canadian Personal Banking'
    -- AND assessable_unit_name = 'Real Estate Secured Lending (RESL)'

    -- Optional: restrict to active/customers if you have status flags
    -- AND cas_active_acct_ct > 0

    -- Optional: ensure customer id not null
    AND cust_intrl_id IS NOT NULL
),

rated AS (
  SELECT DISTINCT
         cust_intrl_id
  FROM rafy2025_centralized.haha_rated_cust_cde_1_1_2025
  WHERE cust_intrl_id IS NOT NULL
)

SELECT
  COUNT(*) AS unscored_unrated_customer_cnt
FROM (
  SELECT p.cust_intrl_id
  FROM population p
  LEFT ANTI JOIN rated r
    ON p.cust_intrl_id = r.cust_intrl_id
) x;
```

---

## Version B (debug-friendly): LEFT JOIN + NULL check

```sql
WITH population AS (
  SELECT DISTINCT cust_intrl_id
  FROM ra_fy_2025.resl_full_gen
  WHERE cust_intrl_id IS NOT NULL
),

rated AS (
  SELECT DISTINCT cust_intrl_id
  FROM rafy2025_centralized.haha_rated_cust_cde_1_1_2025
  WHERE cust_intrl_id IS NOT NULL
),

unrated AS (
  SELECT p.cust_intrl_id
  FROM population p
  LEFT JOIN rated r
    ON p.cust_intrl_id = r.cust_intrl_id
  WHERE r.cust_intrl_id IS NULL
)

SELECT COUNT(DISTINCT cust_intrl_id) AS unscored_unrated_customer_cnt
FROM unrated;
```

---

## Optional: Breakdown by product type (acct_type_na) for validation

This helps you sanity-check the number and find weird spikes.

```sql
WITH population AS (
  SELECT DISTINCT
         cust_intrl_id,
         acct_type_na
  FROM ra_fy_2025.resl_full_gen
  WHERE cust_intrl_id IS NOT NULL
    AND acct_type_na IS NOT NULL
),

rated AS (
  SELECT DISTINCT cust_intrl_id
  FROM rafy2025_centralized.haha_rated_cust_cde_1_1_2025
  WHERE cust_intrl_id IS NOT NULL
),

unrated AS (
  SELECT p.cust_intrl_id, p.acct_type_na
  FROM population p
  LEFT ANTI JOIN rated r
    ON p.cust_intrl_id = r.cust_intrl_id
)

SELECT
  acct_type_na,
  COUNT(DISTINCT cust_intrl_id) AS unrated_customers
FROM unrated
GROUP BY acct_type_na
ORDER BY unrated_customers DESC;
```

---

## One important thing to confirm (so your join is correct)

In your screenshot, the rated view has **three IDs**:

* `cust_intrl_id`
* `v_global_id`
* `v_entity_id`

Your base table must use **one of these**. If `ra_fy_2025.resl_full_gen` doesn’t have `cust_intrl_id`, you can switch the join key to `v_global_id` or map via your entity mapping table (your RA logic mentioned an ER mapping view).

If you paste your base table’s `DESCRIBE ra_fy_2025.resl_full_gen` output (just the column names), I’ll rewrite the query with the **exact join key + filters**.



```
-- Spark SQL version of your PySpark logic

WITH resl AS (
  SELECT
    -- original columns kept; rename the two you renamed in PySpark
    *,
    cust_cust_id    AS cust_no,
    cust_cust_type_mn AS cust_type_mn
  FROM ra_fy_2025.resl_full_gen
),

hrc AS (
  SELECT *
  FROM rafy2025_centralized.haha_HRC_Tier_1_2_CDE_1_2_2025
),

hrc_cif AS (
  SELECT
    *,
    -- last 9 chars of v_entity_id (Spark SQL supports negative start)
    substring(v_entity_id, -9, 9) AS cust_no,

    -- 8th char logic: if '1' then 'N' else 'P'
    CASE
      WHEN substring(v_entity_id, 8, 1) = '1' THEN 'N'
      ELSE 'P'
    END AS cust_type_mn
  FROM hrc
  WHERE v_entity_id LIKE 'CIF%'
),

resl_hrc AS (
  SELECT
    h.*,
    r.*
  FROM hrc_cif h
  INNER JOIN resl r
    ON h.cust_no = r.cust_no
   AND h.cust_type_mn = r.cust_type_mn
  WHERE h.risk_rating IN ('Tier 1', 'Tier 2')
)

-- 1) Equivalent to hrc_cif.count()
SELECT COUNT(*) AS hrc_cif_cnt
FROM hrc_cif;

-- 2) If you want the metric-style output you commented out:
-- SELECT COUNT(DISTINCT cust_intrl_id) AS distinct_cust_intrl_id_cnt
-- FROM resl_hrc;


```


```
-- Databricks / Spark SQL equivalent

WITH resl AS (
  SELECT
    *,
    cust_cust_id      AS cust_no,
    cust_cust_type_mn AS cust_type_mn
  FROM ra_fy_2025.resl_full_gen
),

hrc AS (
  SELECT *
  FROM rafy2025_centralized.haha_HRC_CDE_1_3_2025
),

hrc_cif AS (
  SELECT
    *,
    -- last 9 characters of v_entity_id
    substring(v_entity_id, -9, 9) AS cust_no,

    -- replicate: when(v_cust_type_cd == 'ORG', 'N').otherwise('P')
    CASE
      WHEN v_cust_type_cd = 'ORG' THEN 'N'
      ELSE 'P'
    END AS cust_type_mn
  FROM hrc
  WHERE v_entity_id LIKE 'CIF%'
),

resl_hrc AS (
  SELECT
    h.*,
    r.*
  FROM hrc_cif h
  INNER JOIN resl r
    ON h.cust_no = r.cust_no
   AND h.cust_type_mn = r.cust_type_mn
)

-- Equivalent to hrc_cif.count()
SELECT COUNT(*) AS hrc_cif_cnt
FROM hrc_cif;

-- If you also want the join row count (similar to checking resl_hrc)
-- SELECT COUNT(*) AS resl_hrc_cnt FROM resl_hrc;

```

```
from typing import List

def find_tables_with_columns(db: str, keywords: List[str] = None) -> List[str]:
    """
    For a given database/schema, list all tables and return those that contain
    any column whose name includes one of the keywords (case-insensitive).

    keywords default: ["intrl", "entity"]
    """
    if keywords is None:
        keywords = ["intrl", "entity"]

    keywords = [k.lower() for k in keywords]

    # List tables in the database
    tables_df = spark.sql(f"SHOW TABLES IN {db}")
    tables = [r.tableName for r in tables_df.collect()]

    matched = []

    for t in tables:
        full_name = f"{db}.{t}"
        try:
            cols = [c.lower() for c in spark.table(full_name).columns]
            # match if any column contains any keyword as substring
            if any(any(k in col for k in keywords) for col in cols):
                print(full_name)
                matched.append(full_name)
        except Exception as e:
            # Skip views / inaccessible tables / permission issues
            # You can log e if you want:
            # print(f"Skipping {full_name}: {e}")
            continue

    return matched

# Example usage
matches = find_tables_with_columns("my_db")

```
