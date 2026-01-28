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
