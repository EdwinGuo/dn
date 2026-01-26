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


