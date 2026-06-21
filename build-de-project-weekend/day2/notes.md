# Day 2 — Silver Layer: Clean & Conform

> **Project Day:** 2 · **Layer:** Silver  
> **Study Window:** 3 hours  
> **Theme:** Read from Bronze → clean, type-cast, enrich → write to Silver

---

## 1. What is the Silver Layer?

Silver is the **cleaned, conformed** layer. Data from Bronze is:
- **Typed correctly** — strings become dates, floats, booleans
- **Stripped and normalized** — whitespace removed, email lowercased, category uppercased
- **Deduplicated** — primary key duplicates removed
- **Enriched** — derived columns added (`full_name`, `line_total`, `is_low_stock`)
- **Joined** — small lookup joins (orders + customer name)

Silver rows are **trustworthy** — analysts and Gold queries can rely on these types and values.

---

## 2. Reading from Postgres into Pandas

```python
import pandas as pd
from sqlalchemy import create_engine

engine = create_engine("postgresql+psycopg2://postgres:hariom@localhost:5432/postgres")

df = pd.read_sql('SELECT * FROM bronze.customers', engine)
# or with a full query:
df = pd.read_sql('SELECT * FROM bronze.orders WHERE status != \'cancelled\'', engine)
```

---

## 3. String Cleaning

```python
# Strip leading/trailing whitespace
df["first_name"] = df["first_name"].str.strip()

# Lowercase
df["email"] = df["email"].str.lower()

# Uppercase
df["category"] = df["category"].str.upper()

# Replace — remove non-digit chars from phone
df["phone_clean"] = df["phone"].str.replace(r"[^\d]", "", regex=True)

# Extract domain from email
df["email_domain"] = df["email"].str.split("@").str[1]
```

`.str` accessor works on any string Series — gives you the full `str` method suite vectorized over all rows.

---

## 4. Type Casting

```python
# String → float
df["total_amount"] = pd.to_numeric(df["total_amount"], errors="coerce")
# errors="coerce" → bad values become NaN instead of raising

# String → datetime
df["order_date"] = pd.to_datetime(df["order_date"], errors="coerce")

# String → bool
df["is_active"] = df["is_active"].astype(str).str.lower().map({"true": True, "false": False})

# Float → int (after filling nulls)
df["stock_qty"] = df["stock_qty"].fillna(0).astype(int)
```

Always use `errors="coerce"` for production pipelines — bad values silently become NaN, letting the pipeline continue.

---

## 5. Derived / Computed Columns

```python
# Concatenate
df["full_name"] = df["first_name"] + " " + df["last_name"]

# Boolean flag
df["is_cancelled"] = df["status"] == "cancelled"

# Low stock flag
df["is_low_stock"] = df["stock_qty"] < df["reorder_level"]

# Days since a date
from datetime import date
today = pd.Timestamp(date.today())
df["days_since_update"] = (today - df["last_updated"]).dt.days
```

---

## 6. Deduplication

```python
before = len(df)
df = df.drop_duplicates(subset=["customer_id"])    # keep first occurrence
print(f"Deduped: {before} → {len(df)}")
```

For Silver, always deduplicate on the primary key before writing. Bronze may contain duplicates from re-runs or append bugs.

---

## 7. Lookup Joins with map()

When joining a small lookup (e.g., customer name onto orders), `.map()` is faster and cleaner than a full merge:

```python
# Build a lookup dict from customers
name_map = df_customers.set_index("customer_id")["full_name"]
# name_map is a Series: customer_id → full_name

# Map onto orders
df_orders["customer_name"] = df_orders["customer_id"].map(name_map)
# Unmatched IDs → NaN automatically
```

Use `.merge()` when you need multiple columns from the lookup table.

---

## 8. PySpark in Silver — Window Functions

PySpark is used when transformations are complex or the data is large.

### Setup
```python
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window

spark = SparkSession.builder.appName("Silver").master("local[*]").getOrCreate()
df = spark.read.option("header", "true").csv("data/raw/order_items.csv")
```

### Type casting in PySpark
```python
from pyspark.sql.types import DoubleType, IntegerType

df = df.withColumn("quantity",   F.col("quantity").cast(IntegerType()))
df = df.withColumn("unit_price", F.col("unit_price").cast(DoubleType()))
```

### Derived column
```python
df = df.withColumn(
    "line_total",
    F.round(F.col("quantity") * F.col("unit_price") * (1 - F.col("discount_pct") / 100), 2)
)
```

### Window function — rank within group
```python
window = Window.partitionBy("order_id").orderBy(F.col("line_total").desc())
df = df.withColumn("item_rank_in_order", F.rank().over(window))
```

**Mental model:** `PARTITION BY order_id` = reset rank at each order boundary. `ORDER BY line_total DESC` = rank 1 = highest value item.

### Running total (cumulative sum)
```python
window_run = Window.partitionBy("order_id").orderBy("item_id").rowsBetween(
    Window.unboundedPreceding, Window.currentRow
)
df = df.withColumn("running_order_total", F.sum("line_total").over(window_run))
```

### Write to Parquet
```python
df.coalesce(1).write.mode("overwrite").parquet("data/processed/silver_order_items.parquet")
```

`coalesce(1)` — merge all partitions into one file. Good for small datasets. On large datasets, skip this.

---

## 9. SQL Views on Silver

Views are virtual tables — no data stored, just a saved query.

```sql
CREATE OR REPLACE VIEW silver.v_order_summary AS
SELECT
    o.order_id,
    o.customer_id,
    o.customer_name,
    c.city,
    o.order_date,
    o.status,
    o.total_amount
FROM silver.orders o
LEFT JOIN silver.customers c ON o.customer_id = c.customer_id;
```

Use views for:
- Repeated joins you don't want to copy-paste
- Giving analysts a stable query surface even if underlying tables change
- Silver-to-Gold bridge queries

---

## 10. Key Concepts Summary

| Concept | Tool | Use |
|---------|------|-----|
| String normalize | `pandas .str.*` | strip, lower, upper, replace, split |
| Type cast | `pd.to_numeric`, `pd.to_datetime` | Safe conversion with `errors="coerce"` |
| Dedup | `drop_duplicates(subset=[pk])` | Remove duplicate primary keys |
| Derived col | `df["x"] = expression` | full_name, line_total, is_low_stock |
| Lookup join | `series.map(lookup_series)` | Fast scalar joins |
| PySpark window | `Window.partitionBy().orderBy()` | rank, running total, lag |
| Parquet write | `df.write.parquet(path)` | Columnar, compressed, fast |
| SQL view | `CREATE OR REPLACE VIEW` | Reusable join queries |

---

## 11. Common Mistakes

| Mistake | Fix |
|---------|-----|
| `pd.to_numeric` without `errors="coerce"` | One bad value crashes the whole column cast |
| Forgetting `.fillna(0)` before `.astype(int)` | NaN cannot be cast to int — raises ValueError |
| `map()` vs `merge()` confusion | Use `map()` for single-column lookups; `merge()` for multi-column |
| PySpark: `col("x") + col("y")` with nulls | Any null in arithmetic = null result; use `fillna` first |
| Writing PySpark result to Postgres directly | Requires JDBC jar; easier to `toPandas()` → `to_sql()` |

---

## 12. Quick Reference

```python
# Read from bronze
df = pd.read_sql('SELECT * FROM bronze.orders', engine)

# Clean strings
df["email"] = df["email"].str.strip().str.lower()

# Type cast
df["amount"] = pd.to_numeric(df["amount"], errors="coerce")
df["date"]   = pd.to_datetime(df["date"], errors="coerce")

# Derived columns
df["full_name"]   = df["first_name"] + " " + df["last_name"]
df["is_cancelled"] = df["status"] == "cancelled"

# Dedup
df = df.drop_duplicates(subset=["order_id"])

# Lookup join
df["customer_name"] = df["customer_id"].map(df_cust.set_index("customer_id")["full_name"])

# Write to silver
df.to_sql("orders", engine, schema="silver", if_exists="replace", index=False)

# PySpark window
window = Window.partitionBy("order_id").orderBy(F.col("line_total").desc())
df = df.withColumn("rank", F.rank().over(window))
```
