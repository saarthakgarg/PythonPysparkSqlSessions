# Day 4 — Orchestration, Data Quality & Incremental Load

> **Project Day:** 4 · **Layer:** All (end-to-end)  
> **Study Window:** 3 hours  
> **Theme:** Wire up the pipeline, validate data quality, handle new data incrementally

---

## 1. Pipeline Orchestration — The Basics

An orchestrator is a script that runs stages in the right order, logs progress, and handles failures gracefully.

```python
import time
from datetime import datetime

def log(msg):
    print(f"[{datetime.now().strftime('%H:%M:%S')}] {msg}")

def run_stage(name, fn):
    log(f"START  ── {name}")
    t0 = time.time()
    try:
        fn()
        log(f"OK     ── {name}  ({round(time.time()-t0, 1)}s)")
        return True
    except Exception as e:
        log(f"FAILED ── {name}  ERROR: {e}")
        return False
```

A production orchestrator (Airflow, Prefect, Dagster) does the same thing — defines stages, runs them in order (or parallel), and tracks success/failure. Understanding this manual version is the foundation.

---

## 2. Data Quality Checks

Data quality (DQ) checks are assertions about your data. They catch bad data before it reaches Gold or dashboards.

### Four types of DQ checks

**1. Null checks — required columns must not be empty**
```python
nulls = df["customer_id"].isna().sum()
assert nulls == 0, f"customer_id has {nulls} nulls"
```

**2. Duplicate checks — primary keys must be unique**
```python
dups = df["order_id"].duplicated().sum()
assert dups == 0, f"order_id has {dups} duplicates"
```

**3. Referential integrity — foreign keys must exist in parent table**
```python
# Are all order.customer_id values in customers?
orphans = ~df_orders["customer_id"].isin(df_customers["customer_id"])
assert orphans.sum() == 0, f"{orphans.sum()} orders with unknown customer_id"
```
`~series.isin(set)` — tilde inverts the boolean. `True` = not found = orphan.

**4. Value range — numeric values must be sensible**
```python
bad = (df["total_amount"] <= 0).sum()
assert bad == 0, f"{bad} orders with non-positive amount"
```

### Collecting results without crashing
Use a results list instead of `assert` so all checks run even if some fail:
```python
results = []

def check(name, passed, detail=""):
    results.append({"Check": name, "Status": "PASS" if passed else "FAIL", "Detail": detail})

check("orders.customer_id not null", df["customer_id"].isna().sum() == 0)
check("orders.order_id unique",      df["order_id"].duplicated().sum() == 0)

from tabulate import tabulate
print(tabulate(pd.DataFrame(results), headers="keys", tablefmt="simple"))
```

---

## 3. Watermark-Based Incremental Loading

**Full load** — truncate and reload everything. Simple but slow for large tables.  
**Incremental load** — only process rows newer than the last successful run.

### Watermark pattern
```python
from sqlalchemy import text

# Step 1: find the high watermark (latest value already processed)
with engine.connect() as conn:
    wm = conn.execute(text("SELECT MAX(order_date) FROM bronze.orders")).scalar()
print(f"Watermark: {wm}")

# Step 2: generate or receive new data
df_new = get_new_orders_since(wm)

# Step 3: filter — only rows strictly newer than watermark
df_to_load = df_new[df_new["order_date"] > str(wm)]

# Step 4: append (not replace!)
df_to_load.to_sql("orders", engine, schema="bronze", if_exists="append", index=False)
```

The watermark is always `MAX(date_column)` from the target table. Store it in a variable — never hard-code a date.

---

## 4. Upsert with ON CONFLICT DO UPDATE

An upsert inserts a new row or updates an existing one if the primary key already exists.

```sql
INSERT INTO silver.orders (order_id, status, total_amount, ...)
VALUES (:order_id, :status, :total_amount, ...)
ON CONFLICT (order_id) DO UPDATE SET
    status       = EXCLUDED.status,
    total_amount = EXCLUDED.total_amount;
```

`EXCLUDED` — a virtual table representing the row that was attempted to be inserted. You can reference `EXCLUDED.column_name` in the `DO UPDATE` clause.

**Requires:** A `PRIMARY KEY` or `UNIQUE` constraint on the conflict column.

```python
# Add PK constraint if missing (run once):
engine.execute("ALTER TABLE silver.orders ADD PRIMARY KEY (order_id)")
```

### Full upsert in Python
```python
with engine.connect() as conn:
    for _, row in df_new.iterrows():
        conn.execute(text("""
            INSERT INTO silver.orders (order_id, status, total_amount)
            VALUES (:order_id, :status, :total_amount)
            ON CONFLICT (order_id) DO UPDATE SET
                status       = EXCLUDED.status,
                total_amount = EXCLUDED.total_amount
        """), {"order_id": row["order_id"], "status": row["status"], "total_amount": row["total_amount"]})
    conn.commit()
```

---

## 5. Business Report SQL Patterns

### Overall KPIs
```sql
SELECT
    COUNT(DISTINCT order_id)                                   AS total_orders,
    ROUND(SUM(total_amount) FILTER (WHERE NOT is_cancelled)::NUMERIC, 2) AS gross_revenue,
    ROUND(AVG(total_amount) FILTER (WHERE NOT is_cancelled)::NUMERIC, 2) AS avg_order_value,
    COUNT(DISTINCT customer_id)                                AS active_customers
FROM silver.orders;
```

`FILTER (WHERE ...)` — PostgreSQL aggregate filter. Applies the WHERE only to that specific aggregate, not the whole query. Cleaner than CASE WHEN inside SUM.

### Revenue share with window
```sql
SELECT
    category,
    SUM(line_total)                                    AS revenue,
    ROUND(100.0 * SUM(line_total) / SUM(SUM(line_total)) OVER (), 1) AS share_pct
FROM silver.order_items oi
JOIN silver.products p ON oi.product_id = p.product_id
GROUP BY category;
```
`SUM(SUM(line_total)) OVER ()` — nested aggregate + window function. The inner `SUM` aggregates per group; the outer `SUM(...) OVER ()` sums across all groups = grand total.

### Running customer count
```sql
SELECT
    TO_CHAR(DATE_TRUNC('month', signup_date), 'YYYY-MM')   AS month,
    COUNT(*)                                                AS new_customers,
    SUM(COUNT(*)) OVER (ORDER BY DATE_TRUNC('month', signup_date)) AS cumulative
FROM silver.customers
GROUP BY DATE_TRUNC('month', signup_date);
```

---

## 6. Key Concepts Summary

| Concept | Pattern | Use |
|---------|---------|-----|
| Orchestrator | `try/except` stage runner | Run pipeline in order, log, report |
| Null check | `df[col].isna().sum() == 0` | Required field validation |
| Duplicate check | `df[col].duplicated().sum() == 0` | PK uniqueness |
| Referential integrity | `~df[fk].isin(parent[pk])` | FK validation |
| Range check | `(df[col] <= 0).sum() == 0` | Business rule validation |
| Watermark | `MAX(date_col)` from target table | Incremental boundary |
| Incremental filter | `df[df["date"] > watermark]` | Only new rows |
| Append mode | `if_exists="append"` | Add rows, don't replace |
| Upsert | `ON CONFLICT (pk) DO UPDATE SET` | Insert or update |
| Aggregate filter | `SUM(x) FILTER (WHERE cond)` | Conditional aggregation |

---

## 7. Common Mistakes

| Mistake | Fix |
|---------|-----|
| `assert` in DQ — crashes on first failure | Use a results list; `assert` is for tests not pipelines |
| Watermark from Silver instead of Bronze | Read watermark from the target table you're loading into |
| `if_exists="replace"` in incremental load | Deletes all existing data — must use `"append"` |
| Upsert without PK constraint | `ON CONFLICT` silently fails or errors without a unique constraint |
| `EXCLUDED` typo in SQL | `EXCLUDED` is a keyword — not a table name, can't be aliased |
| Missing `conn.commit()` after upsert | Row changes not persisted — always commit explicitly |

---

## 8. Quick Reference

```python
# Orchestrator stage
def run_stage(name, fn):
    try:
        fn()
        return True
    except Exception as e:
        print(f"FAILED: {e}")
        return False

# DQ checks
nulls = df["col"].isna().sum()
dups  = df["pk"].duplicated().sum()
orphans = (~df["fk"].isin(parent["pk"])).sum()
bad_range = (df["amount"] <= 0).sum()

# Watermark
wm = engine.execute("SELECT MAX(order_date) FROM bronze.orders").scalar()
df_new = df_new[df_new["order_date"] > str(wm)]

# Incremental append
df_new.to_sql("orders", engine, schema="bronze", if_exists="append", index=False)

# Upsert SQL
"""
INSERT INTO silver.orders (order_id, status)
VALUES (:order_id, :status)
ON CONFLICT (order_id) DO UPDATE SET status = EXCLUDED.status
"""
```

```sql
-- Conditional aggregate
SUM(amount) FILTER (WHERE status = 'delivered')

-- Revenue share
100.0 * SUM(x) / SUM(SUM(x)) OVER ()

-- Cumulative count
SUM(COUNT(*)) OVER (ORDER BY month)
```
