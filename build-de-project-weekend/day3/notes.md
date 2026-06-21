# Day 3 — Gold Layer: Aggregations & Analytics

> **Project Day:** 3 · **Layer:** Gold  
> **Study Window:** 3 hours  
> **Theme:** Silver → aggregate, model, and produce business-ready tables

---

## 1. What is the Gold Layer?

Gold is the **business-ready** layer — fully aggregated, modelled, and named for business users and dashboards. It answers specific questions:

- "What was total revenue per day?"
- "Which customers are about to churn?"
- "Which product category drives the most revenue?"

Gold tables are **wide, denormalized** — no joins needed at query time. Analysts and BI tools read directly from Gold.

---

## 2. Daily & Monthly Sales Aggregations (Pandas)

### Daily sales
```python
df_orders["order_day"] = pd.to_datetime(df_orders["order_date"]).dt.date

daily = (
    df_orders[~df_orders["is_cancelled"]]          # exclude cancelled
    .groupby("order_day")
    .agg(
        total_revenue    = ("total_amount", "sum"),
        order_count      = ("order_id",     "count"),
        unique_customers = ("customer_id",  "nunique"),
    )
    .reset_index()
)
daily["avg_order_value"] = (daily["total_revenue"] / daily["order_count"]).round(2)
```

`.dt.date` — extracts Python `date` from a datetime column, dropping the time component.  
`~df["is_cancelled"]` — tilde `~` is boolean NOT — keeps rows where `is_cancelled == False`.

### Monthly by state
```python
df["order_month"] = pd.to_datetime(df["order_date"]).dt.to_period("M").astype(str)
# "M" period = "2024-01", "2024-02", etc.

monthly = df.groupby(["order_month", "state"]).agg(
    total_revenue = ("total_amount", "sum"),
    order_count   = ("order_id",     "count"),
).reset_index()
```

---

## 3. RFM Customer Segmentation

RFM is a classic customer analytics model used in every retail/e-commerce company:

| Dimension | Definition | Lower is Better? |
|-----------|-----------|-----------------|
| **R**ecency | Days since last purchase | Yes (recent = good) |
| **F**requency | Number of orders placed | No (more = better) |
| **M**onetary | Total spend | No (more = better) |

### Step 1 — Compute raw RFM values
```python
rfm = (
    df_orders[~df_orders["is_cancelled"]]
    .groupby("customer_id")
    .agg(
        last_order_date = ("order_date",   "max"),
        frequency       = ("order_id",     "count"),
        monetary        = ("total_amount", "sum"),
    )
    .reset_index()
)
rfm["recency_days"] = (pd.Timestamp.today() - rfm["last_order_date"]).dt.days
```

### Step 2 — Score 1–4 with qcut
```python
rfm["r_score"] = pd.qcut(rfm["recency_days"], q=4, labels=[4, 3, 2, 1]).astype(int)
# Label 4 = lowest recency_days = most recent = best
# Label 1 = highest recency_days = oldest = worst

rfm["f_score"] = pd.qcut(rfm["frequency"].rank(method="first"), q=4, labels=[1, 2, 3, 4]).astype(int)
rfm["m_score"] = pd.qcut(rfm["monetary"].rank(method="first"),  q=4, labels=[1, 2, 3, 4]).astype(int)
rfm["rfm_score"] = rfm["r_score"] + rfm["f_score"] + rfm["m_score"]
```

`pd.qcut()` — quantile-based binning. Splits data into `q` equal-size buckets.  
`.rank(method="first")` — needed when there are ties (qcut requires unique bin edges).

### Step 3 — Assign segment labels
```python
def assign_segment(row):
    r, f, m = row["r_score"], row["f_score"], row["m_score"]
    if r >= 4 and f >= 3 and m >= 3:
        return "Champion"
    elif f >= 3 and m >= 3:
        return "Loyal"
    elif r <= 2 and f >= 2:
        return "At Risk"
    else:
        return "Lost"

rfm["segment"] = rfm.apply(assign_segment, axis=1)
```

---

## 4. PySpark Gold Aggregations

### Read from Parquet (Silver output)
```python
df_items = spark.read.parquet("data/processed/silver_order_items.parquet")
```

### GroupBy + multi-agg
```python
df_revenue = (
    df_items.groupBy("product_id")
    .agg(
        F.round(F.sum("line_total"), 2).alias("total_revenue"),
        F.count("item_id").alias("total_units_sold"),
        F.countDistinct("order_id").alias("total_orders"),
    )
    .orderBy(F.col("total_revenue").desc())
)
```

### Cumulative revenue by month (window)
```python
win = Window.partitionBy("product_id").orderBy("order_month").rowsBetween(
    Window.unboundedPreceding, Window.currentRow
)
df_monthly = df_monthly.withColumn(
    "cumulative_revenue",
    F.round(F.sum("monthly_revenue").over(win), 2)
)
```

`rowsBetween(unboundedPreceding, currentRow)` — sum all rows from the first row in the partition up to the current row = running total.

---

## 5. Advanced SQL for Gold

### RANK() — rank by value
```sql
SELECT customer_id, full_name, total_revenue,
    RANK() OVER (ORDER BY total_revenue DESC) AS revenue_rank
FROM customer_revenue;
```
`RANK()` — tied rows get the same rank; the next rank number is skipped (1, 1, 3...).  
`DENSE_RANK()` — tied rows get the same rank; no gap (1, 1, 2...).  
`ROW_NUMBER()` — always unique, no ties.

### LAG() — month-over-month growth
```sql
SELECT
    order_month,
    revenue,
    LAG(revenue) OVER (ORDER BY order_month) AS prev_revenue,
    ROUND(100.0 * (revenue - LAG(revenue) OVER (ORDER BY order_month))
          / LAG(revenue) OVER (ORDER BY order_month), 1) AS growth_pct
FROM monthly_revenue;
```
`LAG(col, n)` — value from `n` rows before the current row. `n=1` (default) = previous row.

### Anti-Join — "never ordered" products
```sql
SELECT p.product_id, p.product_name
FROM silver.products p
LEFT JOIN silver.order_items oi ON p.product_id = oi.product_id
WHERE oi.product_id IS NULL;   -- NULL = no matching row in order_items
```
This is the standard anti-join pattern. `LEFT JOIN` keeps all products; `WHERE right_table.key IS NULL` keeps only those with no match.

### Cohort Analysis — months to first order
```sql
WITH first_orders AS (
    SELECT customer_id, MIN(order_date) AS first_order_date
    FROM silver.orders WHERE is_cancelled = FALSE
    GROUP BY customer_id
)
SELECT
    c.customer_id,
    TO_CHAR(DATE_TRUNC('month', c.signup_date), 'YYYY-MM') AS signup_month,
    DATE_PART('month', AGE(fo.first_order_date, c.signup_date)) AS months_to_first_order
FROM silver.customers c
LEFT JOIN first_orders fo ON c.customer_id = fo.customer_id;
```

---

## 6. Key Concepts Summary

| Concept | Code Pattern | Use |
|---------|-------------|-----|
| Daily aggregation | `groupby("date").agg(sum, count)` | Revenue, orders per day |
| Period conversion | `.dt.to_period("M").astype(str)` | Monthly rollup |
| RFM scoring | `pd.qcut(col, q=4, labels=[...])` | Customer segmentation |
| PySpark groupBy | `groupBy().agg(F.sum(), F.count())` | Large-scale aggregation |
| Cumulative sum | `F.sum().over(Window...rowsBetween(...unbounded..currentRow))` | Running totals |
| SQL RANK | `RANK() OVER (ORDER BY col DESC)` | Top-N ranking |
| SQL LAG | `LAG(col) OVER (ORDER BY date)` | Period-over-period comparison |
| Anti-join | `LEFT JOIN ... WHERE right.key IS NULL` | "Never appeared in" queries |

---

## 7. Common Mistakes

| Mistake | Fix |
|---------|-----|
| Including cancelled orders in revenue | Always filter `~is_cancelled` before aggregating |
| `pd.qcut` with ties → error | Add `.rank(method="first")` before qcut for F and M scores |
| `RANK()` vs `ROW_NUMBER()` confusion | RANK skips after ties; ROW_NUMBER never ties |
| `LAG()` on first row = NULL | Handle with `CASE WHEN prev IS NULL THEN NULL END` or `COALESCE` |
| Gold table already exists → SQL error | Use `CREATE TABLE IF NOT EXISTS` or `DROP TABLE IF EXISTS` first |

---

## 8. Quick Reference

```python
# Daily rollup
df["day"] = pd.to_datetime(df["date"]).dt.date
daily = df.groupby("day").agg(revenue=("amount","sum"), orders=("id","count")).reset_index()

# Monthly
df["month"] = pd.to_datetime(df["date"]).dt.to_period("M").astype(str)

# RFM
rfm = df.groupby("customer_id").agg(last=("date","max"), freq=("id","count"), spend=("amount","sum")).reset_index()
rfm["recency"] = (pd.Timestamp.today() - rfm["last"]).dt.days
rfm["r"] = pd.qcut(rfm["recency"], 4, labels=[4,3,2,1]).astype(int)
rfm["f"] = pd.qcut(rfm["freq"].rank(method="first"), 4, labels=[1,2,3,4]).astype(int)
rfm["m"] = pd.qcut(rfm["spend"].rank(method="first"), 4, labels=[1,2,3,4]).astype(int)

# PySpark cumulative
win = Window.partitionBy("product_id").orderBy("month").rowsBetween(Window.unboundedPreceding, Window.currentRow)
df = df.withColumn("cum_revenue", F.sum("revenue").over(win))
```

```sql
-- RANK
RANK() OVER (ORDER BY revenue DESC)

-- LAG
LAG(revenue) OVER (ORDER BY month)

-- Anti-join
LEFT JOIN t2 ON t1.id = t2.id WHERE t2.id IS NULL

-- Running total
SUM(revenue) OVER (ORDER BY date ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)
```
