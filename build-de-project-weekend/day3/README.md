# Day 3 — Gold Layer: Aggregations & Analytics (3 hours)

## What You'll Learn
- Building business-ready aggregation tables from silver
- RFM customer segmentation (Recency, Frequency, Monetary)
- PySpark: multi-level aggregations, cumulative window functions
- Advanced SQL: LAG/LEAD, RANK, CTEs, anti-joins, cohort analysis

## 3-Hour Schedule

| Time        | Script                            | Key Concepts                                       |
|-------------|-----------------------------------|----------------------------------------------------|
| 0:00–0:45   | `01_gold_sales_summary.py`        | `groupby`, `agg`, `pd.Period`, daily/monthly rollups |
| 0:45–1:30   | `02_gold_customer_segments.py`    | `pd.qcut`, scoring logic, `apply()`, RFM model    |
| 1:30–2:15   | `03_gold_pyspark_aggregations.py` | `groupBy`, `agg`, cumulative `sum` over window    |
| 2:15–3:00   | `04_gold_sql_analytics.sql`       | `WITH`, `LAG`, `RANK`, `LEFT JOIN ... WHERE NULL` |

## Expected Outputs
- `gold.daily_sales`              — revenue + order count per day
- `gold.monthly_sales_by_state`   — revenue per month per state
- `gold.customer_rfm_segments`    — RFM scores + segment labels
- `gold.product_revenue`          — total revenue per product
- `gold.product_revenue_monthly`  — monthly revenue with cumulative column
- `gold.top_customers`            — ranked by total revenue
- `gold.category_revenue`         — revenue share by category
- `data/processed/gold_product_revenue.parquet`

## Run Commands
```bash
python day3/01_gold_sales_summary.py
python day3/02_gold_customer_segments.py
python day3/03_gold_pyspark_aggregations.py
psql -U postgres -d postgres -f day3/04_gold_sql_analytics.sql
```

## Practice Challenges
1. Add a `revenue_7day_rolling_avg` column to `gold.daily_sales` using pandas rolling window
2. Extend RFM: add a 5th segment "New Customer" for signup within last 60 days
3. In PySpark: compute `DENSE_RANK()` of products by monthly revenue within each month
4. Write SQL: find the top product per category by revenue (use `RANK() PARTITION BY category`)
