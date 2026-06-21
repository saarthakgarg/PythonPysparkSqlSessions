# Day 2 — Silver Layer: Clean & Conform (3 hours)

## What You'll Learn
- Reading from Postgres back into pandas (`pd.read_sql`)
- String normalization, type casting, null handling
- Deriving computed columns (full_name, is_low_stock, line_total)
- PySpark local mode: DataFrames, window functions, parquet output
- Creating SQL views for reusable analytical queries

## 3-Hour Schedule

| Time        | Script                           | Key Concepts                                      |
|-------------|----------------------------------|---------------------------------------------------|
| 0:00–0:45   | `01_silver_customers_orders.py`  | `.str.strip()`, `.str.lower()`, `pd.to_datetime`, dedup |
| 0:45–1:30   | `02_silver_products_inventory.py`| `.map()`, `fillna`, boolean flags, table joins    |
| 1:30–2:15   | `03_silver_pyspark_transform.py` | `SparkSession`, `cast()`, `Window`, `rank()`, parquet |
| 2:15–3:00   | `04_silver_sql_views.sql`        | `CREATE VIEW`, `LEFT JOIN`, `FILTER WHERE`, `ROUND` |

## Expected Outputs
- `silver.customers`        — cleaned, full_name, email lowercase
- `silver.orders`           — typed dates, is_cancelled flag, customer_name joined
- `silver.products`         — category uppercase, price as float
- `silver.inventory`        — days_since_update, is_low_stock flag
- `silver.product_inventory`— joined table
- `silver.order_items`      — line_total, item_rank_in_order (from PySpark)
- `data/processed/silver_order_items.parquet`
- SQL Views: `v_order_summary`, `v_product_stock`, `v_top_endpoints`, `v_daily_orders`

## Run Commands
```bash
python day2/01_silver_customers_orders.py
python day2/02_silver_products_inventory.py
python day2/03_silver_pyspark_transform.py
psql -U postgres -d postgres -f day2/04_silver_sql_views.sql
```

## Practice Challenges
1. Add email domain extraction: `df['email_domain'] = df['email'].str.split('@').str[1]`
2. In PySpark, add a `LEAD` window function alongside `RANK`
3. Create a fifth SQL view: `silver.v_cancelled_orders_by_customer`
4. Check: how many customers have placed more than 2 orders? (SQL or pandas)
