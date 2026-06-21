# Retail Analytics — Local Medallion Architecture DE Project

A 4-day, hands-on Data Engineering project using the **Bronze → Silver → Gold** medallion pattern on a local Postgres database.

---

## Architecture

```
Raw Sources (CSV / JSON / XML / Logs / API)
         │
         ▼
  ┌─────────────┐
  │   BRONZE    │  Raw ingestion, no transformations, metadata columns added
  └─────────────┘
         │  clean + conform
         ▼
  ┌─────────────┐
  │   SILVER    │  Typed, deduplicated, joined, enriched
  └─────────────┘
         │  aggregate + model
         ▼
  ┌─────────────┐
  │    GOLD     │  Business-ready aggregations, RFM, KPIs
  └─────────────┘
```

## Tech Stack

| Tool       | Purpose                              |
|------------|--------------------------------------|
| Python     | Orchestration, data loading          |
| Pandas     | DataFrame transformations            |
| PySpark    | Large-scale transforms, window fns   |
| SQL        | Views, aggregations, analytics       |
| PostgreSQL | Data warehouse (local)               |

## Source Files

| File                       | Type | Description                          |
|----------------------------|------|--------------------------------------|
| customers.csv              | CSV  | 20 customer records                  |
| orders.csv                 | CSV  | 50 orders                            |
| order_items.csv            | CSV  | 100 line items                       |
| products.csv               | CSV  | 15 products                          |
| inventory.json             | JSON | Stock levels per product             |
| store_locations.xml        | XML  | 5 retail store locations             |
| weather_api_response.json  | JSON | Simulated API response (nested)      |
| webserver_access.log       | LOG  | Apache Combined Log Format (30 rows) |

---

## Setup

### 1. Install dependencies
```bash
pip install -r requirements.txt
```

### 2. Postgres schemas
Run once in psql or any SQL client:
```sql
CREATE SCHEMA IF NOT EXISTS bronze;
CREATE SCHEMA IF NOT EXISTS silver;
CREATE SCHEMA IF NOT EXISTS gold;
```
Or just run any Day 1 script — it calls `ensure_schemas()` automatically.

### 3. DB credentials
Edit `config/db_config.py` if your local Postgres differs:
```python
USERNAME = "postgres"
PASSWORD = "hariom"
HOST     = "localhost"
PORT     = "5432"
DBNAME   = "postgres"
```

---

## 4-Day Schedule (3 hours/day)

### Day 1 — Bronze Layer: Raw Ingestion
| Block       | Script                       | Topic                              |
|-------------|------------------------------|------------------------------------|
| 0:00–0:45   | `01_explore_sources.py`      | Read & profile all source files    |
| 0:45–1:30   | `02_load_bronze_csv.py`      | Load CSVs → bronze (pandas + SQLAlchemy) |
| 1:30–2:15   | `03_load_bronze_json_xml.py` | Flatten JSON, parse XML → bronze   |
| 2:15–3:00   | `04_load_bronze_logs.py`     | Regex-parse logs → bronze          |

### Day 2 — Silver Layer: Clean & Conform
| Block       | Script                           | Topic                              |
|-------------|----------------------------------|------------------------------------|
| 0:00–0:45   | `01_silver_customers_orders.py`  | Normalize strings, dates, joins    |
| 0:45–1:30   | `02_silver_products_inventory.py`| Low-stock flags, derived columns   |
| 1:30–2:15   | `03_silver_pyspark_transform.py` | PySpark: types, window rank        |
| 2:15–3:00   | `04_silver_sql_views.sql`        | SQL views on silver schema         |

### Day 3 — Gold Layer: Aggregations & Analytics
| Block       | Script                          | Topic                              |
|-------------|---------------------------------|------------------------------------|
| 0:00–0:45   | `01_gold_sales_summary.py`      | Daily & monthly sales aggregations |
| 0:45–1:30   | `02_gold_customer_segments.py`  | RFM scoring & segmentation         |
| 1:30–2:15   | `03_gold_pyspark_aggregations.py`| PySpark: product revenue, cumulative |
| 2:15–3:00   | `04_gold_sql_analytics.sql`     | LAG, RANK, anti-join, cohort SQL   |

### Day 4 — Orchestration & Quality
| Block       | Script                      | Topic                              |
|-------------|-----------------------------|------------------------------------|
| 0:00–0:45   | `01_pipeline_orchestrator.py`| Run full pipeline end-to-end       |
| 0:45–1:30   | `02_data_quality_checks.py` | Null, duplicate, referential checks|
| 1:30–2:15   | `03_incremental_load.py`    | Watermark + upsert pattern         |
| 2:15–3:00   | `04_final_report_query.sql` | Business KPI report in SQL         |

---

## Running Scripts

From the project root:
```bash
# Day 1
python day1/01_explore_sources.py
python day1/02_load_bronze_csv.py
python day1/03_load_bronze_json_xml.py
python day1/04_load_bronze_logs.py

# Day 2
python day2/01_silver_customers_orders.py
python day2/02_silver_products_inventory.py
python day2/03_silver_pyspark_transform.py
psql -U postgres -d postgres -f day2/04_silver_sql_views.sql

# Day 3
python day3/01_gold_sales_summary.py
python day3/02_gold_customer_segments.py
python day3/03_gold_pyspark_aggregations.py
psql -U postgres -d postgres -f day3/04_gold_sql_analytics.sql

# Day 4
python day4/01_pipeline_orchestrator.py
python day4/02_data_quality_checks.py
python day4/03_incremental_load.py
psql -U postgres -d postgres -f day4/04_final_report_query.sql
```
