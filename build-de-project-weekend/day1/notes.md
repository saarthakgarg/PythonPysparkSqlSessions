# Day 1 — Project Architecture · Medallion · HTTP · SQLAlchemy · Bronze Ingestion

> **Project Day:** 1 · **Layer:** Bronze  
> **Study Window:** 3 hours  
> **Theme:** Retail Analytics — understand the stack, connect to Postgres, land every source into bronze

---

## 1. Project Architecture

### Directory Layout

```
build-de-project-weekend/
│
├── config/
│   └── db_config.py          ← single config imported by ALL notebooks
│
├── data/
│   └── raw/                  ← source files (CSV, JSON, XML, logs)
│       ├── customers.csv
│       ├── orders.csv
│       ├── order_items.csv
│       ├── products.csv
│       ├── inventory.json
│       ├── weather_api_response.json
│       ├── store_locations.xml
│       └── webserver_access.log
│
├── day1/  notebook.ipynb · practice_questions.ipynb · notes.md
├── day2/  notebook.ipynb · practice_questions.ipynb · notes.md
├── day3/  notebook.ipynb · practice_questions.ipynb · notes.md
└── day4/  notebook.ipynb · practice_questions.ipynb · notes.md
```

### Data Flow (end-to-end)

```
Source Files (CSV/JSON/XML/Logs/API)
        │
        ▼  [Day 1 — PySpark read + metadata]
   PostgreSQL: bronze.*          ← raw, as-is, + _source_file/_ingested_at/_batch_id
        │
        ▼  [Day 2 — PySpark transforms]
   PostgreSQL: silver.*          ← cleaned, typed, deduped, derived columns
        │
        ▼  [Day 3 — PySpark aggregations]
   PostgreSQL: gold.*            ← business aggregates (RFM, weekly sales, product rank)
        │
        ▼  [Day 4 — Orchestration]
   Pipeline Runner               ← stage timing, DQ checks, incremental load, upsert
```

### Tech Stack

| Component | Tool | Why |
|-----------|------|-----|
| Compute | PySpark (local[*]) | Distributed-style transforms on a laptop |
| Storage | PostgreSQL | Persistent layered schemas |
| JDBC connector | `postgresql-42.7.3.jar` | Pure PySpark read/write to Postgres |
| SQLAlchemy | psycopg2 driver | DDL, raw SQL queries, upserts |
| HTTP | `requests` | Fetch live API data |
| Config | `config/db_config.py` | One import for engine + Spark + JDBC helpers |

### Standard Notebook Header

Every notebook starts with this exact pattern:

```python
import sys, os
sys.path.insert(0, os.path.join(os.getcwd(), '..'))

from config.db_config import (
    get_engine, ensure_schemas, set_spark_env, get_spark,
    new_batch, raw_path, spark_read_pg, spark_write_pg
)

engine = get_engine()
ensure_schemas(engine)
BATCH_ID, INGESTED_AT = new_batch()

set_spark_env()                    # must come BEFORE any pyspark import
from pyspark.sql import functions as F

spark = get_spark('AppName')
```

---

## 2. Medallion Architecture

### What Is It?

A **three-layer data organization pattern** that separates raw data from clean data from business-ready data. Each layer has a single job.

```
┌─────────────────────────────────────────────────────────────┐
│  BRONZE  (Raw Landing Zone)                                 │
│  ─ Data arrives exactly as it came from the source          │
│  ─ Only adds: _source_file, _ingested_at, _batch_id         │
│  ─ Never modifies values — even nulls/bad formats/dups      │
│  ─ "if Silver breaks, Bronze still has the original truth"  │
└───────────────────┬─────────────────────────────────────────┘
                    │  clean · type · dedup · derive
                    ▼
┌─────────────────────────────────────────────────────────────┐
│  SILVER  (Cleaned / Conforming)                             │
│  ─ Trim whitespace, normalize case, fix encoding            │
│  ─ Cast strings to correct types (dates, booleans, floats)  │
│  ─ Remove duplicates on primary key                         │
│  ─ Add derived columns (full_name, is_cancelled, tier)      │
│  ─ Remove bronze metadata cols, add _silver_loaded_at       │
└───────────────────┬─────────────────────────────────────────┘
                    │  aggregate · join · score · rank
                    ▼
┌─────────────────────────────────────────────────────────────┐
│  GOLD  (Business-Ready / Aggregated)                        │
│  ─ Aggregations: weekly sales, product revenue              │
│  ─ RFM customer segments (Recency, Frequency, Monetary)     │
│  ─ Window functions: RANK, LAG, running totals              │
│  ─ Ready to plug into dashboards or BI tools                │
└─────────────────────────────────────────────────────────────┘
```

### Layer Comparison

| Property | Bronze | Silver | Gold |
|----------|--------|--------|------|
| Data quality | Raw — no changes | Cleaned, typed | Aggregated |
| Duplicates | Allowed | Removed | N/A (grouped) |
| Nulls | Allowed | Handled | N/A (filled/dropped) |
| Metadata | `_source_file`, `_ingested_at`, `_batch_id` | `_silver_loaded_at` | `_gold_loaded_at` |
| Load mode | `if_exists='replace'` (full) or `'append'` (incremental) | `replace` | `replace` |
| Audience | Engineers (debugging) | Analysts (querying) | Business (dashboards) |

### Why This Pattern?

1. **Auditability** — every raw byte is preserved in bronze; you can always re-derive silver/gold
2. **Recovery** — if a Silver bug corrupts data, re-run Silver from Bronze without re-ingesting sources
3. **Separation of concerns** — ingestion logic, cleaning logic, and business logic never mix
4. **Incremental-friendly** — bronze supports append; silver/gold can rebuild from bronze

---

## 3. HTTP Requests in Python

### The `requests` Library

`requests.get()` is the standard way to call REST APIs in Python.

```python
import requests

# Basic GET
response = requests.get('https://api.example.com/data', timeout=10)
```

### Anatomy of a Request

```python
response = requests.get(
    url     = 'https://api.open-meteo.com/v1/forecast',
    params  = {                          # appended as ?key=val&key=val
        'latitude'  : 40.7128,
        'longitude' : -74.0060,
        'current'   : 'temperature_2m,wind_speed_10m',
        'timezone'  : 'UTC',
    },
    headers = {'Accept': 'application/json'},  # optional for most free APIs
    timeout = 10,                        # seconds before ConnectionError
)
```

### Response Object

```python
response.status_code    # 200, 404, 500, ...
response.url            # final URL including params
response.json()         # parse body as JSON → Python dict
response.text           # raw body as string
response.raise_for_status()   # raises HTTPError if status >= 400
```

### HTTP Methods

| Method | Used For | Example |
|--------|----------|---------|
| `GET` | Read data (no side effects) | `requests.get(url)` |
| `POST` | Create / submit data | `requests.post(url, json={...})` |
| `PUT` | Replace a resource | `requests.put(url, json={...})` |
| `PATCH` | Partially update a resource | `requests.patch(url, json={...})` |
| `DELETE` | Delete a resource | `requests.delete(url)` |

### Status Code Ranges

| Range | Meaning | Example |
|-------|---------|---------|
| 2xx | Success | 200 OK, 201 Created |
| 3xx | Redirect | 301 Moved Permanently |
| 4xx | Client error | 400 Bad Request, 404 Not Found |
| 5xx | Server error | 500 Internal Server Error |

### Robust Request Pattern

```python
try:
    resp = requests.get(url, params=params, timeout=10)
    resp.raise_for_status()      # raise for 4xx/5xx
    data = resp.json()
except requests.exceptions.Timeout:
    print('Request timed out')
except requests.exceptions.HTTPError as e:
    print(f'HTTP error: {e.response.status_code}')
except requests.exceptions.ConnectionError:
    print('Network unreachable')
```

### Live API Example — Open-Meteo (free, no key)

```python
import requests

CITIES = [
    {'name': 'New York', 'lat': 40.7128, 'lon': -74.0060},
    {'name': 'Chicago',  'lat': 41.8781, 'lon': -87.6298},
]

for city in CITIES:
    resp = requests.get(
        'https://api.open-meteo.com/v1/forecast',
        params={
            'latitude'  : city['lat'],
            'longitude' : city['lon'],
            'current'   : 'temperature_2m,relative_humidity_2m,wind_speed_10m,weathercode',
            'timezone'  : 'America/New_York',
        },
        timeout=10,
    )
    resp.raise_for_status()
    cur = resp.json()['current']
    print(f"{city['name']}: {cur['temperature_2m']}°C, wind {cur['wind_speed_10m']} km/h")
```

---

## 4. Database Connection — SQLAlchemy

### What Is SQLAlchemy?

SQLAlchemy is the standard Python SQL toolkit. It provides:
- **Engine** — manages the connection pool to a database
- **text()** — wraps raw SQL strings safely
- **inspect()** — introspects database metadata (tables, columns)

> **Note:** We do NOT use pandas `to_sql()` / `pd.read_sql()` in this project.  
> All DataFrame I/O is done via pure PySpark JDBC using `spark_write_pg()` / `spark_read_pg()`.  
> SQLAlchemy is used only for DDL, raw SQL queries, and upserts.

### Connection Stack

```
Python code
    └─ SQLAlchemy (engine, ORM)
           └─ psycopg2  (PostgreSQL driver / adapter)
                  └─ PostgreSQL server
```

### Connection URL Format

```
postgresql+psycopg2://USERNAME:PASSWORD@HOST:PORT/DBNAME
```

### Building an Engine

```python
from sqlalchemy import create_engine

DATABASE_URL = 'postgresql+psycopg2://postgres:hariom@localhost:5432/postgres'
engine = create_engine(DATABASE_URL, echo=False)
```

`echo=True` → logs every SQL statement (useful for debugging, noisy for production).

### Running Raw SQL

```python
from sqlalchemy import text

# Pattern 1: execute a DDL statement
with engine.connect() as conn:
    conn.execute(text('CREATE SCHEMA IF NOT EXISTS bronze;'))
    conn.commit()             # always commit mutations

# Pattern 2: read scalar value
with engine.connect() as conn:
    n = conn.execute(text('SELECT COUNT(*) FROM bronze.orders')).scalar()
print(n)

# Pattern 3: read multiple rows
with engine.connect() as conn:
    rows = conn.execute(
        text('SELECT order_id, total_amount FROM bronze.orders LIMIT 5')
    ).fetchall()
for row in rows:
    print(row['order_id'], row['total_amount'])
```

### Inspecting the Database

```python
from sqlalchemy import inspect

insp = inspect(engine)
tables = insp.get_table_names(schema='bronze')   # list of table names
print(tables)  # ['customers', 'orders', 'products', ...]
```

### JDBC Write + Read (pure PySpark — no pandas)

```python
from config.db_config import spark_write_pg, spark_read_pg

# Write PySpark DF → Postgres
spark_write_pg(df, 'bronze', 'customers', mode='overwrite')  # full load
spark_write_pg(df, 'bronze', 'orders',    mode='append')     # incremental

# Read Postgres → PySpark DF
df = spark_read_pg(spark, 'bronze', 'customers')
df = spark_read_pg(spark, 'silver', 'orders')
```

### `mode` Values (JDBC write)

| Value | What happens |
|-------|-------------|
| `'overwrite'` | Drop and recreate the table — use for full loads |
| `'append'` | Insert rows without touching existing data — use for incremental |
| `'ignore'` | Skip if table already exists |
| `'error'` | Raise if table already exists (default) |

---

## 5. Bronze Ingestion Patterns

### Adding Metadata Columns

Every bronze table gets three columns added via PySpark `F.lit()`:

```python
def add_bronze_meta(df, source_file):
    return (df
        .withColumn('_source_file', F.lit(source_file))
        .withColumn('_ingested_at', F.lit(INGESTED_AT))
        .withColumn('_batch_id',    F.lit(BATCH_ID)))
```

| Column | Value | Purpose |
|--------|-------|---------|
| `_source_file` | `'customers.csv'` | Which file this row came from |
| `_ingested_at` | UTC ISO timestamp | When this batch was loaded |
| `_batch_id` | UUID | Groups all tables from one pipeline run |

### Reading Each Source Format in PySpark

```python
# CSV
df_csv = spark.read.option('header','true').option('inferSchema','true').csv(raw_path('file.csv'))

# JSON (flat array) — via Python → createDataFrame
import json
data = json.load(open(raw_path('file.json')))
df_json = spark.createDataFrame(data)

# JSON (nested API envelope)
payload = json.load(open(raw_path('api.json')))
df_api = spark.createDataFrame(payload['data'])

# XML — ElementTree → list of dicts → createDataFrame
import xml.etree.ElementTree as ET
root = ET.parse(raw_path('file.xml')).getroot()
rows = [{c.tag: c.text for c in s} for s in root.findall('store')]
df_xml = spark.createDataFrame(rows)

# Log file — split on " (double-quote) → list of dicts → createDataFrame
rows = []
with open(raw_path('app.log')) as f:
    for line in f:
        parts  = line.strip().split('"')   # 5 parts split by double-quote
        left   = parts[0].strip().split()  # ip, dash, user, timestamp
        req    = parts[1].split()          # ['GET', '/path', 'HTTP/1.1']
        middle = parts[2].strip().split()  # ['200', '1234']
        rows.append({
            'ip'           : left[0],
            'user'         : left[2] if left[2] != '-' else None,
            'timestamp'    : left[3].lstrip('['),
            'method'       : req[0],
            'endpoint'     : req[1],
            'status_code'  : int(middle[0]),
            'response_size': int(middle[1]) if middle[1].isdigit() else None,
            'referrer'     : parts[3] if parts[3] != '-' else None,
            'user_agent'   : parts[4].strip(),
        })
df_logs = spark.createDataFrame(rows)
```

### Writing Bronze to Postgres (pure PySpark JDBC)

```python
from config.db_config import spark_write_pg

def to_bronze_pg(df, table_name):
    spark_write_pg(df, 'bronze', table_name, mode='overwrite')
```

`spark_write_pg(df, schema, table, mode)` is defined in `config/db_config.py` and handles all JDBC options internally. Use `mode='append'` for incremental loads.

### Reading from Postgres (pure PySpark JDBC)

```python
from config.db_config import spark_read_pg

df = spark_read_pg(spark, 'bronze', 'customers')
```

---

## 6. Quick Reference

```python
# Engine + helpers from config
from config.db_config import (
    get_engine, get_spark, set_spark_env,
    new_batch, raw_path, spark_read_pg, spark_write_pg
)
engine = get_engine()

# Spark from config
set_spark_env()
spark = get_spark('MyApp')

# New batch IDs
BATCH_ID, INGESTED_AT = new_batch()

# HTTP
import requests
resp = requests.get(url, params={...}, timeout=10)
resp.raise_for_status()
data = resp.json()

# SQLAlchemy raw SQL (DDL, upserts, scalar queries)
from sqlalchemy import text
with engine.connect() as conn:
    conn.execute(text('...'))
    conn.commit()

# Write PySpark DF to Postgres via JDBC (pure PySpark)
spark_write_pg(df, 'bronze', 'customers', mode='overwrite')
spark_write_pg(df, 'bronze', 'orders',    mode='append')    # incremental

# Read Postgres into PySpark via JDBC (pure PySpark)
df = spark_read_pg(spark, 'bronze', 'customers')
df = spark_read_pg(spark, 'silver', 'orders')
```

---

## 7. Common Mistakes

| Mistake | Fix |
|---------|-----|
| Calling `get_spark()` before `set_spark_env()` | Always call `set_spark_env()` first |
| JDBC jar not on classpath | `get_spark()` in `db_config.py` already adds `postgresql-42.7.3.jar` automatically |
| Using `toPandas().to_sql()` | Use `spark_write_pg(df, schema, table)` — pure JDBC, no pandas |
| Using `pd.read_sql()` + `createDataFrame()` | Use `spark_read_pg(spark, schema, table)` — pure JDBC |
| Forgetting `conn.commit()` after DDL/DML | Transaction never written to disk |
| Not calling `resp.raise_for_status()` | Silent 404/500 with no error raised |
| `xml.text` is always a string | Cast numeric fields after `spark.createDataFrame(rows)` |
