# Day 1 — Bronze Layer: Raw Ingestion (3 hours)

## What You'll Learn
- Reading CSV, JSON, XML, and log files with Python
- Adding metadata columns (`_source_file`, `_ingested_at`, `_batch_id`)
- Loading raw data into PostgreSQL using SQLAlchemy/pandas
- Regex parsing of unstructured log files
- Why bronze = "no transformation, just landing"

## 3-Hour Schedule

| Time        | Script                       | Key Concepts                              |
|-------------|------------------------------|-------------------------------------------|
| 0:00–0:45   | `01_explore_sources.py`      | `pd.read_csv`, `json.load`, `ElementTree`, file profiling |
| 0:45–1:30   | `02_load_bronze_csv.py`      | `df.to_sql()`, SQLAlchemy engine, `if_exists='replace'` |
| 1:30–2:15   | `03_load_bronze_json_xml.py` | `pd.DataFrame(json)`, `ET.parse()`, nested JSON flattening |
| 2:15–3:00   | `04_load_bronze_logs.py`     | `re.compile`, `groupdict()`, log field extraction |

## Expected Outputs (Postgres tables)
- `bronze.customers`      — 20 rows
- `bronze.orders`         — 50 rows
- `bronze.order_items`    — 100 rows
- `bronze.products`       — 15 rows
- `bronze.inventory`      — 15 rows
- `bronze.store_locations`— 5 rows
- `bronze.weather_data`   — 5 rows
- `bronze.web_logs`       — 30 rows

## Run Commands
```bash
python day1/01_explore_sources.py
python day1/02_load_bronze_csv.py
python day1/03_load_bronze_json_xml.py
python day1/04_load_bronze_logs.py
```

## Practice Challenges (after scripts run)
1. Add a new column `_file_size_kb` to the metadata (use `os.path.getsize`)
2. Change `if_exists='replace'` to `'append'` and observe what happens
3. Parse the log `timestamp` field into a Python `datetime` object
4. Add a status code distribution print to the log loader
