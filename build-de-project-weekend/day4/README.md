# Day 4 — Orchestration, Quality & Incremental Load (3 hours)

## What You'll Learn
- Building a simple pipeline orchestrator with timing and logging
- Writing data quality checks (null, duplicate, referential integrity, range)
- Watermark-based incremental loading
- Postgres upsert with ON CONFLICT DO UPDATE
- Writing a final business SQL report

## 3-Hour Schedule

| Time        | Script                       | Key Concepts                                        |
|-------------|------------------------------|-----------------------------------------------------|
| 0:00–0:45   | `01_pipeline_orchestrator.py`| Stage runner, try/except, `time.time()`, row counts |
| 0:45–1:30   | `02_data_quality_checks.py`  | Null checks, `duplicated()`, set membership, `tabulate` |
| 1:30–2:15   | `03_incremental_load.py`     | `MAX(order_date)` watermark, `ON CONFLICT DO UPDATE` |
| 2:15–3:00   | `04_final_report_query.sql`  | Business KPIs: revenue, inventory, web traffic      |

## Expected Outputs
- Full pipeline run log with timing per stage
- DQ report: pass/fail table for all checks
- 5 new orders inserted into bronze + upserted to silver
- Comprehensive business report printed from SQL

## Run Commands
```bash
python day4/01_pipeline_orchestrator.py
python day4/02_data_quality_checks.py
python day4/03_incremental_load.py
psql -U postgres -d postgres -f day4/04_final_report_query.sql
```

## Practice Challenges
1. Add DQ check: `total_amount` outliers (> 3 standard deviations from mean)
2. Add a `dq_results` table to the gold schema and persist each check run
3. Extend orchestrator to also run silver + gold stages (import and call the functions)
4. Add a `--dry-run` CLI flag to the orchestrator that prints what would run without executing
