import sys, os
sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

import pandas as pd
from sqlalchemy import text
from config.db_config import get_engine, get_connection


def df_to_table(df: pd.DataFrame, table_name: str, schema: str, if_exists: str = "replace"):
    engine = get_engine()
    df.to_sql(table_name, engine, schema=schema, if_exists=if_exists, index=False)
    print(f"[db_utils] Loaded {len(df)} rows → {schema}.{table_name} (if_exists={if_exists})")


def read_table(table_name: str, schema: str) -> pd.DataFrame:
    engine = get_engine()
    df = pd.read_sql(f'SELECT * FROM "{schema}"."{table_name}"', engine)
    print(f"[db_utils] Read {len(df)} rows ← {schema}.{table_name}")
    return df


def run_sql_file(path: str):
    with open(path, "r") as f:
        sql = f.read()
    engine = get_engine()
    with engine.connect() as conn:
        for statement in sql.split(";"):
            stmt = statement.strip()
            if stmt:
                conn.execute(text(stmt))
        conn.commit()
    print(f"[db_utils] Executed SQL file: {path}")


def table_row_count(schema: str, table: str) -> int:
    engine = get_engine()
    with engine.connect() as conn:
        result = conn.execute(text(f'SELECT COUNT(*) FROM "{schema}"."{table}"'))
        return result.scalar()
