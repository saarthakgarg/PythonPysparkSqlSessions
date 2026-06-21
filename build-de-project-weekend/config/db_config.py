"""
config/db_config.py
-------------------
Central configuration for the Retail Analytics DE Project.
Every notebook imports this file via:

    import sys, os
    sys.path.insert(0, os.path.join(os.getcwd(), '..'))
    from config.db_config import *

Provides:
  - DB credentials + SQLAlchemy engine
  - PySpark environment variables (set before any pyspark import)
  - SparkSession factory
  - Schema bootstrap (bronze / silver / gold)
  - Shared batch metadata helpers
"""

import os
import uuid
from datetime import datetime
from sqlalchemy import create_engine, text

# ── PostgreSQL credentials ─────────────────────────────────────────────────────
DB_USER = "postgres"
DB_PASS = "hariom"
DB_HOST = "localhost"
DB_PORT = "5432"
DB_NAME = "postgres"

DATABASE_URL = f"postgresql+psycopg2://{DB_USER}:{DB_PASS}@{DB_HOST}:{DB_PORT}/{DB_NAME}"


def get_engine():
    """Return a SQLAlchemy engine connected to PostgreSQL."""
    return create_engine(DATABASE_URL, echo=False)


def ensure_schemas(engine=None):
    """Create bronze / silver / gold schemas if they don't already exist."""
    eng = engine or get_engine()
    with eng.connect() as conn:
        conn.execute(text("CREATE SCHEMA IF NOT EXISTS bronze;"))
        conn.execute(text("CREATE SCHEMA IF NOT EXISTS silver;"))
        conn.execute(text("CREATE SCHEMA IF NOT EXISTS gold;"))
        conn.commit()
    print("[db_config] Schemas bronze / silver / gold are ready.")


# ── PySpark environment ────────────────────────────────────────────────────────
# Must be set BEFORE any pyspark import.
# Call set_spark_env() at the top of any notebook that uses PySpark.
def set_spark_env():
    """Set JAVA_HOME and PYSPARK_* env vars needed on this machine."""
    os.environ["JAVA_HOME"]             = "C:/Program Files/DBeaver/jre"
    os.environ["PYSPARK_PYTHON"]        = r"C:\Users\hariom\AppData\Local\Programs\Python\Python311\python.exe"
    os.environ["PYSPARK_DRIVER_PYTHON"] = r"C:\Users\hariom\AppData\Local\Programs\Python\Python311\python.exe"
    print("[db_config] Spark environment variables set.")


def get_spark(app_name: str = "RetailDE"):
    """
    Return a local SparkSession.
    Always call set_spark_env() before this function.
    """
    import site
    pg_jar = os.path.join(site.getsitepackages()[0], "pyspark", "jars", "postgresql-42.7.3.jar")

    from pyspark.sql import SparkSession
    spark = (
        SparkSession.builder
        .appName(app_name)
        .master("local[*]")
        .config("spark.sql.shuffle.partitions", "4")
        .config("spark.ui.showConsoleProgress", "false")
        .config("spark.driver.extraClassPath", pg_jar)
        .config("spark.executor.extraClassPath", pg_jar)
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("ERROR")
    print(f"[db_config] Spark {spark.version} ready — app: {app_name}")
    return spark


# ── Shared batch metadata ──────────────────────────────────────────────────────
def new_batch():
    """Return (BATCH_ID, INGESTED_AT) for tagging bronze rows."""
    return str(uuid.uuid4()), datetime.utcnow().isoformat()


# ── Raw data path helper ───────────────────────────────────────────────────────
def raw_path(*parts):
    """
    Build path to data/raw relative to this config file.
    Usage: raw_path('customers.csv')  →  '<project>/data/raw/customers.csv'
    """
    base = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    return os.path.join(base, "data", "raw", *parts)


# ── JDBC write helper ──────────────────────────────────────────────────────────
JDBC_URL = f"jdbc:postgresql://{DB_HOST}:{DB_PORT}/{DB_NAME}"

def spark_write_pg(df, schema, table, mode="overwrite"):
    """
    Write a PySpark DataFrame to PostgreSQL via JDBC.

    Usage:
        spark_write_pg(df, 'bronze', 'customers')
        spark_write_pg(df, 'silver', 'orders', mode='overwrite')
    """
    (
        df.write
        .format("jdbc")
        .option("url",           JDBC_URL)
        .option("dbtable",       f"{schema}.{table}")
        .option("user",          DB_USER)
        .option("password",      DB_PASS)
        .option("driver",        "org.postgresql.Driver")
        .option("currentSchema", schema)
        .mode(mode)
        .save()
    )
    print(f"  {schema}.{table:<26} → {df.count():>5} rows  [{mode}]")


# ── JDBC read helper ───────────────────────────────────────────────────────────
def spark_read_pg(spark, schema, table):
    """
    Read a PostgreSQL table into a PySpark DataFrame via JDBC.

    Usage:
        df = spark_read_pg(spark, 'bronze', 'customers')
    """
    return (
        spark.read
        .format("jdbc")
        .option("url",           JDBC_URL)
        .option("dbtable",       f"{schema}.{table}")
        .option("user",          DB_USER)
        .option("password",      DB_PASS)
        .option("driver",        "org.postgresql.Driver")
        .option("currentSchema", schema)
        .load()
    )
