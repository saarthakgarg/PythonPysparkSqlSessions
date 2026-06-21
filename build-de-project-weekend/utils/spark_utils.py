from pyspark.sql import SparkSession


def get_spark_session(app_name: str = "RetailDE") -> SparkSession:
    spark = (
        SparkSession.builder
        .appName(app_name)
        .master("local[*]")
        .config("spark.sql.shuffle.partitions", "4")
        .config("spark.driver.memory", "2g")
        # JDBC jar (download postgresql-42.x.x.jar and place in project root, then uncomment):
        # .config("spark.jars", "postgresql-42.7.3.jar")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")
    print(f"[spark_utils] Spark session started: {app_name}")
    return spark
