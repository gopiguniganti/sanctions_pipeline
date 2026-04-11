"""
Runs the bronze -> silver (clean, pre-merge) -> gold pipeline locally using
plain PySpark + Parquet, to validate the actual transformation and matching
logic before it's pasted into Databricks notebooks.

IMPORTANT — why this doesn't use real Delta locally:
Delta Lake's Python package (delta-spark) is a thin wrapper; the actual JVM
jar is fetched at runtime from Maven Central via Spark's --packages
mechanism. This dev machine's network is restricted to PyPI/npm/GitHub and
can't reach Maven Central, so real `MERGE INTO`, `OPTIMIZE ... ZORDER BY`,
and `DESCRIBE HISTORY` can't be exercised in *this* environment.

Those Delta-specific commands are written in notebooks/02_silver_transform.py
exactly as they'll run in Databricks (which has full internet access and
resolves the Delta jar automatically). This script instead validates the
part that's environment-independent: reading raw data, cleaning/standardizing
it, deduplicating, and the fuzzy-match screening logic — using Parquet as a
stand-in for the storage format. When you run the notebooks in Databricks
(or locally with the delta jar pre-cached), the same functions in
bronze.py / silver.py / screening.py are reused unchanged — only the
merge/optimize/history calls in silver.py are Delta-specific and skipped here.

Usage:
    python local_test/run_local.py
"""

import shutil
import sys
import os

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "src"))

from pyspark.sql import SparkSession
from pyspark.sql import functions as F

from sanctions_pipeline import bronze, screening
from sanctions_pipeline.silver import clean_bronze

BASE = os.path.join(os.path.dirname(__file__), "_local_lake")
BRONZE_PATH = os.path.join(BASE, "bronze_sdn_parquet")
SILVER_PATH = os.path.join(BASE, "silver_sdn_parquet")
GOLD_PATH = os.path.join(BASE, "gold_alerts_parquet")

SAMPLE_SDN = os.path.join(os.path.dirname(__file__), "..", "data", "sample_sdn.csv")
SAMPLE_CUSTOMERS = os.path.join(os.path.dirname(__file__), "..", "data", "sample_customers.csv")


def build_spark() -> SparkSession:
    return (
        SparkSession.builder.appName("sanctions-screening-local-logic-test")
        .master("local[*]")
        .getOrCreate()
    )


def main():
    if os.path.exists(BASE):
        shutil.rmtree(BASE)

    spark = build_spark()
    spark.sparkContext.setLogLevel("ERROR")

    # ---- Bronze: land raw SDN sample as-is (Parquet stand-in for Delta) ----
    raw = bronze.read_raw_sdn_csv(spark, SAMPLE_SDN)
    (
        raw.withColumn("_source", F.lit("OFAC_SDN_sample"))
        .write.mode("overwrite")
        .parquet(BRONZE_PATH)
    )
    print(f"[bronze] wrote {raw.count()} raw rows -> {BRONZE_PATH} (parquet, not delta — see docstring)")

    # ---- Silver: clean + standardize (same clean_bronze() used in Databricks) ----
    bronze_df = spark.read.parquet(BRONZE_PATH)
    clean = clean_bronze(bronze_df)
    clean.write.mode("overwrite").parquet(SILVER_PATH)
    print(f"[silver] cleaned {clean.count()} entity records -> {SILVER_PATH}")

    print("\n[silver] sample cleaned records:")
    spark.read.parquet(SILVER_PATH).show(5, truncate=60)

    print(
        "[silver] NOTE: MERGE INTO upsert, OPTIMIZE ZORDER BY, and DESCRIBE HISTORY\n"
        "are Delta-specific and are exercised directly in Databricks — see\n"
        "notebooks/02_silver_transform.py. This local run validates the clean_bronze()\n"
        "transformation logic only.\n"
    )

    # ---- Gold: screen sample customers against cleaned sanctions records ----
    customers_df = spark.read.option("header", True).csv(SAMPLE_CUSTOMERS)
    silver_df = spark.read.parquet(SILVER_PATH)
    alerts = screening.screen_customers(spark, customers_df, silver_df)
    alerts.write.mode("overwrite").parquet(GOLD_PATH)

    print(f"\n[gold] {alerts.count()} alert(s) at match_score >= {screening.MATCH_THRESHOLD}:")
    alerts.show(truncate=60)

    spark.stop()


if __name__ == "__main__":
    main()

