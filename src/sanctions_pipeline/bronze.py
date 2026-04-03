"""
Bronze layer: land raw sanctions list data as-is into Delta, no transformation.

The point of bronze is fidelity, not cleanliness — if OFAC changes a column
tomorrow, bronze should still ingest it and let silver deal with the fallout.
That's why this reads everything as string and adds lineage metadata rather
than casting types here.
"""

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F


def read_raw_sdn_csv(spark: SparkSession, path: str) -> DataFrame:
    """
    Read the raw SDN CSV (or a local sample with the same schema) as all-string
    columns. In production this `path` would point at a fresh download of
    https://sanctionslistservice.ofac.treas.gov (the SDN.CSV export) staged to
    cloud storage by a scheduled job; here it can also be a local sample file.
    """
    return (
        spark.read.option("header", True)
        .option("inferSchema", False)  # bronze stays string-typed on purpose
        .csv(path)
    )


def write_bronze(df: DataFrame, bronze_path: str, source_name: str) -> None:
    """
    Append raw rows into the bronze Delta table, stamped with ingestion
    lineage columns. Bronze is append-only — we never overwrite history here,
    because the audit trail of "what did OFAC publish and when" is itself a
    compliance artifact.
    """
    enriched = (
        df.withColumn("_source", F.lit(source_name))
        .withColumn("_ingested_at", F.current_timestamp())
        .withColumn("_ingestion_date", F.current_date())
    )

    (
        enriched.write.format("delta")
        .mode("append")
        .option("mergeSchema", "true")  # OFAC adds/renames columns periodically
        .save(bronze_path)
    )
