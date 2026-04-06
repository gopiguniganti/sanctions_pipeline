"""
Silver layer: clean, standardize, and deduplicate sanctions entities using a
Delta MERGE INTO upsert — not a full overwrite.

Why MERGE and not overwrite: OFAC republishes the entire SDN list on every
update, not just the deltas. A naive overwrite works but throws away Delta's
row-level change tracking (DESCRIBE HISTORY becomes "table replaced" instead
of "these 4 rows changed, these 2 were added"), which is exactly the audit
trail a compliance reviewer would ask for. MERGE preserves that.
"""

from delta.tables import DeltaTable
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F


def clean_bronze(df: DataFrame) -> DataFrame:
    """
    Standardize raw bronze rows into a clean entity record:
    - trim/upper the name for consistent matching downstream
    - split OFAC's semicolon-delimited alias/remarks blob into a proper array
    - cast ent_num to a real key type
    """
    return (
        df.withColumn("entity_id", F.col("ent_num").cast("long"))
        .withColumn("entity_name_clean", F.upper(F.trim(F.col("SDN_Name"))))
        .withColumn(
            "aliases",
            F.expr(
                r"""
                filter(
                  transform(
                    split(coalesce(Remarks, ''), ';'),
                    x -> trim(regexp_extract(x, 'a\\.k\\.a\\.\\s*(.*)', 1))
                  ),
                  x -> x != ''
                )
                """
            ),
        )
        .withColumn("entity_type", F.lower(F.col("SDN_Type")))
        .withColumn("program", F.col("Program"))
        .withColumn("_updated_at", F.current_timestamp())
        .select(
            "entity_id",
            "entity_name_clean",
            "aliases",
            "entity_type",
            "program",
            "Remarks",
            "_updated_at",
        )
        .dropDuplicates(["entity_id"])
    )


def merge_into_silver(spark: SparkSession, clean_df: DataFrame, silver_path: str) -> None:
    """
    Upsert clean entity records into the silver Delta table on entity_id.
    First run creates the table; subsequent runs merge — this is the pattern
    that mirrors production incremental-load logic rather than a batch
    truncate-and-reload.
    """
    if not DeltaTable.isDeltaTable(spark, silver_path):
        clean_df.write.format("delta").mode("overwrite").save(silver_path)
        return

    target = DeltaTable.forPath(spark, silver_path)
    (
        target.alias("t")
        .merge(clean_df.alias("s"), "t.entity_id = s.entity_id")
        .whenMatchedUpdateAll()
        .whenNotMatchedInsertAll()
        .execute()
    )


def optimize_silver(spark: SparkSession, silver_path: str) -> None:
    """
    Compact small files and Z-order by entity_name_clean, since every
    screening query filters/joins on name. Z-ordering co-locates similar
    values in the same files, which cuts file-scan overhead on lookups —
    this matters more as the table grows past a handful of OFAC updates.
    """
    spark.sql(f"OPTIMIZE delta.`{silver_path}` ZORDER BY (entity_name_clean)")


def show_history(spark: SparkSession, silver_path: str, limit: int = 10):
    """
    Delta's built-in versioned history — this is the audit/lineage trail:
    who (which job run) changed what, and when, without any custom
    audit-table plumbing. Directly relevant to compliance lineage requirements.
    """
    return spark.sql(f"DESCRIBE HISTORY delta.`{silver_path}` LIMIT {limit}")
