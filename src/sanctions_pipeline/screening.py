"""
Gold layer: screen a customer list against the silver sanctions table and
produce scored alerts.

This is intentionally a plain fuzzy-match scorer, not a claim to reproduce
what Bridger/NetReveal do internally — those are vendor systems with their
own matching engines. This is a self-contained implementation of the same
*category* of problem (name similarity + alias expansion -> match score),
built to understand the mechanics, not to imitate a specific vendor's logic.
"""

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.types import StructType, StructField, StringType, FloatType
from rapidfuzz import fuzz


MATCH_THRESHOLD = 85.0  # score out of 100; tune based on false-positive tolerance


def _best_match_score(name: str, candidates) -> float:
    """Best fuzzy-match score of `name` against a list of candidate strings
    (an entity's primary name plus its known aliases)."""
    if not candidates:
        return 0.0
    return max(fuzz.token_sort_ratio(name.upper(), c.upper()) for c in candidates if c)


def screen_customers(spark: SparkSession, customers_df: DataFrame, silver_df: DataFrame) -> DataFrame:
    """
    Cross-join customers against sanctions entities and score each pair.
    A real production version would pre-block on first-letter or phonetic key
    to avoid an O(n*m) cross-join at scale — noted here rather than hidden,
    since a full-scale version needs that optimization and this sample-size
    demo doesn't.
    """
    customers = customers_df.select(
        "customer_id", "customer_name", "dob", "nationality"
    ).collect()
    entities = silver_df.select(
        "entity_id", "entity_name_clean", "aliases", "program"
    ).collect()

    schema = StructType(
        [
            StructField("customer_id", StringType()),
            StructField("customer_name", StringType()),
            StructField("entity_id", StringType()),
            StructField("matched_entity_name", StringType()),
            StructField("program", StringType()),
            StructField("match_score", FloatType()),
        ]
    )

    rows = []
    for cust in customers:
        for ent in entities:
            candidates = [ent["entity_name_clean"]] + list(ent["aliases"] or [])
            score = _best_match_score(cust["customer_name"], candidates)
            if score >= MATCH_THRESHOLD:
                rows.append(
                    (
                        cust["customer_id"],
                        cust["customer_name"],
                        str(ent["entity_id"]),
                        ent["entity_name_clean"],
                        ent["program"],
                        float(score),
                    )
                )

    return spark.createDataFrame(rows, schema=schema).orderBy(
        "customer_id", "match_score", ascending=[True, False]
    )


def write_gold_alerts(df: DataFrame, gold_path: str) -> None:
    """Overwrite the alerts table each run — alerts are a point-in-time
    screening result, not an accumulating log, so overwrite is the right
    semantics here (unlike silver, which upserts)."""
    df.write.format("delta").mode("overwrite").save(gold_path)
