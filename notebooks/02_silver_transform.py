# Databricks notebook source
# MAGIC %md
# MAGIC # Silver: Clean, Standardize, Upsert
# MAGIC
# MAGIC Cleans bronze records and **upserts** them into silver via Delta
# MAGIC `MERGE INTO` — not a full overwrite. OFAC republishes the entire SDN list
# MAGIC on every update, so a naive overwrite works too, but it throws away
# MAGIC Delta's row-level change tracking (`DESCRIBE HISTORY` becomes "table
# MAGIC replaced" instead of "these 4 rows changed, these 2 were added"), which
# MAGIC is exactly the audit trail a compliance reviewer would ask for.

# COMMAND ----------

dbutils.widgets.text("bronze_path", "/tmp/sanctions_screening/bronze_sdn")
dbutils.widgets.text("silver_path", "/tmp/sanctions_screening/silver_sdn")

bronze_path = dbutils.widgets.get("bronze_path")
silver_path = dbutils.widgets.get("silver_path")

# COMMAND ----------

import sys
sys.path.append("/Workspace/Repos/<your-repo-path>/sanctions-screening-databricks/src")  # noqa: E501

from sanctions_pipeline import silver

bronze_df = spark.read.format("delta").load(bronze_path)
clean_df = silver.clean_bronze(bronze_df)
display(clean_df.limit(10))

# COMMAND ----------

# MAGIC %md
# MAGIC ### MERGE INTO — the upsert
# MAGIC First run creates the table. Every subsequent run merges on `entity_id`:
# MAGIC matched rows get updated in place, unmatched (new) rows get inserted.
# MAGIC Re-running this notebook against an unchanged SDN export is idempotent —
# MAGIC row count won't grow, which is what makes this safe to schedule daily.

# COMMAND ----------

silver.merge_into_silver(spark, clean_df, silver_path)
print(f"Silver merge complete -> {silver_path}")
spark.read.format("delta").load(silver_path).count()

# COMMAND ----------

# MAGIC %md
# MAGIC ### OPTIMIZE + Z-ORDER
# MAGIC Every downstream screening query filters/joins on `entity_name_clean`.
# MAGIC Z-ordering co-locates similar values in the same files, cutting file-scan
# MAGIC overhead on those lookups. This matters more as the table accumulates
# MAGIC history across many OFAC updates than it does on a single small run.

# COMMAND ----------

silver.optimize_silver(spark, silver_path)

# COMMAND ----------

# MAGIC %md
# MAGIC ### DESCRIBE HISTORY — the audit trail
# MAGIC This is Delta's built-in version history: which operation ran, when, and
# MAGIC how many rows it touched — without any custom audit-table plumbing.
# MAGIC Directly relevant to the lineage/audit requirements compliance teams ask
# MAGIC for on any AML data pipeline.

# COMMAND ----------

display(silver.show_history(spark, silver_path, limit=10))

# COMMAND ----------

# MAGIC %md
# MAGIC ### Time travel
# MAGIC Delta lets you query any prior version directly — useful for proving
# MAGIC "what did the sanctions list look like on the date this transaction was
# MAGIC screened," which is exactly the kind of question a regulator can ask.

# COMMAND ----------

history_df = silver.show_history(spark, silver_path, limit=1)
latest_version = history_df.collect()[0]["version"]
if latest_version > 0:
    prior_version_df = spark.read.format("delta").option("versionAsOf", latest_version - 1).load(
        silver_path
    )
    print(f"Row count at version {latest_version - 1}: {prior_version_df.count()}")
else:
    print("Only one version exists so far — time travel has nothing to compare yet.")
