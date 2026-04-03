# Databricks notebook source
# MAGIC %md
# MAGIC # Bronze: Raw OFAC SDN Ingestion
# MAGIC
# MAGIC Lands the OFAC Specially Designated Nationals (SDN) list as-is into a Delta
# MAGIC table. Bronze is append-only and string-typed on purpose — fidelity over
# MAGIC cleanliness. If OFAC adds or renames a column tomorrow, this still ingests
# MAGIC it; `silver` is where we deal with the fallout.

# COMMAND ----------

dbutils.widgets.text("sdn_url", "https://sanctionslistservice.ofac.treas.gov/api/PublicationPreview/exports/SDN.CSV")
dbutils.widgets.text("bronze_path", "/tmp/sanctions_screening/bronze_sdn")
dbutils.widgets.text("landing_path", "/tmp/sanctions_screening/landing/sdn_raw.csv")

sdn_url = dbutils.widgets.get("sdn_url")
bronze_path = dbutils.widgets.get("bronze_path")
landing_path = dbutils.widgets.get("landing_path")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Download the current SDN list
# MAGIC Databricks driver nodes have full internet access, so this runs as a
# MAGIC scheduled job step — no manual download step needed in production.

# COMMAND ----------

import urllib.request

urllib.request.urlretrieve(sdn_url, "/tmp/sdn_raw_download.csv")
dbutils.fs.cp("file:/tmp/sdn_raw_download.csv", landing_path)

# COMMAND ----------

import sys
sys.path.append("/Workspace/Repos/<your-repo-path>/sanctions-screening-databricks/src")  # noqa: E501
# In Databricks Repos, this resolves automatically once the repo is attached;
# update the path above if you clone it to a different Workspace location.

from sanctions_pipeline import bronze

raw_df = bronze.read_raw_sdn_csv(spark, landing_path)
display(raw_df.limit(10))

# COMMAND ----------

bronze.write_bronze(raw_df, bronze_path, source_name="OFAC_SDN")
print(f"Bronze write complete: {raw_df.count()} rows -> {bronze_path}")

# COMMAND ----------

# MAGIC %md
# MAGIC Sanity check: bronze should only ever grow (append-only), so row count
# MAGIC across runs should increase or hold steady, never shrink.

# COMMAND ----------

spark.read.format("delta").load(bronze_path).groupBy("_ingestion_date").count().orderBy(
    "_ingestion_date"
).show()
