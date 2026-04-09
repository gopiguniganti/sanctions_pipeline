# Databricks notebook source
# MAGIC %md
# MAGIC # Gold: Screen Customers Against Sanctions List
# MAGIC
# MAGIC Cross-references a customer list against the cleaned silver sanctions
# MAGIC table and produces scored alerts above a match threshold.
# MAGIC
# MAGIC This is a self-contained fuzzy-match implementation built to understand
# MAGIC the mechanics of name-matching/alias-expansion — it does not claim to
# MAGIC reproduce how any specific vendor screening engine works internally.

# COMMAND ----------

dbutils.widgets.text("silver_path", "/tmp/sanctions_screening/silver_sdn")
dbutils.widgets.text("customers_path", "/tmp/sanctions_screening/landing/sample_customers.csv")
dbutils.widgets.text("gold_path", "/tmp/sanctions_screening/gold_alerts")

silver_path = dbutils.widgets.get("silver_path")
customers_path = dbutils.widgets.get("customers_path")
gold_path = dbutils.widgets.get("gold_path")

# COMMAND ----------

import sys
sys.path.append("/Workspace/Repos/<your-repo-path>/sanctions-screening-databricks/src")  # noqa: E501

from sanctions_pipeline import screening

silver_df = spark.read.format("delta").load(silver_path)
customers_df = spark.read.option("header", True).csv(customers_path)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Scoring
# MAGIC Note: this cross-joins customers x entities and scores every pair, which
# MAGIC is fine at sample scale but would need blocking (e.g. first-letter or
# MAGIC phonetic-key pre-filtering) before it could handle a real customer book
# MAGIC against the full SDN list without an O(n*m) blowup. Naming that limit
# MAGIC here rather than hiding it.

# COMMAND ----------

alerts_df = screening.screen_customers(spark, customers_df, silver_df)
display(alerts_df)

# COMMAND ----------

screening.write_gold_alerts(alerts_df, gold_path)
print(f"Gold alerts written -> {gold_path}: {alerts_df.count()} alert(s)")
