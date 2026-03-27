# Databricks notebook source
# MAGIC %md
# MAGIC # Sync Lakebase `underwriter` → Delta Table (Append-Only)
# MAGIC
# MAGIC Reads new rows from Lakebase PostgreSQL `underwriter` table and **merges**
# MAGIC them into the Delta table. Only new rows (by `id`) are inserted — existing
# MAGIC rows are updated. This ensures the Delta table is append-friendly for
# MAGIC downstream streaming consumers.
# MAGIC
# MAGIC Scheduled to run every 10 seconds via a Databricks Job.

# COMMAND ----------

CATALOG = "dvin100_email_to_quote"
SCHEMA = "email_to_quote"
TABLE = "underwriter"

LAKEBASE_HOST = "ep-bitter-term-d8cbhgar.database.us-east-2.cloud.databricks.com"
LAKEBASE_USER = "fc0bfb5f-6069-4a8f-b4ce-84cc19949784"
LAKEBASE_PASSWORD = "BricksH0use!Ins2026#"
LAKEBASE_DB = "databricks_postgres"
LAKEBASE_SCHEMA = "email_to_quote"

# COMMAND ----------

# Read current state from Lakebase
source_df = (
    spark.read
    .format("postgresql")
    .option("host", LAKEBASE_HOST)
    .option("port", "5432")
    .option("database", LAKEBASE_DB)
    .option("dbtable", f"{LAKEBASE_SCHEMA}.{TABLE}")
    .option("user", LAKEBASE_USER)
    .option("password", LAKEBASE_PASSWORD)
    .load()
)

# COMMAND ----------

from delta.tables import DeltaTable
from pyspark.sql.functions import current_timestamp

target_table = f"{CATALOG}.{SCHEMA}.{TABLE}"

if spark.catalog.tableExists(target_table):
    dt = DeltaTable.forName(spark, target_table)
    dt.alias("t").merge(
        source_df.alias("s"),
        "t.id = s.id"
    ).whenMatchedUpdateAll(
    ).whenNotMatchedInsertAll(
    ).execute()
else:
    source_df.write.format("delta").saveAsTable(target_table)
    # Enable CDF for downstream streaming
    spark.sql(f"ALTER TABLE {target_table} SET TBLPROPERTIES (delta.enableChangeDataFeed = true)")

# COMMAND ----------

print(f"Synced {source_df.count()} rows to {target_table}")
