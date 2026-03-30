# Databricks notebook source
# MAGIC %md
# MAGIC # Continuous Sync: Lakebase `underwriter` → Delta Table
# MAGIC
# MAGIC Polls Lakebase every 5 seconds and merges new/updated underwriter decisions
# MAGIC into the Delta table with CDF enabled, so the DLT pipeline's append flows
# MAGIC pick up changes in near real-time.

# COMMAND ----------

CATALOG = "dvin100_email_to_quote"
SCHEMA = "email_to_quote"
TABLE = "underwriter"

import os
import time

LAKEBASE_HOST = os.environ.get("LAKEBASE_HOST", "ep-icy-pond-d8d33jwn.database.us-east-2.cloud.databricks.com")
LAKEBASE_DB = os.environ.get("LAKEBASE_DB", "databricks_postgres")
LAKEBASE_SCHEMA = "email_to_quote"

dbutils.widgets.text("lb_user", "")
dbutils.widgets.text("lb_pass", "")
dbutils.widgets.text("poll_interval", "5")
LAKEBASE_USER = dbutils.widgets.get("lb_user")
LAKEBASE_PASSWORD = dbutils.widgets.get("lb_pass")
POLL_INTERVAL = int(dbutils.widgets.get("poll_interval"))

target_table = f"{CATALOG}.{SCHEMA}.{TABLE}"

# COMMAND ----------

from delta.tables import DeltaTable

def read_lakebase():
    return (
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

def sync_once():
    source_df = read_lakebase()
    if spark.catalog.tableExists(target_table):
        dt = DeltaTable.forName(spark, target_table)
        # Only INSERT new rows — never update existing ones.
        # This prevents CDF from generating spurious update_postimage events
        # on every sync cycle, which would cause duplicate downstream processing.
        dt.alias("t").merge(
            source_df.alias("s"),
            "t.id = s.id"
        ).whenNotMatchedInsertAll(
        ).execute()
    else:
        source_df.write.format("delta").saveAsTable(target_table)
        spark.sql(f"ALTER TABLE {target_table} SET TBLPROPERTIES (delta.enableChangeDataFeed = true)")
    return source_df.count()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Continuous polling loop

# COMMAND ----------

print(f"Starting continuous sync (every {POLL_INTERVAL}s)...")
cycle = 0
while True:
    try:
        cycle += 1
        row_count = sync_once()
        if cycle % 12 == 1:  # Log every ~60s
            print(f"[cycle {cycle}] Synced {row_count} rows")
    except Exception as e:
        print(f"[cycle {cycle}] Error: {e}")
    time.sleep(POLL_INTERVAL)
