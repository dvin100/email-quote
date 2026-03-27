# Databricks notebook source
# MAGIC %md
# MAGIC # Reset All `pipe_*` Tables
# MAGIC
# MAGIC Truncates every table in `dvin100_email_to_quote.email_to_quote` whose name
# MAGIC starts with `pipe_`. Use this to start fresh before a full pipeline refresh.

# COMMAND ----------

CATALOG = "dvin100_email_to_quote"
SCHEMA = "email_to_quote"

# Drop stale intermediate tables from previous approach
DROP_TABLES = [
    "pipe_uw_completed_input",
    "pipe_uw_quote_creation_input",
]

print("Dropping stale tables:\n")
for t in DROP_TABLES:
    fqn = f"{CATALOG}.{SCHEMA}.{t}"
    try:
        spark.sql(f"DROP TABLE IF EXISTS {fqn}")
        print(f"  ✓ Dropped {fqn}")
    except Exception as e:
        print(f"  ✗ {fqn}: {e}")

# Truncate all pipe_ tables
tables = [row.tableName for row in spark.sql(f"SHOW TABLES IN {CATALOG}.{SCHEMA} LIKE 'pipe_*'").collect()]

print(f"\nFound {len(tables)} pipe_ tables to truncate:\n")

for t in sorted(tables):
    fqn = f"{CATALOG}.{SCHEMA}.{t}"
    try:
        spark.sql(f"TRUNCATE TABLE {fqn}")
        print(f"  ✓ {fqn}")
    except Exception as e:
        # Streaming tables may not support TRUNCATE — use DELETE instead
        try:
            spark.sql(f"DELETE FROM {fqn}")
            print(f"  ✓ {fqn} (via DELETE)")
        except Exception as e2:
            print(f"  ✗ {fqn}: {e2}")

print("\nDone.")
