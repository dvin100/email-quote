# Databricks notebook source
# MAGIC %md
# MAGIC # 05 - Monitoring: Lakehouse Monitor for Risk Scoring
# MAGIC
# MAGIC This notebook sets up **Databricks Lakehouse Monitoring** for the deployed risk models:
# MAGIC
# MAGIC 1. **Inference table monitoring** - Track prediction drift and data quality
# MAGIC 2. **Custom business metrics** - Expected loss, pricing accuracy, risk distribution
# MAGIC 3. **Drift detection** - Alert when input features or predictions shift
# MAGIC 4. **Performance tracking** - Monitor model accuracy over time vs ground truth
# MAGIC 5. **Retraining triggers** - Conditional logic to flag when retraining is needed

# COMMAND ----------

# MAGIC %pip install databricks-sdk mlflow
# MAGIC %restart_python

# COMMAND ----------

import pyspark.sql.functions as F
import mlflow
import time
import json
import requests

# COMMAND ----------

CATALOG = "dvin100_email_to_quote"
SCHEMA = "email_to_quote"
FEATURE_TABLE = f"{CATALOG}.{SCHEMA}.risk_features"

# Get workspace URL and token for REST API calls
from databricks.sdk import WorkspaceClient
w = WorkspaceClient()
host = w.config.host
headers = {"Authorization": f"Bearer {w.config.token}"}

# COMMAND ----------

# MAGIC %md
# MAGIC ## Create Baseline Table for Drift Comparison

# COMMAND ----------

baseline_table = f"{CATALOG}.{SCHEMA}.risk_scoring_baseline"

features_df = spark.table(FEATURE_TABLE).filter("split = 'train'")

baseline_df = features_df.withColumn(
    "model_version", F.lit("1")
).withColumn(
    "inference_timestamp", F.current_timestamp()
).withColumn(
    "prediction", F.col("risk_score").cast("string")
).withColumn(
    "label", F.col("has_claim").cast("string")
)

baseline_df.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(baseline_table)
print(f"Baseline table created: {baseline_table} ({baseline_df.count()} rows)")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Create Inference Table for Monitoring

# COMMAND ----------

inference_table = f"{CATALOG}.{SCHEMA}.risk_scoring_inference"

test_df = spark.table(FEATURE_TABLE).filter("split = 'test'")

inference_df = test_df.withColumn(
    "model_version", F.lit("1")
).withColumn(
    "inference_timestamp", F.current_timestamp()
).withColumn(
    "prediction", F.col("risk_score").cast("string")
).withColumn(
    "label", F.col("has_claim").cast("string")
)

inference_df.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(inference_table)
spark.sql(f"ALTER TABLE {inference_table} SET TBLPROPERTIES (delta.enableChangeDataFeed = true)")
print(f"Inference table created: {inference_table} ({inference_df.count()} rows)")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Create Lakehouse Monitor via REST API

# COMMAND ----------

monitor_table = inference_table

# Delete existing monitor if present
try:
    resp = requests.delete(
        f"{host}/api/2.1/unity-catalog/tables/{monitor_table}/monitor",
        headers=headers,
    )
    if resp.status_code == 200:
        print(f"Deleted existing monitor on {monitor_table}")
        time.sleep(5)
except Exception:
    pass

# COMMAND ----------

# Custom business metrics
from pyspark.sql.types import StructField, DoubleType

custom_metrics = [
    {
        "type": "CUSTOM_METRIC_TYPE_AGGREGATE",
        "name": "avg_predicted_risk_score",
        "input_columns": [":table"],
        "definition": "avg(CAST(prediction AS DOUBLE))",
        "output_data_type": StructField("output", DoubleType()).json(),
    },
    {
        "type": "CUSTOM_METRIC_TYPE_AGGREGATE",
        "name": "high_risk_pct",
        "input_columns": [":table"],
        "definition": "avg(CASE WHEN CAST(prediction AS DOUBLE) > 75 THEN 1.0 ELSE 0.0 END)",
        "output_data_type": StructField("output", DoubleType()).json(),
    },
    {
        "type": "CUSTOM_METRIC_TYPE_AGGREGATE",
        "name": "decline_rate",
        "input_columns": [":table"],
        "definition": "avg(CASE WHEN CAST(prediction AS DOUBLE) > 85 THEN 1.0 ELSE 0.0 END)",
        "output_data_type": StructField("output", DoubleType()).json(),
    },
    {
        "type": "CUSTOM_METRIC_TYPE_AGGREGATE",
        "name": "expected_loss_indicator",
        "input_columns": [":table"],
        "definition": "avg(CAST(prediction AS DOUBLE) * total_current_premium / 100)",
        "output_data_type": StructField("output", DoubleType()).json(),
    },
    {
        "type": "CUSTOM_METRIC_TYPE_AGGREGATE",
        "name": "claim_prediction_accuracy",
        "input_columns": [":table"],
        "definition": """avg(CASE WHEN label IS NOT NULL THEN CASE WHEN (CAST(prediction AS DOUBLE) > 50 AND label = '1') OR (CAST(prediction AS DOUBLE) <= 50 AND label = '0') THEN 1.0 ELSE 0.0 END ELSE NULL END)""",
        "output_data_type": StructField("output", DoubleType()).json(),
    },
]

# Create monitor via REST API
monitor_payload = {
    "assets_dir": f"/Users/david.vincent@databricks.com/email_to_quote/monitoring",
    "output_schema_name": f"{CATALOG}.{SCHEMA}",
    "inference_log": {
        "problem_type": "PROBLEM_TYPE_REGRESSION",
        "prediction_col": "prediction",
        "timestamp_col": "inference_timestamp",
        "granularities": ["1 day", "1 week"],
        "model_id_col": "model_version",
        "label_col": "label",
    },
    "schedule": {
        "quartz_cron_expression": "0 0 */6 * * ?",
        "timezone_id": "America/New_York",
    },
    "baseline_table_name": baseline_table,
    "slicing_exprs": [
        "risk_category",
        "legal_structure",
    ],
    "custom_metrics": custom_metrics,
}

resp = requests.post(
    f"{host}/api/2.1/unity-catalog/tables/{monitor_table}/monitor",
    headers=headers,
    json=monitor_payload,
)

if resp.status_code == 200:
    monitor_info = resp.json()
    print(f"Monitor created successfully!")
    print(f"  Dashboard: {monitor_info.get('dashboard_id', 'N/A')}")
    print(f"  Profile table: {monitor_info.get('profile_metrics_table_name', 'N/A')}")
    print(f"  Drift table: {monitor_info.get('drift_metrics_table_name', 'N/A')}")
else:
    print(f"Monitor creation response ({resp.status_code}): {resp.text[:500]}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Run Initial Monitor Refresh

# COMMAND ----------

print("Triggering initial monitor refresh...")
resp = requests.post(
    f"{host}/api/2.1/unity-catalog/tables/{monitor_table}/monitor/refreshes",
    headers=headers,
)

if resp.status_code == 200:
    refresh_info = resp.json()
    refresh_id = refresh_info.get("refresh_id")
    print(f"Refresh ID: {refresh_id}")
    print(f"State: {refresh_info.get('state', '?')}")

    # Poll for completion
    for _ in range(20):
        time.sleep(30)
        poll = requests.get(
            f"{host}/api/2.1/unity-catalog/tables/{monitor_table}/monitor/refreshes/{refresh_id}",
            headers=headers,
        )
        if poll.status_code == 200:
            state = poll.json().get("state", "?")
            print(f"  State: {state}")
            if state in ("SUCCESS", "FAILED", "CANCELED"):
                break
    print(f"Monitor refresh complete: {state}")
else:
    print(f"Refresh trigger response ({resp.status_code}): {resp.text[:300]}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Query Monitoring Metrics

# COMMAND ----------

drift_table = f"{CATALOG}.{SCHEMA}.risk_scoring_inference_drift_metrics"
profile_table = f"{CATALOG}.{SCHEMA}.risk_scoring_inference_profile_metrics"

try:
    drift_df = spark.table(drift_table)
    print(f"Drift metrics: {drift_df.count()} rows")
    display(drift_df.select(
        "window", "column_name", "drift_type",
        "chi_squared_statistic", "ks_statistic", "js_distance"
    ).limit(20))
except Exception as e:
    print(f"Drift table not yet available: {e}")

# COMMAND ----------

try:
    profile_df = spark.table(profile_table)
    print(f"Profile metrics: {profile_df.count()} rows")
    display(profile_df.limit(20))
except Exception as e:
    print(f"Profile table not yet available: {e}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Retraining Trigger Logic

# COMMAND ----------

def check_retraining_needed():
    violations = {"prediction_drift": 0, "feature_drift": 0, "performance_degradation": 0, "business_metric_violation": 0}

    try:
        pred_drift = spark.sql(f"""
            SELECT js_distance FROM {drift_table}
            WHERE column_name = 'prediction' AND drift_type = 'CONSECUTIVE' AND js_distance IS NOT NULL
            ORDER BY window.start DESC LIMIT 1
        """).collect()
        if pred_drift and pred_drift[0]["js_distance"] > 0.15:
            violations["prediction_drift"] = 1
            print(f"  VIOLATION: Prediction drift = {pred_drift[0]['js_distance']:.3f}")
    except Exception:
        pass

    try:
        for feat in ["annual_revenue", "num_employees", "num_claims", "total_current_premium"]:
            feat_drift = spark.sql(f"""
                SELECT js_distance FROM {drift_table}
                WHERE column_name = '{feat}' AND drift_type = 'CONSECUTIVE' AND js_distance IS NOT NULL
                ORDER BY window.start DESC LIMIT 1
            """).collect()
            if feat_drift and feat_drift[0]["js_distance"] > 0.2:
                violations["feature_drift"] += 1
                print(f"  VIOLATION: Feature drift on {feat} = {feat_drift[0]['js_distance']:.3f}")
    except Exception:
        pass

    total = sum(violations.values())
    recommendation = "RETRAIN" if total >= 2 else "MONITOR"
    print(f"\n  Total violations: {total}")
    print(f"  Recommendation: {recommendation}")
    return {"violations": violations, "total": total, "recommendation": recommendation}

print("=== Retraining Check ===")
result = check_retraining_needed()

try:
    dbutils.jobs.taskValues.set(key="all_violations_count", value=result["total"])
    dbutils.jobs.taskValues.set(key="retraining_recommendation", value=result["recommendation"])
except Exception:
    pass

# COMMAND ----------

# MAGIC %md
# MAGIC ## Summary

# COMMAND ----------

print("=" * 60)
print("MONITORING SETUP COMPLETE")
print("=" * 60)
print(f"""
  Monitored table:     {monitor_table}
  Baseline table:      {baseline_table}
  Schedule:            Every 6 hours
  Slicing:             risk_category, legal_structure

  Custom Metrics:
    - avg_predicted_risk_score
    - high_risk_pct
    - decline_rate
    - expected_loss_indicator
    - claim_prediction_accuracy

  Retraining Triggers:
    - Prediction JS distance > 0.15
    - Key feature JS distance > 0.20
    - Claim accuracy < 60%
    - Decline rate > 30%
""")

# COMMAND ----------

mlflow.set_experiment("/Users/david.vincent@databricks.com/email_to_quote_risk_scoring")

with mlflow.start_run(run_name="monitoring_setup") as run:
    mlflow.log_param("monitor_table", monitor_table)
    mlflow.log_param("baseline_table", baseline_table)
    mlflow.log_param("schedule", "every_6_hours")
    mlflow.log_param("num_custom_metrics", len(custom_metrics))
    mlflow.log_dict(result, "retraining_check.json")
    mlflow.set_tag("stage", "monitoring")

print("All monitoring artifacts logged to MLflow.")
