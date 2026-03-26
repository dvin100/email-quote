# Databricks notebook source
# MAGIC %md
# MAGIC # 03 - Model Registration: Unity Catalog Model Registry
# MAGIC
# MAGIC This notebook registers the best models from AutoML training into **Unity Catalog**
# MAGIC using the Champion/Challenger pattern:
# MAGIC
# MAGIC 1. Register best models from AutoML runs
# MAGIC 2. Add descriptions, tags, and metadata
# MAGIC 3. Validate Challenger against Champion (if exists)
# MAGIC 4. Promote to Champion alias

# COMMAND ----------

# MAGIC %pip install mlflow
# MAGIC %restart_python

# COMMAND ----------

import mlflow
from mlflow import MlflowClient
import json

# COMMAND ----------

CATALOG = "dvin100_email_to_quote"
SCHEMA = "email_to_quote"
EXPERIMENT_PATH = "/Users/david.vincent@databricks.com/email_to_quote_risk_scoring"

# Model names in Unity Catalog
MODEL_CLAIM_CLS = f"{CATALOG}.{SCHEMA}.risk_claim_classifier"
MODEL_RISK_SCORE = f"{CATALOG}.{SCHEMA}.risk_score_regressor"
MODEL_LOSS_RATIO = f"{CATALOG}.{SCHEMA}.loss_ratio_regressor"

mlflow.set_registry_uri("databricks-uc")
client = MlflowClient()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Retrieve Best Run IDs
# MAGIC
# MAGIC Get the best runs from AutoML training (via task values or experiment search).

# COMMAND ----------

def get_best_run_id(run_name, metric_name, ascending=True):
    """Find the best run by name and metric in the main experiment."""
    experiment = mlflow.get_experiment_by_name(EXPERIMENT_PATH)
    if experiment is None:
        raise ValueError(f"Experiment not found: {EXPERIMENT_PATH}")

    order = "ASC" if ascending else "DESC"
    runs = mlflow.search_runs(
        experiment_ids=[experiment.experiment_id],
        order_by=[f"metrics.{metric_name} {order}"],
        max_results=1,
        filter_string=f"status = 'FINISHED' and tags.mlflow.runName = '{run_name}'",
    )
    if runs.empty:
        raise ValueError(f"No finished runs found with name {run_name}")

    run_id = runs.iloc[0]["run_id"]
    metric_val = runs.iloc[0][f"metrics.{metric_name}"]
    print(f"  Best run for {run_name}: {run_id} ({metric_name}={metric_val:.4f})")
    return run_id, metric_val

# Try task values first (if running as job), then search experiments
try:
    best_cls_run_id = dbutils.jobs.taskValues.get(taskKey="automl_training", key="best_cls_run_id")
    best_risk_run_id = dbutils.jobs.taskValues.get(taskKey="automl_training", key="best_risk_run_id")
    best_loss_run_id = dbutils.jobs.taskValues.get(taskKey="automl_training", key="best_loss_run_id")
    print("Retrieved run IDs from task values")
except Exception:
    print("Searching experiments for best runs...")
    best_cls_run_id, cls_metric = get_best_run_id("claim_classifier_lgbm", "test_f1_score", ascending=False)
    best_risk_run_id, risk_metric = get_best_run_id("risk_score_lgbm", "test_rmse", ascending=True)
    best_loss_run_id, loss_metric = get_best_run_id("loss_ratio_lgbm", "test_rmse", ascending=True)

print(f"\nRun IDs to register:")
print(f"  Claim classifier:   {best_cls_run_id}")
print(f"  Risk score:         {best_risk_run_id}")
print(f"  Loss ratio:         {best_loss_run_id}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Register Models in Unity Catalog

# COMMAND ----------

def register_model(run_id, model_name, description, tags):
    """Register a model from a run into Unity Catalog."""
    model_uri = f"runs:/{run_id}/model"

    # Register
    mv = mlflow.register_model(model_uri, model_name)
    print(f"Registered {model_name} version {mv.version}")

    # Add description to registered model
    client.update_registered_model(
        name=model_name,
        description=description,
    )

    # Add description and tags to this version
    client.update_model_version(
        name=model_name,
        version=mv.version,
        description=f"Trained via AutoML from run {run_id}",
    )

    for key, value in tags.items():
        client.set_model_version_tag(
            name=model_name,
            version=mv.version,
            key=key,
            value=str(value),
        )

    return mv

# --- Register Claim Classifier ---
mv_cls = register_model(
    run_id=best_cls_run_id,
    model_name=MODEL_CLAIM_CLS,
    description=(
        "Binary classifier predicting whether a commercial insurance account will have a claim. "
        "Used for submission triage and risk selection. "
        "Output: 0 (no claim expected) or 1 (claim expected)."
    ),
    tags={
        "model_type": "classification",
        "target": "has_claim",
        "use_case": "submission_triage_risk_selection",
        "training_method": "automl",
        "insurance_domain": "commercial_p_and_c",
    },
)

# COMMAND ----------

# --- Register Risk Score Regressor ---
mv_risk = register_model(
    run_id=best_risk_run_id,
    model_name=MODEL_RISK_SCORE,
    description=(
        "Regression model predicting composite risk score (0-100) for commercial insurance accounts. "
        "Higher scores indicate worse risk. Used by underwriters to assess account risk band "
        "and determine pricing adjustments. Does NOT use claim history as input (no leakage)."
    ),
    tags={
        "model_type": "regression",
        "target": "risk_score",
        "use_case": "risk_assessment_scoring",
        "training_method": "automl",
        "insurance_domain": "commercial_p_and_c",
        "score_range": "0-100",
    },
)

# COMMAND ----------

# --- Register Loss Ratio Regressor ---
mv_loss = register_model(
    run_id=best_loss_run_id,
    model_name=MODEL_LOSS_RATIO,
    description=(
        "Regression model predicting expected loss ratio (total claims / total premium) "
        "for commercial insurance accounts. Used for pricing optimization to determine "
        "technical premium and acceptable price range."
    ),
    tags={
        "model_type": "regression",
        "target": "loss_ratio",
        "use_case": "pricing_optimization",
        "training_method": "automl",
        "insurance_domain": "commercial_p_and_c",
    },
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Champion / Challenger Validation

# COMMAND ----------

def validate_and_promote(model_name, new_version, metric_name, lower_is_better=True):
    """
    Compare Challenger (new version) against current Champion.
    Promote if better or if no Champion exists.
    """
    new_run_id = client.get_model_version(model_name, new_version).run_id
    new_run = mlflow.get_run(new_run_id)
    new_metric = new_run.data.metrics.get(metric_name)

    if new_metric is None:
        # Try test_ prefix
        new_metric = new_run.data.metrics.get(f"test_{metric_name}")

    # Set as Challenger first
    client.set_registered_model_alias(
        name=model_name, alias="Challenger", version=new_version
    )
    print(f"\n--- Validating {model_name} v{new_version} ---")
    print(f"  Challenger {metric_name}: {new_metric}")

    # Check if Champion exists
    try:
        champion_info = client.get_model_version_by_alias(model_name, "Champion")
        champion_run = mlflow.get_run(champion_info.run_id)
        champion_metric = champion_run.data.metrics.get(metric_name)
        if champion_metric is None:
            champion_metric = champion_run.data.metrics.get(f"test_{metric_name}")

        print(f"  Champion   {metric_name}: {champion_metric} (v{champion_info.version})")

        # Compare
        if lower_is_better:
            is_better = (new_metric is not None) and (champion_metric is None or new_metric < champion_metric)
        else:
            is_better = (new_metric is not None) and (champion_metric is None or new_metric > champion_metric)

        if is_better:
            print(f"  --> Challenger is BETTER. Promoting to Champion!")
            client.set_registered_model_alias(name=model_name, alias="Champion", version=new_version)
            client.set_model_version_tag(model_name, new_version, "validation_status", "passed")
        else:
            print(f"  --> Champion is still better. Keeping existing Champion.")
            client.set_model_version_tag(model_name, new_version, "validation_status", "challenger_only")

    except Exception:
        # No Champion exists yet - promote directly
        print(f"  No existing Champion. Promoting v{new_version} to Champion.")
        client.set_registered_model_alias(name=model_name, alias="Champion", version=new_version)
        client.set_model_version_tag(model_name, new_version, "validation_status", "first_champion")

# COMMAND ----------

# Validate and promote each model
validate_and_promote(MODEL_CLAIM_CLS, mv_cls.version, "f1_score", lower_is_better=False)
validate_and_promote(MODEL_RISK_SCORE, mv_risk.version, "rmse", lower_is_better=True)
validate_and_promote(MODEL_LOSS_RATIO, mv_loss.version, "rmse", lower_is_better=True)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Summary

# COMMAND ----------

print("=" * 60)
print("MODEL REGISTRATION SUMMARY")
print("=" * 60)
for name, mv in [(MODEL_CLAIM_CLS, mv_cls), (MODEL_RISK_SCORE, mv_risk), (MODEL_LOSS_RATIO, mv_loss)]:
    try:
        champion = client.get_model_version_by_alias(name, "Champion")
        champ_ver = champion.version
    except Exception:
        champ_ver = "N/A"
    print(f"\n  {name}")
    print(f"    Registered version: {mv.version}")
    print(f"    Champion version:   {champ_ver}")

print(f"\nModel registration complete! Proceed to 04_model_serving.")
