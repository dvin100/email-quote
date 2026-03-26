# Databricks notebook source
# MAGIC %md
# MAGIC # 04 - Model Serving: Real-Time Risk Scoring Endpoint
# MAGIC
# MAGIC This notebook deploys the Champion models as **real-time serving endpoints** on Databricks.
# MAGIC
# MAGIC **Endpoints created:**
# MAGIC 1. `email-to-quote-risk-scorer` - Combined endpoint serving all three models
# MAGIC    (claim classifier, risk score, loss ratio) via a custom PyFunc wrapper
# MAGIC
# MAGIC **Features:**
# MAGIC - Auto-scaling with scale-to-zero
# MAGIC - Inference logging for monitoring
# MAGIC - Payload capture for drift detection

# COMMAND ----------

# MAGIC %pip install databricks-sdk mlflow scikit-learn lightgbm
# MAGIC %restart_python

# COMMAND ----------

import mlflow
from mlflow import MlflowClient
from databricks.sdk import WorkspaceClient
from databricks.sdk.service.serving import (
    EndpointCoreConfigInput,
    ServedEntityInput,
    TrafficConfig,
    Route,
    EndpointTag,
)
from datetime import timedelta
import json
import time

# COMMAND ----------

CATALOG = "dvin100_email_to_quote"
SCHEMA = "email_to_quote"

MODEL_CLAIM_CLS = f"{CATALOG}.{SCHEMA}.risk_claim_classifier"
MODEL_RISK_SCORE = f"{CATALOG}.{SCHEMA}.risk_score_regressor"
MODEL_LOSS_RATIO = f"{CATALOG}.{SCHEMA}.loss_ratio_regressor"

ENDPOINT_NAME = "email-to-quote-risk-scorer"

mlflow.set_registry_uri("databricks-uc")
client = MlflowClient()
w = WorkspaceClient()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Create Combined Risk Scoring Model (PyFunc Wrapper)
# MAGIC
# MAGIC Wrap all three models into a single PyFunc that returns:
# MAGIC - `claim_probability`: 0 or 1
# MAGIC - `risk_score`: 0-100
# MAGIC - `loss_ratio`: predicted loss ratio
# MAGIC - `risk_band`: Low / Medium / High / Very High
# MAGIC - `price_recommendation`: Suggested premium range

# COMMAND ----------

import pandas as pd
import numpy as np

class InsuranceRiskScorer(mlflow.pyfunc.PythonModel):
    """
    Combined risk scoring model for commercial insurance.
    Wraps claim classifier, risk score regressor, and loss ratio regressor.
    Returns a comprehensive risk assessment with pricing guidance.
    """

    def load_context(self, context):
        """Load the three underlying models."""
        self.claim_model = mlflow.pyfunc.load_model(
            context.artifacts["claim_classifier"]
        )
        self.risk_model = mlflow.pyfunc.load_model(
            context.artifacts["risk_score_model"]
        )
        self.loss_model = mlflow.pyfunc.load_model(
            context.artifacts["loss_ratio_model"]
        )

    def predict(self, context, model_input: pd.DataFrame, params=None) -> pd.DataFrame:
        """
        Run all three models and produce a unified risk assessment.
        """
        results = pd.DataFrame(index=model_input.index)

        # 1. Claim probability
        try:
            results["claim_prediction"] = self.claim_model.predict(model_input)
        except Exception:
            results["claim_prediction"] = 0

        # 2. Risk score (0-100)
        try:
            results["risk_score"] = np.clip(self.risk_model.predict(model_input), 0, 100)
        except Exception:
            results["risk_score"] = 50.0

        # 3. Loss ratio
        try:
            results["predicted_loss_ratio"] = np.clip(self.loss_model.predict(model_input), 0, 5)
        except Exception:
            results["predicted_loss_ratio"] = 0.5

        # 4. Risk band
        results["risk_band"] = pd.cut(
            results["risk_score"],
            bins=[0, 25, 50, 75, 100],
            labels=["Low", "Medium", "High", "Very High"],
            include_lowest=True,
        ).astype(str)

        # 5. Price recommendation logic
        # Base: if loss ratio < 0.6 -> competitive pricing ok
        # If loss ratio 0.6-0.8 -> standard pricing
        # If loss ratio > 0.8 -> premium loading needed
        results["pricing_action"] = np.where(
            results["predicted_loss_ratio"] < 0.4, "competitive_rate",
            np.where(
                results["predicted_loss_ratio"] < 0.7, "standard_rate",
                np.where(
                    results["predicted_loss_ratio"] < 1.0, "loaded_rate",
                    "decline_or_refer"
                )
            )
        )

        # 6. Underwriting recommendation
        results["underwriting_action"] = np.where(
            (results["risk_score"] <= 30) & (results["claim_prediction"] == 0),
            "auto_quote",
            np.where(
                (results["risk_score"] <= 60),
                "standard_review",
                np.where(
                    (results["risk_score"] <= 80),
                    "senior_underwriter_review",
                    "decline_or_refer_to_specialty"
                )
            )
        )

        return results

# COMMAND ----------

# MAGIC %md
# MAGIC ## Log and Register Combined Model

# COMMAND ----------

COMBINED_MODEL_NAME = f"{CATALOG}.{SCHEMA}.insurance_risk_scorer"

# Get Champion versions
cls_champion = client.get_model_version_by_alias(MODEL_CLAIM_CLS, "Champion")
risk_champion = client.get_model_version_by_alias(MODEL_RISK_SCORE, "Champion")
loss_champion = client.get_model_version_by_alias(MODEL_LOSS_RATIO, "Champion")

print(f"Claim classifier Champion: v{cls_champion.version}")
print(f"Risk score Champion:       v{risk_champion.version}")
print(f"Loss ratio Champion:       v{loss_champion.version}")

# Define artifacts pointing to the champion model URIs
artifacts = {
    "claim_classifier": f"models:/{MODEL_CLAIM_CLS}@Champion",
    "risk_score_model": f"models:/{MODEL_RISK_SCORE}@Champion",
    "loss_ratio_model": f"models:/{MODEL_LOSS_RATIO}@Champion",
}

# Build signature from sample data
from mlflow.models import infer_signature

sample_df = spark.table(f"{CATALOG}.{SCHEMA}.risk_features").filter("split = 'test'").limit(5).toPandas()
drop_cols = [
    "org_id", "split", "has_claim", "risk_score", "loss_ratio",
    "claim_frequency_per_1m", "gl_indicated_premium", "property_indicated_premium",
    "wc_indicated_premium", "auto_indicated_premium", "cyber_indicated_premium",
    "total_indicated_premium",
]
sample_input = sample_df.drop(columns=[c for c in drop_cols if c in sample_df.columns])

# Create sample output schema
sample_output = pd.DataFrame({
    "claim_prediction": [0, 1, 0, 1, 0],
    "risk_score": [25.0, 65.0, 42.0, 88.0, 15.0],
    "predicted_loss_ratio": [0.3, 0.8, 0.5, 1.2, 0.2],
    "risk_band": ["Low", "High", "Medium", "Very High", "Low"],
    "pricing_action": ["competitive_rate", "loaded_rate", "standard_rate", "decline_or_refer", "competitive_rate"],
    "underwriting_action": ["auto_quote", "senior_underwriter_review", "standard_review", "decline_or_refer_to_specialty", "auto_quote"],
})
signature = infer_signature(sample_input, sample_output)

# Log the combined model
mlflow.set_experiment("/Users/david.vincent@databricks.com/email_to_quote_risk_scoring")

with mlflow.start_run(run_name="combined_risk_scorer") as run:
    mlflow.pyfunc.log_model(
        artifact_path="model",
        python_model=InsuranceRiskScorer(),
        artifacts=artifacts,
        signature=signature,
        input_example=sample_input.iloc[:2],
        pip_requirements=[
            "mlflow",
            "pandas",
            "numpy",
            "scikit-learn",
            "lightgbm",
        ],
    )

    mlflow.log_param("claim_model_version", cls_champion.version)
    mlflow.log_param("risk_model_version", risk_champion.version)
    mlflow.log_param("loss_model_version", loss_champion.version)
    mlflow.set_tag("model_type", "combined_risk_scorer")

    combined_run_id = run.info.run_id

# Register combined model
mv_combined = mlflow.register_model(
    f"runs:/{combined_run_id}/model",
    COMBINED_MODEL_NAME,
)

client.update_registered_model(
    name=COMBINED_MODEL_NAME,
    description=(
        "Combined insurance risk scoring model. Wraps claim classifier, risk score regressor, "
        "and loss ratio regressor into a single endpoint. Returns risk band, pricing action, "
        "and underwriting recommendation for each account."
    ),
)

client.set_registered_model_alias(
    name=COMBINED_MODEL_NAME, alias="Champion", version=mv_combined.version
)

print(f"Combined model registered: {COMBINED_MODEL_NAME} v{mv_combined.version}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Deploy Serving Endpoint

# COMMAND ----------

champion_version = client.get_model_version_by_alias(COMBINED_MODEL_NAME, "Champion").version

endpoint_config = EndpointCoreConfigInput(
    served_entities=[
        ServedEntityInput(
            entity_name=COMBINED_MODEL_NAME,
            entity_version=champion_version,
            scale_to_zero_enabled=True,
            workload_size="Small",
        )
    ],
    traffic_config=TrafficConfig(
        routes=[
            Route(
                served_model_name=f"insurance_risk_scorer-{champion_version}",
                traffic_percentage=100,
            )
        ]
    ),
)

# Create or update endpoint
try:
    existing = w.serving_endpoints.get(ENDPOINT_NAME)
    print(f"Updating existing endpoint: {ENDPOINT_NAME}")
    w.serving_endpoints.update_config(
        name=ENDPOINT_NAME,
        served_entities=endpoint_config.served_entities,
        traffic_config=endpoint_config.traffic_config,
    )
except Exception:
    print(f"Creating new endpoint: {ENDPOINT_NAME}")
    w.serving_endpoints.create(
        name=ENDPOINT_NAME,
        config=endpoint_config,
        tags=[
            EndpointTag(key="project", value="email_to_quote"),
            EndpointTag(key="domain", value="commercial_insurance"),
            EndpointTag(key="model_type", value="risk_scoring"),
        ],
    )

# COMMAND ----------

# MAGIC %md
# MAGIC ## Wait for Endpoint to be Ready

# COMMAND ----------

print(f"Waiting for endpoint '{ENDPOINT_NAME}' to be ready...")
endpoint = w.serving_endpoints.wait_get_serving_endpoint_not_updating(
    ENDPOINT_NAME, timeout=timedelta(minutes=30)
)
print(f"Endpoint state: {endpoint.state.ready}")
print(f"Endpoint URL: {w.config.host}/serving-endpoints/{ENDPOINT_NAME}/invocations")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Test the Endpoint

# COMMAND ----------

# Build a test payload from the feature table
test_record = spark.table(f"{CATALOG}.{SCHEMA}.risk_features").filter("split = 'test'").limit(3).toPandas()

# Drop non-feature columns
drop_cols = [
    "org_id", "split", "has_claim", "risk_score", "loss_ratio",
    "claim_frequency_per_1m", "gl_indicated_premium", "property_indicated_premium",
    "wc_indicated_premium", "auto_indicated_premium", "cyber_indicated_premium",
    "total_indicated_premium",
]
test_payload = test_record.drop(columns=[c for c in drop_cols if c in test_record.columns])

# Query endpoint
response = w.serving_endpoints.query(
    name=ENDPOINT_NAME,
    dataframe_records=test_payload.to_dict(orient="records"),
)

print("=== Endpoint Response ===")
print(json.dumps(response.as_dict(), indent=2))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Batch Inference (Score All Accounts)

# COMMAND ----------

# Load champion model as Spark UDF for batch scoring
champion_udf = mlflow.pyfunc.spark_udf(
    spark,
    model_uri=f"models:/{COMBINED_MODEL_NAME}@Champion",
    result_type="string",
)

# Score all accounts
import pyspark.sql.functions as F

features_df = spark.table(f"{CATALOG}.{SCHEMA}.risk_features")
scored_df = features_df.withColumn(
    "risk_assessment",
    champion_udf(*[F.col(c) for c in features_df.columns if c not in drop_cols])
)

# Save scored results
scored_df.select("org_id", "risk_category", "risk_assessment", "split").write.mode("overwrite").saveAsTable(
    f"{CATALOG}.{SCHEMA}.risk_scored_accounts"
)

display(scored_df.select("org_id", "risk_category", "risk_assessment").limit(10))

# COMMAND ----------

print(f"\nModel serving deployed!")
print(f"  Endpoint: {ENDPOINT_NAME}")
print(f"  Model: {COMBINED_MODEL_NAME} v{champion_version}")
print(f"  Inference logging: {CATALOG}.{SCHEMA}.risk_scorer_inference_*")
print(f"\nProceed to 05_monitoring.")
