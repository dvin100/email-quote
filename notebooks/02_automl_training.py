# Databricks notebook source
# MAGIC %md
# MAGIC # 02 - ML Training: Commercial Insurance Risk Models
# MAGIC
# MAGIC This notebook trains ML models for three key targets:
# MAGIC 1. **Risk Classification** (`has_claim`): Predict whether an account will have a claim
# MAGIC 2. **Risk Score Regression** (`risk_score`): Predict composite risk score (0-100)
# MAGIC 3. **Loss Ratio Regression** (`loss_ratio`): Predict loss ratio for pricing
# MAGIC
# MAGIC Uses **LightGBM** with hyperparameter tuning via **Optuna**, tracked with **MLflow**.

# COMMAND ----------

# MAGIC %pip install mlflow scikit-learn lightgbm xgboost optuna
# MAGIC %restart_python

# COMMAND ----------

import mlflow
import mlflow.sklearn
import pyspark.sql.functions as F
import pandas as pd
import numpy as np
from sklearn.model_selection import cross_val_score
from sklearn.pipeline import Pipeline
from sklearn.compose import ColumnTransformer
from sklearn.preprocessing import OneHotEncoder, StandardScaler
from sklearn.impute import SimpleImputer
from sklearn.metrics import f1_score, precision_score, recall_score, mean_squared_error, r2_score, mean_absolute_error
from mlflow.models import infer_signature
import lightgbm as lgb
import optuna

# COMMAND ----------

CATALOG = "dvin100_email_to_quote"
SCHEMA = "email_to_quote"
FEATURE_TABLE = f"{CATALOG}.{SCHEMA}.risk_features"
EXPERIMENT_PATH = "/Users/david.vincent@databricks.com/email_to_quote_risk_scoring"

mlflow.set_experiment(EXPERIMENT_PATH)
mlflow.set_registry_uri("databricks-uc")
mlflow.sklearn.autolog(disable=True)  # We'll log manually for control

# COMMAND ----------

# MAGIC %md
# MAGIC ## Load Feature Table

# COMMAND ----------

features_df = spark.table(FEATURE_TABLE)
print(f"Feature table: {features_df.count()} rows, {len(features_df.columns)} columns")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Prepare Training Data

# COMMAND ----------

# Columns to exclude from features (identifiers, targets, leakage)
exclude_cols = [
    "org_id", "split",
    # Targets
    "has_claim", "risk_score", "loss_ratio", "claim_frequency_per_1m",
    # Target-derived / pricing outputs
    "gl_indicated_premium", "property_indicated_premium",
    "wc_indicated_premium", "auto_indicated_premium",
    "cyber_indicated_premium", "total_indicated_premium",
]

# Categorical columns
cat_cols = ["legal_structure", "risk_category", "naics_code"]

# All feature columns (including categoricals)
all_feature_cols = [c for c in features_df.columns if c not in exclude_cols]
num_cols = [c for c in all_feature_cols if c not in cat_cols]

print(f"Numeric features: {len(num_cols)}")
print(f"Categorical features: {len(cat_cols)}")

# Split data
train_pdf = features_df.filter("split = 'train'").toPandas()
test_pdf = features_df.filter("split = 'test'").toPandas()

print(f"Train: {len(train_pdf)} rows")
print(f"Test: {len(test_pdf)} rows")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Build Preprocessing Pipeline

# COMMAND ----------

def build_preprocessor(num_cols, cat_cols):
    """Build a sklearn ColumnTransformer for numeric + categorical features."""
    numeric_transformer = Pipeline(steps=[
        ("imputer", SimpleImputer(strategy="median")),
        ("scaler", StandardScaler()),
    ])
    categorical_transformer = Pipeline(steps=[
        ("imputer", SimpleImputer(strategy="constant", fill_value="unknown")),
        ("onehot", OneHotEncoder(handle_unknown="ignore", sparse_output=False)),
    ])
    return ColumnTransformer(transformers=[
        ("num", numeric_transformer, num_cols),
        ("cat", categorical_transformer, cat_cols),
    ])

# COMMAND ----------

# MAGIC %md
# MAGIC ## Model 1: Claim Classification (LightGBM + Optuna HPO)
# MAGIC
# MAGIC Predict `has_claim` - binary classification for submission triage and risk selection.

# COMMAND ----------

# Classification features - include categoricals
cls_feature_cols = num_cols + cat_cols
X_train_cls = train_pdf[cls_feature_cols]
y_train_cls = train_pdf["has_claim"]
X_test_cls = test_pdf[cls_feature_cols]
y_test_cls = test_pdf["has_claim"]

print(f"Target balance: {y_train_cls.value_counts().to_dict()}")

# COMMAND ----------

def objective_cls(trial):
    params = {
        "n_estimators": trial.suggest_int("n_estimators", 100, 500),
        "max_depth": trial.suggest_int("max_depth", 3, 10),
        "learning_rate": trial.suggest_float("learning_rate", 0.01, 0.3, log=True),
        "num_leaves": trial.suggest_int("num_leaves", 15, 127),
        "min_child_samples": trial.suggest_int("min_child_samples", 5, 50),
        "subsample": trial.suggest_float("subsample", 0.6, 1.0),
        "colsample_bytree": trial.suggest_float("colsample_bytree", 0.6, 1.0),
        "reg_alpha": trial.suggest_float("reg_alpha", 1e-8, 10.0, log=True),
        "reg_lambda": trial.suggest_float("reg_lambda", 1e-8, 10.0, log=True),
    }
    preprocessor = build_preprocessor(num_cols, cat_cols)
    model = Pipeline(steps=[
        ("preprocessor", preprocessor),
        ("classifier", lgb.LGBMClassifier(**params, random_state=42, verbose=-1)),
    ])
    scores = cross_val_score(model, X_train_cls, y_train_cls, cv=5, scoring="f1")
    return scores.mean()

study_cls = optuna.create_study(direction="maximize", study_name="claim_classification")
study_cls.optimize(objective_cls, n_trials=20, show_progress_bar=True)

print(f"Best F1 (CV): {study_cls.best_value:.4f}")
print(f"Best params: {study_cls.best_params}")

# COMMAND ----------

# Train final classification model with best params and log to MLflow
best_params_cls = study_cls.best_params
preprocessor_cls = build_preprocessor(num_cols, cat_cols)
best_cls_model = Pipeline(steps=[
    ("preprocessor", preprocessor_cls),
    ("classifier", lgb.LGBMClassifier(**best_params_cls, random_state=42, verbose=-1)),
])
best_cls_model.fit(X_train_cls, y_train_cls)

y_pred_cls = best_cls_model.predict(X_test_cls)
cls_f1 = f1_score(y_test_cls, y_pred_cls)
cls_precision = precision_score(y_test_cls, y_pred_cls)
cls_recall = recall_score(y_test_cls, y_pred_cls)

print(f"=== Claim Classification - Test Set ===")
print(f"  F1:        {cls_f1:.4f}")
print(f"  Precision: {cls_precision:.4f}")
print(f"  Recall:    {cls_recall:.4f}")

cls_signature = infer_signature(X_train_cls, y_pred_cls)

with mlflow.start_run(run_name="claim_classifier_lgbm") as run_cls:
    mlflow.log_params(best_params_cls)
    mlflow.log_metric("test_f1_score", cls_f1)
    mlflow.log_metric("test_precision", cls_precision)
    mlflow.log_metric("test_recall", cls_recall)
    mlflow.log_metric("cv_f1_score", study_cls.best_value)
    mlflow.sklearn.log_model(best_cls_model, "model", signature=cls_signature, input_example=X_train_cls.iloc[:3])
    mlflow.set_tag("model_type", "classification")
    mlflow.set_tag("target", "has_claim")
    mlflow.set_tag("algorithm", "lightgbm")
    best_cls_run_id = run_cls.info.run_id

print(f"Logged run: {best_cls_run_id}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Model 2: Risk Score Regression (LightGBM + Optuna HPO)
# MAGIC
# MAGIC Predict `risk_score` (0-100) for underwriter risk assessment.
# MAGIC Excludes claim history features to avoid leakage.

# COMMAND ----------

# Exclude claim-derived features for risk scoring (no leakage)
risk_exclude = [
    "num_claims", "total_claims_paid", "total_claims_reserved",
    "avg_claim_severity", "max_claim_severity", "num_claim_types",
    "num_open_claims", "num_bodily_injury_claims", "num_workers_comp_claims",
    "num_property_damage_claims", "num_cyber_claims", "days_since_last_claim",
    "num_claims_last_2yr",
]
risk_num_cols = [c for c in num_cols if c not in risk_exclude]
risk_feature_cols = risk_num_cols + cat_cols

X_train_risk = train_pdf[risk_feature_cols]
y_train_risk = train_pdf["risk_score"]
X_test_risk = test_pdf[risk_feature_cols]
y_test_risk = test_pdf["risk_score"]

# COMMAND ----------

def objective_risk(trial):
    params = {
        "n_estimators": trial.suggest_int("n_estimators", 100, 500),
        "max_depth": trial.suggest_int("max_depth", 3, 10),
        "learning_rate": trial.suggest_float("learning_rate", 0.01, 0.3, log=True),
        "num_leaves": trial.suggest_int("num_leaves", 15, 127),
        "min_child_samples": trial.suggest_int("min_child_samples", 5, 50),
        "subsample": trial.suggest_float("subsample", 0.6, 1.0),
        "colsample_bytree": trial.suggest_float("colsample_bytree", 0.6, 1.0),
        "reg_alpha": trial.suggest_float("reg_alpha", 1e-8, 10.0, log=True),
        "reg_lambda": trial.suggest_float("reg_lambda", 1e-8, 10.0, log=True),
    }
    preprocessor = build_preprocessor(risk_num_cols, cat_cols)
    model = Pipeline(steps=[
        ("preprocessor", preprocessor),
        ("regressor", lgb.LGBMRegressor(**params, random_state=42, verbose=-1)),
    ])
    scores = cross_val_score(model, X_train_risk, y_train_risk, cv=5, scoring="neg_root_mean_squared_error")
    return scores.mean()  # negative RMSE, so maximize

study_risk = optuna.create_study(direction="maximize", study_name="risk_score_regression")
study_risk.optimize(objective_risk, n_trials=20, show_progress_bar=True)

print(f"Best neg-RMSE (CV): {study_risk.best_value:.4f}")

# COMMAND ----------

# Train final risk score model
best_params_risk = study_risk.best_params
preprocessor_risk = build_preprocessor(risk_num_cols, cat_cols)
best_risk_model = Pipeline(steps=[
    ("preprocessor", preprocessor_risk),
    ("regressor", lgb.LGBMRegressor(**best_params_risk, random_state=42, verbose=-1)),
])
best_risk_model.fit(X_train_risk, y_train_risk)

y_pred_risk = best_risk_model.predict(X_test_risk)
risk_rmse = np.sqrt(mean_squared_error(y_test_risk, y_pred_risk))
risk_mae = mean_absolute_error(y_test_risk, y_pred_risk)
risk_r2 = r2_score(y_test_risk, y_pred_risk)

print(f"=== Risk Score Regression - Test Set ===")
print(f"  RMSE: {risk_rmse:.4f}")
print(f"  MAE:  {risk_mae:.4f}")
print(f"  R2:   {risk_r2:.4f}")

risk_signature = infer_signature(X_train_risk, y_pred_risk)

with mlflow.start_run(run_name="risk_score_lgbm") as run_risk:
    mlflow.log_params(best_params_risk)
    mlflow.log_metric("test_rmse", risk_rmse)
    mlflow.log_metric("test_mae", risk_mae)
    mlflow.log_metric("test_r2", risk_r2)
    mlflow.log_metric("cv_neg_rmse", study_risk.best_value)
    mlflow.sklearn.log_model(best_risk_model, "model", signature=risk_signature, input_example=X_train_risk.iloc[:3])
    mlflow.set_tag("model_type", "regression")
    mlflow.set_tag("target", "risk_score")
    mlflow.set_tag("algorithm", "lightgbm")
    best_risk_run_id = run_risk.info.run_id

print(f"Logged run: {best_risk_run_id}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Model 3: Loss Ratio Regression (LightGBM + Optuna HPO)
# MAGIC
# MAGIC Predict `loss_ratio` (total claims / total premium) for pricing optimization.

# COMMAND ----------

loss_feature_cols = num_cols + cat_cols
X_train_loss = train_pdf[loss_feature_cols]
y_train_loss = train_pdf["loss_ratio"]
X_test_loss = test_pdf[loss_feature_cols]
y_test_loss = test_pdf["loss_ratio"]

# COMMAND ----------

def objective_loss(trial):
    params = {
        "n_estimators": trial.suggest_int("n_estimators", 100, 500),
        "max_depth": trial.suggest_int("max_depth", 3, 10),
        "learning_rate": trial.suggest_float("learning_rate", 0.01, 0.3, log=True),
        "num_leaves": trial.suggest_int("num_leaves", 15, 127),
        "min_child_samples": trial.suggest_int("min_child_samples", 5, 50),
        "subsample": trial.suggest_float("subsample", 0.6, 1.0),
        "colsample_bytree": trial.suggest_float("colsample_bytree", 0.6, 1.0),
        "reg_alpha": trial.suggest_float("reg_alpha", 1e-8, 10.0, log=True),
        "reg_lambda": trial.suggest_float("reg_lambda", 1e-8, 10.0, log=True),
    }
    preprocessor = build_preprocessor(num_cols, cat_cols)
    model = Pipeline(steps=[
        ("preprocessor", preprocessor),
        ("regressor", lgb.LGBMRegressor(**params, random_state=42, verbose=-1)),
    ])
    scores = cross_val_score(model, X_train_loss, y_train_loss, cv=5, scoring="neg_root_mean_squared_error")
    return scores.mean()

study_loss = optuna.create_study(direction="maximize", study_name="loss_ratio_regression")
study_loss.optimize(objective_loss, n_trials=20, show_progress_bar=True)

print(f"Best neg-RMSE (CV): {study_loss.best_value:.4f}")

# COMMAND ----------

# Train final loss ratio model
best_params_loss = study_loss.best_params
preprocessor_loss = build_preprocessor(num_cols, cat_cols)
best_loss_model = Pipeline(steps=[
    ("preprocessor", preprocessor_loss),
    ("regressor", lgb.LGBMRegressor(**best_params_loss, random_state=42, verbose=-1)),
])
best_loss_model.fit(X_train_loss, y_train_loss)

y_pred_loss = best_loss_model.predict(X_test_loss)
loss_rmse = np.sqrt(mean_squared_error(y_test_loss, y_pred_loss))
loss_mae = mean_absolute_error(y_test_loss, y_pred_loss)
loss_r2 = r2_score(y_test_loss, y_pred_loss)

print(f"=== Loss Ratio Regression - Test Set ===")
print(f"  RMSE: {loss_rmse:.4f}")
print(f"  MAE:  {loss_mae:.4f}")
print(f"  R2:   {loss_r2:.4f}")

loss_signature = infer_signature(X_train_loss, y_pred_loss)

with mlflow.start_run(run_name="loss_ratio_lgbm") as run_loss:
    mlflow.log_params(best_params_loss)
    mlflow.log_metric("test_rmse", loss_rmse)
    mlflow.log_metric("test_mae", loss_mae)
    mlflow.log_metric("test_r2", loss_r2)
    mlflow.log_metric("cv_neg_rmse", study_loss.best_value)
    mlflow.sklearn.log_model(best_loss_model, "model", signature=loss_signature, input_example=X_train_loss.iloc[:3])
    mlflow.set_tag("model_type", "regression")
    mlflow.set_tag("target", "loss_ratio")
    mlflow.set_tag("algorithm", "lightgbm")
    best_loss_run_id = run_loss.info.run_id

print(f"Logged run: {best_loss_run_id}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## MLflow Model Evaluation Summary

# COMMAND ----------

with mlflow.start_run(run_name="training_summary") as eval_run:
    mlflow.log_metric("cls_test_f1", cls_f1)
    mlflow.log_metric("cls_test_precision", cls_precision)
    mlflow.log_metric("cls_test_recall", cls_recall)
    mlflow.log_metric("risk_test_rmse", risk_rmse)
    mlflow.log_metric("risk_test_r2", risk_r2)
    mlflow.log_metric("loss_test_rmse", loss_rmse)
    mlflow.log_metric("loss_test_r2", loss_r2)
    mlflow.log_param("best_cls_run_id", best_cls_run_id)
    mlflow.log_param("best_risk_run_id", best_risk_run_id)
    mlflow.log_param("best_loss_run_id", best_loss_run_id)
    mlflow.log_param("hpo_trials_per_model", 20)
    mlflow.log_param("algorithm", "lightgbm")
    mlflow.set_tag("stage", "training_evaluation")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Store Best Run IDs for Downstream Notebooks

# COMMAND ----------

try:
    dbutils.jobs.taskValues.set(key="best_cls_run_id", value=best_cls_run_id)
    dbutils.jobs.taskValues.set(key="best_risk_run_id", value=best_risk_run_id)
    dbutils.jobs.taskValues.set(key="best_loss_run_id", value=best_loss_run_id)
except Exception:
    pass

print(f"\nBest Run IDs:")
print(f"  Classification: {best_cls_run_id}")
print(f"  Risk Score:     {best_risk_run_id}")
print(f"  Loss Ratio:     {best_loss_run_id}")
print(f"\nTraining complete! Proceed to 03_model_registration.")
