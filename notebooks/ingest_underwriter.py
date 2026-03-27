# Databricks notebook source
# MAGIC %md
# MAGIC # Underwriter Decision Routing via Append Flows
# MAGIC
# MAGIC Reads from the `underwriter` Delta table (synced from Lakebase by a separate job)
# MAGIC using Change Data Feed streaming. Routes underwriter decisions:
# MAGIC - **uw-approved** → appends to `pipe_quote_creation`
# MAGIC - **uw-declined / uw-info** → appends to `pipe_completed`

# COMMAND ----------

import dlt
import pyspark.sql.functions as F

CATALOG = "dvin100_email_to_quote"
SCHEMA = "email_to_quote"

# COMMAND ----------

# MAGIC %md
# MAGIC ## Append Flow: uw-approved → pipe_quote_creation
# MAGIC
# MAGIC Streams new/updated rows from the `underwriter` table via CDF,
# MAGIC joins with `pipe_quote_review` (batch), and appends uw-approved quotes.

# COMMAND ----------

@dlt.append_flow(target="pipe_quote_creation")
def uw_approved_to_quote_creation():
    uw = (
        spark.readStream
        .option("readChangeFeed", "true")
        .table(f"{CATALOG}.{SCHEMA}.underwriter")
        .filter(F.col("_change_type").isin("insert", "update_postimage"))
        .filter(F.col("decision") == "uw-approved")
        .select(
            F.col("email_id").alias("uw_email_id"),
            F.col("decision").alias("uw_decision_tag"),
            "surcharge_pct",
            "discount_pct",
            "notes",
            "decided_at",
            "decided_by",
        )
    )

    review = (
        dlt.read("pipe_quote_review")
        .filter(F.col("decision_tag") == "pending-review")
        .drop("decision_tag")
    )

    joined = (
        uw.join(review, uw.uw_email_id == review.email_id, "inner")
        .drop("uw_email_id")
        .withColumn("decision_tag", F.col("uw_decision_tag"))
        .drop("uw_decision_tag")
    )

    # ── Multipliers (same logic as pipe_quote_creation) ──────────────
    risk_mult = (
        F.when(F.col("risk_score") <= 20, 0.85)
         .when(F.col("risk_score") <= 35, 0.95)
         .when(F.col("risk_score") <= 50, 1.0)
         .when(F.col("risk_score") <= 65, 1.10)
         .when(F.col("risk_score") <= 80, 1.25)
         .otherwise(1.50)
    )

    industry_mult = (
        F.when(F.col("risk_category") == "construction", 2.0)
         .when(F.col("risk_category") == "manufacturing", 1.5)
         .when(F.col("risk_category") == "healthcare", 1.8)
         .when(F.col("risk_category") == "food_service", 1.6)
         .when(F.col("risk_category") == "transportation", 1.7)
         .when(F.col("risk_category") == "hospitality", 1.4)
         .when(F.col("risk_category") == "retail", 1.2)
         .when(F.col("risk_category") == "professional_services", 0.8)
         .when(F.col("risk_category") == "technology", 0.9)
         .otherwise(1.0)
    )

    experience_mod = (
        F.when(F.col("num_claims_5yr") == 0, 0.90)
         .when(F.col("num_claims_5yr") <= 2, 1.0)
         .when(F.col("num_claims_5yr") <= 4, 1.15)
         .otherwise(1.35)
    )

    q = (
        joined
        .withColumn("risk_mult", risk_mult)
        .withColumn("industry_mult", industry_mult)
        .withColumn("experience_mod", experience_mod)
    )

    # ── Coverage-level premiums ──────────────────────────────────────
    q = q.withColumn("gl_premium", F.when(
        F.col("gl_limit_requested") > 0,
        F.round(F.col("annual_revenue") / 1000 * 1.50
                * F.col("industry_mult") * F.col("risk_mult"), 2),
    ).otherwise(0.0))

    q = q.withColumn("liquor_premium", F.when(
        F.col("risk_category").isin("food_service", "hospitality")
        & (F.col("gl_limit_requested") > 0),
        F.round(F.lit(1000.0) * F.col("risk_mult"), 2),
    ).otherwise(0.0))

    q = q.withColumn("property_building_premium", F.when(
        F.col("property_tiv") > 0,
        F.round(F.col("property_tiv") * 0.70 / 100 * 0.65 * F.col("risk_mult"), 2),
    ).otherwise(0.0))

    q = q.withColumn("property_contents_premium", F.when(
        F.col("property_tiv") > 0,
        F.round(F.col("property_tiv") * 0.30 / 100 * 0.80 * F.col("risk_mult"), 2),
    ).otherwise(0.0))

    q = q.withColumn(
        "bi_limit",
        F.least(F.col("annual_revenue") * 0.5, F.lit(500000.0)),
    )
    q = q.withColumn("bi_premium", F.when(
        F.col("annual_revenue") > 0,
        F.round(F.col("bi_limit") / 100 * 0.25 * F.col("risk_mult"), 2),
    ).otherwise(0.0))

    q = q.withColumn("equipment_premium", F.when(
        F.col("property_tiv") > 0,
        F.round(F.col("num_locations") * 300.0, 2),
    ).otherwise(0.0))

    q = q.withColumn("wc_premium", F.when(
        F.col("annual_payroll") > 0,
        F.round(F.col("annual_payroll") / 100 * 1.50
                * F.col("industry_mult") * F.col("experience_mod"), 2),
    ).otherwise(0.0))

    q = q.withColumn("auto_premium", F.when(
        F.col("auto_fleet_size") > 0,
        F.round(F.col("auto_fleet_size") * 2000.0 * F.col("risk_mult"), 2),
    ).otherwise(0.0))

    q = q.withColumn("cyber_premium", F.when(
        F.col("cyber_limit_requested") > 0,
        F.round(F.col("cyber_limit_requested") / 1000 * 2.50 * F.col("risk_mult"), 2),
    ).otherwise(0.0))

    q = q.withColumn("umbrella_premium", F.when(
        F.col("umbrella_limit_requested") > 0,
        F.round(F.col("umbrella_limit_requested") / 1_000_000 * 1500.0
                * F.col("risk_mult"), 2),
    ).otherwise(0.0))

    q = q.withColumn("terrorism_premium", F.round(
        (F.col("gl_premium") + F.col("property_building_premium")
         + F.col("property_contents_premium")) * 0.01,
        2,
    ))

    q = q.withColumn("policy_fees", F.lit(150.0))

    # ── Subtotal ─────────────────────────────────────────────────────
    q = q.withColumn("subtotal_premium", F.round(
        F.col("gl_premium") + F.col("liquor_premium")
        + F.col("property_building_premium") + F.col("property_contents_premium")
        + F.col("bi_premium") + F.col("equipment_premium")
        + F.col("wc_premium") + F.col("auto_premium")
        + F.col("cyber_premium") + F.col("umbrella_premium")
        + F.col("terrorism_premium") + F.col("policy_fees"),
        2,
    ))

    # ── Total premium with UW surcharge / discount ───────────────────
    q = q.withColumn("total_premium", F.round(
        F.col("subtotal_premium")
        * (1.0 + F.coalesce(F.col("surcharge_pct"), F.lit(0.0)) / 100.0)
        * (1.0 - F.coalesce(F.col("discount_pct"), F.lit(0.0)) / 100.0),
        2,
    ))

    # ── Quote metadata ───────────────────────────────────────────────
    q = (
        q
        .withColumn("quote_number", F.concat(
            F.lit("QT-"),
            F.date_format(F.current_timestamp(), "yyyyMMdd"),
            F.lit("-"),
            F.substring(F.col("email_id"), 1, 8),
        ))
        .withColumn("effective_date", F.date_add(F.current_date(), 14))
        .withColumn("expiration_date",
                     F.date_add(F.col("effective_date"), 365))
    )

    return q.select(
        "email_id", "quote_number", "business_name", "risk_category", "decision_tag",
        "review_summary",
        "risk_score", "risk_band", "predicted_loss_ratio", "pricing_action",
        "risk_mult", "industry_mult", "experience_mod",
        "gl_premium", "liquor_premium",
        "property_building_premium", "property_contents_premium",
        "bi_premium", "equipment_premium",
        "wc_premium", "auto_premium", "cyber_premium", "umbrella_premium",
        "terrorism_premium", "policy_fees",
        "subtotal_premium",
        # Underwriter adjustments
        "surcharge_pct", "discount_pct",
        F.col("notes").alias("underwriter_notes"),
        "decided_at", "decided_by",
        "total_premium",
        # Coverage details
        "gl_limit_requested", "property_tiv", "bi_limit",
        "auto_fleet_size", "cyber_limit_requested", "umbrella_limit_requested",
        "annual_revenue", "annual_payroll", "num_employees", "num_locations",
        "num_claims_5yr", "total_claims_amount",
        "has_safety_procedures", "has_employee_training",
        "effective_date", "expiration_date",
        "ingestion_timestamp", "scoring_timestamp", "review_timestamp",
        F.current_timestamp().alias("quote_timestamp"),
    )

# COMMAND ----------

# MAGIC %md
# MAGIC ## Append Flow: uw-declined / uw-info → pipe_completed

# COMMAND ----------

@dlt.append_flow(target="pipe_completed")
def uw_declined_info_to_completed():
    uw = (
        spark.readStream
        .option("readChangeFeed", "true")
        .table(f"{CATALOG}.{SCHEMA}.underwriter")
        .filter(F.col("_change_type").isin("insert", "update_postimage"))
        .filter(F.col("decision").isin("uw-declined", "uw-info"))
        .select(
            F.col("email_id").alias("uw_email_id"),
            F.col("decision").alias("decision_tag"),
            "notes",
            "info_request",
            "decided_at",
            "decided_by",
        )
    )

    review = (
        dlt.read("pipe_quote_review")
        .filter(F.col("decision_tag") == "pending-review")
        .drop("decision_tag")
    )

    joined = (
        uw.join(review, uw.uw_email_id == review.email_id, "inner")
        .drop("uw_email_id")
    )

    return joined.select(
        "email_id",
        F.lit(None).cast("string").alias("quote_number"),
        "business_name", "risk_category",
        "decision_tag", "review_summary",
        F.lit(None).cast("double").alias("total_premium"),
        "risk_score", "risk_band",
        F.lit(None).cast("string").alias("pdf_path"),
        F.lit("not_applicable").alias("pdf_status"),
        "ingestion_timestamp",
        F.current_timestamp().alias("completed_timestamp"),
        F.when(F.col("decision_tag") == "uw-declined", "uw_declined")
         .otherwise("uw_info_requested")
         .alias("final_status"),
    )
