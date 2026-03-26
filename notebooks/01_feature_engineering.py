# Databricks notebook source
# MAGIC %md
# MAGIC # 01 - Feature Engineering: Commercial Insurance Risk Scoring
# MAGIC
# MAGIC This notebook builds an **org-level feature table** by joining all 11 source tables
# MAGIC into a single risk-profile dataset suitable for ML training.
# MAGIC
# MAGIC **Features include:**
# MAGIC - Business descriptors (industry, size, revenue, payroll)
# MAGIC - Location & property risk indicators (building age, fire protection, construction type)
# MAGIC - Fleet & driver risk (vehicle count, avg violations, driver experience)
# MAGIC - Cyber posture score
# MAGIC - Financial concentration & trends
# MAGIC - Historical loss experience (claim frequency, severity, loss ratio)
# MAGIC - Coverage complexity (policy count, contract requirements)

# COMMAND ----------

# MAGIC %pip install mlflow
# MAGIC %restart_python

# COMMAND ----------

import pyspark.sql.functions as F
from pyspark.sql.types import DoubleType, IntegerType
import mlflow

# COMMAND ----------

CATALOG = "dvin100_email_to_quote"
SCHEMA = "email_to_quote"

def table(name):
    return f"{CATALOG}.{SCHEMA}.{name}"

# COMMAND ----------

# MAGIC %md
# MAGIC ## Load Source Tables

# COMMAND ----------

orgs = spark.table(table("organizations"))
locations = spark.table(table("locations"))
employees = spark.table(table("employees"))
property_assets = spark.table(table("property_assets"))
vehicles = spark.table(table("vehicles"))
cyber = spark.table(table("cyber_profiles"))
financials = spark.table(table("financials"))
policies = spark.table(table("policies"))
claims = spark.table(table("claims"))
coverage_requests = spark.table(table("coverage_requests"))
contracts = spark.table(table("contracts"))

print(f"Organizations: {orgs.count()}")
print(f"Claims: {claims.count()}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Organization Base Features

# COMMAND ----------

org_features = orgs.select(
    "org_id",
    "legal_structure",
    "naics_code",
    "risk_category",
    "num_employees",
    "num_contractors",
    F.col("uses_subcontractors").cast("int").alias("uses_subcontractors"),
    "annual_revenue",
    "annual_payroll",
    F.col("has_written_safety_procedures").cast("int").alias("has_safety_procedures"),
    F.col("has_employee_training_program").cast("int").alias("has_training_program"),
    F.col("has_cyber_controls").cast("int").alias("has_cyber_controls"),
    # Derived features
    F.when(F.col("years_current_ownership").isNull(), 0).otherwise(F.col("years_current_ownership")).alias("years_current_ownership"),
    F.datediff(F.current_date(), F.col("date_established")).alias("business_age_days"),
    # Payroll per employee
    F.when(F.col("num_employees") > 0, F.col("annual_payroll") / F.col("num_employees")).otherwise(0).alias("payroll_per_employee"),
    # Contractor ratio
    F.when(
        (F.col("num_employees") + F.col("num_contractors")) > 0,
        F.col("num_contractors") / (F.col("num_employees") + F.col("num_contractors"))
    ).otherwise(0).alias("contractor_ratio"),
)

print(f"Org features: {org_features.count()} rows, {len(org_features.columns)} cols")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Location Risk Features (aggregated per org)

# COMMAND ----------

location_features = locations.groupBy("org_id").agg(
    F.count("*").alias("num_locations"),
    F.avg("square_footage").alias("avg_square_footage"),
    F.sum("square_footage").alias("total_square_footage"),
    F.avg("building_age_years").alias("avg_building_age_years"),
    F.max("building_age_years").alias("max_building_age_years"),
    F.avg("num_stories").alias("avg_num_stories"),
    F.avg(F.col("has_sprinkler_system").cast("int")).alias("pct_locations_sprinkler"),
    F.avg(F.col("has_fire_alarm").cast("int")).alias("pct_locations_fire_alarm"),
    F.avg(F.col("has_security_system").cast("int")).alias("pct_locations_security"),
    F.avg(F.col("has_cctv").cast("int")).alias("pct_locations_cctv"),
    F.avg("distance_to_fire_station_miles").alias("avg_distance_fire_station"),
    F.avg("fire_protection_class").alias("avg_fire_protection_class"),
    # Worst-case fire protection (higher = worse)
    F.max("fire_protection_class").alias("worst_fire_protection_class"),
    # Construction risk: count of frame buildings (most fire-prone)
    F.sum(F.when(F.col("construction_type") == "frame", 1).otherwise(0)).alias("num_frame_buildings"),
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Employee Features (aggregated per org)

# COMMAND ----------

employee_features = employees.groupBy("org_id").agg(
    F.sum("num_employees").alias("total_classified_employees"),
    F.sum("annual_payroll").alias("total_classified_payroll"),
    # High-risk job class ratios
    F.sum(F.when(F.col("job_classification").isin("skilled_labor", "unskilled_labor", "driver"), F.col("num_employees")).otherwise(0)).alias("num_high_risk_employees"),
    F.sum(F.when(F.col("job_classification") == "driver", F.col("num_employees")).otherwise(0)).alias("num_drivers"),
    # Remote worker ratio
    F.avg(F.col("is_remote").cast("int")).alias("pct_remote_classifications"),
    # Number of distinct job classes (complexity indicator)
    F.countDistinct("job_classification").alias("num_job_classifications"),
)

# Derive high-risk employee ratio
employee_features = employee_features.withColumn(
    "high_risk_employee_ratio",
    F.when(F.col("total_classified_employees") > 0,
           F.col("num_high_risk_employees") / F.col("total_classified_employees")).otherwise(0)
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Property Asset Features (aggregated per org)

# COMMAND ----------

property_features = property_assets.groupBy("org_id").agg(
    F.count("*").alias("num_assets"),
    F.sum("current_value").alias("total_property_value"),
    F.sum("replacement_cost").alias("total_replacement_cost"),
    F.avg("replacement_cost").alias("avg_replacement_cost"),
    # Condition risk: count of poor/fair condition assets
    F.sum(F.when(F.col("condition").isin("poor", "fair"), 1).otherwise(0)).alias("num_poor_fair_assets"),
    # Owned vs leased ratio
    F.avg(F.col("is_owned").cast("int")).alias("pct_owned_assets"),
    # Replacement gap (underinsurance risk)
    F.sum(F.col("replacement_cost") - F.col("current_value")).alias("total_replacement_gap"),
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Vehicle / Fleet Features (aggregated per org)

# COMMAND ----------

vehicle_features = vehicles.groupBy("org_id").agg(
    F.count("*").alias("fleet_size"),
    F.avg("annual_mileage").alias("avg_annual_mileage"),
    F.sum("annual_mileage").alias("total_annual_mileage"),
    F.avg("driver_age").alias("avg_driver_age"),
    F.min("driver_age").alias("min_driver_age"),
    F.avg("driver_years_experience").alias("avg_driver_experience"),
    F.avg("driver_violations_3yr").alias("avg_driver_violations"),
    F.sum("driver_violations_3yr").alias("total_driver_violations"),
    F.avg("current_value").alias("avg_vehicle_value"),
    F.sum("current_value").alias("total_vehicle_value"),
    # High-risk vehicle types
    F.sum(F.when(F.col("vehicle_type").isin("semi", "box_truck"), 1).otherwise(0)).alias("num_heavy_vehicles"),
    # Young driver count (under 25)
    F.sum(F.when(F.col("driver_age") < 25, 1).otherwise(0)).alias("num_young_drivers"),
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Cyber Risk Features

# COMMAND ----------

cyber_features = cyber.select(
    "org_id",
    "num_endpoints",
    "num_servers",
    F.col("has_cloud_infrastructure").cast("int").alias("has_cloud"),
    F.col("stores_pii").cast("int").alias("stores_pii"),
    F.col("stores_phi").cast("int").alias("stores_phi"),
    F.col("stores_payment_data").cast("int").alias("stores_payment_data"),
    "num_records_stored",
    # Cyber security score (sum of controls, 0-8)
    (
        F.col("has_endpoint_protection").cast("int") +
        F.col("has_firewall").cast("int") +
        F.col("has_mfa").cast("int") +
        F.col("has_encryption_at_rest").cast("int") +
        F.col("has_encryption_in_transit").cast("int") +
        F.col("has_backup_plan").cast("int") +
        F.col("has_incident_response_plan").cast("int") +
        F.col("has_security_training").cast("int")
    ).alias("cyber_security_score"),
    # Sensitive data exposure score (sum of sensitive data types)
    (
        F.col("stores_pii").cast("int") +
        F.col("stores_phi").cast("int") +
        F.col("stores_payment_data").cast("int")
    ).alias("sensitive_data_score"),
    F.col("has_cyber_insurance_history").cast("int").alias("has_cyber_insurance_history"),
    # Days since last security audit (staleness indicator)
    F.datediff(F.current_date(), F.col("last_security_audit_date")).alias("days_since_security_audit"),
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Financial Features (latest year + trends)

# COMMAND ----------

from pyspark.sql.window import Window

# Get latest fiscal year per org
fin_window = Window.partitionBy("org_id").orderBy(F.col("fiscal_year").desc())
latest_financials = financials.withColumn("rn", F.row_number().over(fin_window)).filter("rn = 1").drop("rn")

financial_features = latest_financials.select(
    "org_id",
    F.col("total_revenue").alias("latest_revenue"),
    F.col("total_payroll").alias("latest_payroll"),
    F.col("net_income").alias("latest_net_income"),
    "num_key_customers",
    "largest_customer_revenue_pct",
    # Revenue concentration risk
    F.when(F.col("largest_customer_revenue_pct") > 50, 1).otherwise(0).alias("high_customer_concentration"),
    # International exposure
    F.when(
        (F.col("total_revenue") > 0) & (F.col("revenue_international").isNotNull()),
        F.col("revenue_international") / F.col("total_revenue")
    ).otherwise(0).alias("international_revenue_pct"),
    # Profitability
    F.when(F.col("total_revenue") > 0, F.col("net_income") / F.col("total_revenue")).otherwise(0).alias("profit_margin"),
)

# Revenue trend (YoY growth from multi-year data)
fin_trend = financials.groupBy("org_id").agg(
    F.max("fiscal_year").alias("max_year"),
    F.min("fiscal_year").alias("min_year"),
    F.count("*").alias("num_fiscal_years"),
)
# Join with first and last year revenue for trend
fin_first = financials.alias("ff")
fin_last = financials.alias("fl")
revenue_trend = (
    fin_trend.alias("t")
    .join(fin_first, (F.col("t.org_id") == F.col("ff.org_id")) & (F.col("t.min_year") == F.col("ff.fiscal_year")), "left")
    .join(fin_last, (F.col("t.org_id") == F.col("fl.org_id")) & (F.col("t.max_year") == F.col("fl.fiscal_year")), "left")
    .select(
        F.col("t.org_id"),
        F.when(
            (F.col("ff.total_revenue") > 0) & (F.col("t.num_fiscal_years") > 1),
            (F.col("fl.total_revenue") - F.col("ff.total_revenue")) / F.col("ff.total_revenue")
        ).otherwise(0).alias("revenue_growth_rate"),
    )
)

financial_features = financial_features.join(revenue_trend, "org_id", "left")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Policy & Coverage Features (aggregated per org)

# COMMAND ----------

policy_features = policies.groupBy("org_id").agg(
    F.count("*").alias("num_policies_total"),
    F.sum(F.when(F.col("is_current") == True, 1).otherwise(0)).alias("num_current_policies"),
    F.sum(F.when(F.col("is_current") == True, F.col("annual_premium")).otherwise(0)).alias("total_current_premium"),
    F.avg("coverage_limit").alias("avg_coverage_limit"),
    F.avg("deductible").alias("avg_deductible"),
    # Number of distinct coverage types
    F.countDistinct("policy_type").alias("num_coverage_types"),
    # Has umbrella (indicates risk awareness)
    F.max(F.when(F.col("policy_type") == "umbrella", 1).otherwise(0)).alias("has_umbrella_policy"),
)

# Coverage requests complexity
coverage_features = coverage_requests.groupBy("org_id").agg(
    F.count("*").alias("num_coverage_requests"),
    F.sum(F.when(F.col("priority") == "must_have", 1).otherwise(0)).alias("num_must_have_coverages"),
    F.avg("requested_limit").alias("avg_requested_limit"),
    F.avg("requested_deductible").alias("avg_requested_deductible"),
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Contract Features (aggregated per org)

# COMMAND ----------

contract_features = contracts.groupBy("org_id").agg(
    F.count("*").alias("num_contracts"),
    F.sum("annual_value").alias("total_contract_value"),
    F.sum(F.col("requires_additional_insured").cast("int")).alias("num_additional_insured_required"),
    F.sum(F.col("requires_waiver_of_subrogation").cast("int")).alias("num_waiver_subrogation_required"),
    F.max("minimum_liability_limit").alias("max_contractual_liability_limit"),
    # Government contract exposure
    F.sum(F.when(F.col("contract_type") == "government", 1).otherwise(0)).alias("num_government_contracts"),
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Claims / Loss History Features (TARGET LABELS + historical features)
# MAGIC
# MAGIC We compute both:
# MAGIC 1. **Historical claim features** (frequency, severity, patterns)
# MAGIC 2. **Target labels** (has_claim, total_loss_amount, loss_ratio, risk_score)

# COMMAND ----------

# --- Historical claim features per org ---
claim_features = claims.groupBy("org_id").agg(
    F.count("*").alias("num_claims"),
    F.sum("amount_paid").alias("total_claims_paid"),
    F.sum("amount_reserved").alias("total_claims_reserved"),
    F.avg("amount_paid").alias("avg_claim_severity"),
    F.max("amount_paid").alias("max_claim_severity"),
    # Claim type diversity
    F.countDistinct("claim_type").alias("num_claim_types"),
    # Open claims (ongoing liability)
    F.sum(F.when(F.col("status") == "open", 1).otherwise(0)).alias("num_open_claims"),
    # Bodily injury claims (high severity type)
    F.sum(F.when(F.col("claim_type") == "bodily_injury", 1).otherwise(0)).alias("num_bodily_injury_claims"),
    # Workers comp claims
    F.sum(F.when(F.col("claim_type") == "workers_comp_injury", 1).otherwise(0)).alias("num_workers_comp_claims"),
    # Property damage claims
    F.sum(F.when(F.col("claim_type") == "property_damage", 1).otherwise(0)).alias("num_property_damage_claims"),
    # Cyber breach claims
    F.sum(F.when(F.col("claim_type") == "cyber_breach", 1).otherwise(0)).alias("num_cyber_claims"),
    # Recency of last claim
    F.datediff(F.current_date(), F.max("claim_date")).alias("days_since_last_claim"),
    # Claims trend: claims in last 2 years vs older
    F.sum(F.when(F.datediff(F.current_date(), F.col("claim_date")) <= 730, 1).otherwise(0)).alias("num_claims_last_2yr"),
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Join All Features into Master Feature Table

# COMMAND ----------

# Start with org base
master_features = org_features

# Left join all feature groups
for feat_df in [location_features, employee_features, property_features,
                vehicle_features, cyber_features, financial_features,
                policy_features, coverage_features, contract_features, claim_features]:
    master_features = master_features.join(feat_df, "org_id", "left")

# Fill nulls for orgs with no records in some tables
master_features = master_features.fillna(0)

print(f"Master feature table: {master_features.count()} rows, {len(master_features.columns)} columns")
master_features.printSchema()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Compute Target Labels
# MAGIC
# MAGIC For ML training we create multiple targets:
# MAGIC - `has_claim`: Binary - did the org ever have a claim (classification)
# MAGIC - `claim_frequency`: Number of claims per $1M revenue (regression)
# MAGIC - `loss_ratio`: Total claims paid / total premium (regression)
# MAGIC - `risk_score`: Composite 0-100 risk score (regression)

# COMMAND ----------

master_features = master_features.withColumn(
    "has_claim", F.when(F.col("num_claims") > 0, 1).otherwise(0)
)

master_features = master_features.withColumn(
    "claim_frequency_per_1m",
    F.when(F.col("annual_revenue") > 0,
           (F.col("num_claims") / F.col("annual_revenue")) * 1_000_000).otherwise(0)
)

master_features = master_features.withColumn(
    "loss_ratio",
    F.when(F.col("total_current_premium") > 0,
           F.col("total_claims_paid") / F.col("total_current_premium")).otherwise(0)
)

# Composite risk score (0-100): weighted combination of risk factors
# Higher = worse risk
master_features = master_features.withColumn(
    "risk_score",
    F.least(F.lit(100), F.greatest(F.lit(0),
        # Claim history (40% weight)
        F.least(F.col("num_claims") * 5, F.lit(40)).cast("double") +
        # Loss severity (20% weight)
        F.when(F.col("avg_claim_severity") > 100000, 20)
         .when(F.col("avg_claim_severity") > 50000, 15)
         .when(F.col("avg_claim_severity") > 10000, 10)
         .when(F.col("avg_claim_severity") > 0, 5)
         .otherwise(0).cast("double") +
        # Safety controls deficit (15% weight)
        ((3 - F.col("has_safety_procedures") - F.col("has_training_program") - F.col("has_cyber_controls")) * 5).cast("double") +
        # Property risk (10% weight)
        F.when(F.col("avg_building_age_years") > 40, 10)
         .when(F.col("avg_building_age_years") > 20, 5)
         .otherwise(0).cast("double") +
        # Fleet risk (10% weight)
        F.least(F.col("total_driver_violations") * 2, F.lit(10)).cast("double") +
        # Cyber exposure (5% weight)
        (F.col("sensitive_data_score") * F.lit(1.67)).cast("double")
    ))
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Compute Insurance Line-Specific Premiums (Pricing Targets)
# MAGIC
# MAGIC Compute expected premium per insurance line using actuarial-style formulas:
# MAGIC - **GL**: Based on revenue, risk category, claim history
# MAGIC - **Property**: Based on property values, building risk
# MAGIC - **Workers Comp**: Based on payroll, job classifications
# MAGIC - **Commercial Auto**: Based on fleet size, driver risk
# MAGIC - **Cyber**: Based on endpoints, data sensitivity, controls

# COMMAND ----------

# General Liability indicated premium
master_features = master_features.withColumn(
    "gl_indicated_premium",
    F.col("annual_revenue") * 0.005 *  # base rate 0.5% of revenue
    (1 + F.col("risk_score") / 100) *   # risk adjustment
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

# Property indicated premium
master_features = master_features.withColumn(
    "property_indicated_premium",
    F.col("total_replacement_cost") * 0.003 *  # base rate 0.3% of TIV
    (1 + (10 - F.least(F.col("pct_locations_sprinkler") * 10, F.lit(10))) * 0.02) *  # sprinkler discount
    (1 + F.col("avg_fire_protection_class") * 0.01)  # fire class adjustment
)

# Workers comp indicated premium
master_features = master_features.withColumn(
    "wc_indicated_premium",
    F.col("annual_payroll") / 100 *  # rate per $100 payroll
    F.lit(1.5) *  # base modifier
    (1 + F.col("high_risk_employee_ratio") * 2) *  # high-risk class surcharge
    (1 + F.least(F.col("num_workers_comp_claims") * 0.1, F.lit(0.5)))  # experience mod
)

# Commercial auto indicated premium
master_features = master_features.withColumn(
    "auto_indicated_premium",
    F.col("fleet_size") * 2000 *  # base $2000/vehicle
    (1 + F.col("avg_driver_violations") * 0.15) *  # violation surcharge
    (1 + F.col("num_young_drivers") * 0.05)  # young driver surcharge
)

# Cyber indicated premium
master_features = master_features.withColumn(
    "cyber_indicated_premium",
    (F.col("num_endpoints") * 50 + F.col("num_records_stored") * 0.001) *  # base exposure
    F.greatest(F.lit(0.5), (9 - F.col("cyber_security_score")) / 8) *  # controls discount
    (1 + F.col("sensitive_data_score") * 0.2)  # sensitive data surcharge
)

# Total indicated premium across all lines
master_features = master_features.withColumn(
    "total_indicated_premium",
    F.col("gl_indicated_premium") +
    F.col("property_indicated_premium") +
    F.col("wc_indicated_premium") +
    F.col("auto_indicated_premium") +
    F.col("cyber_indicated_premium")
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Add Train/Test Split

# COMMAND ----------

# Deterministic split based on org_id hash
master_features = master_features.withColumn(
    "split",
    F.when(F.abs(F.hash("org_id")) % 100 < 80, "train").otherwise("test")
)

split_counts = master_features.groupBy("split").count().collect()
for row in split_counts:
    print(f"  {row['split']}: {row['count']} rows")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Register Feature Table in Unity Catalog

# COMMAND ----------

feature_table_name = f"{CATALOG}.{SCHEMA}.risk_features"

# Write feature table
spark.sql(f"DROP TABLE IF EXISTS {feature_table_name}")

master_features.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(feature_table_name)

# Enable CDF for online serving later
spark.sql(f"ALTER TABLE {feature_table_name} SET TBLPROPERTIES (delta.enableChangeDataFeed = true)")

# Make org_id NOT NULL then add primary key constraint
spark.sql(f"ALTER TABLE {feature_table_name} ALTER COLUMN org_id SET NOT NULL")
spark.sql(f"ALTER TABLE {feature_table_name} ADD CONSTRAINT risk_features_pk PRIMARY KEY (org_id)")

print(f"Feature table saved: {feature_table_name}")
print(f"Total features: {len(master_features.columns)}")
print(f"Total rows: {master_features.count()}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Log Feature Table as MLflow Dataset

# COMMAND ----------

mlflow.set_experiment(f"/Users/david.vincent@databricks.com/email_to_quote_risk_scoring")

src_dataset = mlflow.data.load_delta(table_name=feature_table_name)
with mlflow.start_run(run_name="feature_engineering") as run:
    mlflow.log_input(src_dataset, context="feature-engineering")
    mlflow.log_param("num_features", len(master_features.columns))
    mlflow.log_param("num_orgs", master_features.count())
    mlflow.log_param("catalog", CATALOG)
    mlflow.log_param("schema", SCHEMA)
    mlflow.set_tag("stage", "feature_engineering")

print("Feature engineering complete!")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Preview Feature Table

# COMMAND ----------

display(spark.table(feature_table_name).limit(20))
