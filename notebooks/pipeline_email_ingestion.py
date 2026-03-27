# Databricks notebook source
# MAGIC %pip install fpdf2

# COMMAND ----------

# MAGIC %md
# MAGIC # Email-to-Quote Ingestion Pipeline (Lakeflow / Spark Declarative Pipeline)
# MAGIC
# MAGIC This pipeline runs in **continuous mode** and processes incoming quote request emails
# MAGIC through 4 stages from raw ingestion to ML-ready features.
# MAGIC
# MAGIC | Step | Table | Description |
# MAGIC |------|-------|-------------|
# MAGIC | 1 | `pipe_email_received` | AutoLoader captures emails with auto-generated UUID |
# MAGIC | 2 | `pipe_email_parsed` | LLM parses email content to extract structured fields |
# MAGIC | 3 | `pipe_email_enriched` | Joins parsed data with reference tables for enrichment |
# MAGIC | 4 | `pipe_quote_features` | Computes ML features for risk scoring and premium calculation |
# MAGIC | 5 | `pipe_quote_risk_scoring` | Real-time ML serving endpoint scores risk with heuristic fallback |
# MAGIC | 6 | `pipe_quote_review` | Decision engine: auto-approved / pending-review / auto-declined + LLM summary |
# MAGIC | 7 | `pipe_quote_creation` | Coverage-level premium calculation from auto-approved quotes |
# MAGIC | 8 | `pipe_pdf_created` | PDF quote document generation and volume storage |
# MAGIC | 9 | `pipe_completed` | Terminal state: unifies all completed quote paths (auto-declined and PDF-issued) |
# MAGIC | 10 | `pipe_response_email` | LLM-generated response email saved to outgoing_email volume |

# COMMAND ----------

import dlt
import pyspark.sql.functions as F
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, IntegerType, ArrayType

# COMMAND ----------

CATALOG = "dvin100_email_to_quote"
SCHEMA = "email_to_quote"
VOLUME_PATH = f"/Volumes/{CATALOG}/{SCHEMA}/incoming_email"
LLM_ENDPOINT = "databricks-claude-sonnet-4-5"

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 1 - Email Received (Streaming Table)
# MAGIC
# MAGIC AutoLoader runs in **continuous mode** and captures each email as soon as it lands
# MAGIC in the `incoming_email` volume. A UUID is auto-generated for each email.

# COMMAND ----------

@dlt.table(
    name="pipe_email_received",
    comment="Raw incoming quote request emails captured by AutoLoader in continuous mode. Each email gets a unique UUID.",
    table_properties={
        "quality": "bronze",
        "pipelines.autoOptimize.managed": "true",
    },
)
def pipe_email_received():
    return (
        spark.readStream
        .format("cloudFiles")
        .option("cloudFiles.format", "text")
        .option("cloudFiles.inferColumnTypes", "false")
        .option("wholeText", "true")
        .option("cloudFiles.schemaLocation", f"{VOLUME_PATH}/_schema")
        .option("cloudFiles.includeExistingFiles", "true")
        .option("pathGlobFilter", "*.eml")
        .load(VOLUME_PATH)
        .withColumn("email_id", F.expr("uuid()"))
        .withColumn("file_name", F.col("_metadata.file_name"))
        .withColumn("file_path", F.col("_metadata.file_path"))
        .withColumn("file_size", F.col("_metadata.file_size"))
        .withColumn("file_modification_time", F.col("_metadata.file_modification_time"))
        .withColumn("ingestion_timestamp", F.current_timestamp())
        .select(
            "email_id",
            F.col("value").alias("raw_content"),
            "file_name",
            "file_path",
            "file_size",
            "file_modification_time",
            "ingestion_timestamp",
        )
    )

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 2 - Email LLM Parsing (Streaming Table)
# MAGIC
# MAGIC Uses a Foundation Model API endpoint to parse each email and extract structured
# MAGIC insurance submission fields: business info, coverage requests, loss history, etc.

# COMMAND ----------

LLM_PROMPT = """You are an insurance underwriting assistant. Parse the following commercial insurance quote request email and extract structured data as a JSON object.

Return ONLY a valid JSON object with these fields (use null for missing values):
{
  "sender_name": "string - contact person name",
  "sender_email": "string - sender email address",
  "sender_phone": "string - phone number",
  "sender_title": "string - job title",
  "business_name": "string - legal business name",
  "business_dba": "string - DBA / trading name",
  "naics_code": "string - NAICS code if mentioned",
  "industry_description": "string - brief industry description",
  "risk_category": "string - one of: office, retail, construction, manufacturing, healthcare, technology, food_service, transportation, professional_services, hospitality",
  "date_established": "string - year established",
  "num_locations": 0,
  "location_states": "string - comma-separated state abbreviations",
  "annual_revenue": 0.0,
  "annual_payroll": 0.0,
  "num_employees": 0,
  "coverages_requested": "string - comma-separated list of coverage types requested",
  "gl_limit_requested": 0.0,
  "property_tiv": 0.0,
  "auto_fleet_size": 0,
  "cyber_limit_requested": 0.0,
  "umbrella_limit_requested": 0.0,
  "num_claims_5yr": 0,
  "total_claims_amount": 0.0,
  "worst_claim_description": "string - brief description of largest/worst claim",
  "claim_types": "string - comma-separated claim types mentioned",
  "current_carrier": "string - current insurance carrier name",
  "current_premium": 0.0,
  "renewal_date": "string - policy renewal date if mentioned",
  "has_safety_procedures": true,
  "has_employee_training": true,
  "special_requirements": "string - any special endorsements, contract requirements, or notes",
  "urgency": "string - one of: standard, urgent, rush"
}

EMAIL:
"""

# COMMAND ----------

@dlt.table(
    name="pipe_email_parsed",
    comment="LLM-parsed email content with structured insurance submission fields extracted from raw email text.",
    table_properties={
        "quality": "silver",
        "pipelines.autoOptimize.managed": "true",
    },
)
def pipe_email_parsed():
    received = dlt.read_stream("pipe_email_received")

    # Call LLM to parse email content using ai_query
    parsed = received.withColumn(
        "llm_response",
        F.expr(f"""
            ai_query(
                '{LLM_ENDPOINT}',
                CONCAT('{LLM_PROMPT.replace(chr(39), chr(39)+chr(39))}', raw_content),
                'STRING'
            )
        """)
    ).withColumn(
        "parse_timestamp", F.current_timestamp()
    )

    # Extract JSON fields from LLM response
    parsed = parsed.withColumn(
        "parsed_json",
        F.from_json(
            # Strip markdown code fences if present
            F.regexp_replace(
                F.regexp_replace(F.col("llm_response"), "```json\\s*", ""),
                "```\\s*$", ""
            ),
            """struct<
                sender_name:string, sender_email:string, sender_phone:string, sender_title:string,
                business_name:string, business_dba:string, naics_code:string, industry_description:string,
                risk_category:string, date_established:string, num_locations:int, location_states:string,
                annual_revenue:double, annual_payroll:double, num_employees:int,
                coverages_requested:string, gl_limit_requested:double, property_tiv:double,
                auto_fleet_size:int, cyber_limit_requested:double, umbrella_limit_requested:double,
                num_claims_5yr:int, total_claims_amount:double, worst_claim_description:string,
                claim_types:string, current_carrier:string, current_premium:double,
                renewal_date:string, has_safety_procedures:boolean, has_employee_training:boolean,
                special_requirements:string, urgency:string
            >"""
        )
    )

    return parsed.select(
        "email_id",
        "raw_content",
        "file_name",
        "ingestion_timestamp",
        "parse_timestamp",
        "llm_response",
        F.col("parsed_json.sender_name").alias("sender_name"),
        F.col("parsed_json.sender_email").alias("sender_email"),
        F.col("parsed_json.sender_phone").alias("sender_phone"),
        F.col("parsed_json.sender_title").alias("sender_title"),
        F.col("parsed_json.business_name").alias("business_name"),
        F.col("parsed_json.business_dba").alias("business_dba"),
        F.col("parsed_json.naics_code").alias("naics_code"),
        F.col("parsed_json.industry_description").alias("industry_description"),
        F.col("parsed_json.risk_category").alias("risk_category"),
        F.col("parsed_json.date_established").alias("date_established"),
        F.col("parsed_json.num_locations").alias("num_locations"),
        F.col("parsed_json.location_states").alias("location_states"),
        F.col("parsed_json.annual_revenue").alias("annual_revenue"),
        F.col("parsed_json.annual_payroll").alias("annual_payroll"),
        F.col("parsed_json.num_employees").alias("num_employees"),
        F.col("parsed_json.coverages_requested").alias("coverages_requested"),
        F.col("parsed_json.gl_limit_requested").alias("gl_limit_requested"),
        F.col("parsed_json.property_tiv").alias("property_tiv"),
        F.col("parsed_json.auto_fleet_size").alias("auto_fleet_size"),
        F.col("parsed_json.cyber_limit_requested").alias("cyber_limit_requested"),
        F.col("parsed_json.umbrella_limit_requested").alias("umbrella_limit_requested"),
        F.col("parsed_json.num_claims_5yr").alias("num_claims_5yr"),
        F.col("parsed_json.total_claims_amount").alias("total_claims_amount"),
        F.col("parsed_json.worst_claim_description").alias("worst_claim_description"),
        F.col("parsed_json.claim_types").alias("claim_types"),
        F.col("parsed_json.current_carrier").alias("current_carrier"),
        F.col("parsed_json.current_premium").alias("current_premium"),
        F.col("parsed_json.renewal_date").alias("renewal_date"),
        F.col("parsed_json.has_safety_procedures").alias("has_safety_procedures"),
        F.col("parsed_json.has_employee_training").alias("has_employee_training"),
        F.col("parsed_json.special_requirements").alias("special_requirements"),
        F.col("parsed_json.urgency").alias("urgency"),
    )

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 3 - Email Enrichment (Streaming Table)
# MAGIC
# MAGIC Joins parsed email data with reference tables to enrich with:
# MAGIC - Organization data (if existing customer matched by name/email)
# MAGIC - Industry risk benchmarks from existing organizations
# MAGIC - Claims history benchmarks for similar businesses
# MAGIC - Policy/coverage benchmarks

# COMMAND ----------

@dlt.table(
    name="pipe_email_enriched",
    comment="Parsed email data enriched with reference data from organizations, claims, policies, and industry benchmarks.",
    table_properties={
        "quality": "silver",
        "pipelines.autoOptimize.managed": "true",
    },
)
def pipe_email_enriched():
    parsed = dlt.read_stream("pipe_email_parsed")

    # --- Load reference tables ---
    orgs = spark.table(f"{CATALOG}.{SCHEMA}.organizations")
    claims = spark.table(f"{CATALOG}.{SCHEMA}.claims")
    policies = spark.table(f"{CATALOG}.{SCHEMA}.policies")
    locations = spark.table(f"{CATALOG}.{SCHEMA}.locations")
    financials = spark.table(f"{CATALOG}.{SCHEMA}.financials")

    # --- Industry benchmarks: avg claims, avg premium by risk_category ---
    org_claims = orgs.join(claims, "org_id", "left")
    industry_benchmarks = (
        org_claims.groupBy("risk_category")
        .agg(
            F.count(F.col("claim_id")).alias("industry_avg_claims"),
            F.avg("amount_paid").alias("industry_avg_claim_severity"),
            F.countDistinct("org_id").alias("industry_org_count"),
        )
    )
    # Normalize to per-org averages
    industry_benchmarks = industry_benchmarks.withColumn(
        "industry_avg_claims_per_org",
        F.when(F.col("industry_org_count") > 0,
               F.col("industry_avg_claims") / F.col("industry_org_count")).otherwise(0)
    )

    # Premium benchmarks by risk_category
    org_policies = orgs.join(policies.filter("is_current = true"), "org_id", "left")
    premium_benchmarks = (
        org_policies.groupBy("risk_category")
        .agg(
            F.avg("annual_premium").alias("industry_avg_premium"),
            F.avg("coverage_limit").alias("industry_avg_coverage_limit"),
            F.avg("deductible").alias("industry_avg_deductible"),
        )
    )

    # Revenue benchmarks by risk_category
    from pyspark.sql.window import Window
    fin_window = Window.partitionBy("org_id").orderBy(F.col("fiscal_year").desc())
    latest_fin = financials.withColumn("rn", F.row_number().over(fin_window)).filter("rn = 1").drop("rn")
    revenue_benchmarks = (
        orgs.join(latest_fin, "org_id", "left")
        .groupBy("risk_category")
        .agg(
            F.avg("total_revenue").alias("industry_avg_revenue"),
            F.avg("total_payroll").alias("industry_avg_payroll"),
            F.avg("num_employees").alias("industry_avg_employees"),
        )
    )

    # Location risk benchmarks
    location_benchmarks = (
        orgs.join(locations, "org_id", "left")
        .groupBy("risk_category")
        .agg(
            F.avg("building_age_years").alias("industry_avg_building_age"),
            F.avg("fire_protection_class").alias("industry_avg_fire_protection_class"),
            F.avg(F.col("has_sprinkler_system").cast("int")).alias("industry_pct_sprinkler"),
        )
    )

    # --- Try to match existing customer by email or business name ---
    existing_match = orgs.select(
        F.col("org_id").alias("matched_org_id"),
        F.col("legal_name").alias("matched_legal_name"),
        F.col("primary_email").alias("matched_email"),
        F.col("annual_revenue").alias("existing_revenue"),
        F.col("num_employees").alias("existing_employees"),
        F.col("risk_category").alias("existing_risk_category"),
    )

    # --- Enrich parsed data ---
    enriched = (
        parsed
        # Match existing customer by email
        .join(
            existing_match,
            F.lower(parsed.sender_email) == F.lower(existing_match.matched_email),
            "left"
        )
        # Add industry benchmarks
        .join(industry_benchmarks, parsed.risk_category == industry_benchmarks.risk_category, "left")
        .drop(industry_benchmarks.risk_category)
        .join(premium_benchmarks, parsed.risk_category == premium_benchmarks.risk_category, "left")
        .drop(premium_benchmarks.risk_category)
        .join(revenue_benchmarks, parsed.risk_category == revenue_benchmarks.risk_category, "left")
        .drop(revenue_benchmarks.risk_category)
        .join(location_benchmarks, parsed.risk_category == location_benchmarks.risk_category, "left")
        .drop(location_benchmarks.risk_category)
        # Derived enrichment fields
        .withColumn("is_existing_customer", F.when(F.col("matched_org_id").isNotNull(), True).otherwise(False))
        .withColumn("enrichment_timestamp", F.current_timestamp())
        # Revenue comparison vs industry
        .withColumn(
            "revenue_vs_industry_pct",
            F.when(
                (F.col("industry_avg_revenue").isNotNull()) & (F.col("industry_avg_revenue") > 0),
                F.col("annual_revenue") / F.col("industry_avg_revenue") * 100
            ).otherwise(F.lit(None))
        )
        # Premium comparison vs industry
        .withColumn(
            "premium_vs_industry_pct",
            F.when(
                (F.col("industry_avg_premium").isNotNull()) & (F.col("industry_avg_premium") > 0),
                F.col("current_premium") / F.col("industry_avg_premium") * 100
            ).otherwise(F.lit(None))
        )
        # Claims comparison vs industry
        .withColumn(
            "claims_vs_industry",
            F.when(
                F.col("industry_avg_claims_per_org").isNotNull(),
                F.col("num_claims_5yr") - F.col("industry_avg_claims_per_org")
            ).otherwise(F.lit(None))
        )
    )

    return enriched

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 4 - Featurization (Streaming Table)
# MAGIC
# MAGIC Computes ML-ready features from the enriched data for the risk scoring model
# MAGIC and premium calculation. These features align with the training feature schema.

# COMMAND ----------

@dlt.table(
    name="pipe_quote_features",
    comment="ML-ready features computed from enriched email data for risk scoring and premium calculation.",
    table_properties={
        "quality": "gold",
        "pipelines.autoOptimize.managed": "true",
    },
)
def pipe_quote_features():
    enriched = dlt.read_stream("pipe_email_enriched")

    features = enriched.select(
        # --- Identifiers ---
        "email_id",
        "business_name",
        "risk_category",

        # --- Business descriptors ---
        F.coalesce(F.col("annual_revenue"), F.lit(0)).alias("annual_revenue"),
        F.coalesce(F.col("annual_payroll"), F.lit(0)).alias("annual_payroll"),
        F.coalesce(F.col("num_employees"), F.lit(0)).alias("num_employees"),
        F.coalesce(F.col("num_locations"), F.lit(0)).alias("num_locations"),
        F.when(F.col("date_established").isNotNull(),
               F.year(F.current_date()) - F.col("date_established").cast("int")).otherwise(0).alias("business_age_years"),

        # Payroll per employee
        F.when(F.col("num_employees") > 0,
               F.col("annual_payroll") / F.col("num_employees")).otherwise(0).alias("payroll_per_employee"),

        # --- Coverage complexity ---
        F.size(F.split(F.coalesce(F.col("coverages_requested"), F.lit("")), ",")).alias("num_coverages_requested"),
        F.coalesce(F.col("gl_limit_requested"), F.lit(0)).alias("gl_limit_requested"),
        F.coalesce(F.col("property_tiv"), F.lit(0)).alias("property_tiv"),
        F.coalesce(F.col("auto_fleet_size"), F.lit(0)).alias("auto_fleet_size"),
        F.coalesce(F.col("cyber_limit_requested"), F.lit(0)).alias("cyber_limit_requested"),
        F.coalesce(F.col("umbrella_limit_requested"), F.lit(0)).alias("umbrella_limit_requested"),
        F.when(F.col("umbrella_limit_requested") > 0, 1).otherwise(0).alias("has_umbrella_request"),

        # --- Claims / loss history ---
        F.coalesce(F.col("num_claims_5yr"), F.lit(0)).alias("num_claims_5yr"),
        F.coalesce(F.col("total_claims_amount"), F.lit(0)).alias("total_claims_amount"),
        F.when(F.col("num_claims_5yr") > 0,
               F.col("total_claims_amount") / F.col("num_claims_5yr")).otherwise(0).alias("avg_claim_severity"),
        F.when(F.col("num_claims_5yr") > 3, 1).otherwise(0).alias("high_claim_frequency_flag"),

        # --- Current insurance ---
        F.coalesce(F.col("current_premium"), F.lit(0)).alias("current_premium"),
        F.when(F.col("current_carrier").isNotNull(), 1).otherwise(0).alias("has_current_coverage"),

        # --- Safety controls ---
        F.when(F.col("has_safety_procedures") == True, 1).otherwise(0).alias("has_safety_procedures"),
        F.when(F.col("has_employee_training") == True, 1).otherwise(0).alias("has_employee_training"),
        (F.when(F.col("has_safety_procedures") == True, 1).otherwise(0) +
         F.when(F.col("has_employee_training") == True, 1).otherwise(0)).alias("safety_score"),

        # --- Industry benchmarks (from enrichment) ---
        F.coalesce(F.col("industry_avg_claims_per_org"), F.lit(0)).alias("industry_avg_claims_per_org"),
        F.coalesce(F.col("industry_avg_claim_severity"), F.lit(0)).alias("industry_avg_claim_severity"),
        F.coalesce(F.col("industry_avg_premium"), F.lit(0)).alias("industry_avg_premium"),
        F.coalesce(F.col("industry_avg_revenue"), F.lit(0)).alias("industry_avg_revenue"),
        F.coalesce(F.col("industry_avg_employees"), F.lit(0)).alias("industry_avg_employees"),
        F.coalesce(F.col("industry_avg_building_age"), F.lit(0)).alias("industry_avg_building_age"),
        F.coalesce(F.col("industry_avg_fire_protection_class"), F.lit(0)).alias("industry_avg_fire_protection_class"),

        # --- Relative position vs industry ---
        F.coalesce(F.col("revenue_vs_industry_pct"), F.lit(100)).alias("revenue_vs_industry_pct"),
        F.coalesce(F.col("premium_vs_industry_pct"), F.lit(100)).alias("premium_vs_industry_pct"),
        F.coalesce(F.col("claims_vs_industry"), F.lit(0)).alias("claims_vs_industry"),

        # --- Existing customer flag ---
        F.when(F.col("is_existing_customer") == True, 1).otherwise(0).alias("is_existing_customer"),

        # --- Risk category encoded (for model input) ---
        F.when(F.col("risk_category") == "construction", 1.0)
         .when(F.col("risk_category") == "manufacturing", 0.9)
         .when(F.col("risk_category") == "healthcare", 0.85)
         .when(F.col("risk_category") == "transportation", 0.8)
         .when(F.col("risk_category") == "food_service", 0.75)
         .when(F.col("risk_category") == "hospitality", 0.65)
         .when(F.col("risk_category") == "retail", 0.5)
         .when(F.col("risk_category") == "professional_services", 0.3)
         .when(F.col("risk_category") == "technology", 0.35)
         .when(F.col("risk_category") == "office", 0.2)
         .otherwise(0.5).alias("risk_category_score"),

        # --- Composite quick-score (heuristic, pre-ML) ---
        # Weighted combination for initial triage before ML model runs
        (
            # Claims history weight (40%)
            F.least(F.coalesce(F.col("num_claims_5yr"), F.lit(0)) * 8, F.lit(40)).cast("double") +
            # Loss severity weight (25%)
            F.when(F.coalesce(F.col("total_claims_amount"), F.lit(0)) > 500000, 25)
             .when(F.col("total_claims_amount") > 100000, 18)
             .when(F.col("total_claims_amount") > 50000, 12)
             .when(F.col("total_claims_amount") > 0, 6)
             .otherwise(0).cast("double") +
            # Safety deficit weight (15%)
            ((2 - F.when(F.col("has_safety_procedures") == True, 1).otherwise(0)
                - F.when(F.col("has_employee_training") == True, 1).otherwise(0)) * 7.5).cast("double") +
            # Size / complexity weight (20%)
            F.when(F.coalesce(F.col("annual_revenue"), F.lit(0)) > 20000000, 20)
             .when(F.col("annual_revenue") > 5000000, 12)
             .when(F.col("annual_revenue") > 1000000, 6)
             .otherwise(3).cast("double")
        ).alias("heuristic_risk_score"),

        # --- Metadata ---
        "ingestion_timestamp",
        "parse_timestamp",
        F.col("enrichment_timestamp"),
        F.current_timestamp().alias("feature_timestamp"),
    )

    return features

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## Step 5 – Risk Scoring (Streaming Table)
# MAGIC
# MAGIC Calls the **real-time ML serving endpoint** (`email-to-quote-risk-scorer`) to get
# MAGIC predictions: risk score, risk band, claim prediction, loss ratio, pricing action,
# MAGIC and underwriting recommendation. Falls back to heuristic scoring if the endpoint
# MAGIC is unavailable or the feature schema doesn't match.

# COMMAND ----------

RISK_ENDPOINT = "email-to-quote-risk-scorer"
QUOTE_VOLUME_PATH = f"/Volumes/{CATALOG}/{SCHEMA}/quote_documents"

# Feature columns sent to the risk scoring model endpoint
SCORING_FEATURES = [
    "annual_revenue", "annual_payroll", "num_employees", "num_locations",
    "business_age_years", "payroll_per_employee", "num_coverages_requested",
    "gl_limit_requested", "property_tiv", "auto_fleet_size",
    "cyber_limit_requested", "umbrella_limit_requested", "has_umbrella_request",
    "num_claims_5yr", "total_claims_amount", "avg_claim_severity",
    "high_claim_frequency_flag", "current_premium", "has_current_coverage",
    "has_safety_procedures", "has_employee_training", "safety_score",
    "industry_avg_claims_per_org", "industry_avg_claim_severity",
    "industry_avg_premium", "industry_avg_revenue", "industry_avg_employees",
    "industry_avg_building_age", "industry_avg_fire_protection_class",
    "revenue_vs_industry_pct", "premium_vs_industry_pct", "claims_vs_industry",
    "is_existing_customer", "risk_category_score", "heuristic_risk_score",
]

# Return schema for the model serving endpoint response
RISK_RETURN_SCHEMA = (
    "STRUCT<claim_prediction INT, risk_score DOUBLE, predicted_loss_ratio DOUBLE, "
    "risk_band STRING, pricing_action STRING, underwriting_action STRING>"
)

# COMMAND ----------

_risk_result_schema = StructType([
    StructField("claim_prediction", IntegerType()),
    StructField("risk_score", DoubleType()),
    StructField("predicted_loss_ratio", DoubleType()),
    StructField("risk_band", StringType()),
    StructField("pricing_action", StringType()),
    StructField("underwriting_action", StringType()),
    StructField("scoring_method", StringType()),
])


@F.udf(returnType=_risk_result_schema)
def _call_risk_endpoint(features_json):
    """Call the ML serving endpoint with fallback to heuristic scoring."""
    import json
    import os

    try:
        data = json.loads(features_json)
    except Exception:
        return (0, 50.0, 0.75, "Medium", "standard_rate", "standard_review", "error")

    h = data.get("heuristic_risk_score", 50.0)

    # --- Try real-time serving endpoint ---
    try:
        import requests as _req

        host = os.environ.get("DATABRICKS_HOST", "")
        token = os.environ.get("DATABRICKS_TOKEN", "")

        if host and token:
            endpoint_url = f"{host}/serving-endpoints/{RISK_ENDPOINT}/invocations"
            # Build payload from available features
            payload = {"dataframe_records": [{k: data.get(k, 0) for k in SCORING_FEATURES}]}
            resp = _req.post(
                endpoint_url,
                headers={"Authorization": f"Bearer {token}",
                         "Content-Type": "application/json"},
                json=payload,
                timeout=10,
            )
            if resp.status_code == 200:
                preds = resp.json().get("predictions", [{}])
                if preds:
                    p = preds[0] if isinstance(preds[0], dict) else {}
                    if p:
                        return (
                            int(p.get("claim_prediction", 0)),
                            float(p.get("risk_score", h)),
                            float(p.get("predicted_loss_ratio", h / 100 * 1.5)),
                            str(p.get("risk_band", "Medium")),
                            str(p.get("pricing_action", "standard_rate")),
                            str(p.get("underwriting_action", "standard_review")),
                            "model",
                        )
    except Exception:
        pass

    # --- Heuristic fallback ---
    claim_pred = 1 if h > 50 else 0
    loss_ratio = h / 100 * 1.5
    band = "Low" if h <= 25 else "Medium" if h <= 50 else "High" if h <= 75 else "Very High"
    pricing = ("competitive_rate" if h <= 30 else "standard_rate" if h <= 60
               else "loaded_rate" if h <= 80 else "decline_or_refer")
    uw = ("auto_quote" if h <= 30 else "standard_review" if h <= 60
          else "senior_underwriter_review" if h <= 80 else "decline_or_refer_to_specialty")
    return (claim_pred, float(h), loss_ratio, band, pricing, uw, "heuristic")


# COMMAND ----------

@dlt.table(
    name="pipe_quote_risk_scoring",
    comment="ML risk scoring results from real-time serving endpoint. Falls back to heuristic scoring if endpoint is unavailable.",
    table_properties={
        "quality": "gold",
        "pipelines.autoOptimize.managed": "true",
    },
)
def pipe_quote_risk_scoring():
    features = dlt.read_stream("pipe_quote_features")

    # Serialize feature columns to JSON for the scoring UDF
    feature_struct = F.struct(*[F.col(c) for c in SCORING_FEATURES])
    scored = features.withColumn(
        "risk_result",
        _call_risk_endpoint(F.to_json(feature_struct)),
    )

    return scored.select(
        # --- Identifiers ---
        "email_id", "business_name", "risk_category",
        # --- Carry-forward features for downstream steps ---
        "annual_revenue", "annual_payroll", "num_employees", "num_locations",
        "gl_limit_requested", "property_tiv", "auto_fleet_size",
        "cyber_limit_requested", "umbrella_limit_requested",
        "num_claims_5yr", "total_claims_amount", "current_premium",
        "has_safety_procedures", "has_employee_training",
        "num_coverages_requested", "heuristic_risk_score",
        # --- Risk scoring results ---
        F.col("risk_result.claim_prediction").alias("claim_prediction"),
        F.col("risk_result.risk_score").alias("risk_score"),
        F.col("risk_result.predicted_loss_ratio").alias("predicted_loss_ratio"),
        F.col("risk_result.risk_band").alias("risk_band"),
        F.col("risk_result.pricing_action").alias("pricing_action"),
        F.col("risk_result.underwriting_action").alias("underwriting_action"),
        F.col("risk_result.scoring_method").alias("scoring_method"),
        # --- Timestamps ---
        "ingestion_timestamp",
        F.current_timestamp().alias("scoring_timestamp"),
    )

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 6 – Quote Review (Streaming Table)
# MAGIC
# MAGIC Applies underwriting decision rules based on the risk scoring results:
# MAGIC
# MAGIC | Risk Level | Tag | Next Step |
# MAGIC |------------|-----|-----------|
# MAGIC | Low (score ≤ 30, no claims predicted) | `auto-approved` | → Step 7 (Quote Creation) |
# MAGIC | Medium | `pending-review` | → Waits for underwriter |
# MAGIC | High (score > 80) | `auto-declined` | → Step 9 (Completed) |
# MAGIC
# MAGIC An LLM generates a short summary explaining the decision for each quote.

# COMMAND ----------

@dlt.table(
    name="pipe_quote_review",
    comment="Underwriting decision with LLM-generated summary. Tagged: auto-approved, pending-review, or auto-declined.",
    table_properties={
        "quality": "gold",
        "pipelines.autoOptimize.managed": "true",
    },
)
def pipe_quote_review():
    scored = dlt.read_stream("pipe_quote_risk_scoring")

    # --- Decision logic ---
    reviewed = scored.withColumn(
        "decision_tag",
        F.when(
            (F.col("underwriting_action") == "auto_quote")
            | ((F.col("risk_score") <= 30) & (F.col("claim_prediction") == 0)),
            "auto-approved",
        )
        .when(
            (F.col("underwriting_action") == "decline_or_refer_to_specialty")
            | (F.col("risk_score") > 80),
            "auto-declined",
        )
        .otherwise("pending-review"),
    )

    # --- LLM prompt for review summary ---
    reviewed = reviewed.withColumn(
        "review_prompt",
        F.concat(
            F.lit(
                "You are an insurance underwriting assistant. Write a concise 2-3 sentence "
                "summary explaining why this commercial insurance quote request was "
            ),
            F.col("decision_tag"),
            F.lit(".\n\nBusiness: "), F.col("business_name"),
            F.lit("\nIndustry: "), F.col("risk_category"),
            F.lit("\nRisk Score: "), F.col("risk_score").cast("string"),
            F.lit("/100 ("), F.col("risk_band"), F.lit(")"),
            F.lit("\nPredicted Loss Ratio: "),
            F.round(F.col("predicted_loss_ratio"), 2).cast("string"),
            F.lit("\nAnnual Revenue: $"),
            F.format_number(F.col("annual_revenue"), 0),
            F.lit("\nEmployees: "), F.col("num_employees").cast("string"),
            F.lit("\nClaims (5yr): "), F.col("num_claims_5yr").cast("string"),
            F.lit(" totaling $"),
            F.format_number(F.col("total_claims_amount"), 0),
            F.lit("\nSafety Procedures: "),
            F.when(F.col("has_safety_procedures") == 1, "Yes").otherwise("No"),
            F.lit("\nEmployee Training: "),
            F.when(F.col("has_employee_training") == 1, "Yes").otherwise("No"),
            F.lit(
                "\n\nWrite ONLY the summary paragraph. Be specific about the risk "
                "factors that influenced this decision."
            ),
        ),
    )

    # --- Call LLM for review summary ---
    reviewed = reviewed.withColumn(
        "review_summary",
        F.expr(f"ai_query('{LLM_ENDPOINT}', review_prompt, 'STRING')"),
    )

    return reviewed.select(
        "email_id", "business_name", "risk_category",
        "decision_tag", "review_summary",
        # Scoring results
        "risk_score", "risk_band", "predicted_loss_ratio",
        "pricing_action", "claim_prediction", "scoring_method",
        # Carry-forward features for quote creation
        "annual_revenue", "annual_payroll", "num_employees", "num_locations",
        "gl_limit_requested", "property_tiv", "auto_fleet_size",
        "cyber_limit_requested", "umbrella_limit_requested",
        "num_claims_5yr", "total_claims_amount", "current_premium",
        "has_safety_procedures", "has_employee_training",
        "num_coverages_requested", "heuristic_risk_score",
        # Timestamps
        "ingestion_timestamp", "scoring_timestamp",
        F.current_timestamp().alias("review_timestamp"),
    )

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 7 – Quote Creation (Streaming Table)
# MAGIC
# MAGIC Triggered by `pipe_quote_review` – auto-approved quotes.
# MAGIC
# MAGIC Computes **coverage-level premiums** following actuarial-style rate × exposure logic,
# MAGIC then applies ML risk factor and experience mod.

# COMMAND ----------

@dlt.table(
    name="pipe_quote_creation",
    comment="Assembled quote with coverage-level premiums from auto-approved quotes.",
    table_properties={
        "quality": "gold",
        "pipelines.autoOptimize.managed": "true",
    },
)
def pipe_quote_creation():
    combined = (
        dlt.read_stream("pipe_quote_review")
        .filter(F.col("decision_tag") == "auto-approved")
    )

    # ── Multipliers ──────────────────────────────────────────────────────
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
        F.when(F.col("num_claims_5yr") == 0, 0.90)    # Credit for clean history
         .when(F.col("num_claims_5yr") <= 2, 1.0)      # Standard
         .when(F.col("num_claims_5yr") <= 4, 1.15)     # Moderate debit
         .otherwise(1.35)                                # Heavy claims debit
    )

    q = (
        combined
        .withColumn("risk_mult", risk_mult)
        .withColumn("industry_mult", industry_mult)
        .withColumn("experience_mod", experience_mod)
    )

    # ── Coverage-level premiums ──────────────────────────────────────────

    # General Liability – Occurrence: $1.50 per $1,000 revenue
    q = q.withColumn("gl_premium", F.when(
        F.col("gl_limit_requested") > 0,
        F.round(F.col("annual_revenue") / 1000 * 1.50
                * F.col("industry_mult") * F.col("risk_mult"), 2),
    ).otherwise(0.0))

    # Liquor Liability: flat rate for food_service / hospitality
    q = q.withColumn("liquor_premium", F.when(
        F.col("risk_category").isin("food_service", "hospitality")
        & (F.col("gl_limit_requested") > 0),
        F.round(F.lit(1000.0) * F.col("risk_mult"), 2),
    ).otherwise(0.0))

    # Property – Building: $0.65 per $100 of building TIV (70 % of total TIV)
    q = q.withColumn("property_building_premium", F.when(
        F.col("property_tiv") > 0,
        F.round(F.col("property_tiv") * 0.70 / 100 * 0.65 * F.col("risk_mult"), 2),
    ).otherwise(0.0))

    # Property – Contents: $0.80 per $100 of contents TIV (30 % of total TIV)
    q = q.withColumn("property_contents_premium", F.when(
        F.col("property_tiv") > 0,
        F.round(F.col("property_tiv") * 0.30 / 100 * 0.80 * F.col("risk_mult"), 2),
    ).otherwise(0.0))

    # Business Income: $0.25 per $100 of BI limit (capped at 50 % revenue or $500 K)
    q = q.withColumn(
        "bi_limit",
        F.least(F.col("annual_revenue") * 0.5, F.lit(500000.0)),
    )
    q = q.withColumn("bi_premium", F.when(
        F.col("annual_revenue") > 0,
        F.round(F.col("bi_limit") / 100 * 0.25 * F.col("risk_mult"), 2),
    ).otherwise(0.0))

    # Equipment Breakdown: flat $300 per location
    q = q.withColumn("equipment_premium", F.when(
        F.col("property_tiv") > 0,
        F.round(F.col("num_locations") * 300.0, 2),
    ).otherwise(0.0))

    # Workers Compensation: $1.50 per $100 payroll
    q = q.withColumn("wc_premium", F.when(
        F.col("annual_payroll") > 0,
        F.round(F.col("annual_payroll") / 100 * 1.50
                * F.col("industry_mult") * F.col("experience_mod"), 2),
    ).otherwise(0.0))

    # Commercial Auto: $2,000 per vehicle
    q = q.withColumn("auto_premium", F.when(
        F.col("auto_fleet_size") > 0,
        F.round(F.col("auto_fleet_size") * 2000.0 * F.col("risk_mult"), 2),
    ).otherwise(0.0))

    # Cyber Liability: $2.50 per $1,000 of limit
    q = q.withColumn("cyber_premium", F.when(
        F.col("cyber_limit_requested") > 0,
        F.round(F.col("cyber_limit_requested") / 1000 * 2.50 * F.col("risk_mult"), 2),
    ).otherwise(0.0))

    # Umbrella / Excess: $1,500 per $1 M of limit
    q = q.withColumn("umbrella_premium", F.when(
        F.col("umbrella_limit_requested") > 0,
        F.round(F.col("umbrella_limit_requested") / 1_000_000 * 1500.0
                * F.col("risk_mult"), 2),
    ).otherwise(0.0))

    # Terrorism (TRIA): ~1 % of GL + Property premiums
    q = q.withColumn("terrorism_premium", F.round(
        (F.col("gl_premium") + F.col("property_building_premium")
         + F.col("property_contents_premium")) * 0.01,
        2,
    ))

    # Policy & inspection fees
    q = q.withColumn("policy_fees", F.lit(150.0))

    # ── Subtotal ─────────────────────────────────────────────────────────
    q = q.withColumn("subtotal_premium", F.round(
        F.col("gl_premium") + F.col("liquor_premium")
        + F.col("property_building_premium") + F.col("property_contents_premium")
        + F.col("bi_premium") + F.col("equipment_premium")
        + F.col("wc_premium") + F.col("auto_premium")
        + F.col("cyber_premium") + F.col("umbrella_premium")
        + F.col("terrorism_premium") + F.col("policy_fees"),
        2,
    ))

    # ── Underwriter fields (NULL for auto-approved; populated by append flow) ──
    q = (
        q
        .withColumn("surcharge_pct", F.lit(None).cast("double"))
        .withColumn("discount_pct", F.lit(None).cast("double"))
        .withColumn("underwriter_notes", F.lit(None).cast("string"))
        .withColumn("decided_at", F.lit(None).cast("timestamp"))
        .withColumn("decided_by", F.lit(None).cast("string"))
    )

    # ── Total premium ────────────────────────────────────────────────────
    q = q.withColumn("total_premium", F.col("subtotal_premium"))

    # ── Quote metadata ───────────────────────────────────────────────────
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
        # Risk scoring
        "risk_score", "risk_band", "predicted_loss_ratio", "pricing_action",
        "risk_mult", "industry_mult", "experience_mod",
        # Coverage premiums
        "gl_premium", "liquor_premium",
        "property_building_premium", "property_contents_premium",
        "bi_premium", "equipment_premium",
        "wc_premium", "auto_premium", "cyber_premium", "umbrella_premium",
        "terrorism_premium", "policy_fees",
        "subtotal_premium",
        # Underwriter adjustments
        "surcharge_pct", "discount_pct", "underwriter_notes",
        "decided_at", "decided_by",
        "total_premium",
        # Coverage details
        "gl_limit_requested", "property_tiv", "bi_limit",
        "auto_fleet_size", "cyber_limit_requested", "umbrella_limit_requested",
        "annual_revenue", "annual_payroll", "num_employees", "num_locations",
        "num_claims_5yr", "total_claims_amount",
        "has_safety_procedures", "has_employee_training",
        # Policy details
        "effective_date", "expiration_date",
        # Timestamps
        "ingestion_timestamp", "scoring_timestamp", "review_timestamp",
        F.current_timestamp().alias("quote_timestamp"),
    )

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 8 – PDF Creation (Streaming Table)
# MAGIC
# MAGIC Reads from `pipe_quote_creation` and generates a professional PDF quote document:
# MAGIC - **auto-approved** or **uw-approved** → PDF is generated, written to the
# MAGIC   `quote_documents` volume, and the path is stored in `pipe_pdf_created`.
# MAGIC
# MAGIC > **Dependency:** `fpdf2` must be installed on the pipeline cluster.
# MAGIC > Add `"fpdf2"` to the pipeline's Python package configuration.

# COMMAND ----------

from pyspark.sql.types import StructType, StructField, StringType as _ST

_pdf_result_schema = StructType([
    StructField("pdf_path", _ST()),
    StructField("pdf_status", _ST()),
    StructField("pdf_error", _ST()),
])


@F.udf(returnType=_pdf_result_schema)
def _generate_quote_pdf_udf(quote_json, output_path, executive_summary):
    """Spark UDF wrapper – delegates to the standalone pdf_generator module."""
    import sys
    _vol = "/Volumes/dvin100_email_to_quote/email_to_quote/incoming_email"
    if _vol not in sys.path:
        sys.path.insert(0, _vol)
    from pdf_generator import generate_quote_pdf
    return generate_quote_pdf(quote_json, output_path, executive_summary)

# COMMAND ----------

@dlt.table(
    name="pipe_pdf_created",
    comment="PDF quote documents generated and stored in the quote_documents volume. Tracks path and generation status.",
    table_properties={
        "quality": "gold",
        "pipelines.autoOptimize.managed": "true",
    },
)
def pipe_pdf_created():
    quotes = dlt.read_stream("pipe_quote_creation")

    # Build JSON with all quote data for the PDF generator
    result = quotes.withColumn(
        "quote_data_json",
        F.to_json(F.struct(
            "quote_number", "business_name", "risk_category", "decision_tag",
            "review_summary", "risk_score", "risk_band", "predicted_loss_ratio",
            "pricing_action", "risk_mult", "experience_mod",
            "gl_premium", "liquor_premium",
            "property_building_premium", "property_contents_premium",
            "bi_premium", "equipment_premium",
            "wc_premium", "auto_premium", "cyber_premium", "umbrella_premium",
            "terrorism_premium", "policy_fees", "subtotal_premium",
            "surcharge_pct", "discount_pct", "underwriter_notes",
            "total_premium",
            "gl_limit_requested", "property_tiv", "bi_limit",
            "auto_fleet_size", "cyber_limit_requested", "umbrella_limit_requested",
            "annual_revenue", "annual_payroll", "num_employees", "num_locations",
            "num_claims_5yr", "total_claims_amount",
            "has_safety_procedures", "has_employee_training",
            "effective_date", "expiration_date",
        )),
    )

    # LLM executive summary for the PDF (only for approved quotes)
    result = result.withColumn(
        "pdf_executive_summary",
        F.when(
            F.col("decision_tag").isin("auto-approved", "uw-approved"),
            F.expr(f"""
                ai_query('{LLM_ENDPOINT}', CONCAT(
                    'You are writing the executive summary paragraph for a commercial ',
                    'insurance proposal document. Write 3-4 professional sentences ',
                    'summarizing this quote. Do not include headers or formatting.\\n\\n',
                    'Business: ', business_name, ' (',
                    REPLACE(risk_category, '_', ' '), ')\\n',
                    'Total Premium: $', FORMAT_NUMBER(total_premium, 0), '\\n',
                    'Risk Band: ', risk_band, ' (',
                    CAST(ROUND(risk_score, 1) AS STRING), '/100)\\n',
                    'Policy Period: ', CAST(effective_date AS STRING),
                    ' to ', CAST(expiration_date AS STRING), '\\n',
                    'Revenue: $', FORMAT_NUMBER(annual_revenue, 0),
                    ' | Employees: ', CAST(num_employees AS STRING)
                ), 'STRING')
            """),
        ).otherwise(F.lit(None)),
    )

    # PDF output path
    result = result.withColumn(
        "pdf_output_path",
        F.when(
            F.col("decision_tag").isin("auto-approved", "uw-approved"),
            F.concat(F.lit(QUOTE_VOLUME_PATH + "/"),
                     F.col("quote_number"), F.lit(".pdf")),
        ).otherwise(F.lit(None)),
    )

    # Generate PDF via UDF (only for approved quotes)
    result = result.withColumn(
        "pdf_result",
        F.when(
            F.col("decision_tag").isin("auto-approved", "uw-approved"),
            _generate_quote_pdf_udf(
                F.col("quote_data_json"),
                F.col("pdf_output_path"),
                F.col("pdf_executive_summary"),
            ),
        ).otherwise(F.lit(None).cast(_pdf_result_schema)),
    )

    return result.select(
        "email_id", "quote_number", "business_name", "risk_category",
        "decision_tag", "review_summary", "pdf_executive_summary",
        "quote_data_json",  # kept for standalone PDF regeneration
        F.coalesce(F.col("pdf_result.pdf_path"), F.lit(None)).alias("pdf_path"),
        F.when(
            F.col("decision_tag").isin("auto-approved", "uw-approved"),
            F.coalesce(F.col("pdf_result.pdf_status"), F.lit("error")),
        ).otherwise("skipped").alias("pdf_status"),
        F.col("pdf_result.pdf_error").alias("pdf_error"),
        "total_premium", "risk_score", "risk_band",
        # Timestamps
        "ingestion_timestamp", "scoring_timestamp",
        "review_timestamp", "quote_timestamp",
        F.current_timestamp().alias("pdf_timestamp"),
    )

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 9 – Completed (Streaming Table)
# MAGIC
# MAGIC Terminal state that **closes the loop** for every quote request UUID.
# MAGIC Unifies all final outcomes:
# MAGIC
# MAGIC | Source | Path |
# MAGIC |--------|------|
# MAGIC | `pipe_pdf_created` | auto-approved (PDF issued) |
# MAGIC | `pipe_quote_review` | auto-declined (skipped Steps 7-8 entirely) |

# COMMAND ----------

@dlt.table(
    name="pipe_completed",
    comment="Terminal state for all quote request UUIDs. Unifies auto-approved (via PDF) and auto-declined paths.",
    table_properties={
        "quality": "gold",
        "pipelines.autoOptimize.managed": "true",
    },
)
def pipe_completed():
    # ── Source 1: quotes that went through Steps 8-9 ─────────────────────
    from_pdf = (
        dlt.read_stream("pipe_pdf_created")
        .select(
            "email_id", "quote_number", "business_name", "risk_category",
            "decision_tag", "review_summary",
            "total_premium", "risk_score", "risk_band",
            "pdf_path", "pdf_status",
            "ingestion_timestamp",
            F.col("pdf_timestamp").alias("completed_timestamp"),
        )
        .withColumn(
            "final_status",
            F.when(F.col("pdf_status") == "generated", "quote_issued")
             .when(F.col("pdf_status") == "generated_txt", "quote_issued_txt")
             .when(F.col("pdf_status") == "skipped", "declined")
             .otherwise("error"),
        )
    )

    # ── Source 2: auto-declined (skipped Steps 8 & 9) ────────────────────
    from_declined = (
        dlt.read_stream("pipe_quote_review")
        .filter(F.col("decision_tag") == "auto-declined")
        .select(
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
        )
        .withColumn("final_status", F.lit("auto_declined"))
    )

    return from_pdf.unionByName(from_declined)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 10 – Response Email (Streaming Table)
# MAGIC
# MAGIC Uses an LLM to generate a professional response email for each completed quote:
# MAGIC - **Approved quotes** get a detailed quote summary with coverage breakdown and next steps
# MAGIC - **Declined quotes** get a polite notification with guidance on how to proceed
# MAGIC
# MAGIC The generated `.eml` file is saved to the `outgoing_email` volume for delivery.

# COMMAND ----------

OUTGOING_VOLUME_PATH = f"/Volumes/{CATALOG}/{SCHEMA}/outgoing_email"

RESPONSE_EMAIL_PROMPT_APPROVED = """You are the senior underwriting manager at BricksHouse Insurance Company.
Write a professional, warm, and detailed email to the customer with their approved insurance quote.

RULES:
- Address the customer by name
- Reference their business name
- Include the quote number prominently
- List each coverage line with its premium amount
- State the total annual premium clearly
- Mention the policy effective and expiration dates
- Include next steps: review the quote, contact with questions, reply to bind
- Note the quote is valid for 30 days
- Sign off as "BricksHouse Insurance Underwriting Team"
- Include phone (555) 123-4567 and email underwriting@brickshouse-insurance.com
- Keep it professional but personable
- Do NOT include email headers (From, To, Subject, etc.) - just the body text

QUOTE DATA:
"""

RESPONSE_EMAIL_PROMPT_DECLINED = """You are the senior underwriting manager at BricksHouse Insurance Company.
Write a professional, empathetic email to the customer informing them their quote request was declined.

RULES:
- Address the customer by name
- Reference their business name
- Be respectful and empathetic - do NOT be blunt
- Briefly mention the assessment without being overly specific about risk details
- Suggest they can: contact to discuss, provide additional information, or try again in the future
- Sign off as "BricksHouse Insurance Underwriting Team"
- Include phone (555) 123-4567 and email underwriting@brickshouse-insurance.com
- Do NOT include email headers (From, To, Subject, etc.) - just the body text

QUOTE DATA:
"""


@dlt.table(
    name="pipe_response_email",
    comment="LLM-generated response emails for completed quotes, saved to outgoing_email volume.",
    table_properties={
        "quality": "gold",
        "pipelines.autoOptimize.managed": "true",
    },
)
def pipe_response_email():
    completed = dlt.read_stream("pipe_completed")

    # Build a context column with all relevant quote details
    result = completed.withColumn(
        "quote_context",
        F.concat(
            F.lit("Customer Name: "), F.coalesce(F.col("business_name"), F.lit("N/A")),
            F.lit("\nRisk Category: "), F.coalesce(F.col("risk_category"), F.lit("N/A")),
            F.lit("\nDecision: "), F.coalesce(F.col("decision_tag"), F.lit("N/A")),
            F.lit("\nRisk Band: "), F.coalesce(F.col("risk_band"), F.lit("N/A")),
            F.lit("\nRisk Score: "), F.coalesce(F.col("risk_score").cast("string"), F.lit("N/A")),
            F.lit("\nReview Summary: "), F.coalesce(F.col("review_summary"), F.lit("N/A")),
            F.lit("\nQuote Number: "), F.coalesce(F.col("quote_number"), F.lit("N/A")),
            F.lit("\nTotal Premium: $"), F.coalesce(F.format_number(F.col("total_premium"), 2), F.lit("N/A")),
            F.lit("\nFinal Status: "), F.coalesce(F.col("final_status"), F.lit("N/A")),
        ),
    )

    # Determine which prompt to use based on decision
    result = result.withColumn(
        "email_prompt",
        F.when(
            F.col("decision_tag").isin("auto-approved", "uw-approved"),
            F.concat(
                F.lit(RESPONSE_EMAIL_PROMPT_APPROVED.replace("'", "''")),
                F.col("quote_context"),
            ),
        ).otherwise(
            F.concat(
                F.lit(RESPONSE_EMAIL_PROMPT_DECLINED.replace("'", "''")),
                F.col("quote_context"),
            ),
        ),
    )

    # Generate email body via LLM
    result = result.withColumn(
        "email_body",
        F.expr(f"ai_query('{LLM_ENDPOINT}', email_prompt, 'STRING')"),
    )

    # Build email subject
    result = result.withColumn(
        "email_subject",
        F.when(
            F.col("decision_tag").isin("auto-approved", "uw-approved"),
            F.concat(
                F.lit("Your Commercial Insurance Quote "),
                F.coalesce(F.col("quote_number"), F.lit("")),
                F.lit(" - "),
                F.coalesce(F.col("business_name"), F.lit("Your Business")),
            ),
        ).otherwise(
            F.concat(
                F.lit("Quote Request Update - "),
                F.coalesce(F.col("business_name"), F.lit("Your Business")),
            ),
        ),
    )

    # Build the .eml file name
    result = result.withColumn(
        "eml_file_name",
        F.concat(
            F.when(F.col("decision_tag").isin("auto-approved", "uw-approved"), F.lit("quote_"))
             .otherwise(F.lit("declined_")),
            F.coalesce(F.col("quote_number"), F.lit("NONE")),
            F.lit("_"),
            F.substring(F.col("email_id"), 1, 8),
            F.lit(".eml"),
        ),
    )

    # Build full .eml content
    result = result.withColumn(
        "eml_content",
        F.concat(
            F.lit("From: underwriting@brickshouse-insurance.com\n"),
            F.lit("To: customer@placeholder.com\n"),
            F.lit("Subject: "), F.col("email_subject"), F.lit("\n"),
            F.lit("Date: "), F.date_format(F.current_timestamp(), "EEE, dd MMM yyyy HH:mm:ss +0000"), F.lit("\n"),
            F.lit("MIME-Version: 1.0\n"),
            F.lit("Content-Type: text/plain; charset=\"UTF-8\"\n"),
            F.lit("Message-ID: <"), F.col("email_id"), F.lit("@brickshouse-insurance.com>\n"),
            F.lit("\n"),
            F.col("email_body"),
        ),
    )

    # Write the .eml file to the outgoing_email volume
    result = result.withColumn(
        "eml_volume_path",
        F.concat(F.lit(OUTGOING_VOLUME_PATH + "/"), F.col("eml_file_name")),
    )

    # Use copy_into-style approach: write the file content via a UDF
    @F.udf("string")
    def write_eml_to_volume(eml_content: str, file_path: str) -> str:
        """Write an .eml file to a Unity Catalog Volume and return the path."""
        try:
            import os
            os.makedirs(os.path.dirname(file_path), exist_ok=True)
            with open(file_path, "w", encoding="utf-8") as f:
                f.write(eml_content)
            return file_path
        except Exception as e:
            return f"ERROR: {e}"

    result = result.withColumn(
        "eml_write_status",
        write_eml_to_volume(F.col("eml_content"), F.col("eml_volume_path")),
    )

    result = result.withColumn("response_timestamp", F.current_timestamp())

    return result.select(
        "email_id",
        "business_name",
        "decision_tag",
        "final_status",
        "quote_number",
        "total_premium",
        "email_subject",
        "email_body",
        "eml_file_name",
        "eml_volume_path",
        "eml_write_status",
        "ingestion_timestamp",
        "completed_timestamp",
        "response_timestamp",
    )
