# Databricks notebook source
# MAGIC %md
# MAGIC # Migrate Reference Tables from UC (Delta) to Lakebase (PostgreSQL)

# COMMAND ----------

import psycopg

import os

ENDPOINT_RESOURCE = os.environ.get(
    "LAKEBASE_ENDPOINT",
    "projects/emai2quote/branches/production/endpoints/primary",
)
LB_HOST = os.environ.get("LAKEBASE_HOST", "ep-bitter-term-d8cbhgar.database.us-east-2.cloud.databricks.com")
LB_DB = os.environ.get("LAKEBASE_DB", "databricks_postgres")
LB_SCHEMA = "email_to_quote"
UC_CATALOG = "dvin100_email_to_quote"
UC_SCHEMA = "email_to_quote"

TABLES_DDL = {
    "organizations": """
        CREATE TABLE IF NOT EXISTS organizations (
            org_id TEXT PRIMARY KEY,
            legal_name TEXT NOT NULL,
            trading_name TEXT,
            legal_structure TEXT NOT NULL,
            date_established DATE,
            years_current_ownership INT,
            primary_phone TEXT,
            primary_email TEXT,
            primary_contact_name TEXT,
            primary_contact_title TEXT,
            naics_code TEXT,
            sic_code TEXT,
            business_description TEXT,
            main_products_services TEXT,
            industries_served TEXT,
            risk_category TEXT,
            num_employees INT,
            num_contractors INT,
            uses_subcontractors BOOLEAN,
            annual_revenue DOUBLE PRECISION,
            annual_payroll DOUBLE PRECISION,
            has_written_safety_procedures BOOLEAN,
            has_employee_training_program BOOLEAN,
            has_cyber_controls BOOLEAN,
            website TEXT,
            created_at TIMESTAMP
        )
    """,
    "locations": """
        CREATE TABLE IF NOT EXISTS locations (
            location_id TEXT PRIMARY KEY,
            org_id TEXT NOT NULL,
            location_type TEXT NOT NULL,
            address_line1 TEXT,
            address_line2 TEXT,
            city TEXT,
            state TEXT,
            zip_code TEXT,
            country TEXT,
            square_footage INT,
            building_age_years INT,
            construction_type TEXT,
            num_stories INT,
            has_sprinkler_system BOOLEAN,
            has_fire_alarm BOOLEAN,
            has_security_system BOOLEAN,
            has_cctv BOOLEAN,
            distance_to_fire_station_miles DOUBLE PRECISION,
            fire_protection_class INT,
            is_primary BOOLEAN,
            created_at TIMESTAMP
        )
    """,
    "policies": """
        CREATE TABLE IF NOT EXISTS policies (
            policy_id TEXT PRIMARY KEY,
            org_id TEXT NOT NULL,
            policy_type TEXT NOT NULL,
            insurer_name TEXT,
            policy_number TEXT,
            effective_date DATE,
            expiration_date DATE,
            coverage_limit DOUBLE PRECISION,
            aggregate_limit DOUBLE PRECISION,
            deductible DOUBLE PRECISION,
            annual_premium DOUBLE PRECISION,
            is_current BOOLEAN,
            special_endorsements TEXT,
            created_at TIMESTAMP
        )
    """,
    "claims": """
        CREATE TABLE IF NOT EXISTS claims (
            claim_id TEXT PRIMARY KEY,
            org_id TEXT NOT NULL,
            policy_id TEXT,
            claim_date DATE NOT NULL,
            claim_type TEXT NOT NULL,
            description TEXT,
            amount_paid DOUBLE PRECISION,
            amount_reserved DOUBLE PRECISION,
            status TEXT,
            cause TEXT,
            corrective_action_taken TEXT,
            created_at TIMESTAMP
        )
    """,
    "coverage_requests": """
        CREATE TABLE IF NOT EXISTS coverage_requests (
            request_id TEXT PRIMARY KEY,
            org_id TEXT NOT NULL,
            coverage_type TEXT NOT NULL,
            requested_limit DOUBLE PRECISION,
            requested_deductible DOUBLE PRECISION,
            special_terms TEXT,
            priority TEXT,
            effective_date_requested DATE,
            created_at TIMESTAMP
        )
    """,
    "vehicles": """
        CREATE TABLE IF NOT EXISTS vehicles (
            vehicle_id TEXT PRIMARY KEY,
            org_id TEXT NOT NULL,
            vin TEXT,
            year INT,
            make TEXT,
            model TEXT,
            vehicle_type TEXT,
            usage_type TEXT,
            annual_mileage INT,
            garaging_zip TEXT,
            driver_name TEXT,
            driver_license_number TEXT,
            driver_age INT,
            driver_years_experience INT,
            driver_violations_3yr INT,
            current_value DOUBLE PRECISION,
            created_at TIMESTAMP
        )
    """,
    "cyber_profiles": """
        CREATE TABLE IF NOT EXISTS cyber_profiles (
            cyber_profile_id TEXT PRIMARY KEY,
            org_id TEXT NOT NULL,
            num_endpoints INT,
            num_servers INT,
            has_cloud_infrastructure BOOLEAN,
            cloud_providers TEXT,
            stores_pii BOOLEAN,
            stores_phi BOOLEAN,
            stores_payment_data BOOLEAN,
            num_records_stored INT,
            has_endpoint_protection BOOLEAN,
            has_firewall BOOLEAN,
            has_mfa BOOLEAN,
            has_encryption_at_rest BOOLEAN,
            has_encryption_in_transit BOOLEAN,
            has_backup_plan BOOLEAN,
            has_incident_response_plan BOOLEAN,
            has_security_training BOOLEAN,
            compliance_frameworks TEXT,
            last_security_audit_date DATE,
            has_cyber_insurance_history BOOLEAN,
            created_at TIMESTAMP
        )
    """,
}

# COMMAND ----------

# Connect to Lakebase using OAuth credential
from databricks.sdk import WorkspaceClient
w = WorkspaceClient()
cred = w.postgres.generate_database_credential(ENDPOINT_RESOURCE)
conn = psycopg.connect(
    host=LB_HOST, dbname=LB_DB, user=w.config.username, password=cred.token,
    sslmode="require", options=f"-csearch_path={LB_SCHEMA}",
)
cur = conn.cursor()

for table_name, create_ddl in TABLES_DDL.items():
    print(f"\n{'='*60}")
    print(f"Migrating: {table_name}")

    # Drop and recreate in Lakebase
    cur.execute(f"DROP TABLE IF EXISTS {table_name} CASCADE")
    cur.execute(create_ddl)
    conn.commit()
    print(f"  Created table in Lakebase")

    # Read from UC Delta
    df = spark.table(f"{UC_CATALOG}.{UC_SCHEMA}.{table_name}")
    rows = df.collect()
    cols = df.columns
    print(f"  Read {len(rows)} rows from UC")

    if not rows:
        continue

    # Insert into Lakebase in batches
    placeholders = ", ".join(["%s"] * len(cols))
    col_list = ", ".join(cols)
    insert_sql = f"INSERT INTO {table_name} ({col_list}) VALUES ({placeholders}) ON CONFLICT DO NOTHING"

    batch_size = 500
    inserted = 0
    for i in range(0, len(rows), batch_size):
        batch = [tuple(row.asDict()[c] for c in cols) for row in rows[i:i+batch_size]]
        cur.executemany(insert_sql, batch)
        conn.commit()
        inserted += len(batch)

    print(f"  Inserted {inserted} rows into Lakebase")

cur.close()
conn.close()
print(f"\n{'='*60}")
print("Migration complete!")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Drop UC tables (now that data is in Lakebase)

# COMMAND ----------

for table_name in TABLES_DDL.keys():
    spark.sql(f"DROP TABLE IF EXISTS {UC_CATALOG}.{UC_SCHEMA}.{table_name}")
    print(f"  Dropped UC table: {table_name}")

print("All UC reference tables dropped.")
