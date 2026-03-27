"""
Migrate reference tables from Unity Catalog (Delta) to Lakebase (PostgreSQL).
Reads via Databricks SQL Statements API, writes via psycopg to Lakebase.
"""

import json
import time
import psycopg
import requests
from databricks.sdk import WorkspaceClient

PROFILE = "EMAIL_QUOTE"
UC_CATALOG = "dvin100_email_to_quote"
UC_SCHEMA = "email_to_quote"
WAREHOUSE_ID = "9d37ebbf410ea6d5"

LB_HOST = "ep-bitter-term-d8cbhgar.database.us-east-2.cloud.databricks.com"
LB_DB = "databricks_postgres"
LB_USER = "fc0bfb5f-6069-4a8f-b4ce-84cc19949784"
LB_PASSWORD = "BricksH0use!Ins2026#"
LB_SCHEMA = "email_to_quote"

# Tables to migrate and their Lakebase DDL (PostgreSQL syntax)
TABLES = {
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


def sql_api_query(host: str, token: str, sql: str) -> list[list]:
    """Execute SQL via Statements API, handle pagination."""
    resp = requests.post(
        f"{host}/api/2.0/sql/statements",
        headers={"Authorization": f"Bearer {token}", "Content-Type": "application/json"},
        json={
            "warehouse_id": WAREHOUSE_ID,
            "statement": sql,
            "catalog": UC_CATALOG,
            "schema": UC_SCHEMA,
            "wait_timeout": "60s",
            "disposition": "INLINE",
            "format": "JSON_ARRAY",
        },
        timeout=120,
    )
    data = resp.json()
    status = data.get("status", {}).get("state", "")

    # Poll if still running
    stmt_id = data.get("statement_id")
    while status in ("PENDING", "RUNNING"):
        time.sleep(2)
        poll = requests.get(
            f"{host}/api/2.0/sql/statements/{stmt_id}",
            headers={"Authorization": f"Bearer {token}"},
            timeout=30,
        )
        data = poll.json()
        status = data.get("status", {}).get("state", "")

    if status != "SUCCEEDED":
        err = data.get("status", {}).get("error", {}).get("message", "unknown")
        raise RuntimeError(f"SQL failed: {err}")

    columns = [c["name"] for c in data.get("manifest", {}).get("schema", {}).get("columns", [])]
    rows = data.get("result", {}).get("data_array", [])

    # Handle external links (chunked results)
    ext_links = data.get("result", {}).get("external_links", [])
    for link in ext_links:
        chunk_resp = requests.get(link["external_link"], timeout=60)
        rows.extend(chunk_resp.json())

    # Follow next_chunk_internal_link if present
    chunk_link = data.get("result", {}).get("next_chunk_internal_link")
    while chunk_link:
        chunk_resp = requests.get(
            f"{host}{chunk_link}",
            headers={"Authorization": f"Bearer {token}"},
            timeout=60,
        )
        chunk_data = chunk_resp.json()
        rows.extend(chunk_data.get("data_array", []))
        chunk_link = chunk_data.get("next_chunk_internal_link")

    return columns, rows


def migrate():
    w = WorkspaceClient(profile=PROFILE)
    host = w.config.host.rstrip("/")
    token = w.config.authenticate()
    # Get the token string
    import types
    if isinstance(token, types.GeneratorType) or hasattr(token, '__next__'):
        token = dict(token)
    if isinstance(token, dict):
        token = token.get("Authorization", "").replace("Bearer ", "")
    else:
        token = str(token)

    # Actually use the SDK's auth
    headers = dict(w.config.authenticate())
    auth_token = headers.get("Authorization", "").replace("Bearer ", "")

    # Connect to Lakebase
    conn = psycopg.connect(
        host=LB_HOST,
        dbname=LB_DB,
        user=LB_USER,
        password=LB_PASSWORD,
        sslmode="require",
        options=f"-csearch_path={LB_SCHEMA}",
    )
    conn.autocommit = False
    cur = conn.cursor()

    for table_name, create_ddl in TABLES.items():
        print(f"\n{'='*60}")
        print(f"Migrating: {table_name}")
        print(f"{'='*60}")

        # 1. Create table in Lakebase
        cur.execute(f"DROP TABLE IF EXISTS {table_name} CASCADE")
        cur.execute(create_ddl)
        conn.commit()
        print(f"  Created table in Lakebase")

        # 2. Read from UC
        print(f"  Reading from UC...")
        columns, rows = sql_api_query(host, auth_token, f"SELECT * FROM {table_name}")
        print(f"  Got {len(rows)} rows, {len(columns)} columns")

        if not rows:
            print(f"  No data to migrate")
            continue

        # 3. Insert into Lakebase in batches
        placeholders = ", ".join(["%s"] * len(columns))
        col_list = ", ".join(columns)
        insert_sql = f"INSERT INTO {table_name} ({col_list}) VALUES ({placeholders})"

        batch_size = 500
        inserted = 0
        for i in range(0, len(rows), batch_size):
            batch = rows[i : i + batch_size]
            # Convert "null" strings and handle types
            clean_batch = []
            for row in batch:
                clean_row = []
                for val in row:
                    if val is None or val == "null" or val == "NULL":
                        clean_row.append(None)
                    elif isinstance(val, str) and val.lower() in ("true", "false"):
                        clean_row.append(val.lower() == "true")
                    else:
                        clean_row.append(val)
                clean_batch.append(clean_row)

            cur.executemany(insert_sql, clean_batch)
            conn.commit()
            inserted += len(batch)
            print(f"  Inserted {inserted}/{len(rows)} rows...", end="\r")

        print(f"  Inserted {inserted}/{len(rows)} rows - DONE")

    cur.close()
    conn.close()
    print(f"\n{'='*60}")
    print("Migration complete!")
    print(f"{'='*60}")


if __name__ == "__main__":
    migrate()
