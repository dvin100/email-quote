"""
Migrate reference tables from Unity Catalog (Delta) to Lakebase (PostgreSQL).
Reads via Databricks SQL Statements API (curl), writes via psycopg to Lakebase.
No databricks-sdk dependency - uses CLI for auth.
"""

import json
import os
import subprocess
import sys
import tempfile
import time

import psycopg
import requests

PROFILE = "EMAIL_QUOTE"
UC_CATALOG = "dvin100_email_to_quote"
UC_SCHEMA = "email_to_quote"
UC_FQN = f"{UC_CATALOG}.{UC_SCHEMA}"
WAREHOUSE_ID = "6aa0e30549cb3d17"

ENDPOINT_RESOURCE = "projects/emai2quote/branches/production/endpoints/primary"
LB_HOST = "ep-icy-pond-d8d33jwn.database.us-east-2.cloud.databricks.com"
LB_DB = "databricks_postgres"
LB_SCHEMA = "email_to_quote"

BATCH_SIZE = 2000

TABLES = {
    "organizations": "org_id",
    "locations": "location_id",
    "policies": "policy_id",
    "claims": "claim_id",
    "coverage_requests": "request_id",
    "vehicles": "vehicle_id",
    "cyber_profiles": "cyber_profile_id",
}


def _cli_env():
    return {**os.environ, "DATABRICKS_CONFIG_PROFILE": PROFILE}


def get_workspace_host():
    """Get workspace host from databricks CLI config."""
    import configparser
    cfg = configparser.ConfigParser()
    cfg.read(os.path.expanduser("~/.databrickscfg"))
    return cfg[PROFILE]["host"].rstrip("/")


def get_auth_token():
    """Get workspace auth token via CLI."""
    r = subprocess.run(
        ["databricks", "auth", "token", "--output", "json"],
        capture_output=True, text=True, env=_cli_env(),
    )
    return json.loads(r.stdout)["access_token"]


def get_lb_token():
    """Generate a fresh Lakebase credential token."""
    r = subprocess.run(
        ["databricks", "postgres", "generate-database-credential",
         ENDPOINT_RESOURCE, "--output", "json"],
        capture_output=True, text=True, env=_cli_env(),
    )
    return json.loads(r.stdout)["token"]


def get_lb_user():
    """Get the current Databricks user email."""
    r = subprocess.run(
        ["databricks", "current-user", "me", "--output", "json"],
        capture_output=True, text=True, env=_cli_env(),
    )
    return json.loads(r.stdout)["userName"]


def sql_query(host, token, sql):
    """Execute SQL via Statements API, return (columns, rows)."""
    resp = requests.post(
        f"{host}/api/2.0/sql/statements",
        headers={"Authorization": f"Bearer {token}", "Content-Type": "application/json"},
        json={
            "warehouse_id": WAREHOUSE_ID,
            "statement": sql,
            "wait_timeout": "50s",
            "disposition": "INLINE",
            "format": "JSON_ARRAY",
        },
        timeout=180,
    )
    data = resp.json()
    status = data.get("status", {}).get("state", "")
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
        raise RuntimeError(f"SQL failed ({status}): {err}")

    columns = [c["name"] for c in data["manifest"]["schema"]["columns"]]
    rows = data.get("result", {}).get("data_array", [])

    # Handle chunked results
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


def clean_row(row):
    """Convert null strings and booleans for PostgreSQL."""
    out = []
    for val in row:
        if val is None or val == "null" or val == "NULL":
            out.append(None)
        elif isinstance(val, str) and val.lower() in ("true", "false"):
            out.append(val.lower() == "true")
        else:
            out.append(val)
    return out


def migrate():
    host = get_workspace_host()
    user = get_lb_user()
    print(f"Workspace: {host}")
    print(f"User: {user}")
    print(f"Source: {UC_FQN}")
    print(f"Target: {LB_HOST}/{LB_DB}/{LB_SCHEMA}")
    print()

    for table, pk_col in TABLES.items():
        print(f"{'='*60}")
        print(f"Migrating: {table}")
        print(f"{'='*60}")

        # 1. Get row count from UC
        auth_token = get_auth_token()
        _, cnt_rows = sql_query(host, auth_token, f"SELECT count(*) FROM {UC_FQN}.{table}")
        total = int(cnt_rows[0][0])
        print(f"  UC rows: {total}")

        if total == 0:
            print(f"  Skipping empty table")
            continue

        # 2. Connect to Lakebase and truncate
        lb_token = get_lb_token()
        conn = psycopg.connect(
            host=LB_HOST, dbname=LB_DB, user=user,
            password=lb_token, sslmode="require",
            options=f"-csearch_path={LB_SCHEMA}",
            autocommit=True,
        )
        cur = conn.cursor()
        cur.execute(f"TRUNCATE TABLE {table} CASCADE")
        print(f"  Truncated Lakebase table")
        cur.close()
        conn.close()

        # 3. Read from UC in batches and insert
        offset = 0
        inserted = 0
        while offset < total:
            print(f"  Fetching rows {offset}..{offset+BATCH_SIZE}...")
            auth_token = get_auth_token()
            columns, rows = sql_query(
                host, auth_token,
                f"SELECT * FROM {UC_FQN}.{table} ORDER BY {pk_col} LIMIT {BATCH_SIZE} OFFSET {offset}"
            )
            if not rows:
                break

            placeholders = ", ".join(["%s"] * len(columns))
            col_list = ", ".join(f'"{c}"' for c in columns)
            insert_sql = f'INSERT INTO {table} ({col_list}) VALUES ({placeholders})'

            # Fresh Lakebase connection for each batch
            lb_token = get_lb_token()
            conn = psycopg.connect(
                host=LB_HOST, dbname=LB_DB, user=user,
                password=lb_token, sslmode="require",
                options=f"-csearch_path={LB_SCHEMA}",
            )
            conn.autocommit = False
            cur = conn.cursor()

            try:
                # Insert in sub-batches of 500
                for i in range(0, len(rows), 500):
                    batch = [clean_row(r) for r in rows[i:i+500]]
                    cur.executemany(insert_sql, batch)
                conn.commit()
                inserted += len(rows)
                print(f"  Loaded {inserted}/{total}")
            except Exception as e:
                conn.rollback()
                print(f"  ERROR inserting batch at offset {offset}: {e}")
            finally:
                cur.close()
                conn.close()

            offset += BATCH_SIZE

        print(f"  DONE: {table} = {inserted} rows")

    # Verification
    print(f"\n{'='*60}")
    print("VERIFICATION")
    print(f"{'='*60}")
    lb_token = get_lb_token()
    conn = psycopg.connect(
        host=LB_HOST, dbname=LB_DB, user=user,
        password=lb_token, sslmode="require",
        options=f"-csearch_path={LB_SCHEMA}",
    )
    cur = conn.cursor()
    for table in TABLES.keys():
        cur.execute(f"SELECT count(*) FROM {table}")
        count = cur.fetchone()[0]
        print(f"  {table}: {count}")
    cur.close()
    conn.close()
    print(f"\nMigration complete!")


if __name__ == "__main__":
    migrate()
