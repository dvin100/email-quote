# Mail2Quote Deployment Runbook

Complete guide to deploy the Email-to-Quote pipeline on a fresh Databricks workspace.

## Prerequisites

- Databricks CLI installed and authenticated
- `psql` available (e.g., `brew install libpq`)
- Python 3 with `jq` available
- A Databricks workspace with serverless enabled, Lakebase enabled, and Foundation Model API access

## Configuration

Set these variables before running any commands:

```bash
export DATABRICKS_CONFIG_PROFILE=EMAIL_QUOTE
export WORKSPACE_HOST="https://dbc-d0045e0a-a058.cloud.databricks.com"
export CATALOG="dvin100_email_to_quote"
export SCHEMA="email_to_quote"
export LAKEBASE_PROJECT="emai2quote"
export APP_NAME="mail2quote"
export PROJECT_DIR="/Users/david.vincent/vibe/mail2quote"
```

## Step 1: Authenticate

```bash
databricks auth login --host $WORKSPACE_HOST --profile $DATABRICKS_CONFIG_PROFILE
databricks current-user me  # verify
```

## Step 2: Create Unity Catalog Resources

```bash
# Catalog (uses default storage)
databricks api post /api/2.0/sql/statements --json '{
  "warehouse_id": "AUTO",
  "statement": "CREATE CATALOG IF NOT EXISTS '"$CATALOG"'",
  "wait_timeout": "30s"
}'

# Schema
databricks api post /api/2.0/sql/statements --json '{
  "warehouse_id": "AUTO",
  "statement": "CREATE SCHEMA IF NOT EXISTS '"$CATALOG"'.'"$SCHEMA"'",
  "wait_timeout": "30s"
}'
```

Then create all tables by running the DDL:
```bash
# Uses execute_sql_multi or run each statement from ddl.sql
# Also create the underwriter table WITH CDF (no IDENTITY column):
# CREATE TABLE $CATALOG.$SCHEMA.underwriter (
#   id BIGINT, email_id STRING NOT NULL, decision STRING NOT NULL,
#   surcharge_pct DOUBLE, discount_pct DOUBLE, notes STRING,
#   decided_at TIMESTAMP, decided_by STRING, created_at TIMESTAMP, info_request STRING
# ) TBLPROPERTIES (delta.enableChangeDataFeed = true)
```

Create volumes:
```bash
# incoming_email — for .eml files (AutoLoader source)
# quote_documents — for generated PDF quotes
```

## Step 3: Load Reference Data

```bash
# Update WAREHOUSE_ID in load_data.py to match the target workspace
cd $PROJECT_DIR
python3 load_data.py
```

This loads 582 SQL batch files from `data/` into all 11 reference tables.

## Step 4: Upload Notebooks to Workspace

```bash
# Pipeline notebooks
databricks workspace mkdirs /Users/$USER/email_to_quote/pipelines
databricks workspace mkdirs /Users/$USER/email_to_quote/ml_pipeline
databricks workspace mkdirs /Users/$USER/mail2quote

databricks workspace import /Users/$USER/email_to_quote/pipelines/pipeline_email_ingestion \
  --file $PROJECT_DIR/notebooks/pipeline_email_ingestion.py --language PYTHON --format SOURCE --overwrite

databricks workspace import /Users/$USER/mail2quote/ingest_underwriter \
  --file $PROJECT_DIR/notebooks/ingest_underwriter.py --language PYTHON --format SOURCE --overwrite

# ML pipeline notebooks
for nb in 01_feature_engineering 02_automl_training 03_model_registration 04_model_serving 05_monitoring; do
  databricks workspace import "/Users/$USER/email_to_quote/ml_pipeline/${nb}" \
    --file "$PROJECT_DIR/notebooks/${nb}.py" --language PYTHON --format SOURCE --overwrite
done

# Utility notebooks
for nb in generate_test_emails reset_pipe_tables migrate_ref_to_lakebase sync_underwriter; do
  databricks workspace import "/Users/$USER/mail2quote/${nb}" \
    --file "$PROJECT_DIR/notebooks/${nb}.py" --language PYTHON --format SOURCE --overwrite
done
```

## Step 5: Upload pdf_generator.py to Volume

**CRITICAL** — the DLT pipeline's PDF creation UDF imports this from the volume at runtime.

```bash
databricks fs cp $PROJECT_DIR/app/backend/pdf_generator.py \
  /Volumes/$CATALOG/$SCHEMA/incoming_email/pdf_generator.py --overwrite
```

## Step 6: Create Lakebase Database

```bash
databricks postgres create-project $LAKEBASE_PROJECT --no-wait
# Wait for project creation, then check:
databricks postgres get-project projects/$LAKEBASE_PROJECT
# Note the endpoint host from: projects/$LAKEBASE_PROJECT/branches/production/endpoints/primary
```

Record the Lakebase endpoint host (e.g., `ep-icy-pond-d8d33jwn.database.us-east-2.cloud.databricks.com`) and update these files:
- `app/app.yaml` — `LAKEBASE_HOST` env var
- `app/backend/main.py` — default host fallback
- `notebooks/sync_underwriter.py` — default host
- `notebooks/migrate_ref_to_lakebase.py` — default host
- `generate_sample_emails.py` — default host
- `migrate_to_lakebase.py` — default host

Create the Lakebase schema and tables:
```bash
LB_TOKEN=$(databricks postgres generate-database-credential \
  "projects/$LAKEBASE_PROJECT/branches/production/endpoints/primary" --output json | jq -r '.token')
LB_HOST="<endpoint-host>"
LB_USER="<your-email>"

PGPASSWORD="$LB_TOKEN" psql "host=$LB_HOST dbname=databricks_postgres user=$LB_USER sslmode=require" -c "
  CREATE SCHEMA IF NOT EXISTS email_to_quote;
"

# Then create tables (organizations, locations, policies, claims, coverage_requests,
# vehicles, cyber_profiles, underwriter, sample_emails) — see ddl.sql for schemas.
# The underwriter table needs: id SERIAL PRIMARY KEY, email_id, decision, surcharge_pct,
# discount_pct, notes, decided_at, decided_by, created_at, info_request
```

## Step 7: Migrate Data to Lakebase

Run the local migration script (reads from UC via SQL API, writes via psql):
```bash
# Update WAREHOUSE_ID in migrate_to_lakebase.py first
python3 $PROJECT_DIR/migrate_to_lakebase.py
```

Also generate sample emails in Lakebase:
```bash
# Update WAREHOUSE_ID in generate_sample_emails.py first
python3 $PROJECT_DIR/generate_sample_emails.py
```

## Step 8: Generate Test Emails

Submit as a serverless notebook run:
```bash
databricks jobs submit --json '{
  "run_name": "Generate Test Emails",
  "tasks": [{"task_key": "gen", "notebook_task": {
    "notebook_path": "/Users/'"$USER"'/mail2quote/generate_test_emails", "source": "WORKSPACE"
  }, "environment_key": "e"}],
  "environments": [{"environment_key": "e", "spec": {"client": "1"}}]
}'
```

This writes .eml files to `/Volumes/$CATALOG/$SCHEMA/incoming_email/`.
The verification step at the end may fail (queries a DLT-managed table) — that's OK as long as .eml files exist.

## Step 9: Create & Start the DLT Pipeline

```bash
databricks pipelines create --json @$PROJECT_DIR/underwriter_pipeline_config.json
# Note the pipeline_id
# Remove "clusters" key from config if present (serverless doesn't accept it)
```

The pipeline config must reference:
- `pipeline_email_ingestion` notebook
- `ingest_underwriter` notebook
- `catalog: dvin100_email_to_quote`, `target: email_to_quote`
- `continuous: true`, `serverless: true`
- `pipelines.trigger.interval: "1 seconds"`

Start it:
```bash
databricks pipelines start-update <pipeline_id> --full-refresh
```

## Step 10: Create the Sync Underwriter Job

This is the **critical** link between the app (writes to Lakebase) and the DLT pipeline (reads from UC via CDF).

```bash
databricks jobs create --json '{
  "name": "Sync Underwriter Continuous (Lakebase to Delta)",
  "tasks": [{"task_key": "sync", "notebook_task": {
    "notebook_path": "/Users/'"$USER"'/mail2quote/sync_underwriter", "source": "WORKSPACE"
  }, "environment_key": "e", "timeout_seconds": 0}],
  "environments": [{"environment_key": "e", "spec": {"client": "1"}}],
  "max_concurrent_runs": 1
}'
databricks jobs run-now <job_id>
```

**Key design decisions for sync_underwriter.py:**
- Uses **insert-only MERGE** (`whenNotMatchedInsertAll` only, NO `whenMatchedUpdateAll`)
- This prevents CDF pollution — each poll cycle would otherwise generate `update_postimage` events for every existing row, flooding the DLT append flows
- The UC `underwriter` table must NOT have an IDENTITY column (breaks MERGE)
- Credentials passed via `dbutils.widgets` with defaults

## Step 11: Deploy the Databricks App

### Grant app service principal permissions

After creating the app, note its SP UUID from `databricks apps get $APP_NAME`.

```sql
GRANT ALL PRIVILEGES ON CATALOG dvin100_email_to_quote TO `<sp-uuid>`
```

In Lakebase:
```sql
-- Create native PG role for the SP
CREATE ROLE "<sp-uuid>" WITH LOGIN PASSWORD '<password>';
GRANT USAGE ON SCHEMA email_to_quote TO "<sp-uuid>";
GRANT SELECT, INSERT, UPDATE, DELETE ON ALL TABLES IN SCHEMA email_to_quote TO "<sp-uuid>";
GRANT USAGE, SELECT ON ALL SEQUENCES IN SCHEMA email_to_quote TO "<sp-uuid>";
ALTER DEFAULT PRIVILEGES IN SCHEMA email_to_quote GRANT SELECT, INSERT, UPDATE, DELETE ON TABLES TO "<sp-uuid>";
ALTER DEFAULT PRIVILEGES IN SCHEMA email_to_quote GRANT USAGE, SELECT ON SEQUENCES TO "<sp-uuid>";
-- Also grant PUBLIC access for the lb_* sync tables (owned by databricks_writer)
GRANT USAGE ON SCHEMA email_to_quote TO PUBLIC;
GRANT SELECT ON ALL TABLES IN SCHEMA email_to_quote TO PUBLIC;
```

### Configure app.yaml

```yaml
env:
  - name: APP_PORT
    value: "8000"
  - name: LAKEBASE_HOST
    value: "<lakebase-endpoint-host>"
  - name: LAKEBASE_ENDPOINT
    value: "projects/$LAKEBASE_PROJECT/branches/production/endpoints/primary"
  - name: LAKEBASE_WORKSPACE_HOST
    value: "$WORKSPACE_HOST"
  - name: LAKEBASE_USER
    value: "<sp-uuid>"
  - name: LAKEBASE_PASSWORD
    value: "<sp-password>"
```

### Deploy

```bash
databricks apps create $APP_NAME --description "BricksHouse Insurance - Email to Quote Pipeline"
# Wait for compute to be ACTIVE

# Sync app files to workspace
databricks sync $PROJECT_DIR/app /Users/$USER/mail2quote/app --watch=false --full

# Deploy
databricks apps deploy $APP_NAME \
  --source-code-path /Workspace/Users/$USER/mail2quote/app --no-wait
```

## Step 12: Run the ML Pipeline

```bash
databricks jobs create --json @$PROJECT_DIR/ml_pipeline_job.json
databricks jobs run-now <job_id>
```

## Deployment Order (Critical)

1. Auth + UC catalog/schema/tables/volumes
2. Load reference data into UC
3. Upload ALL notebooks + pdf_generator.py to volume
4. Create Lakebase + schema + tables + permissions
5. Migrate data to Lakebase + generate sample emails
6. Generate test .eml files (into volume)
7. Create & start DLT pipeline
8. Create & start sync underwriter job
9. Create app + grant SP permissions + deploy
10. Run ML pipeline

## Known Gotchas

| Issue | Fix |
|-------|-----|
| `pdf_generator.py` not in volume | Pipeline PDF stage fails silently. Upload to `/Volumes/.../incoming_email/pdf_generator.py` |
| UC underwriter table has IDENTITY column | MERGE fails. Use plain `BIGINT` — ID comes from Lakebase SERIAL |
| Sync job uses `whenMatchedUpdateAll` | CDF floods with spurious updates every 5s, DLT append flows break. Use insert-only MERGE |
| App SP can't auth to Lakebase via OAuth | Serverless SDK lacks `w.postgres`. Use native PG role + password in app.yaml |
| App SP can't INSERT into underwriter | Missing `GRANT USAGE ON SEQUENCES`. Grant on `underwriter_id_seq` |
| `generate_test_emails` verification fails | Expected — it queries DLT-managed tables that don't exist yet. Emails are already written |
| Serverless notebooks can't use `spark.conf.set` custom keys | Use `dbutils.widgets` with defaults instead |
| Pipeline fails on `uw_declined_info_to_completed` | `underwriter` table missing `info_request` column. Add it to both UC and Lakebase |
