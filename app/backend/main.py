"""
FastAPI backend for the Mail-to-Quote Databricks App.

Connects to a Lakebase PostgreSQL database, serves quote pipeline data,
and hosts a React SPA frontend.
"""

import logging
import os
import time
from contextlib import asynccontextmanager
from pathlib import Path
from typing import Any

import psycopg
from databricks.sdk import WorkspaceClient
from fastapi import FastAPI, HTTPException, Request
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import FileResponse, JSONResponse
from fastapi.staticfiles import StaticFiles
from psycopg_pool import ConnectionPool

logger = logging.getLogger("mail2quote")
logging.basicConfig(level=logging.INFO)

# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------
DB_HOST = os.environ.get(
    "LAKEBASE_HOST",
    "ep-bitter-term-d8cbhgar.database.us-east-2.cloud.databricks.com",
)
DB_NAME = os.environ.get("LAKEBASE_DB", "databricks_postgres")
DB_SCHEMA = os.environ.get("LAKEBASE_SCHEMA", "email_to_quote")
ENDPOINT_RESOURCE = os.environ.get(
    "LAKEBASE_ENDPOINT",
    "projects/emai2quote/branches/production/endpoints/primary",
)
# Workspace host where Lakebase lives (may differ from the App workspace)
LAKEBASE_WORKSPACE_HOST = os.environ.get(
    "LAKEBASE_WORKSPACE_HOST",
    "https://dbc-77550b22-6efb.cloud.databricks.com",
)
TOKEN_LIFETIME_SECONDS = 45 * 60  # refresh every 45 minutes

FRONTEND_DIR = Path(__file__).resolve().parent.parent / "frontend" / "dist"

# ---------------------------------------------------------------------------
# Token management
# ---------------------------------------------------------------------------
_token_cache: dict[str, Any] = {"token": None, "expires_at": 0.0}


def _get_token() -> str:
    """Return a valid Lakebase password, refreshing OAuth token if needed.

    Tries strategies in order:
    1. Native Postgres password from LAKEBASE_PASSWORD env var (no expiry)
    2. OAuth token via generate_database_credential (1-hour expiry)
    """
    # Strategy 1: Native password (preferred for Databricks Apps)
    native_pw = os.environ.get("LAKEBASE_PASSWORD")
    if native_pw:
        return native_pw

    # Strategy 2: OAuth token
    now = time.time()
    if _token_cache["token"] and now < _token_cache["expires_at"]:
        return _token_cache["token"]

    logger.info("Refreshing Databricks Lakebase OAuth token...")
    for host in [None, LAKEBASE_WORKSPACE_HOST]:
        try:
            kwargs = {"host": host} if host else {}
            w = WorkspaceClient(**kwargs)
            cred = w.postgres.generate_database_credential(ENDPOINT_RESOURCE)
            _token_cache["token"] = cred.token
            _token_cache["expires_at"] = now + TOKEN_LIFETIME_SECONDS
            logger.info("Token refreshed, valid for ~45 minutes.")
            return _token_cache["token"]
        except Exception as exc:
            logger.warning("OAuth token via %s failed: %s", host or "default", exc)
            continue

    raise RuntimeError("Cannot get Lakebase token via any strategy")


def _get_user() -> str:
    """Get the Lakebase DB username."""
    # Use env var if set, otherwise detect from SDK
    env_user = os.environ.get("LAKEBASE_USER")
    if env_user:
        return env_user
    try:
        w = WorkspaceClient()
        return w.current_user.me().user_name
    except Exception:
        return "databricks"


_db_user: str | None = None


def _conninfo() -> str:
    """Build a psycopg connection string."""
    global _db_user
    if _db_user is None:
        _db_user = _get_user()
    token = _get_token()
    return (
        f"host={DB_HOST} "
        f"dbname={DB_NAME} "
        f"user={_db_user} "
        f"password={token} "
        f"sslmode=require "
        f"options=-csearch_path={DB_SCHEMA}"
    )


# ---------------------------------------------------------------------------
# Connection pool
# ---------------------------------------------------------------------------
pool: ConnectionPool | None = None


def _create_pool() -> ConnectionPool:
    """Create a new connection pool with the current token."""
    return ConnectionPool(
        conninfo=_conninfo(),
        min_size=1,
        max_size=10,
        open=True,
    )


def _get_pool() -> ConnectionPool:
    """Return the connection pool, recreating if needed."""
    global pool
    if pool is None:
        pool = _create_pool()
    return pool


def _reset_pool() -> None:
    """Close and recreate the pool (e.g. after a token refresh)."""
    global pool
    if pool is not None:
        try:
            pool.close()
        except Exception:
            pass
    pool = _create_pool()


def _execute_query(query: str, params: tuple | None = None) -> list[dict]:
    """Execute a SELECT query with automatic retry on auth/connection errors."""
    for attempt in range(2):
        try:
            p = _get_pool()
            with p.connection() as conn:
                with conn.cursor(row_factory=psycopg.rows.dict_row) as cur:
                    cur.execute(query, params)
                    return cur.fetchall()
        except (psycopg.OperationalError, psycopg.InterfaceError) as exc:
            logger.warning("DB connection error (attempt %d): %s", attempt + 1, exc)
            if attempt == 0:
                _token_cache["token"] = None
                _token_cache["expires_at"] = 0.0
                _reset_pool()
            else:
                raise HTTPException(status_code=503, detail="Database unavailable") from exc
    return []


def _execute_write(query: str, params: tuple | None = None) -> None:
    """Execute an INSERT/UPDATE/DDL query with automatic retry."""
    for attempt in range(2):
        try:
            p = _get_pool()
            with p.connection() as conn:
                with conn.cursor() as cur:
                    cur.execute(query, params)
                conn.commit()
            return
        except (psycopg.OperationalError, psycopg.InterfaceError) as exc:
            logger.warning("DB write error (attempt %d): %s", attempt + 1, exc)
            if attempt == 0:
                _token_cache["token"] = None
                _token_cache["expires_at"] = 0.0
                _reset_pool()
            else:
                raise HTTPException(status_code=503, detail="Database unavailable") from exc


# ---------------------------------------------------------------------------
# SQL query
# ---------------------------------------------------------------------------
QUOTES_QUERY = """
SELECT
    recv.email_id,
    recv.file_name,
    recv.ingestion_timestamp,

    parsed.business_name,
    parsed.risk_category,
    parsed.sender_name,
    parsed.sender_email,
    parsed.annual_revenue,
    parsed.num_employees,
    parsed.coverages_requested,

    review.decision_tag,
    review.risk_score,
    review.risk_band,
    review.review_summary,

    creation.total_premium,
    creation.quote_number,

    pdf.pdf_path,
    pdf.pdf_status,

    completed.final_status,

    -- step booleans
    (recv.email_id IS NOT NULL)      AS step_received,
    (parsed.email_id IS NOT NULL)    AS step_parsed,
    (enriched.email_id IS NOT NULL)  AS step_enriched,
    (features.email_id IS NOT NULL)  AS step_features,
    (risk.email_id IS NOT NULL)      AS step_risk_scoring,
    (review.email_id IS NOT NULL)    AS step_quote_review,
    (creation.email_id IS NOT NULL)  AS step_quote_creation,
    (pdf.email_id IS NOT NULL)       AS step_pdf_created,
    (completed.email_id IS NOT NULL) AS step_completed

FROM lb_pipe_email_received recv
LEFT JOIN lb_pipe_email_parsed    parsed   ON parsed.email_id   = recv.email_id
LEFT JOIN lb_pipe_email_enriched  enriched ON enriched.email_id = recv.email_id
LEFT JOIN lb_pipe_quote_features  features ON features.email_id = recv.email_id
LEFT JOIN lb_pipe_quote_risk_scoring risk  ON risk.email_id     = recv.email_id
LEFT JOIN lb_pipe_quote_review    review   ON review.email_id   = recv.email_id
LEFT JOIN lb_pipe_quote_creation  creation ON creation.email_id = recv.email_id
LEFT JOIN lb_pipe_pdf_created     pdf      ON pdf.email_id      = recv.email_id
LEFT JOIN lb_pipe_completed       completed ON completed.email_id = recv.email_id
"""

QUOTES_ALL = QUOTES_QUERY + " ORDER BY recv.ingestion_timestamp DESC"
QUOTES_BY_ID = QUOTES_QUERY + " WHERE recv.email_id = %s"


def _format_quote(row: dict) -> dict:
    """Transform a raw DB row into the API response shape."""
    return {
        "email_id": row["email_id"],
        "file_name": row["file_name"],
        "ingestion_timestamp": (
            row["ingestion_timestamp"].isoformat()
            if row.get("ingestion_timestamp")
            else None
        ),
        "business_name": row.get("business_name"),
        "risk_category": row.get("risk_category"),
        "sender_name": row.get("sender_name"),
        "sender_email": row.get("sender_email"),
        "annual_revenue": row.get("annual_revenue"),
        "num_employees": row.get("num_employees"),
        "coverages_requested": row.get("coverages_requested"),
        "decision_tag": row.get("decision_tag"),
        "risk_score": row.get("risk_score"),
        "risk_band": row.get("risk_band"),
        "review_summary": row.get("review_summary"),
        "total_premium": row.get("total_premium"),
        "quote_number": row.get("quote_number"),
        "pdf_path": row.get("pdf_path"),
        "pdf_status": row.get("pdf_status"),
        "final_status": row.get("final_status"),
        "steps": {
            "received": bool(row.get("step_received")),
            "parsed": bool(row.get("step_parsed")),
            "enriched": bool(row.get("step_enriched")),
            "features": bool(row.get("step_features")),
            "risk_scoring": bool(row.get("step_risk_scoring")),
            "quote_review": bool(row.get("step_quote_review")),
            "quote_creation": bool(row.get("step_quote_creation")),
            "pdf_created": bool(row.get("step_pdf_created")),
            "completed": bool(row.get("step_completed")),
        },
    }


# ---------------------------------------------------------------------------
# Numeric/Decimal serialization helper
# ---------------------------------------------------------------------------
def _make_serializable(obj: Any) -> Any:
    """Recursively convert Decimal, datetime, date, and other non-JSON types."""
    import datetime
    import decimal

    if isinstance(obj, dict):
        return {k: _make_serializable(v) for k, v in obj.items()}
    if isinstance(obj, list):
        return [_make_serializable(i) for i in obj]
    if isinstance(obj, decimal.Decimal):
        return float(obj)
    if isinstance(obj, (datetime.datetime, datetime.date)):
        return obj.isoformat()
    return obj


# ---------------------------------------------------------------------------
# FastAPI app
# ---------------------------------------------------------------------------
UNDERWRITER_DDL = """
CREATE TABLE IF NOT EXISTS underwriter (
    id              SERIAL PRIMARY KEY,
    email_id        TEXT NOT NULL,
    decision        TEXT NOT NULL CHECK (decision IN ('uw-approved', 'uw-declined', 'uw-info')),
    surcharge_pct   DOUBLE PRECISION DEFAULT 0,
    discount_pct    DOUBLE PRECISION DEFAULT 0,
    notes           TEXT,
    info_request    TEXT,
    decided_at      TIMESTAMPTZ DEFAULT NOW(),
    decided_by      TEXT
)
"""


@asynccontextmanager
async def lifespan(app: FastAPI):
    """Startup / shutdown."""
    logger.info("Starting mail2quote backend...")
    try:
        _get_pool()
        logger.info("Database pool created.")
        # Verify underwriter table is accessible
        _execute_query("SELECT 1 FROM underwriter LIMIT 0")
        logger.info("Underwriter table ready.")
    except Exception as exc:
        logger.warning("Startup check: %s (underwriter table may need manual creation)", exc)
    yield
    logger.info("Shutting down...")
    if pool is not None:
        pool.close()


app = FastAPI(title="Mail-to-Quote", lifespan=lifespan)

# CORS
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


# ---------------------------------------------------------------------------
# API routes
# ---------------------------------------------------------------------------
@app.get("/api/quotes")
def get_quotes():
    """Return all quotes with pipeline progress."""
    rows = _execute_query(QUOTES_ALL)
    quotes = [_make_serializable(_format_quote(r)) for r in rows]
    return JSONResponse(content={"quotes": quotes})


@app.get("/api/quotes/{email_id}")
def get_quote(email_id: str):
    """Return detailed info for a single quote."""
    rows = _execute_query(QUOTES_BY_ID, (email_id,))
    if not rows:
        raise HTTPException(status_code=404, detail="Quote not found")
    return JSONResponse(content=_make_serializable(_format_quote(rows[0])))


# ---------------------------------------------------------------------------
# Step detail endpoint
# ---------------------------------------------------------------------------
STEP_TABLES = {
    "received": "lb_pipe_email_received",
    "parsed": "lb_pipe_email_parsed",
    "enriched": "lb_pipe_email_enriched",
    "features": "lb_pipe_quote_features",
    "risk_scoring": "lb_pipe_quote_risk_scoring",
    "quote_review": "lb_pipe_quote_review",
    "quote_creation": "lb_pipe_quote_creation",
    "pdf_created": "lb_pipe_pdf_created",
    "completed": "lb_pipe_completed",
}

# Columns to exclude from step detail responses (too large / internal)
_EXCLUDE_COLS = {"raw_content", "llm_response", "quote_data_json"}


@app.get("/api/quotes/{email_id}/step/{step_key}")
def get_step_detail(email_id: str, step_key: str):
    """Return all columns from a specific pipeline step table for the given email_id."""
    table = STEP_TABLES.get(step_key)
    if not table:
        raise HTTPException(status_code=400, detail=f"Unknown step: {step_key}")

    rows = _execute_query(
        f"SELECT * FROM {table} WHERE email_id = %s LIMIT 1",  # noqa: S608
        (email_id,),
    )
    if not rows:
        raise HTTPException(status_code=404, detail=f"No data for step '{step_key}'")

    row = rows[0]
    # Remove overly large columns
    filtered = {k: v for k, v in row.items() if k not in _EXCLUDE_COLS}
    return JSONResponse(content=_make_serializable(filtered))


# ---------------------------------------------------------------------------
# Underwriter review endpoints
# ---------------------------------------------------------------------------
PENDING_REVIEW_QUERY = """
SELECT
    r.email_id,
    r.ingestion_timestamp,
    p.business_name, p.business_dba, p.risk_category,
    p.sender_name, p.sender_email, p.sender_phone, p.sender_title,
    p.annual_revenue, p.annual_payroll, p.num_employees, p.num_locations,
    p.coverages_requested, p.gl_limit_requested, p.property_tiv,
    p.auto_fleet_size, p.cyber_limit_requested, p.umbrella_limit_requested,
    p.num_claims_5yr, p.total_claims_amount, p.worst_claim_description,
    p.current_carrier, p.current_premium, p.special_requirements, p.urgency,
    rev.risk_score, rev.risk_band, rev.predicted_loss_ratio,
    rev.pricing_action, rev.review_summary, rev.decision_tag,
    rev.claim_prediction, rev.scoring_method,
    rev.review_timestamp,
    uw.decision AS uw_decision, uw.decided_at AS uw_decided_at
FROM lb_pipe_email_received r
JOIN lb_pipe_email_parsed p ON p.email_id = r.email_id
JOIN lb_pipe_quote_review rev ON rev.email_id = r.email_id
LEFT JOIN underwriter uw ON uw.email_id = r.email_id
WHERE rev.decision_tag = 'pending-review'
ORDER BY r.ingestion_timestamp ASC
"""


@app.get("/api/underwriter/pending")
def get_pending_reviews():
    """Return pending-review quotes ordered oldest first."""
    rows = _execute_query(PENDING_REVIEW_QUERY)
    return JSONResponse(content=_make_serializable(rows))


from pydantic import BaseModel


class AskQuoteQuestion(BaseModel):
    email_id: str
    question: str


@app.post("/api/underwriter/ask")
def ask_quote_question(body: AskQuoteQuestion):
    """Ask an LLM a question about a specific quote using all available data."""
    import requests as _req

    # Gather all quote data from pipeline tables
    context_parts: list[str] = []
    for step_key, table in STEP_TABLES.items():
        rows = _execute_query(
            f"SELECT * FROM {table} WHERE email_id = %s LIMIT 1",  # noqa: S608
            (body.email_id,),
        )
        if rows:
            row = {k: v for k, v in rows[0].items() if k not in _EXCLUDE_COLS}
            row_str = "\n".join(f"  {k}: {v}" for k, v in _make_serializable(row).items() if v is not None)
            context_parts.append(f"[{step_key.upper()}]\n{row_str}")

    if not context_parts:
        raise HTTPException(status_code=404, detail="No data found for this quote")

    context = "\n\n".join(context_parts)

    # Call LLM via the Databricks serving endpoint
    try:
        prompt = (
            "You are an expert insurance underwriting assistant helping an underwriter "
            "review a commercial insurance quote request. You have access to all the data "
            "about this quote from the processing pipeline.\n\n"
            f"QUOTE DATA:\n{context}\n\n"
            f"UNDERWRITER QUESTION:\n{body.question}\n\n"
            "Provide a clear, concise, and helpful answer based on the quote data above. "
            "If the data doesn't contain enough information to fully answer, say so. "
            "Focus on actionable insights relevant to underwriting decisions."
        )

        import requests as _req

        # Get a proper token from the Databricks SDK
        w = WorkspaceClient()
        host = w.config.host

        # The SDK's headers() method returns the correct auth header
        # including proper token refresh for the app's SP
        headers = dict(w.config.authenticate())
        headers["Content-Type"] = "application/json"

        logger.info("LLM call: host=%s, auth_type=%s, has_auth=%s",
                     host, w.config.auth_type,
                     "Authorization" in headers or "authorization" in headers)

        resp = _req.post(
            f"{host}/serving-endpoints/databricks-claude-sonnet-4-5/invocations",
            headers=headers,
            json={"messages": [{"role": "user", "content": prompt}], "max_tokens": 1024},
            timeout=60,
        )

        if resp.status_code == 200:
            data = resp.json()
            answer = data.get("choices", [{}])[0].get("message", {}).get("content", "No response")
            return JSONResponse(content={"answer": answer})
        else:
            logger.warning("LLM call returned %s: %s", resp.status_code, resp.text[:300])
            return JSONResponse(content={"answer": f"LLM returned status {resp.status_code}: {resp.text[:150]}"})
    except Exception as exc:
        logger.warning("LLM call error: %s", exc)
        return JSONResponse(content={"answer": f"Could not reach LLM: {exc}"})


class UnderwriterDecision(BaseModel):
    email_id: str
    decision: str  # uw-approved | uw-declined | uw-info
    surcharge_pct: float = 0
    discount_pct: float = 0
    notes: str = ""
    info_request: str = ""


@app.post("/api/underwriter/decide")
def submit_decision(body: UnderwriterDecision):
    """Record an underwriter decision."""
    if body.decision not in ("uw-approved", "uw-declined", "uw-info"):
        raise HTTPException(status_code=400, detail="Invalid decision")

    # Insert decision
    _execute_write(
        """INSERT INTO underwriter (email_id, decision, surcharge_pct, discount_pct, notes, info_request)
           VALUES (%s, %s, %s, %s, %s, %s)""",
        (body.email_id, body.decision, body.surcharge_pct, body.discount_pct,
         body.notes, body.info_request),
    )

    # If requesting info, generate an outgoing email file in the volume
    if body.decision == "uw-info" and body.info_request:
        _generate_info_request_email(body.email_id, body.info_request)

    return JSONResponse(content={"status": "ok", "decision": body.decision})


def _generate_info_request_email(email_id: str, info_request: str) -> None:
    """Generate an .eml file in the outgoing_email volume requesting more info."""
    import uuid
    from datetime import datetime

    # Get original sender info
    rows = _execute_query(
        "SELECT sender_name, sender_email, business_name FROM lb_pipe_email_parsed WHERE email_id = %s",
        (email_id,),
    )
    if not rows:
        return
    sender = rows[0]
    to_email = sender.get("sender_email", "unknown@example.com")
    to_name = sender.get("sender_name", "Valued Client")
    biz_name = sender.get("business_name", "your organization")
    now = datetime.utcnow()

    eml = f"""From: underwriting@brickshouse-insurance.com
To: {to_email}
Subject: Additional Information Required - Quote for {biz_name}
Date: {now.strftime('%a, %d %b %Y %H:%M:%S +0000')}
MIME-Version: 1.0
Content-Type: text/plain; charset="UTF-8"
Message-ID: <{uuid.uuid4()}@brickshouse-insurance.com>

Dear {to_name},

Thank you for your commercial insurance quote request for {biz_name}.

After reviewing your submission, our underwriting team requires additional information before we can proceed:

{info_request}

Please reply to this email with the requested information at your earliest convenience. Once received, we will continue processing your quote.

Best regards,
BricksHouse Insurance Underwriting Team
underwriting@brickshouse-insurance.com
"""

    # Write to outgoing_email volume via the Databricks workspace files API
    try:
        import requests as _req
        w = WorkspaceClient()
        token = w.config.token
        host = w.config.host
        vol_path = f"/Volumes/dvin100_email_to_quote/email_to_quote/outgoing_email"
        file_name = f"info_request_{email_id[:8]}_{now.strftime('%Y%m%d%H%M%S')}.eml"
        upload_url = f"{host}/api/2.0/fs/files{vol_path}/{file_name}"
        resp = _req.put(
            upload_url,
            headers={"Authorization": f"Bearer {token}", "Content-Type": "application/octet-stream"},
            data=eml.encode("utf-8"),
            timeout=15,
        )
        if resp.status_code in (200, 201):
            logger.info("Outgoing email written: %s/%s", vol_path, file_name)
        else:
            logger.warning("Failed to write outgoing email: %s %s", resp.status_code, resp.text[:200])
    except Exception as exc:
        logger.warning("Could not write outgoing email: %s", exc)


# ---------------------------------------------------------------------------
# PDF file proxy (serves PDFs from Databricks volumes)
# ---------------------------------------------------------------------------
from fastapi.responses import Response


@app.get("/api/pdf/{quote_number}")
def get_pdf(quote_number: str):
    """Serve a PDF quote file from the Databricks volume."""
    # Validate quote_number format to prevent path traversal
    if not quote_number.startswith("QT-") or ".." in quote_number:
        raise HTTPException(status_code=400, detail="Invalid quote number")

    file_name = f"{quote_number}.pdf"
    vol_path = f"/Volumes/dvin100_email_to_quote/email_to_quote/quote_documents/{file_name}"

    try:
        # Read the PDF using the Databricks SDK files API
        w = WorkspaceClient()
        resp = w.files.download(vol_path)
        content = resp.contents.read()

        return Response(
            content=content,
            media_type="application/pdf",
            headers={"Content-Disposition": f'inline; filename="{file_name}"'},
        )
    except Exception as exc:
        logger.warning("PDF download error for %s: %s", quote_number, exc)
        raise HTTPException(status_code=404, detail="PDF not found") from exc


# ---------------------------------------------------------------------------
# Static files & SPA fallback
# ---------------------------------------------------------------------------
if FRONTEND_DIR.is_dir():
    # Serve static assets (JS, CSS, images, etc.)
    app.mount(
        "/assets",
        StaticFiles(directory=str(FRONTEND_DIR / "assets")),
        name="assets",
    )

    @app.get("/{full_path:path}")
    async def serve_spa(request: Request, full_path: str):
        """Serve the React SPA. Fall back to index.html for client-side routing."""
        file_path = FRONTEND_DIR / full_path
        if file_path.is_file():
            return FileResponse(str(file_path))
        index = FRONTEND_DIR / "index.html"
        if index.is_file():
            return FileResponse(str(index))
        raise HTTPException(status_code=404, detail="Frontend not found")
else:
    logger.warning("Frontend dist directory not found at %s", FRONTEND_DIR)


# ---------------------------------------------------------------------------
# Entrypoint
# ---------------------------------------------------------------------------
if __name__ == "__main__":
    import uvicorn

    port = int(os.environ.get("APP_PORT", "8000"))
    uvicorn.run("main:app", host="0.0.0.0", port=port, reload=True)
