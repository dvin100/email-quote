#!/usr/bin/env python3
"""
Generate 120+ sample quote-request emails from existing UC organization data
and insert into Lakebase sample_emails table.

Reads organizations + related data from UC via SQL Statements API,
builds realistic .eml content, assigns risk_level (low/medium/high),
and writes to Lakebase PostgreSQL.
"""

import json
import os
import random
import subprocess
import tempfile
import time
import uuid
from datetime import datetime

import psycopg

random.seed(42)

# ---------------------------------------------------------------------------
# Config
# ---------------------------------------------------------------------------
PROFILE = "EMAIL_QUOTE"
UC_CATALOG = "dvin100_email_to_quote"
UC_SCHEMA = "email_to_quote"
WAREHOUSE_ID = "9d37ebbf410ea6d5"

LB_HOST = os.environ.get("LAKEBASE_HOST", "ep-bitter-term-d8cbhgar.database.us-east-2.cloud.databricks.com")
LB_DB = os.environ.get("LAKEBASE_DB", "databricks_postgres")
LB_SCHEMA = "email_to_quote"

# 12 per risk category × 10 categories = 120 base, add a few extras
ORGS_PER_CATEGORY = 12

RISK_CATEGORIES = [
    "office", "retail", "construction", "manufacturing", "healthcare",
    "technology", "food_service", "transportation", "professional_services",
    "hospitality",
]

# Coverage type display names
COV_NAMES = {
    "general_liability": "General Liability",
    "property": "Commercial Property",
    "workers_comp": "Workers Compensation",
    "commercial_auto": "Commercial Auto",
    "professional_liability": "Professional Liability / E&O",
    "cyber": "Cyber Liability",
    "umbrella": "Umbrella / Excess Liability",
    "bop": "Business Owners Policy",
    "directors_officers": "Directors & Officers",
    "employment_practices": "Employment Practices Liability",
}

# ---------------------------------------------------------------------------
# UC SQL API helper
# ---------------------------------------------------------------------------
def sql_api_query(sql: str) -> tuple[list[str], list[list]]:
    """Execute SQL via Databricks CLI + SQL Statements API, return (columns, rows)."""
    payload = {
        "warehouse_id": WAREHOUSE_ID,
        "statement": sql,
        "catalog": UC_CATALOG,
        "schema": UC_SCHEMA,
        "wait_timeout": "50s",
        "disposition": "INLINE",
        "format": "JSON_ARRAY",
    }

    with tempfile.NamedTemporaryFile(mode="w", suffix=".json", delete=False) as tmp:
        json.dump(payload, tmp)
        tmp_path = tmp.name

    result = subprocess.run(
        ["databricks", "api", "post", "/api/2.0/sql/statements",
         "--profile", PROFILE, "--json", f"@{tmp_path}"],
        capture_output=True, text=True, timeout=120,
    )
    if not result.stdout.strip():
        raise RuntimeError(f"Empty response from SQL API: {result.stderr}")

    data = json.loads(result.stdout)
    status = data.get("status", {}).get("state", "")
    stmt_id = data.get("statement_id")

    # Poll if still running
    while status in ("PENDING", "RUNNING"):
        time.sleep(2)
        poll = subprocess.run(
            ["databricks", "api", "get",
             f"/api/2.0/sql/statements/{stmt_id}",
             "--profile", PROFILE],
            capture_output=True, text=True, timeout=30,
        )
        data = json.loads(poll.stdout)
        status = data.get("status", {}).get("state", "")

    if status != "SUCCEEDED":
        err = data.get("status", {}).get("error", {}).get("message", "unknown")
        raise RuntimeError(f"SQL failed: {err}")

    columns = [c["name"] for c in data.get("manifest", {}).get("schema", {}).get("columns", [])]
    rows = data.get("result", {}).get("data_array", [])

    return columns, rows


def rows_to_dicts(columns: list[str], rows: list[list]) -> list[dict]:
    return [dict(zip(columns, row)) for row in rows]


# ---------------------------------------------------------------------------
# Risk level assignment logic
# ---------------------------------------------------------------------------
def compute_risk_level(org: dict, claims: list[dict], policies: list[dict]) -> str:
    """
    Assign low / medium / high risk based on business characteristics:
    - Claims count & severity
    - Industry inherent risk
    - Safety controls in place
    - Years in business
    - Revenue/employee ratio (proxy for operational maturity)
    """
    score = 0  # higher = riskier

    # Industry inherent risk
    high_risk_cats = {"construction", "transportation", "manufacturing"}
    med_risk_cats = {"healthcare", "food_service", "hospitality"}
    cat = org.get("risk_category", "")
    if cat in high_risk_cats:
        score += 3
    elif cat in med_risk_cats:
        score += 2
    else:
        score += 1

    # Claims history
    num_claims = len(claims)
    total_paid = sum(float(c.get("amount_paid") or 0) for c in claims)
    if num_claims >= 4 or total_paid > 200000:
        score += 3
    elif num_claims >= 2 or total_paid > 50000:
        score += 2
    elif num_claims >= 1:
        score += 1

    # Safety controls (each missing one adds risk)
    for key in ("has_written_safety_procedures", "has_employee_training_program", "has_cyber_controls"):
        val = org.get(key)
        if isinstance(val, str):
            val = val.lower() in ("true", "1", "yes")
        if not val:
            score += 1

    # Years in business (newer = riskier)
    est = org.get("date_established")
    if est:
        try:
            year = int(str(est)[:4])
            age = 2026 - year
            if age < 3:
                score += 2
            elif age < 7:
                score += 1
        except (ValueError, TypeError):
            pass

    # Subcontractors add risk
    uses_sub = org.get("uses_subcontractors")
    if isinstance(uses_sub, str):
        uses_sub = uses_sub.lower() in ("true", "1", "yes")
    if uses_sub:
        score += 1

    if score >= 6:
        return "high"
    elif score >= 4:
        return "medium"
    else:
        return "low"


# ---------------------------------------------------------------------------
# Email body & EML builders (adapted from backend)
# ---------------------------------------------------------------------------
def safe_float(v):
    try:
        return float(v) if v is not None else 0
    except (ValueError, TypeError):
        return 0

def safe_int(v):
    try:
        return int(float(v)) if v is not None else 0
    except (ValueError, TypeError):
        return 0

def safe_bool(v):
    if isinstance(v, bool):
        return v
    if isinstance(v, str):
        return v.lower() in ("true", "1", "yes")
    return False


def build_email_body(org: dict, loc: dict | None, policies: list, claims: list,
                     coverages: list, vehicles: dict | None, cyber: dict | None) -> str:
    """Build a realistic insurance quote request email body."""
    greetings = [
        "Dear Underwriting Team,", "Hello,", "Good morning,",
        "Dear Sir/Madam,", "To Whom It May Concern,",
        "Dear Insurance Team,", "Hi there,",
        "Good afternoon,", "Dear Quoting Department,",
    ]
    greeting = random.choice(greetings)

    rev_str = f"${org['annual_revenue']:,.0f}" if org.get("annual_revenue") else "N/A"
    payroll_str = f"${org['annual_payroll']:,.0f}" if org.get("annual_payroll") else "N/A"
    est_year = org.get("date_established")
    est_str = str(est_year)[:4] if est_year else "N/A"

    loc_str = f"{loc['city']}, {loc['state']}" if loc else "Multiple locations"
    body = f"""{greeting}

We are requesting a commercial insurance quote for {org['legal_name']}. Below is a summary of our operations:

Business Details:
- Legal Name: {org['legal_name']}"""

    if org.get("trading_name"):
        body += f"\n- DBA: {org['trading_name']}"

    body += f"""
- NAICS: {org.get('naics_code', 'N/A')}
- Legal Structure: {(org.get('legal_structure') or '').replace('_', ' ').title()}
- Established: {est_str}
- Location: {loc_str}
- Annual Revenue: {rev_str}
- Annual Payroll: {payroll_str}
- Employees: {org.get('num_employees', 'N/A')}"""

    if org.get("num_contractors"):
        body += f" (plus {org['num_contractors']} contractors)"
    if safe_bool(org.get("uses_subcontractors")):
        body += "\n- Uses subcontractors: Yes"

    if coverages:
        body += "\n\nCoverage Requested:"
        for i, cov in enumerate(coverages, 1):
            ctype = COV_NAMES.get(cov.get("coverage_type", ""), cov.get("coverage_type", ""))
            limit = f"${safe_float(cov.get('requested_limit')):,.0f}" if cov.get("requested_limit") else "TBD"
            ded = f"${safe_float(cov.get('requested_deductible')):,.0f}" if cov.get("requested_deductible") else "standard"
            line = f"\n{i}. {ctype}: {limit} limit, {ded} deductible"
            if cov.get("special_terms"):
                line += f" ({cov['special_terms']})"
            body += line
    elif policies:
        body += "\n\nCoverage Requested (based on current program):"
        for i, pol in enumerate(policies, 1):
            ptype = COV_NAMES.get(pol.get("policy_type", ""), pol.get("policy_type", ""))
            limit = f"${safe_float(pol.get('coverage_limit')):,.0f}" if pol.get("coverage_limit") else "TBD"
            body += f"\n{i}. {ptype}: {limit} limit"

    if vehicles:
        body += f"\n\nFleet: {vehicles['fleet_size']} vehicles ({vehicles['vehicle_types']})"

    if cyber and org.get("risk_category") in ("technology", "healthcare", "professional_services"):
        body += "\n\nCyber Profile:"
        body += f"\n- Endpoints: {cyber.get('num_endpoints', 'N/A')}"
        if safe_bool(cyber.get("stores_pii")):
            body += "\n- Stores PII: Yes"
        if safe_bool(cyber.get("stores_phi")):
            body += "\n- Stores PHI: Yes"
        if safe_bool(cyber.get("stores_payment_data")):
            body += "\n- Stores payment data: Yes"
        if cyber.get("num_records_stored"):
            body += f"\n- Records stored: ~{safe_int(cyber['num_records_stored']):,}"
        if safe_bool(cyber.get("has_mfa")):
            body += "\n- MFA enabled: Yes"
        if cyber.get("compliance_frameworks"):
            body += f"\n- Compliance: {cyber['compliance_frameworks']}"

    if claims:
        body += "\n\nLoss History (recent claims):"
        for cl in claims[:5]:
            yr = str(cl.get("claim_date", ""))[:4]
            amt = f"${safe_float(cl.get('amount_paid')):,.0f}" if cl.get("amount_paid") else "pending"
            body += f"\n- {yr}: {cl.get('description', cl.get('claim_type', ''))} - {amt} ({cl.get('status', '')})"
    else:
        body += "\n\nLoss History: Clean - no claims in the past 5 years."

    if policies:
        carrier = policies[0].get("insurer_name", "N/A")
        total_prem = sum(safe_float(p.get("annual_premium")) for p in policies)
        prem_str = f"${total_prem:,.0f}" if total_prem else "N/A"
        exp = policies[0].get("expiration_date")
        exp_str = str(exp) if exp else "N/A"
        body += f"\n\nCurrent Carrier: {carrier}, renewing {exp_str}"
        body += f"\nCurrent Premium: ~{prem_str} annually"

    safety_notes = []
    if safe_bool(org.get("has_written_safety_procedures")):
        safety_notes.append("Written safety procedures in place")
    if safe_bool(org.get("has_employee_training_program")):
        safety_notes.append("Employee training program active")
    if safe_bool(org.get("has_cyber_controls")):
        safety_notes.append("Cyber security controls implemented")
    if safety_notes:
        body += f"\n\nSafety & Risk Management:\n{'. '.join(safety_notes)}."

    closings = [
        ("Best regards,", ""),
        ("Sincerely,", ""),
        ("Thank you,", ""),
        ("Kind regards,", "Looking forward to your response."),
        ("Thanks in advance,", "Please let me know if you need any additional documentation."),
        ("Respectfully,", "We appreciate your prompt attention to this request."),
    ]
    sign, extra = random.choice(closings)
    if extra:
        body += f"\n\n{extra}"

    body += f"""

{sign}
{org.get('primary_contact_name', 'Contact')}
{org.get('primary_contact_title', '')}, {org['legal_name']}
{org.get('primary_email', '')}
{org.get('primary_phone', '')}
"""
    return body


def build_eml(org: dict, body: str) -> str:
    """Wrap email body in .eml MIME envelope."""
    now = datetime.utcnow()
    days = ["Mon", "Tue", "Wed", "Thu", "Fri", "Sat", "Sun"]
    months = ["Jan", "Feb", "Mar", "Apr", "May", "Jun", "Jul", "Aug", "Sep", "Oct", "Nov", "Dec"]
    date_str = f"{days[now.weekday()]}, {now.day} {months[now.month-1]} {now.year} {now.strftime('%H:%M:%S')} +0000"

    from_email = org.get("primary_email", "client@example.com")
    domain = from_email.split("@")[1] if "@" in from_email else "example.com"

    subjects = [
        f"Quote Request - Commercial Insurance for {org['legal_name']}",
        f"RFQ: {org.get('trading_name') or org['legal_name']} - Multi-Line Insurance Program",
        f"Insurance Quote Needed - {org['legal_name']}",
        f"Submission: {org.get('trading_name') or org['legal_name']} Commercial Lines Program",
        f"New Business Submission - {org['legal_name']}",
    ]

    return f"""From: {from_email}
To: quotes@brickshouse-insurance.com
Subject: {random.choice(subjects)}
Date: {date_str}
MIME-Version: 1.0
Content-Type: text/plain; charset="UTF-8"
Message-ID: <{uuid.uuid4()}@{domain}>

{body}"""


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------
def main():
    # ── Step 1: Query orgs from UC (12 per category, spread across revenue tiers) ──
    print("Querying organizations from UC...")
    orgs_sql = """
    WITH ranked AS (
        SELECT
            org_id, legal_name, trading_name, risk_category,
            naics_code, primary_contact_name, primary_contact_title,
            primary_email, primary_phone, num_employees, num_contractors,
            annual_revenue, annual_payroll, legal_structure,
            date_established, main_products_services, industries_served,
            has_written_safety_procedures, has_employee_training_program,
            has_cyber_controls, uses_subcontractors,
            ROW_NUMBER() OVER (
                PARTITION BY risk_category
                ORDER BY annual_revenue DESC
            ) AS rn,
            NTILE(3) OVER (
                PARTITION BY risk_category
                ORDER BY annual_revenue DESC
            ) AS revenue_tier
        FROM organizations
    ),
    selected AS (
        -- Pick 4 from each revenue tier (top, mid, bottom) per category
        SELECT *, ROW_NUMBER() OVER (
            PARTITION BY risk_category, revenue_tier
            ORDER BY RAND()
        ) AS tier_rn
        FROM ranked
    )
    SELECT * FROM selected WHERE tier_rn <= 4
    ORDER BY risk_category, annual_revenue DESC
    """
    cols, rows = sql_api_query( orgs_sql)
    orgs = rows_to_dicts(cols, rows)
    print(f"  Got {len(orgs)} organizations across {len(RISK_CATEGORIES)} categories")

    org_ids = [o["org_id"] for o in orgs]
    in_clause = "(" + ", ".join(f"'{oid}'" for oid in org_ids) + ")"

    # ── Step 2: Batch-fetch related data ──
    print("Fetching related data (locations, policies, claims, coverages, vehicles, cyber)...")

    loc_cols, loc_rows = sql_api_query(
        f"SELECT org_id, location_type, city, state, square_footage, construction_type "
        f"FROM locations WHERE org_id IN {in_clause} AND is_primary = true")
    locations = rows_to_dicts(loc_cols, loc_rows)

    pol_cols, pol_rows = sql_api_query(
        f"SELECT org_id, policy_type, insurer_name, coverage_limit, annual_premium, expiration_date "
        f"FROM policies WHERE org_id IN {in_clause} AND is_current = true")
    policies = rows_to_dicts(pol_cols, pol_rows)

    clm_cols, clm_rows = sql_api_query(
        f"SELECT c.org_id, c.claim_type, c.claim_date, c.amount_paid, c.description, c.status "
        f"FROM claims c JOIN policies p ON p.policy_id = c.policy_id "
        f"WHERE c.org_id IN {in_clause} ORDER BY c.claim_date DESC")
    claims = rows_to_dicts(clm_cols, clm_rows)

    cov_cols, cov_rows = sql_api_query(
        f"SELECT org_id, coverage_type, requested_limit, requested_deductible, special_terms, priority "
        f"FROM coverage_requests WHERE org_id IN {in_clause}")
    coverages = rows_to_dicts(cov_cols, cov_rows)

    veh_cols, veh_rows = sql_api_query(
        f"SELECT org_id, COUNT(*) as fleet_size, "
        f"CONCAT_WS(', ', COLLECT_SET(vehicle_type)) as vehicle_types "
        f"FROM vehicles WHERE org_id IN {in_clause} GROUP BY org_id")
    vehicles = rows_to_dicts(veh_cols, veh_rows)

    cyb_cols, cyb_rows = sql_api_query(
        f"SELECT org_id, num_endpoints, stores_pii, stores_phi, stores_payment_data, "
        f"num_records_stored, has_mfa, has_encryption_at_rest, compliance_frameworks "
        f"FROM cyber_profiles WHERE org_id IN {in_clause}")
    cyber = rows_to_dicts(cyb_cols, cyb_rows)

    # Index by org_id
    loc_map = {r["org_id"]: r for r in locations}
    pol_map: dict[str, list] = {}
    for p in policies:
        pol_map.setdefault(p["org_id"], []).append(p)
    claim_map: dict[str, list] = {}
    for c in claims:
        claim_map.setdefault(c["org_id"], []).append(c)
    cov_map: dict[str, list] = {}
    for c in coverages:
        cov_map.setdefault(c["org_id"], []).append(c)
    veh_map = {r["org_id"]: r for r in vehicles}
    cyber_map = {r["org_id"]: r for r in cyber}

    print(f"  Locations: {len(locations)}, Policies: {len(policies)}, "
          f"Claims: {len(claims)}, Coverages: {len(coverages)}, "
          f"Vehicles: {len(vehicles)}, Cyber: {len(cyber)}")

    # ── Step 3: Build sample emails with risk_level ──
    print("Building sample emails...")
    sample_rows = []
    for org in orgs:
        oid = org["org_id"]

        # Type conversions
        org["annual_revenue"] = safe_float(org.get("annual_revenue"))
        org["annual_payroll"] = safe_float(org.get("annual_payroll"))
        org["num_employees"] = safe_int(org.get("num_employees"))
        org["num_contractors"] = safe_int(org.get("num_contractors"))
        for bkey in ("has_written_safety_procedures", "has_employee_training_program",
                     "has_cyber_controls", "uses_subcontractors"):
            org[bkey] = safe_bool(org.get(bkey))

        # Convert vehicle fleet_size
        if oid in veh_map:
            veh_map[oid]["fleet_size"] = safe_int(veh_map[oid].get("fleet_size"))
        if oid in cyber_map:
            cyber_map[oid]["num_endpoints"] = safe_int(cyber_map[oid].get("num_endpoints"))
            cyber_map[oid]["num_records_stored"] = safe_int(cyber_map[oid].get("num_records_stored"))

        org_claims = claim_map.get(oid, [])
        org_policies = pol_map.get(oid, [])

        # Compute risk level
        risk_level = compute_risk_level(org, org_claims, org_policies)

        # Build email
        body = build_email_body(
            org, loc_map.get(oid), org_policies,
            org_claims, cov_map.get(oid, []),
            veh_map.get(oid), cyber_map.get(oid),
        )
        eml = build_eml(org, body)
        rev = org.get("annual_revenue", 0) or 0

        sample_rows.append((
            str(uuid.uuid4()),                            # id
            oid,                                          # org_id
            org["legal_name"],                            # business_name
            org.get("risk_category"),                     # risk_category
            risk_level,                                   # risk_level
            org.get("primary_contact_name"),              # sender_name
            org.get("primary_email"),                     # sender_email
            org.get("num_employees"),                     # num_employees
            rev,                                          # annual_revenue
            eml,                                          # eml_content
            body[:200],                                   # body_preview
            f"{org['legal_name']} ({(org.get('risk_category') or '').replace('_', ' ').title()}) "
            f"— ${rev:,.0f} rev, {org.get('num_employees', '?')} emp",  # label
            datetime.utcnow(),                            # created_at
        ))

    print(f"  Generated {len(sample_rows)} sample emails")

    # Distribution summary
    from collections import Counter
    cat_counts = Counter(r[3] for r in sample_rows)
    level_counts = Counter(r[4] for r in sample_rows)
    print(f"\n  By risk_category: {dict(sorted(cat_counts.items()))}")
    print(f"  By risk_level:    {dict(sorted(level_counts.items()))}")

    # ── Step 4: Create Lakebase table and insert ──
    print("\nConnecting to Lakebase (OAuth)...")
    # Get OAuth token via Databricks CLI for schema owner permissions
    token_result = subprocess.run(
        ["databricks", "auth", "token", "--profile", PROFILE],
        capture_output=True, text=True, timeout=30,
    )
    token_data = json.loads(token_result.stdout)
    oauth_token = token_data["access_token"]
    # OAuth user is david.vincent@databricks.com who owns the schema
    conn = psycopg.connect(
        host=LB_HOST,
        dbname=LB_DB,
        user="david.vincent@databricks.com",
        password=oauth_token,
        sslmode="require",
        options=f"-csearch_path={LB_SCHEMA}",
    )
    conn.autocommit = False
    cur = conn.cursor()

    print("Creating sample_emails table...")
    cur.execute("DROP TABLE IF EXISTS sample_emails CASCADE")
    cur.execute("""
        CREATE TABLE sample_emails (
            id TEXT PRIMARY KEY,
            org_id TEXT NOT NULL,
            business_name TEXT NOT NULL,
            risk_category TEXT NOT NULL,
            risk_level TEXT NOT NULL CHECK (risk_level IN ('low', 'medium', 'high')),
            sender_name TEXT,
            sender_email TEXT,
            num_employees INT,
            annual_revenue DOUBLE PRECISION,
            eml_content TEXT NOT NULL,
            body_preview TEXT,
            label TEXT,
            created_at TIMESTAMP DEFAULT NOW()
        )
    """)
    cur.execute("CREATE INDEX idx_sample_emails_risk_category ON sample_emails(risk_category)")
    cur.execute("CREATE INDEX idx_sample_emails_risk_level ON sample_emails(risk_level)")
    cur.execute("CREATE INDEX idx_sample_emails_org_id ON sample_emails(org_id)")
    conn.commit()
    print("  Table created with indexes")

    print(f"Inserting {len(sample_rows)} sample emails...")
    insert_sql = """
        INSERT INTO sample_emails
            (id, org_id, business_name, risk_category, risk_level,
             sender_name, sender_email, num_employees, annual_revenue,
             eml_content, body_preview, label, created_at)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
    """
    cur.executemany(insert_sql, sample_rows)
    conn.commit()
    print(f"  Inserted {len(sample_rows)} rows")

    # Verify
    cur.execute("SELECT COUNT(*) FROM sample_emails")
    count = cur.fetchone()[0]
    cur.execute("SELECT risk_level, COUNT(*) FROM sample_emails GROUP BY risk_level ORDER BY risk_level")
    level_dist = cur.fetchall()
    cur.execute("SELECT risk_category, COUNT(*) FROM sample_emails GROUP BY risk_category ORDER BY risk_category")
    cat_dist = cur.fetchall()

    print(f"\nVerification:")
    print(f"  Total rows: {count}")
    print(f"  By risk_level:    {dict(level_dist)}")
    print(f"  By risk_category: {dict(cat_dist)}")

    cur.close()
    conn.close()
    print("\nDone!")


if __name__ == "__main__":
    main()
