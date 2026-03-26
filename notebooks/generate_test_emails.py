# Databricks notebook source
# MAGIC %md
# MAGIC # Generate Test Emails for Ingestion Pipeline
# MAGIC
# MAGIC Generates realistic commercial insurance quote request emails and writes them
# MAGIC to the `incoming_email` volume to test the AutoLoader ingestion pipeline.

# COMMAND ----------

import random
import uuid
from datetime import datetime, timedelta

# COMMAND ----------

CATALOG = "dvin100_email_to_quote"
SCHEMA = "email_to_quote"
VOLUME_PATH = f"/Volumes/{CATALOG}/{SCHEMA}/incoming_email"

# COMMAND ----------

# MAGIC %md
# MAGIC ## Email Templates & Randomized Data

# COMMAND ----------

BUSINESSES = [
    {
        "name": "Summit Construction Group LLC",
        "dba": "Summit Builders",
        "naics": "236220 (Commercial Building Construction)",
        "category": "construction",
        "established": 2008,
        "locations": "2 (Denver CO, Colorado Springs CO)",
        "revenue": "$12,500,000",
        "payroll": "$6,200,000",
        "employees": "142 (110 FT, 32 PT)",
        "contact": "Maria Rodriguez",
        "title": "Risk Manager",
        "email": "maria.rodriguez@summitconstruction.com",
        "phone": "(303) 555-0198",
        "coverages": [
            "General Liability: $2M/$4M limits",
            "Commercial Property: $8M TIV across 2 locations",
            "Workers Compensation: Statutory limits, high-hazard classes",
            "Commercial Auto: Fleet of 28 vehicles",
            "Umbrella/Excess: $10M",
            "Builders Risk: Project-specific, up to $5M per project",
        ],
        "claims": [
            "2025: Worker fell from scaffold, $85,000 WC claim, corrective action: enhanced fall protection training",
            "2024: Subcontractor property damage, $32,000 GL claim, settled",
            "2023: Vehicle rear-end collision, $12,000 auto claim",
        ],
        "current_carrier": "National Indemnity Co",
        "current_premium": "$185,000",
        "safety_notes": "OSHA 30-hour certified supervisors on all sites. Weekly toolbox talks. Written safety manual updated annually. Drug testing program in place.",
        "special_notes": "Several contracts require additional insured endorsements and waiver of subrogation. Government contracts require $5M umbrella minimum.",
    },
    {
        "name": "Pinnacle Healthcare Associates PC",
        "dba": "Pinnacle Medical Group",
        "naics": "621111 (Offices of Physicians)",
        "category": "healthcare",
        "established": 2012,
        "locations": "4 (Atlanta GA metro area)",
        "revenue": "$8,900,000",
        "payroll": "$5,100,000",
        "employees": "68 (52 FT, 16 PT)",
        "contact": "Dr. James Chen",
        "title": "Managing Partner",
        "email": "j.chen@pinnaclehealth.com",
        "phone": "(404) 555-0267",
        "coverages": [
            "Professional Liability (Med Mal): $1M/$3M per physician",
            "General Liability: $1M/$2M limits",
            "Property: $3.2M TIV, medical equipment included",
            "Workers Compensation: Statutory limits",
            "Cyber Liability: $2M limit (store PHI/PII for 45,000 patients)",
            "Employment Practices Liability: $1M limit",
        ],
        "claims": [
            "2024: Patient slip-and-fall in waiting room, $22,000 settled",
            "2022: Med mal allegation, dismissed without payment, defense costs $45,000",
        ],
        "current_carrier": "The Doctors Company",
        "current_premium": "$210,000",
        "safety_notes": "HIPAA compliant. SOC 2 certified EHR system. MFA on all systems. Annual security audits. Incident response plan in place.",
        "special_notes": "Expanding to 5th location in Q3 2026. Need tail coverage quote for retiring physician Dr. Williams.",
    },
    {
        "name": "Pacific Freight Solutions Inc",
        "dba": "Pacific Freight",
        "naics": "484121 (General Freight Trucking, Long-Distance)",
        "category": "transportation",
        "established": 2005,
        "locations": "3 (Los Angeles CA, Phoenix AZ, Las Vegas NV)",
        "revenue": "$22,000,000",
        "payroll": "$9,800,000",
        "employees": "195 (180 FT, 15 PT)",
        "contact": "Robert Kim",
        "title": "VP Operations",
        "email": "r.kim@pacificfreight.com",
        "phone": "(213) 555-0334",
        "coverages": [
            "Commercial Auto / Trucking: Fleet of 85 trucks + 120 trailers",
            "General Liability: $2M/$4M limits",
            "Motor Truck Cargo: $500K per load",
            "Workers Compensation: Statutory limits",
            "Umbrella/Excess: $15M",
            "Property: $12M TIV (terminals and equipment)",
        ],
        "claims": [
            "2025: Jackknife accident on I-10, $220,000 property + BI claim, open",
            "2024: Cargo damage (electronics), $95,000 cargo claim",
            "2024: Driver injury during loading, $38,000 WC claim",
            "2023: Rear-end collision, $55,000 auto claim",
            "2022: Cargo theft at truck stop, $42,000 cargo claim",
        ],
        "current_carrier": "Great West Casualty",
        "current_premium": "$890,000",
        "safety_notes": "DOT compliant. ELD devices on all trucks. Dashcams fleet-wide. Monthly driver safety meetings. CSA scores within acceptable range.",
        "special_notes": "Adding 15 new trucks in 2026. Need MCS-90 endorsement. Several drivers under 25 - need confirmation on young driver surcharges.",
    },
    {
        "name": "ByteShield Cybersecurity Corp",
        "dba": "ByteShield",
        "naics": "541512 (Computer Systems Design Services)",
        "category": "technology",
        "established": 2017,
        "locations": "2 (Austin TX, Remote workforce across 12 states)",
        "revenue": "$6,500,000",
        "payroll": "$4,800,000",
        "employees": "52 (48 FT, 4 contractors)",
        "contact": "Sarah Patel",
        "title": "COO",
        "email": "s.patel@byteshield.io",
        "phone": "(512) 555-0445",
        "coverages": [
            "Professional Liability / E&O: $5M limit",
            "Cyber Liability: $5M limit",
            "General Liability: $1M/$2M limits",
            "D&O: $3M limit",
            "Employment Practices: $1M limit",
            "Property: $800K TIV (office + equipment)",
        ],
        "claims": [
            "No claims in company history",
        ],
        "current_carrier": "Hartford (first renewal)",
        "current_premium": "$78,000",
        "safety_notes": "ISO 27001 certified. SOC 2 Type II compliant. Zero-trust architecture. Penetration testing quarterly. All employees complete annual security training.",
        "special_notes": "Client contracts require $5M+ E&O limits and cyber coverage. Processing sensitive government data under FedRAMP moderate. Need contractual liability endorsement.",
    },
    {
        "name": "Golden Harvest Hospitality Group LLC",
        "dba": "Golden Harvest Hotels & Resorts",
        "naics": "721110 (Hotels and Motels)",
        "category": "hospitality",
        "established": 2001,
        "locations": "6 (Florida: Miami, Orlando, Tampa, Jacksonville, Fort Lauderdale, Key West)",
        "revenue": "$45,000,000",
        "payroll": "$18,500,000",
        "employees": "620 (410 FT, 210 PT/seasonal)",
        "contact": "Alexandra Fontaine",
        "title": "Director of Risk Management",
        "email": "a.fontaine@goldenharvest.com",
        "phone": "(305) 555-0512",
        "coverages": [
            "General Liability: $5M/$10M limits",
            "Property: $95M TIV across 6 properties (wind/flood zone exposure)",
            "Workers Compensation: Statutory limits",
            "Commercial Auto: Fleet of 18 shuttle vehicles",
            "Liquor Liability: $2M per location",
            "Umbrella/Excess: $25M",
            "Cyber: $3M (PCI-DSS data, guest PII)",
            "Employment Practices: $2M",
        ],
        "claims": [
            "2025: Guest pool injury, $150,000 GL claim, open",
            "2024: Hurricane damage (Milton), $2.1M property claim, partially settled",
            "2024: Employee slip in kitchen, $28,000 WC claim",
            "2023: Guest data breach (credit cards), $340,000 cyber + notification costs",
            "2023: Wrongful termination allegation, $65,000 EPLI settlement",
            "2022: Roof leak water damage, $180,000 property claim",
        ],
        "current_carrier": "Zurich North America",
        "current_premium": "$1,250,000",
        "safety_notes": "Dedicated risk management team. Quarterly property inspections. Cat modeling done annually with AIR. Hurricane preparedness plans at each location. PCI-DSS compliant payment systems.",
        "special_notes": "Key West property in flood zone AE. Orlando property near theme parks sees highest liability exposure. Acquiring 7th property in Naples FL - need quote to include. Current program includes $500K wind deductible.",
    },
]

GREETINGS = [
    "Dear Underwriting Team,",
    "Hello,",
    "Good morning,",
    "Dear Sir/Madam,",
    "To Whom It May Concern,",
    "Hi there,",
    "Dear Insurance Team,",
]

CLOSINGS = [
    ("Best regards,", ""),
    ("Sincerely,", ""),
    ("Thank you,", ""),
    ("Kind regards,", "Looking forward to your response."),
    ("Thanks in advance,", "Please let me know if you need any additional documentation."),
    ("Respectfully,", "We'd appreciate a quote within 2 weeks if possible."),
]

DAYS = ["Mon", "Tue", "Wed", "Thu", "Fri"]
MONTHS = ["Jan", "Feb", "Mar", "Apr", "May", "Jun", "Jul", "Aug", "Sep", "Oct", "Nov", "Dec"]

# COMMAND ----------

# MAGIC %md
# MAGIC ## Generate Emails

# COMMAND ----------

def generate_email(business):
    """Generate a realistic .eml file for a commercial insurance quote request."""
    now = datetime.now() - timedelta(minutes=random.randint(0, 120))
    day_name = DAYS[now.weekday() % 5]
    month_name = MONTHS[now.month - 1]
    date_str = f"{day_name}, {now.day} {month_name} {now.year} {now.strftime('%H:%M:%S')} -0500"

    greeting = random.choice(GREETINGS)
    closing_sign, closing_extra = random.choice(CLOSINGS)

    coverages_text = "\n".join(f"{i+1}. {c}" for i, c in enumerate(business["coverages"]))
    claims_text = "\n".join(f"- {c}" for c in business["claims"])

    body = f"""{greeting}

We are requesting a commercial insurance quote for {business["name"]}. Below is a summary of our operations:

Business Details:
- Legal Name: {business["name"]}
- DBA: {business["dba"]}
- NAICS: {business["naics"]}
- Established: {business["established"]}
- Locations: {business["locations"]}
- Annual Revenue: {business["revenue"]}
- Annual Payroll: {business["payroll"]}
- Employees: {business["employees"]}

Coverage Requested:
{coverages_text}

Loss History (5 years):
{claims_text}

Current Carrier: {business["current_carrier"]}, renewing {(now + timedelta(days=random.randint(30, 90))).strftime('%m/%d/%Y')}
Current Premium: ~{business["current_premium"]} annually

Safety & Risk Management:
{business["safety_notes"]}

Additional Notes:
{business["special_notes"]}

{closing_extra}

{closing_sign}
{business["contact"]}
{business["title"]}, {business["name"]}
{business["email"]}
{business["phone"]}
"""

    subject = random.choice([
        f"Quote Request - Commercial Insurance for {business['name']}",
        f"RFQ: {business['dba']} - Multi-Line Insurance Program",
        f"Insurance Quote Needed - {business['name']}",
        f"Submission: {business['dba']} Commercial Lines Program",
        f"New Business Submission - {business['name']}",
    ])

    eml = f"""From: {business["email"]}
To: quotes@insurer.com
Subject: {subject}
Date: {date_str}
MIME-Version: 1.0
Content-Type: text/plain; charset="UTF-8"
Message-ID: <{uuid.uuid4()}@{business["email"].split("@")[1]}>

{body}"""

    return eml

# COMMAND ----------

# MAGIC %md
# MAGIC ## Write Emails to Volume

# COMMAND ----------

num_emails = len(BUSINESSES)
generated_files = []

for i, biz in enumerate(BUSINESSES):
    eml_content = generate_email(biz)
    file_name = f"quote_request_{uuid.uuid4().hex[:8]}.eml"
    file_path = f"{VOLUME_PATH}/{file_name}"

    dbutils.fs.put(file_path, eml_content, overwrite=True)
    generated_files.append(file_name)
    print(f"  [{i+1}/{num_emails}] Written: {file_name} ({biz['dba']})")

print(f"\nGenerated {num_emails} test emails in {VOLUME_PATH}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Verify Files in Volume

# COMMAND ----------

files = dbutils.fs.ls(VOLUME_PATH)
eml_files = [f for f in files if f.name.endswith(".eml")]
print(f"Total .eml files in volume: {len(eml_files)}")
for f in eml_files:
    print(f"  {f.name:45s}  {f.size:>6} bytes")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Check Ingestion (wait for AutoLoader to pick up)

# COMMAND ----------

import time

print("Waiting 30s for AutoLoader to process new files...")
time.sleep(30)

received_count = spark.sql(f"SELECT count(*) as cnt FROM {CATALOG}.{SCHEMA}.pipe_email_received").collect()[0]["cnt"]
print(f"pipe_email_received: {received_count} rows")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Preview Received Emails

# COMMAND ----------

display(
    spark.sql(f"""
        SELECT email_id, file_name, file_size, ingestion_timestamp,
               substring(raw_content, 1, 200) as preview
        FROM {CATALOG}.{SCHEMA}.pipe_email_received
        ORDER BY ingestion_timestamp DESC
    """)
)
