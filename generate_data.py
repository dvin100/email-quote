#!/usr/bin/env python3
"""
Generate synthetic insurance business data for 10,000 organizations.
Uses only stdlib - no Faker needed. Outputs SQL INSERT files for batch loading.
"""
import random
import uuid
import json
import os
import string
from datetime import datetime, date, timedelta

random.seed(42)

CATALOG = "dvin100_email_to_quote"
SCHEMA = "email_to_quote"
OUTPUT_DIR = "/Users/david.vincent/vibe/mail2quote/data"
os.makedirs(OUTPUT_DIR, exist_ok=True)

NUM_ORGS = 10000

# ============================================================
# Reference data for realistic generation
# ============================================================

LEGAL_STRUCTURES = ["sole_trader", "partnership", "corporation", "llc", "non_profit"]
LEGAL_STRUCTURE_WEIGHTS = [15, 10, 35, 35, 5]

RISK_CATEGORIES = [
    "office", "retail", "construction", "manufacturing", "healthcare",
    "technology", "food_service", "transportation", "professional_services", "hospitality"
]
RISK_CAT_WEIGHTS = [15, 15, 10, 10, 8, 12, 8, 7, 10, 5]

# Business name components
PREFIXES = ["Advanced", "Premier", "Pacific", "Atlantic", "National", "United", "Global",
            "American", "Western", "Eastern", "Northern", "Southern", "Central", "Metro",
            "Summit", "Apex", "Peak", "Valley", "Horizon", "Liberty", "Eagle", "Pioneer",
            "Golden", "Silver", "Blue", "Green", "Red", "First", "Next", "Pro", "Elite",
            "Superior", "Precision", "Integrated", "Dynamic", "Strategic", "Creative",
            "Innovative", "Modern", "Classic", "Legacy", "Heritage", "Frontier", "Coastal",
            "Mountain", "Prairie", "Delta", "Omega", "Alpha", "Pinnacle", "Catalyst"]

INDUSTRY_WORDS = {
    "office": ["Solutions", "Consulting", "Services", "Associates", "Group", "Partners", "Advisors", "Management"],
    "retail": ["Goods", "Supply", "Mart", "Market", "Store", "Outlet", "Trading", "Retail"],
    "construction": ["Construction", "Builders", "Contractors", "Development", "Infrastructure", "Building", "Structural"],
    "manufacturing": ["Manufacturing", "Industries", "Fabrication", "Products", "Production", "Works", "Forge"],
    "healthcare": ["Medical", "Health", "Clinic", "Care", "Wellness", "Therapeutics", "Diagnostics", "Pharma"],
    "technology": ["Technologies", "Tech", "Systems", "Software", "Digital", "Data", "Computing", "Networks"],
    "food_service": ["Foods", "Catering", "Kitchen", "Dining", "Bistro", "Restaurants", "Culinary", "Provisions"],
    "transportation": ["Transport", "Logistics", "Freight", "Shipping", "Hauling", "Carriers", "Express", "Delivery"],
    "professional_services": ["Legal", "Accounting", "Advisory", "Consulting", "Professional", "Fiduciary", "Analytics"],
    "hospitality": ["Hospitality", "Hotels", "Resorts", "Lodging", "Inn", "Suites", "Accommodations", "Venues"]
}

NAICS_CODES = {
    "office": ["541110", "541211", "541219", "561110", "561320"],
    "retail": ["441110", "442110", "443142", "444110", "445110", "448110", "452210"],
    "construction": ["236115", "236116", "236210", "236220", "237110", "237310", "238110", "238210"],
    "manufacturing": ["311411", "321113", "325611", "326199", "332710", "333249", "334118", "336111"],
    "healthcare": ["621111", "621210", "621310", "621399", "621491", "622110", "623110", "624110"],
    "technology": ["511210", "518210", "519130", "541511", "541512", "541519", "541715"],
    "food_service": ["311811", "311812", "722310", "722320", "722330", "722511", "722513", "722515"],
    "transportation": ["484110", "484121", "484122", "484210", "484220", "484230", "485310", "488510"],
    "professional_services": ["541110", "541211", "541310", "541330", "541380", "541611", "541612", "541613"],
    "hospitality": ["721110", "721120", "721191", "721199", "721211", "721214", "722511", "713110"]
}

SIC_CODES = {
    "office": ["7371", "7372", "7374", "7389", "8111", "8721"],
    "retail": ["5211", "5311", "5411", "5541", "5651", "5712", "5731"],
    "construction": ["1521", "1522", "1541", "1542", "1611", "1622", "1711", "1721"],
    "manufacturing": ["2011", "2051", "2411", "2821", "3312", "3442", "3559", "3599"],
    "healthcare": ["8011", "8021", "8031", "8041", "8042", "8049", "8062", "8071"],
    "technology": ["3572", "3577", "3661", "3672", "3674", "3679", "7371", "7372"],
    "food_service": ["2051", "2099", "5812", "5812", "5813", "5812", "5461"],
    "transportation": ["4011", "4212", "4213", "4214", "4215", "4222", "4231", "4731"],
    "professional_services": ["8111", "8721", "8712", "8713", "8742", "8748", "8999"],
    "hospitality": ["7011", "7021", "7032", "7033", "7041", "7211", "7941"]
}

PRODUCTS_SERVICES = {
    "office": ["business consulting", "accounting services", "legal services", "office administration", "HR management", "financial planning", "marketing services", "IT support"],
    "retail": ["consumer electronics", "clothing and apparel", "home furnishings", "auto parts", "grocery items", "sporting goods", "hardware supplies", "specialty gifts"],
    "construction": ["residential building", "commercial construction", "electrical work", "plumbing services", "HVAC installation", "roofing", "concrete work", "site preparation"],
    "manufacturing": ["metal fabrication", "plastic molding", "food processing", "textile production", "chemical manufacturing", "electronics assembly", "furniture making", "auto parts"],
    "healthcare": ["primary care", "dental services", "physical therapy", "mental health counseling", "lab testing", "imaging services", "pharmacy", "home health care"],
    "technology": ["software development", "cloud computing", "cybersecurity", "data analytics", "mobile applications", "AI/ML solutions", "network infrastructure", "SaaS platforms"],
    "food_service": ["restaurant dining", "catering services", "food truck operations", "bakery products", "beverage manufacturing", "meal prep delivery", "event catering", "commissary kitchen"],
    "transportation": ["long-haul trucking", "local delivery", "freight brokerage", "warehouse logistics", "moving services", "courier services", "fleet management", "supply chain"],
    "professional_services": ["legal representation", "tax preparation", "management consulting", "engineering design", "architecture", "market research", "public relations", "recruitment"],
    "hospitality": ["hotel accommodation", "event hosting", "resort services", "conference facilities", "banquet services", "concierge services", "tourism packages", "spa services"]
}

INDUSTRIES_SERVED = {
    "office": ["finance", "real estate", "insurance", "government", "education"],
    "retail": ["consumers", "small business", "contractors", "restaurants", "schools"],
    "construction": ["residential", "commercial", "government", "industrial", "institutional"],
    "manufacturing": ["automotive", "aerospace", "consumer goods", "electronics", "defense"],
    "healthcare": ["individuals", "families", "elderly care", "pediatrics", "sports medicine"],
    "technology": ["enterprise", "SMB", "government", "education", "healthcare"],
    "food_service": ["consumers", "corporate events", "weddings", "schools", "hospitals"],
    "transportation": ["retail", "manufacturing", "e-commerce", "agriculture", "construction"],
    "professional_services": ["corporate", "individual", "government", "non-profit", "startups"],
    "hospitality": ["corporate travelers", "leisure travelers", "event planners", "tour groups", "families"]
}

CONTACT_TITLES = ["Owner", "CEO", "President", "Managing Director", "General Manager",
                   "CFO", "COO", "VP Operations", "Risk Manager", "Office Manager"]

US_STATES = ["AL","AK","AZ","AR","CA","CO","CT","DE","FL","GA","HI","ID","IL","IN","IA",
             "KS","KY","LA","ME","MD","MA","MI","MN","MS","MO","MT","NE","NV","NH","NJ",
             "NM","NY","NC","ND","OH","OK","OR","PA","RI","SC","SD","TN","TX","UT","VT",
             "VA","WA","WV","WI","WY"]

CITIES = {
    "CA": ["Los Angeles", "San Francisco", "San Diego", "Sacramento", "San Jose", "Oakland", "Fresno", "Long Beach"],
    "TX": ["Houston", "Dallas", "Austin", "San Antonio", "Fort Worth", "El Paso", "Plano", "Arlington"],
    "NY": ["New York", "Buffalo", "Rochester", "Albany", "Syracuse", "Yonkers", "White Plains"],
    "FL": ["Miami", "Tampa", "Orlando", "Jacksonville", "St Petersburg", "Fort Lauderdale", "Tallahassee"],
    "IL": ["Chicago", "Springfield", "Naperville", "Joliet", "Rockford", "Aurora", "Peoria"],
    "PA": ["Philadelphia", "Pittsburgh", "Allentown", "Harrisburg", "Erie", "Reading", "Scranton"],
    "OH": ["Columbus", "Cleveland", "Cincinnati", "Toledo", "Akron", "Dayton"],
    "GA": ["Atlanta", "Savannah", "Augusta", "Athens", "Macon", "Roswell"],
    "NC": ["Charlotte", "Raleigh", "Durham", "Greensboro", "Winston-Salem", "Fayetteville"],
    "WA": ["Seattle", "Tacoma", "Spokane", "Vancouver", "Bellevue", "Olympia"],
    "MA": ["Boston", "Worcester", "Springfield", "Cambridge", "Lowell", "Quincy"],
    "CO": ["Denver", "Colorado Springs", "Aurora", "Fort Collins", "Boulder", "Lakewood"],
    "AZ": ["Phoenix", "Tucson", "Mesa", "Chandler", "Scottsdale", "Tempe"],
    "MI": ["Detroit", "Grand Rapids", "Ann Arbor", "Lansing", "Flint", "Kalamazoo"],
    "NJ": ["Newark", "Jersey City", "Trenton", "Princeton", "Camden", "Edison"],
}
DEFAULT_CITIES = ["Springfield", "Riverside", "Fairview", "Madison", "Georgetown", "Franklin", "Clinton", "Salem"]

STREET_NAMES = ["Main St", "Oak Ave", "Maple Dr", "Cedar Ln", "Pine St", "Elm St", "Washington Blvd",
                "Park Ave", "Lake Rd", "Hill St", "River Rd", "Market St", "Church St", "Mill Rd",
                "Industrial Blvd", "Commerce Dr", "Technology Way", "Innovation Pkwy", "Enterprise Ave",
                "Business Loop", "Corporate Dr", "Trade Center Blvd", "Harbor Dr", "Airport Rd"]

CONSTRUCTION_TYPES = ["frame", "masonry", "fire_resistive", "non_combustible", "modified_fire_resistive"]
LOCATION_TYPES = ["headquarters", "branch", "warehouse", "remote_site", "retail_store"]

JOB_CLASSIFICATIONS = ["office_clerical", "sales", "management", "skilled_labor", "unskilled_labor",
                        "professional", "driver", "healthcare_worker"]
DEPARTMENTS = ["Administration", "Sales", "Operations", "Engineering", "Finance", "Marketing",
               "Human Resources", "IT", "Customer Service", "Warehouse", "Production", "R&D"]

ASSET_TYPES = ["building", "equipment", "machinery", "it_hardware", "stock_inventory", "furniture", "signage", "tenant_improvements"]
ASSET_CONDITIONS = ["excellent", "good", "fair", "poor"]

VEHICLE_MAKES = {"sedan": [("Toyota", "Camry"), ("Honda", "Accord"), ("Ford", "Fusion"), ("Chevrolet", "Malibu"), ("Nissan", "Altima")],
                  "suv": [("Toyota", "RAV4"), ("Ford", "Explorer"), ("Chevrolet", "Equinox"), ("Honda", "CR-V"), ("Jeep", "Grand Cherokee")],
                  "pickup": [("Ford", "F-150"), ("Chevrolet", "Silverado"), ("RAM", "1500"), ("Toyota", "Tacoma"), ("GMC", "Sierra")],
                  "van": [("Ford", "Transit"), ("Mercedes", "Sprinter"), ("Chevrolet", "Express"), ("RAM", "ProMaster"), ("Nissan", "NV")],
                  "box_truck": [("Isuzu", "NPR"), ("Ford", "E-450"), ("Hino", "195"), ("Freightliner", "M2"), ("Chevrolet", "4500")],
                  "semi": [("Freightliner", "Cascadia"), ("Kenworth", "T680"), ("Peterbilt", "579"), ("Volvo", "VNL"), ("International", "LT")],
                  "specialty": [("Caterpillar", "320"), ("John Deere", "310L"), ("Bobcat", "S650"), ("Case", "580"), ("Kubota", "U55")]}
VEHICLE_TYPES = list(VEHICLE_MAKES.keys())
USAGE_TYPES = ["business_use", "commercial", "delivery", "passenger", "hauling"]

INSURERS = ["State Farm", "Travelers", "Liberty Mutual", "Hartford", "Chubb", "Zurich",
            "AIG", "CNA", "Nationwide", "Progressive Commercial", "Berkshire Hathaway",
            "Hiscox", "Markel", "Employers Holdings", "AmTrust", "USLI", "Berk Insurance"]

POLICY_TYPES = ["general_liability", "property", "workers_comp", "commercial_auto",
                "professional_liability", "cyber", "umbrella", "bop", "directors_officers",
                "employment_practices"]

CLAIM_TYPES = ["property_damage", "bodily_injury", "theft", "fire", "water_damage",
               "auto_collision", "workers_comp_injury", "professional_error", "cyber_breach",
               "slip_and_fall", "product_liability"]

CLAIM_CAUSES = {
    "property_damage": ["storm damage to roof", "vandalism to storefront", "tree fell on building", "pipe burst", "vehicle struck building"],
    "bodily_injury": ["customer slip on wet floor", "falling merchandise", "parking lot fall", "delivery accident", "equipment malfunction"],
    "theft": ["burglary after hours", "employee theft of inventory", "shoplifting loss", "vehicle break-in", "copper wire theft"],
    "fire": ["electrical fire in server room", "kitchen grease fire", "welding spark ignition", "HVAC malfunction", "arson"],
    "water_damage": ["burst pipe in winter", "roof leak during storm", "sprinkler malfunction", "flood damage", "sewage backup"],
    "auto_collision": ["rear-end collision on delivery", "parking lot accident", "intersection collision", "backing accident", "highway incident"],
    "workers_comp_injury": ["back strain lifting boxes", "repetitive stress injury", "fall from ladder", "machinery accident", "chemical exposure"],
    "professional_error": ["missed filing deadline", "calculation error in report", "design specification error", "incorrect advice given", "data entry mistake"],
    "cyber_breach": ["phishing email compromised credentials", "ransomware attack", "data breach of customer records", "DDoS attack on services", "insider data theft"],
    "slip_and_fall": ["icy parking lot fall", "wet floor in restroom", "uneven sidewalk trip", "stairwell fall", "debris on walkway"],
    "product_liability": ["defective product caused injury", "contaminated food product", "product recall costs", "failure to warn", "design defect claim"]
}

CORRECTIVE_ACTIONS = [
    "Installed additional security cameras", "Updated safety training program", "Replaced aging equipment",
    "Implemented new safety protocols", "Upgraded fire suppression system", "Added slip-resistant flooring",
    "Increased cybersecurity training", "Hired dedicated safety officer", "Installed better lighting",
    "Added warning signage", "Improved ventilation system", "Upgraded electrical wiring",
    "Implemented two-person verification", "Added guardrails and barriers", "Updated employee handbook",
    "Installed backup power system", "Improved drainage system", "Added driver training program"
]

CLOUD_PROVIDERS = ["aws", "azure", "gcp"]
COMPLIANCE_FRAMEWORKS = ["soc2", "hipaa", "pci_dss", "gdpr", "iso27001"]

CONTRACT_TYPES = ["service_agreement", "master_service", "subcontract", "lease", "vendor", "government"]
INDEMNITY_TYPES = ["mutual", "one_way_client", "one_way_vendor", "none"]

CLIENT_INDUSTRIES = ["finance", "healthcare", "technology", "retail", "manufacturing",
                     "government", "education", "real_estate", "energy", "telecommunications"]

FIRST_NAMES = ["James", "Mary", "Robert", "Patricia", "John", "Jennifer", "Michael", "Linda",
               "David", "Elizabeth", "William", "Barbara", "Richard", "Susan", "Joseph", "Jessica",
               "Thomas", "Sarah", "Christopher", "Karen", "Charles", "Lisa", "Daniel", "Nancy",
               "Matthew", "Betty", "Anthony", "Margaret", "Mark", "Sandra", "Donald", "Ashley",
               "Steven", "Dorothy", "Andrew", "Kimberly", "Paul", "Emily", "Joshua", "Donna",
               "Kenneth", "Michelle", "Kevin", "Carol", "Brian", "Amanda", "George", "Melissa",
               "Timothy", "Deborah", "Ronald", "Stephanie", "Edward", "Rebecca", "Jason", "Sharon",
               "Jeffrey", "Laura", "Ryan", "Cynthia", "Jacob", "Kathleen", "Gary", "Amy",
               "Nicholas", "Angela", "Eric", "Shirley", "Jonathan", "Anna", "Stephen", "Brenda"]

LAST_NAMES = ["Smith", "Johnson", "Williams", "Brown", "Jones", "Garcia", "Miller", "Davis",
              "Rodriguez", "Martinez", "Hernandez", "Lopez", "Gonzalez", "Wilson", "Anderson",
              "Thomas", "Taylor", "Moore", "Jackson", "Martin", "Lee", "Perez", "Thompson",
              "White", "Harris", "Sanchez", "Clark", "Ramirez", "Lewis", "Robinson", "Walker",
              "Young", "Allen", "King", "Wright", "Scott", "Torres", "Nguyen", "Hill", "Flores",
              "Green", "Adams", "Nelson", "Baker", "Hall", "Rivera", "Campbell", "Mitchell",
              "Carter", "Roberts", "Gomez", "Phillips", "Evans", "Turner", "Diaz", "Parker",
              "Cruz", "Edwards", "Collins", "Reyes", "Stewart", "Morris", "Morales", "Murphy"]

# ============================================================
# Helper functions
# ============================================================

def uid():
    return str(uuid.uuid4())

def rand_date(start_year, end_year):
    start = date(start_year, 1, 1)
    end = date(end_year, 12, 31)
    delta = (end - start).days
    return start + timedelta(days=random.randint(0, delta))

def rand_phone():
    return f"({random.randint(200,999)}) {random.randint(200,999)}-{random.randint(1000,9999)}"

def rand_zip():
    return f"{random.randint(10000,99999)}"

def rand_city(state):
    if state in CITIES:
        return random.choice(CITIES[state])
    return random.choice(DEFAULT_CITIES)

def rand_vin():
    chars = string.ascii_uppercase.replace('I','').replace('O','').replace('Q','') + string.digits
    return ''.join(random.choices(chars, k=17))

def sql_str(val):
    if val is None:
        return "NULL"
    if isinstance(val, bool):
        return "true" if val else "false"
    if isinstance(val, (int, float)):
        return str(val)
    if isinstance(val, (date, datetime)):
        return f"'{val}'"
    # Escape single quotes
    return "'" + str(val).replace("'", "''") + "'"

def sql_values(row):
    return "(" + ", ".join(sql_str(v) for v in row) + ")"


# ============================================================
# Generate data
# ============================================================

print("Generating organizations...")

orgs = []
org_meta = {}  # org_id -> metadata for child table generation

for i in range(NUM_ORGS):
    org_id = uid()
    risk_cat = random.choices(RISK_CATEGORIES, weights=RISK_CAT_WEIGHTS, k=1)[0]
    legal_struct = random.choices(LEGAL_STRUCTURES, weights=LEGAL_STRUCTURE_WEIGHTS, k=1)[0]

    prefix = random.choice(PREFIXES)
    suffix = random.choice(INDUSTRY_WORDS[risk_cat])
    legal_name = f"{prefix} {suffix}"
    # ~40% have a different trading name
    trading_name = f"{prefix} {random.choice(INDUSTRY_WORDS[risk_cat])}" if random.random() < 0.4 else None

    date_est = rand_date(1950, 2023)
    years_ownership = min(random.randint(1, 35), (date(2026,1,1) - date_est).days // 365)

    state = random.choice(US_STATES)
    city = rand_city(state)

    first = random.choice(FIRST_NAMES)
    last = random.choice(LAST_NAMES)
    contact_name = f"{first} {last}"
    domain = legal_name.lower().replace(" ", "").replace("'", "")[:20]
    email = f"{first.lower()}.{last.lower()}@{domain}.com"
    website = f"www.{domain}.com"

    # Size varies by structure
    if legal_struct == "sole_trader":
        num_emp = random.randint(1, 10)
    elif legal_struct == "partnership":
        num_emp = random.randint(2, 50)
    elif legal_struct in ("corporation", "llc"):
        num_emp = random.choices([random.randint(5, 50), random.randint(50, 500), random.randint(500, 5000)],
                                  weights=[50, 35, 15], k=1)[0]
    else:  # non_profit
        num_emp = random.randint(3, 200)

    num_contractors = int(num_emp * random.uniform(0, 0.4)) if random.random() < 0.5 else 0
    uses_sub = risk_cat in ("construction", "technology", "professional_services") and random.random() < 0.6

    # Revenue scales with employees
    rev_per_emp = random.uniform(50000, 500000) if risk_cat in ("technology", "professional_services") else random.uniform(30000, 200000)
    annual_rev = round(num_emp * rev_per_emp, 2)
    annual_payroll = round(annual_rev * random.uniform(0.2, 0.6), 2)

    products = ", ".join(random.sample(PRODUCTS_SERVICES[risk_cat], k=random.randint(2, 4)))
    industries = ", ".join(random.sample(INDUSTRIES_SERVED[risk_cat], k=random.randint(1, 3)))

    naics = random.choice(NAICS_CODES[risk_cat])
    sic = random.choice(SIC_CODES[risk_cat])

    desc_templates = [
        f"We are a {legal_struct.replace('_',' ')} specializing in {products.split(',')[0].strip()} for the {industries.split(',')[0].strip()} sector.",
        f"{legal_name} provides {products.split(',')[0].strip()} and {products.split(',')[1].strip() if ',' in products else 'related services'} to clients in {industries}.",
        f"Established in {date_est.year}, we offer {products} to diverse clientele across {industries}.",
        f"A {risk_cat.replace('_',' ')} business providing {products.split(',')[0].strip()} with {num_emp} dedicated team members.",
    ]
    business_desc = random.choice(desc_templates)

    has_safety = risk_cat in ("construction", "manufacturing", "healthcare", "transportation") or random.random() < 0.4
    has_training = has_safety or random.random() < 0.3
    has_cyber = risk_cat in ("technology", "healthcare") or random.random() < 0.35

    now = datetime(2026, 3, 25, random.randint(0,23), random.randint(0,59), random.randint(0,59))

    orgs.append([
        org_id, legal_name, trading_name, legal_struct, date_est, years_ownership,
        rand_phone(), email, website, contact_name, random.choice(CONTACT_TITLES),
        naics, sic, business_desc, products, industries, risk_cat,
        num_emp, num_contractors, uses_sub, annual_rev, annual_payroll,
        has_safety, has_training, has_cyber, now
    ])

    org_meta[org_id] = {
        "risk_cat": risk_cat, "num_emp": num_emp, "annual_rev": annual_rev,
        "annual_payroll": annual_payroll, "state": state, "city": city,
        "legal_struct": legal_struct, "date_est": date_est, "has_cyber": has_cyber
    }

print(f"  Generated {len(orgs)} organizations")

# ============================================================
# LOCATIONS
# ============================================================
print("Generating locations...")
locations = []
org_locations = {}  # org_id -> [location_ids]

for org_id, meta in org_meta.items():
    num_locs = 1
    if meta["num_emp"] > 50: num_locs = random.randint(1, 3)
    if meta["num_emp"] > 200: num_locs = random.randint(2, 5)
    if meta["num_emp"] > 1000: num_locs = random.randint(3, 8)

    loc_ids = []
    for j in range(num_locs):
        loc_id = uid()
        loc_ids.append(loc_id)
        loc_type = "headquarters" if j == 0 else random.choice(LOCATION_TYPES[1:])
        st = meta["state"] if j == 0 else random.choice(US_STATES)
        ct = meta["city"] if j == 0 else rand_city(st)

        sqft_ranges = {
            "office": (500, 20000), "retail": (800, 15000), "construction": (2000, 50000),
            "manufacturing": (5000, 100000), "healthcare": (1000, 30000), "technology": (1000, 25000),
            "food_service": (500, 10000), "transportation": (3000, 80000),
            "professional_services": (500, 15000), "hospitality": (2000, 50000)
        }
        sqft_min, sqft_max = sqft_ranges.get(meta["risk_cat"], (500, 20000))
        sqft = random.randint(sqft_min, sqft_max)

        bldg_age = random.randint(1, 80)
        const_type = random.choice(CONSTRUCTION_TYPES)
        stories = random.randint(1, 3) if sqft < 10000 else random.randint(1, 10)

        has_sprinkler = random.random() < 0.6
        has_alarm = random.random() < 0.7
        has_security = random.random() < 0.5
        has_cctv = random.random() < 0.4
        dist_fire = round(random.uniform(0.5, 15.0), 1)
        fire_class = random.randint(1, 10)

        locations.append([
            loc_id, org_id, loc_type,
            f"{random.randint(100,9999)} {random.choice(STREET_NAMES)}",
            f"Suite {random.randint(100,999)}" if random.random() < 0.3 else None,
            ct, st, rand_zip(), "US", sqft, bldg_age, const_type, stories,
            has_sprinkler, has_alarm, has_security, has_cctv,
            dist_fire, fire_class, j == 0,
            datetime(2026, 3, 25, random.randint(0,23), random.randint(0,59), random.randint(0,59))
        ])
    org_locations[org_id] = loc_ids

print(f"  Generated {len(locations)} locations")

# ============================================================
# EMPLOYEES
# ============================================================
print("Generating employees...")
employees = []

for org_id, meta in org_meta.items():
    # 2-6 job classifications per org
    num_classes = min(random.randint(2, 6), len(JOB_CLASSIFICATIONS))
    classifications = random.sample(JOB_CLASSIFICATIONS, k=num_classes)
    remaining_emp = meta["num_emp"]
    remaining_payroll = meta["annual_payroll"]

    for k, jc in enumerate(classifications):
        if k == len(classifications) - 1:
            n_emp = remaining_emp
            payroll = remaining_payroll
        else:
            n_emp = max(1, int(remaining_emp * random.uniform(0.1, 0.5)))
            payroll = round(remaining_payroll * random.uniform(0.1, 0.5), 2)
            remaining_emp -= n_emp
            remaining_payroll -= payroll

        dept = random.choice(DEPARTMENTS)
        wc_code = f"{random.randint(1000,9999)}"
        is_remote = random.random() < 0.2 if jc in ("office_clerical", "professional", "sales") else False

        employees.append([
            uid(), org_id, jc, dept, max(1, n_emp), max(0, round(payroll, 2)),
            wc_code, is_remote,
            datetime(2026, 3, 25, random.randint(0,23), random.randint(0,59), random.randint(0,59))
        ])

print(f"  Generated {len(employees)} employee records")

# ============================================================
# PROPERTY ASSETS
# ============================================================
print("Generating property assets...")
assets = []

for org_id, meta in org_meta.items():
    loc_ids = org_locations[org_id]
    num_assets = random.randint(2, 8)

    for _ in range(num_assets):
        loc_id = random.choice(loc_ids)
        atype = random.choice(ASSET_TYPES)
        year_acq = random.randint(max(2000, meta["date_est"].year), 2025)

        cost_ranges = {
            "building": (100000, 5000000), "equipment": (5000, 500000), "machinery": (10000, 2000000),
            "it_hardware": (1000, 200000), "stock_inventory": (5000, 1000000), "furniture": (2000, 100000),
            "signage": (500, 20000), "tenant_improvements": (10000, 500000)
        }
        cmin, cmax = cost_ranges.get(atype, (1000, 100000))
        orig_cost = round(random.uniform(cmin, cmax), 2)
        age_factor = max(0.1, 1 - (2026 - year_acq) * 0.05)
        cur_val = round(orig_cost * age_factor, 2)
        repl_cost = round(orig_cost * random.uniform(1.1, 1.8), 2)

        desc_map = {
            "building": "Commercial building", "equipment": "Business equipment",
            "machinery": "Industrial machinery", "it_hardware": "IT systems and hardware",
            "stock_inventory": "Inventory and stock", "furniture": "Office furniture and fixtures",
            "signage": "Business signage", "tenant_improvements": "Leasehold improvements"
        }

        assets.append([
            uid(), org_id, loc_id, atype, desc_map.get(atype, atype),
            year_acq, orig_cost, cur_val, repl_cost,
            random.choice(ASSET_CONDITIONS), random.random() < 0.7,
            datetime(2026, 3, 25, random.randint(0,23), random.randint(0,59), random.randint(0,59))
        ])

print(f"  Generated {len(assets)} property assets")

# ============================================================
# VEHICLES (only for orgs that would have them)
# ============================================================
print("Generating vehicles...")
vehicles = []

for org_id, meta in org_meta.items():
    # Not all orgs have vehicles
    has_vehicles = (
        meta["risk_cat"] in ("construction", "transportation", "food_service", "retail", "manufacturing")
        or random.random() < 0.3
    )
    if not has_vehicles:
        continue

    if meta["risk_cat"] == "transportation":
        num_vehicles = random.randint(3, max(4, min(50, meta["num_emp"])))
    elif meta["risk_cat"] == "construction":
        num_vehicles = random.randint(2, max(3, min(20, meta["num_emp"])))
    else:
        num_vehicles = random.randint(1, max(2, min(10, meta["num_emp"] // 10)))

    for _ in range(num_vehicles):
        vtype = random.choices(VEHICLE_TYPES, weights=[20, 15, 20, 15, 10, 10, 10], k=1)[0]
        make, model = random.choice(VEHICLE_MAKES[vtype])
        year = random.randint(2015, 2026)

        usage = "hauling" if vtype in ("semi", "box_truck") else random.choice(USAGE_TYPES)
        mileage = random.randint(5000, 80000) if vtype != "semi" else random.randint(50000, 150000)

        driver_first = random.choice(FIRST_NAMES)
        driver_last = random.choice(LAST_NAMES)
        driver_age = random.randint(21, 65)

        value_map = {"sedan": (15000, 45000), "suv": (20000, 60000), "pickup": (25000, 65000),
                     "van": (25000, 55000), "box_truck": (30000, 80000), "semi": (80000, 200000),
                     "specialty": (40000, 300000)}
        vmin, vmax = value_map.get(vtype, (10000, 50000))
        cur_val = round(random.uniform(vmin, vmax) * max(0.3, 1 - (2026 - year) * 0.08), 2)

        vehicles.append([
            uid(), org_id, rand_vin(), year, make, model, vtype, usage, mileage,
            rand_zip(), f"{driver_first} {driver_last}",
            f"{meta['state']}{random.randint(100000,999999)}",
            driver_age, max(1, driver_age - random.randint(16, 25)),
            random.choices([0, 1, 2, 3], weights=[60, 25, 10, 5], k=1)[0],
            cur_val,
            datetime(2026, 3, 25, random.randint(0,23), random.randint(0,59), random.randint(0,59))
        ])

print(f"  Generated {len(vehicles)} vehicles")

# ============================================================
# CYBER PROFILES (all orgs get one)
# ============================================================
print("Generating cyber profiles...")
cyber = []

for org_id, meta in org_meta.items():
    is_tech = meta["risk_cat"] in ("technology", "healthcare", "professional_services")
    num_endpoints = max(1, int(meta["num_emp"] * random.uniform(0.8, 1.5)))
    num_servers = max(0, int(num_endpoints * random.uniform(0.01, 0.15)))

    has_cloud = is_tech or random.random() < 0.4
    cloud_prov = ", ".join(random.sample(CLOUD_PROVIDERS, k=random.randint(1, 3))) if has_cloud else None

    stores_pii = random.random() < 0.7
    stores_phi = meta["risk_cat"] == "healthcare" or random.random() < 0.1
    stores_payment = meta["risk_cat"] in ("retail", "food_service", "hospitality") or random.random() < 0.3

    num_records = random.randint(100, 10000000) if stores_pii else random.randint(0, 1000)

    maturity = random.random()  # 0=low, 1=high cyber maturity
    if is_tech:
        maturity = min(1.0, maturity + 0.3)

    comp = []
    if stores_phi: comp.append("hipaa")
    if stores_payment: comp.append("pci_dss")
    if maturity > 0.5: comp.extend(random.sample(["soc2", "gdpr", "iso27001"], k=random.randint(0, 2)))
    comp_str = ", ".join(comp) if comp else "none"

    last_audit = rand_date(2022, 2026) if maturity > 0.4 else None

    cyber.append([
        uid(), org_id, num_endpoints, num_servers, has_cloud, cloud_prov,
        stores_pii, stores_phi, stores_payment, num_records,
        maturity > 0.3, maturity > 0.25, maturity > 0.4,
        maturity > 0.5, maturity > 0.45, maturity > 0.35,
        maturity > 0.6, maturity > 0.5,
        comp_str, last_audit, random.random() < 0.3,
        datetime(2026, 3, 25, random.randint(0,23), random.randint(0,59), random.randint(0,59))
    ])

print(f"  Generated {len(cyber)} cyber profiles")

# ============================================================
# FINANCIALS (3 years per org)
# ============================================================
print("Generating financials...")
financials = []

activity_type_map = {
    "retail": "visitors", "manufacturing": "units_produced",
    "professional_services": "professional_hours", "healthcare": "patients_treated",
    "food_service": "meals_served", "hospitality": "visitors"
}

for org_id, meta in org_meta.items():
    base_rev = meta["annual_rev"]
    base_payroll = meta["annual_payroll"]

    for yr in [2023, 2024, 2025]:
        growth = random.uniform(-0.1, 0.2)
        factor = 1 + growth * (yr - 2023)
        rev = round(base_rev * factor, 2)
        intl_pct = random.uniform(0, 0.3) if meta["risk_cat"] in ("technology", "manufacturing") else random.uniform(0, 0.05)

        num_key = random.randint(0, 5)
        largest_pct = round(random.uniform(5, 60), 1) if num_key > 0 else 0

        act_type = activity_type_map.get(meta["risk_cat"], random.choice(["visitors", "units_produced", "professional_hours"]))
        act_count = random.randint(1000, 1000000) if act_type in ("visitors", "meals_served", "units_produced") else random.randint(500, 50000)

        financials.append([
            uid(), org_id, yr, rev,
            round(rev * (1 - intl_pct), 2), round(rev * intl_pct, 2),
            round(base_payroll * factor, 2),
            round(rev * random.uniform(-0.05, 0.25), 2),
            num_key, largest_pct, act_count, act_type,
            datetime(2026, 3, 25, random.randint(0,23), random.randint(0,59), random.randint(0,59))
        ])

print(f"  Generated {len(financials)} financial records")

# ============================================================
# POLICIES (2-6 per org)
# ============================================================
print("Generating policies...")
policies = []
org_policies = {}  # org_id -> [(policy_id, policy_type)]

for org_id, meta in org_meta.items():
    # Most orgs have GL + property at minimum
    core_types = ["general_liability", "property"]
    if meta["num_emp"] > 5:
        core_types.append("workers_comp")

    # Add more based on risk
    optional = []
    if org_id in {v[1] for v in vehicles}:
        optional.append("commercial_auto")
    if meta["risk_cat"] in ("professional_services", "technology", "healthcare"):
        optional.append("professional_liability")
    if meta["has_cyber"]:
        optional.append("cyber")
    if meta["annual_rev"] > 2000000:
        optional.append("umbrella")
    if meta["legal_struct"] == "corporation":
        optional.append("directors_officers")

    # Small businesses might have BOP instead
    if meta["num_emp"] < 50 and random.random() < 0.3:
        selected = ["bop"] + random.sample(optional, k=min(len(optional), random.randint(0, 2)))
    else:
        selected = core_types + random.sample(optional, k=min(len(optional), random.randint(0, 3)))

    pol_list = []
    for pt in selected:
        pol_id = uid()
        pol_list.append((pol_id, pt))

        eff_date = rand_date(2024, 2025)
        exp_date = eff_date + timedelta(days=365)

        limit_map = {
            "general_liability": (500000, 2000000), "property": (100000, 5000000),
            "workers_comp": (500000, 1000000), "commercial_auto": (500000, 2000000),
            "professional_liability": (500000, 5000000), "cyber": (500000, 5000000),
            "umbrella": (1000000, 10000000), "bop": (500000, 2000000),
            "directors_officers": (1000000, 10000000), "employment_practices": (500000, 3000000)
        }
        lmin, lmax = limit_map.get(pt, (500000, 2000000))
        limit = round(random.choice([500000, 1000000, 2000000, 3000000, 5000000]), 2)
        limit = max(lmin, min(lmax, limit))
        agg = limit * 2

        ded_options = [500, 1000, 2500, 5000, 10000, 25000, 50000]
        ded = random.choice(ded_options)

        premium = round(limit * random.uniform(0.003, 0.02), 2)

        policies.append([
            pol_id, org_id, pt, random.choice(INSURERS),
            f"POL-{random.randint(100000,999999)}-{random.randint(10,99)}",
            eff_date, exp_date, limit, agg, ded, premium, True, None,
            datetime(2026, 3, 25, random.randint(0,23), random.randint(0,59), random.randint(0,59))
        ])

    org_policies[org_id] = pol_list

print(f"  Generated {len(policies)} policies")

# ============================================================
# CLAIMS (~30% of orgs have claims)
# ============================================================
print("Generating claims...")
claims = []

for org_id, meta in org_meta.items():
    if random.random() > 0.35:  # ~35% have claims
        continue

    num_claims = random.choices([1, 2, 3, 4, 5], weights=[40, 30, 15, 10, 5], k=1)[0]
    pol_list = org_policies.get(org_id, [])

    for _ in range(num_claims):
        claim_type = random.choice(CLAIM_TYPES)

        # Match to a policy if possible
        matching_pol = None
        type_to_policy = {
            "property_damage": "property", "fire": "property", "water_damage": "property", "theft": "property",
            "bodily_injury": "general_liability", "slip_and_fall": "general_liability", "product_liability": "general_liability",
            "auto_collision": "commercial_auto",
            "workers_comp_injury": "workers_comp",
            "professional_error": "professional_liability",
            "cyber_breach": "cyber"
        }
        target_pol_type = type_to_policy.get(claim_type)
        for pid, ptype in pol_list:
            if ptype == target_pol_type:
                matching_pol = pid
                break

        claim_date = rand_date(2020, 2025)
        cause = random.choice(CLAIM_CAUSES.get(claim_type, ["Unforeseen incident"]))

        severity = random.choices(["minor", "moderate", "major"], weights=[50, 35, 15], k=1)[0]
        if severity == "minor":
            amount = round(random.uniform(500, 15000), 2)
        elif severity == "moderate":
            amount = round(random.uniform(15000, 100000), 2)
        else:
            amount = round(random.uniform(100000, 1000000), 2)

        status = random.choices(["closed", "settled", "open"], weights=[50, 35, 15], k=1)[0]
        reserved = round(amount * random.uniform(0, 0.3), 2) if status == "open" else 0

        corrective = random.choice(CORRECTIVE_ACTIONS) if status != "open" else None

        claims.append([
            uid(), org_id, matching_pol, claim_date, claim_type,
            f"{claim_type.replace('_', ' ').title()} - {cause}",
            amount, reserved, status, cause, corrective,
            datetime(2026, 3, 25, random.randint(0,23), random.randint(0,59), random.randint(0,59))
        ])

print(f"  Generated {len(claims)} claims")

# ============================================================
# COVERAGE REQUESTS (1-4 per org)
# ============================================================
print("Generating coverage requests...")
cov_requests = []

for org_id, meta in org_meta.items():
    num_req = random.randint(1, 4)
    requested_types = random.sample(POLICY_TYPES, k=min(num_req, len(POLICY_TYPES)))

    for ct in requested_types:
        limit = random.choice([500000, 1000000, 2000000, 3000000, 5000000])
        ded = random.choice([500, 1000, 2500, 5000, 10000, 25000])

        special = None
        if random.random() < 0.3:
            specials = [
                "Additional insured endorsement required",
                "Waiver of subrogation needed",
                "Include hired and non-owned auto",
                "Professional liability retroactive date needed",
                "Cyber incident response coverage required",
                "Include employee dishonesty coverage",
                "Blanket additional insured",
                "Per project aggregate needed",
                "Include pollution liability",
                "Excess liability over primary"
            ]
            special = random.choice(specials)

        cov_requests.append([
            uid(), org_id, ct, limit, ded, special,
            random.choice(["must_have", "nice_to_have"]),
            rand_date(2026, 2026),
            datetime(2026, 3, 25, random.randint(0,23), random.randint(0,59), random.randint(0,59))
        ])

print(f"  Generated {len(cov_requests)} coverage requests")

# ============================================================
# CONTRACTS (1-5 per org)
# ============================================================
print("Generating contracts...")
contracts = []

for org_id, meta in org_meta.items():
    if random.random() > 0.7:  # 70% have contracts
        continue

    num_contracts = random.randint(1, 5)
    for _ in range(num_contracts):
        client_first = random.choice(PREFIXES)
        client_suffix = random.choice(["Corp", "Inc", "LLC", "Group", "Co", "Industries", "International"])

        req_ai = random.random() < 0.5
        req_wos = random.random() < 0.3
        min_limit = random.choice([1000000, 2000000, 5000000]) if req_ai else None

        contracts.append([
            uid(), org_id, f"{client_first} {client_suffix}",
            random.choice(CLIENT_INDUSTRIES),
            random.choice(CONTRACT_TYPES),
            round(random.uniform(10000, 5000000), 2),
            req_ai, req_wos,
            random.choice(INDEMNITY_TYPES),
            min_limit,
            datetime(2026, 3, 25, random.randint(0,23), random.randint(0,59), random.randint(0,59))
        ])

print(f"  Generated {len(contracts)} contracts")

# ============================================================
# Write to SQL files in batches
# ============================================================

def write_inserts(table_name, columns, rows, batch_size=500):
    """Write INSERT statements in batch files."""
    fqn = f"{CATALOG}.{SCHEMA}.{table_name}"
    cols_str = ", ".join(columns)

    num_batches = (len(rows) + batch_size - 1) // batch_size
    files = []

    for b in range(num_batches):
        batch = rows[b * batch_size : (b + 1) * batch_size]
        fname = f"{OUTPUT_DIR}/{table_name}_{b:04d}.sql"

        values_list = []
        for row in batch:
            values_list.append(sql_values(row))

        sql = f"INSERT INTO {fqn} ({cols_str}) VALUES\n" + ",\n".join(values_list)

        with open(fname, 'w') as f:
            f.write(sql)
        files.append(fname)

    print(f"  {table_name}: {len(rows)} rows -> {num_batches} batch files")
    return files

print("\nWriting SQL batch files...")

all_files = []

all_files += write_inserts("organizations", [
    "org_id", "legal_name", "trading_name", "legal_structure", "date_established",
    "years_current_ownership", "primary_phone", "primary_email", "website",
    "primary_contact_name", "primary_contact_title", "naics_code", "sic_code",
    "business_description", "main_products_services", "industries_served", "risk_category",
    "num_employees", "num_contractors", "uses_subcontractors", "annual_revenue",
    "annual_payroll", "has_written_safety_procedures", "has_employee_training_program",
    "has_cyber_controls", "created_at"
], orgs)

all_files += write_inserts("locations", [
    "location_id", "org_id", "location_type", "address_line1", "address_line2",
    "city", "state", "zip_code", "country", "square_footage", "building_age_years",
    "construction_type", "num_stories", "has_sprinkler_system", "has_fire_alarm",
    "has_security_system", "has_cctv", "distance_to_fire_station_miles",
    "fire_protection_class", "is_primary", "created_at"
], locations)

all_files += write_inserts("employees", [
    "employee_record_id", "org_id", "job_classification", "department",
    "num_employees", "annual_payroll", "workers_comp_class_code", "is_remote", "created_at"
], employees)

all_files += write_inserts("property_assets", [
    "asset_id", "org_id", "location_id", "asset_type", "description",
    "year_acquired", "original_cost", "current_value", "replacement_cost",
    "condition", "is_owned", "created_at"
], assets)

all_files += write_inserts("vehicles", [
    "vehicle_id", "org_id", "vin", "year", "make", "model", "vehicle_type",
    "usage_type", "annual_mileage", "garaging_zip", "driver_name",
    "driver_license_number", "driver_age", "driver_years_experience",
    "driver_violations_3yr", "current_value", "created_at"
], vehicles)

all_files += write_inserts("cyber_profiles", [
    "cyber_profile_id", "org_id", "num_endpoints", "num_servers",
    "has_cloud_infrastructure", "cloud_providers", "stores_pii", "stores_phi",
    "stores_payment_data", "num_records_stored", "has_endpoint_protection",
    "has_firewall", "has_mfa", "has_encryption_at_rest", "has_encryption_in_transit",
    "has_backup_plan", "has_incident_response_plan", "has_security_training",
    "compliance_frameworks", "last_security_audit_date", "has_cyber_insurance_history",
    "created_at"
], cyber)

all_files += write_inserts("financials", [
    "financial_id", "org_id", "fiscal_year", "total_revenue", "revenue_domestic",
    "revenue_international", "total_payroll", "net_income", "num_key_customers",
    "largest_customer_revenue_pct", "activity_count", "activity_type", "created_at"
], financials)

all_files += write_inserts("policies", [
    "policy_id", "org_id", "policy_type", "insurer_name", "policy_number",
    "effective_date", "expiration_date", "coverage_limit", "aggregate_limit",
    "deductible", "annual_premium", "is_current", "special_endorsements", "created_at"
], policies)

all_files += write_inserts("claims", [
    "claim_id", "org_id", "policy_id", "claim_date", "claim_type",
    "description", "amount_paid", "amount_reserved", "status", "cause",
    "corrective_action_taken", "created_at"
], claims)

all_files += write_inserts("coverage_requests", [
    "request_id", "org_id", "coverage_type", "requested_limit",
    "requested_deductible", "special_terms", "priority",
    "effective_date_requested", "created_at"
], cov_requests)

all_files += write_inserts("contracts", [
    "contract_id", "org_id", "client_name", "client_industry", "contract_type",
    "annual_value", "requires_additional_insured", "requires_waiver_of_subrogation",
    "indemnity_requirements", "minimum_liability_limit", "created_at"
], contracts)

# Write manifest
with open(f"{OUTPUT_DIR}/manifest.json", 'w') as f:
    json.dump({"files": [os.path.basename(fp) for fp in all_files], "total_files": len(all_files)}, f, indent=2)

print(f"\nTotal: {len(all_files)} SQL batch files written to {OUTPUT_DIR}/")
print("Done!")
