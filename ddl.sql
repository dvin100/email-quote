-- 1. ORGANIZATIONS
CREATE OR REPLACE TABLE dvin100_email_to_quote.email_to_quote.organizations (
  org_id STRING NOT NULL COMMENT 'Unique organization identifier',
  legal_name STRING NOT NULL COMMENT 'Legal business name',
  trading_name STRING COMMENT 'Trading / DBA name',
  legal_structure STRING NOT NULL COMMENT 'sole_trader, partnership, corporation, llc, non_profit',
  date_established DATE COMMENT 'Date business was established',
  years_current_ownership INT COMMENT 'Years under current ownership',
  primary_phone STRING,
  primary_email STRING,
  website STRING,
  primary_contact_name STRING,
  primary_contact_title STRING,
  naics_code STRING COMMENT 'NAICS industry code',
  sic_code STRING COMMENT 'SIC industry code',
  business_description STRING COMMENT 'Free-text description of what the business does',
  main_products_services STRING COMMENT 'Comma-separated main products/services',
  industries_served STRING COMMENT 'Comma-separated industries served',
  risk_category STRING COMMENT 'office, retail, construction, manufacturing, healthcare, technology, food_service, transportation, professional_services, hospitality',
  num_employees INT COMMENT 'Total number of employees',
  num_contractors INT COMMENT 'Number of contractors',
  uses_subcontractors BOOLEAN COMMENT 'Whether the business uses subcontractors',
  annual_revenue DOUBLE COMMENT 'Most recent annual revenue in USD',
  annual_payroll DOUBLE COMMENT 'Annual payroll in USD',
  has_written_safety_procedures BOOLEAN,
  has_employee_training_program BOOLEAN,
  has_cyber_controls BOOLEAN,
  created_at TIMESTAMP
) COMMENT 'Core organization profiles for insurance quoting';

-- 2. LOCATIONS
CREATE OR REPLACE TABLE dvin100_email_to_quote.email_to_quote.locations (
  location_id STRING NOT NULL,
  org_id STRING NOT NULL,
  location_type STRING NOT NULL COMMENT 'headquarters, branch, warehouse, remote_site, retail_store',
  address_line1 STRING,
  address_line2 STRING,
  city STRING,
  state STRING,
  zip_code STRING,
  country STRING,
  square_footage INT,
  building_age_years INT,
  construction_type STRING COMMENT 'frame, masonry, fire_resistive, non_combustible, modified_fire_resistive',
  num_stories INT,
  has_sprinkler_system BOOLEAN,
  has_fire_alarm BOOLEAN,
  has_security_system BOOLEAN,
  has_cctv BOOLEAN,
  distance_to_fire_station_miles DOUBLE,
  fire_protection_class INT COMMENT '1-10 ISO fire protection class',
  is_primary BOOLEAN,
  created_at TIMESTAMP
) COMMENT 'Organization premises and locations';

-- 3. EMPLOYEES
CREATE OR REPLACE TABLE dvin100_email_to_quote.email_to_quote.employees (
  employee_record_id STRING NOT NULL,
  org_id STRING NOT NULL,
  job_classification STRING NOT NULL COMMENT 'office_clerical, sales, management, skilled_labor, unskilled_labor, professional, driver, healthcare_worker',
  department STRING,
  num_employees INT COMMENT 'Count of employees in this classification',
  annual_payroll DOUBLE COMMENT 'Payroll for this classification',
  workers_comp_class_code STRING,
  is_remote BOOLEAN,
  created_at TIMESTAMP
) COMMENT 'Employee classifications and payroll by department';

-- 4. PROPERTY ASSETS
CREATE OR REPLACE TABLE dvin100_email_to_quote.email_to_quote.property_assets (
  asset_id STRING NOT NULL,
  org_id STRING NOT NULL,
  location_id STRING NOT NULL,
  asset_type STRING NOT NULL COMMENT 'building, equipment, machinery, it_hardware, stock_inventory, furniture, signage, tenant_improvements',
  description STRING,
  year_acquired INT,
  original_cost DOUBLE,
  current_value DOUBLE,
  replacement_cost DOUBLE,
  condition STRING COMMENT 'excellent, good, fair, poor',
  is_owned BOOLEAN COMMENT 'true=owned, false=leased',
  created_at TIMESTAMP
) COMMENT 'Property and asset details for insurance coverage';

-- 5. VEHICLES
CREATE OR REPLACE TABLE dvin100_email_to_quote.email_to_quote.vehicles (
  vehicle_id STRING NOT NULL,
  org_id STRING NOT NULL,
  vin STRING,
  year INT,
  make STRING,
  model STRING,
  vehicle_type STRING COMMENT 'sedan, suv, pickup, van, box_truck, semi, specialty',
  usage_type STRING COMMENT 'business_use, commercial, delivery, passenger, hauling',
  annual_mileage INT,
  garaging_zip STRING,
  driver_name STRING,
  driver_license_number STRING,
  driver_age INT,
  driver_years_experience INT,
  driver_violations_3yr INT COMMENT 'Number of violations in past 3 years',
  current_value DOUBLE,
  created_at TIMESTAMP
) COMMENT 'Vehicle and fleet details';

-- 6. CYBER PROFILES
CREATE OR REPLACE TABLE dvin100_email_to_quote.email_to_quote.cyber_profiles (
  cyber_profile_id STRING NOT NULL,
  org_id STRING NOT NULL,
  num_endpoints INT COMMENT 'Number of computers/devices',
  num_servers INT,
  has_cloud_infrastructure BOOLEAN,
  cloud_providers STRING COMMENT 'Comma-separated: aws, azure, gcp, other',
  stores_pii BOOLEAN COMMENT 'Stores personally identifiable information',
  stores_phi BOOLEAN COMMENT 'Stores protected health information',
  stores_payment_data BOOLEAN COMMENT 'Stores credit card / payment data',
  num_records_stored INT COMMENT 'Approximate number of sensitive records',
  has_endpoint_protection BOOLEAN,
  has_firewall BOOLEAN,
  has_mfa BOOLEAN,
  has_encryption_at_rest BOOLEAN,
  has_encryption_in_transit BOOLEAN,
  has_backup_plan BOOLEAN,
  has_incident_response_plan BOOLEAN,
  has_security_training BOOLEAN,
  compliance_frameworks STRING COMMENT 'Comma-separated: soc2, hipaa, pci_dss, gdpr, iso27001, none',
  last_security_audit_date DATE,
  has_cyber_insurance_history BOOLEAN,
  created_at TIMESTAMP
) COMMENT 'IT systems and cybersecurity posture';

-- 7. FINANCIALS
CREATE OR REPLACE TABLE dvin100_email_to_quote.email_to_quote.financials (
  financial_id STRING NOT NULL,
  org_id STRING NOT NULL,
  fiscal_year INT NOT NULL,
  total_revenue DOUBLE,
  revenue_domestic DOUBLE,
  revenue_international DOUBLE,
  total_payroll DOUBLE,
  net_income DOUBLE,
  num_key_customers INT COMMENT 'Number of customers representing >10pct revenue',
  largest_customer_revenue_pct DOUBLE COMMENT 'Percentage of revenue from largest customer',
  activity_count INT COMMENT 'Domain-specific: visitors, units produced, hours billed, etc.',
  activity_type STRING COMMENT 'visitors, units_produced, professional_hours, patients_treated, meals_served',
  created_at TIMESTAMP
) COMMENT 'Annual financial and activity data';

-- 8. POLICIES
CREATE OR REPLACE TABLE dvin100_email_to_quote.email_to_quote.policies (
  policy_id STRING NOT NULL,
  org_id STRING NOT NULL,
  policy_type STRING NOT NULL COMMENT 'general_liability, property, workers_comp, commercial_auto, professional_liability, cyber, umbrella, bop, directors_officers, employment_practices',
  insurer_name STRING,
  policy_number STRING,
  effective_date DATE,
  expiration_date DATE,
  coverage_limit DOUBLE,
  aggregate_limit DOUBLE,
  deductible DOUBLE,
  annual_premium DOUBLE,
  is_current BOOLEAN,
  special_endorsements STRING,
  created_at TIMESTAMP
) COMMENT 'Existing and historical insurance policies';

-- 9. CLAIMS
CREATE OR REPLACE TABLE dvin100_email_to_quote.email_to_quote.claims (
  claim_id STRING NOT NULL,
  org_id STRING NOT NULL,
  policy_id STRING,
  claim_date DATE NOT NULL,
  claim_type STRING NOT NULL COMMENT 'property_damage, bodily_injury, theft, fire, water_damage, auto_collision, workers_comp_injury, professional_error, cyber_breach, slip_and_fall, product_liability',
  description STRING,
  amount_paid DOUBLE,
  amount_reserved DOUBLE,
  status STRING COMMENT 'open, closed, settled',
  cause STRING COMMENT 'Brief cause description',
  corrective_action_taken STRING COMMENT 'What changed after the claim',
  created_at TIMESTAMP
) COMMENT 'Claims and loss history';

-- 10. COVERAGE REQUESTS
CREATE OR REPLACE TABLE dvin100_email_to_quote.email_to_quote.coverage_requests (
  request_id STRING NOT NULL,
  org_id STRING NOT NULL,
  coverage_type STRING NOT NULL COMMENT 'Same types as policy_type',
  requested_limit DOUBLE,
  requested_deductible DOUBLE,
  special_terms STRING COMMENT 'Any special terms or endorsements requested',
  priority STRING COMMENT 'must_have, nice_to_have',
  effective_date_requested DATE,
  created_at TIMESTAMP
) COMMENT 'Requested coverages for quoting';

-- 11. CONTRACTS
CREATE OR REPLACE TABLE dvin100_email_to_quote.email_to_quote.contracts (
  contract_id STRING NOT NULL,
  org_id STRING NOT NULL,
  client_name STRING,
  client_industry STRING,
  contract_type STRING COMMENT 'service_agreement, master_service, subcontract, lease, vendor, government',
  annual_value DOUBLE,
  requires_additional_insured BOOLEAN,
  requires_waiver_of_subrogation BOOLEAN,
  indemnity_requirements STRING COMMENT 'mutual, one_way_client, one_way_vendor, none',
  minimum_liability_limit DOUBLE COMMENT 'Minimum GL limit required by contract',
  created_at TIMESTAMP
) COMMENT 'Client contracts and contractual liability requirements';
