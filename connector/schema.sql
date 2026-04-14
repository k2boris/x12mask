PRAGMA foreign_keys = ON;

-- Canonical tables (single-parent FK model)
CREATE TABLE IF NOT EXISTS biz_transaction_entity (
  transaction_id TEXT PRIMARY KEY,
  interchange_control_no TEXT,
  functional_group_control_no TEXT,
  transaction_set_control_no TEXT,
  message_type TEXT,
  version TEXT,
  source_format TEXT,
  created_date TEXT,
  created_time TEXT
);

CREATE TABLE IF NOT EXISTS biz_submitter_entity (
  submitter_id TEXT PRIMARY KEY,
  transaction_id TEXT NOT NULL,
  name TEXT,
  identifier_type TEXT,
  identifier_value TEXT,
  contact_name TEXT,
  phone TEXT,
  email TEXT,
  FOREIGN KEY (transaction_id) REFERENCES biz_transaction_entity(transaction_id)
);

CREATE TABLE IF NOT EXISTS biz_receiver_entity (
  receiver_id TEXT PRIMARY KEY,
  transaction_id TEXT NOT NULL,
  name TEXT,
  identifier_type TEXT,
  identifier_value TEXT,
  FOREIGN KEY (transaction_id) REFERENCES biz_transaction_entity(transaction_id)
);

CREATE TABLE IF NOT EXISTS biz_billing_provider_entity (
  billing_provider_id TEXT PRIMARY KEY,
  transaction_id TEXT NOT NULL,
  organization_name TEXT,
  npi TEXT,
  taxonomy TEXT,
  tax_id TEXT,
  address_line1 TEXT,
  address_line2 TEXT,
  city TEXT,
  state TEXT,
  postal_code TEXT,
  contact_name TEXT,
  contact_phone TEXT,
  FOREIGN KEY (transaction_id) REFERENCES biz_transaction_entity(transaction_id)
);

CREATE TABLE IF NOT EXISTS biz_payto_provider_entity (
  payto_provider_id TEXT PRIMARY KEY,
  transaction_id TEXT NOT NULL,
  organization_name TEXT,
  npi TEXT,
  address_line1 TEXT,
  address_line2 TEXT,
  city TEXT,
  state TEXT,
  postal_code TEXT,
  FOREIGN KEY (transaction_id) REFERENCES biz_transaction_entity(transaction_id)
);

CREATE TABLE IF NOT EXISTS biz_payer_entity (
  payer_id TEXT PRIMARY KEY,
  transaction_id TEXT NOT NULL,
  name TEXT,
  payer_identifier TEXT,
  FOREIGN KEY (transaction_id) REFERENCES biz_transaction_entity(transaction_id)
);

CREATE TABLE IF NOT EXISTS biz_person_entity (
  person_id TEXT PRIMARY KEY,
  transaction_id TEXT NOT NULL,
  person_type TEXT,
  mastering_note TEXT,
  FOREIGN KEY (transaction_id) REFERENCES biz_transaction_entity(transaction_id)
);

CREATE TABLE IF NOT EXISTS biz_person_name (
  person_name_id TEXT PRIMARY KEY,
  transaction_id TEXT NOT NULL,
  person_id TEXT NOT NULL,
  first_name TEXT,
  last_name TEXT,
  middle_name TEXT,
  FOREIGN KEY (person_id) REFERENCES biz_person_entity(person_id)
);

CREATE TABLE IF NOT EXISTS biz_person_demographic (
  person_demographic_id TEXT PRIMARY KEY,
  transaction_id TEXT NOT NULL,
  person_id TEXT NOT NULL,
  birth_date TEXT,
  gender TEXT,
  FOREIGN KEY (person_id) REFERENCES biz_person_entity(person_id)
);

CREATE TABLE IF NOT EXISTS biz_person_identifier (
  person_identifier_id TEXT PRIMARY KEY,
  transaction_id TEXT NOT NULL,
  person_id TEXT NOT NULL,
  identifier_type TEXT,
  identifier_value TEXT,
  assigning_authority TEXT,
  FOREIGN KEY (person_id) REFERENCES biz_person_entity(person_id)
);

CREATE TABLE IF NOT EXISTS biz_person_address (
  person_address_id TEXT PRIMARY KEY,
  transaction_id TEXT NOT NULL,
  person_id TEXT NOT NULL,
  address_line1 TEXT,
  city TEXT,
  state TEXT,
  postal_code TEXT,
  FOREIGN KEY (person_id) REFERENCES biz_person_entity(person_id)
);

CREATE TABLE IF NOT EXISTS biz_provider_entity (
  provider_id TEXT PRIMARY KEY,
  transaction_id TEXT NOT NULL,
  last_name TEXT,
  first_name TEXT,
  npi TEXT,
  taxonomy TEXT,
  FOREIGN KEY (transaction_id) REFERENCES biz_transaction_entity(transaction_id)
);

CREATE TABLE IF NOT EXISTS biz_service_facility_entity (
  service_facility_id TEXT PRIMARY KEY,
  transaction_id TEXT NOT NULL,
  organization_name TEXT,
  npi TEXT,
  address_line1 TEXT,
  city TEXT,
  state TEXT,
  postal_code TEXT,
  FOREIGN KEY (transaction_id) REFERENCES biz_transaction_entity(transaction_id)
);

CREATE TABLE IF NOT EXISTS biz_claim_entity (
  claim_id TEXT PRIMARY KEY,
  transaction_id TEXT NOT NULL,
  claim_submitter_id TEXT,
  payer_id TEXT,
  subscriber_person_id TEXT,
  patient_person_id TEXT,
  total_charge_amount NUMERIC,
  place_of_service TEXT,
  facility_code_qualifier TEXT,
  claim_frequency_type_code TEXT,
  FOREIGN KEY (transaction_id) REFERENCES biz_transaction_entity(transaction_id)
);

CREATE TABLE IF NOT EXISTS biz_claim_policy_context (
  claim_policy_context_id TEXT PRIMARY KEY,
  transaction_id TEXT NOT NULL,
  claim_id TEXT NOT NULL,
  payer_responsibility TEXT,
  subscriber_relationship_code TEXT,
  group_number TEXT,
  plan_name TEXT,
  insurance_type TEXT,
  benefit_plan_name TEXT,
  patient_relationship_code TEXT,
  same_as_subscriber INTEGER CHECK (same_as_subscriber IN (0, 1) OR same_as_subscriber IS NULL),
  FOREIGN KEY (claim_id) REFERENCES biz_claim_entity(claim_id)
);

CREATE TABLE IF NOT EXISTS biz_claim_flags (
  claim_flag_id TEXT PRIMARY KEY,
  transaction_id TEXT NOT NULL,
  claim_id TEXT NOT NULL,
  provider_accept_assignment INTEGER CHECK (provider_accept_assignment IN (0, 1) OR provider_accept_assignment IS NULL),
  benefits_assigned INTEGER CHECK (benefits_assigned IN (0, 1) OR benefits_assigned IS NULL),
  release_of_information INTEGER CHECK (release_of_information IN (0, 1) OR release_of_information IS NULL),
  delay_reason_code_present INTEGER CHECK (delay_reason_code_present IN (0, 1) OR delay_reason_code_present IS NULL),
  FOREIGN KEY (claim_id) REFERENCES biz_claim_entity(claim_id)
);

CREATE TABLE IF NOT EXISTS biz_claim_date (
  claim_date_id TEXT PRIMARY KEY,
  transaction_id TEXT NOT NULL,
  claim_id TEXT NOT NULL,
  date_type TEXT,
  qualifier TEXT,
  date_format TEXT,
  date_value TEXT,
  FOREIGN KEY (claim_id) REFERENCES biz_claim_entity(claim_id)
);

CREATE TABLE IF NOT EXISTS biz_claim_identifier (
  claim_identifier_id TEXT PRIMARY KEY,
  transaction_id TEXT NOT NULL,
  claim_id TEXT NOT NULL,
  qualifier TEXT,
  identifier_value TEXT,
  meaning TEXT,
  FOREIGN KEY (claim_id) REFERENCES biz_claim_entity(claim_id)
);

CREATE TABLE IF NOT EXISTS biz_claim_attachment (
  claim_attachment_id TEXT PRIMARY KEY,
  transaction_id TEXT NOT NULL,
  claim_id TEXT NOT NULL,
  report_type_code TEXT,
  transmission_code TEXT,
  control_number_qualifier TEXT,
  control_number TEXT,
  FOREIGN KEY (claim_id) REFERENCES biz_claim_entity(claim_id)
);

CREATE TABLE IF NOT EXISTS biz_claim_condition (
  claim_condition_id TEXT PRIMARY KEY,
  transaction_id TEXT NOT NULL,
  claim_id TEXT NOT NULL,
  segment TEXT,
  category TEXT,
  certification_condition_indicator TEXT,
  condition_code1 TEXT,
  FOREIGN KEY (claim_id) REFERENCES biz_claim_entity(claim_id)
);

CREATE TABLE IF NOT EXISTS biz_claim_note (
  claim_note_id TEXT PRIMARY KEY,
  transaction_id TEXT NOT NULL,
  claim_id TEXT NOT NULL,
  note_type TEXT,
  note_text TEXT,
  FOREIGN KEY (claim_id) REFERENCES biz_claim_entity(claim_id)
);

CREATE TABLE IF NOT EXISTS biz_claim_diagnosis (
  claim_diagnosis_id TEXT PRIMARY KEY,
  transaction_id TEXT NOT NULL,
  claim_id TEXT NOT NULL,
  sequence INTEGER,
  qualifier TEXT,
  code TEXT,
  FOREIGN KEY (claim_id) REFERENCES biz_claim_entity(claim_id)
);

CREATE TABLE IF NOT EXISTS biz_claim_provider_role (
  claim_provider_role_id TEXT PRIMARY KEY,
  transaction_id TEXT NOT NULL,
  claim_id TEXT NOT NULL,
  provider_id TEXT NOT NULL,
  role_code TEXT,
  FOREIGN KEY (claim_id) REFERENCES biz_claim_entity(claim_id)
);

CREATE TABLE IF NOT EXISTS biz_claim_service_facility (
  claim_service_facility_id TEXT PRIMARY KEY,
  transaction_id TEXT NOT NULL,
  claim_id TEXT NOT NULL,
  service_facility_id TEXT NOT NULL,
  FOREIGN KEY (claim_id) REFERENCES biz_claim_entity(claim_id)
);

CREATE TABLE IF NOT EXISTS biz_service_line_entity (
  service_line_id TEXT PRIMARY KEY,
  transaction_id TEXT NOT NULL,
  claim_id TEXT NOT NULL,
  line_number INTEGER,
  procedure_qualifier TEXT,
  procedure_code TEXT,
  charge_amount NUMERIC,
  unit_measure TEXT,
  units NUMERIC,
  date_of_service TEXT,
  FOREIGN KEY (claim_id) REFERENCES biz_claim_entity(claim_id)
);

CREATE TABLE IF NOT EXISTS biz_service_line_identifier (
  service_line_identifier_id TEXT PRIMARY KEY,
  transaction_id TEXT NOT NULL,
  service_line_id TEXT NOT NULL,
  qualifier TEXT,
  identifier_value TEXT,
  FOREIGN KEY (service_line_id) REFERENCES biz_service_line_entity(service_line_id)
);

-- Mapping tables (single-parent FK model)
CREATE TABLE IF NOT EXISTS map_parser_profile (
  parser_profile_id TEXT PRIMARY KEY,
  transaction_id TEXT NOT NULL,
  message_type TEXT,
  version TEXT,
  segment_delimiter TEXT,
  element_delimiter TEXT,
  component_delimiter TEXT,
  FOREIGN KEY (transaction_id) REFERENCES biz_transaction_entity(transaction_id)
);

CREATE TABLE IF NOT EXISTS map_x12_locator (
  locator_id TEXT PRIMARY KEY,
  transaction_id TEXT NOT NULL,
  loop_id TEXT,
  segment_ordinal INTEGER,
  segment_id TEXT,
  element_position INTEGER,
  component_position INTEGER,
  repetition_ordinal INTEGER,
  qualifier TEXT,
  value TEXT,
  FOREIGN KEY (transaction_id) REFERENCES biz_transaction_entity(transaction_id)
);

CREATE TABLE IF NOT EXISTS map_extraction_rule (
  extraction_rule_id TEXT PRIMARY KEY,
  transaction_id TEXT NOT NULL,
  source_locator_pattern TEXT,
  target_table TEXT,
  target_column TEXT,
  target_key_strategy TEXT,
  transform_type TEXT,
  FOREIGN KEY (transaction_id) REFERENCES biz_transaction_entity(transaction_id)
);

CREATE TABLE IF NOT EXISTS map_phi_classification_rule (
  phi_rule_id TEXT PRIMARY KEY,
  transaction_id TEXT NOT NULL,
  source_locator_pattern TEXT,
  phi_category TEXT,
  masking_required TEXT,
  FOREIGN KEY (transaction_id) REFERENCES biz_transaction_entity(transaction_id)
);

CREATE TABLE IF NOT EXISTS map_canonical_binding (
  binding_id TEXT PRIMARY KEY,
  transaction_id TEXT NOT NULL,
  locator_id TEXT,
  target_table TEXT,
  target_row_id TEXT,
  target_column TEXT,
  FOREIGN KEY (locator_id) REFERENCES map_x12_locator(locator_id)
);

CREATE TABLE IF NOT EXISTS map_replacement_manifest_rule (
  replacement_rule_id TEXT PRIMARY KEY,
  transaction_id TEXT NOT NULL,
  target_table TEXT,
  target_column TEXT,
  locator_pattern TEXT,
  replacement_mode TEXT,
  FOREIGN KEY (transaction_id) REFERENCES biz_transaction_entity(transaction_id)
);
