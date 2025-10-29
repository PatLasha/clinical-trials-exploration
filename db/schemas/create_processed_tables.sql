-- Create processed table for cleaned clinical trials data

-- Normalized and cleaned data from staging.raw_clinical_trials

-- This seemed to be too much for me right now, so leaving it out for the time being
-- CREATE TYPE processed.org_class_enum AS ENUM (
--     'Industry',
--     'NIH',
--     'Other',
--     'U.S. Fed'
-- );

-- Organization class table
CREATE TABLE IF NOT EXISTS processed.org_class (
    id SERIAL PRIMARY KEY,
    org_class VARCHAR(50) NOT NULL UNIQUE
);

-- Responsible party table
CREATE TABLE IF NOT EXISTS processed.responsible_party (
    id SERIAL PRIMARY KEY,
    responsible_party VARCHAR(50) NOT NULL UNIQUE
);

-- Overall status table
CREATE TABLE IF NOT EXISTS processed.overall_status (
    id SERIAL PRIMARY KEY,
    overall_status VARCHAR(50) NOT NULL UNIQUE
);

-- Standard age table
CREATE TABLE IF NOT EXISTS processed.standard_age (
    id SERIAL PRIMARY KEY,
    standard_age VARCHAR(50) NOT NULL UNIQUE
);

-- Bridge table for study age groups, as some studies can have multiple age groups
CREATE TABLE IF NOT EXISTS processed.study_age_groups (
    study_id INTEGER REFERENCES processed.studies(study_id),
    age_group_id INTEGER REFERENCES processed.standard_age(id),
    PRIMARY KEY (study_id, age_group_id)
);

-- Primary purpose table
CREATE TABLE IF NOT EXISTS processed.primary_purpose (
    id SERIAL PRIMARY KEY,
    primary_purpose VARCHAR(50) NOT NULL UNIQUE
);

-- Study type table
CREATE TABLE IF NOT EXISTS processed.study_type (
    id SERIAL PRIMARY KEY,
    study_type VARCHAR(50) NOT NULL UNIQUE
);

-- Phase table
CREATE TABLE IF NOT EXISTS processed.phase (
    id SERIAL PRIMARY KEY,
    phase VARCHAR(50) NOT NULL UNIQUE
);

-- Organization table
CREATE TABLE IF NOT EXISTS processed.organization (
    id SERIAL PRIMARY KEY,
    org_name VARCHAR(255) NOT NULL UNIQUE,
    org_class_id INTEGER REFERENCES processed.org_class(id) ON DELETE SET NULL
);

-- Conditions table
CREATE TABLE IF NOT EXISTS processed.condition (
    condition_id SERIAL PRIMARY KEY,
    condition VARCHAR(255) NOT NULL UNIQUE,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Conditions bridge table
CREATE TABLE IF NOT EXISTS processed.study_conditions (
    study_id INTEGER REFERENCES processed.studies(study_id),
    condition_id INTEGER REFERENCES processed.condition(condition_id),
    PRIMARY KEY (study_id, condition_id)
);

-- Interventions table
CREATE TABLE IF NOT EXISTS processed.intervention (
    intervention_id SERIAL PRIMARY KEY,
    intervention VARCHAR(255) NOT NULL UNIQUE,
    intervention_description TEXT,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Interventions bridge table
CREATE TABLE IF NOT EXISTS processed.study_interventions (
    study_id INTEGER REFERENCES processed.studies(study_id),
    intervention_id INTEGER REFERENCES processed.intervention(intervention_id),
    PRIMARY KEY (study_id, intervention_id)
);

-- Main studies table
-- Records will be set to NULL on delete of referenced records to preserve historical data
CREATE TABLE IF NOT EXISTS processed.studies (
    study_id SERIAL PRIMARY KEY,
    org_id INTEGER REFERENCES processed.organization(id) ON DELETE SET NULL,
    responsible_party_id INTEGER REFERENCES processed.responsible_party(id) ON DELETE SET NULL,
    brief_title TEXT,
    full_title TEXT,
    overall_status_id INTEGER REFERENCES processed.overall_status(id) ON DELETE SET NULL,
    start_date DATE,
    primary_purpose_id INTEGER REFERENCES processed.primary_purpose(id) ON DELETE SET NULL,
    study_type_id INTEGER REFERENCES processed.study_type(id) ON DELETE SET NULL,
    phase_id INTEGER REFERENCES processed.phase(id) ON DELETE SET NULL,
    outcome_measure TEXT,
    medical_subject_heading TEXT,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Indexes for faster querying
CREATE INDEX IF NOT EXISTS idx_study_type ON processed.studies(study_type_id);
CREATE INDEX IF NOT EXISTS idx_phase ON processed.studies(phase_id);
CREATE INDEX IF NOT EXISTS idx_start_date ON processed.studies(start_date);
CREATE INDEX IF NOT EXISTS indx_condition_id ON processed.study_conditions(condition_id);
CREATE INDEX IF NOT EXISTS indx_intervention_id ON processed.study_interventions(intervention_id);