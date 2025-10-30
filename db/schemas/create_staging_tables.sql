--- Create staging table for raw clinical trials data

CREATE TABLE IF NOT EXISTS staging.raw_studies (
    id SERIAL PRIMARY KEY,
    batch_id VARCHAR(50),
    ingestion_timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    source_file VARCHAR(255),
    raw_data JSONB NOT NULL, -- Storing entire row as json for flexibility
    row_id INTEGER UNIQUE, -- Unique identifier for each clinical trial
    org_name VARCHAR(255),
    org_class VARCHAR(50),
    responsible_party VARCHAR(255),
    brief_title TEXT,
    full_title TEXT,
    overall_status VARCHAR(50),
    start_date VARCHAR(50),
    standard_age VARCHAR(50),
    conditions TEXT,
    primary_purpose VARCHAR(50),
    interventions TEXT,
    intervention_description TEXT,
    study_type VARCHAR(50),
    phase VARCHAR(50),
    outcome_measure TEXT,
    medical_subject_heading TEXT
);

-- Indexes for performance optimization
CREATE INDEX IF NOT EXISTS idx_staging_raw_studies_row_id ON staging.raw_studies(row_id);
CREATE INDEX IF NOT EXISTS idx_staging_raw_studies_batch_id ON staging.raw_studies(batch_id);