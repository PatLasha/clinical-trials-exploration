# Disclaimer

This project and even this readme is made using AI tools (mainly GitHub Copilot, Claude Sonnet 4 (and 4.5), Chat GPT-5) and is a work in progress and may contain incomplete sections, placeholders, or areas marked as "TO DO" that require further development.

# clinical-trials-exploration

Exploring public clinical trials dataset as a part of the DE tech challenge.

## High-Level Architecture

```
┌─────────────────────────────────────────────────────────────────┐
│                        DATA SOURCES LAYER                       │
├─────────────────┬─────────────────┬─────────────────────────────┤
│   CSV Files     │   JSON API      │   SQL Database              │
│     (Local)     │ (ClinicalTrials)│   (PostgreSQL)              │
└────────┬────────┴────────┬────────┴──────────┬──────────────────┘
         │                 │                    │
         v                 v                    v
┌─────────────────────────────────────────────────────────────────┐
│                    INGESTION LAYER (Python)                     │
├─────────────────┬─────────────────┬─────────────────────────────┤
│  CSV Loader     │  API Connector  │  DB Connector               │
│  (pandas)       │  (requests)     │  (SQLAlchemy)               │
└────────┬────────┴────────┬────────┴──────────┬──────────────────┘
         │                 │                    │
         v                 v                    v
┌─────────────────────────────────────────────────────────────────┐
│                   STAGING LAYER (Raw Zone)                      │
│                    PostgreSQL - staging schema                  │
│              Tables: raw_studies, raw_interventions, etc.       │
└──────────────────────────────┬──────────────────────────────────┘
                               │
                               v
┌─────────────────────────────────────────────────────────────────┐
│              TRANSFORMATION & VALIDATION LAYER                  │
├─────────────────┬─────────────────┬─────────────────────────────┤
│ Data Cleaning   │  Validation     │  Enrichment                 │
│ - Nulls         │  - Integrity    │  - Standardization          │
│ - Duplicates    │  - Constraints  │  - Derived Fields           │
│ - Formatting    │  - Business     │  - Lookups                  │
└────────┬────────┴────────┬────────┴──────────┬──────────────────┘
         │                 │                    │
         v                 v                    v
┌─────────────────────────────────────────────────────────────────┐
│                 PROCESSED LAYER (Gold Zone)                     │
│                PostgreSQL - processed schema                    │
│         Normalized Tables: studies, interventions, etc.         │
└──────────────────────────────┬──────────────────────────────────┘
                               │
                               v
┌─────────────────────────────────────────────────────────────────┐
│                    ANALYTICS LAYER                              │
├─────────────────┬─────────────────┬─────────────────────────────┤
│  Aggregations   │  Metrics Tables │  Reports                    │
│  (Materialized  │  (Pre-computed) │  (SQL Views)                │
│   Views)        │                 │                             │
└────────┬────────┴────────┬────────┴──────────┬──────────────────┘
         │                 │                    │
         v                 v                    v
┌─────────────────────────────────────────────────────────────────┐
│                   PRESENTATION LAYER                            │
│                   Streamlit Web Interface                       │
│              Dashboard | Data Upload | Analytics                │
└─────────────────────────────────────────────────────────────────┘
```

This architecture follows a modern data engineering pattern with:
- **Multi-source ingestion** supporting various data formats
- **Medallion architecture** (Bronze/Silver/Gold) with staging → processed layers  
- **Separation of concerns** between raw data, transformation, and analytics
- **Web-based presentation** for interactive exploration and visualization

# Clinical Trial Pipeline - Setup

This repository contains a clinical-trial data pipeline and UI. Below are recommended steps to set up local development and production (runtime) environments.

## Files
- `requirements.txt` — production / runtime dependencies (minimal).
- `requirements-dev.txt` — development dependencies (includes `-r requirements.txt`).

## Quick start (recommended: use a virtual environment)

1. Create & activate a virtualenv (Linux/macOS):
```bash
python3 -m venv .venv
source .venv/bin/activate
```

2. Install runtime deps (for deploying or running the app):
```bash
pip install -r requirements.txt
```

3. Install development deps (for development, linting, tests):
```bash
pip install -r requirements-dev.txt
```

## Environment Setup

### 1. Environment Variables

Copy the example environment file and configure your settings:

```bash
cp .env.example .env
```

Edit `.env` with your database credentials:
```bash
# Database Configuration
POSTGRES_USER=your_username
POSTGRES_PASSWORD=your_password
POSTGRES_DB=clinical_trials
POSTGRES_HOST=localhost
POSTGRES_PORT=5432

# Application Configuration
LOG_LEVEL=INFO
DB_URL=postgresql://${POSTGRES_USER}:${POSTGRES_PASSWORD}@${POSTGRES_HOST}:${POSTGRES_PORT}/${POSTGRES_DB}
```

### 2. Pre-commit Setup

Install and configure pre-commit hooks for code quality:

```bash
# Install pre-commit (included in requirements-dev.txt)
pip install -r requirements-dev.txt

# Install the git hook scripts
pre-commit install

# (Optional) Run against all files
pre-commit run --all-files
```

The pre-commit hooks will automatically run on each commit to:
- Format code with Black and isort
- Lint with Flake8
- Check file hygiene (trailing whitespace, YAML syntax, etc.)
- Run tests with coverage

### 3. PostgreSQL Database Setup

#### Option A: Using Docker (Recommended)

1. **Start the database:**
```bash
docker-compose up -d
```

2. **Initialize the database schema:**
The SQL scripts in `db/schemas/` will automatically run when the container starts for the first time:
- `create_schemas.sql` - Creates database schemas
- `create_staging_tables.sql` - Creates staging/raw tables
- `create_processed_tables.sql` - Creates processed/analytics tables

3. **Verify the setup:**
```bash
# Connect to the database
docker exec -it postgres_db psql -U your_username -d clinical_trials
```

**Initialize the schema:**
```bash
# Run the schema creation scripts
python scripts/init_db.py
```

### 4. Database Connection Test

## Test your database connection:
```bash
python scripts/db_connection.py
```

### TO DO
- Finish implementing configs, for logging and for environment variable handling
- Change logging statements throughout the codebase to use the logging module (moving logs to a file would be a good start)
- Add unit tests
- Improve error handling and validation
- Create data ingestion and processing scripts
- Create Streamlit app for data exploration and visualization