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
         │                 │                   │
         v                 v                   v
┌─────────────────────────────────────────────────────────────────┐
│                    INGESTION LAYER (Python)                     │
├─────────────────┬─────────────────┬─────────────────────────────┤
│  CSV Loader     │  API Connector  │  DB Connector               │
│  (pandas)       │  (requests)     │  (SQLAlchemy)               │
└────────┬────────┴────────┬────────┴──────────┬──────────────────┘
         │                 │                   │
         v                 v                   v
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
         │                 │                   │
         v                 v                   v
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
         │                 │                   │
         v                 v                   v
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

## Project Structure

```
clinical-trials-exploration/
├── configs/                          # Configuration management
│   └── app_config.py                 # Main application configuration & logging setup
├── data/                             # Data storage directory
│   ├── notebooks/                    # Jupyter notebooks for exploration
│   │   └── explore_clin_trials_csv.ipynb
│   ├── processed/                    # Processed/cleaned data files
│   └── raw/                          # Raw data files
│       └── clin_trials.csv          # Source CSV data
├── data_models/                      # Data models and schemas
│   ├── settings.py                   # Settings dataclass definition
│   └── studies_raw.py               # Raw studies data model
├── db/                              # Database related code
│   ├── db_connection.py             # Database connection management
│   └── schemas/                     # SQL schema definitions
│       ├── create_processed_tables.sql  # Production tables
│       ├── create_schemas.sql           # Database schemas
│       └── create_staging_tables.sql    # Staging/raw tables
├── db_data/                         # Database data directory (Docker volumes)
├── logs/                            # Application logs (auto-generated)
│   └── log-DD-MM-YYYY.log          # Daily log files (format: log-DD-MM-YYYY.log)
├── parsers/                         # Data parsing modules
│   └── studies_csv_parser.py        # CSV file parser for clinical trials data
├── scripts/                         # Executable scripts
│   ├── csv_to_staging.py           # CSV ingestion to staging tables
│   ├── init_db.py                  # Database schema initialization
│   └── raw_to_processed.py         # Data transformation pipeline
├── tests/                           # Unit tests
│   ├── csv_integration_test.py      # Integration tests for CSV processing
│   ├── csv_to_staging_simple_test.py # Simple unit tests for CSV staging
│   ├── csv_to_staging_test.py       # Comprehensive CSV staging tests
│   ├── db_connection_test.py        # Database connection tests
│   ├── db_init_test.py             # Database initialization tests
│   └── test_data/                   # Test data files
│       ├── clin_trials_empty.csv
│       ├── clin_trials_malformed.csv
│       ├── clin_trials_test.csv
│       └── clin_trials_wrong_headers.csv
├── .env                             # Environment variables (not in git)
├── .env.example                     # Environment variables template
├── .gitignore                       # Git ignore rules
├── docker-compose.yaml              # Docker services configuration
├── main.py                          # Main application entry point
├── pyproject.toml                   # Python project configuration
├── requirements.txt                 # Production dependencies (minimal)
└── requirements-dev.txt             # Development dependencies
```

### Key Components

#### **Data Directory (`data/`)**
- **`notebooks/`**: Jupyter notebooks for data exploration and prototyping
- **`raw/`**: Place your raw data files here (e.g., `clin_trials.csv`)
- **`processed/`**: Processed/cleaned data files

#### **Configuration (`configs/`)**
- **`app_config.py`**: Centralized configuration management with environment variable validation and logging setup. Supports daily log rotation and handles different entry points (csv_to_staging, init_db).

#### **Data Models (`data_models/`)**
- **`settings.py`**: Dataclass defining application settings schema
- **`studies_raw.py`**: Data model for raw clinical trials studies

#### **Database (`db/`)**
- **`db_connection.py`**: SQLAlchemy-based database connection management with connection pooling
- **`schemas/`**: SQL scripts for database schema creation (staging, processed, and analytics layers)

#### **Data Processing (`parsers/`, `scripts/`)**
- **`parsers/`**: Modular data parsing components for different data sources
- **`scripts/`**: Executable data pipeline scripts for ingestion and transformation

#### **Testing (`tests/`)**
- Comprehensive test suite with unit tests, integration tests, and test data
- Tests adapted to work with the new configuration system using `Settings` dataclass

#### **Logging (`logs/`)**
- Daily rotating log files with format: `log-DD-MM-YYYY.log`
- Dual output: console and file logging
- Configurable log levels (DEBUG, INFO, WARNING, ERROR)

### Dependencies
- **`requirements.txt`**: Production/runtime dependencies (minimal for deployment)
- **`requirements-dev.txt`**: Development dependencies (includes testing, linting, formatting tools)

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
BATCH_SIZE=1000
ENABLE_BACKFILL=True
ENTRY_POINT=csv_to_staging
FILE_PATH=file/path/example.csv
# ENTRY_POINT=init_db
DB_URL=postgresql://${POSTGRES_USER}:${POSTGRES_PASSWORD}@${POSTGRES_HOST}:${POSTGRES_PORT}/${POSTGRES_DB}
```

LOG_LEVEL options:
- DEBUG - Shows all messages
- INFO - Default level, shows info, warnings, and errors
- ERROR - Only errors and critical issues

BATCH_SIZE - optional (default: 1000) - Number of records to process in each batch during data ingestion
ENABLE_BACKFILL - optional (default: True) - Whether to backfill existing data during ingestion (True/False)
ENTRY_POINT - required - The main function to run, options:
- csv_to_staging - Ingest CSV data to staging tables
- init_db - Initialize database schema
- TODO: add other entry points as implemented
FILE_PATH - required only with csv_to_staging - Path to the CSV file to ingest (used with csv_to_staging)

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

## Running Unit Tests
Run unit tests with coverage:
```bash
coverage run -m unittest discover -s tests -p "*_test.py"
```

## Design Decision Breakdown

### Data Processing
- **Pandas** I chose it as a csv processing tool for its powerful DataFrame API and I have used it before, so I can fit into time constraints.

### Database
- **PostgreSQL** I chose to set it up locally using Docker for ease of setup and consistency across environments and it's pretty easy to scale later if needed, although for production a managed cloud DB would be better.
- **SQLAlchemy** I chose to use ORM for database interactions to abstract away raw SQL and improve maintainability and I can easily switch to another database backend if needed, although I would need to update the connection strings and possibly some queries.

### Web Framework
- **Streamlit** it seems like a good fit for quickly building data exploration UIs and dashboards with minimal code.

## Time Allocation Breakdown
- Initial Setup & Research: 3 hours
- Docker & PostgreSQL Setup: 3 hours (it took a while to get Docker working properly on my machine, as I had to learn some basics)
- DB schema design: 2 hours
- DB connection & schema initialization scripts: 2 hours
- Data ingestion scripts: 3 hours
- Unit tests & pre-commit setup: 2 hours

## Next Steps / TODOs
- Finish implementing data transformation and processing logic
- Implement data enrichment (standardization, derived fields)
- Add data validation and error handling
- Implement analytics layer with aggregations and metrics
- Create parser for JSON API data ingestion for: https://clinicaltrials.gov/api/v2/studies
- Create parser for XML data ingestion for: https://www.kaggle.com/datasets/skylord/all-clinical-trials
- Improve data ingestion performance for large datasets
- Create tests for data ingestion and processing
- Optimize database queries and indexing
- Create Streamlit app for data exploration and visualization