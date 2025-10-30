# SQLAlchemy Table Models

This directory contains the SQLAlchemy ORM models for the clinical trials database.

## Structure

### `processed_data.py`
Contains 14 SQLAlchemy models for the processed schema tables:

#### Lookup Tables
- **OrgClass** - Organization class types (`org_class`)
- **ResponsibleParty** - Responsible party types (`responsible_party`)
- **OverallStatus** - Study status values (`overall_status`)
- **PrimaryPurpose** - Study purposes (`primary_purpose`)
- **StudyType** - Study type classifications (`study_type`)
- **Phase** - Clinical trial phases (`phase`)
- **StandardAge** - Age group classifications (`standard_age`)

#### Entity Tables
- **Organization** - Organizations/agencies (`organization`)
- **Condition** - Medical conditions (`condition`)
- **Intervention** - Medical interventions (`intervention`)
- **Studies** - Main studies table with foreign keys to lookup tables (`studies`)

#### Bridge Tables (Many-to-Many Relationships)
- **StudyConditions** - Links studies to conditions (`study_conditions`)
- **StudyAgeGroups** - Links studies to age groups (`study_age_groups`)
- **StudyInterventions** - Links studies to interventions (`study_interventions`)

### `raw_studies.py`
Contains the SQLAlchemy model for the raw staging table:
- **RawStudies** - Raw CSV data import table (`raw_studies`)

## Schema Configuration

All models are properly configured for PostgreSQL with:
- **Schema**: All processed models use the `processed` schema
- **Indexes**: Performance indexes on frequently queried columns
- **Foreign Keys**: Proper relationships between tables
- **Constraints**: Primary keys, unique constraints, and null constraints

## Usage

```python
from data_models.table_models.processed_data import Studies, Condition, Intervention
from data_models.table_models.raw_studies import RawStudies

# Create a new study
study = Studies(
    brief_title="Example Clinical Trial",
    full_title="A Comprehensive Example Clinical Trial"
)

# Create a condition
condition = Condition(condition="Diabetes Type 2")

# Models are ready for SQLAlchemy session operations
```

## Relationships

The models include proper SQLAlchemy relationships:
- Studies ↔ Conditions (Many-to-Many via StudyConditions)
- Studies ↔ Interventions (Many-to-Many via StudyInterventions)  
- Studies ↔ Age Groups (Many-to-Many via StudyAgeGroups)
- Studies → Organization (Many-to-One)
- Studies → Lookup tables (Many-to-One for status, type, phase, etc.)

## Database Integration

These models work with the existing database schema defined in:
- `db/schemas/create_processed_tables.sql`
- `db/schemas/create_staging_tables.sql`

The SQLAlchemy models provide an ORM layer over the existing SQL schema structure.