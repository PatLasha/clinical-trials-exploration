# Data Processing Helper Modules

## Overview
Minimal helper modules for validating and transforming raw clinical trials data.

## Modules

### `data_validator.py`
Validates raw study data before transformation.

**Methods:**
- `validate_study(study)` - Validates single study (checks row_id, titles)
- `validate_batch(studies)` - Validates batch, returns (valid, invalid) tuple

### `data_transformer.py`
Transforms raw study data for processed tables.

**Methods:**
- `split_multi_value(value, delimiter)` - Splits comma-separated values
- `parse_date(date_str)` - Parses dates (YYYY-MM-DD or YYYY-MM)
- `extract_conditions(study)` - Extracts list of conditions
- `extract_interventions(study)` - Extracts interventions with descriptions
- `extract_age_groups(study)` - Extracts age groups (handles space/underscore separators)
- `transform_study(study)` - Full transformation returning dict

## Usage Example

```python
from scripts.helpers import DataValidator, DataTransformer

validator = DataValidator()
transformer = DataTransformer()

# Validate
valid, invalid = validator.validate_batch(raw_studies)

# Transform
for study in valid:
    transformed = transformer.transform_study(study)
    # transformed dict contains:
    # - conditions: list[str]
    # - interventions: list[dict] with 'intervention' and 'description'
    # - age_groups: list[str]
    # - start_date: datetime object
```

## Data Format Handling

### Multi-value Fields
- **Conditions**: `"Condition1, Condition2, Condition3"` → `["Condition1", "Condition2", "Condition3"]`
  - Empty/None → `[]` (empty list)
  - Spaces trimmed automatically
  
- **Interventions**: `"Drug A, Drug B"` → `[{"intervention": "Drug A", "description": "..."}, ...]`
  - Empty/None → `[]` (empty list)
  - More interventions than descriptions → Remaining get `None` as description
  - Empty values filtered out
  
- **Age Groups**: `"ADULT OLDER_ADULT"` or `"ADULT_OLDER_ADULT"` → `["ADULT", "OLDER", "ADULT"]`
  - Handles both space and underscore separators
  - Empty/None → `[]` (empty list)
  - Underscores replaced with spaces before splitting

### Date Parsing
- Full format: `"2021-10-18"` → `datetime(2021, 10, 18, tzinfo=timezone.utc)`
- Partial format: `"2004-10"` → `datetime(2004, 10, 1, tzinfo=timezone.utc)`
- Year only: `"2022"` → `datetime(2022, 1, 1, tzinfo=timezone.utc)`
- Invalid/Non-date/None: Returns `None` (e.g., `"COMPLETED"` → `None`)
  - Strings with non-date text are automatically skipped
  - Malformed dates log warning and return `None`

### String Fields
All text fields (org_name, brief_title, etc.) are passed through as-is:
- `None` → `None` (preserved)
- Empty string `""` → `""` (preserved)
- Whitespace preserved (not trimmed)

### Field-by-Field Transformation

| Field | Input Type | Output Type | Missing/Invalid Handling |
|-------|-----------|-------------|-------------------------|
| `org_name` | string | string | `None` → `None` |
| `org_class` | string | string | `None` → `None` |
| `responsible_party` | string | string | `None` → `None` |
| `brief_title` | string | string | `None` → `None` (but validation may flag) |
| `full_title` | string | string | `None` → `None` |
| `overall_status` | string | string | `None` → `None` |
| `start_date` | string | datetime with UTC | Invalid → `None`, logs warning |
| `primary_purpose` | string | string | `None` → `None` |
| `study_type` | string | string | `None` → `None` |
| `phase` | string | string | `None` → `None` |
| `outcome_measure` | string | string | `None` → `None` |
| `medical_subject_heading` | string | string | `None` → `None` |
| `conditions` | string (comma-sep) | list[string] | `None` → `[]` |
| `interventions` | string (comma-sep) | list[dict] | `None` → `[]` |
| `age_groups` | string (space/underscore) | list[string] | `None` → `[]` |

## Validation Rules

The `DataValidator` performs the following checks before transformation:

1. **Required Fields**:
   - `row_id` must be present (not `None`)
   - At least one title (`brief_title` or `full_title`) must be present

2. **Validation Results**:
   - Valid studies pass all checks
   - Invalid studies are separated and logged
   - Only valid studies proceed to transformation

3. **Batch Processing**:
   - Returns tuple: `(valid_studies, invalid_studies)`
   - Logs count of valid vs invalid
   - Allows caller to handle invalid studies separately

## Tests
- **data_validator_test.py**: 5 tests (all passing)
- **data_transformer_test.py**: 17 tests (all passing)
- Total: 22 tests covering all functionality

## Recent Updates
- Added UTC timezone support for all parsed dates
- Enhanced date parsing to handle YYYY format (year only)
- Added validation to skip non-date values (e.g., status fields)
