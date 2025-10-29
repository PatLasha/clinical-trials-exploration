import dataclasses
from typing import Optional


@dataclasses.dataclass
class StudiesRaw:
    """
    Dataclass representing a row in the staging.raw_studies table.
    Matches the schema in create_staging_tables.sql.
    """

    batch_id: Optional[str] = None
    ingestion_timestamp: Optional[str] = None
    source_file: Optional[str] = None
    raw_data: Optional[str] = None
    row_id: Optional[int] = None
    org_name: Optional[str] = None
    org_class: Optional[str] = None
    responsible_party: Optional[str] = None
    brief_title: Optional[str] = None
    full_title: Optional[str] = None
    overall_status: Optional[str] = None
    start_date: Optional[str] = None
    standard_age: Optional[str] = None
    conditions: Optional[str] = None
    primary_purpose: Optional[str] = None
    interventions: Optional[str] = None
    intervention_description: Optional[str] = None
    study_type: Optional[str] = None
    phase: Optional[str] = None
    outcome_measure: Optional[str] = None
    medical_subject_heading: Optional[str] = None

    def to_dict(self) -> dict:
        """
        Convert the dataclass instance to a dictionary, excluding None values.
        :return: Dictionary representation of the dataclass.
        """
        return {k: v for k, v in dataclasses.asdict(self).items() if v is not None}
