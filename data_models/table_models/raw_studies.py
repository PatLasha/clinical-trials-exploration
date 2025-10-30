from datetime import datetime

from sqlalchemy import Column, DateTime, Index, Integer, String, Text
from sqlalchemy.dialects.postgresql import JSONB

from data_models.table_models.base import Base


class RawStudies(Base):
    """
    SQLAlchemy model for the staging.raw_studies table.
    Corresponds to the schema defined in create_staging_tables.sql.
    """

    __tablename__ = "raw_studies"
    __table_args__ = (
        Index("idx_staging_raw_studies_row_id", "row_id"),
        Index("idx_staging_raw_studies_batch_id", "batch_id"),
        {"schema": "staging"},
    )

    # Primary key
    id = Column(Integer, primary_key=True, autoincrement=True)

    # Metadata fields
    batch_id = Column(String(50), nullable=True)
    ingestion_timestamp = Column(DateTime, default=datetime.utcnow, nullable=True)
    source_file = Column(String(255), nullable=True)
    raw_data = Column(JSONB, nullable=False)  # Storing entire row as JSON for flexibility

    # Unique identifier for each clinical trial
    row_id = Column(Integer, unique=True, nullable=True)

    # Clinical trial fields
    org_name = Column(String(255), nullable=True)
    org_class = Column(String(50), nullable=True)
    responsible_party = Column(String(255), nullable=True)
    brief_title = Column(Text, nullable=True)
    full_title = Column(Text, nullable=True)
    overall_status = Column(String(50), nullable=True)
    start_date = Column(String(50), nullable=True)
    standard_age = Column(String(50), nullable=True)
    conditions = Column(Text, nullable=True)
    primary_purpose = Column(String(50), nullable=True)
    interventions = Column(Text, nullable=True)
    intervention_description = Column(Text, nullable=True)
    study_type = Column(String(50), nullable=True)
    phase = Column(String(50), nullable=True)
    outcome_measure = Column(Text, nullable=True)
    medical_subject_heading = Column(Text, nullable=True)

    def __repr__(self) -> str:
        """String representation of the RawStudies instance."""
        brief_title_preview = "None"
        if self.brief_title is not None:
            title_str = str(self.brief_title)
            brief_title_preview = title_str[:50] + "..." if len(title_str) > 50 else title_str
        return f"<RawStudies(id={self.id}, row_id={self.row_id}, brief_title='{brief_title_preview}')>"

    def to_dict(self) -> dict:
        """
        Convert the SQLAlchemy model instance to a dictionary.
        Useful for serialization and debugging.
        """
        return {
            "id": self.id,
            "batch_id": self.batch_id,
            "ingestion_timestamp": (
                self.ingestion_timestamp.isoformat() if self.ingestion_timestamp is not None else None
            ),
            "source_file": self.source_file,
            "raw_data": self.raw_data,
            "row_id": self.row_id,
            "org_name": self.org_name,
            "org_class": self.org_class,
            "responsible_party": self.responsible_party,
            "brief_title": self.brief_title,
            "full_title": self.full_title,
            "overall_status": self.overall_status,
            "start_date": self.start_date,
            "standard_age": self.standard_age,
            "conditions": self.conditions,
            "primary_purpose": self.primary_purpose,
            "interventions": self.interventions,
            "intervention_description": self.intervention_description,
            "study_type": self.study_type,
            "phase": self.phase,
            "outcome_measure": self.outcome_measure,
            "medical_subject_heading": self.medical_subject_heading,
        }

    @classmethod
    def from_dict(cls, data: dict) -> "RawStudies":
        """
        Create a RawStudies instance from a dictionary.
        Useful for creating instances from parsed CSV data.
        """
        # Filter out keys that don't correspond to model fields
        valid_fields = {
            "batch_id",
            "ingestion_timestamp",
            "source_file",
            "raw_data",
            "row_id",
            "org_name",
            "org_class",
            "responsible_party",
            "brief_title",
            "full_title",
            "overall_status",
            "start_date",
            "standard_age",
            "conditions",
            "primary_purpose",
            "interventions",
            "intervention_description",
            "study_type",
            "phase",
            "outcome_measure",
            "medical_subject_heading",
        }

        filtered_data = {key: value for key, value in data.items() if key in valid_fields}
        return cls(**filtered_data)
