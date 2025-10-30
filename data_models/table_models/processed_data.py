from datetime import datetime

from base import Base
from sqlalchemy import Column, Date, DateTime, ForeignKey, Index, Integer, String, Text
from sqlalchemy.orm import relationship

# Lookup Tables (Reference Data)


class OrgClass(Base):
    """Organization class lookup table."""

    __tablename__ = "org_class"
    __table_args__ = ({"schema": "processed"},)

    id = Column(Integer, primary_key=True, autoincrement=True)
    org_class = Column(String(50), nullable=False, unique=True)

    # Relationships
    organizations = relationship("Organization", back_populates="org_class_ref")

    def __repr__(self) -> str:
        return f"<OrgClass(id={self.id}, org_class='{self.org_class}')>"


class ResponsibleParty(Base):
    """Responsible party lookup table."""

    __tablename__ = "responsible_party"
    __table_args__ = ({"schema": "processed"},)

    id = Column(Integer, primary_key=True, autoincrement=True)
    responsible_party = Column(String(50), nullable=False, unique=True)

    # Relationships
    studies = relationship("Studies", back_populates="responsible_party_ref")

    def __repr__(self) -> str:
        return f"<ResponsibleParty(id={self.id}, responsible_party='{self.responsible_party}')>"


class OverallStatus(Base):
    """Overall status lookup table."""

    __tablename__ = "overall_status"
    __table_args__ = ({"schema": "processed"},)

    id = Column(Integer, primary_key=True, autoincrement=True)
    overall_status = Column(String(50), nullable=False, unique=True)

    # Relationships
    studies = relationship("Studies", back_populates="overall_status_ref")

    def __repr__(self) -> str:
        return f"<OverallStatus(id={self.id}, overall_status='{self.overall_status}')>"


class PrimaryPurpose(Base):
    """Primary purpose lookup table."""

    __tablename__ = "primary_purpose"
    __table_args__ = ({"schema": "processed"},)

    id = Column(Integer, primary_key=True, autoincrement=True)
    primary_purpose = Column(String(50), nullable=False, unique=True)

    # Relationships
    studies = relationship("Studies", back_populates="primary_purpose_ref")

    def __repr__(self) -> str:
        return f"<PrimaryPurpose(id={self.id}, primary_purpose='{self.primary_purpose}')>"


class StudyType(Base):
    """Study type lookup table."""

    __tablename__ = "study_type"
    __table_args__ = ({"schema": "processed"},)

    id = Column(Integer, primary_key=True, autoincrement=True)
    study_type = Column(String(50), nullable=False, unique=True)

    # Relationships
    studies = relationship("Studies", back_populates="study_type_ref")

    def __repr__(self) -> str:
        return f"<StudyType(id={self.id}, study_type='{self.study_type}')>"


class Phase(Base):
    """Phase lookup table."""

    __tablename__ = "phase"
    __table_args__ = ({"schema": "processed"},)

    id = Column(Integer, primary_key=True, autoincrement=True)
    phase = Column(String(50), nullable=False, unique=True)

    # Relationships
    studies = relationship("Studies", back_populates="phase_ref")

    def __repr__(self) -> str:
        return f"<Phase(id={self.id}, phase='{self.phase}')>"


class StandardAge(Base):
    """Standard age lookup table."""

    __tablename__ = "standard_age"
    __table_args__ = ({"schema": "processed"},)

    id = Column(Integer, primary_key=True, autoincrement=True)
    standard_age = Column(String(50), nullable=False, unique=True)

    # Relationships
    study_age_groups = relationship("StudyAgeGroups", back_populates="age_group")

    def __repr__(self) -> str:
        return f"<StandardAge(id={self.id}, standard_age='{self.standard_age}')>"


# Main Entity Tables


class Organization(Base):
    """Organization main entity table."""

    __tablename__ = "organization"
    __table_args__ = ({"schema": "processed"},)

    id = Column(Integer, primary_key=True, autoincrement=True)
    org_name = Column(String(255), nullable=False, unique=True)
    org_class_id = Column(Integer, ForeignKey("processed.org_class.id", ondelete="SET NULL"), nullable=True)

    # Relationships
    org_class_ref = relationship("OrgClass", back_populates="organizations")
    studies = relationship("Studies", back_populates="organization")

    def __repr__(self) -> str:
        return f"<Organization(id={self.id}, org_name='{self.org_name}')>"


class Studies(Base):
    """Main studies table."""

    __tablename__ = "studies"
    __table_args__ = (
        Index("idx_study_type", "study_type_id"),
        Index("idx_phase", "phase_id"),
        Index("idx_start_date", "start_date"),
        {"schema": "processed"},
    )

    study_id = Column(Integer, primary_key=True, autoincrement=True)
    org_id = Column(Integer, ForeignKey("processed.organization.id", ondelete="SET NULL"), nullable=True)
    responsible_party_id = Column(
        Integer, ForeignKey("processed.responsible_party.id", ondelete="SET NULL"), nullable=True
    )
    brief_title = Column(Text, nullable=True)
    full_title = Column(Text, nullable=True)
    overall_status_id = Column(Integer, ForeignKey("processed.overall_status.id", ondelete="SET NULL"), nullable=True)
    start_date = Column(Date, nullable=True)
    primary_purpose_id = Column(Integer, ForeignKey("processed.primary_purpose.id", ondelete="SET NULL"), nullable=True)
    study_type_id = Column(Integer, ForeignKey("processed.study_type.id", ondelete="SET NULL"), nullable=True)
    phase_id = Column(Integer, ForeignKey("processed.phase.id", ondelete="SET NULL"), nullable=True)
    outcome_measure = Column(Text, nullable=True)
    medical_subject_heading = Column(Text, nullable=True)
    created_at = Column(DateTime, default=datetime.utcnow, nullable=False)
    updated_at = Column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow, nullable=False)

    # Relationships
    organization = relationship("Organization", back_populates="studies")
    responsible_party_ref = relationship("ResponsibleParty", back_populates="studies")
    overall_status_ref = relationship("OverallStatus", back_populates="studies")
    primary_purpose_ref = relationship("PrimaryPurpose", back_populates="studies")
    study_type_ref = relationship("StudyType", back_populates="studies")
    phase_ref = relationship("Phase", back_populates="studies")

    # Many-to-many relationships
    age_groups = relationship("StudyAgeGroups", back_populates="study")
    conditions = relationship("StudyConditions", back_populates="study")
    interventions = relationship("StudyInterventions", back_populates="study")

    def __repr__(self) -> str:
        title_preview = "None"
        if self.brief_title is not None:
            title_str = str(self.brief_title)
            title_preview = title_str[:50] + "..." if len(title_str) > 50 else title_str
        return f"<Studies(study_id={self.study_id}, brief_title='{title_preview}')>"


class Condition(Base):
    """Conditions entity table."""

    __tablename__ = "condition"
    __table_args__ = ({"schema": "processed"},)

    condition_id = Column(Integer, primary_key=True, autoincrement=True)
    condition = Column(String(255), nullable=False, unique=True)
    created_at = Column(DateTime, default=datetime.utcnow, nullable=False)

    # Relationships
    study_conditions = relationship("StudyConditions", back_populates="condition_ref")

    def __repr__(self) -> str:
        return f"<Condition(condition_id={self.condition_id}, condition='{self.condition}')>"


class Intervention(Base):
    """Interventions entity table."""

    __tablename__ = "intervention"
    __table_args__ = ({"schema": "processed"},)

    intervention_id = Column(Integer, primary_key=True, autoincrement=True)
    intervention = Column(String(255), nullable=False, unique=True)
    intervention_description = Column(Text, nullable=True)
    created_at = Column(DateTime, default=datetime.utcnow, nullable=False)

    # Relationships
    study_interventions = relationship("StudyInterventions", back_populates="intervention_ref")

    def __repr__(self) -> str:
        return f"<Intervention(intervention_id={self.intervention_id}, intervention='{self.intervention}')>"


# Bridge Tables (Many-to-Many Relationships)


class StudyAgeGroups(Base):
    """Bridge table for study age groups relationship."""

    __tablename__ = "study_age_groups"
    __table_args__ = ({"schema": "processed"},)

    study_id = Column(Integer, ForeignKey("processed.studies.study_id"), primary_key=True)
    age_group_id = Column(Integer, ForeignKey("processed.standard_age.id"), primary_key=True)

    # Relationships
    study = relationship("Studies", back_populates="age_groups")
    age_group = relationship("StandardAge", back_populates="study_age_groups")

    def __repr__(self) -> str:
        return f"<StudyAgeGroups(study_id={self.study_id}, age_group_id={self.age_group_id})>"


class StudyConditions(Base):
    """Bridge table for study conditions."""

    __tablename__ = "study_conditions"
    __table_args__ = (Index("indx_condition_id", "condition_id"), {"schema": "processed"})

    study_id = Column(Integer, ForeignKey("processed.studies.study_id"), primary_key=True)
    condition_id = Column(Integer, ForeignKey("processed.condition.condition_id"), primary_key=True)

    # Relationships
    study = relationship("Studies", back_populates="conditions")
    condition_ref = relationship("Condition", back_populates="study_conditions")

    def __repr__(self) -> str:
        return f"<StudyConditions(study_id={self.study_id}, condition_id={self.condition_id})>"


class StudyInterventions(Base):
    """Bridge table for study interventions."""

    __tablename__ = "study_interventions"
    __table_args__ = (Index("indx_intervention_id", "intervention_id"), {"schema": "processed"})

    study_id = Column(Integer, ForeignKey("processed.studies.study_id"), primary_key=True)
    intervention_id = Column(Integer, ForeignKey("processed.intervention.intervention_id"), primary_key=True)

    # Relationships
    study = relationship("Studies", back_populates="interventions")
    intervention_ref = relationship("Intervention", back_populates="study_interventions")

    def __repr__(self) -> str:
        return f"<StudyInterventions(study_id={self.study_id}, intervention_id={self.intervention_id})>"
