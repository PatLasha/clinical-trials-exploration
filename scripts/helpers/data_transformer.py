import logging
from datetime import datetime, timezone
from typing import Optional

from data_models.table_models.raw_studies import RawStudies


class DataTransformer:
    """Transforms raw study data for processed tables."""

    def __init__(self):
        self.logger = logging.getLogger(__name__)

    def split_multi_value(self, value: Optional[str], delimiter: str = ",") -> list[str]:
        """
        Split multi-value strings into list.
        :param value: String with multiple values
        :param delimiter: Delimiter to split on
        :return: List of cleaned values
        """
        if not value:
            return []
        return [v.strip() for v in value.split(delimiter) if v.strip()]

    def parse_date(self, date_str: Optional[str]) -> Optional[datetime]:
        """
        Parse date string to datetime with UTC timezone.
        :param date_str: Date string to parse
        :return: datetime object with UTC timezone or None
        """
        if not date_str:
            return None

        # Strip and clean the date string
        date_str = date_str.strip()

        # Skip if it's clearly not a date (too short or contains letters besides month abbreviations)
        if len(date_str) < 4 or any(c.isalpha() and c not in "JanFebMarAprMayJunJulAugSepOctNovDec" for c in date_str):
            return None

        # Try different date formats
        formats = [
            "%Y-%m-%d",  # 2021-10-18
            "%Y-%m",  # 2004-10
            "%Y",  # 2021
        ]

        for fmt in formats:
            try:
                dt = datetime.strptime(date_str, fmt)
                # Add UTC timezone
                return dt.replace(tzinfo=timezone.utc)
            except ValueError:
                continue

        self.logger.warning(f"Could not parse date: {date_str}")
        return None

    def extract_conditions(self, study: RawStudies) -> list[str]:
        """
        Extract and clean conditions from study.
        :param study: RawStudies instance
        :return: List of condition strings
        """
        return self.split_multi_value(study.conditions)

    def extract_interventions(self, study: RawStudies) -> list[dict]:
        """
        Extract interventions with descriptions from study.
        :param study: RawStudies instance
        :return: List of dicts with intervention and description
        """
        interventions = self.split_multi_value(study.interventions)
        descriptions = self.split_multi_value(study.intervention_description)

        # Pair interventions with descriptions (or None if no description)
        result = []
        for i, intervention in enumerate(interventions):
            description = descriptions[i] if i < len(descriptions) else None
            result.append({"intervention": intervention, "description": description})

        return result

    def extract_age_groups(self, study: RawStudies) -> list[str]:
        """
        Extract and clean age groups from study.
        :param study: RawStudies instance
        :return: List of age group strings
        """
        # Age groups are separated by space or underscore (e.g., "ADULT OLDER_ADULT")
        if not study.standard_age:
            return []

        # Replace underscores with spaces and split
        age_str = study.standard_age.replace("_", " ")
        return [age.strip() for age in age_str.split() if age.strip()]

    def transform_study(self, study: RawStudies) -> dict:
        """
        Transform raw study into dict ready for processed tables.
        :param study: RawStudies instance
        :return: Dict with transformed data
        """
        return {
            "org_name": study.org_name,
            "org_class": study.org_class,
            "responsible_party": study.responsible_party,
            "brief_title": study.brief_title,
            "full_title": study.full_title,
            "overall_status": study.overall_status,
            "start_date": self.parse_date(study.start_date),
            "primary_purpose": study.primary_purpose,
            "study_type": study.study_type,
            "phase": study.phase,
            "outcome_measure": study.outcome_measure,
            "medical_subject_heading": study.medical_subject_heading,
            "conditions": self.extract_conditions(study),
            "interventions": self.extract_interventions(study),
            "age_groups": self.extract_age_groups(study),
        }
