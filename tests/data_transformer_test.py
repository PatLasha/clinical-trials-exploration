import unittest
from datetime import datetime, timezone
from unittest.mock import Mock

from data_models.table_models.raw_studies import RawStudies
from scripts.helpers.data_transformer import DataTransformer


class TestDataTransformer(unittest.TestCase):
    """Unit tests for DataTransformer."""

    def setUp(self):
        self.transformer = DataTransformer()

    def test_split_multi_value(self):
        """Test splitting multi-value strings."""
        result = self.transformer.split_multi_value("value1, value2, value3")
        self.assertEqual(result, ["value1", "value2", "value3"])

    def test_split_multi_value_empty(self):
        """Test splitting empty string."""
        result = self.transformer.split_multi_value(None)
        self.assertEqual(result, [])

    def test_split_multi_value_with_spaces(self):
        """Test splitting with extra spaces."""
        result = self.transformer.split_multi_value("  value1  ,  value2  ")
        self.assertEqual(result, ["value1", "value2"])

    def test_parse_date_full_format(self):
        """Test parsing date in YYYY-MM-DD format."""
        result = self.transformer.parse_date("2021-10-18")
        self.assertEqual(result, datetime(2021, 10, 18, tzinfo=timezone.utc))

    def test_parse_date_partial_format(self):
        """Test parsing date in YYYY-MM format."""
        result = self.transformer.parse_date("2004-10")
        self.assertEqual(result, datetime(2004, 10, 1, tzinfo=timezone.utc))

    def test_parse_date_year_only(self):
        """Test parsing date in YYYY format."""
        result = self.transformer.parse_date("2022")
        self.assertEqual(result, datetime(2022, 1, 1, tzinfo=timezone.utc))

    def test_parse_date_invalid(self):
        """Test parsing invalid date."""
        result = self.transformer.parse_date("invalid-date")
        self.assertIsNone(result)

    def test_parse_date_non_date_value(self):
        """Test parsing non-date values like status."""
        result = self.transformer.parse_date("COMPLETED")
        self.assertIsNone(result)

    def test_parse_date_none(self):
        """Test parsing None date."""
        result = self.transformer.parse_date(None)
        self.assertIsNone(result)

    def test_extract_conditions(self):
        """Test extracting conditions from study."""
        study = Mock(spec=RawStudies)
        study.conditions = "Condition1, Condition2, Condition3"

        result = self.transformer.extract_conditions(study)
        self.assertEqual(result, ["Condition1", "Condition2", "Condition3"])

    def test_extract_interventions_with_descriptions(self):
        """Test extracting interventions with descriptions."""
        study = Mock(spec=RawStudies)
        study.interventions = "Drug A, Drug B"
        study.intervention_description = "Description A, Description B"

        result = self.transformer.extract_interventions(study)
        self.assertEqual(len(result), 2)
        self.assertEqual(result[0]["intervention"], "Drug A")
        self.assertEqual(result[0]["description"], "Description A")
        self.assertEqual(result[1]["intervention"], "Drug B")
        self.assertEqual(result[1]["description"], "Description B")

    def test_extract_interventions_more_interventions_than_descriptions(self):
        """Test extracting when interventions outnumber descriptions."""
        study = Mock(spec=RawStudies)
        study.interventions = "Drug A, Drug B, Drug C"
        study.intervention_description = "Description A"

        result = self.transformer.extract_interventions(study)
        self.assertEqual(len(result), 3)
        self.assertEqual(result[0]["intervention"], "Drug A")
        self.assertEqual(result[0]["description"], "Description A")
        self.assertEqual(result[1]["intervention"], "Drug B")
        self.assertIsNone(result[1]["description"])
        self.assertEqual(result[2]["intervention"], "Drug C")
        self.assertIsNone(result[2]["description"])

    def test_extract_age_groups_space_separated(self):
        """Test extracting age groups with space separator."""
        study = Mock(spec=RawStudies)
        study.standard_age = "ADULT OLDER_ADULT"

        result = self.transformer.extract_age_groups(study)
        self.assertEqual(result, ["ADULT", "OLDER", "ADULT"])

    def test_extract_age_groups_underscore_separated(self):
        """Test extracting age groups with underscore separator."""
        study = Mock(spec=RawStudies)
        study.standard_age = "ADULT_OLDER_ADULT"

        result = self.transformer.extract_age_groups(study)
        self.assertEqual(result, ["ADULT", "OLDER", "ADULT"])

    def test_extract_age_groups_single(self):
        """Test extracting single age group."""
        study = Mock(spec=RawStudies)
        study.standard_age = "CHILD"

        result = self.transformer.extract_age_groups(study)
        self.assertEqual(result, ["CHILD"])

    def test_extract_age_groups_none(self):
        """Test extracting age groups when None."""
        study = Mock(spec=RawStudies)
        study.standard_age = None

        result = self.transformer.extract_age_groups(study)
        self.assertEqual(result, [])

    def test_transform_study(self):
        """Test full study transformation."""
        study = Mock(spec=RawStudies)
        study.org_name = "Test Organization"
        study.org_class = "OTHER"
        study.responsible_party = "SPONSOR"
        study.brief_title = "Test Study"
        study.full_title = "Full Test Study Title"
        study.overall_status = "COMPLETED"
        study.start_date = "2021-10-18"
        study.primary_purpose = "TREATMENT"
        study.study_type = "INTERVENTIONAL"
        study.phase = "PHASE2"
        study.outcome_measure = "Test Outcome"
        study.medical_subject_heading = "Test MeSH"
        study.conditions = "Condition1, Condition2"
        study.interventions = "Drug A, Drug B"
        study.intervention_description = "Desc A, Desc B"
        study.standard_age = "ADULT OLDER_ADULT"

        result = self.transformer.transform_study(study)

        self.assertEqual(result["org_name"], "Test Organization")
        self.assertEqual(result["org_class"], "OTHER")
        self.assertEqual(result["start_date"], datetime(2021, 10, 18, tzinfo=timezone.utc))
        self.assertEqual(len(result["conditions"]), 2)
        self.assertEqual(len(result["interventions"]), 2)
        self.assertIn("ADULT", result["age_groups"])


if __name__ == "__main__":
    unittest.main()
