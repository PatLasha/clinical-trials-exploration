import unittest
from unittest.mock import Mock

from data_models.table_models.raw_studies import RawStudies
from scripts.helpers.data_validator import DataValidator


class TestDataValidator(unittest.TestCase):
    """Unit tests for DataValidator."""

    def setUp(self):
        self.validator = DataValidator()

    def test_validate_study_valid(self):
        """Test validation of a valid study."""
        study = Mock(spec=RawStudies)
        study.id = 1
        study.row_id = 100
        study.brief_title = "Test Study"
        study.full_title = "Full Test Study Title"

        result = self.validator.validate_study(study)
        self.assertTrue(result)

    def test_validate_study_missing_row_id(self):
        """Test validation fails when row_id is missing."""
        study = Mock(spec=RawStudies)
        study.id = 1
        study.row_id = None
        study.brief_title = "Test Study"

        result = self.validator.validate_study(study)
        self.assertFalse(result)

    def test_validate_study_missing_both_titles(self):
        """Test validation fails when both titles are missing."""
        study = Mock(spec=RawStudies)
        study.id = 1
        study.row_id = 100
        study.brief_title = None
        study.full_title = None

        result = self.validator.validate_study(study)
        self.assertFalse(result)

    def test_validate_study_only_brief_title(self):
        """Test validation passes with only brief_title."""
        study = Mock(spec=RawStudies)
        study.id = 1
        study.row_id = 100
        study.brief_title = "Test Study"
        study.full_title = None

        result = self.validator.validate_study(study)
        self.assertTrue(result)

    def test_validate_batch(self):
        """Test batch validation."""
        valid_study = Mock(spec=RawStudies)
        valid_study.id = 1
        valid_study.row_id = 100
        valid_study.brief_title = "Valid Study"
        valid_study.full_title = "Full Title"

        invalid_study = Mock(spec=RawStudies)
        invalid_study.id = 2
        invalid_study.row_id = None
        invalid_study.brief_title = "Invalid Study"

        studies = [valid_study, invalid_study]
        valid, invalid = self.validator.validate_batch(studies)

        self.assertEqual(len(valid), 1)
        self.assertEqual(len(invalid), 1)
        self.assertEqual(valid[0].id, 1)
        self.assertEqual(invalid[0].id, 2)


if __name__ == "__main__":
    unittest.main()
