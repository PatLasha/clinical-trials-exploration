import unittest
from datetime import datetime
from unittest.mock import MagicMock, Mock, patch

from data_models.settings import Settings
from data_models.table_models.raw_studies import RawStudies
from db.db_connection import DBConnection
from scripts.raw_to_processed import ProcessRawData


class TestProcessRawData(unittest.TestCase):
    """Unit tests for the ProcessRawData class."""

    def setUp(self):
        """Set up test fixtures."""
        self.test_settings = Settings(
            db_url="postgresql://test:test@localhost:5432/test_db",
            entry_point="test_entry",
            file_path="/test/path.csv",
            chunk_size=1000,
            enable_backfill=True,
            log_level="INFO",
        )

        # Create mock database connection
        self.mock_db = Mock(spec=DBConnection)
        self.processor = ProcessRawData(self.mock_db)

        # Create sample RawStudies objects for testing
        self.sample_studies = [
            self._create_mock_study(1, "batch_001", "Study 1", "Completed"),
            self._create_mock_study(2, "batch_001", "Study 2", "Active"),
            self._create_mock_study(3, "batch_002", "Study 3", "Recruiting"),
            self._create_mock_study(4, "batch_002", "Study 4", "Terminated"),
            self._create_mock_study(5, "batch_003", "Study 5", "Completed"),
        ]

    def _create_mock_study(self, study_id: int, batch_id: str, brief_title: str, status: str) -> Mock:
        """Helper method to create a mock RawStudies object."""
        study = Mock(spec=RawStudies)
        study.id = study_id
        study.batch_id = batch_id
        study.brief_title = brief_title
        study.overall_status = status
        study.row_id = study_id
        study.org_name = f"Organization {study_id}"
        study.ingestion_timestamp = datetime.now()
        study.raw_data = {"test": "data"}
        return study

    def test_get_all_raw_studies_success(self):
        """Test successful retrieval of all raw studies."""
        # Setup mock session
        mock_session = MagicMock()
        mock_query = MagicMock()
        mock_query.all.return_value = self.sample_studies
        mock_session.query.return_value = mock_query
        mock_session.__enter__ = Mock(return_value=mock_session)
        mock_session.__exit__ = Mock(return_value=False)

        self.mock_db.get_session.return_value = mock_session

        # Execute test
        result = self.processor.get_all_raw_studies()

        # Assertions
        self.assertEqual(len(result), 5)
        self.assertEqual(result, self.sample_studies)
        mock_session.query.assert_called_once_with(RawStudies)

    def test_get_all_raw_studies_empty(self):
        """Test retrieval when no studies exist."""
        # Setup mock session
        mock_session = MagicMock()
        mock_query = MagicMock()
        mock_query.all.return_value = []
        mock_session.query.return_value = mock_query
        mock_session.__enter__ = Mock(return_value=mock_session)
        mock_session.__exit__ = Mock(return_value=False)

        self.mock_db.get_session.return_value = mock_session

        # Execute test
        result = self.processor.get_all_raw_studies()

        # Assertions
        self.assertEqual(len(result), 0)
        self.assertIsInstance(result, list)

    def test_get_raw_studies_by_batch_success(self):
        """Test successful retrieval of studies by batch_id."""
        # Filter studies for batch_001
        batch_001_studies = [s for s in self.sample_studies if s.batch_id == "batch_001"]

        # Setup mock session
        mock_session = MagicMock()
        mock_query = MagicMock()
        mock_filter = MagicMock()
        mock_filter.all.return_value = batch_001_studies
        mock_query.filter.return_value = mock_filter
        mock_session.query.return_value = mock_query
        mock_session.__enter__ = Mock(return_value=mock_session)
        mock_session.__exit__ = Mock(return_value=False)

        self.mock_db.get_session.return_value = mock_session

        # Execute test
        result = self.processor.get_raw_studies("batch_001")

        # Assertions
        self.assertEqual(len(result), 2)
        self.assertTrue(all(s.batch_id == "batch_001" for s in result))
        mock_session.query.assert_called_once_with(RawStudies)

    def test_get_raw_studies_by_batch_not_found(self):
        """Test retrieval with batch_id that doesn't exist."""
        # Setup mock session
        mock_session = MagicMock()
        mock_query = MagicMock()
        mock_filter = MagicMock()
        mock_filter.all.return_value = []
        mock_query.filter.return_value = mock_filter
        mock_session.query.return_value = mock_query
        mock_session.__enter__ = Mock(return_value=mock_session)
        mock_session.__exit__ = Mock(return_value=False)

        self.mock_db.get_session.return_value = mock_session

        # Execute test
        result = self.processor.get_raw_studies("nonexistent_batch")

        # Assertions
        self.assertEqual(len(result), 0)

    def test_get_all_batch_ids_success(self):
        """Test successful retrieval of all unique batch IDs."""
        # Setup mock session
        mock_session = MagicMock()
        mock_query = MagicMock()
        mock_distinct = MagicMock()
        mock_distinct.all.return_value = [("batch_001",), ("batch_002",), ("batch_003",)]
        mock_query.distinct.return_value = mock_distinct
        mock_session.query.return_value = mock_query
        mock_session.__enter__ = Mock(return_value=mock_session)
        mock_session.__exit__ = Mock(return_value=False)

        self.mock_db.get_session.return_value = mock_session

        # Execute test
        result = self.processor.get_all_batch_ids()

        # Assertions
        self.assertEqual(len(result), 3)
        self.assertIn("batch_001", result)
        self.assertIn("batch_002", result)
        self.assertIn("batch_003", result)

    def test_get_all_batch_ids_with_none_values(self):
        """Test retrieval of batch IDs filters out None values."""
        # Setup mock session
        mock_session = MagicMock()
        mock_query = MagicMock()
        mock_distinct = MagicMock()
        mock_distinct.all.return_value = [("batch_001",), (None,), ("batch_002",), (None,), ("batch_003",)]
        mock_query.distinct.return_value = mock_distinct
        mock_session.query.return_value = mock_query
        mock_session.__enter__ = Mock(return_value=mock_session)
        mock_session.__exit__ = Mock(return_value=False)

        self.mock_db.get_session.return_value = mock_session

        # Execute test
        result = self.processor.get_all_batch_ids()

        # Assertions
        self.assertEqual(len(result), 3)  # None values filtered out
        self.assertNotIn(None, result)

    def test_get_all_batch_ids_empty(self):
        """Test retrieval when no batch IDs exist."""
        # Setup mock session
        mock_session = MagicMock()
        mock_query = MagicMock()
        mock_distinct = MagicMock()
        mock_distinct.all.return_value = []
        mock_query.distinct.return_value = mock_distinct
        mock_session.query.return_value = mock_query
        mock_session.__enter__ = Mock(return_value=mock_session)
        mock_session.__exit__ = Mock(return_value=False)

        self.mock_db.get_session.return_value = mock_session

        # Execute test
        result = self.processor.get_all_batch_ids()

        # Assertions
        self.assertEqual(len(result), 0)
        self.assertIsInstance(result, list)

    def test_get_all_batch_ids_database_error(self):
        """Test error handling when database query fails."""
        # Setup mock session to raise exception
        mock_session = MagicMock()
        mock_session.query.side_effect = Exception("Database error")
        mock_session.__enter__ = Mock(return_value=mock_session)
        mock_session.__exit__ = Mock(return_value=False)

        self.mock_db.get_session.return_value = mock_session

        # Execute test and verify exception
        with self.assertRaises(Exception) as context:
            self.processor.get_all_batch_ids()

        self.assertIn("Database error", str(context.exception))

    def test_processor_initialization(self):
        """Test ProcessRawData initialization."""
        processor = ProcessRawData(self.mock_db)

        # Assertions
        self.assertIsNotNone(processor.db)
        self.assertIsNotNone(processor.logger)
        self.assertEqual(processor.db, self.mock_db)

    @patch("scripts.raw_to_processed.logging.getLogger")
    def test_logging_calls(self, mock_get_logger):
        """Test that appropriate logging occurs during operations."""
        mock_logger = Mock()
        mock_get_logger.return_value = mock_logger

        processor = ProcessRawData(self.mock_db)
        processor.logger = mock_logger

        # Setup mock session for successful query
        mock_session = MagicMock()
        mock_query = MagicMock()
        mock_filter = MagicMock()
        mock_filter.all.return_value = [self.sample_studies[0]]
        mock_query.filter.return_value = mock_filter
        mock_session.query.return_value = mock_query
        mock_session.__enter__ = Mock(return_value=mock_session)
        mock_session.__exit__ = Mock(return_value=False)

        self.mock_db.get_session.return_value = mock_session

        # Execute test
        processor.get_raw_studies("batch_001")

        # Verify logging was called
        mock_logger.info.assert_called_once()


if __name__ == "__main__":
    unittest.main()
