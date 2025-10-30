import os
import sys
import unittest
from unittest.mock import Mock, patch

from data_models.settings import Settings
from scripts.csv_to_staging import CSVToStagingLoader

sys.path.append(os.path.join(os.path.dirname(__file__), ".."))


class TestCSVToStagingLoader(unittest.TestCase):
    """
    Simple, focused unit tests for the CSV to staging loader.
    Perfect for explaining in interviews - each test has a clear purpose.
    """

    def setUp(self):
        """Set up test data paths and settings."""
        self.test_data_dir = os.path.join(os.path.dirname(__file__), "test_data")
        self.test_csv_path = os.path.join(self.test_data_dir, "clin_trials_test.csv")

        self.test_settings = Settings(
            db_url="postgresql://test:test@localhost:5432/test_db",
            entry_point="test_entry",
            file_path=self.test_csv_path,
            chunk_size=100,
            enable_backfill=False,
            log_level="INFO",
        )

    def test_database_connection_failure(self):
        """Test that loader fails gracefully when database is unreachable."""
        # Create settings that will fail connection test
        with patch("scripts.csv_to_staging.DBConnection") as mock_db_class:
            mock_db = Mock()
            mock_db.test_connection.return_value = False  # Simulate DB failure
            mock_db_class.return_value = mock_db

            # Should exit gracefully when DB connection fails
            with self.assertRaises(SystemExit):
                CSVToStagingLoader(self.test_settings)

    def test_successful_initialization(self):
        """Test that loader initializes correctly with valid inputs."""
        with patch("scripts.csv_to_staging.DBConnection") as mock_db_class:
            with patch("scripts.csv_to_staging.StudiesCSVParser"):
                # Mock successful database connection
                mock_db = Mock()
                mock_db.test_connection.return_value = True
                mock_db_class.return_value = mock_db

                loader = CSVToStagingLoader(self.test_settings)

                # Verify loader was initialized with correct values
                self.assertEqual(loader.file_path, self.test_settings.file_path)
                self.assertEqual(loader.chunk_size, self.test_settings.chunk_size)
                self.assertEqual(loader.enable_backfill, self.test_settings.enable_backfill)

    def test_backfill_logic(self):
        """Test that backfill logic correctly skips existing records."""
        # Create settings with backfill enabled
        backfill_settings = Settings(
            db_url="postgresql://test:test@localhost:5432/test_db",
            entry_point="test_entry",
            file_path=self.test_csv_path,
            chunk_size=100,
            enable_backfill=True,
            log_level="INFO",
        )

        with patch("scripts.csv_to_staging.DBConnection") as mock_db_class:
            with patch("scripts.csv_to_staging.StudiesCSVParser"):
                mock_db = Mock()
                mock_db.test_connection.return_value = True
                mock_db_class.return_value = mock_db

                # Mock the existing row IDs query
                mock_engine = Mock()
                mock_db.get_engine.return_value = mock_engine
                mock_conn = Mock()
                mock_result = Mock()
                mock_result.fetchall.return_value = [(100,), (200,)]  # Simulate existing records
                mock_conn.execute.return_value = mock_result
                mock_context = Mock()
                mock_context.__enter__ = Mock(return_value=mock_conn)
                mock_context.__exit__ = Mock(return_value=None)
                mock_engine.connect.return_value = mock_context

                loader = CSVToStagingLoader(backfill_settings)

                # Test with existing record (should be skipped)
                mock_existing_record = Mock()
                mock_existing_record.row_id = 100
                result = loader._prepare_record_for_insertion(mock_existing_record)
                self.assertFalse(result)  # Should skip existing record

                # Test with new record (should be processed)
                mock_new_record = Mock()
                mock_new_record.row_id = 300
                result = loader._prepare_record_for_insertion(mock_new_record)
                self.assertTrue(result)  # Should process new record

    def test_batch_processing(self):
        """Test that records are processed in batches correctly."""
        with patch("scripts.csv_to_staging.DBConnection") as mock_db_class:
            with patch("scripts.csv_to_staging.StudiesCSVParser"):
                with patch("pandas.DataFrame.to_sql") as mock_to_sql:
                    # Setup mocks
                    mock_db = Mock()
                    mock_db.test_connection.return_value = True
                    mock_engine = Mock()
                    mock_db.get_engine.return_value = mock_engine
                    mock_db_class.return_value = mock_db

                    loader = CSVToStagingLoader(self.test_settings)

                    # Test batch processing with sample data
                    test_batch = [{"row_id": 1, "brief_title": "Study 1"}, {"row_id": 2, "brief_title": "Study 2"}]

                    loader._process_batch(test_batch, batch_count=1)

                    # Verify pandas to_sql was called with correct parameters
                    mock_to_sql.assert_called_once_with(
                        "raw_studies", con=mock_engine, schema="staging", if_exists="append", index=False
                    )

    def test_nonexistent_file_error(self):
        """Test proper error handling for missing files."""
        # Create settings with nonexistent file
        bad_settings = Settings(
            db_url="postgresql://test:test@localhost:5432/test_db",
            entry_point="test_entry",
            file_path="nonexistent_file.csv",
            chunk_size=100,
            enable_backfill=False,
            log_level="INFO",
        )

        with patch("scripts.csv_to_staging.DBConnection") as mock_db_class:
            mock_db = Mock()
            mock_db.test_connection.return_value = True
            mock_db_class.return_value = mock_db

            loader = CSVToStagingLoader(bad_settings)

            # Should raise FileNotFoundError when trying to process
            with self.assertRaises(FileNotFoundError):
                loader.process()


class TestFileValidation(unittest.TestCase):
    """Test that our test data files exist and are properly structured."""

    def setUp(self):
        """Set up test data directory path."""
        self.test_data_dir = os.path.join(os.path.dirname(__file__), "test_data")

    def test_required_test_files_exist(self):
        """Verify all required test data files are present."""
        required_files = [
            "clin_trials_test.csv",
            "clin_trials_empty.csv",
            "clin_trials_malformed.csv",
            "clin_trials_wrong_headers.csv",
        ]

        for filename in required_files:
            file_path = os.path.join(self.test_data_dir, filename)
            self.assertTrue(os.path.exists(file_path), f"Required test file {filename} is missing")

    def test_main_test_csv_structure(self):
        """Verify the main test CSV has the expected columns."""
        test_csv_path = os.path.join(self.test_data_dir, "clin_trials_test.csv")

        with open(test_csv_path, "r") as f:
            header_line = f.readline().strip()

        # Check for key columns that our processor expects
        expected_columns = ["Organization Full Name", "Brief Title", "Overall Status", "Start Date"]

        for column in expected_columns:
            self.assertIn(column, header_line, f"Test CSV should contain column: {column}")


if __name__ == "__main__":
    unittest.main(verbosity=2)
