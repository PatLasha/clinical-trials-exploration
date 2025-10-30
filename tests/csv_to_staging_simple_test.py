import os
import sys
import unittest
from unittest.mock import Mock, patch

from scripts.csv_to_staging import CSVToStagingProcessor, csv_to_staging

sys.path.append(os.path.join(os.path.dirname(__file__), ".."))


class TestCSVToStagingProcessor(unittest.TestCase):
    """
    Unit tests for CSVToStagingProcessor class.
    Tests core functionality with mocked dependencies.
    """

    def setUp(self):
        """Set up test fixtures before each test method."""
        self.test_data_dir = os.path.join(os.path.dirname(__file__), "test_data")
        self.valid_csv_path = os.path.join(self.test_data_dir, "clin_trials_test.csv")
        self.nonexistent_csv_path = os.path.join(self.test_data_dir, "nonexistent_file.csv")

    def test_database_connection_failure(self):
        """Test initialization failure when database connection fails."""
        with patch("scripts.csv_to_staging.DBConnection") as mock_db_class:
            mock_db = Mock()
            mock_db.test_connection.return_value = False
            mock_db_class.return_value = mock_db

            with self.assertRaises(SystemExit):
                CSVToStagingProcessor(self.valid_csv_path)

    def test_convenience_function(self):
        """Test the convenience function csv_to_staging."""
        with patch("scripts.csv_to_staging.CSVToStagingProcessor") as mock_processor_class:
            mock_processor = Mock()
            mock_processor_class.return_value = mock_processor

            csv_to_staging(file_path=self.valid_csv_path, chunk_size=500, enable_backfill=False)

            # Verify processor was created with correct parameters
            mock_processor_class.assert_called_once_with(self.valid_csv_path, 500, False)
            # Verify process was called
            mock_processor.process.assert_called_once()

    def test_nonexistent_file_handling(self):
        """Test behavior when CSV file doesn't exist."""
        with patch("scripts.csv_to_staging.DBConnection") as mock_db_class:
            mock_db = Mock()
            mock_db.test_connection.return_value = True
            mock_db_class.return_value = mock_db

            with patch("scripts.csv_to_staging.StudiesCSVParser") as mock_parser_class:
                # StudiesCSVParser should raise an exception for non-existent file
                mock_parser_class.side_effect = FileNotFoundError("File not found")

                with self.assertRaises(FileNotFoundError):
                    CSVToStagingProcessor(self.nonexistent_csv_path)

    def test_backfill_disabled_initialization(self):
        """Test initialization with backfill disabled."""
        with patch("scripts.csv_to_staging.DBConnection") as mock_db_class:
            mock_db = Mock()
            mock_db.test_connection.return_value = True
            mock_db_class.return_value = mock_db

            with patch("scripts.csv_to_staging.StudiesCSVParser"):
                processor = CSVToStagingProcessor(file_path=self.valid_csv_path, enable_backfill=False)

                # When backfill is disabled, existing_row_ids should be empty
                self.assertEqual(processor.existing_row_ids, set())
                self.assertFalse(processor.enable_backfill)

    def test_chunk_size_configuration(self):
        """Test processor with different chunk sizes."""
        with patch("scripts.csv_to_staging.DBConnection") as mock_db_class:
            mock_db = Mock()
            mock_db.test_connection.return_value = True
            mock_db_class.return_value = mock_db

            with patch("scripts.csv_to_staging.StudiesCSVParser"):
                for chunk_size in [1, 10, 100, 1000]:
                    processor = CSVToStagingProcessor(self.valid_csv_path, chunk_size=chunk_size, enable_backfill=False)
                    self.assertEqual(processor.chunk_size, chunk_size)

    def test_record_preparation_logic(self):
        """Test record preparation for insertion."""
        with patch("scripts.csv_to_staging.DBConnection") as mock_db_class:
            mock_db = Mock()
            mock_db.test_connection.return_value = True
            mock_db_class.return_value = mock_db

            with patch("scripts.csv_to_staging.StudiesCSVParser"):
                # Test with backfill enabled and existing records
                processor = CSVToStagingProcessor(
                    self.valid_csv_path, enable_backfill=False  # Disable to avoid database query
                )
                processor.existing_row_ids = {100, 200}  # Manually set existing IDs
                processor.enable_backfill = True

                # Mock StudiesRaw object with existing row_id
                mock_studies_existing = Mock()
                mock_studies_existing.row_id = 100

                # Should return False (skip existing record)
                result = processor._prepare_record_for_insertion(mock_studies_existing)
                self.assertFalse(result)

                # Mock StudiesRaw object with new row_id
                mock_studies_new = Mock()
                mock_studies_new.row_id = 300

                # Should return True (process new record)
                result = processor._prepare_record_for_insertion(mock_studies_new)
                self.assertTrue(result)
                # Should have set ingestion timestamp
                self.assertIsNotNone(mock_studies_new.ingestion_timestamp)

    def test_batch_processing_logic(self):
        """Test batch processing functionality."""
        with patch("scripts.csv_to_staging.DBConnection") as mock_db_class:
            mock_db = Mock()
            mock_db.test_connection.return_value = True
            mock_engine = Mock()
            mock_db.get_engine.return_value = mock_engine
            mock_db_class.return_value = mock_db

            with patch("scripts.csv_to_staging.StudiesCSVParser"):
                with patch("pandas.DataFrame.to_sql") as mock_to_sql:
                    processor = CSVToStagingProcessor(self.valid_csv_path, enable_backfill=False)

                    # Test batch processing
                    test_batch = [{"row_id": 1, "brief_title": "Test 1"}, {"row_id": 2, "brief_title": "Test 2"}]

                    processor._process_batch(test_batch, 1)

                    # Verify to_sql was called correctly
                    mock_to_sql.assert_called_once_with(
                        "raw_studies", con=mock_engine, schema="staging", if_exists="append", index=False
                    )

    def test_summary_printing(self):
        """Test ingestion summary printing."""
        with patch("scripts.csv_to_staging.DBConnection") as mock_db_class:
            mock_db = Mock()
            mock_db.test_connection.return_value = True
            mock_db_class.return_value = mock_db

            with patch("scripts.csv_to_staging.StudiesCSVParser"):
                with patch("builtins.print") as mock_print:
                    processor = CSVToStagingProcessor(self.valid_csv_path, enable_backfill=False)

                    processor._print_ingestion_summary(100, 50)

                    # Check that print was called multiple times
                    self.assertGreater(mock_print.call_count, 0)

                    # Check that summary information was printed
                    print_args = [str(call) for call in mock_print.call_args_list]
                    summary_found = any("Ingestion Summary" in arg for arg in print_args)
                    self.assertTrue(summary_found)


class TestErrorHandling(unittest.TestCase):
    """Test error handling scenarios."""

    def setUp(self):
        """Set up test fixtures."""
        self.test_data_dir = os.path.join(os.path.dirname(__file__), "test_data")
        self.valid_csv_path = os.path.join(self.test_data_dir, "clin_trials_test.csv")

    def test_process_method_exception_handling(self):
        """Test process method handles exceptions properly."""
        with patch("scripts.csv_to_staging.DBConnection") as mock_db_class:
            mock_db = Mock()
            mock_db.test_connection.return_value = True
            mock_db_class.return_value = mock_db

            with patch("scripts.csv_to_staging.StudiesCSVParser") as mock_parser_class:
                mock_parser = Mock()
                mock_parser_class.return_value = mock_parser

                processor = CSVToStagingProcessor(self.valid_csv_path, enable_backfill=False)

                # Mock _process_records_in_batches to raise an exception
                processor._process_records_in_batches = Mock(side_effect=Exception("Test exception"))

                with self.assertRaises(Exception) as context:
                    processor.process()

                self.assertIn("Test exception", str(context.exception))


class TestFileHandling(unittest.TestCase):
    """Test various file scenarios."""

    def setUp(self):
        """Set up test fixtures."""
        self.test_data_dir = os.path.join(os.path.dirname(__file__), "test_data")

    def test_file_existence_check(self):
        """Test that test files exist."""
        test_files = [
            "clin_trials_test.csv",
            "clin_trials_malformed.csv",
            "clin_trials_empty.csv",
            "clin_trials_wrong_headers.csv",
        ]

        for test_file in test_files:
            file_path = os.path.join(self.test_data_dir, test_file)
            self.assertTrue(os.path.exists(file_path), f"Test file {test_file} should exist")

    def test_csv_file_structure(self):
        """Test that the main test CSV has expected structure."""
        test_csv_path = os.path.join(self.test_data_dir, "clin_trials_test.csv")

        with open(test_csv_path, "r") as f:
            header = f.readline().strip()

        # Check that header contains expected columns
        expected_columns = ["Organization Full Name", "Brief Title", "Overall Status", "Start Date"]

        for column in expected_columns:
            self.assertIn(column, header, f"Header should contain '{column}'")


if __name__ == "__main__":
    # Run all tests
    unittest.main(verbosity=2)
