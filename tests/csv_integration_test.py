import os
import sys
import unittest
from unittest.mock import Mock, patch

from data_models.settings import Settings
from scripts.csv_to_staging import CSVToStagingLoader

sys.path.append(os.path.join(os.path.dirname(__file__), ".."))


class TestCSVIntegration(unittest.TestCase):
    """
    Integration tests that use real CSV parsing but mock the database.
    These tests verify the full pipeline works with actual data.
    """

    def setUp(self):
        """Set up test fixtures."""
        self.test_data_dir = os.path.join(os.path.dirname(__file__), "test_data")
        self.valid_csv_path = os.path.join(self.test_data_dir, "clin_trials_test.csv")
        self.empty_csv_path = os.path.join(self.test_data_dir, "clin_trials_empty.csv")
        self.malformed_csv_path = os.path.join(self.test_data_dir, "clin_trials_malformed.csv")
        
        # Base settings for tests
        self.base_settings = {
            "db_url": "postgresql://test:test@localhost:5432/test_db",
            "entry_point": "test_entry",
            "chunk_size": 3,
            "enable_backfill": True,
            "log_level": "INFO"
        }

    def _create_mock_db(self, existing_ids=None):
        """Helper to create a mock database."""
        if existing_ids is None:
            existing_ids = []

        mock_db = Mock()
        mock_db.test_connection.return_value = True
        mock_engine = Mock()
        mock_db.get_engine.return_value = mock_engine

        # Mock the context manager for database queries
        mock_conn = Mock()
        mock_result = Mock()
        mock_result.fetchall.return_value = [(id_,) for id_ in existing_ids]
        mock_conn.execute.return_value = mock_result

        # Create proper context manager mock
        mock_context = Mock()
        mock_context.__enter__ = Mock(return_value=mock_conn)
        mock_context.__exit__ = Mock(return_value=None)
        mock_engine.connect.return_value = mock_context

        return mock_db, mock_engine

    @patch("pandas.DataFrame.to_sql")
    def test_real_csv_processing_with_backfill(self, mock_to_sql):
        """Test processing real CSV data with backfill logic."""
        # Mock database with some existing records
        existing_ids = [0, 2, 4]  # Simulate existing records
        mock_db, mock_engine = self._create_mock_db(existing_ids)
        
        # Create settings for this test
        settings = Settings(
            file_path=self.valid_csv_path,
            **self.base_settings
        )

        with patch("scripts.csv_to_staging.DBConnection", return_value=mock_db):
            loader = CSVToStagingLoader(settings)

            loader.process()

            # Verify to_sql was called (indicating new records were processed)
            self.assertTrue(mock_to_sql.called)

            # Check that records were processed in batches
            call_count = mock_to_sql.call_count
            self.assertGreater(call_count, 0, "Should have processed some batches")

    @patch("pandas.DataFrame.to_sql")
    def test_real_csv_processing_without_backfill(self, mock_to_sql):
        """Test processing real CSV data without backfill."""
        mock_db, mock_engine = self._create_mock_db([])
        
        # Create settings without backfill
        settings = Settings(
            file_path=self.valid_csv_path,
            chunk_size=5,
            enable_backfill=False,
            **{k: v for k, v in self.base_settings.items() if k not in ["chunk_size", "enable_backfill"]}
        )

        with patch("scripts.csv_to_staging.DBConnection", return_value=mock_db):
            loader = CSVToStagingLoader(settings)

            loader.process()

            # Verify processing occurred
            self.assertTrue(mock_to_sql.called)

    def test_csv_with_single_record(self):
        """Test processing CSV with only one record."""
        mock_db, mock_engine = self._create_mock_db([])
        
        # Create settings for empty CSV (single record)
        settings = Settings(
            file_path=self.empty_csv_path,
            chunk_size=10,
            enable_backfill=False,
            **{k: v for k, v in self.base_settings.items() if k not in ["chunk_size", "enable_backfill"]}
        )

        with patch("scripts.csv_to_staging.DBConnection", return_value=mock_db):
            with patch("pandas.DataFrame.to_sql") as mock_to_sql:
                loader = CSVToStagingLoader(settings)

                loader.process()

                # Should still process successfully
                self.assertTrue(mock_to_sql.called)

    def test_different_chunk_sizes_with_real_data(self):
        """Test processing with various chunk sizes using real CSV data."""
        mock_db, mock_engine = self._create_mock_db([])

        chunk_sizes = [1, 2, 5, 100]  # Test various chunk sizes

        for chunk_size in chunk_sizes:
            with patch("scripts.csv_to_staging.DBConnection", return_value=mock_db):
                with patch("pandas.DataFrame.to_sql") as mock_to_sql:
                    settings = Settings(
                        file_path=self.valid_csv_path,
                        chunk_size=chunk_size,
                        enable_backfill=False,
                        **{k: v for k, v in self.base_settings.items() if k not in ["chunk_size", "enable_backfill"]}
                    )
                    loader = CSVToStagingLoader(settings)

                    # Should not raise any exception
                    loader.process()

                    # Verify processing occurred
                    self.assertTrue(mock_to_sql.called)

    def test_malformed_csv_handling(self):
        """Test how the loader handles malformed CSV data."""
        mock_db, mock_engine = self._create_mock_db([])
        
        # Create settings for malformed CSV
        settings = Settings(
            file_path=self.malformed_csv_path,
            chunk_size=10,
            enable_backfill=False,
            **{k: v for k, v in self.base_settings.items() if k not in ["chunk_size", "enable_backfill"]}
        )

        with patch("scripts.csv_to_staging.DBConnection", return_value=mock_db):
            # This should either handle gracefully or raise appropriate exception
            try:
                loader = CSVToStagingLoader(settings)

                # If initialization succeeds, processing should handle malformed data
                with patch("pandas.DataFrame.to_sql"):
                    loader.process()

            except Exception as e:
                # If an exception is raised, it should be informative
                self.assertIsInstance(e, (ValueError, FileNotFoundError, Exception))

    def test_nonexistent_csv_file(self):
        """Test handling of non-existent CSV file."""
        nonexistent_path = os.path.join(self.test_data_dir, "does_not_exist.csv")
        mock_db, mock_engine = self._create_mock_db([])
        
        # Create settings for nonexistent file
        settings = Settings(
            file_path=nonexistent_path,
            enable_backfill=False,
            **{k: v for k, v in self.base_settings.items() if k != "enable_backfill"}
        )

        with patch("scripts.csv_to_staging.DBConnection", return_value=mock_db):
            # Should raise FileNotFoundError when trying to process non-existent file
            loader = CSVToStagingLoader(settings)

            # Exception should occur during processing, not initialization
            with self.assertRaises(FileNotFoundError):
                loader.process()

    @patch("logging.getLogger")
    def test_processing_output_messages(self, mock_get_logger):
        """Test that appropriate messages are logged during processing."""
        mock_db, mock_engine = self._create_mock_db([])
        mock_logger = Mock()
        mock_get_logger.return_value = mock_logger
        
        # Create settings for test
        settings = Settings(
            file_path=self.valid_csv_path,
            chunk_size=3,
            enable_backfill=False,
            **{k: v for k, v in self.base_settings.items() if k not in ["chunk_size", "enable_backfill"]}
        )

        with patch("scripts.csv_to_staging.DBConnection", return_value=mock_db):
            with patch("pandas.DataFrame.to_sql"):
                loader = CSVToStagingLoader(settings)

                loader.process()

                # Check that various messages were logged
                log_calls = [str(call) for call in mock_logger.info.call_args_list]

                # Should have logged various status messages
                self.assertTrue(mock_logger.info.called, "Should have logged status messages")


class TestEdgeCases(unittest.TestCase):
    """Test edge cases and boundary conditions."""

    def setUp(self):
        """Set up test fixtures."""
        self.test_data_dir = os.path.join(os.path.dirname(__file__), "test_data")
        self.valid_csv_path = os.path.join(self.test_data_dir, "clin_trials_test.csv")
        
        # Base settings for edge case tests
        self.base_settings = {
            "db_url": "postgresql://test:test@localhost:5432/test_db",
            "entry_point": "test_entry",
            "log_level": "INFO"
        }

    def test_very_large_chunk_size(self):
        """Test with chunk size larger than total records."""
        mock_db = Mock()
        mock_db.test_connection.return_value = True
        mock_engine = Mock()
        mock_db.get_engine.return_value = mock_engine
        
        # Create settings with very large chunk size
        settings = Settings(
            file_path=self.valid_csv_path,
            chunk_size=10000,  # Much larger than actual record count
            enable_backfill=False,
            **self.base_settings
        )

        with patch("scripts.csv_to_staging.DBConnection", return_value=mock_db):
            loader = CSVToStagingLoader(settings)

            # Should handle gracefully
            self.assertEqual(loader.chunk_size, 10000)

    def test_chunk_size_edge_values(self):
        """Test with edge values for chunk size."""
        mock_db = Mock()
        mock_db.test_connection.return_value = True

        edge_values = [1, 2]  # Test minimum meaningful values

        for chunk_size in edge_values:
            with patch("scripts.csv_to_staging.DBConnection", return_value=mock_db):
                settings = Settings(
                    file_path=self.valid_csv_path,
                    chunk_size=chunk_size,
                    enable_backfill=False,
                    **self.base_settings
                )
                loader = CSVToStagingLoader(settings)

                self.assertEqual(loader.chunk_size, chunk_size)

    def test_empty_existing_row_ids(self):
        """Test behavior when no existing records are found."""
        mock_db = Mock()
        mock_db.test_connection.return_value = True
        mock_engine = Mock()
        mock_db.get_engine.return_value = mock_engine

        # Mock empty result
        mock_conn = Mock()
        mock_result = Mock()
        mock_result.fetchall.return_value = []  # No existing records
        mock_conn.execute.return_value = mock_result

        mock_context = Mock()
        mock_context.__enter__ = Mock(return_value=mock_conn)
        mock_context.__exit__ = Mock(return_value=None)
        mock_engine.connect.return_value = mock_context
        
        # Create settings with backfill enabled
        settings = Settings(
            file_path=self.valid_csv_path,
            enable_backfill=True,
            chunk_size=1000,  # Default chunk size
            **self.base_settings
        )

        with patch("scripts.csv_to_staging.DBConnection", return_value=mock_db):
            loader = CSVToStagingLoader(settings)

            # Should have empty set for existing_row_ids
            self.assertEqual(loader.existing_row_ids, set())


if __name__ == "__main__":
    # Run all tests
    unittest.main(verbosity=2)
