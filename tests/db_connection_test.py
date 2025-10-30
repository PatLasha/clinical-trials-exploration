import unittest
from unittest.mock import MagicMock, Mock, mock_open, patch

from sqlalchemy import Engine
from sqlalchemy.orm import Session

from data_models.settings import Settings
from data_models.table_models.raw_studies import RawStudies
from db.db_connection import DBConnection


class TestDBConnection(unittest.TestCase):
    def setUp(self):
        """Set up test settings."""
        self.test_settings = Settings(
            db_url="postgresql://test:test@localhost:5432/test_db",
            entry_point="test_entry",
            file_path="/test/path.csv",
            chunk_size=1000,
            enable_backfill=True,
            log_level="INFO",
        )

    @patch("db.db_connection.create_engine")
    @patch("db.db_connection.sessionmaker")
    def test_init_and_missing_db_url(self, mock_sessionmaker, mock_create_engine):
        mock_engine = Mock(spec=Engine)
        mock_create_engine.return_value = mock_engine
        db = DBConnection(self.test_settings)
        self.assertEqual(db.db_url, self.test_settings.db_url)
        mock_create_engine.assert_called_once_with(self.test_settings.db_url)
        mock_sessionmaker.assert_called_once_with(bind=mock_engine)

        # Test with missing db_url
        empty_settings = Settings(
            db_url="", entry_point="test", file_path="/test/path.csv"  # Empty URL should raise RuntimeError
        )
        with self.assertRaises(RuntimeError):
            DBConnection(empty_settings)

    @patch("db.db_connection.create_engine")
    @patch("db.db_connection.sessionmaker")
    def test_get_engine_and_session(self, mock_sessionmaker, mock_create_engine):
        mock_engine = Mock(spec=Engine)
        mock_create_engine.return_value = mock_engine
        mock_session_class = Mock()
        mock_session_instance = Mock(spec=Session)
        mock_session_class.return_value = mock_session_instance
        mock_sessionmaker.return_value = mock_session_class
        db = DBConnection(self.test_settings)
        self.assertIs(db.get_engine(), mock_engine)
        self.assertIsInstance(db.get_session(), Mock)

    @patch("db.db_connection.create_engine")
    @patch("db.db_connection.sessionmaker")
    def test_test_connection_success_and_failure(self, mock_sessionmaker, mock_create_engine):
        mock_engine = Mock(spec=Engine)
        # Use MagicMock for context manager support
        mock_engine.connect.return_value = MagicMock()
        mock_engine.connect.return_value.__enter__.return_value = MagicMock()
        mock_create_engine.return_value = mock_engine
        db = DBConnection(self.test_settings)
        self.assertTrue(db.test_connection())
        mock_engine.connect.side_effect = Exception("fail")
        # Note: The new DBConnection raises exceptions instead of returning False
        self.assertFalse(db.test_connection())

    @patch("db.db_connection.create_engine")
    @patch("db.db_connection.sessionmaker")
    def test_execute_sql_file_scenarios(self, mock_sessionmaker, mock_create_engine):
        mock_engine = Mock(spec=Engine)
        mock_create_engine.return_value = mock_engine
        # Success
        mock_raw = MagicMock()
        mock_cursor = MagicMock()
        mock_raw.cursor.return_value = mock_cursor
        mock_engine.raw_connection.return_value = mock_raw
        sql = "CREATE TABLE t (id INT);"
        with patch("builtins.open", mock_open(read_data=sql)):
            db = DBConnection(self.test_settings)
            db.execute_sql_file("/fake/path.sql")
            mock_engine.raw_connection.assert_called_once()
            mock_cursor.execute.assert_called_once_with(sql)
        # File not found
        db = DBConnection(self.test_settings)
        with self.assertRaises(FileNotFoundError):
            db.execute_sql_file("/no/such/file.sql")
        # SQL error
        mock_raw = MagicMock()
        mock_cursor = MagicMock()
        mock_cursor.execute.side_effect = Exception("sql error")
        mock_raw.cursor.return_value = mock_cursor
        mock_engine.raw_connection.return_value = mock_raw
        with patch("builtins.open", mock_open(read_data="INVALID")):
            db = DBConnection(self.test_settings)
            with self.assertRaises(Exception):
                db.execute_sql_file("/fake/path.sql")
            mock_cursor.close.assert_called_once()
            mock_raw.close.assert_called_once()

    @patch("db.db_connection.create_engine")
    @patch("db.db_connection.sessionmaker")
    def test_get_raw_studies_success(self, mock_sessionmaker, mock_create_engine):
        """Test successful retrieval of raw studies by batch_id."""
        # Setup mock engine and session
        mock_engine = Mock(spec=Engine)
        mock_create_engine.return_value = mock_engine

        # Create mock session and query
        mock_session = MagicMock(spec=Session)
        mock_session_class = Mock()
        mock_session_class.return_value = mock_session
        mock_sessionmaker.return_value = mock_session_class

        # Create mock RawStudies objects
        mock_study1 = Mock(spec=RawStudies)
        mock_study1.batch_id = "batch_123"
        mock_study1.brief_title = "Study 1"
        mock_study1.row_id = 1

        mock_study2 = Mock(spec=RawStudies)
        mock_study2.batch_id = "batch_123"
        mock_study2.brief_title = "Study 2"
        mock_study2.row_id = 2

        # Setup query chain mock
        mock_query = MagicMock()
        mock_filter = MagicMock()
        mock_filter.all.return_value = [mock_study1, mock_study2]
        mock_query.filter.return_value = mock_filter
        mock_session.query.return_value = mock_query

        # Execute test
        db = DBConnection(self.test_settings)
        result = db.get_raw_studies("batch_123")

        # Assertions
        self.assertEqual(len(result), 2)
        self.assertEqual(result[0].batch_id, "batch_123")
        self.assertEqual(result[1].batch_id, "batch_123")
        mock_session.query.assert_called_once_with(RawStudies)
        mock_session.close.assert_called_once()

    @patch("db.db_connection.create_engine")
    @patch("db.db_connection.sessionmaker")
    def test_get_raw_studies_empty_result(self, mock_sessionmaker, mock_create_engine):
        """Test retrieval when no studies match the batch_id."""
        # Setup mock engine and session
        mock_engine = Mock(spec=Engine)
        mock_create_engine.return_value = mock_engine

        mock_session = MagicMock(spec=Session)
        mock_session_class = Mock()
        mock_session_class.return_value = mock_session
        mock_sessionmaker.return_value = mock_session_class

        # Setup query chain to return empty list
        mock_query = MagicMock()
        mock_filter = MagicMock()
        mock_filter.all.return_value = []
        mock_query.filter.return_value = mock_filter
        mock_session.query.return_value = mock_query

        # Execute test
        db = DBConnection(self.test_settings)
        result = db.get_raw_studies("nonexistent_batch")

        # Assertions
        self.assertEqual(len(result), 0)
        self.assertIsInstance(result, list)
        mock_session.close.assert_called_once()

    @patch("db.db_connection.create_engine")
    @patch("db.db_connection.sessionmaker")
    def test_get_raw_studies_database_error(self, mock_sessionmaker, mock_create_engine):
        """Test error handling when database query fails."""
        # Setup mock engine and session
        mock_engine = Mock(spec=Engine)
        mock_create_engine.return_value = mock_engine

        mock_session = MagicMock(spec=Session)
        mock_session_class = Mock()
        mock_session_class.return_value = mock_session
        mock_sessionmaker.return_value = mock_session_class

        # Setup query to raise an exception
        mock_session.query.side_effect = Exception("Database connection error")

        # Execute test and verify exception is raised
        db = DBConnection(self.test_settings)
        with self.assertRaises(Exception) as context:
            db.get_raw_studies("batch_123")

        self.assertIn("Database connection error", str(context.exception))
        mock_session.close.assert_called_once()

    @patch("db.db_connection.create_engine")
    @patch("db.db_connection.sessionmaker")
    def test_get_raw_studies_session_cleanup(self, mock_sessionmaker, mock_create_engine):
        """Test that session is properly closed even when an error occurs."""
        # Setup mock engine and session
        mock_engine = Mock(spec=Engine)
        mock_create_engine.return_value = mock_engine

        mock_session = MagicMock(spec=Session)
        mock_session_class = Mock()
        mock_session_class.return_value = mock_session
        mock_sessionmaker.return_value = mock_session_class

        # Setup query to raise an exception during filtering
        mock_query = MagicMock()
        mock_query.filter.side_effect = Exception("Query error")
        mock_session.query.return_value = mock_query

        # Execute test and verify exception is raised
        db = DBConnection(self.test_settings)
        with self.assertRaises(Exception):
            db.get_raw_studies("batch_123")

        # Verify session was closed despite the error
        mock_session.close.assert_called_once()


if __name__ == "__main__":
    unittest.main()
