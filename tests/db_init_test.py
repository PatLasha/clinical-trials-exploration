"""
Unit tests for init_db.py and schema file execution.
"""

import os
import sys
import unittest
from pathlib import Path
from unittest.mock import MagicMock, Mock, patch

from data_models.settings import Settings
from db.db_connection import DBConnection
from scripts.init_db import init_db

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))


class TestInitDB(unittest.TestCase):
    def setUp(self):
        """Set up test settings."""
        self.test_settings = Settings(
            db_url="postgresql://test:test@localhost:5432/test_db",
            entry_point="test_entry",
            file_path="/test/path.csv",
            chunk_size=1000,
            enable_backfill=True,
            log_level="INFO"
        )

    @patch("db.db_connection.create_engine")
    @patch("db.db_connection.sessionmaker")
    @patch("db.db_connection.DBConnection.test_connection", return_value=True)
    def test_execute_sql_files_from_schema_dir(self, mock_test_conn, mock_sessionmaker, mock_create_engine):
        mock_engine = Mock()
        mock_raw = MagicMock()
        mock_cursor = MagicMock()
        mock_raw.cursor.return_value = mock_cursor
        mock_engine.raw_connection.return_value = mock_raw
        mock_create_engine.return_value = mock_engine
        
        db = DBConnection(self.test_settings)
        schemas = Path(__file__).resolve().parents[1] / "db" / "schemas"
        self.assertTrue(schemas.exists())
        for p in sorted(schemas.glob("*.sql")):
            mock_raw.reset_mock()
            mock_cursor.reset_mock()
            db.execute_sql_file(str(p))
            self.assertGreaterEqual(mock_engine.raw_connection.call_count, 1)
            mock_cursor.execute.assert_called()
            mock_raw.commit.assert_called_once()

    @patch("db.db_connection.DBConnection.test_connection", return_value=False)
    def test_init_db_connection_failure(self, mock_test_conn):
        # Should log error and not attempt to execute SQL files
        with patch("logging.getLogger") as mock_logger:
            mock_logger_instance = Mock()
            mock_logger.return_value = mock_logger_instance
            
            init_db(self.test_settings)
            
            # Verify that error was logged
            mock_logger_instance.error.assert_called_with("Cannot initialize database schema: Database connection failed.")


if __name__ == "__main__":
    unittest.main()
