"""
Unit tests for init_db.py and schema file execution.
"""

import os
import sys
import unittest
from pathlib import Path
from unittest.mock import MagicMock, Mock, patch

from db.db_connection import DBConnection
from scripts.init_db import init_db

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))


class TestInitDB(unittest.TestCase):
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
        os.environ["DB_URL"] = "postgresql://test:test@localhost:5432/test_db"
        db = DBConnection()
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
        # Should print error and not attempt to execute SQL files
        with patch("builtins.print") as mock_print:
            init_db()
            mock_print.assert_any_call("Cannot initialize database schema: Database connection failed.")


if __name__ == "__main__":
    unittest.main()
