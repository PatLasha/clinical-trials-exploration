import os
import unittest
from unittest.mock import MagicMock, Mock, mock_open, patch

from sqlalchemy import Engine
from sqlalchemy.orm import Session

from db.db_connection import DBConnection


class TestDBConnection(unittest.TestCase):
    def setUp(self):
        self.orig_db_url = os.environ.get("DB_URL")
        os.environ["DB_URL"] = "postgresql://test:test@localhost:5432/test_db"

    def tearDown(self):
        if self.orig_db_url is None:
            os.environ.pop("DB_URL", None)
        else:
            os.environ["DB_URL"] = self.orig_db_url

    @patch("db.db_connection.create_engine")
    @patch("db.db_connection.sessionmaker")
    def test_init_and_missing_db_url(self, mock_sessionmaker, mock_create_engine):
        mock_engine = Mock(spec=Engine)
        mock_create_engine.return_value = mock_engine
        db = DBConnection()
        self.assertEqual(db.db_url, os.environ["DB_URL"])
        mock_create_engine.assert_called_once_with(os.environ["DB_URL"])
        mock_sessionmaker.assert_called_once_with(bind=mock_engine)
        os.environ.pop("DB_URL", None)
        with self.assertRaises(RuntimeError):
            DBConnection()

    @patch("db.db_connection.create_engine")
    @patch("db.db_connection.sessionmaker")
    def test_get_engine_and_session(self, mock_sessionmaker, mock_create_engine):
        mock_engine = Mock(spec=Engine)
        mock_create_engine.return_value = mock_engine
        mock_session_class = Mock()
        mock_session_instance = Mock(spec=Session)
        mock_session_class.return_value = mock_session_instance
        mock_sessionmaker.return_value = mock_session_class
        db = DBConnection()
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
        db = DBConnection()
        self.assertTrue(db.test_connection())
        mock_engine.connect.side_effect = Exception("fail")
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
            db = DBConnection()
            db.execute_sql_file("/fake/path.sql")
            mock_engine.raw_connection.assert_called_once()
            mock_cursor.execute.assert_called_once_with(sql)
        # File not found
        db = DBConnection()
        with self.assertRaises(FileNotFoundError):
            db.execute_sql_file("/no/such/file.sql")
        # SQL error
        mock_raw = MagicMock()
        mock_cursor = MagicMock()
        mock_cursor.execute.side_effect = Exception("sql error")
        mock_raw.cursor.return_value = mock_cursor
        mock_engine.raw_connection.return_value = mock_raw
        with patch("builtins.open", mock_open(read_data="INVALID")):
            db = DBConnection()
            with self.assertRaises(Exception):
                db.execute_sql_file("/fake/path.sql")
            mock_cursor.close.assert_called_once()
            mock_raw.close.assert_called_once()


if __name__ == "__main__":
    unittest.main()
