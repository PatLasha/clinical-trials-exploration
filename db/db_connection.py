import logging

from dotenv import load_dotenv
from sqlalchemy import Engine, create_engine, text
from sqlalchemy.orm import Session, sessionmaker

from configs.app_config import AppConfig
from data_models.settings import Settings

load_dotenv()


class DBConnection:
    """
    Database connection class to manage connections to the PostgreSQL database.
    """

    def __init__(self, settings: Settings):
        self.db_url = settings.db_url
        self.logger = logging.getLogger(__name__)

        if not self.db_url:
            raise RuntimeError("DB_URL environment variable is not set. Cannot connect to the database.")
        try:
            self.engine = create_engine(self.db_url)
            # Create a configured "Session" class
            self.LocalSession = sessionmaker(bind=self.engine)
        except Exception as e:
            self.logger.error(f"Error creating database engine: {e}")
            raise

    def get_engine(self) -> Engine:
        return self.engine

    def get_session(self) -> Session:
        return self.LocalSession()

    def execute_sql_file(self, file_path: str):
        """
        Execute SQL commands from a file.
        :param file_path: Path to the SQL file.
        """
        try:
            with open(file_path, "r") as f:
                sql_commands = f.read()

            raw_conn = self.engine.raw_connection()  # Get raw connection for executing multiple statements
            try:
                cursor = raw_conn.cursor()
                cursor.execute(sql_commands)
                raw_conn.commit()
            finally:
                cursor.close()
                raw_conn.close()
        except Exception as e:
            self.logger.error(f"Error executing SQL file {file_path}: {e}")
            raise e

    def test_connection(self) -> bool:
        """
        Test the database connection.
        """
        try:
            with self.engine.connect() as conn:
                conn.execute(text("SELECT 1"))
            self.logger.info("Database connection successful.")
            return True
        except Exception as e:
            self.logger.error(f"Database connection failed: {e}")
            raise e


if __name__ == "__main__":
    configs = AppConfig()
    db = DBConnection(configs.settings)
    db.test_connection()
