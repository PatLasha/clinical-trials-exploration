import logging

from dotenv import load_dotenv
from sqlalchemy import Engine, create_engine, text
from sqlalchemy.orm import Session, sessionmaker

from data_models.settings import Settings
from data_models.table_models.raw_studies import RawStudies

load_dotenv()


class DBConnection:
    """
    Database connection class to manage connections to the PostgreSQL database.
    """

    def __init__(self, settings: Settings):
        self.db_url = settings.db_url
        self.logger = logging.getLogger(__name__)

        if not self.db_url:
            self.logger.error("DB_URL environment variable is not set.")
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
            raise

    def get_raw_studies(self, batch_id: str) -> list[RawStudies]:
        """
        Retrieve raw studies from the staging schema by batch_id.

        :param batch_id: The batch identifier to filter studies.
        :return: List of RawStudies objects for the specified batch.
        """
        try:
            with self.get_session() as session:
                # Query for all raw studies with the given batch_id
                studies = session.query(RawStudies).filter(RawStudies.batch_id == batch_id).all()

                self.logger.info(f"Retrieved {len(studies)} raw studies for batch_id: {batch_id}")
                return studies
        except Exception as e:
            self.logger.error(f"Error retrieving raw studies for batch_id {batch_id}: {e}")
            raise

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
            return False


if __name__ == "__main__":
    from configs.app_config import AppConfig

    configs = AppConfig()
    db = DBConnection(configs.settings)
    db.test_connection()
