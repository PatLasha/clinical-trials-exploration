import os

from dotenv import load_dotenv
from sqlalchemy import Engine, create_engine, text
from sqlalchemy.orm import sessionmaker

load_dotenv()


class DBConnection:
    """
    Database connection class to manage connections to the PostgreSQL database.
    """

    def __init__(self):
        self.db_url = os.environ.get("DB_URL")

        if not self.db_url:
            raise RuntimeError("DB_URL environment variable is not set. Cannot connect to the database.")
        self.engine = create_engine(self.db_url)
        # Create a configured "Session" class
        self.LocalSession = sessionmaker(bind=self.engine)

    def get_engine(self) -> Engine:
        return self.engine

    def get_session(self) -> sessionmaker:
        return self.LocalSession()

    def execute_sql_file(self, file_path: str):
        """
        Execute SQL commands from a file.
        :param file_path: Path to the SQL file.
        """
        try:
            with open(file_path, "r") as f:
                sql_commands = f.read()

            raw_conn = (
                self.engine.raw_connection()
            )  # Get raw connection for executing multiple statements
            try:
                cursor = raw_conn.cursor()
                cursor.execute(sql_commands)
                raw_conn.commit()
            finally:
                cursor.close()
                raw_conn.close()
        except Exception as e:
            print(f"Error executing SQL file {file_path}: {e}")
            raise e

    def test_connection(self) -> bool:
        """
        Test the database connection.
        """
        try:
            with self.engine.connect() as conn:
                conn.execute(text("SELECT 1"))
            print("Database connection successful.")
            return True
        except Exception as e:
            print(f"Database connection failed: {e}")
            return False


if __name__ == "__main__":
    db = DBConnection()
    db.test_connection()
