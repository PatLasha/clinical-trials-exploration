import os
import sys
import logging

from data_models.settings import Settings
from db.db_connection import DBConnection

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

def init_db(settings: Settings):
    """
    Initialize the database schema by executing SQL files.
    :param settings: Application settings containing DB connection info
    """
    logger = logging.getLogger(__name__)
    db = DBConnection(settings)

    try:  
        logger.info("Initializing database schema...")
        if not db.test_connection():
            logger.error("Cannot initialize database schema: Database connection failed.")
            return

        logger.info("Executing schema.sql...")

        sql_file_paths = [
            "db/schemas/create_schemas.sql",
            "db/schemas/create_staging_tables.sql",
            "db/schemas/create_processed_tables.sql",
        ]

        for sql_file in sql_file_paths:
            if os.path.exists(sql_file):
                logger.info(f"Executing {sql_file}...")
                db.execute_sql_file(sql_file)
                logger.info(f"Executed {sql_file} successfully.")
            else:
                logger.info(f"SQL file {sql_file} does not exist. Skipping.")

        logger.info("Database schema initialization completed.")
    except Exception as e:
        logger.error(f"Error during database initialization: {e}")
        raise e


if __name__ == "__main__":
    from configs.app_config import AppConfig

    configs = AppConfig()
    init_db(configs.settings)
