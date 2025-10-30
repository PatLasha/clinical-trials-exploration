import logging
from typing import Set

import pandas as pd
from sqlalchemy import text

from configs.app_config import AppConfig
from data_models.settings import Settings
from db.db_connection import DBConnection
from parsers.studies_csv_parser import StudiesCSVParser


class CSVToStagingLoader:
    """
    A class to handle CSV ingestion into the staging.raw_studies table.

    This class encapsulates all the logic for processing CSV files, managing
    database connections, handling backfill logic, and batch processing.
    """

    def __init__(self, settings: Settings):
        """
        Initialize the CSV to Staging Loader.
        :param settings: Application settings containing DB connection info and CSV file path
        """
        self.settings = settings
        self.file_path = settings.file_path
        self.chunk_size = settings.chunk_size
        self.enable_backfill = settings.enable_backfill
        self.db = DBConnection(settings)
        self.logger = logging.getLogger(__name__)

        # Initialize database connection and parser
        self._initialize_database_connection()
        self.parser = StudiesCSVParser(settings.file_path, settings.chunk_size)

        # Get existing row_ids for backfill logic
        self.existing_row_ids = set()
        if settings.enable_backfill:
            self.existing_row_ids = self._get_existing_row_ids()

    def _initialize_database_connection(self) -> DBConnection:
        """
        Initialize and test database connection.

        :return: Database connection instance
        :raises: SystemExit if connection fails
        """
        self.logger.info("Testing database connection...")
        if not self.db.test_connection():
            self.logger.error("Database connection failed.")
            raise SystemExit(1)
        return self.db

    def _get_existing_row_ids(self) -> Set[int]:
        """
        Retrieve existing row_ids from the staging table for backfill logic.

        :return: Set of existing row_ids
        """
        self.logger.info("Checking existing records for backfill logic...")
        with self.db.get_engine().connect() as conn:
            result = conn.execute(text("SELECT row_id FROM staging.raw_studies WHERE row_id IS NOT NULL"))
            existing_row_ids = {row[0] for row in result.fetchall()}
        self.logger.info(f"Found {len(existing_row_ids)} existing records in the database.")
        return existing_row_ids

    def _process_batch(self, batch: list, batch_count: int) -> None:
        """
        Process and insert a batch of records into the staging table.

        :param batch: List of record dictionaries to insert
        :param batch_count: Current batch number for logging
        """
        self.logger.info(f"Processing batch {batch_count} with {len(batch)} records...")
        staging_df = pd.DataFrame(batch)
        staging_df.to_sql("raw_studies", con=self.db.get_engine(), schema="staging", if_exists="append", index=False)

    def _print_ingestion_summary(self, processed_count: int, skipped_count: int) -> None:
        """
        Print summary statistics of the ingestion process.

        :param processed_count: Number of new records inserted
        :param skipped_count: Number of existing records skipped
        """
        total_records = processed_count + skipped_count
        self.logger.info("\n=== Ingestion Summary ===")
        self.logger.info(f"Total records processed: {total_records}")
        self.logger.info(f"New records inserted: {processed_count}")
        self.logger.info(f"Existing records skipped: {skipped_count}")
        self.logger.info("Data successfully ingested into staging.raw_studies table.")

    def _prepare_record_for_insertion(self, studies_raw) -> bool:
        """
        Prepare a record for insertion by checking backfill logic and setting timestamp.

        :param studies_raw: StudiesRaw instance to prepare
        :return: True if record should be processed, False if should be skipped
        """
        # Skip records that already exist (backfill logic)
        if self.enable_backfill and studies_raw.row_id in self.existing_row_ids:
            return False

        # Set UTC timezone for ingestion timestamp for consistency
        studies_raw.ingestion_timestamp = pd.Timestamp.now(tz="UTC").isoformat()
        return True

    def _process_records_in_batches(self) -> tuple[int, int]:
        """
        Process CSV records in batches and insert into database.

        :return: Tuple of (processed_count, skipped_count)
        """
        batch = []
        batch_count = 0
        skipped_count = 0
        processed_count = 0

        for studies_raw in self.parser.parse_csv():
            # Check if record should be processed or skipped
            if not self._prepare_record_for_insertion(studies_raw):
                skipped_count += 1
                continue

            # Add to batch
            batch.append(studies_raw.to_dict())
            processed_count += 1

            # Process batch when it reaches chunk_size
            if len(batch) >= self.chunk_size:
                batch_count += 1
                self._process_batch(batch, batch_count)
                batch = []  # Clear batch for next iteration

        # Process any remaining records in the final batch
        if batch:
            batch_count += 1
            self.logger.info(f"Processing final batch {batch_count} with {len(batch)} records...")
            self._process_batch(batch, batch_count)

        return processed_count, skipped_count

    def process(self) -> None:
        """
        Main method to process the CSV file and ingest data into staging table.

        :raises: Exception if processing fails
        """
        try:
            # Process records in batches
            processed_count, skipped_count = self._process_records_in_batches()

            # Print summary statistics
            self._print_ingestion_summary(processed_count, skipped_count)

        except Exception as e:
            self.logger.error(f"Error parsing CSV file: {e}")
            raise


if __name__ == "__main__":
    configs = AppConfig()
    configs.settings.file_path = "data/raw/clin_trials.csv"  # Example file path
    loader = CSVToStagingLoader(configs.settings)
    loader.process()
