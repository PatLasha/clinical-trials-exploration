from typing import Set

import pandas as pd
from sqlalchemy import text

from db.db_connection import DBConnection
from parsers.studies_csv_parser import StudiesCSVParser


class CSVToStagingProcessor:
    """
    A class to handle CSV ingestion into the staging.raw_studies table.

    This class encapsulates all the logic for processing CSV files, managing
    database connections, handling backfill logic, and batch processing.
    """

    def __init__(self, file_path: str, chunk_size: int = 1000, enable_backfill: bool = True):
        """
        Initialize the CSV to staging processor.

        :param file_path: Path to the CSV file
        :param chunk_size: Number of rows to process in each chunk
        :param enable_backfill: If True, skip records that already exist based on row_id
        """
        self.file_path = file_path
        self.chunk_size = chunk_size
        self.enable_backfill = enable_backfill

        # Initialize database connection and parser
        self.db = self._initialize_database_connection()
        self.parser = StudiesCSVParser(file_path, chunk_size)

        # Get existing row_ids for backfill logic
        self.existing_row_ids = set()
        if enable_backfill:
            self.existing_row_ids = self._get_existing_row_ids()

    def _initialize_database_connection(self) -> DBConnection:
        """
        Initialize and test database connection.

        :return: Database connection instance
        :raises: SystemExit if connection fails
        """
        print("Testing database connection...")
        db = DBConnection()
        if not db.test_connection():
            print("Database connection failed.")
            raise SystemExit(1)
        return db

    def _get_existing_row_ids(self) -> Set[int]:
        """
        Retrieve existing row_ids from the staging table for backfill logic.

        :return: Set of existing row_ids
        """
        print("Checking existing records for backfill logic...")
        with self.db.get_engine().connect() as conn:
            result = conn.execute(text("SELECT row_id FROM staging.raw_studies WHERE row_id IS NOT NULL"))
            existing_row_ids = {row[0] for row in result.fetchall()}
        print(f"Found {len(existing_row_ids)} existing records in the database.")
        return existing_row_ids

    def _process_batch(self, batch: list, batch_count: int) -> None:
        """
        Process and insert a batch of records into the staging table.

        :param batch: List of record dictionaries to insert
        :param batch_count: Current batch number for logging
        """
        print(f"Processing batch {batch_count} with {len(batch)} records...")
        staging_df = pd.DataFrame(batch)
        staging_df.to_sql("raw_studies", con=self.db.get_engine(), schema="staging", if_exists="append", index=False)

    def _print_ingestion_summary(self, processed_count: int, skipped_count: int) -> None:
        """
        Print summary statistics of the ingestion process.

        :param processed_count: Number of new records inserted
        :param skipped_count: Number of existing records skipped
        """
        total_records = processed_count + skipped_count
        print("\n=== Ingestion Summary ===")
        print(f"Total records processed: {total_records}")
        print(f"New records inserted: {processed_count}")
        print(f"Existing records skipped: {skipped_count}")
        print("Data successfully ingested into staging.raw_studies table.")

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
            print(f"Processing final batch {batch_count} with {len(batch)} records...")
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
            print(f"Error parsing CSV file: {e}")
            raise e


def csv_to_staging(file_path: str, chunk_size: int = 1000, enable_backfill: bool = True):
    """
    Convenience function to process CSV file using the CSVToStagingProcessor class.

    :param file_path: Path to the CSV file
    :param chunk_size: Number of rows to process in each chunk
    :param enable_backfill: If True, skip records that already exist based on row_id
    """
    processor = CSVToStagingProcessor(file_path, chunk_size, enable_backfill)
    processor.process()


if __name__ == "__main__":
    csv_file_path = "data/raw/clin_trials.csv"  # Update with your CSV file path
    csv_to_staging(csv_file_path)
