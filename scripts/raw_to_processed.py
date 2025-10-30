import logging
from typing import Optional

from data_models.table_models.raw_studies import RawStudies
from db.db_connection import DBConnection


class ProcessRawData:
    """
    A class to process raw data from the raw_studies table.
    """

    def __init__(self, db: DBConnection):
        self.db = db
        self.logger = logging.getLogger(__name__)

    def get_all_raw_studies(self) -> list[RawStudies]:
        """
        Retrieve all records from the raw_studies table.
        :return: List of RawStudies records
        """
        with self.db.get_session() as session:
            records = session.query(RawStudies).all()
        return records

    def get_raw_studies(self, batch_id: Optional[str] = None) -> list[RawStudies]:
        """
        Get the raw studies model.
        :param batch_id: The batch ID to filter studies.
        :return: List of RawStudies objects matching the batch_id.
        """

        try:
            with self.db.get_session() as session:
                studies = session.query(RawStudies).filter(RawStudies.batch_id == batch_id).all()
                self.logger.info(f"Retrieved {len(studies)} studies for batch_id {batch_id}.")
                return studies
        except Exception as e:
            self.logger.error(f"Error retrieving raw studies for batch_id {batch_id}: {e}")
            raise

    def get_all_batch_ids(self) -> list[str]:
        """
        Get all unique batch IDs from the raw_studies table.
        :return: List of unique batch_id values.
        """
        try:
            with self.db.get_session() as session:
                batch_ids = session.query(RawStudies.batch_id).distinct().all()
                # Extract batch_id from tuples and filter out None values
                unique_batch_ids = [bid[0] for bid in batch_ids if bid[0] is not None]
                self.logger.info(f"Found {len(unique_batch_ids)} unique batch IDs.")
                return unique_batch_ids
        except Exception as e:
            self.logger.error(f"Error retrieving batch IDs: {e}")
            raise


if __name__ == "__main__":
    from configs.app_config import AppConfig

    configs = AppConfig()
    db = DBConnection(configs.settings)
    db.test_connection()
    processor = ProcessRawData(db)

    batch_ids = processor.get_all_batch_ids()

    for batch_id in batch_ids:
        studies = processor.get_raw_studies(batch_id)
        print(f"Batch ID: {batch_id}, Number of Studies: {len(studies)}")
