import pandas as pd

from db.db_connection import DBConnection
from parsers.studies_csv_parser import StudiesCSVParser


def csv_to_staging(file_path: str, chunk_size: int = 1000):
    """
    Parse the CSV file in chunks and convert each row to StudiesRaw dataclass instances.
    :param file_path: Path to the CSV file.
    :param chunk_size: Number of rows per chunk.
    :return: Iterator of StudiesRaw instances.
    """
    db = DBConnection()

    print("Initializing database schema...")
    if not db.test_connection():
        print("Cannot initialize database schema: Database connection failed.")
        return

    try:
        parser = StudiesCSVParser(file_path, chunk_size)
        parsed_data = [studies_raw for studies_raw in parser.parse_csv()]
        staging_df = pd.DataFrame([data.to_dict() for data in parsed_data])
        staging_df.to_sql("staging.raw_studies", con=db.get_engine(), if_exists="append", index=False)
    except Exception as e:
        print(f"Error parsing CSV file: {e}")
        raise e

    print("Data successfully ingested into staging.raw_studies table.")


if __name__ == "__main__":
    csv_file_path = "data/raw/clin_trials.csv"  # Update with your CSV file path
    csv_to_staging(csv_file_path)
