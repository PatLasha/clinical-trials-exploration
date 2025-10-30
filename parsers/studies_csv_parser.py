import json
from typing import Iterator

import pandas as pd

from data_models.studies_raw import StudiesRaw


class StudiesCSVParser:
    def __init__(self, file_path: str, chunk_size: int = 1000):
        self.file_path = file_path
        self.chunk_size = chunk_size
        self.batch_id = str(pd.Timestamp.now().timestamp()).replace(".", "")

    def parse_csv(self) -> Iterator[StudiesRaw]:
        """
        Parse the CSV file in chunks and convert each row to StudiesRaw dataclass instances.
        :return: Iterator of StudiesRaw instances.
        """
        for chunk_num, chunk in enumerate(pd.read_csv(self.file_path, chunksize=self.chunk_size)):
            print(f"Processed chunk {chunk_num + 1} with {len(chunk)} records.")
            yield from self.chunk_to_raw_data(chunk)
        print("Completed parsing the CSV file.")

    def chunk_to_raw_data(self, chunk: pd.DataFrame) -> Iterator[StudiesRaw]:
        """
        Convert a DataFrame chunk to an iterator of StudiesRaw instances.
        :param chunk: DataFrame chunk.
        :return: Iterator of StudiesRaw instances.
        """
        yield from (self.row_to_studies(row) for _, row in chunk.iterrows())

    def row_to_studies(self, row: pd.Series) -> StudiesRaw:
        """
        Convert a DataFrame row to a StudiesRaw instance.
        :param row: DataFrame row.
        :return: StudiesRaw instance.
        """
        row_dict = row.to_dict()

        # Replace NaN values with None for proper JSON serialization
        cleaned_row_dict = {}
        for key, value in row_dict.items():
            if pd.isna(value):
                cleaned_row_dict[key] = None
            else:
                cleaned_row_dict[key] = value

        # Map CSV columns to dataclass fields
        studies_raw = StudiesRaw(
            batch_id=self.batch_id,
            source_file=self.file_path,  # Store the source file path
            raw_data=json.dumps(cleaned_row_dict),  # Store the entire row as raw data
            row_id=cleaned_row_dict.get("Unnamed: 0"),  # Assuming this is the row ID
            org_name=cleaned_row_dict.get("Organization Full Name"),
            org_class=cleaned_row_dict.get("Organization Class"),
            responsible_party=cleaned_row_dict.get("Responsible Party"),
            brief_title=cleaned_row_dict.get("Brief Title"),
            full_title=cleaned_row_dict.get("Full Title"),
            overall_status=cleaned_row_dict.get("Overall Status"),
            start_date=cleaned_row_dict.get("Start Date"),
            standard_age=cleaned_row_dict.get("Standard Age"),
            conditions=cleaned_row_dict.get("Conditions"),
            primary_purpose=cleaned_row_dict.get("Primary Purpose"),
            interventions=cleaned_row_dict.get("Interventions"),
            intervention_description=cleaned_row_dict.get("Intervention Description"),
            study_type=cleaned_row_dict.get("Study Type"),
            phase=cleaned_row_dict.get("Phases"),
            outcome_measure=cleaned_row_dict.get("Outcome Measure"),
            medical_subject_heading=cleaned_row_dict.get("Medical Subject Headings"),
        )
        return studies_raw


if __name__ == "__main__":
    parser = StudiesCSVParser("data/raw/clin_trials.csv")
    for study in parser.parse_csv():
        print(study)
