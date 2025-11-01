import logging

from data_models.table_models.raw_studies import RawStudies


class DataValidator:
    """Validates raw study data before transformation."""

    def __init__(self):
        self.logger = logging.getLogger(__name__)

    def validate_study(self, study: RawStudies) -> bool:
        """
        Validate a single raw study record.
        :param study: RawStudies instance to validate
        :return: True if valid, False otherwise
        """
        if not study.row_id:
            self.logger.warning(f"Study missing row_id: {study.id}")
            return False

        if not study.brief_title and not study.full_title:
            self.logger.warning(f"Study {study.row_id} missing both titles")
            return False

        return True

    def validate_batch(self, studies: list[RawStudies]) -> tuple[list[RawStudies], list[RawStudies]]:
        """
        Validate a batch of studies.
        :param studies: List of RawStudies to validate
        :return: Tuple of (valid_studies, invalid_studies)
        """
        valid = []
        invalid = []

        for study in studies:
            if self.validate_study(study):
                valid.append(study)
            else:
                invalid.append(study)

        self.logger.info(f"Validated {len(valid)} valid, {len(invalid)} invalid studies")
        return valid, invalid
