import dataclasses
from typing import Optional

@dataclasses.dataclass
class Settings:
    db_url: str
    entry_point: str
    file_path: str # this will be optional when I start implementing API ingestion
    chunk_size: int = 1000
    enable_backfill: bool = True
    log_level: Optional[str] = "INFO"