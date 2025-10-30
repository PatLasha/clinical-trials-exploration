import logging
import os
from datetime import datetime
from logging.handlers import TimedRotatingFileHandler

from dotenv import load_dotenv

from data_models.settings import Settings

load_dotenv()


class AppConfig:
    """
    Application configuration and logging setup.
    Manages environment variables and initializes logging.
    """

    def __init__(self):
        self.logger = logging.getLogger(__name__)
        try:
            self._validate_required_env_vars()
            self.settings = self._create_settings()
            self.setup_logging()
        except Exception as e:
            self.logger.error(f"Failed to initialize AppConfig: {e}")
            raise

    def _validate_required_env_vars(self):
        """Validate that all required environment variables are set."""
        required_vars = ["DB_URL", "ENTRY_POINT"]
        missing_vars = [var for var in required_vars if not os.getenv(var)]

        # FILE_PATH is only required for csv_to_staging entry point
        entry_point = os.getenv("ENTRY_POINT")
        if entry_point == "csv_to_staging" and not os.getenv("FILE_PATH"):
            missing_vars.append("FILE_PATH")

        if missing_vars:
            raise RuntimeError(
                f"Required environment variables are missing: {', '.join(missing_vars)}. "
                f"Please check your .env file."
            )

    def _create_settings(self):
        """Create Settings object from environment variables."""
        try:
            return Settings(
                db_url=os.getenv("DB_URL", ""),
                entry_point=os.getenv("ENTRY_POINT", ""),
                file_path=os.getenv("FILE_PATH", ""),
                chunk_size=int(os.getenv("CHUNK_SIZE", 1000)),
                enable_backfill=os.getenv("ENABLE_BACKFILL", "true").lower() == "true",
                log_level=os.getenv("LOG_LEVEL", "INFO"),
            )
        except Exception as e:
            self.logger.error(f"Error creating Settings object: {e}")
            raise

    def setup_logging(self):
        """Setup logging configuration with file and console output."""
        try:
            # Ensure logs directory exists
            logs_dir = "logs"
            os.makedirs(logs_dir, exist_ok=True)

            # Get current date for log filename
            current_date = datetime.now().strftime("%d-%m-%Y")
            log_filename = f"log-{current_date}.log"
            log_filepath = os.path.join(logs_dir, log_filename)

            # Set log level, default to INFO if not specified or invalid
            log_level = getattr(logging, (self.settings.log_level or "INFO").upper(), logging.INFO)

            # Create formatter
            formatter = logging.Formatter("%(asctime)s - %(name)s - %(levelname)s - %(message)s")

            # Get root logger
            root_logger = logging.getLogger()
            root_logger.setLevel(log_level)

            # Clear any existing handlers to avoid duplication
            root_logger.handlers.clear()

            # Create file handler with daily rotation
            file_handler = TimedRotatingFileHandler(
                log_filepath, when="midnight", interval=1, backupCount=30, encoding="utf-8"  # Keep 30 days of logs
            )
            file_handler.setLevel(log_level)
            file_handler.setFormatter(formatter)

            # Create console handler
            console_handler = logging.StreamHandler()
            console_handler.setLevel(log_level)
            console_handler.setFormatter(formatter)

            # Add handlers to root logger
            root_logger.addHandler(file_handler)
            root_logger.addHandler(console_handler)

            # Log the initial setup message
            logging.info(f"Logging initialized. Logs will be saved to: {log_filepath}")

        except Exception as e:
            # Fallback to basic logging if file setup fails
            logging.basicConfig(
                level=logging.INFO,
                format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
            )
            logging.error(f"Error setting up file logging: {e}. Using basic logging instead.")
            raise
