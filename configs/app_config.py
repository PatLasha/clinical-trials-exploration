from dotenv import load_dotenv
import os
import logging

load_dotenv()

class AppConfig:
    """
    Application configuration and logging setup.
    Manages environment variables and initializes logging.
    """
    def __init__(self):
        self.db_url = os.environ.get("DB_URL")
        self.log_level = os.environ.get("LOG_LEVEL", "INFO")

    def get_db_url(self):
        return self.db_url
    
    def setup_logging(self):
        """Setup logging configuration based on LOG_LEVEL environment variable."""
        logging.basicConfig(
            level=getattr(logging, self.log_level.upper()),
            # format will look like: 2024-06-01 12:00:00,000 - root - INFO - This is a log message
            format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
        )
        logging.info("Logging configured successfully")