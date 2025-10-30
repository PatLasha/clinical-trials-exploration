from configs.app_config import AppConfig
from scripts.csv_to_staging import CSVToStagingLoader
from scripts.init_db import init_db
import logging

def main():
    """
    Main entry point for the application.
    Initializes configuration and starts the CSV to staging process.
    """
    try:
        # Initialize application configuration
        app_config = AppConfig()
        logger = logging.getLogger(__name__)
        
        logger.info(f"Application started with entry point: {app_config.settings.entry_point}")
        if app_config.settings.entry_point == "csv_to_staging":
            loader = CSVToStagingLoader(app_config.settings)
            loader.process()
        elif app_config.settings.entry_point == "init_db":
            init_db(app_config.settings)
        else:
            logging.error(f"Unknown entry point: {app_config.settings.entry_point}")
            raise ValueError(f"Unknown entry point: {app_config.settings.entry_point}")

    except Exception as e:
        raise e
    
if __name__ == "__main__":
    main()