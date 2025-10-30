#!/usr/bin/env python3
"""
Example usage of the CSVToStagingProcessor class.

This file demonstrates different ways to use the CSV processing functionality.
"""

from scripts.csv_to_staging import CSVToStagingProcessor, csv_to_staging


def example_class_usage():
    """Example of using the CSVToStagingProcessor class directly."""
    print("=== Using CSVToStagingProcessor class directly ===")

    # Create processor instance with custom settings
    processor = CSVToStagingProcessor(
        file_path="data/raw/clin_trials.csv", chunk_size=500, enable_backfill=True  # Smaller batch size
    )

    # Process the CSV file
    processor.process()


def example_convenience_function():
    """Example of using the convenience function."""
    print("\n=== Using convenience function ===")

    # Use the convenience function
    csv_to_staging(file_path="data/raw/clin_trials.csv", chunk_size=2000, enable_backfill=True)  # Larger batch size


def example_no_backfill():
    """Example of processing without backfill (will insert duplicates)."""
    print("\n=== Processing without backfill (demonstration only) ===")

    # WARNING: This would insert duplicates - only for demonstration
    # processor = CSVToStagingProcessor(
    #     file_path="data/raw/clin_trials.csv",
    #     chunk_size=1000,
    #     enable_backfill=False  # This would cause duplicates
    # )
    # processor.process()

    print("Skipped - would create duplicates in the database")


if __name__ == "__main__":
    # Example 1: Using the class directly
    example_class_usage()

    # Example 2: Using the convenience function (commented out to avoid duplicate processing)
    # example_convenience_function()

    # Example 3: Without backfill (demonstration only)
    example_no_backfill()
