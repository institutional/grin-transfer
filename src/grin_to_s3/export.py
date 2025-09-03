#!/usr/bin/env python3
"""
CSV Export Module

Exports books from SQLite database to CSV format. Works at any pipeline stage
and exports books with whatever metadata is available.
"""

import argparse
import asyncio
import csv
import logging
import sys
from pathlib import Path

from grin_to_s3.collect_books.models import BookRecord, SQLiteProgressTracker
from grin_to_s3.constants import OUTPUT_DIR
from grin_to_s3.database.database_utils import validate_database_file
from grin_to_s3.logging_config import setup_logging
from grin_to_s3.run_config import apply_run_config_to_args, load_run_config

logger = logging.getLogger(__name__)


async def export_csv(db_path: Path, output_file: str) -> None:
    """Export books from database to CSV format.

    Exports books in the collection with whatever metadata is available.

    Args:
        db_path: Path to SQLite database
        output_file: Output CSV file path
    """
    print(f"Exporting books from {db_path} to {output_file}")

    sqlite_tracker = SQLiteProgressTracker(db_path)

    try:
        books = await sqlite_tracker.get_all_books_csv_data()

        print(f"Found {len(books):,} books in database")

        # Write CSV
        with open(output_file, "w", newline="", encoding="utf-8") as f:
            writer = csv.writer(f)
            writer.writerow(BookRecord.csv_headers())

            for book in books:
                writer.writerow(book.to_csv_row())

        print(f"✅ CSV export completed: {output_file}")

    except Exception as e:
        print(f"❌ CSV export failed: {e}")
        logger.error(f"CSV export failed: {e}", exc_info=True)
        raise
    finally:
        # Clean up database connections
        await sqlite_tracker.close()


def create_parser() -> argparse.ArgumentParser:
    """Create command line parser for export functionality."""
    parser = argparse.ArgumentParser(
        description="Export ALL books in collection to CSV with available metadata",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Example:

  python grin.py export --run-name harvard_2024 --output books.csv

Note: This command exports ALL books in the database regardless of processing stage.
      Books will include whatever metadata is available (basic after collect,
      enriched after enrich, sync status after sync, MARC metadata after
      METS extraction).
        """,
    )

    parser.add_argument("--run-name", required=True, help="Run name (e.g., harvard_2024)")
    parser.add_argument("--output", default="books.csv", help="Output CSV file (default: books.csv)")
    parser.add_argument("--log-level", choices=["DEBUG", "INFO", "WARNING", "ERROR"], default="INFO")

    return parser


async def main():
    """Main entry point for export command."""
    args = create_parser().parse_args()
    run_config = load_run_config(OUTPUT_DIR / args.run_name / "run_config.json")
    apply_run_config_to_args(args, run_config)

    # Validate database
    validate_database_file(run_config.sqlite_db_path, check_tables=True, check_books_count=True)
    setup_logging(args.log_level, run_config.log_file)

    # Log export startup
    logger = logging.getLogger(__name__)
    logger.info(f"CSV EXPORT STARTED - output={args.output}")

    try:
        await export_csv(run_config.sqlite_db_path, args.output)
        logger.info("CSV export completed successfully")
    except Exception as e:
        logger.error(f"CSV export failed: {e}", exc_info=True)
        sys.exit(1)


if __name__ == "__main__":
    asyncio.run(main())
