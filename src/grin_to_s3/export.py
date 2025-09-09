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
from datetime import datetime
from pathlib import Path

from grin_to_s3.collect_books.models import BookRecord, SQLiteProgressTracker
from grin_to_s3.common import compress_file_to_temp
from grin_to_s3.constants import BOOKS_EXPORT_CSV_FILENAME, OUTPUT_DIR
from grin_to_s3.database.database_utils import validate_database_file
from grin_to_s3.run_config import apply_run_config_to_args, load_run_config

logger = logging.getLogger(__name__)


async def write_books_to_csv(db_tracker, csv_path: Path) -> tuple[Path, int]:
    """Write books from database to a CSV file at the specified path.

    Args:
        db_tracker: SQLiteProgressTracker instance
        csv_path: Full path including filename for the CSV file

    Returns:
        Tuple of (csv_path, record_count)
    """
    # Get all books from database
    books = await db_tracker.get_all_books_csv_data()
    record_count = 0

    # Create parent directory if it doesn't exist
    csv_path.parent.mkdir(parents=True, exist_ok=True)

    # Write CSV file directly to specified path
    with csv_path.open("w", encoding="utf-8", newline="") as csv_file:
        writer = csv.writer(csv_file)
        writer.writerow(BookRecord.csv_headers())

        for book in books:
            record_count += 1
            writer.writerow(book.to_csv_row())

    logger.info(f"Wrote {record_count:,} records to CSV file at {csv_path}")
    return csv_path, record_count


async def upload_csv_to_storage(
    csv_path: Path,
    book_manager,
    compression_enabled: bool = True,
) -> str:
    """Upload a CSV file to storage with optional compression.

    Args:
        csv_path: Path to the CSV file to upload
        book_manager: BookManager for storage upload
        compression_enabled: Whether to compress for storage upload

    Returns:
        Storage URI where the file was uploaded
    """

    # Upload to storage (handles both local and cloud storage)
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")

    if compression_enabled:
        async with compress_file_to_temp(csv_path) as compressed_path:
            # Upload compressed file to both locations
            bucket_path = book_manager.meta_path("books_latest.csv.gz")
            bucket_path_timestamped = book_manager.meta_path(f"timestamped/{timestamp}_books.csv.gz")

            await asyncio.gather(
                book_manager.storage.write_file(bucket_path, str(compressed_path)),
                book_manager.storage.write_file(bucket_path_timestamped, str(compressed_path)),
            )
            logger.info(f"Uploaded compressed CSV to {bucket_path} and {bucket_path_timestamped}")
            return book_manager.storage.get_display_uri(bucket_path)
    else:
        # Upload uncompressed file to both locations
        bucket_path = book_manager.meta_path("books_latest.csv")
        bucket_path_timestamped = book_manager.meta_path(f"timestamped/{timestamp}_books.csv")

        await asyncio.gather(
            book_manager.storage.write_file(bucket_path, str(csv_path)),
            book_manager.storage.write_file(bucket_path_timestamped, str(csv_path)),
        )
        logger.info(f"Uploaded CSV to {bucket_path} and {bucket_path_timestamped}")
        return book_manager.storage.get_display_uri(bucket_path)


def create_parser() -> argparse.ArgumentParser:
    """Create command line parser for export functionality."""
    parser = argparse.ArgumentParser(
        description="Export all books in collection to CSV with available metadata",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Example:

  python grin.py export --run-name harvard_2024 --output books.csv

        """,
    )

    parser.add_argument("--run-name", required=True, help="Run name (e.g., harvard_2024)")
    parser.add_argument("--output", help=f"Output CSV file (default: output/{{run_name}}/{BOOKS_EXPORT_CSV_FILENAME})")
    parser.add_argument("--log-level", choices=["DEBUG", "INFO", "WARNING", "ERROR"], default="INFO")

    return parser


async def main():
    """Main entry point for export command."""
    args = create_parser().parse_args()
    run_config = load_run_config(OUTPUT_DIR / args.run_name / "run_config.json")
    apply_run_config_to_args(args, run_config)

    # Set default output path if not specified
    if not args.output:
        args.output = str(run_config.output_directory / BOOKS_EXPORT_CSV_FILENAME)

    # Validate database
    validate_database_file(run_config.sqlite_db_path, check_tables=True, check_books_count=True)

    print(f"Exporting books from {run_config.sqlite_db_path} to {args.output}")

    sqlite_tracker = SQLiteProgressTracker(run_config.sqlite_db_path)

    try:
        # Write directly to the user-specified output path
        csv_path, record_count = await write_books_to_csv(sqlite_tracker, Path(args.output))

        print(f"Wrote {record_count:,} rows to {csv_path}")

    finally:
        # Clean up database connections
        await sqlite_tracker.close()


if __name__ == "__main__":
    asyncio.run(main())
