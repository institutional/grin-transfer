#!/usr/bin/env python3
"""
CSV Export Operations for Sync Pipeline

Handles CSV export and upload operations with comprehensive error handling
and cleanup. Provides utility functions for exporting database contents
to CSV format and uploading to storage.
"""

import csv
import logging
import shutil
import time
from datetime import UTC, datetime
from pathlib import Path

from grin_to_s3.collect_books.models import BookRecord, SQLiteProgressTracker
from grin_to_s3.storage.staging import StagingDirectoryManager

from .models import FileResult

logger = logging.getLogger(__name__)


class CSVExportResult(FileResult):
    """Result dictionary for CSV export and upload operations."""

    num_rows: int
    export_time: float


async def export_and_upload_csv(
    db_path: str,
    staging_manager: StagingDirectoryManager,
    book_manager,
    skip_export: bool = False,
    custom_filename: str | None = None,
) -> CSVExportResult:
    """Export CSV from database and upload to storage with comprehensive error handling.

    Creates temporary CSV file in staging directory, uploads to both latest and
    timestamped locations in metadata bucket, and ensures proper cleanup.

    Args:
        db_path: Path to SQLite database file
        staging_manager: StagingDirectoryManager instance for temporary file creation
        book_manager: BookStorage instance for upload operations
        skip_export: If True, skip export and return early
        custom_filename: Optional custom filename for latest version

    Returns:
        Dict with operation results:
        - status: str - Operation status ("completed", "failed", "skipped")
        - file_size: int - Size of exported CSV file in bytes
        - num_rows: int - Number of rows exported (including header)
        - export_time: float - Time taken for export operation in seconds

    Raises:
        Exception: Only if cleanup fails after successful operation
    """
    start_time = time.time()
    result: CSVExportResult = {
        "status": "pending",
        "file_size": 0,
        "num_rows": 0,
        "export_time": 0.0,
    }

    if skip_export:
        logger.info("CSV export skipped due to skip_export flag")
        result["status"] = "skipped"
        result["export_time"] = time.time() - start_time
        return result

    temp_csv_path = None
    try:
        logger.debug(f"Using staging directory: {staging_manager.staging_path}")

        # Create CSV file in staging directory with proper name
        csv_filename = custom_filename or "books_export.csv"
        csv_path = staging_manager.staging_path / csv_filename
        temp_csv_path = str(csv_path)
        logger.debug(f"Creating CSV file: {temp_csv_path}")

        # Export to CSV file
        sqlite_tracker = SQLiteProgressTracker(db_path)
        books = await sqlite_tracker.get_all_books_csv_data()
        logger.info(f"Exporting {len(books)} books to CSV")

        # Write CSV data to file
        with open(csv_path, "w", encoding="utf-8", newline="") as csv_file:
            writer = csv.writer(csv_file)
            writer.writerow(BookRecord.csv_headers())
            for book in books:
                writer.writerow(book.to_csv_row())

        logger.debug(f"CSV data written to file: {temp_csv_path}")

        # Get file size and row count
        csv_file_size = Path(temp_csv_path).stat().st_size
        result["file_size"] = csv_file_size
        result["num_rows"] = len(books) + 1
        logger.info(
            f"CSV export completed successfully: {len(books)} books, {result['num_rows']} rows, {csv_file_size} bytes"
        )

        # Upload CSV file to storage
        logger.info("Uploading CSV file to storage")
        latest_path, timestamped_path = await book_manager.upload_csv_file(temp_csv_path, custom_filename)

        logger.info(f"CSV upload completed successfully: {latest_path}")

        # Mark overall operation as successful
        result["status"] = "completed"
        result["export_time"] = time.time() - start_time

    except Exception as e:
        logger.error(f"CSV export and upload failed: {e}", exc_info=True)
        result["status"] = "failed"
        result["export_time"] = time.time() - start_time


    return result


async def export_csv_local(book_manager, db_path: str) -> CSVExportResult:
    """Export CSV directly to local storage without temporary files."""
    start_time = time.time()
    try:
        sqlite_tracker = SQLiteProgressTracker(db_path)
        books = await sqlite_tracker.get_all_books_csv_data()
        logger.info(f"Exporting {len(books)} books to local CSV")

        timestamp = datetime.now(UTC).strftime("%Y%m%d_%H%M%S")
        base_path = book_manager.storage.config.options.get("base_path")
        if not base_path:
            raise ValueError("Local storage requires base_path in configuration")

        latest_path = Path(base_path) / "meta" / "books_latest.csv"
        timestamped_path = Path(base_path) / "meta" / "timestamped" / f"books_{timestamp}.csv"

        latest_path.parent.mkdir(parents=True, exist_ok=True)
        timestamped_path.parent.mkdir(parents=True, exist_ok=True)

        with open(latest_path, "w", encoding="utf-8", newline="") as f:
            writer = csv.writer(f)
            writer.writerow(BookRecord.csv_headers())
            for book in books:
                writer.writerow(book.to_csv_row())

        shutil.copy2(latest_path, timestamped_path)

        file_size = latest_path.stat().st_size
        num_rows = len(books) + 1

        logger.info(f"CSV written directly to {latest_path}")
        logger.info(f"CSV timestamped copy at {timestamped_path}")

        return {
            "status": "completed",
            "num_rows": num_rows,
            "file_size": file_size,
            "export_time": time.time() - start_time,
        }

    except Exception as e:
        logger.error(f"Local CSV export failed: {e}", exc_info=True)
        return {"status": "failed", "file_size": 0, "num_rows": 0, "export_time": time.time() - start_time}
