#!/usr/bin/env python3
"""
CSV Export Operations for Sync Pipeline

Handles CSV export and upload operations with comprehensive error handling
and cleanup. Provides utility functions for exporting database contents
to CSV format and uploading to storage.
"""

import csv
import logging
import tempfile
import time
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
    book_storage,
    skip_export: bool = False,
    custom_filename: str | None = None,
) -> CSVExportResult:
    """Export CSV from database and upload to storage with comprehensive error handling.

    Creates temporary CSV file in staging directory, uploads to both latest and
    timestamped locations in metadata bucket, and ensures proper cleanup.

    Args:
        db_path: Path to SQLite database file
        staging_manager: StagingDirectoryManager instance for temporary file creation
        book_storage: BookStorage instance for upload operations
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

        # Create temporary CSV file in staging directory
        with tempfile.NamedTemporaryFile(
            mode="w", suffix=".csv", dir=staging_manager.staging_path, delete=False, encoding="utf-8"
        ) as temp_file:
            temp_csv_path = temp_file.name
            logger.debug(f"Created temporary CSV file: {temp_csv_path}")

            # Export to temporary file
            sqlite_tracker = SQLiteProgressTracker(db_path)
            books = await sqlite_tracker.get_all_books_csv_data()
            logger.info(f"Exporting {len(books)} books to CSV")

            # Write CSV data to temporary file
            writer = csv.writer(temp_file)
            writer.writerow(BookRecord.csv_headers())
            for book in books:
                writer.writerow(book.to_csv_row())

            temp_file.flush()
            logger.debug(f"CSV data written to temporary file: {temp_csv_path}")

        # Get file size and row count
        csv_file_size = Path(temp_csv_path).stat().st_size
        result["file_size"] = csv_file_size
        result["num_rows"] = len(books) + 1
        logger.info(
            f"CSV export completed successfully: {len(books)} books, {result['num_rows']} rows, {csv_file_size} bytes"
        )

        # Upload CSV file to storage
        logger.info("Uploading CSV file to storage")
        latest_path, timestamped_path = await book_storage.upload_csv_file(
            temp_csv_path, custom_filename
        )

        logger.info(f"CSV upload completed successfully: {latest_path}")

        # Mark overall operation as successful
        result["status"] = "completed"
        result["export_time"] = time.time() - start_time

    except Exception as e:
        logger.error(f"CSV export and upload failed: {e}", exc_info=True)
        result["status"] = "failed"
        result["export_time"] = time.time() - start_time

    finally:
        # Clean up temporary file in all cases
        if temp_csv_path and Path(temp_csv_path).exists():
            try:
                Path(temp_csv_path).unlink()
                logger.debug(f"Cleaned up temporary file: {temp_csv_path}")
            except Exception as cleanup_error:
                logger.warning(f"Failed to cleanup temporary CSV file {temp_csv_path}: {cleanup_error}")
                # Don't fail the overall operation due to cleanup failure

    return result
