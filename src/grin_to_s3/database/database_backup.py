#!/usr/bin/env python3
"""
Database Backup Operations for Sync Pipeline

Handles database backup creation and upload operations with comprehensive
error handling and cleanup.
"""

import logging
import time
from datetime import UTC, datetime
from pathlib import Path
from typing import TYPE_CHECKING, TypedDict

from grin_to_s3.common import BackupManager, compress_file_to_temp, get_compressed_filename

if TYPE_CHECKING:
    from grin_to_s3.storage.book_manager import BookManager

logger = logging.getLogger(__name__)


class DatabaseBackupResult(TypedDict):
    """Result dictionary for database backup operations."""

    status: str  # "completed", "failed", "skipped"
    file_size: int
    compressed_size: int
    backup_time: float
    backup_filename: str | None


async def create_local_database_backup(db_path: str, backup_dir: str | None = None) -> DatabaseBackupResult:
    """Create local timestamped backup of database.

    Args:
        db_path: Path to SQLite database file
        backup_dir: Optional custom backup directory (defaults to db_path parent/backups)

    Returns:
        Dict with backup operation results
    """
    start_time = time.time()
    result: DatabaseBackupResult = {
        "status": "pending",
        "file_size": 0,
        "compressed_size": 0,
        "backup_time": 0.0,
        "backup_filename": None,
    }

    try:
        db_path_obj = Path(db_path)
        if not db_path_obj.exists():
            logger.warning(f"Database file does not exist: {db_path}")
            result["status"] = "skipped"
            result["backup_time"] = time.time() - start_time
            return result

        # Set up backup directory
        if backup_dir is None:
            backup_dir_path = db_path_obj.parent / "backups"
        else:
            backup_dir_path = Path(backup_dir)

        backup_manager = BackupManager(backup_dir_path)

        # Create backup
        backup_success = await backup_manager.backup_file(db_path, "database")

        if backup_success:
            # Find the most recent backup file to get details
            backup_pattern = f"{db_path_obj.stem}_backup_*{db_path_obj.suffix}"
            backup_files = list(backup_dir_path.glob(backup_pattern))

            if backup_files:
                # Get most recent backup
                latest_backup = max(backup_files, key=lambda f: f.stat().st_mtime)
                result["file_size"] = latest_backup.stat().st_size
                result["compressed_size"] = latest_backup.stat().st_size  # Local backups are not compressed
                result["backup_filename"] = latest_backup.name
                result["status"] = "completed"
                logger.info(f"Database backup created: {latest_backup.name} ({result['file_size']} bytes)")
            else:
                result["status"] = "failed"
                logger.error("Backup appeared to succeed but no backup file found")
        else:
            result["status"] = "failed"
            logger.error("Database backup creation failed")

    except Exception as e:
        result["status"] = "failed"
        logger.error(f"Database backup failed with exception: {e}", exc_info=True)

    result["backup_time"] = time.time() - start_time
    return result


async def upload_database_to_storage(
    db_path: str, book_manager: "BookManager", run_name: str, upload_type: str = "latest"
) -> DatabaseBackupResult:
    """Upload database file to metadata bucket with compression.

    Args:
        db_path: Path to SQLite database file
        book_manager: BookManager instance for upload operations
        run_name: Name of the run for path organization
        upload_type: "latest" for books_latest.db.gz or "timestamped" for books_backup_{timestamp}.db.gz

    Returns:
        Dict with upload operation results
    """
    start_time = time.time()
    result: DatabaseBackupResult = {
        "status": "pending",
        "file_size": 0,
        "compressed_size": 0,
        "backup_time": 0.0,
        "backup_filename": None,
    }

    try:
        db_path_obj = Path(db_path)
        if not db_path_obj.exists():
            logger.warning(f"Database file does not exist: {db_path}")
            result["status"] = "skipped"
            result["backup_time"] = time.time() - start_time
            return result

        # Generate filename based on upload type (with compression)
        if upload_type == "latest":
            base_filename = "books_latest.db"
            compressed_filename = get_compressed_filename(base_filename)
            storage_path = book_manager.meta_path(compressed_filename)
        else:  # timestamped
            timestamp = datetime.now(UTC).strftime("%Y%m%d_%H%M%S")
            base_filename = f"books_{timestamp}.db"
            compressed_filename = get_compressed_filename(base_filename)
            storage_path = book_manager.meta_path(f"timestamped/{compressed_filename}")

        result["backup_filename"] = compressed_filename

        # Get original file size
        result["file_size"] = db_path_obj.stat().st_size

        # Upload compressed database
        async with compress_file_to_temp(db_path_obj) as compressed_path:
            result["compressed_size"] = compressed_path.stat().st_size
            await book_manager.storage.write_file(storage_path, str(compressed_path))

        result["status"] = "completed"
        compression_ratio = (
            (1 - result["compressed_size"] / result["file_size"]) * 100 if result["file_size"] > 0 else 0
        )
        logger.info(
            f"Database uploaded to storage: {compressed_filename} "
            f"({result['file_size']:,} -> {result['compressed_size']:,} bytes, {compression_ratio:.1f}% compression)"
        )

    except Exception as e:
        result["status"] = "failed"
        logger.error(f"Database upload failed with exception: {e}", exc_info=True)

    result["backup_time"] = time.time() - start_time
    return result
