#!/usr/bin/env python3
"""
Database Backup Operations for Sync Pipeline

Handles database backup creation and upload operations with comprehensive
error handling and cleanup.
"""

import asyncio
import logging
import sqlite3
import tempfile
import time
from datetime import UTC, datetime
from pathlib import Path
from typing import TYPE_CHECKING, TypedDict

from grin_transfer.common import compress_file_to_temp, get_compressed_filename

if TYPE_CHECKING:
    from grin_transfer.storage.book_manager import BookManager

logger = logging.getLogger(__name__)


async def backup_sqlite_database(source_path: Path, dest_path: Path) -> None:
    """
    Backup SQLite database using SQLite's backup API.

    This properly handles WAL mode databases by incorporating all
    committed transactions into the backup.

    Args:
        source_path: Path to source database
        dest_path: Path where backup should be created

    Raises:
        sqlite3.Error: If backup operation fails
    """

    def _backup():
        source_conn = sqlite3.connect(str(source_path))
        dest_conn = sqlite3.connect(str(dest_path))

        try:
            with dest_conn:
                source_conn.backup(dest_conn)
        finally:
            source_conn.close()
            dest_conn.close()

    loop = asyncio.get_event_loop()
    await loop.run_in_executor(None, _backup)


async def _cleanup_old_backups(backup_dir: Path, file_stem: str, file_suffix: str) -> None:
    """Keep only the last 10 backups to prevent disk space issues."""
    try:
        backup_pattern = f"{file_stem}_backup_*{file_suffix}"
        backup_files = list(backup_dir.glob(backup_pattern))

        if len(backup_files) > 10:
            # Sort by modification time (newest first)
            backup_files.sort(key=lambda f: f.stat().st_mtime, reverse=True)

            # Remove old backups beyond the 10 most recent
            for old_backup in backup_files[10:]:
                old_backup.unlink()
                logger.debug(f"Removed old backup: {old_backup.name}")

    except Exception as e:
        logger.warning(f"Failed to cleanup old backups: {e}")


class DatabaseBackupResult(TypedDict):
    """Result dictionary for database backup operations."""

    status: str  # "completed", "failed", "skipped"
    file_size: int
    compressed_size: int
    backup_time: float
    backup_filename: str | None


async def create_local_database_backup(db_path: Path, backup_dir: str | None = None) -> DatabaseBackupResult:
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

        backup_dir_path.mkdir(exist_ok=True)

        # Create timestamped backup filename
        now = datetime.now(UTC)
        timestamp = now.strftime("%Y%m%d_%H%M%S")
        backup_filename = f"{db_path_obj.stem}_backup_{timestamp}{db_path_obj.suffix}"
        backup_path = backup_dir_path / backup_filename

        # Create backup using SQLite backup API
        await backup_sqlite_database(db_path_obj, backup_path)
        backup_success = True

        if backup_success:
            result["file_size"] = backup_path.stat().st_size
            result["compressed_size"] = result["file_size"]  # Local backups are not compressed
            result["backup_filename"] = backup_filename
            result["status"] = "completed"
            logger.info(f"Database backup created: {backup_filename} ({result['file_size']} bytes)")

            # Keep only the last 10 backups to prevent disk space issues
            await _cleanup_old_backups(backup_dir_path, db_path_obj.stem, db_path_obj.suffix)
        else:
            result["status"] = "failed"
            logger.error("Database backup creation failed")

    except Exception as e:
        result["status"] = "failed"
        logger.error(f"Database backup failed with exception: {e}", exc_info=True)

    result["backup_time"] = time.time() - start_time
    return result


async def upload_database_to_storage(db_path: Path, book_manager: "BookManager"):
    """Upload database file to metadata bucket with compression.

    Args:
        db_path: Path to SQLite database file
        book_manager: BookManager instance for upload operations

    Returns:
        Result with upload operation data and metadata
    """
    from grin_transfer.sync.tasks.task_types import DatabaseBackupData, Result, TaskAction, TaskType

    start_time = time.time()

    try:
        base_filename = "books_latest.db"
        compressed_filename = get_compressed_filename(base_filename)
        storage_path = book_manager.meta_path(compressed_filename)

        # Get original file size
        file_size = db_path.stat().st_size

        # Create temporary backup using SQLite backup
        with tempfile.NamedTemporaryFile(suffix=".db", delete=True) as temp_backup:
            temp_backup_path = Path(temp_backup.name)
            await backup_sqlite_database(db_path, temp_backup_path)

            # Upload compressed database to both paths simultaneously
            async with compress_file_to_temp(temp_backup_path) as compressed_path:
                compressed_size = compressed_path.stat().st_size
                timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
                timestamped_path = book_manager.meta_path(f"timestamped/{timestamp}_{compressed_filename}")

                # Upload to both paths concurrently
                await asyncio.gather(
                    book_manager.storage.write_file(storage_path, str(compressed_path)),
                    book_manager.storage.write_file(timestamped_path, str(compressed_path)),
                )

        backup_time = time.time() - start_time
        compression_ratio = (1 - compressed_size / file_size) * 100 if file_size > 0 else 0
        logger.info(
            f"Database uploaded to storage: {storage_path} and {timestamped_path} "
            f"({file_size:,} -> {compressed_size:,} bytes, {compression_ratio:.1f}% compression)"
        )

        data = {
            "backup_filename": compressed_filename,
            "file_size": file_size,
            "backup_time": backup_time,
        }

        return Result(
            task_type=TaskType.FINAL_DATABASE_UPLOAD,
            action=TaskAction.COMPLETED,
            data=data,
        )

    except Exception as e:
        backup_time = time.time() - start_time
        logger.error(f"Database upload failed with exception: {e}", exc_info=True)

        return Result(
            task_type=TaskType.FINAL_DATABASE_UPLOAD,
            action=TaskAction.FAILED,
            error=f"Database upload failed: {e}",
            data=DatabaseBackupData(
                backup_filename=None,
                file_size=0,
                backup_time=backup_time,
            ),
        )
