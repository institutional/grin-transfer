#!/usr/bin/env python3
"""
Preflight Operations for Sync Pipeline

Handles batch-level operations that run before book processing begins,
including database backup and other initialization tasks.
"""

import logging
from pathlib import Path
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from grin_to_s3.sync.pipeline import SyncPipeline

from ..database.database_backup import create_local_database_backup, upload_database_to_storage
from .tasks.task_types import (
    DatabaseBackupData,
    DatabaseBackupResult,
    DatabaseUploadData,
    DatabaseUploadResult,
    TaskAction,
    TaskType,
)

logger = logging.getLogger(__name__)


async def run_database_backup(pipeline: "SyncPipeline") -> DatabaseBackupResult:
    """Create local database backup before sync processing."""
    if pipeline.skip_database_backup:
        logger.debug("Database backup skipped due to --skip-database-backup flag")
        return DatabaseBackupResult(
            task_type=TaskType.DATABASE_BACKUP,
            action=TaskAction.SKIPPED,
            reason="skip_database_backup_flag",
        )

    logger.info("Creating database backup before sync...")
    local_backup_result = await create_local_database_backup(pipeline.db_path)

    if local_backup_result["status"] != "completed":
        return DatabaseBackupResult(
            task_type=TaskType.DATABASE_BACKUP,
            action=TaskAction.FAILED,
            error=f"Local database backup failed: {local_backup_result['status']}",
        )

    logger.info(f"Local backup created: {local_backup_result['backup_filename']}")

    data: DatabaseBackupData = {
        "backup_filename": local_backup_result["backup_filename"],
        "file_size": local_backup_result["file_size"],
        "backup_time": local_backup_result["backup_time"],
    }

    return DatabaseBackupResult(
        task_type=TaskType.DATABASE_BACKUP,
        action=TaskAction.COMPLETED,
        data=data,
    )


async def run_database_upload(pipeline: "SyncPipeline") -> DatabaseUploadResult:
    """Upload database backup to block storage."""
    upload_result = await upload_database_to_storage(
        pipeline.db_path,
        pipeline.book_manager,
        upload_type="timestamped",
    )

    if upload_result["status"] != "completed":
        return DatabaseUploadResult(
            task_type=TaskType.DATABASE_UPLOAD,
            action=TaskAction.FAILED,
            error=f"Database backup upload failed: {upload_result['status']}",
        )

    logger.info(f"Database backup uploaded: {upload_result['backup_filename']}")

    data: DatabaseUploadData = {
        "backup_filename": upload_result["backup_filename"],
        "file_size": upload_result["file_size"],
        "compressed_size": upload_result["compressed_size"],
        "backup_time": upload_result["backup_time"],
    }

    return DatabaseUploadResult(
        task_type=TaskType.DATABASE_UPLOAD,
        action=TaskAction.COMPLETED,
        data=data,
    )


async def cleanup_local_backup(db_path: str, backup_filename: str) -> None:
    """Remove local database backup file after successful upload to block storage."""
    backup_file = Path(db_path).parent / "backups" / backup_filename
    backup_file.unlink(missing_ok=True)
    logger.debug(f"Cleaned up local backup: {backup_filename}")


async def run_preflight_operations(pipeline: "SyncPipeline") -> dict[str, DatabaseBackupResult | DatabaseUploadResult]:
    """Run all preflight operations before batch processing begins.

    Args:
        pipeline: SyncPipeline instance with configuration

    Returns:
        Dict mapping operation names to their results
    """
    results: dict[str, DatabaseBackupResult | DatabaseUploadResult] = {}

    # Skip preflight operations in dry-run mode
    if pipeline.dry_run:
        logger.info("Preflight operations skipped in dry-run mode")
        return results

    # Always create local backup
    backup_result = await run_database_backup(pipeline)
    results["database_backup"] = backup_result

    return results
