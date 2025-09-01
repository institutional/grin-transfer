#!/usr/bin/env python3
"""
Teardown Operations for Sync Pipeline

Handles batch-level operations that run after book processing completes,
including final database upload and staging cleanup.
"""

import logging
import shutil
import time
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from grin_to_s3.sync.pipeline import SyncPipeline

from ..database.database_backup import upload_database_to_storage
from .tasks import export_csv
from .tasks.task_types import (
    CsvExportTeardownResult,
    FinalDatabaseUploadData,
    FinalDatabaseUploadResult,
    StagingCleanupData,
    StagingCleanupResult,
    TaskAction,
    TaskType,
)

logger = logging.getLogger(__name__)


async def run_csv_export(pipeline: "SyncPipeline") -> CsvExportTeardownResult:
    """Export book metadata to CSV format as part of teardown operations."""
    if pipeline.dry_run:
        logger.debug("CSV export skipped in dry-run mode")
        return CsvExportTeardownResult(
            task_type=TaskType.EXPORT_CSV,
            action=TaskAction.SKIPPED,
            reason="skip_dry_run",
        )

    if pipeline.skip_csv_export:
        logger.debug("CSV export skipped due to --skip-csv-export flag")
        return CsvExportTeardownResult(
            task_type=TaskType.EXPORT_CSV,
            action=TaskAction.SKIPPED,
            reason="skip_csv_export",
        )

    try:
        result = await export_csv.main(pipeline)

        # Return the result directly since it's already the right type
        return result

    except Exception as e:
        logger.error(f"CSV export failed: {e}", exc_info=True)
        return CsvExportTeardownResult(
            task_type=TaskType.EXPORT_CSV,
            action=TaskAction.FAILED,
            error=f"CSV export failed: {e}",
        )


async def run_final_database_upload(pipeline: "SyncPipeline") -> FinalDatabaseUploadResult:
    """Upload final database state as latest version to storage."""
    if pipeline.dry_run:
        logger.debug("Final database upload skipped in dry-run mode")
        return FinalDatabaseUploadResult(
            task_type=TaskType.FINAL_DATABASE_UPLOAD,
            action=TaskAction.SKIPPED,
            reason="skip_dry_run",
        )
    if not pipeline.uses_block_storage:
        logger.debug("Final database upload skipped for local storage")
        return FinalDatabaseUploadResult(
            task_type=TaskType.FINAL_DATABASE_UPLOAD,
            action=TaskAction.SKIPPED,
            reason="skip_not_applicable",
        )

    logger.info("Uploading database as latest version...")
    upload_result = await upload_database_to_storage(
        pipeline.db_path,
        pipeline.book_manager,
        pipeline.config.run_name,
        upload_type="latest",
    )

    if upload_result["status"] != "completed":
        return FinalDatabaseUploadResult(
            task_type=TaskType.FINAL_DATABASE_UPLOAD,
            action=TaskAction.FAILED,
            error=f"Final database upload failed: {upload_result['status']}",
        )

    data: FinalDatabaseUploadData = {
        "backup_filename": upload_result["backup_filename"],
        "file_size": upload_result["file_size"],
        "compressed_size": upload_result["compressed_size"],
        "backup_time": upload_result["backup_time"],
    }

    return FinalDatabaseUploadResult(
        task_type=TaskType.FINAL_DATABASE_UPLOAD,
        action=TaskAction.COMPLETED,
        data=data,
    )


async def run_staging_cleanup(pipeline: "SyncPipeline") -> StagingCleanupResult:
    """Clean up staging directory after batch processing completes."""
    start_time = time.time()

    if pipeline.dry_run:
        logger.debug("Staging cleanup skipped in dry-run mode")
        return StagingCleanupResult(
            task_type=TaskType.STAGING_CLEANUP,
            action=TaskAction.SKIPPED,
            reason="skip_dry_run",
        )

    if not pipeline.uses_block_storage:
        logger.debug("Staging cleanup skipped for local storage")
        return StagingCleanupResult(
            task_type=TaskType.STAGING_CLEANUP,
            action=TaskAction.SKIPPED,
            reason="skip_not_applicable",
        )

    if pipeline.skip_staging_cleanup:
        logger.debug("Staging cleanup skipped due to --skip-staging-cleanup flag")
        return StagingCleanupResult(
            task_type=TaskType.STAGING_CLEANUP,
            action=TaskAction.SKIPPED,
            reason="skip_staging_cleanup",
        )

    shutil.rmtree(pipeline.filesystem_manager.staging_path, ignore_errors=True)

    cleanup_time = time.time() - start_time

    data: StagingCleanupData = {
        "staging_path": pipeline.filesystem_manager.staging_path,
        "cleanup_time": cleanup_time,
    }

    return StagingCleanupResult(
        task_type=TaskType.STAGING_CLEANUP,
        action=TaskAction.COMPLETED,
        data=data,
    )


async def run_teardown_operations(
    pipeline: "SyncPipeline",
) -> dict[str, CsvExportTeardownResult | FinalDatabaseUploadResult | StagingCleanupResult]:
    """Run all teardown operations after batch processing completes.

    Args:
        pipeline: SyncPipeline instance with configuration

    Returns:
        Dict mapping operation names to their results
    """
    results: dict[str, CsvExportTeardownResult | FinalDatabaseUploadResult | StagingCleanupResult] = {}

    # Skip teardown operations in dry-run mode
    if pipeline.dry_run:
        logger.info("Teardown operations skipped in dry-run mode")
        return results

    # Export CSV with all processed books first
    csv_export_result = await run_csv_export(pipeline)
    results["csv_export"] = csv_export_result

    # Upload final database state
    final_upload_result = await run_final_database_upload(pipeline)
    results["final_database_upload"] = final_upload_result

    # Clean up staging directory
    staging_cleanup_result = await run_staging_cleanup(pipeline)
    results["staging_cleanup"] = staging_cleanup_result

    # Close database tracker
    await pipeline.db_tracker.close()

    # Close storage to clean up persistent S3 client
    await pipeline.storage.close()

    return results
