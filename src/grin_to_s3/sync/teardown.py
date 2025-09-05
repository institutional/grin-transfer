#!/usr/bin/env python3
"""
Teardown Operations for Sync Pipeline

Handles batch-level operations that run after book processing completes,
including final database upload and staging cleanup.
"""

import logging
from typing import TYPE_CHECKING

from grin_to_s3.storage.staging import run_staging_cleanup

if TYPE_CHECKING:
    from grin_to_s3.sync.pipeline import SyncPipeline

from ..database.database_backup import upload_database_to_storage
from .tasks import export_csv
from .tasks.task_types import (
    DatabaseBackupData,
    ExportCsvData,
    Result,
    StagingCleanupData,
    TaskAction,
    TaskType,
)

logger = logging.getLogger(__name__)


async def run_teardown_operations(
    pipeline: "SyncPipeline",
) -> dict[str, Result[ExportCsvData] | Result[DatabaseBackupData] | Result[StagingCleanupData]]:
    """Run all teardown operations after batch processing completes.

    Args:
        pipeline: SyncPipeline instance with configuration

    Returns:
        Dict mapping operation names to their results
    """
    results: dict[str, Result[ExportCsvData] | Result[DatabaseBackupData] | Result[StagingCleanupData]] = {}

    # Skip teardown operations in dry-run mode
    if pipeline.dry_run:
        logger.info("Teardown operations skipped in dry-run mode")
        return results

    # Export CSV with all processed books first
    csv_export_result: Result[ExportCsvData]
    if pipeline.skip_csv_export:
        logger.debug("CSV export skipped due to --skip-csv-export flag")
        csv_export_result = Result(
            task_type=TaskType.EXPORT_CSV,
            action=TaskAction.SKIPPED,
            reason="skip_csv_export",
        )
    else:
        try:
            csv_export_result = await export_csv.main(pipeline)
        except Exception as e:
            logger.error(f"CSV export failed: {e}", exc_info=True)
            csv_export_result = Result(
                task_type=TaskType.EXPORT_CSV,
                action=TaskAction.FAILED,
                error=f"CSV export failed: {e}",
            )
    results["csv_export"] = csv_export_result

    # Upload final database state
    logger.info("Uploading database as latest version...")
    final_upload_result = await upload_database_to_storage(
        pipeline.db_path,
        pipeline.book_manager,
    )
    results["final_database_upload"] = final_upload_result

    # Clean up staging directory
    staging_cleanup_result = await run_staging_cleanup(pipeline)
    results["staging_cleanup"] = staging_cleanup_result

    # Close database tracker
    await pipeline.db_tracker.close()

    # Close storage to clean up persistent S3 client
    await pipeline.storage.close()

    return results
