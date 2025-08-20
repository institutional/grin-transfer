import logging
import shutil
from typing import TYPE_CHECKING, Any

from grin_to_s3.common import (
    Barcode,
)
from grin_to_s3.sync.tasks.task_types import CleanupResult, TaskAction, TaskResult, TaskType

if TYPE_CHECKING:
    from grin_to_s3.sync.pipeline import SyncPipeline

logger = logging.getLogger(__name__)


async def main(
    barcode: Barcode,
    pipeline: "SyncPipeline",
    all_results: dict[TaskType, TaskResult[Any]],
) -> CleanupResult:
    """Per-barcode cleanup tasks

    Removes staging data for a barcode to keep disk space available:
    - Always removes extracted directory (temporary data)
    - For cloud storage, removes downloaded/decrypted files when cleanup is enabled
    """

    # Always clean up extracted directory
    extracted_dir = pipeline.filesystem_manager.get_extracted_directory_path(barcode)
    if extracted_dir.exists():
        shutil.rmtree(extracted_dir)

    # Clean up download/decrypt files for cloud storage
    if pipeline.uses_block_storage and not pipeline.skip_staging_cleanup:
        from grin_to_s3.storage.staging import StagingDirectoryManager
        if isinstance(pipeline.filesystem_manager, StagingDirectoryManager):
            pipeline.filesystem_manager.cleanup_files(barcode)

    logger.debug(f"[{barcode}] Per-barcode cleanup completed")

    return CleanupResult(barcode=barcode, task_type=TaskType.CLEANUP, action=TaskAction.COMPLETED, data={})
