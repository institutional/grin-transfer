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
    """Per-barcode cleanup tasks"""

    if not pipeline.skip_staging_cleanup:
        paths_to_remove = pipeline.filesystem_manager.get_paths_for_cleanup(barcode)
        for path in paths_to_remove:
            if not path.exists():
                continue
            if path.is_file():
                path.unlink()
            elif path.is_dir():
                shutil.rmtree(path)

    logger.info(f"[{barcode}] ✅ Task pipeline completed for {barcode}")

    return CleanupResult(barcode=barcode, task_type=TaskType.CLEANUP, action=TaskAction.COMPLETED, data={})
