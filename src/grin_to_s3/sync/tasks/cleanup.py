import logging
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
    """Per-barcode cleanup task.

    This task handles any per-barcode cleanup operations.
    Batch-level operations (staging cleanup, database operations) have been
    moved to the teardown module.
    """
    logger.debug(f"[{barcode}] Per-barcode cleanup completed")

    return CleanupResult(barcode=barcode, task_type=TaskType.CLEANUP, action=TaskAction.COMPLETED, data={})
