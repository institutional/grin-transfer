import logging
from typing import TYPE_CHECKING

from grin_to_s3.common import (
    Barcode,
)
from grin_to_s3.metadata.marc_extraction import convert_marc_keys_to_db_fields, extract_marc_metadata
from grin_to_s3.sync.tasks.task_types import ExtractMarcData, ExtractMarcResult, TaskAction, TaskType, UnpackData

if TYPE_CHECKING:
    from grin_to_s3.sync.pipeline import SyncPipeline

logger = logging.getLogger(__name__)


async def main(barcode: Barcode, unpack_data: UnpackData, pipeline: "SyncPipeline") -> ExtractMarcResult:
    data = await extract_marc(barcode, unpack_data)
    normalized_metadata = convert_marc_keys_to_db_fields(data["marc_metadata"])

    if not data["marc_metadata"]:
        logger.warning(f"No MARC metadata found for {barcode}")
        return ExtractMarcResult(
            barcode=barcode,
            task_type=TaskType.EXTRACT_MARC,
            action=TaskAction.FAILED,
            data=data,
            reason="fail_no_marc_metadata",
        )

    await pipeline.db_tracker.update_book_marc_metadata(barcode, normalized_metadata)

    return ExtractMarcResult(barcode=barcode, task_type=TaskType.EXTRACT_MARC, action=TaskAction.COMPLETED, data=data)


async def extract_marc(barcode: Barcode, unpack_data: UnpackData) -> ExtractMarcData:
    return {"marc_metadata": extract_marc_metadata(unpack_data["unpacked_path"])}
