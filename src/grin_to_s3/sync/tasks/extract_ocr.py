import logging
import time
from datetime import datetime
from pathlib import Path
from typing import TYPE_CHECKING, cast

from grin_to_s3.common import (
    Barcode,
    compress_file_to_temp,
)
from grin_to_s3.extract.text_extraction import extract_ocr_pages
from grin_to_s3.sync.tasks.task_types import ArchiveOcrMetadata, ExtractOcrResult, TaskAction, TaskType, UnpackData

if TYPE_CHECKING:
    from grin_to_s3.sync.pipeline import SyncPipeline

logger = logging.getLogger(__name__)


async def upload_ocr_to_storage(
    source_path: Path, barcode: Barcode, pipeline: "SyncPipeline", metadata: ArchiveOcrMetadata
) -> str:
    """Store OCR file to the appropriate location based on pipeline configuration."""
    destination_path = pipeline.book_manager.full_text_path(f"{barcode}_ocr.jsonl")
    if source_path.name.endswith(".gz"):
        destination_path = f"{destination_path}.gz"

    logger.info(f"Uploading OCR file to {destination_path}")
    await pipeline.storage.write_file(destination_path, str(source_path), cast(dict[str, str], metadata))

    return destination_path


async def main(barcode: Barcode, unpack_data: UnpackData, pipeline: "SyncPipeline") -> ExtractOcrResult:
    start_time = time.time()
    manager_id = getattr(pipeline, "book_manager", {})
    manager_id = getattr(manager_id, "_manager_id", "unknown")

    logger.debug(f"EXTRACT_OCR task started for {barcode} (manager_id={manager_id})")

    jsonl_filename = f"{barcode}_ocr.jsonl"
    jsonl_path = pipeline.filesystem_manager.staging_path / jsonl_filename

    page_count = await extract_ocr_pages(unpack_data, jsonl_path)

    metadata: ArchiveOcrMetadata = {
        "barcode": barcode,
        "acquisition_date": datetime.now().isoformat(),
        "page_count": str(page_count),
    }

    if pipeline.config.sync_compression_full_enabled:
        async with compress_file_to_temp(jsonl_path) as compressed_path:
            jsonl_final_path = await upload_ocr_to_storage(compressed_path, barcode, pipeline, metadata)
            # For compression with local storage, clean up original file
            if not pipeline.uses_block_storage:
                jsonl_path.unlink()
    else:
        jsonl_final_path = await upload_ocr_to_storage(jsonl_path, barcode, pipeline, metadata)
        # For no compression with block storage, clean up original file
        if pipeline.uses_block_storage:
            jsonl_path.unlink()

    duration = time.time() - start_time
    logger.debug(f"EXTRACT_OCR task completed for {barcode} in {duration:.3f}s (manager_id={manager_id})")

    return ExtractOcrResult(
        barcode=barcode,
        task_type=TaskType.EXTRACT_OCR,
        action=TaskAction.COMPLETED,
        data={"json_file_path": Path(jsonl_final_path), "page_count": page_count},
    )
