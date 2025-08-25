import logging
import shutil
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


async def _store_ocr_file(
    source_path: Path, barcode: Barcode, filename: str, pipeline: "SyncPipeline", metadata: ArchiveOcrMetadata
) -> str:
    """Store OCR file to the appropriate location based on pipeline configuration."""
    if pipeline.uses_block_storage:
        bucket = pipeline.config.storage_config["config"].get("bucket_full")
        final_path = f"{bucket}/{filename}"
        await pipeline.storage.write_file(final_path, str(source_path), cast(dict[str, str], metadata))
    else:
        # Local storage: move to 'full' subdirectory
        base_path = Path(pipeline.config.storage_config["config"].get("base_path", ""))
        final_path = base_path / "full" / barcode / filename
        final_path.parent.mkdir(parents=True, exist_ok=True)
        shutil.move(source_path, final_path)
        final_path = str(final_path)

    return final_path


async def main(barcode: Barcode, unpack_data: UnpackData, pipeline: "SyncPipeline") -> ExtractOcrResult:
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
            jsonl_final_path = await _store_ocr_file(
                compressed_path, barcode, f"{jsonl_filename}.gz", pipeline, metadata
            )
            # For compression with local storage, clean up original file
            if not pipeline.uses_block_storage:
                jsonl_path.unlink()
    else:
        jsonl_final_path = await _store_ocr_file(
            jsonl_path, barcode, jsonl_filename, pipeline, metadata
        )
        # For no compression with block storage, clean up original file
        if pipeline.uses_block_storage:
            jsonl_path.unlink()

    return ExtractOcrResult(
        barcode=barcode,
        task_type=TaskType.EXTRACT_OCR,
        action=TaskAction.COMPLETED,
        data={"json_file_path": Path(jsonl_final_path), "page_count": page_count},
    )
