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


async def main(barcode: Barcode, unpack_data: UnpackData, pipeline: "SyncPipeline") -> ExtractOcrResult:
    jsonl_filename = f"{barcode}_ocr.jsonl"
    jsonl_path = pipeline.filesystem_manager.staging_path / jsonl_filename

    page_count = await extract_ocr_pages(unpack_data, jsonl_path)

    compression_enabled = pipeline.config.sync_compression_full_enabled
    metadata: ArchiveOcrMetadata = {
        "barcode": barcode,
        "acquisition_date": datetime.now().isoformat(),
        "page_count": str(page_count),
    }

    if compression_enabled:
        async with compress_file_to_temp(jsonl_path) as compressed_path:
            if pipeline.uses_block_storage:
                bucket = pipeline.config.storage_config["config"].get("bucket_full")
                jsonl_final_path = f"{bucket}/{jsonl_filename}.gz"
                await pipeline.storage.write_file(jsonl_final_path, str(compressed_path), cast(dict[str, str], metadata))
            else:
                # Local storage: move to 'full' subdirectory
                base_path = Path(pipeline.config.storage_config["config"].get("base_path", ""))
                jsonl_final_path = base_path / "full" / barcode / f"{jsonl_filename}.gz"
                jsonl_final_path.parent.mkdir(parents=True, exist_ok=True)
                shutil.move(compressed_path, jsonl_final_path)
                jsonl_path.unlink()
    else:
        # No compression: upload/move the original file
        if pipeline.uses_block_storage:
            bucket = pipeline.config.storage_config["config"].get("bucket_full")
            jsonl_final_path = f"{bucket}/{jsonl_filename}"
            await pipeline.storage.write_file(jsonl_final_path, str(jsonl_path), cast(dict[str, str], metadata))
            jsonl_path.unlink()
        else:
            # Local storage: move to 'full' subdirectory
            base_path = Path(pipeline.config.storage_config["config"].get("base_path", ""))
            jsonl_final_path = base_path / "full" / barcode / jsonl_filename
            jsonl_final_path.parent.mkdir(parents=True, exist_ok=True)
            shutil.move(jsonl_path, jsonl_final_path)

    return ExtractOcrResult(
        barcode=barcode,
        task_type=TaskType.EXTRACT_OCR,
        action=TaskAction.COMPLETED,
        data={"json_file_path": Path(jsonl_final_path), "page_count": page_count},
    )
