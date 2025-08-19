import json
import logging
from datetime import datetime
from pathlib import Path
from typing import TYPE_CHECKING, cast

from grin_to_s3.common import (
    Barcode,
    compress_file_to_temp,
)
from grin_to_s3.extract.text_extraction import filesystem_page_generator
from grin_to_s3.sync.tasks.task_types import ArchiveOcrMetadata, ExtractOcrResult, TaskAction, TaskType, UnpackData

if TYPE_CHECKING:
    from grin_to_s3.sync.pipeline import SyncPipeline

logger = logging.getLogger(__name__)


async def main(barcode: Barcode, unpack_data: UnpackData, pipeline: "SyncPipeline") -> ExtractOcrResult:
    jsonl_filename = f"{barcode}_ocr.jsonl"
    jsonl_path = pipeline.filesystem_manager.staging_path / jsonl_filename
    page_count = await extract_ocr_pages(unpack_data, jsonl_path)
    bucket = pipeline.config.storage_config["config"].get("bucket_full")

    if bucket:
        jsonl_final_path = f"{bucket}/{jsonl_filename}.gz"
        async with compress_file_to_temp(jsonl_path) as compressed_path:
            metadata: ArchiveOcrMetadata = {
                "barcode": barcode,
                "acquisition_date": datetime.now().isoformat(),
                "page_count": str(page_count),
            }
            await pipeline.storage.write_file(jsonl_final_path, str(compressed_path), cast(dict[str, str], metadata))
    else:
        jsonl_final_path = jsonl_path

    return ExtractOcrResult(
        barcode=barcode,
        task_type=TaskType.EXTRACT_OCR,
        action=TaskAction.COMPLETED,
        data={"json_file_path": Path(jsonl_final_path), "page_count": page_count},
    )


async def extract_ocr_pages(unpack_data: UnpackData, jsonl_path: Path) -> int:
    with open(jsonl_path, "w", encoding="utf-8") as f:
        page_count = 0

        for _, content in filesystem_page_generator(unpack_data["unpacked_path"]):
            # Write the JSON-encoded content as a single line
            f.write(json.dumps(content, ensure_ascii=False) + "\n")
            page_count += 1
    return page_count
