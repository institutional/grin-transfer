import csv
import logging
from datetime import datetime
from pathlib import Path
from typing import TYPE_CHECKING

from grin_to_s3.collect_books.models import BookRecord
from grin_to_s3.common import compress_file_to_temp

if TYPE_CHECKING:
    from grin_to_s3.sync.pipeline import SyncPipeline

from .task_types import ExportCsvData, Result, TaskAction, TaskType

logger = logging.getLogger(__name__)


async def main(pipeline: "SyncPipeline") -> Result[ExportCsvData]:
    """Export book metadata to CSV format."""
    base_path = pipeline.filesystem_manager.staging_path
    filename = "books_latest.csv"

    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    books = await pipeline.db_tracker.get_all_books_csv_data()

    csv_path = base_path / "meta" / filename
    timestamped_path = base_path / "meta" / "timestamped" / f"books_{timestamp}.csv"
    bucket_path = None

    csv_path.parent.mkdir(parents=True, exist_ok=True)
    timestamped_path.parent.mkdir(parents=True, exist_ok=True)

    record_count = 0

    with open(csv_path, "w", encoding="utf-8", newline="") as f:
        writer = csv.writer(f)
        writer.writerow(BookRecord.csv_headers())
        for book in books:
            record_count += 1
            writer.writerow(book.to_csv_row())

    if pipeline.uses_block_storage:
        bucket = pipeline.config.storage_config["config"].get("bucket_meta")
        run_name = pipeline.config.run_name

        if pipeline.config.sync_compression_meta_enabled:
            async with compress_file_to_temp(csv_path) as compressed_path:
                # Get compression statistics
                original_size = csv_path.stat().st_size
                compressed_size = compressed_path.stat().st_size
                compression_ratio = (1 - compressed_size / original_size) * 100 if original_size > 0 else 0

                logger.debug(
                    f"CSV compression: {original_size:,} -> {compressed_size:,} bytes ({compression_ratio:.1f}% reduction)"
                )

                file_extension = ".gz"
                source_path = str(compressed_path)

                # Upload to both locations
                bucket_path = f"{bucket}/{run_name}/{filename}{file_extension}"
                await pipeline.storage.write_file(bucket_path, source_path)
                await pipeline.storage.write_file(
                    f"{bucket}/{run_name}/timestamped/books_{timestamp}.csv{file_extension}", source_path
                )
                logger.info(f"Successfully uploaded latest CSV to {bucket_path}")
        else:
            file_extension = ""
            source_path = str(csv_path)

            # Upload uncompressed files
            bucket_path = f"{bucket}/{run_name}/{filename}{file_extension}"
            await pipeline.storage.write_file(bucket_path, source_path)
            await pipeline.storage.write_file(
                f"{bucket}/{run_name}/timestamped/books_{timestamp}.csv{file_extension}", source_path
            )
            logger.info(f"Successfully uploaded latest CSV to {bucket_path}")
    else:
        logger.info(f"CSV exported as {csv_path}")

    data: ExportCsvData = {
        "csv_file_path": Path(bucket_path) if bucket_path else csv_path,
        "record_count": record_count,
    }

    return Result(
        task_type=TaskType.EXPORT_CSV,
        action=TaskAction.COMPLETED,
        data=data,
    )
