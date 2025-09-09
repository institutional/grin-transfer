import logging
from typing import TYPE_CHECKING

from grin_to_s3.constants import BOOKS_EXPORT_CSV_FILENAME
from grin_to_s3.export import upload_csv_to_storage, write_books_to_csv

if TYPE_CHECKING:
    from grin_to_s3.sync.pipeline import SyncPipeline

from .task_types import ExportCsvData, Result, TaskAction, TaskType

logger = logging.getLogger(__name__)


async def main(pipeline: "SyncPipeline") -> Result[ExportCsvData]:
    """Export book metadata to CSV format."""
    # First generate the CSV file in the run directory
    csv_path, record_count = await write_books_to_csv(
        pipeline.db_tracker, pipeline.config.output_directory / BOOKS_EXPORT_CSV_FILENAME
    )

    # Then upload it to storage
    bucket_path = await upload_csv_to_storage(
        csv_path=csv_path,
        book_manager=pipeline.book_manager,
        compression_enabled=pipeline.config.sync_compression_meta_enabled,
    )

    data: ExportCsvData = {
        "csv_file_path": bucket_path,
        "local_csv_path": csv_path,  # Include local path in result data
        "record_count": record_count,
    }

    return Result(
        task_type=TaskType.EXPORT_CSV,
        action=TaskAction.COMPLETED,
        data=data,
    )
