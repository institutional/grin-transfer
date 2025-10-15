import logging
import time
from datetime import datetime
from pathlib import Path
from typing import TYPE_CHECKING

from tenacity import before_sleep_log, retry, stop_after_attempt, wait_fixed

from grin_transfer.common import Barcode
from grin_transfer.storage.book_manager import BookManager
from grin_transfer.sync.tasks.task_types import (
    ArchiveMetadata,
    DecryptData,
    DownloadData,
    TaskAction,
    TaskType,
    UploadData,
    UploadResult,
)

if TYPE_CHECKING:
    from grin_transfer.sync.pipeline import SyncPipeline
from typing import cast

logger = logging.getLogger(__name__)


async def main(
    barcode: Barcode, download_data: DownloadData, decrypted_data: DecryptData, pipeline: "SyncPipeline"
) -> UploadResult:
    start_time = time.time()
    manager_id = getattr(pipeline.book_manager, "_manager_id", "unknown")

    logger.debug(f"UPLOAD task started for {barcode} (manager_id={manager_id})")

    if pipeline.uses_local_storage:
        # For local storage, the LocalDirectoryManager already places the decrypted file
        # in the final location, so no copy is needed
        assert decrypted_data["decrypted_path"] is not None
        final_path = Path(decrypted_data["decrypted_path"])
        logger.debug(f"[{barcode}] Local storage: decrypted archive already at final location {final_path}")
        data: UploadData = {"upload_path": final_path}
    else:
        data = await upload_book_from_filesystem(barcode, decrypted_data, download_data, pipeline.book_manager)

    duration = time.time() - start_time
    logger.debug(f"UPLOAD task completed for {barcode} in {duration:.3f}s (manager_id={manager_id})")

    return UploadResult(
        barcode=barcode,
        task_type=TaskType.UPLOAD,
        action=TaskAction.COMPLETED,
        data=data,
    )


@retry(
    stop=stop_after_attempt(4),  # 4 total attempts (3 retries)
    wait=wait_fixed(2),  # 2 second fixed delay between retries
    before_sleep=before_sleep_log(logger, logging.WARNING),
    reraise=True,
)
async def upload_book_from_filesystem(
    barcode: Barcode,
    decrypted: DecryptData,
    downloaded: DownloadData,
    book_manager: BookManager,
) -> UploadData:
    """Upload book from staging directory to storage."""

    assert downloaded["etag"] is not None
    assert decrypted["decrypted_path"] is not None

    metadata: ArchiveMetadata = {
        "barcode": barcode,
        "acquisition_date": datetime.now().isoformat(),
        "encrypted_etag": downloaded["etag"],
        "original_filename": decrypted["original_path"].name,
    }

    # Generate a path to deposit the file
    to_path = book_manager.raw_archive_path(f"{barcode}.tar.gz")

    logger.info(f"[{barcode}] Uploading raw archive to {to_path}.")

    await book_manager.storage.write_file(to_path, str(decrypted["decrypted_path"]), cast(dict[str, str], metadata))

    return {
        "upload_path": Path(to_path),
    }
