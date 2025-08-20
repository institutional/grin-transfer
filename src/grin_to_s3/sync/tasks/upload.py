import logging
from datetime import datetime
from pathlib import Path
from typing import TYPE_CHECKING

from grin_to_s3.common import Barcode
from grin_to_s3.storage.book_manager import BookManager
from grin_to_s3.sync.tasks.task_types import (
    ArchiveMetadata,
    DecryptData,
    DownloadData,
    TaskAction,
    TaskType,
    UploadData,
    UploadResult,
)

if TYPE_CHECKING:
    from grin_to_s3.sync.pipeline import SyncPipeline
from typing import cast

logger = logging.getLogger(__name__)


async def main(
    barcode: Barcode, download_data: DownloadData, decrypted_data: DecryptData, pipeline: "SyncPipeline"
) -> UploadResult:
    book_manager = BookManager(pipeline.storage, storage_config=pipeline.config.storage_config)

    if pipeline.uses_local_storage:
        # For local storage, the LocalDirectoryManager already places the decrypted file
        # in the final location, so no copy is needed
        assert decrypted_data["decrypted_path"] is not None
        final_path = Path(decrypted_data["decrypted_path"])
        logger.debug(f"[{barcode}] Local storage: decrypted archive already at final location {final_path}")
        data: UploadData = {"upload_path": final_path}
    else:
        data = await upload_book_from_filesystem(barcode, decrypted_data, download_data, book_manager)

    return UploadResult(
        barcode=barcode,
        task_type=TaskType.UPLOAD,
        action=TaskAction.COMPLETED,
        data=data,
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
    to_path = book_manager.raw_archive_path(barcode, f"{barcode}.tar.gz")

    logger.info(f"[{barcode}] Starting upload of decrypted archive to {to_path}.")

    await book_manager.storage.write_file(to_path, str(decrypted["decrypted_path"]), cast(dict[str, str], metadata))

    return {
        "upload_path": Path(to_path),
    }
