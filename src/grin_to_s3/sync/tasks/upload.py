import logging
import shutil
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
        print("LOCAL")
        data = copy_file_to_base_path(barcode, decrypted_data, book_manager)
    else:
        data = await upload_book_from_filesystem(barcode, decrypted_data, download_data, book_manager)

    return UploadResult(
        barcode=barcode,
        task_type=TaskType.UPLOAD,
        action=TaskAction.COMPLETED,
        data=data,
    )


def copy_file_to_base_path(
    barcode: Barcode,
    decrypted: DecryptData,
    book_manager: BookManager,
) -> UploadData:
    """Copy a file to the correct local storage output directory."""
    assert decrypted["decrypted_path"] is not None
    to_path = Path(book_manager.raw_archive_path(barcode, f"{barcode}.tar.gz"))
    to_path.parent.mkdir(parents=True, exist_ok=True)

    shutil.copy2(decrypted["decrypted_path"], to_path)
    logger.info(f"[{barcode}] Copying decrypted archive to final {to_path}..")

    return {"upload_path": Path(to_path)}


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

    logger.info(f"[{barcode}] Uploading decrypted archive with encrypted ETag metadata...")

    await book_manager.storage.write_file(to_path, str(decrypted["decrypted_path"]), cast(dict[str, str], metadata))

    return {
        "upload_path": Path(to_path),
    }
