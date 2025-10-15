import logging
from typing import TYPE_CHECKING

from grin_transfer.common import (
    Barcode,
    decrypt_gpg_file,
)
from grin_transfer.storage.staging import DirectoryManager
from grin_transfer.sync.tasks.task_types import DecryptData, DecryptResult, DownloadData, TaskAction, TaskType

if TYPE_CHECKING:
    from grin_transfer.sync.pipeline import SyncPipeline

logger = logging.getLogger(__name__)


async def main(barcode: Barcode, download_data: DownloadData, pipeline: "SyncPipeline") -> DecryptResult:
    data = await decrypt_book(barcode, download_data, pipeline.filesystem_manager, pipeline.secrets_dir)
    return DecryptResult(barcode=barcode, task_type=TaskType.DECRYPT, action=TaskAction.COMPLETED, data=data)


async def decrypt_book(
    barcode: Barcode,
    download: DownloadData,
    filesystem_manager: DirectoryManager,
    secrets_dir: str | None = None,
) -> DecryptData:
    encrypted_file = download["file_path"]
    decrypted_file = filesystem_manager.get_decrypted_file_path(barcode)
    decrypted_file.parent.mkdir(parents=True, exist_ok=True)

    # FIXME make these Paths
    await decrypt_gpg_file(str(encrypted_file), str(decrypted_file), secrets_dir)

    return {
        "decrypted_path": decrypted_file,
        "original_path": encrypted_file,
    }
