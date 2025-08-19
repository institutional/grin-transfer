import logging
import tarfile
from typing import TYPE_CHECKING

from grin_to_s3.common import (
    Barcode,
)
from grin_to_s3.storage.staging import DirectoryManager
from grin_to_s3.sync.tasks.task_types import DecryptData, TaskAction, TaskType, UnpackData, UnpackResult

if TYPE_CHECKING:
    from grin_to_s3.sync.pipeline import SyncPipeline

logger = logging.getLogger(__name__)


async def main(barcode: Barcode, decrypt_data: DecryptData, pipeline: "SyncPipeline") -> UnpackResult:
    data = await unpack_book(barcode, decrypt_data, pipeline.filesystem_manager)
    return UnpackResult(barcode=barcode, task_type=TaskType.UNPACK, action=TaskAction.COMPLETED, data=data)


async def unpack_book(
    barcode: Barcode,
    decrypted: DecryptData,
    filesystem_manager: DirectoryManager,
) -> UnpackData:
    decrypted_file = decrypted["decrypted_path"]

    extracted_path = filesystem_manager.get_extracted_directory_path(barcode)
    extracted_path.mkdir(parents=True, exist_ok=True)

    with tarfile.open(decrypted_file, "r:gz") as tar:
        tar.extractall(path=extracted_path)

    logger.info(f"[{barcode}] Archive unpacked to {extracted_path}")

    return {"unpacked_path": extracted_path}
