import logging
from pathlib import Path
from typing import TYPE_CHECKING

import aiofiles
import aiohttp
from tenacity import retry, stop_after_attempt, wait_fixed

from grin_to_s3.client import GRINClient
from grin_to_s3.common import (
    DEFAULT_DOWNLOAD_RETRIES,
    DEFAULT_DOWNLOAD_TIMEOUT,
    DEFAULT_RETRY_WAIT_SECONDS,
    Barcode,
)
from grin_to_s3.storage.staging import DirectoryManager
from grin_to_s3.sync.tasks.task_types import DownloadData, DownloadResult, TaskAction, TaskType

if TYPE_CHECKING:
    from grin_to_s3.sync.pipeline import SyncPipeline


logger = logging.getLogger(__name__)


async def main(barcode: Barcode, pipeline: "SyncPipeline") -> DownloadResult:
    to_path = pipeline.filesystem_manager.get_encrypted_file_path(barcode)

    # Ensure parent directories exist
    to_path.parent.mkdir(parents=True, exist_ok=True)

    data = await download_book_to_filesystem(
        barcode,
        to_path,
        pipeline.grin_client,
        pipeline.library_directory,
        pipeline.filesystem_manager,
        pipeline.download_timeout,
        pipeline.download_retries,
    )

    return DownloadResult(
        barcode=barcode,
        task_type=TaskType.DOWNLOAD,
        action=TaskAction.COMPLETED if data["file_size_bytes"] and data["file_size_bytes"] > 0 else TaskAction.FAILED,
        data=data,
    )


@retry(
    stop=stop_after_attempt(DEFAULT_DOWNLOAD_RETRIES + 1),
    retry=lambda retry_state: bool(
        retry_state.outcome
        and retry_state.outcome.failed
        and not (
            isinstance(retry_state.outcome.exception(), aiohttp.ClientResponseError)
            and getattr(retry_state.outcome.exception(), "status", None) == 404
        )
    ),
    wait=wait_fixed(DEFAULT_RETRY_WAIT_SECONDS),
    reraise=True,
)
async def download_book_to_filesystem(
    barcode: str,
    to_path: Path,
    grin_client: GRINClient,
    library_directory: str,
    filesystem_manager: DirectoryManager,
    download_timeout: int = DEFAULT_DOWNLOAD_TIMEOUT,
    download_retries: int = DEFAULT_DOWNLOAD_RETRIES,
) -> DownloadData:
    """Download book archive with retry logic, excluding 404 errors from retries."""
    grin_url = f"https://books.google.com/libraries/{library_directory}/{barcode}.tar.gz.gpg"

    logger.info(f"[{barcode}] Starting download from {grin_url}")

    response = await grin_client.download_archive(grin_url)

    total_bytes = 0
    last_check_bytes = 0
    check_interval = 50 * 1024 * 1024  # 50MB
    async with aiofiles.open(to_path, "wb") as f:
        async for chunk in response.content.iter_chunked(1024 * 1024):
            await f.write(chunk)
            total_bytes += len(chunk)

            # Check disk space when we cross a 50MB boundary since last check
            if total_bytes - last_check_bytes >= check_interval:
                if not filesystem_manager.check_disk_space():
                    # Clean up partial file and wait for space
                    to_path.unlink(missing_ok=True)
                    logger.warning(
                        f"[{barcode}] Disk space exhausted during download, cleaning up and waiting for space..."
                    )
                    await filesystem_manager.wait_for_disk_space(check_interval=60)
                    # Retry the download with the same parameters
                    return await download_book_to_filesystem(
                        barcode,
                        to_path,
                        grin_client,
                        library_directory,
                        filesystem_manager,
                        download_timeout,
                        download_retries,
                    )

                # Update the last check point
                last_check_bytes = total_bytes

        # Ensure all data is flushed to disk
        await f.flush()

        # Verify file size matches what we downloaded
        actual_size = to_path.stat().st_size
        if actual_size != total_bytes:
            to_path.unlink(missing_ok=True)
            raise Exception(f"File size mismatch: expected {total_bytes}, got {actual_size}")

        # Validate that ETag header is present
        etag = response.headers.get("ETag")
        if not etag:
            to_path.unlink(missing_ok=True)
            raise Exception("Missing ETag header in download response - unable to validate data integrity")

        return {
            "file_path": to_path,
            "http_status_code": response.status,
            "etag": etag.strip('"'),
            "file_size_bytes": actual_size,
        }
