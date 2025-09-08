import logging
from pathlib import Path
from typing import TYPE_CHECKING

import aiofiles
import aiohttp
from tenacity import before_sleep_log, retry, stop_after_attempt, wait_exponential

from grin_to_s3.client import GRINClient
from grin_to_s3.common import Barcode
from grin_to_s3.storage.staging import DirectoryManager
from grin_to_s3.sync.tasks.task_types import DownloadData, DownloadResult, TaskAction, TaskType

if TYPE_CHECKING:
    from grin_to_s3.sync.pipeline import SyncPipeline


logger = logging.getLogger(__name__)

# Retry configuration for DOWNLOAD operations
DOWNLOAD_MAX_RETRIES = 5
DOWNLOAD_BACKOFF_MIN = 1
DOWNLOAD_BACKOFF_MAX = 600  # Cap at 10 minutes
DOWNLOAD_BACKOFF_MULTIPLIER = 2


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
    )

    return DownloadResult(
        barcode=barcode,
        task_type=TaskType.DOWNLOAD,
        action=TaskAction.COMPLETED if data["file_size_bytes"] and data["file_size_bytes"] > 0 else TaskAction.FAILED,
        data=data,
    )


# Retry downloads with exponential backoff to handle GRIN rate limiting
# Retry schedule: immediate, 1s, 2s, 4s, 8s, 16s, 32s, 64s, 128s, 256s, 512s, 600s, 600s (total ~37min across 13 attempts)
# Note: 404 errors are excluded from retry as they indicate missing files
@retry(
    stop=stop_after_attempt(DOWNLOAD_MAX_RETRIES + 1),
    retry=lambda retry_state: bool(
        retry_state.outcome
        and retry_state.outcome.failed
        and not (
            isinstance(retry_state.outcome.exception(), aiohttp.ClientResponseError)
            and getattr(retry_state.outcome.exception(), "status", None) == 404
        )
    ),
    wait=wait_exponential(multiplier=DOWNLOAD_BACKOFF_MULTIPLIER, min=DOWNLOAD_BACKOFF_MIN, max=DOWNLOAD_BACKOFF_MAX),
    before_sleep=before_sleep_log(logger, logging.WARNING),
    reraise=True,
)
async def download_book_to_filesystem(
    barcode: str,
    to_path: Path,
    grin_client: GRINClient,
    library_directory: str,
    filesystem_manager: DirectoryManager,
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
