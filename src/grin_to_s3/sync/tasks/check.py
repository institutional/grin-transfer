import logging
from typing import TYPE_CHECKING

import aiohttp
from botocore.exceptions import ClientError
from tenacity import before_sleep_log, retry, stop_after_attempt, wait_exponential

from grin_to_s3.client import GRINClient
from grin_to_s3.common import Barcode

if TYPE_CHECKING:
    from grin_to_s3.sync.pipeline import SyncPipeline

from .task_types import CheckData, CheckResult, TaskAction, TaskType

logger = logging.getLogger(__name__)

# Retry configuration for CHECK operations
CHECK_MAX_RETRIES = 5
CHECK_BACKOFF_MIN = 1
CHECK_BACKOFF_MAX = 300  # Cap at 5 minutes
CHECK_BACKOFF_MULTIPLIER = 2

# Timeout configuration for CHECK operations
CHECK_TIMEOUT_SECONDS = 30  # 30 seconds for HEAD requests


async def main(barcode: Barcode, pipeline: "SyncPipeline") -> CheckResult:
    # Always check storage first, then GRIN
    try:
        storage_metadata = await pipeline.book_manager.get_decrypted_archive_metadata(barcode, pipeline.db_tracker)
        stored_etag = storage_metadata.get("encrypted_etag") if storage_metadata else None
    except ClientError as e:
        # Handle storage 404s gracefully - book simply doesn't exist in storage yet
        if e.response.get("Error", {}).get("Code") == "404":
            logger.debug(f"[{barcode}] Book not found in storage, proceeding to check GRIN")
            stored_etag = None
        else:
            # Re-raise other S3 errors
            raise

    # Single GRIN check
    try:
        grin_data = await grin_head_request(barcode, pipeline.grin_client, pipeline.library_directory)
        assert grin_data["etag"] is not None
        return _handle_grin_available(barcode, stored_etag, grin_data["etag"], grin_data, pipeline)
    except aiohttp.ClientResponseError as e:
        logger.info(f"[{barcode}] HEAD request returned HTTP {e.status}")
        return _handle_grin_error(barcode, stored_etag, e, pipeline)


# Retry check requests with exponential backoff to handle GRIN rate limiting
# Retry schedule: immediate, 1s, 2s, 4s, 8s, 16s, 32s, 64s, 128s, 256s, 512s, 600s, 600s (total ~37min across 13 attempts)
# Note: 404 and 429 errors are excluded from retry as they indicate missing files or queue full
@retry(
    stop=stop_after_attempt(CHECK_MAX_RETRIES + 1),
    retry=lambda retry_state: bool(
        retry_state.outcome
        and retry_state.outcome.failed
        and not (
            isinstance(retry_state.outcome.exception(), aiohttp.ClientResponseError)
            and getattr(retry_state.outcome.exception(), "status", None) in [404, 429]
        )
    ),
    wait=wait_exponential(multiplier=CHECK_BACKOFF_MULTIPLIER, min=CHECK_BACKOFF_MIN, max=CHECK_BACKOFF_MAX),
    before_sleep=before_sleep_log(logger, logging.WARNING),
    reraise=True,
)
async def grin_head_request(barcode: Barcode, grin_client: GRINClient, library_directory: str) -> CheckData:
    """Make HEAD request to get encrypted file's ETag and file size before downloading."""
    result: CheckData = {"etag": None, "file_size_bytes": None, "http_status_code": None}
    grin_url = f"https://books.google.com/libraries/{library_directory}/{barcode}.tar.gz.gpg"
    logger.debug(f"[{barcode}] Checking encrypted ETag via HEAD request to {grin_url}")

    # Make HEAD request to get headers without downloading content
    head_response = await grin_client.head_archive(grin_url, timeout=CHECK_TIMEOUT_SECONDS)

    result["http_status_code"] = head_response.status

    # Look for ETag and Content-Length headers
    etag = head_response.headers.get("ETag", "").strip('"')
    content_length = head_response.headers.get("Content-Length", "")

    file_size = int(content_length) if content_length else None

    result["etag"] = etag
    result["file_size_bytes"] = file_size
    return result


def _handle_grin_available(
    barcode: Barcode, stored_etag: str | None, grin_etag: str, grin_data: CheckData, pipeline: "SyncPipeline"
) -> CheckResult:
    """Handle case where GRIN has the book available."""
    if not stored_etag:
        # We don't have the book in storage, GRIN has book → download
        return CheckResult(
            barcode=barcode,
            task_type=TaskType.CHECK,
            action=TaskAction.COMPLETED,
            data=grin_data,
        )

    # Book exists in storage, check etag
    if stored_etag.strip('"') == grin_etag.strip('"'):
        # Etags match → skip unless force flag
        if pipeline.force:
            return CheckResult(
                barcode=barcode,
                task_type=TaskType.CHECK,
                action=TaskAction.COMPLETED,
                data=grin_data,
                reason="completed_match_with_force",
            )
        return CheckResult(
            barcode=barcode,
            task_type=TaskType.CHECK,
            action=TaskAction.SKIPPED,
            data=grin_data,
            reason="skip_etag_match",
        )

    # Etags differ → re-download needed
    return CheckResult(
        barcode=barcode,
        task_type=TaskType.CHECK,
        action=TaskAction.COMPLETED,
        data=grin_data,
    )


def _handle_grin_error(
    barcode: Barcode, stored_etag: str | None, error: aiohttp.ClientResponseError, pipeline: "SyncPipeline"
) -> CheckResult:
    """Handle GRIN errors (404, other HTTP errors)."""
    if error.status == 404:
        # GRIN doesn't have the book
        if stored_etag:
            # Book exists in storage but not in GRIN download queue → skip and mark as completed in DB
            return CheckResult(
                barcode=barcode,
                task_type=TaskType.CHECK,
                action=TaskAction.SKIPPED,
                data={"etag": None, "file_size_bytes": None, "http_status_code": 404},
                reason="skip_found_in_storage_not_grin",
            )
        # Book not in storage, not in GRIN download queue → request conversion
        return CheckResult(
            barcode=barcode,
            task_type=TaskType.CHECK,
            action=TaskAction.FAILED,
            error="Archive not found in GRIN",
            data={"etag": None, "file_size_bytes": None, "http_status_code": 404},
            reason="fail_archive_missing",
        )

    # Other HTTP errors
    return CheckResult(
        barcode=barcode,
        task_type=TaskType.CHECK,
        action=TaskAction.FAILED,
        error=f"HTTP {error.status}: {error.message}",
        data={"etag": None, "file_size_bytes": None, "http_status_code": error.status},
        reason="fail_unexpected_http_status_code",
    )
