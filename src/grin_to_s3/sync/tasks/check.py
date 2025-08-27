import logging
from typing import TYPE_CHECKING, cast

import aiohttp
from botocore.exceptions import ClientError
from tenacity import before_sleep_log, retry, stop_after_attempt, wait_exponential

from grin_to_s3.client import GRINClient
from grin_to_s3.collect_books.models import SQLiteProgressTracker
from grin_to_s3.common import DEFAULT_DOWNLOAD_RETRIES, Barcode
from grin_to_s3.storage.book_manager import BookManager

if TYPE_CHECKING:
    from grin_to_s3.sync.pipeline import SyncPipeline

from .task_types import ArchiveMetadata, CheckData, CheckResult, ETagMatchResult, TaskAction, TaskType

logger = logging.getLogger(__name__)


async def main(barcode: Barcode, pipeline: "SyncPipeline") -> CheckResult:
    data: CheckData

    try:
        data = await grin_head_request(barcode, pipeline.grin_client, pipeline.library_directory)
    except aiohttp.ClientResponseError as e:
        logger.info(f"[{barcode}] HEAD request returned HTTP {e.status}")
        # If the HEAD response was a 404, this is always a failure
        if e.status == 404:
            logger.info(f"[{barcode}] Archive not found (404)")
            return CheckResult(
                barcode=barcode,
                task_type=TaskType.CHECK,
                action=TaskAction.FAILED,
                data={"etag": None, "file_size_bytes": None, "http_status_code": 404},
                reason="fail_archive_missing",
            )
        # Other failures are not expected and should be treated as FAILED
        return CheckResult(
            barcode=barcode,
            task_type=TaskType.CHECK,
            action=TaskAction.FAILED,
            data={"etag": None, "file_size_bytes": None, "http_status_code": e.status},
            reason="fail_unexpected_http_status_code",
        )

    assert data["etag"] is not None

    # Now check for our own etag
    etag_match = await etag_matches(barcode, data["etag"], pipeline.book_manager, pipeline.db_tracker)
    if etag_match["matched"]:
        if pipeline.force:
            return CheckResult(
                barcode=barcode,
                task_type=TaskType.CHECK,
                action=TaskAction.COMPLETED,
                data=data,
                reason="completed_match_with_force",
            )
        return CheckResult(
            barcode=barcode,
            task_type=TaskType.CHECK,
            action=TaskAction.SKIPPED,
            data=data,
            reason="skip_etag_match",
        )
    # We're going to tell the pipeline to try to get the archive
    return CheckResult(
        barcode=barcode,
        task_type=TaskType.CHECK,
        action=TaskAction.COMPLETED,
        data=data,
    )


# Retry downloads with exponential backoff to handle GRIN rate limiting
# Retry schedule: immediate, 4s, 8s (total ~12s across 3 attempts)
# Note: 404 errors are excluded from retry as they indicate missing files
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
    wait=wait_exponential(multiplier=2, min=4, max=120),
    before_sleep=before_sleep_log(logger, logging.WARNING),
    reraise=True,
)
async def grin_head_request(barcode: Barcode, grin_client: GRINClient, library_directory: str) -> CheckData:
    """Make HEAD request to get encrypted file's ETag and file size before downloading."""
    result: CheckData = {"etag": None, "file_size_bytes": None, "http_status_code": None}
    grin_url = f"https://books.google.com/libraries/{library_directory}/{barcode}.tar.gz.gpg"
    logger.debug(f"[{barcode}] Checking encrypted ETag via HEAD request to {grin_url}")

    # Make HEAD request to get headers without downloading content
    head_response = await grin_client.head_archive(grin_url)

    result["http_status_code"] = head_response.status

    # Look for ETag and Content-Length headers
    etag = head_response.headers.get("ETag", "").strip('"')
    content_length = head_response.headers.get("Content-Length", "")

    file_size = int(content_length) if content_length else None

    result["etag"] = etag
    result["file_size_bytes"] = file_size
    return result


async def etag_matches(
    barcode: Barcode, etag: str, book_manager: BookManager, db_tracker: SQLiteProgressTracker
) -> ETagMatchResult:
    # Check if decrypted archive exists and matches encrypted ETag
    try:
        metadata = cast(ArchiveMetadata, await book_manager.get_decrypted_archive_metadata(barcode, db_tracker))

        stored_encrypted_etag = metadata.get("encrypted_etag")
        if not stored_encrypted_etag:
            logger.debug(f"[{barcode}] No stored etag found; will download")
            return {"matched": False, "reason": "no_etag"}
        matches = stored_encrypted_etag.strip('"') == etag.strip('"')
        if matches:
            return {
                "matched": True,
                "reason": "etag_match",
            }
        else:
            logger.info(f"[{barcode}] Stored etag did not match GRIN etag; will re-download")
            return {"matched": False, "reason": "etag_mismatch"}
    except ClientError as e:
        logger.debug(f"[{barcode}] Don't have this title in block storage")
        if e.response and e.response.get("Error", {}).get("Code") == "404":
            return {"matched": False, "reason": "no_archive"}
        raise e
