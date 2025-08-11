#!/usr/bin/env python3
"""
Core Sync Operations

Core sync functions for downloading and uploading books, extracted from SyncPipeline class.
"""

import asyncio
import logging
import shutil
import tarfile
import tempfile
import time
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

import aiofiles
import aiohttp
from tenacity import retry, stop_after_attempt, wait_fixed

from grin_to_s3.client import GRINClient
from grin_to_s3.common import (
    DEFAULT_DOWNLOAD_RETRIES,
    DEFAULT_DOWNLOAD_TIMEOUT,
    DEFAULT_RETRY_WAIT_SECONDS,
    create_http_session,
    decrypt_gpg_file,
    extract_bucket_config,
)
from grin_to_s3.database_utils import batch_write_status_updates
from grin_to_s3.extract.text_extraction import extract_ocr_pages
from grin_to_s3.extract.tracking import ExtractionStatus, StatusUpdate, collect_status
from grin_to_s3.metadata.marc_extraction import extract_marc_metadata
from grin_to_s3.storage import BookManager, create_storage_from_config
from grin_to_s3.storage.book_manager import BucketConfig
from grin_to_s3.storage.factories import LOCAL_STORAGE_DEFAULTS
from grin_to_s3.storage.staging import StagingDirectoryManager

from .models import BookSyncResult, create_book_sync_result
from .utils import check_encrypted_etag, should_skip_download


def extract_archive(archive_path: Path, extraction_dir: Path, barcode: str, context: str = "") -> float:
    """Extract a tarball archive with timing and logging.

    Args:
        archive_path: Path to the .tar.gz archive file
        extraction_dir: Directory to extract archive contents to
        barcode: Book barcode for logging
        context: Optional context string for log messages

    Returns:
        Extraction duration in seconds
    """
    logger = logging.getLogger(__name__)

    context_suffix = f" ({context})" if context else ""
    logger.info(f"[{barcode}] Extracting archive for processing{context_suffix}")

    extraction_start_time = time.time()
    with tarfile.open(str(archive_path), "r:gz") as tar:
        tar.extractall(path=extraction_dir)
    extraction_duration = time.time() - extraction_start_time

    logger.info(f"[{barcode}] Archive extracted in {extraction_duration:.1f}s{context_suffix}")
    return extraction_duration


def _should_retry_download_error(retry_state) -> bool:
    """
    Determine if a download error should be retried.

    Excludes 404 (Not Found) errors from retries as these indicate the book archive
    is not available, which retrying will not fix. Other HTTP errors are retried.

    Returns True if the error should be retried, False otherwise.
    """
    if not (retry_state.outcome and retry_state.outcome.failed):
        return False  # Don't retry successful operations

    exception = retry_state.outcome.exception()
    if isinstance(exception, aiohttp.ClientResponseError) and exception.status == 404:
        return False  # Don't retry 404 errors

    return True  # Retry all other exceptions


def create_download_retry_decorator(max_retries: int = DEFAULT_DOWNLOAD_RETRIES):
    """
    Create a retry decorator for download operations.

    Excludes 404 (Not Found) errors from retries as these indicate the book archive
    is not available, which retrying will not fix. Other errors are retried.

    Args:
        max_retries: Maximum number of retry attempts

    Returns:
        Tenacity retry decorator configured for download failures
    """
    return retry(
        stop=stop_after_attempt(max_retries + 1),  # +1 because tenacity counts initial attempt
        retry=_should_retry_download_error,  # Only exclude 404s from retries
        wait=wait_fixed(DEFAULT_RETRY_WAIT_SECONDS),  # Wait between retries
        reraise=True,  # Re-raise the exception after max attempts
    )


def _convert_marc_keys_to_db_fields(marc_data: dict[str, str | None]) -> dict[str, str | None]:
    """
    Convert MARC extraction keys to database field names.

    The MARC extraction function returns keys like 'control_number', 'title', etc.
    But the database expects keys like 'marc_control_number', 'marc_title', etc.
    """
    # Mapping from MARC extraction keys to database field names
    key_mapping = {
        "control_number": "marc_control_number",
        "date_type": "marc_date_type",
        "date1": "marc_date_1",
        "date2": "marc_date_2",
        "language": "marc_language",
        "loc_control_number": "marc_lccn",
        "loc_call_number": "marc_lc_call_number",
        "isbn": "marc_isbn",
        "oclc": "marc_oclc_numbers",
        "title": "marc_title",
        "title_remainder": "marc_title_remainder",
        "author100": "marc_author_personal",
        "author110": "marc_author_corporate",
        "author111": "marc_author_meeting",
        "subject": "marc_subjects",
        "genre": "marc_genres",
        "note": "marc_general_note",
    }

    # Convert keys and add extraction timestamp
    db_data = {}
    for marc_key, db_key in key_mapping.items():
        if marc_key in marc_data:
            db_data[db_key] = marc_data[marc_key]

    # Add extraction timestamp
    db_data["marc_extraction_timestamp"] = datetime.now(UTC).isoformat()

    return db_data


logger = logging.getLogger(__name__)


def _is_404_error(exception: Exception) -> bool:
    """Check if an exception is a 404 (Not Found) error."""
    return isinstance(exception, aiohttp.ClientResponseError) and exception.status == 404


async def check_archive_availability_with_etag(
    barcode: str, grin_client: GRINClient, library_directory: str, grin_semaphore: asyncio.Semaphore
) -> dict[str, Any]:
    """Check archive availability and ETag for processing previously downloaded books.

    Makes a HEAD request to determine if an archive exists and returns detailed
    information about availability, ETag, and whether conversion might be needed.

    Args:
        barcode: Book barcode
        grin_client: GRIN client instance
        library_directory: Library directory name
        grin_semaphore: Semaphore to control GRIN API concurrency (shares 5 QPS limit)

    Returns:
        dict with keys:
        - available: bool (True if archive exists)
        - etag: str | None (ETag if available)
        - file_size: int | None (File size if available)
        - http_status: int | None (HTTP status code)
        - needs_conversion: bool (True if 404, indicating conversion may be needed)
    """
    try:
        # Use existing HEAD checking function
        etag, file_size, status_code = await check_encrypted_etag(grin_client, library_directory, barcode, grin_semaphore)

        if status_code == 200:
            # Archive is available
            return {
                "available": True,
                "etag": etag,
                "file_size": file_size,
                "http_status": status_code,
                "needs_conversion": False,
            }
        elif status_code == 404:
            # Archive not found
            logger.debug(f"[{barcode}] Archive not available (404), may need conversion")
            return {
                "available": False,
                "etag": None,
                "file_size": None,
                "http_status": status_code,
                "needs_conversion": True,
            }
        else:
            # Other HTTP error
            logger.warning(f"[{barcode}] Archive check returned HTTP {status_code}")
            return {
                "available": False,
                "etag": None,
                "file_size": None,
                "http_status": status_code,
                "needs_conversion": False,  # Don't attempt conversion on other errors
            }

    except Exception as e:
        # Network/other errors that should be retried upstream
        logger.warning(f"[{barcode}] Archive availability check failed: {e}")
        raise  # Let upstream handle retries


async def check_and_handle_etag_skip(
    barcode: str,
    grin_client: GRINClient,
    library_directory: str,
    storage_type: str,
    storage_config: dict[str, Any],
    db_tracker,
    grin_semaphore: asyncio.Semaphore,
    force: bool = False,
) -> tuple[BookSyncResult | None, str | None, int, list]:
    """Check ETag and handle skip scenario if applicable.

    Args:
        barcode: Book barcode
        grin_client: GRIN client instance
        library_directory: Library directory name
        storage_type: Storage type
        storage_config: Storage configuration
        db_tracker: Database tracker instance
        grin_semaphore: Semaphore to control GRIN API concurrency (shares 5 QPS limit)
        force: Force download even if ETag matches

    Returns:
        tuple: (skip_result, encrypted_etag, encrypted_file_size, sync_status_updates)
        - skip_result: Skip result if file should be skipped, None if processing should continue
        - encrypted_etag: Encrypted ETag for this file
        - encrypted_file_size: File size for this file
        - sync_status_updates: List of status updates to be written by caller
    """
    # Check encrypted ETag first
    encrypted_etag, encrypted_file_size, status_code = await check_encrypted_etag(grin_client, library_directory, barcode, grin_semaphore)

    # If HEAD request returned 404, skip download entirely
    if status_code == 404:
        logger.info(f"[{barcode}] Archive not available (404) - skipping download")

        # Collect status for 404 archives
        sync_status_updates = [
            collect_status(
                barcode,
                "sync",
                "skipped",
                metadata={
                    "etag_checked_at": datetime.now(UTC).isoformat(),
                    "storage_type": storage_type,
                    "skipped": True,
                    "skip_reason": "archive_not_found_404",
                    "http_status": 404,
                },
            )
        ]

        return (
            create_book_sync_result(barcode, "completed", True, None, 0, 0),
            None,
            0,
            sync_status_updates,
        )

    # Check if we should skip download based on ETag match
    should_skip, skip_reason = await should_skip_download(
        barcode, encrypted_etag, storage_type, storage_config, db_tracker, force
    )
    if should_skip:
        logger.info(f"[{barcode}] Skipping download - {skip_reason}")

        # Collect ETag check status for batch writing
        sync_status_updates = [
            collect_status(
                barcode,
                "sync",
                "skipped",
                metadata={
                    "encrypted_etag": encrypted_etag,
                    "etag_checked_at": datetime.now(UTC).isoformat(),
                    "storage_type": storage_type,
                    "skipped": True,
                    "skip_reason": skip_reason,
                },
            )
        ]

        return (
            create_book_sync_result(barcode, "completed", True, encrypted_etag, encrypted_file_size or 0, 0),
            encrypted_etag,
            encrypted_file_size or 0,
            sync_status_updates,
        )

    return None, encrypted_etag, encrypted_file_size or 0, []


async def download_book_to_staging(
    barcode: str,
    grin_client: GRINClient,
    library_directory: str,
    staging_manager,
    encrypted_etag: str | None,
    secrets_dir: str | None = None,
    download_timeout: int = DEFAULT_DOWNLOAD_TIMEOUT,
    download_retries: int = DEFAULT_DOWNLOAD_RETRIES,
) -> tuple[str, str, dict[str, Any]]:
    """Download a book to staging directory.

    Args:
        barcode: Book barcode
        grin_client: GRIN client instance
        library_directory: Library directory name
        staging_manager: Staging directory manager
        encrypted_etag: Encrypted ETag for the file
        secrets_dir: Secrets directory path
        download_timeout: Timeout for download request in seconds
        download_retries: Number of retry attempts for failed downloads

    Returns:
        tuple: (barcode, staging_file_path, metadata)
    """

    # Create retry decorator with specified retries
    retry_decorator = create_download_retry_decorator(download_retries)

    @retry_decorator
    async def _download_with_retry():
        """Inner function with retry logic for download."""
        client = grin_client
        grin_url = f"https://books.google.com/libraries/{library_directory}/{barcode}.tar.gz.gpg"

        logger.info(f"[{barcode}] Starting download from {grin_url}")

        # Get staging file path
        staging_file = staging_manager.get_encrypted_file_path(barcode)

        # Download directly to staging file with specified timeout
        async with create_http_session(timeout=download_timeout) as session:
            response = await client.auth.make_authenticated_request(session, grin_url)

            total_bytes = 0
            async with aiofiles.open(staging_file, "wb") as f:
                async for chunk in response.content.iter_chunked(1024 * 1024):
                    await f.write(chunk)
                    total_bytes += len(chunk)

                    # Check disk space periodically during download
                    if total_bytes % (50 * 1024 * 1024) == 0:  # Every 50MB
                        if not staging_manager.check_disk_space():
                            # Clean up partial file and wait for space
                            staging_file.unlink(missing_ok=True)
                            logger.warning(
                                f"[{barcode}] Disk space exhausted during download, cleaning up and waiting for space..."
                            )
                            await staging_manager.wait_for_disk_space(check_interval=60)
                            # Retry the download with the same parameters
                            return await download_book_to_staging(
                                barcode,
                                grin_client,
                                library_directory,
                                staging_manager,
                                encrypted_etag,
                                secrets_dir,
                                download_timeout,
                                download_retries,
                            )

                # Ensure all data is flushed to disk
                await f.flush()

            # Verify file size matches what we downloaded
            actual_size = staging_file.stat().st_size
            if actual_size != total_bytes:
                staging_file.unlink(missing_ok=True)
                raise Exception(f"File size mismatch: expected {total_bytes}, got {actual_size}")

            return (
                barcode,
                str(staging_file),
                {
                    "file_size": total_bytes,
                    "total_time": 0,  # We'll track this separately
                    "google_etag": encrypted_etag,
                },
            )

    # Execute the download with retry logic
    return await _download_with_retry()


async def extract_and_upload_ocr_text(
    barcode: str,
    extracted_dir: Path,
    book_manager: BookManager,
    db_tracker,
    staging_manager: StagingDirectoryManager | None,
) -> list[StatusUpdate]:
    """
    Extract OCR text from extracted directory and upload to full-text bucket (non-blocking).

    This function is designed to be non-blocking - any failures are logged but do not
    raise exceptions that would interrupt the main sync workflow.

    Args:
        barcode: Book barcode
        extracted_dir: Path to extracted archive directory
        book_manager: BookStorage instance for uploading
        db_tracker: Database tracker for status updates
        staging_manager: Staging manager for temp file handling

    Returns:
        List of status updates to be written by caller
    """
    session_id = f"sync_{int(time.time())}"
    status_updates = []

    try:
        logger.info(f"[{barcode}] Starting OCR text extraction from extracted directory")

        # Collect extraction start status
        status_updates.append(
            collect_status(
                barcode,
                "text_extraction",
                ExtractionStatus.STARTING.value,
                metadata={"session_id": session_id, "source": "sync_pipeline"},
                session_id=session_id,
            )
        )

        # Create temporary JSONL file in staging directory
        staging_dir = staging_manager.staging_path if staging_manager else Path(extracted_dir).parent
        jsonl_file = staging_dir / f"{barcode}_ocr_temp.jsonl"

        # Collect extraction progress status
        status_updates.append(
            collect_status(
                barcode,
                "text_extraction",
                ExtractionStatus.EXTRACTING.value,
                metadata={"jsonl_file": str(jsonl_file)},
                session_id=session_id,
            )
        )

        # Extract OCR text to JSONL file
        start_time = time.time()
        page_count = await extract_ocr_pages(
            str(extracted_dir),
            db_tracker.db_path if db_tracker else "",
            session_id,
            output_file=str(jsonl_file),
        )
        extraction_time_ms = int((time.time() - start_time) * 1000)

        # Get file size
        jsonl_file_size = jsonl_file.stat().st_size if jsonl_file.exists() else 0

        logger.info(f"[{barcode}] Extracted {page_count} pages from archive")

        # Upload JSONL to full-text bucket
        upload_metadata = {
            "source": "sync_pipeline",
            "extraction_time_ms": str(extraction_time_ms),
            "page_count": str(page_count),
            "session_id": session_id,
        }

        await book_manager.save_ocr_text_jsonl_from_file(barcode, str(jsonl_file), metadata=upload_metadata)

        logger.info(
            f"[{barcode}] OCR text JSON saved to bucket_full ({jsonl_file_size / 1024:.1f} KB, {extraction_time_ms}ms)"
        )

        # Collect successful completion status
        status_updates.append(
            collect_status(
                barcode,
                "text_extraction",
                ExtractionStatus.COMPLETED.value,
                metadata={
                    "page_count": page_count,
                    "extraction_time_ms": extraction_time_ms,
                    "jsonl_file_size": jsonl_file_size,
                },
                session_id=session_id,
            )
        )

        # Clean up temporary JSONL file
        jsonl_file.unlink(missing_ok=True)
        logger.debug(f"[{barcode}] Cleaned up temporary OCR file: {jsonl_file}")

    except Exception as e:
        logger.error(f"[{barcode}] OCR extraction failed but sync continues: {e}")

        # Collect failure status but don't raise - this is non-blocking
        status_updates.append(
            collect_status(
                barcode,
                "text_extraction",
                ExtractionStatus.FAILED.value,
                metadata={
                    "error_type": type(e).__name__,
                    "error_message": str(e),
                },
                session_id=session_id,
            )
        )

    return status_updates


async def extract_and_update_marc_metadata(
    barcode: str,
    extracted_dir: Path,
    db_tracker,
) -> list[StatusUpdate]:
    """
    Extract MARC metadata from extracted directory and update database (non-blocking).

    This function is designed to be non-blocking - any failures are logged but do not
    raise exceptions that would interrupt the main sync workflow.

    Args:
        barcode: Book barcode
        extracted_dir: Path to extracted archive directory (not .tar.gz file)
        db_tracker: Database tracker for status updates

    Returns:
        List of status updates to be written by caller
    """
    session_id = f"marc_sync_{int(time.time())}"
    status_updates = []

    try:
        logger.info(f"[{barcode}] Starting MARC metadata extraction from extracted directory")

        # Collect extraction start status
        status_updates.append(
            collect_status(
                barcode,
                "text_extraction",
                ExtractionStatus.STARTING.value,
                metadata={"session_id": session_id, "source": "sync_pipeline", "extraction_type": "marc"},
                session_id=session_id,
            )
        )

        # Extract MARC metadata
        try:
            # Collect extraction progress status
            status_updates.append(
                collect_status(
                    barcode,
                    "text_extraction",
                    ExtractionStatus.EXTRACTING.value,
                    metadata={"extraction_type": "marc", "extracted_dir": str(extracted_dir)},
                    session_id=session_id,
                )
            )

            # Extract MARC metadata from the extracted directory
            marc_metadata = extract_marc_metadata(str(extracted_dir))

            if not marc_metadata:
                logger.warning(f"[{barcode}] No MARC metadata found in archive")
                status_updates.append(
                    collect_status(
                        barcode,
                        "text_extraction",
                        ExtractionStatus.FAILED.value,
                        metadata={
                            "error_type": "NoMARCDataFound",
                            "error_message": "No MARC metadata found",
                            "extraction_type": "marc",
                        },
                        session_id=session_id,
                    )
                )
                return status_updates

            # Update database with MARC metadata
            if db_tracker:
                status_updates.append(
                    collect_status(
                        barcode,
                        "text_extraction",
                        ExtractionStatus.EXTRACTING.value,
                        metadata={
                            "extraction_type": "marc",
                            "fields_count": len(marc_metadata),
                            "stage": "database_update",
                        },
                        session_id=session_id,
                    )
                )

                # Convert MARC extraction keys to database field names
                db_marc_data = _convert_marc_keys_to_db_fields(marc_metadata)

                # Update database using the tracker's method
                await db_tracker.update_book_marc_metadata(barcode, db_marc_data)

                logger.info(
                    f"[{barcode}] Successfully updated database with MARC metadata ({len(marc_metadata)} fields)"
                )

                # Collect completion status
                status_updates.append(
                    collect_status(
                        barcode,
                        "text_extraction",
                        ExtractionStatus.COMPLETED.value,
                        metadata={
                            "extraction_type": "marc",
                            "fields_extracted": len(marc_metadata),
                            "completion_time": datetime.now(UTC).isoformat(),
                        },
                        session_id=session_id,
                    )
                )

        except Exception as extraction_error:
            logger.error(f"[{barcode}] MARC extraction failed: {extraction_error}")
            status_updates.append(
                collect_status(
                    barcode,
                    "text_extraction",
                    ExtractionStatus.FAILED.value,
                    metadata={
                        "error_type": type(extraction_error).__name__,
                        "error_message": str(extraction_error),
                        "extraction_type": "marc",
                        "error_time": datetime.now(UTC).isoformat(),
                    },
                    session_id=session_id,
                )
            )

    except Exception as outer_error:
        logger.error(f"[{barcode}] MARC extraction workflow failed: {outer_error}")
        status_updates.append(
            collect_status(
                barcode,
                "text_extraction",
                ExtractionStatus.FAILED.value,
                metadata={
                    "error_type": type(outer_error).__name__,
                    "error_message": str(outer_error),
                    "extraction_type": "marc",
                    "workflow_error": True,
                },
                session_id=session_id,
            )
        )

    return status_updates


async def upload_book_from_staging(
    barcode: str,
    staging_file_path: str,
    storage_type: str,
    storage_config: dict[str, Any],
    staging_manager,
    db_tracker,
    encrypted_etag: str | None = None,
    secrets_dir: str | None = None,
    skip_extract_ocr: bool = False,
    skip_extract_marc: bool = False,
    skip_staging_cleanup: bool = False,
) -> dict[str, Any]:
    """Upload book from staging directory to storage.

    Args:
        barcode: Book barcode
        staging_file_path: Path to staging file
        storage_type: Storage type
        storage_config: Storage configuration
        staging_manager: Staging directory manager
        db_tracker: Database tracker instance
        encrypted_etag: Encrypted ETag for the file
        secrets_dir: Secrets directory path

    Returns:
        dict: Upload result
    """
    # Initialize extraction tasks list
    extraction_tasks: list = []

    try:
        # Check if this is a skip download scenario
        if staging_file_path == "SKIP_DOWNLOAD":
            logger.info(f"[{barcode}] Skipped download (ETag match), marking as completed")
            return {
                "barcode": barcode,
                "status": "completed",
                "decrypted_success": True,
                "skipped": True,
            }

        # Create storage
        storage = create_storage_from_config(storage_type, storage_config or {})

        # Get prefix information
        base_prefix = storage_config.get("prefix", "")

        # BookStorage handles bucket names as directory paths for all storage types

        # Create bucket configuration
        bucket_config: BucketConfig = extract_bucket_config(storage_type, storage_config)

        book_manager = BookManager(storage, bucket_config=bucket_config, base_prefix=base_prefix)

        # Get staging file paths
        encrypted_file = Path(staging_file_path)
        decrypted_file = staging_manager.get_decrypted_file_path(barcode)

        # Decrypt to staging directory
        try:
            await decrypt_gpg_file(str(encrypted_file), str(decrypted_file), secrets_dir)

            # Extract archive once for all downstream processes
            extracted_dir = staging_manager.get_extracted_directory_path(barcode)
            extracted_dir.mkdir(parents=True, exist_ok=True)

            extract_archive(decrypted_file, extracted_dir, barcode)

        except Exception as e:
            logger.error(f"[{barcode}] Decryption or extraction failed: {e}")
            # Clean up staging files on decryption/extraction failure
            if not skip_staging_cleanup:
                freed_bytes = staging_manager.cleanup_files(barcode)
                logger.info(f"[{barcode}] Freed {freed_bytes / (1024 * 1024):.1f} MB from staging after failure")
            raise Exception(f"GPG decryption or archive extraction failed for {barcode}: {e}") from e

        # Start extractions (OCR and MARC) if enabled (non-blocking)

        if not skip_extract_ocr:
            # Run OCR extraction concurrently with upload for better performance
            ocr_task = asyncio.create_task(
                extract_and_upload_ocr_text(barcode, extracted_dir, book_manager, db_tracker, staging_manager)
            )
            extraction_tasks.append(ocr_task)

        if not skip_extract_marc:
            # Run MARC extraction concurrently with upload for better performance
            marc_task = asyncio.create_task(extract_and_update_marc_metadata(barcode, extracted_dir, db_tracker))
            extraction_tasks.append(marc_task)

        # Upload decrypted file
        logger.info(f"[{barcode}] 🚀 Upload started")

        try:
            logger.info(f"[{barcode}] Uploading decrypted archive with encrypted ETag metadata...")
            decrypted_result = await book_manager.save_decrypted_archive_from_file(
                barcode, str(decrypted_file), encrypted_etag
            )
            logger.info(f"[{barcode}] Decrypted archive upload completed")
        except Exception as e:
            logger.error(f"[{barcode}] Decrypted archive upload failed: {e}")
            raise Exception(f"Upload failed - decrypted: {e}") from e

        # Collect sync completion status updates for batching
        metadata = {
            "encrypted_etag": encrypted_etag,
            "etag_stored_at": datetime.now(UTC).isoformat(),
            "storage_type": storage_type,
            "decrypted_success": True,
        }
        sync_completion_status_updates = [
            collect_status(barcode, "sync", "decrypted", metadata=metadata),
            collect_status(barcode, "sync", "completed", metadata=metadata),
        ]

        # Update book record with sync data including encrypted ETag
        sync_data: dict[str, Any] = {
            "storage_type": storage_type,
            "storage_path": decrypted_result,
            "is_decrypted": True,
            "sync_timestamp": datetime.now(UTC).isoformat(),
            "sync_error": None,
            "encrypted_etag": encrypted_etag,
            "last_etag_check": datetime.now(UTC).isoformat(),
        }

        # Wait for extraction tasks to complete before updating database
        if extraction_tasks:
            try:
                logger.info(f"[{barcode}] Waiting for {len(extraction_tasks)} extraction tasks to complete...")
                extraction_results = await asyncio.gather(*extraction_tasks, return_exceptions=True)
                logger.info(f"[{barcode}] All extraction tasks completed")

                # Collect all status updates from extraction tasks
                all_status_updates = []
                for result in extraction_results:
                    if isinstance(result, list):  # Status updates from extraction tasks
                        all_status_updates.extend(result)
                    elif isinstance(result, Exception):
                        logger.warning(f"[{barcode}] Extraction task failed: {result}")

                # Update book record with sync data after extraction tasks complete
                await db_tracker.update_sync_data(barcode, sync_data)

                # Add sync completion status updates
                all_status_updates.extend(sync_completion_status_updates)

                # Batch write all status updates
                if all_status_updates:
                    await batch_write_status_updates(db_tracker.db_path, all_status_updates)

            except Exception as e:
                # Log extraction failure but don't fail the sync
                logger.warning(f"[{barcode}] Extraction tasks failed: {e}")
                # Still update sync data even if extraction tasks failed
                await db_tracker.update_sync_data(barcode, sync_data)
        else:
            # No extraction tasks, update sync data immediately and write status
            await db_tracker.update_sync_data(barcode, sync_data)
            await batch_write_status_updates(db_tracker.db_path, sync_completion_status_updates)

        # Clean up staging files after successful uploads
        if not skip_staging_cleanup:
            freed_bytes = staging_manager.cleanup_files(barcode)
            logger.info(f"[{barcode}] ✅ Upload completed: decrypted=True")
            if freed_bytes > 0:
                logger.info(f"[{barcode}] Freed {freed_bytes / (1024 * 1024):.1f} MB disk space from staging directory")
        else:
            logger.info(f"[{barcode}] ✅ Upload completed: decrypted=True (staging cleanup skipped)")

        return {
            "barcode": barcode,
            "status": "completed",
            "decrypted_success": True,
        }

    except Exception as e:
        logger.error(f"[{barcode}] Upload failed: {e}")

        # Cancel extraction tasks if upload failed
        if extraction_tasks:
            for task in extraction_tasks:
                task.cancel()
            try:
                await asyncio.gather(*extraction_tasks, return_exceptions=True)
            except asyncio.CancelledError:
                logger.debug(f"[{barcode}] Extraction tasks cancelled due to upload failure")
            except Exception as extraction_error:
                logger.warning(f"[{barcode}] Extraction tasks failed during cleanup: {extraction_error}")

        # Don't clean up staging files on failure - they can be retried
        return {
            "barcode": barcode,
            "status": "failed",
            "error": str(e),
        }


async def sync_book_to_local_storage(
    barcode: str,
    grin_client: GRINClient,
    library_directory: str,
    storage_config: dict[str, Any],
    db_tracker,
    encrypted_etag: str | None = None,
    secrets_dir: str | None = None,
    skip_extract_ocr: bool = False,
    skip_extract_marc: bool = False,
    download_timeout: int = DEFAULT_DOWNLOAD_TIMEOUT,
    download_retries: int = DEFAULT_DOWNLOAD_RETRIES,
) -> dict[str, Any]:
    """Sync a book directly to local storage without staging.

    Args:
        barcode: Book barcode
        grin_client: GRIN client instance
        library_directory: Library directory name
        storage_config: Storage configuration
        db_tracker: Database tracker instance
        encrypted_etag: Encrypted ETag for the file
        secrets_dir: Secrets directory path
        skip_extract_ocr: Skip OCR text extraction
        skip_extract_marc: Skip MARC metadata extraction
        download_timeout: Timeout for download request in seconds
        download_retries: Number of retry attempts for failed downloads

    Returns:
        dict: Sync result
    """
    try:
        # Create storage for base_path validation
        storage = create_storage_from_config("local", storage_config or {})
        base_path = storage_config.get("base_path") if storage_config else None
        if not base_path:
            raise ValueError("Local storage requires base_path in configuration")

        # Create BookManager instance for OCR extraction
        bucket_config_dict = extract_bucket_config("local", storage_config or {})
        bucket_config: BucketConfig = {
            "bucket_raw": bucket_config_dict["bucket_raw"],
            "bucket_meta": bucket_config_dict["bucket_meta"],
            "bucket_full": bucket_config_dict["bucket_full"],
        }
        book_manager = BookManager(storage, bucket_config=bucket_config)

        # Generate final file paths
        encrypted_filename = f"{barcode}.tar.gz.gpg"
        decrypted_filename = f"{barcode}.tar.gz"

        # For local storage, construct paths using proper bucket structure
        bucket_raw = LOCAL_STORAGE_DEFAULTS["bucket_raw"]
        relative_encrypted_path = f"{bucket_raw}/{barcode}/{encrypted_filename}"
        relative_decrypted_path = f"{bucket_raw}/{barcode}/{decrypted_filename}"

        # Get absolute paths for local storage
        final_encrypted_path = Path(base_path) / relative_encrypted_path
        final_decrypted_path = Path(base_path) / relative_decrypted_path

        # Ensure directory exists
        final_encrypted_path.parent.mkdir(parents=True, exist_ok=True)

        # Download directly to final location with retry logic
        retry_decorator = create_download_retry_decorator(download_retries)

        @retry_decorator
        async def _download_local_with_retry():
            """Inner function with retry logic for local download."""
            client = grin_client
            grin_url = f"https://books.google.com/libraries/{library_directory}/{barcode}.tar.gz.gpg"

            logger.info(f"[{barcode}] Downloading directly to {final_encrypted_path}")

            async with create_http_session(timeout=download_timeout) as session:
                response = await client.auth.make_authenticated_request(session, grin_url)

                total_bytes = 0
                async with aiofiles.open(final_encrypted_path, "wb") as f:
                    async for chunk in response.content.iter_chunked(1024 * 1024):
                        await f.write(chunk)
                        total_bytes += len(chunk)
                    await f.flush()

                return total_bytes

        total_bytes = await _download_local_with_retry()

        logger.info(f"[{barcode}] Downloaded {total_bytes / (1024 * 1024):.1f} MB")

        # Decrypt directly to final location
        logger.info(f"[{barcode}] Decrypting to {final_decrypted_path}")
        try:
            await decrypt_gpg_file(str(final_encrypted_path), str(final_decrypted_path), secrets_dir)
            # For local storage, delete encrypted file after successful decryption (new behavior)
            logger.info(f"[{barcode}] Deleting encrypted file after successful decryption")
            final_encrypted_path.unlink(missing_ok=True)
        except Exception as e:
            logger.error(f"[{barcode}] Decryption failed: {e}")
            # Clean up encrypted file on decryption failure
            final_encrypted_path.unlink(missing_ok=True)
            raise

        # Collect sync completion status updates for batching
        metadata = {
            "encrypted_etag": encrypted_etag,
            "etag_stored_at": datetime.now(UTC).isoformat(),
            "storage_type": "local",
            "decrypted_success": True,
        }
        sync_completion_status_updates = [
            collect_status(barcode, "sync", "decrypted", metadata=metadata),
            collect_status(barcode, "sync", "completed", metadata=metadata),
        ]

        # Update book record with sync data
        sync_data: dict[str, Any] = {
            "storage_type": "local",
            "storage_path": str(final_decrypted_path),
            "is_decrypted": True,
            "sync_timestamp": datetime.now(UTC).isoformat(),
            "encrypted_etag": encrypted_etag,
            "last_etag_check": datetime.now(UTC).isoformat(),
        }
        await db_tracker.update_sync_data(barcode, sync_data)

        # Perform OCR and MARC extraction after successful decryption
        extraction_tasks = []
        temp_extracted_dir = Path(tempfile.mkdtemp(prefix=f"{barcode}_extracted_"))

        # Extract archive atomically for both OCR and MARC extraction
        if (not skip_extract_ocr) or (not skip_extract_marc):
            try:
                extract_archive(final_decrypted_path, temp_extracted_dir, barcode, "local storage")

                if not skip_extract_ocr:
                    # Run OCR extraction for local storage
                    ocr_task = asyncio.create_task(
                        extract_and_upload_ocr_text(barcode, temp_extracted_dir, book_manager, db_tracker, None)
                    )
                    extraction_tasks.append(ocr_task)

                if not skip_extract_marc:
                    # Run MARC extraction for local storage
                    marc_task = asyncio.create_task(
                        extract_and_update_marc_metadata(barcode, temp_extracted_dir, db_tracker)
                    )
                    extraction_tasks.append(marc_task)

            except Exception as extraction_error:
                logger.error(f"[{barcode}] Archive extraction failed (local storage): {extraction_error}")

        # Wait for extraction tasks to complete (non-blocking for sync success)
        if extraction_tasks:
            try:
                extraction_results = await asyncio.gather(*extraction_tasks, return_exceptions=True)

                # Collect all status updates from extraction tasks
                all_status_updates = []
                for result in extraction_results:
                    if isinstance(result, list):  # Status updates from extraction tasks
                        all_status_updates.extend(result)
                    elif isinstance(result, Exception):
                        logger.warning(f"[{barcode}] Extraction task failed: {result}")

                # Add sync completion status updates
                all_status_updates.extend(sync_completion_status_updates)

                # Batch write all status updates
                if all_status_updates:
                    await batch_write_status_updates(db_tracker.db_path, all_status_updates)

                logger.info(f"[{barcode}] Extraction tasks completed")
            except Exception as e:
                logger.warning(f"[{barcode}] Some extraction tasks failed: {e}")
            finally:
                # Clean up temporary extracted directory
                shutil.rmtree(temp_extracted_dir, ignore_errors=True)
        else:
            # No extraction tasks, but still need to write sync completion status
            await batch_write_status_updates(db_tracker.db_path, sync_completion_status_updates)

        logger.info(f"[{barcode}] ✅ Successfully synced to local storage")

        return {
            "barcode": barcode,
            "status": "completed",
            "decrypted_success": True,
        }

    except Exception as e:
        # Check if this is a 404 error
        is_404 = _is_404_error(e)

        if is_404:
            logger.info(f"[{barcode}] Archive not found (404)")
        else:
            logger.error(f"[{barcode}] Local storage sync failed: {e}")
        return {
            "barcode": barcode,
            "status": "failed",
            "error": str(e),
            "is_404": is_404,
        }
