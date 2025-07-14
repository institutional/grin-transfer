#!/usr/bin/env python3
"""
Core Sync Operations

Core sync functions for downloading and uploading books, extracted from SyncPipeline class.
"""

import asyncio
import logging
import time
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

import aiofiles

from grin_to_s3.client import GRINClient
from grin_to_s3.common import (
    create_http_session,
    decrypt_gpg_file,
)
from grin_to_s3.extract.text_extraction import extract_ocr_pages
from grin_to_s3.extract.tracking import ExtractionStatus, write_status
from grin_to_s3.metadata.marc_extraction import extract_marc_metadata
from grin_to_s3.storage import BookManager, create_storage_from_config
from grin_to_s3.storage.book_manager import BucketConfig
from grin_to_s3.storage.staging import StagingDirectoryManager

from .models import BookSyncResult, create_book_sync_result
from .utils import check_encrypted_etag, should_skip_download

logger = logging.getLogger(__name__)


async def check_and_handle_etag_skip(
    barcode: str,
    grin_client: GRINClient,
    library_directory: str,
    storage_type: str,
    storage_config: dict[str, Any],
    db_tracker,
    force: bool = False,
) -> tuple[BookSyncResult | None, str | None, int]:
    """Check ETag and handle skip scenario if applicable.

    Args:
        barcode: Book barcode
        grin_client: GRIN client instance
        library_directory: Library directory name
        storage_type: Storage type
        storage_config: Storage configuration
        db_tracker: Database tracker instance
        force: Force download even if ETag matches

    Returns:
        tuple: (skip_result, encrypted_etag, encrypted_file_size)
        - skip_result: Skip result if file should be skipped, None if processing should continue
        - encrypted_etag: Encrypted ETag for this file
        - encrypted_file_size: File size for this file
    """
    # Check encrypted ETag first
    encrypted_etag, encrypted_file_size = await check_encrypted_etag(grin_client, library_directory, barcode)

    # Check if we should skip download based on ETag match
    should_skip, skip_reason = await should_skip_download(
        barcode, encrypted_etag, storage_type, storage_config, db_tracker, force
    )
    if should_skip:
        logger.info(f"[{barcode}] Skipping download - {skip_reason}")

        # Record ETag check in status history with metadata
        await db_tracker.add_status_change(
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

        return (
            create_book_sync_result(barcode, "completed", True, encrypted_etag, encrypted_file_size or 0, 0),
            encrypted_etag,
            encrypted_file_size or 0,
        )

    return None, encrypted_etag, encrypted_file_size or 0


async def download_book_to_staging(
    barcode: str,
    grin_client: GRINClient,
    library_directory: str,
    staging_manager,
    encrypted_etag: str | None,
    secrets_dir: str | None = None,
) -> tuple[str, str, dict[str, Any]]:
    """Download a book to staging directory.

    Args:
        barcode: Book barcode
        grin_client: GRIN client instance
        library_directory: Library directory name
        staging_manager: Staging directory manager
        encrypted_etag: Encrypted ETag for the file
        secrets_dir: Secrets directory path

    Returns:
        tuple: (barcode, staging_file_path, metadata)
    """
    # Wait for disk space to become available
    space_warned = False
    while not staging_manager.check_disk_space():
        if not space_warned:
            used_bytes, total_bytes, usage_ratio = staging_manager.get_disk_usage()
            logger.info(
                f"[{barcode}] Waiting for disk space ({usage_ratio:.1%} full, "
                f"{(total_bytes - used_bytes) / (1024 * 1024 * 1024):.1f} GB available), pausing download..."
            )
            space_warned = True
        await asyncio.sleep(30)  # Check every 30 seconds

    if space_warned:
        logger.info(f"[{barcode}] Disk space available, resuming download")

    client = grin_client
    grin_url = f"https://books.google.com/libraries/{library_directory}/{barcode}.tar.gz.gpg"

    logger.info(f"[{barcode}] Starting download from {grin_url}")

    # Get staging file path
    staging_file = staging_manager.get_encrypted_file_path(barcode)

    # Download directly to staging file
    async with create_http_session() as session:
        response = await client.auth.make_authenticated_request(session, grin_url)

        total_bytes = 0
        async with aiofiles.open(staging_file, "wb") as f:
            async for chunk in response.content.iter_chunked(1024 * 1024):
                await f.write(chunk)
                total_bytes += len(chunk)

                # Check disk space periodically during download
                if total_bytes % (50 * 1024 * 1024) == 0:  # Every 50MB
                    if not staging_manager.check_disk_space():
                        # Clean up partial file and pause
                        staging_file.unlink(missing_ok=True)
                        logger.warning(f"[{barcode}] Disk space exhausted during download, cleaning up and retrying...")
                        await asyncio.sleep(60)  # Wait longer before retry
                        return await download_book_to_staging(
                            barcode, grin_client, library_directory, staging_manager, encrypted_etag, secrets_dir
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


async def extract_and_upload_ocr_text(
    barcode: str,
    decrypted_file: Path,
    book_storage: BookManager,
    db_tracker,
    staging_manager: StagingDirectoryManager | None,
) -> None:
    """
    Extract OCR text from decrypted archive and upload to full-text bucket (non-blocking).

    This function is designed to be non-blocking - any failures are logged but do not
    raise exceptions that would interrupt the main sync workflow.

    Args:
        barcode: Book barcode
        decrypted_file: Path to decrypted tar.gz archive
        book_storage: BookStorage instance for uploading
        db_tracker: Database tracker for status updates
        staging_manager: Staging manager for temp file handling
    """
    session_id = f"sync_{int(time.time())}"

    try:
        logger.info(f"[{barcode}] Starting OCR text extraction from decrypted archive")

        # Track extraction start
        if db_tracker:
            await write_status(
                db_tracker.db_path,
                barcode,
                ExtractionStatus.STARTING,
                metadata={"session_id": session_id, "source": "sync_pipeline"},
                session_id=session_id,
            )

        # Create temporary JSONL file in staging directory
        staging_dir = staging_manager.staging_path if staging_manager else Path(decrypted_file).parent
        jsonl_file = staging_dir / f"{barcode}_ocr_temp.jsonl"

        try:
            # Track extraction progress
            if db_tracker:
                await write_status(
                    db_tracker.db_path,
                    barcode,
                    ExtractionStatus.EXTRACTING,
                    metadata={"jsonl_file": str(jsonl_file)},
                    session_id=session_id,
                )

            # Extract OCR text to JSONL file
            start_time = time.time()
            page_count = await extract_ocr_pages(
                str(decrypted_file),
                db_tracker.db_path if db_tracker else "",
                session_id,
                output_file=str(jsonl_file),
                extract_to_disk=True,
                keep_extracted=False,
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

            await book_storage.save_ocr_text_jsonl_from_file(barcode, str(jsonl_file), metadata=upload_metadata)

            logger.info(
                f"[{barcode}] OCR text JSON saved to bucket_full "
                f"({jsonl_file_size / 1024:.1f} KB, {extraction_time_ms}ms)"
            )

            # Track successful completion
            if db_tracker:
                await write_status(
                    db_tracker.db_path,
                    barcode,
                    ExtractionStatus.COMPLETED,
                    metadata={
                        "page_count": page_count,
                        "extraction_time_ms": extraction_time_ms,
                        "jsonl_file_size": jsonl_file_size,
                    },
                    session_id=session_id,
                )

        finally:
            # Clean up temporary JSONL file
            if jsonl_file.exists():
                try:
                    jsonl_file.unlink()
                except OSError as e:
                    logger.warning(f"[{barcode}] Failed to clean up temporary JSONL file: {e}")

    except Exception as e:
        logger.error(f"[{barcode}] OCR extraction failed but sync continues: {e}")

        # Track failure but don't raise - this is non-blocking
        if db_tracker:
            try:
                await write_status(
                    db_tracker.db_path,
                    barcode,
                    ExtractionStatus.FAILED,
                    metadata={
                        "error_type": type(e).__name__,
                        "error_message": str(e),
                    },
                    session_id=session_id,
                )
            except Exception as db_error:
                logger.warning(f"⚠️ [{barcode}] Failed to track extraction failure in database: {db_error}")


async def extract_and_update_marc_metadata(
    barcode: str,
    decrypted_file: Path,
    db_tracker,
) -> None:
    """
    Extract MARC metadata from decrypted archive and update database (non-blocking).

    This function is designed to be non-blocking - any failures are logged but do not
    raise exceptions that would interrupt the main sync workflow.

    Args:
        barcode: Book barcode
        decrypted_file: Path to decrypted tar.gz archive
        db_tracker: Database tracker for status updates
    """
    session_id = f"marc_sync_{int(time.time())}"

    try:
        logger.info(f"[{barcode}] Starting MARC metadata extraction from decrypted archive")

        # Track extraction start
        if db_tracker:
            await write_status(
                db_tracker.db_path,
                barcode,
                ExtractionStatus.STARTING,
                metadata={"session_id": session_id, "source": "sync_pipeline", "extraction_type": "marc"},
                session_id=session_id,
            )

        # Extract MARC metadata
        try:
            # Track extraction progress
            if db_tracker:
                await write_status(
                    db_tracker.db_path,
                    barcode,
                    ExtractionStatus.EXTRACTING,
                    metadata={"extraction_type": "marc", "archive_path": str(decrypted_file)},
                    session_id=session_id,
                )

            # Extract MARC metadata from the archive
            marc_metadata = extract_marc_metadata(str(decrypted_file))

            if not marc_metadata:
                logger.warning(f"[{barcode}] No MARC metadata found in archive")
                if db_tracker:
                    await write_status(
                        db_tracker.db_path,
                        barcode,
                        ExtractionStatus.FAILED,
                        metadata={"error_type": "NoMARCDataFound", "error_message": "No MARC metadata found", "extraction_type": "marc"},
                        session_id=session_id,
                    )
                return

            # Update database with MARC metadata
            if db_tracker:
                await write_status(
                    db_tracker.db_path,
                    barcode,
                    ExtractionStatus.EXTRACTING,
                    metadata={"extraction_type": "marc", "fields_count": len(marc_metadata), "stage": "database_update"},
                    session_id=session_id,
                )

                # Update database using the tracker's method
                await db_tracker.update_book_marc_metadata(barcode, marc_metadata)

                logger.info(f"[{barcode}] Successfully updated database with MARC metadata ({len(marc_metadata)} fields)")

                # Track completion
                await write_status(
                    db_tracker.db_path,
                    barcode,
                    ExtractionStatus.COMPLETED,
                    metadata={
                        "extraction_type": "marc",
                        "fields_extracted": len(marc_metadata),
                        "completion_time": datetime.now(UTC).isoformat(),
                    },
                    session_id=session_id,
                )

        except Exception as extraction_error:
            logger.error(f"[{barcode}] MARC extraction failed: {extraction_error}")
            if db_tracker:
                await write_status(
                    db_tracker.db_path,
                    barcode,
                    ExtractionStatus.FAILED,
                    metadata={
                        "error_type": type(extraction_error).__name__,
                        "error_message": str(extraction_error),
                        "extraction_type": "marc",
                        "error_time": datetime.now(UTC).isoformat(),
                    },
                    session_id=session_id,
                )

    except Exception as outer_error:
        logger.error(f"[{barcode}] MARC extraction workflow failed: {outer_error}")
        if db_tracker:
            try:
                await write_status(
                    db_tracker.db_path,
                    barcode,
                    ExtractionStatus.FAILED,
                    metadata={
                        "error_type": type(outer_error).__name__,
                        "error_message": str(outer_error),
                        "extraction_type": "marc",
                        "workflow_error": True,
                    },
                    session_id=session_id,
                )
            except Exception as db_error:
                logger.warning(f"⚠️ [{barcode}] Failed to track MARC extraction failure in database: {db_error}")


async def upload_book_from_staging(
    barcode: str,
    staging_file_path: str,
    storage_type: str,
    storage_config: dict[str, Any],
    staging_manager,
    db_tracker,
    encrypted_etag: str | None = None,
    gpg_key_file: str | None = None,
    secrets_dir: str | None = None,
    skip_extract_ocr: bool = False,
    skip_extract_marc: bool = False,
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
        gpg_key_file: GPG key file path
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
        bucket_config: BucketConfig = {
            "bucket_raw": storage_config.get("bucket_raw", ""),
            "bucket_meta": storage_config.get("bucket_meta", ""),
            "bucket_full": storage_config.get("bucket_full", ""),
        }

        book_storage = BookManager(storage, bucket_config=bucket_config, base_prefix=base_prefix)

        # Get staging file paths
        encrypted_file = Path(staging_file_path)
        decrypted_file = staging_manager.get_decrypted_file_path(barcode)

        # Decrypt to staging directory
        try:
            await decrypt_gpg_file(str(encrypted_file), str(decrypted_file), gpg_key_file, secrets_dir)
        except Exception as e:
            logger.error(f"[{barcode}] Decryption failed: {e}")
            # Clean up staging files on decryption failure
            freed_bytes = staging_manager.cleanup_files(barcode)
            logger.info(f"[{barcode}] Freed {freed_bytes / (1024 * 1024):.1f} MB from staging after failure")
            raise Exception(f"GPG decryption failed for {barcode}: {e}") from e

        # Start extractions (OCR and MARC) if enabled (non-blocking)

        if not skip_extract_ocr:
            # Run OCR extraction concurrently with upload for better performance
            ocr_task = asyncio.create_task(
                extract_and_upload_ocr_text(barcode, decrypted_file, book_storage, db_tracker, staging_manager)
            )
            extraction_tasks.append(ocr_task)

        if not skip_extract_marc:
            # Run MARC extraction concurrently with upload for better performance
            marc_task = asyncio.create_task(
                extract_and_update_marc_metadata(barcode, decrypted_file, db_tracker)
            )
            extraction_tasks.append(marc_task)

        # Upload decrypted file
        logger.debug(f"[{barcode}] 🚀 Upload started")

        try:
            logger.debug(f"[{barcode}] Uploading decrypted archive with encrypted ETag metadata...")
            decrypted_result = await book_storage.save_decrypted_archive_from_file(
                barcode, str(decrypted_file), encrypted_etag
            )
            logger.debug(f"[{barcode}] Decrypted archive upload completed")
        except Exception as e:
            logger.error(f"[{barcode}] Decrypted archive upload failed: {e}")
            raise Exception(f"Upload failed - decrypted: {e}") from e

        # Success - update status tracking with ETag
        metadata = {
            "encrypted_etag": encrypted_etag,
            "etag_stored_at": datetime.now(UTC).isoformat(),
            "storage_type": storage_type,
            "decrypted_success": True,
        }
        await db_tracker.add_status_change(barcode, "sync", "decrypted", metadata=metadata)
        await db_tracker.add_status_change(barcode, "sync", "completed", metadata=metadata)

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
        await db_tracker.update_sync_data(barcode, sync_data)

        # Wait for extraction tasks to complete before cleanup
        if extraction_tasks:
            try:
                await asyncio.gather(*extraction_tasks, return_exceptions=True)
            except Exception as e:
                # Log extraction failure but don't fail the sync
                logger.warning(f"[{barcode}] Extraction tasks failed: {e}")

        # Clean up staging files after successful uploads
        freed_bytes = staging_manager.cleanup_files(barcode)
        logger.info(f"[{barcode}] ✅ Upload completed: decrypted=True")
        if freed_bytes > 0:
            logger.info(f"[{barcode}] Freed {freed_bytes / (1024 * 1024):.1f} MB disk space from staging directory")

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
    gpg_key_file: str | None = None,
    secrets_dir: str | None = None,
    skip_extract_ocr: bool = False,
    skip_extract_marc: bool = False,
) -> dict[str, Any]:
    """Sync a book directly to local storage without staging.

    Args:
        barcode: Book barcode
        grin_client: GRIN client instance
        library_directory: Library directory name
        storage_config: Storage configuration
        db_tracker: Database tracker instance
        encrypted_etag: Encrypted ETag for the file
        gpg_key_file: GPG key file path
        secrets_dir: Secrets directory path

    Returns:
        dict: Sync result
    """
    try:
        # Create storage and book storage
        storage = create_storage_from_config("local", storage_config or {})
        base_path = storage_config.get("base_path") if storage_config else None
        if not base_path:
            raise ValueError("Local storage requires base_path in configuration")
        # Create bucket configuration
        bucket_config: BucketConfig = {
            "bucket_raw": "",  # Local storage doesn't use separate buckets
            "bucket_meta": "",
            "bucket_full": "",
        }
        book_storage = BookManager(storage, bucket_config=bucket_config, base_prefix="")

        # Generate final file paths
        encrypted_filename = f"{barcode}.tar.gz.gpg"
        decrypted_filename = f"{barcode}.tar.gz"
        encrypted_path = book_storage._raw_archive_path(barcode, encrypted_filename)
        decrypted_path = book_storage._raw_archive_path(barcode, decrypted_filename)

        # Get absolute paths for local storage
        final_encrypted_path = Path(base_path) / encrypted_path
        final_decrypted_path = Path(base_path) / decrypted_path

        # Ensure directory exists
        final_encrypted_path.parent.mkdir(parents=True, exist_ok=True)

        # Download directly to final location
        client = grin_client
        grin_url = f"https://books.google.com/libraries/{library_directory}/{barcode}.tar.gz.gpg"

        logger.info(f"[{barcode}] Downloading directly to {final_encrypted_path}")

        async with create_http_session() as session:
            response = await client.auth.make_authenticated_request(session, grin_url)

            total_bytes = 0
            async with aiofiles.open(final_encrypted_path, "wb") as f:
                async for chunk in response.content.iter_chunked(1024 * 1024):
                    await f.write(chunk)
                    total_bytes += len(chunk)
                await f.flush()

        logger.info(f"[{barcode}] Downloaded {total_bytes / (1024 * 1024):.1f} MB")

        # Decrypt directly to final location
        logger.info(f"[{barcode}] Decrypting to {final_decrypted_path}")
        try:
            await decrypt_gpg_file(str(final_encrypted_path), str(final_decrypted_path), gpg_key_file, secrets_dir)
            # For local storage, delete encrypted file after successful decryption (new behavior)
            logger.info(f"[{barcode}] Deleting encrypted file after successful decryption")
            final_encrypted_path.unlink(missing_ok=True)
        except Exception as e:
            logger.error(f"[{barcode}] Decryption failed: {e}")
            # Clean up encrypted file on decryption failure
            final_encrypted_path.unlink(missing_ok=True)
            raise

        # Update status tracking with ETag
        metadata = {
            "encrypted_etag": encrypted_etag,
            "etag_stored_at": datetime.now(UTC).isoformat(),
            "storage_type": "local",
            "decrypted_success": True,
        }
        await db_tracker.add_status_change(barcode, "sync", "decrypted", metadata=metadata)
        await db_tracker.add_status_change(barcode, "sync", "completed", metadata=metadata)

        # Update book record with sync data
        sync_data: dict[str, Any] = {
            "storage_type": "local",
            "storage_path": str(decrypted_path),
            "is_decrypted": True,
            "sync_timestamp": datetime.now(UTC).isoformat(),
            "encrypted_etag": encrypted_etag,
            "last_etag_check": datetime.now(UTC).isoformat(),
        }
        await db_tracker.update_sync_data(barcode, sync_data)

        logger.info(f"[{barcode}] ✅ Successfully synced to local storage")

        return {
            "barcode": barcode,
            "status": "completed",
            "decrypted_success": True,
        }

    except Exception as e:
        logger.error(f"[{barcode}] Local storage sync failed: {e}")
        return {
            "barcode": barcode,
            "status": "failed",
            "error": str(e),
        }
