#!/usr/bin/env python3
"""
GRIN Sync Management

Unified script to sync books from GRIN to storage and check sync status.
"""

import argparse
import asyncio
import json
import logging
import signal
import sys
import time
from datetime import datetime
from pathlib import Path
from typing import Any

import aiosqlite

from grin_to_s3.client import GRINClient
from grin_to_s3.collect_books.models import SQLiteProgressTracker
from grin_to_s3.common import (
    ProgressReporter,
    SlidingWindowRateCalculator,
    format_duration,
    pluralize,
    setup_logging,
    setup_storage_with_checks,
)

# Import moved to avoid circular dependency with download.py - imported locally where needed
from grin_to_s3.run_config import (
    apply_run_config_to_args,
    build_storage_config_dict,
    setup_run_database_path,
    validate_bucket_arguments,
)

logger = logging.getLogger(__name__)

# Global cache for bucket existence checks
_bucket_checked_cache: set[str] = set()


def reset_bucket_cache() -> None:
    """Reset the bucket existence cache (useful for testing)."""
    global _bucket_checked_cache
    _bucket_checked_cache.clear()


def validate_and_parse_barcodes(barcodes_str: str) -> list[str]:
    """Validate and parse comma-separated barcode string.

    Args:
        barcodes_str: Comma-separated string of barcodes

    Returns:
        List of validated barcodes

    Raises:
        ValueError: If any barcode is invalid
    """
    if not barcodes_str.strip():
        raise ValueError("Barcodes string cannot be empty")

    # Split by comma and clean up whitespace
    barcodes = [barcode.strip() for barcode in barcodes_str.split(',')]

    # Remove empty entries
    barcodes = [barcode for barcode in barcodes if barcode]

    if not barcodes:
        raise ValueError("No valid barcodes found")

    # Basic barcode validation - check for reasonable format
    for barcode in barcodes:
        if not barcode:
            raise ValueError("Empty barcode found")
        if len(barcode) < 3 or len(barcode) > 50:
            raise ValueError(f"Barcode '{barcode}' has invalid length (must be 3-50 characters)")
        # Check for reasonable characters (alphanumeric, dash, underscore)
        if not all(c.isalnum() or c in '-_' for c in barcode):
            raise ValueError(
                f"Barcode '{barcode}' contains invalid characters "
                f"(only alphanumeric, dash, underscore allowed)"
            )

    return barcodes


class SyncPipeline:
    """Pipeline for syncing converted books from GRIN to storage with database tracking."""

    def __init__(
        self,
        db_path: str,
        storage_type: str,
        storage_config: dict,
        library_directory: str,
        concurrent_downloads: int = 5,
        concurrent_uploads: int = 10,
        batch_size: int = 10,
        secrets_dir: str | None = None,
        gpg_key_file: str | None = None,
        force: bool = False,
        staging_dir: str | None = None,
        disk_space_threshold: float = 0.9,
    ):
        self.db_path = db_path
        self.storage_type = storage_type
        self.storage_config = storage_config
        self.concurrent_downloads = concurrent_downloads
        self.concurrent_uploads = concurrent_uploads
        self.batch_size = batch_size
        self.library_directory = library_directory
        self.secrets_dir = secrets_dir
        self.gpg_key_file = gpg_key_file
        self.force = force

        # Configure staging directory
        if staging_dir is None:
            # Default to run directory + staging
            run_dir = Path(db_path).parent
            self.staging_dir = run_dir / "staging"
        else:
            self.staging_dir = Path(staging_dir)
        self.disk_space_threshold = disk_space_threshold

        # Initialize components
        self.db_tracker = SQLiteProgressTracker(db_path)
        self.progress_reporter = ProgressReporter("sync", None)
        self.grin_client = GRINClient(secrets_dir=secrets_dir)

        # Initialize staging directory manager
        from grin_to_s3.staging import StagingDirectoryManager

        self.staging_manager = StagingDirectoryManager(
            staging_path=self.staging_dir, capacity_threshold=self.disk_space_threshold
        )

        # Concurrency control
        self._download_semaphore = asyncio.Semaphore(concurrent_downloads)
        self._upload_semaphore = asyncio.Semaphore(concurrent_uploads)
        self._shutdown_requested = False
        self._fatal_error: str | None = None  # Store fatal errors that should stop the pipeline

        # Statistics
        self.stats = {
            "processed": 0,
            "completed": 0,
            "failed": 0,
            "skipped": 0,
            "total_bytes": 0,
        }

    async def cleanup(self) -> None:
        """Clean up resources and close connections safely."""
        if self._shutdown_requested:
            return

        self._shutdown_requested = True
        logger.info("Shutting down sync pipeline...")

        try:
            if hasattr(self.db_tracker, "_db") and self.db_tracker._db:
                await self.db_tracker._db.close()
                logger.debug("Closed database connection")
        except Exception as e:
            logger.warning(f"Error closing database connection: {e}")

        try:
            if hasattr(self.grin_client, "session") and self.grin_client.session:
                await self.grin_client.session.close()
                logger.debug("Closed GRIN client session")
        except Exception as e:
            logger.warning(f"Error closing GRIN client session: {e}")

        logger.info("Cleanup completed")

    async def _check_google_etag(self, barcode: str) -> tuple[str | None, int | None]:
        """Make HEAD request to get Google's ETag and file size before downloading.

        Returns:
            tuple: (etag, file_size) or (None, None) if check fails
        """
        try:
            from grin_to_s3.common import create_http_session

            grin_url = f"https://books.google.com/libraries/{self.library_directory}/{barcode}.tar.gz.gpg"
            logger.debug(f"[{barcode}] Checking Google ETag via HEAD request to {grin_url}")

            async with create_http_session() as session:
                # Make HEAD request to get headers without downloading conten
                head_response = await self.grin_client.auth.make_authenticated_request(
                    session, grin_url, method="HEAD"
                )

                # Look for ETag and Content-Length headers
                etag = head_response.headers.get('ETag', '').strip('"')
                content_length = head_response.headers.get('Content-Length', '')

                file_size = int(content_length) if content_length else None

                if etag:
                    logger.debug(f"[{barcode}] Google ETag: {etag}, size: {file_size or 'unknown'}")
                    return etag, file_size
                else:
                    logger.debug(f"[{barcode}] No ETag found in Google response")
                    return None, file_size

        except Exception as e:
            logger.warning(f"[{barcode}] Failed to check Google ETag: {e}")
            return None, None

    async def _should_skip_download(self, barcode: str, google_etag: str | None) -> bool:
        """Check if existing storage files match Google's ETag to skip downloads.

        Args:
            barcode: Book barcode
            google_etag: ETag from Google's HEAD response

        Returns:
            bool: True if download should be skipped (file unchanged)
        """
        if self.force:
            logger.debug(f"[{barcode}] Force flag enabled, not skipping download")
            return False

        if not google_etag:
            logger.debug(f"[{barcode}] No Google ETag available, cannot skip download")
            return False

        if self.storage_type == "local":
            logger.debug(f"[{barcode}] Local storage doesn't support ETag checking, cannot skip")
            return False

        try:
            # Import here to avoid circular imports
            from grin_to_s3.common import create_storage_from_config
            from grin_to_s3.storage import BookStorage

            # Create storage
            storage = create_storage_from_config(self.storage_type, self.storage_config or {})

            # Get bucket and prefix information
            base_prefix = (self.storage_config or {}).get("prefix", "")
            bucket_name = self.storage_config.get("bucket_raw") if self.storage_config else None

            # For S3/MinIO/R2, bucket name must be included in path prefix
            if self.storage_type in ("minio", "s3", "r2") and bucket_name:
                if base_prefix:
                    base_prefix = f"{bucket_name}/{base_prefix}"
                else:
                    base_prefix = bucket_name

            book_storage = BookStorage(storage, base_prefix=base_prefix)

            # Check if archive exists and matches Google's ETag
            if await book_storage.archive_exists(barcode):
                if await book_storage.archive_matches_google_etag(barcode, google_etag):
                    logger.info(f"[{barcode}] File unchanged (ETag match), skipping download")
                    return True
                else:
                    logger.debug(f"[{barcode}] Archive exists but ETag differs, will download")
                    return False
            else:
                logger.debug(f"[{barcode}] Archive doesn't exist, will download")
                return False

        except Exception as e:
            logger.warning(f"[{barcode}] Error checking existing file for ETag: {e}")
            return False

    async def _ensure_bucket_exists(self, bucket_name: str) -> bool:
        """Ensure the bucket exists, create if it doesn't.

        Args:
            bucket_name: Name of the bucket to check/create

        Returns:
            True if bucket exists or was created successfully, False otherwise
        """
        if self.storage_type == "local":
            return True

        bucket_key = f"{self.storage_type}:{bucket_name}"

        # Check cache first to avoid repeated checks
        if bucket_key in _bucket_checked_cache:
            logger.debug(f"Bucket {bucket_name} already verified (skipping check)")
            return True

        try:
            # Use boto3 directly for bucket operations (fsspec doesn't support bucket creation)
            if self.storage_type in ("minio", "s3", "r2"):
                import boto3
                from botocore.exceptions import ClientError

                # Create boto3 client with same credentials
                s3_config = {
                    "aws_access_key_id": self.storage_config.get("access_key"),
                    "aws_secret_access_key": self.storage_config.get("secret_key"),
                }
                if self.storage_type == "minio":
                    s3_config["endpoint_url"] = self.storage_config.get("endpoint_url")
                elif self.storage_type == "r2":
                    account_id = self.storage_config.get("account_id")
                    if account_id:
                        s3_config["endpoint_url"] = f"https://{account_id}.r2.cloudflarestorage.com"

                s3_client = boto3.client('s3', **s3_config)

                try:
                    s3_client.head_bucket(Bucket=bucket_name)
                    logger.debug(f"Bucket '{bucket_name}' exists")
                    _bucket_checked_cache.add(bucket_key)
                    return True
                except ClientError as e:
                    if e.response['Error']['Code'] == '404':
                        # Bucket doesn't exist, try to create it
                        logger.info(f"Bucket '{bucket_name}' does not exist. Creating automatically...")
                        try:
                            s3_client.create_bucket(Bucket=bucket_name)

                            # Verify the bucket was actually created
                            buckets_response = s3_client.list_buckets()
                            bucket_names = [b['Name'] for b in buckets_response.get('Buckets', [])]

                            if bucket_name in bucket_names:
                                logger.info(f"Created and verified bucket '{bucket_name}'")
                                _bucket_checked_cache.add(bucket_key)
                                return True
                            else:
                                logger.error(f"Bucket '{bucket_name}' not found in list after creation")
                                return False

                        except ClientError as create_error:
                            logger.error(f"Failed to create bucket '{bucket_name}': {create_error}")
                            return False
                    else:
                        # Some other error occurred
                        logger.error(f"Error checking bucket '{bucket_name}': {e}")
                        return False
            else:
                # For other storage types, assume bucket exists
                _bucket_checked_cache.add(bucket_key)
                return True

        except Exception as e:
            logger.error(f"Error checking bucket '{bucket_name}': {type(e).__name__}: {e}")
            return False

    async def _mark_book_as_converted(self, barcode: str) -> None:
        """Mark a book as converted in our database after successful download."""
        try:
            # Use atomic status change
            await self.db_tracker.add_status_change(barcode, "processing_request", "converted")
        except Exception as e:
            logger.warning(f"[{barcode}] Failed to mark as converted: {e}")

    async def get_converted_books(self) -> set[str]:
        """Get set of books that are converted and ready for download."""
        try:
            response_text = await self.grin_client.fetch_resource(self.library_directory, "_converted?format=text")
            lines = response_text.strip().split("\n")
            converted_barcodes = set()
            for line in lines:
                if line.strip() and ".tar.gz.gpg" in line:
                    barcode = line.strip().replace(".tar.gz.gpg", "")
                    converted_barcodes.add(barcode)
            return converted_barcodes
        except Exception as e:
            logger.warning(f"Failed to get converted books: {e}")
            return set()

    async def _download_only(self, barcode: str) -> tuple[str, str, dict]:
        """Download a book to staging directory, returning file path for separate upload."""
        # Check Google ETag first to see if we can skip download
        google_etag, google_file_size = await self._check_google_etag(barcode)

        # Check if we should skip download based on ETag match
        if await self._should_skip_download(barcode, google_etag):
            self.stats["skipped"] += 1
            logger.info(f"[{barcode}] Skipping download - file unchanged (ETag match)")

            # Return skip metadata - the upload task will handle this appropriately
            return (
                barcode,
                "SKIP_DOWNLOAD",  # Special marker to indicate skip
                {
                    "file_size": google_file_size or 0,
                    "total_time": 0,
                    "google_etag": google_etag,
                    "skipped": True,
                },
            )

        # Wait for disk space to become available
        space_warned = False
        while not self.staging_manager.check_disk_space():
            if not space_warned:
                used_bytes, total_bytes, usage_ratio = self.staging_manager.get_disk_usage()
                logger.info(
                    f"[{barcode}] Waiting for disk space ({usage_ratio:.1%} full, "
                    f"{(total_bytes - used_bytes) / (1024 * 1024 * 1024):.1f} GB available), pausing download..."
                )
                space_warned = True
            await asyncio.sleep(30)  # Check every 30 seconds

        if space_warned:
            logger.info(f"[{barcode}] Disk space available, resuming download")

        async with self._download_semaphore:
            # Import here to avoid circular imports
            import aiofiles

            from grin_to_s3.common import create_http_session

            client = GRINClient(secrets_dir=self.secrets_dir)
            grin_url = f"https://books.google.com/libraries/{self.library_directory}/{barcode}.tar.gz.gpg"

            logger.info(f"[{barcode}] Starting download from {grin_url}")

            # Get staging file path
            staging_file = self.staging_manager.get_encrypted_file_path(barcode)

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
                            if not self.staging_manager.check_disk_space():
                                # Clean up partial file and pause
                                staging_file.unlink(missing_ok=True)
                                logger.warning(
                                    f"[{barcode}] Disk space exhausted during download, cleaning up and retrying..."
                                )
                                await asyncio.sleep(60)  # Wait longer before retry
                                return await self._download_only(barcode)  # Retry from beginning

                    # Ensure all data is flushed to disk
                    await f.flush()
                    # Note: aiofiles doesn't have fsync, but flush() should ensure data is written

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
                        "google_etag": google_etag,
                    },
                )

    async def _upload_book(self, barcode: str, staging_file_path: str, google_etag: str | None = None):
        """Handle upload task from staging directory files with optional Google ETag."""
        try:
            # Check if this is a skip download scenario
            if staging_file_path == "SKIP_DOWNLOAD":
                logger.info(f"[{barcode}] Skipped download (ETag match), marking as completed")

                # Update sync tracking to record the ETag check
                from datetime import UTC
                etag_sync_data: dict[str, Any] = {
                    "last_etag_check": datetime.now(UTC).isoformat(),
                    "google_etag": google_etag,
                }
                await self.db_tracker.update_sync_data(barcode, etag_sync_data)

                return {
                    "barcode": barcode,
                    "status": "completed",
                    "encrypted_success": True,
                    "decrypted_success": True,
                    "skipped": True,
                }

            # Import here to avoid circular imports
            from grin_to_s3.common import create_storage_from_config, decrypt_gpg_file
            from grin_to_s3.storage import BookStorage

            # Create storage
            storage = create_storage_from_config(self.storage_type, self.storage_config or {})

            # Get bucket and prefix information
            base_prefix = (self.storage_config or {}).get("prefix", "")
            bucket_name = self.storage_config.get("bucket_raw") if self.storage_config else None

            # For S3/MinIO/R2, bucket name must be included in path prefix
            if self.storage_type in ("minio", "s3", "r2") and bucket_name:
                if base_prefix:
                    base_prefix = f"{bucket_name}/{base_prefix}"
                else:
                    base_prefix = bucket_name

            # Ensure bucket exists before upload
            if self.storage_type in ("minio", "s3", "r2") and bucket_name:
                try:
                    await self._ensure_bucket_exists(bucket_name)
                except Exception as e:
                    logger.error(f"[{barcode}] Bucket creation failed: {e}")
                    raise

            book_storage = BookStorage(storage, base_prefix=base_prefix)

            # Get staging file paths
            encrypted_file = Path(staging_file_path)
            decrypted_file = self.staging_manager.get_decrypted_file_path(barcode)

            # Decrypt to staging directory
            try:
                await decrypt_gpg_file(str(encrypted_file), str(decrypted_file), self.gpg_key_file, self.secrets_dir)
            except Exception as e:
                logger.error(f"[{barcode}] Decryption failed: {e}")
                # Clean up staging files on decryption failure
                freed_bytes = self.staging_manager.cleanup_files(barcode)
                logger.info(f"[{barcode}] Freed {freed_bytes / (1024 * 1024):.1f} MB from staging after failure")
                # Decryption failure is fatal - signal pipeline to stop
                self._fatal_error = f"GPG decryption failed for {barcode}: {e}"
                self._shutdown_requested = True
                raise Exception(self._fatal_error) from e

            # Upload with semaphore control for entire book - acquire once for both uploads
            async with self._upload_semaphore:
                logger.info(f"[{barcode}] 🚀 Upload started (acquired semaphore)")
                upload_results = []

                # Upload encrypted file first with Google ETag metadata
                try:
                    logger.info(f"[{barcode}] Uploading encrypted archive...")
                    encrypted_result = await book_storage.save_archive_from_file(
                        barcode, str(encrypted_file), google_etag
                    )
                    upload_results.append(encrypted_result)
                    logger.info(f"[{barcode}] Encrypted archive upload completed")
                except Exception as e:
                    logger.error(f"[{barcode}] Encrypted archive upload failed: {e}")
                    upload_results.append(e)

                # Upload decrypted file second
                try:
                    logger.info(f"[{barcode}] Uploading decrypted archive...")
                    decrypted_result = await book_storage.save_decrypted_archive_from_file(barcode, str(decrypted_file))
                    upload_results.append(decrypted_result)
                    logger.info(f"[{barcode}] Decrypted archive upload completed")
                except Exception as e:
                    logger.error(f"[{barcode}] Decrypted archive upload failed: {e}")
                    upload_results.append(e)

            # Check results
            encrypted_success = not isinstance(upload_results[0], Exception)
            decrypted_success = not isinstance(upload_results[1], Exception)

            if not encrypted_success or not decrypted_success:
                # Upload failure
                errors = []
                if not encrypted_success:
                    errors.append(f"encrypted: {upload_results[0]}")
                if not decrypted_success:
                    errors.append(f"decrypted: {upload_results[1]}")
                raise Exception(f"Upload failed - {', '.join(errors)}")

            # Success
            await self.db_tracker.add_status_change(barcode, "sync", "stored")
            await self.db_tracker.add_status_change(barcode, "sync", "decrypted")
            await self.db_tracker.add_status_change(barcode, "sync", "completed")

            # Update book record with sync data including Google ETag
            from datetime import UTC
            sync_data: dict[str, Any] = {
                "storage_type": self.storage_type,
                "storage_path": upload_results[0],  # encrypted file path
                "storage_decrypted_path": upload_results[1],  # decrypted file path
                "is_decrypted": True,
                "sync_timestamp": datetime.now(UTC).isoformat(),
                "sync_error": None,
                "google_etag": google_etag,
                "last_etag_check": datetime.now(UTC).isoformat(),
            }
            await self.db_tracker.update_sync_data(barcode, sync_data)

            # Clean up staging files after successful uploads
            freed_bytes = self.staging_manager.cleanup_files(barcode)
            logger.info(f"[{barcode}] ✅ Upload completed: encrypted=True, decrypted=True")
            if freed_bytes > 0:
                logger.info(f"[{barcode}] Freed {freed_bytes / (1024 * 1024):.1f} MB disk space from staging directory")

            return {
                "barcode": barcode,
                "status": "completed",
                "encrypted_success": True,
                "decrypted_success": True,
            }

        except Exception as e:
            logger.error(f"[{barcode}] Upload failed: {e}")
            # Don't clean up staging files on failure - they can be retried
            return {
                "barcode": barcode,
                "status": "failed",
                "error": str(e),
            }

    async def _upload_task(self, barcode: str, staging_file_path: str, google_etag: str | None = None):
        """Handle upload task from staging directory files."""
        return await self._upload_book(barcode, staging_file_path, google_etag)

    async def process_batch(self, barcodes: list[str]) -> list[dict]:
        """Process a batch of downloads concurrently."""
        if not barcodes:
            return []

        # Create download tasks
        tasks = [self._download_only(barcode) for barcode in barcodes]

        # Execute all downloads concurrently
        results = await asyncio.gather(*tasks, return_exceptions=True)

        # Process results
        processed_results: list[dict[str, Any]] = []
        for i, result in enumerate(results):
            barcode = barcodes[i]
            self.stats["processed"] += 1

            if isinstance(result, Exception):
                self.stats["failed"] += 1
                logger.error(f"[{barcode}] Download task failed: {result}")
                processed_results.append(
                    {
                        "barcode": barcode,
                        "status": "failed",
                        "error": str(result),
                    }
                )
            elif isinstance(result, dict):
                processed_results.append(result)
            else:
                # Unexpected result type, treat as error
                self.stats["failed"] += 1
                logger.error(f"[{barcode}] Unexpected result type: {type(result)}")
                processed_results.append(
                    {
                        "barcode": barcode,
                        "status": "failed",
                        "error": f"Unexpected result type: {type(result)}",
                    }
                )

        return processed_results

    async def get_sync_status(self) -> dict:
        """Get current sync status and statistics."""
        stats = await self.db_tracker.get_sync_stats(self.storage_type)
        return {
            **stats,
            "session_stats": self.stats,
        }

    async def run_sync(self, limit: int | None = None, specific_barcodes: list[str] | None = None) -> None:
        """Run the complete sync pipeline.

        Args:
            limit: Optional limit on number of books to sync
            specific_barcodes: Optional list of specific barcodes to sync
        """
        print("Starting GRIN-to-Storage sync pipeline")
        print(f"Database: {self.db_path}")
        print(f"Storage: {self.storage_type}")
        print(f"Concurrent downloads: {self.concurrent_downloads}")
        print(f"Batch size: {self.batch_size}")
        if limit:
            print(f"Limit: {limit:,} {pluralize(limit, 'book')}")
        print()

        logger.info("Starting sync pipeline")
        logger.info(f"Database: {self.db_path}")
        logger.info(f"Storage type: {self.storage_type}")
        logger.info(f"Concurrent downloads: {self.concurrent_downloads}")

        # Reset bucket cache at start of sync
        reset_bucket_cache()

        start_time = time.time()

        # Initialize variables that might be referenced in finally block
        downloads_completed = 0
        staging_files_processed = 0
        completed_uploads = 0

        try:
            # Get list of converted books from GRIN
            print("Fetching list of converted books from GRIN...")
            converted_barcodes = await self.get_converted_books()
            if len(converted_barcodes) == 0:
                print("Warning: GRIN reports no converted books available (this could indicate an API issue)")
            else:
                print(f"GRIN reports {len(converted_barcodes):,} converted books available for download")

            # Get initial status
            initial_status = await self.get_sync_status()
            total_converted = initial_status["total_converted"]
            already_synced = initial_status["synced"]
            failed_count = initial_status["failed"]
            pending_count = initial_status["pending"]

            print(
                f"Database sync status: {total_converted:,} total, {already_synced:,} synced, "
                f"{failed_count:,} failed, {pending_count:,} pending"
            )

            # Check how many requested books need syncing (only those actually converted by GRIN)
            available_to_sync = await self.db_tracker.get_books_for_sync(
                storage_type=self.storage_type,
                limit=999999,  # Get all available
                converted_barcodes=converted_barcodes,  # Only sync books that GRIN reports as converted
                specific_barcodes=specific_barcodes,  # Optionally limit to specific barcodes
            )

            print(f"Found {len(available_to_sync):,} converted books that need syncing")

            if not available_to_sync:
                if len(converted_barcodes) == 0:
                    print("No converted books available from GRIN")
                else:
                    print("No converted books found that need syncing (all may already be synced)")

                # Report on pending books
                pending_books = await self.db_tracker.get_books_for_sync(
                    storage_type=self.storage_type,
                    limit=999999,
                    converted_barcodes=None,  # Get all requested books regardless of conversion
                )

                if pending_books:
                    print("Status summary:")
                    print(f"  - {len(pending_books):,} books requested for processing but not yet converted")
                    print(f"  - {len(converted_barcodes):,} books available from GRIN (from other requests)")
                    print("  - 0 books ready to sync (no overlap between requested and converted)")
                    print(
                        f"\nTip: Use 'python grin.py process monitor --run-name "
                        f"{Path(self.db_path).parent.name}' to check processing progress"
                    )

                return

            # Check for existing staging files from previous runs
            staging_files = self.staging_manager.get_staging_files()
            if staging_files:
                print(f"Found {len(staging_files)} existing staging files from previous runs")
                staging_summary = self.staging_manager.get_staging_summary()
                print(
                    f"Staging directory usage: {staging_summary['staging_size_mb']:.1f}MB "
                    f"({staging_summary['disk_usage_ratio']:.1%} disk usage)"
                )

                # Clean up old orphaned files (>24h old)
                cleaned_count = self.staging_manager.cleanup_orphaned_files(max_age_hours=24)
                if cleaned_count > 0:
                    print(f"Cleaned up {cleaned_count} orphaned staging files")

            # Check disk space after clearing staging files
            if not self.staging_manager.check_disk_space():
                print(
                    f"❌ Disk space limit still exceeded after clearing staging files "
                    f"(>{self.disk_space_threshold:.0%} full)"
                )
                print("Cannot start new downloads. Please free up disk space or increase threshold.")
                run_name = Path(self.db_path).parent.name
                print(
                    f"To increase threshold: python grin.py sync pipeline --run-name {run_name} "
                    f"--disk-space-threshold 0.95"
                )
                return

            # Set up progress tracking
            books_to_process = min(limit or len(available_to_sync), len(available_to_sync))
            self.progress_reporter = ProgressReporter("sync", books_to_process)
            self.progress_reporter.start()

            # Decoupled download/upload processing for continuous downloads
            downloads_started = 0  # Track total downloads started to respect limit
            active_download_tasks = {}  # barcode -> download task
            active_upload_tasks: dict[str, asyncio.Task] = {}  # barcode -> upload task

            # Initialize sliding window rate calculator
            rate_calculator = SlidingWindowRateCalculator(window_size=20)

            # Time-based progress reporting
            last_progress_report = start_time
            progress_interval = 300  # 5 minutes
            initial_interval = 60  # 1 minute for first few reports
            initial_reports_count = 0
            max_initial_reports = 3

            print(f"Starting sync of {books_to_process:,} books...")
            print(f"Concurrent limits: {self.concurrent_downloads} downloads, {self.concurrent_downloads * 2} uploads")
            print("Progress updates will be shown every 5 minutes (more frequent initially)")
            print("---")

            # Get initial book queue
            book_queue = await self.db_tracker.get_books_for_sync(
                storage_type=self.storage_type,
                limit=min(books_to_process, 1000),
                converted_barcodes=converted_barcodes,
                specific_barcodes=specific_barcodes
            )

            # Exclude books that already have staging files to avoid double-processing
            staging_barcodes = {barcode for barcode, _, _ in staging_files} if staging_files else set()
            book_queue = [barcode for barcode in book_queue if barcode not in staging_barcodes]

            logger.info(f"Books needing fresh download: {len(book_queue)}")
            logger.info(f"Books with staging files: {len(staging_barcodes)}")

            book_iter = iter(book_queue)

            # Set up staging file iterator for existing files
            staging_iter = iter(staging_files) if staging_files else iter([])

            # Fill initial download queue
            for _ in range(self.concurrent_downloads):
                try:
                    barcode = next(book_iter)
                    task = asyncio.create_task(self._download_only(barcode))
                    active_download_tasks[barcode] = task
                    downloads_started += 1
                    logger.info(f"[{barcode}] Started download (initial queue)")
                except StopIteration:
                    break

            while active_download_tasks or active_upload_tasks:
                # Check for shutdown request or fatal errors
                if self._shutdown_requested:
                    if self._fatal_error:
                        print(f"\n❌ Fatal error: {self._fatal_error}")
                        print("Stopping sync pipeline...")

                        # Cancel all active tasks immediately for fatal errors
                        all_tasks = list(active_download_tasks.values()) + list(active_upload_tasks.values())
                        for task in all_tasks:
                            if not task.done():
                                task.cancel()

                        # Wait briefly for cancellation to take effect
                        if all_tasks:
                            try:
                                await asyncio.wait_for(asyncio.gather(*all_tasks, return_exceptions=True), timeout=5)
                            except TimeoutError:
                                pass  # Tasks were cancelled, this is expected

                        await self.cleanup()
                        print("Pipeline stopped due to fatal error.")
                        import sys

                        sys.exit(1)
                    else:
                        print("\nShutdown requested, stopping sync...")
                        print("Waiting for active tasks to complete...")

                        # Wait for active tasks to complete gracefully
                        all_tasks = list(active_download_tasks.values()) + list(active_upload_tasks.values())
                        if all_tasks:
                            try:
                                await asyncio.wait_for(asyncio.gather(*all_tasks, return_exceptions=True), timeout=30)
                                print("All tasks completed gracefully")
                            except TimeoutError:
                                print("Timeout waiting for tasks, cancelling remaining tasks...")
                                for task in all_tasks:
                                    if not task.done():
                                        task.cancel()

                        # Perform cleanup
                        await self.cleanup()
                        break

                # Check for time-based progress updates
                current_time = time.time()
                current_interval = (
                    initial_interval if initial_reports_count < max_initial_reports else progress_interval
                )
                if current_time - last_progress_report >= current_interval:
                    if books_to_process > 0:
                        percentage = (completed_uploads / books_to_process) * 100
                        remaining = books_to_process - completed_uploads
                        elapsed = current_time - start_time

                        rate = rate_calculator.get_rate(start_time, completed_uploads)

                        eta_text = ""
                        if len(rate_calculator.batch_times) >= 3 and rate > 0:
                            eta_seconds = remaining / rate
                            eta_text = f" (ETA: {format_duration(eta_seconds)})"

                        interval_desc = "1 min" if initial_reports_count < max_initial_reports else "5 min"
                        # Show actual concurrency limits and current usage
                        downloads_running = self.concurrent_downloads - self._download_semaphore._value
                        uploads_running = self.concurrent_uploads - self._upload_semaphore._value
                        uploads_queued = len(active_upload_tasks) - uploads_running
                        print(
                            f"{completed_uploads:,}/{books_to_process:,} "
                            f"({percentage:.0f}%) - {rate:.1f} books/sec - "
                            f"elapsed: {format_duration(elapsed)}{eta_text} "
                            f"[{downloads_running}/{self.concurrent_downloads} downloads, "
                            f"{uploads_running}/{self.concurrent_uploads} uploads, "
                            f"{uploads_queued} uploads queued] [{interval_desc} update]"
                        )

                    last_progress_report = current_time
                    initial_reports_count += 1

                # Combine all tasks for waiting
                all_tasks = list(active_download_tasks.values()) + list(active_upload_tasks.values())

                if all_tasks:
                    done, pending = await asyncio.wait(
                        all_tasks,
                        return_when=asyncio.FIRST_COMPLETED,
                        timeout=1.0,  # Check progress every second
                    )

                    # Process completed downloads
                    for task in done:
                        # Check if it's a download task
                        completed_barcode = None
                        for barcode, task_ref in active_download_tasks.items():
                            if task_ref == task:
                                completed_barcode = barcode
                                break

                        if completed_barcode:
                            # Download completed - start upload immediately
                            del active_download_tasks[completed_barcode]

                            try:
                                barcode, staging_file_path, metadata = await task

                                # Start upload task immediately with Google ETag
                                google_etag = metadata.get("google_etag")
                                upload_task = asyncio.create_task(
                                    self._upload_task(barcode, staging_file_path, google_etag)
                                )
                                active_upload_tasks[barcode] = upload_task
                                file_size_mb = metadata.get("file_size", 0) / (1024 * 1024)

                                if metadata.get("skipped"):
                                    logger.info(
                                        f"[{barcode}] Download skipped (ETag match): "
                                        f"{file_size_mb:.1f} MB, upload queued"
                                    )
                                else:
                                    logger.info(f"[{barcode}] Download complete: {file_size_mb:.1f} MB, upload queued")

                                # Mark as processed for download queue management
                                downloads_completed += 1

                            except Exception as e:
                                self.stats["failed"] += 1
                                logger.error(f"[{completed_barcode}] Download failed: {e}")
                                downloads_completed += 1

                            # Try to start new download immediately to maintain queue
                            if downloads_started < books_to_process and (not limit or downloads_started < limit):
                                try:
                                    # Try to get next book from current batch
                                    next_barcode = next(book_iter)
                                except StopIteration:
                                    # Get new batch of books
                                    remaining_limit = None
                                    if limit:
                                        remaining_limit = limit - downloads_started

                                    if remaining_limit is None or remaining_limit > 0:
                                        new_batch = await self.db_tracker.get_books_for_sync(
                                            storage_type=self.storage_type,
                                            limit=min(remaining_limit or 1000, 1000),
                                            converted_barcodes=converted_barcodes,
                                            specific_barcodes=specific_barcodes,
                                        )
                                        if new_batch:
                                            book_iter = iter(new_batch)
                                            try:
                                                next_barcode = next(book_iter)
                                            except StopIteration:
                                                next_barcode = None
                                        else:
                                            next_barcode = None
                                    else:
                                        next_barcode = None

                                # Start new download if we have a book
                                if next_barcode:
                                    new_task = asyncio.create_task(self._download_only(next_barcode))
                                    active_download_tasks[next_barcode] = new_task
                                    downloads_started += 1
                                    logger.info(f"[{next_barcode}] Started download (queue refill)")

                                # If no new download started, try processing a staging file
                                elif len(active_upload_tasks) < self.concurrent_uploads:
                                    try:
                                        barcode, encrypted_path, _decrypted_path = next(staging_iter)
                                        if encrypted_path.exists():
                                            # For staging files, we don't have Google ETag info
                                            upload_task = asyncio.create_task(
                                                self._upload_task(barcode, str(encrypted_path), None)
                                            )
                                            active_upload_tasks[barcode] = upload_task
                                            staging_files_processed += 1
                                            logger.info(f"[{barcode}] Staging file upload queued")
                                    except StopIteration:
                                        pass  # No more staging files

                        else:
                            # Check if it's an upload task
                            completed_barcode = None
                            for barcode, task_ref in active_upload_tasks.items():
                                if task_ref == task:
                                    completed_barcode = barcode
                                    break

                            if completed_barcode:
                                # Upload completed
                                del active_upload_tasks[completed_barcode]

                                try:
                                    result_dict = await task
                                    if result_dict["status"] == "completed":
                                        self.stats["completed"] += 1
                                        self.stats["processed"] += 1  # Track overall processed count

                                        # Track skipped files separately
                                        if result_dict.get("skipped"):
                                            # Update skipped count (already incremented in _download_only)
                                            logger.debug(f"[{completed_barcode}] Completed upload for skipped download")

                                        await self._mark_book_as_converted(completed_barcode)
                                        completed_uploads += 1
                                        rate_calculator.add_batch(current_time, completed_uploads)
                                    else:
                                        self.stats["failed"] += 1
                                        await self.db_tracker.add_status_change(completed_barcode, "sync", "failed")
                                        logger.error(f"[{completed_barcode}] Upload task failed")

                                        # Check if this was a fatal GPG decryption error in the result
                                        error_msg = result_dict.get("error", "")
                                        if "GPG decryption failed" in error_msg:
                                            self._fatal_error = error_msg
                                            self._shutdown_requested = True

                                except Exception as e:
                                    self.stats["failed"] += 1
                                    await self.db_tracker.add_status_change(completed_barcode, "sync", "failed")
                                    logger.error(f"[{completed_barcode}] Upload task failed: {e}")

                                    # Check if this was a fatal GPG decryption error
                                    if "GPG decryption failed" in str(e):
                                        self._fatal_error = str(e)
                                        self._shutdown_requested = True

        except KeyboardInterrupt:
            print("\nSync interrupted by user")
            logger.info("Sync interrupted by user")

        except Exception as e:
            print(f"\nSync failed: {e}")
            logger.error(f"Sync failed: {e}", exc_info=True)

        finally:
            # Clean up resources
            await self.cleanup()

            # Final statistics
            self.progress_reporter.finish()

            total_elapsed = time.time() - start_time
            final_status = await self.get_sync_status()

            print("\nThis sync session completed:")
            print(f"  Runtime: {format_duration(total_elapsed)}")
            if downloads_completed > 0 and staging_files_processed > 0:
                print(f"  Fresh downloads: {downloads_completed:,}")
                print(f"  From staging: {staging_files_processed:,}")
                print(f"  Total processed: {downloads_completed + staging_files_processed:,}")
            elif downloads_completed > 0:
                print(f"  Books downloaded: {downloads_completed:,}")
            elif staging_files_processed > 0:
                print(f"  Files from staging: {staging_files_processed:,}")
            else:
                print("  No files processed")
            print(f"  Successfully uploaded: {completed_uploads:,}")
            print(f"  Failed uploads: {self.stats['failed']:,}")
            if self.stats['skipped'] > 0:
                print(f"  Skipped (ETag match): {self.stats['skipped']:,}")
            print(f"  Data transferred: {self.stats['total_bytes'] / 1024 / 1024:.1f} MB")
            if total_elapsed > 0 and completed_uploads > 0:
                avg_rate = completed_uploads / total_elapsed
                print(f"  Completion rate: {avg_rate:.1f} books/second")

            print("\nOverall database status:")
            print(f"  Total books in database: {final_status['total_converted']:,}")
            print(f"  Successfully synced (all-time): {final_status['synced']:,}")
            print(f"  Failed syncs (all-time): {final_status['failed']:,}")
            print(f"  Still pending: {final_status['pending']:,}")
            print(f"  With decrypted archives: {final_status['decrypted']:,}")

            logger.info("Sync pipeline completed")


    async def _run_catchup_sync(self, barcodes: list[str], limit: int | None = None) -> None:
        """Run catchup sync for specific books."""
        books_to_process = min(limit or len(barcodes), len(barcodes))
        self.progress_reporter = ProgressReporter("catchup", books_to_process)
        self.progress_reporter.start()

        start_time = time.time()
        processed_count = 0

        # Initialize sliding window rate calculator (same as pipeline for consistency)
        rate_calculator = SlidingWindowRateCalculator(window_size=20)

        # Time-based progress reporting with adaptive intervals
        last_progress_report = start_time
        progress_interval = 300  # 5 minutes in seconds normally
        initial_interval = 60  # 1 minute for first few reports
        initial_reports_count = 0
        max_initial_reports = 3  # Show more frequent updates for first 3 minutes

        print(f"Starting catchup sync of {books_to_process:,} books...")
        print("Progress updates will be shown every 5 minutes (more frequent initially)")
        print("---")

        try:
            # Process books in batches
            for i in range(0, len(barcodes), self.batch_size):
                if self._shutdown_requested:
                    print("\nShutdown requested, stopping catchup sync...")
                    break

                # Check for time-based progress updates (adaptive intervals)
                current_time = time.time()
                current_interval = (
                    initial_interval if initial_reports_count < max_initial_reports else progress_interval
                )
                if current_time - last_progress_report >= current_interval:
                    if books_to_process > 0:
                        percentage = (processed_count / books_to_process) * 100
                        remaining = books_to_process - processed_count
                        elapsed = current_time - start_time

                        # Calculate rate using sliding window
                        rate = rate_calculator.get_rate(start_time, processed_count)

                        # Show ETA only after enough batches for stable estimate
                        eta_text = ""
                        if len(rate_calculator.batch_times) >= 3 and rate > 0:
                            eta_seconds = remaining / rate
                            eta_text = f" (ETA: {format_duration(eta_seconds)})"

                        interval_desc = "1 min" if initial_reports_count < max_initial_reports else "5 min"
                        print(
                            f"Catchup in progress: {processed_count:,}/{books_to_process:,} "
                            f"({percentage:.1f}%) - {rate:.1f} books/sec - "
                            f"elapsed: {format_duration(elapsed)}{eta_text} "
                            f"[{interval_desc} update]"
                        )

                    last_progress_report = current_time
                    initial_reports_count += 1

                if limit and processed_count >= limit:
                    break

                batch = barcodes[i : i + self.batch_size]
                if limit:
                    remaining = limit - processed_count
                    batch = batch[:remaining]

                logger.debug(f"Processing catchup batch of {len(batch)} books...")

                # Show which books are being processed (for user visibility)
                if len(batch) <= 5:
                    barcode_list = ", ".join(batch)
                else:
                    barcode_list = f"{batch[0]}, {batch[1]}, ..., {batch[-1]} ({len(batch)} total)"
                print(f"Processing catchup batch: {barcode_list}")

                # Process the batch
                batch_start = time.time()
                batch_results = await self.process_batch(batch)
                batch_elapsed = time.time() - batch_start

                # Update progress
                processed_count += len(batch)
                self.progress_reporter.update(
                    items=len(batch),
                    bytes_count=sum(r.get("size", 0) for r in batch_results),
                    force=False,  # Don't force output on every batch
                )

                # Track batch completion for sliding window rate calculation
                current_time = time.time()
                rate_calculator.add_batch(current_time, processed_count)

                # Log batch completion
                completed_in_batch = sum(1 for r in batch_results if r["status"] == "completed")

                # Show immediate updates for failed batches
                if completed_in_batch < len(batch):
                    failed_in_batch = len(batch) - completed_in_batch
                    logger.info(
                        f"Catchup batch completed: {completed_in_batch}/{len(batch)} successful, "
                        f"{failed_in_batch} failed in {batch_elapsed:.1f}s"
                    )

                    # Show immediate progress update for failed batches
                    if books_to_process > 0:
                        percentage = (processed_count / books_to_process) * 100
                        print(
                            f"WARNING: Catchup batch completed with {failed_in_batch} failures - "
                            f"Progress: {processed_count:,}/{books_to_process:,} ({percentage:.1f}%)"
                        )
                else:
                    logger.debug(
                        f"Catchup batch completed: {completed_in_batch}/{len(batch)} successful in {batch_elapsed:.1f}s"
                    )

                # Small delay between batches
                await asyncio.sleep(0.1)

        except KeyboardInterrupt:
            print("\nCatchup sync interrupted by user")
            logger.info("Catchup sync interrupted by user")

        except Exception as e:
            print(f"\nCatchup sync failed: {e}")
            logger.error(f"Catchup sync failed: {e}", exc_info=True)

        finally:
            # Clean up resources
            await self.cleanup()

            # Final statistics
            self.progress_reporter.finish()

            total_elapsed = time.time() - start_time
            await self.get_sync_status()

            print("\nCatchup sync completed:")
            print(f"  Total runtime: {format_duration(total_elapsed)}")
            print(f"  Books processed: {self.stats['processed']:,}")
            print(f"  Successful downloads: {self.stats['completed']:,}")
            print(f"  Failed downloads: {self.stats['failed']:,}")
            print(f"  Total data: {self.stats['total_bytes'] / 1024 / 1024:.1f} MB")
            if total_elapsed > 0:
                print(f"  Average rate: {self.stats['processed'] / total_elapsed:.1f} books/second")

            logger.info("Catchup sync pipeline completed")


async def show_sync_status(db_path: str, storage_type: str | None = None) -> None:
    """Show sync status for books in the database."""

    # Validate database file
    db_file = Path(db_path)
    if not db_file.exists():
        print(f"❌ Error: Database file does not exist: {db_path}")
        return

    print("Sync Status Report")
    print(f"Database: {db_path}")
    if storage_type:
        print(f"Storage Type: {storage_type}")
    print("=" * 50)

    tracker = SQLiteProgressTracker(db_path)

    try:
        # Get overall book counts
        total_books = await tracker.get_book_count()
        enriched_books = await tracker.get_enriched_book_count()
        converted_books = await tracker.get_converted_books_count()

        print("Overall Book Counts:")
        print(f"  Total books in database: {total_books:,}")
        print(f"  Books with enrichment data: {enriched_books:,}")
        print(f"  Books in converted state: {converted_books:,}")
        print()

        # Get sync statistics
        sync_stats = await tracker.get_sync_stats(storage_type)

        print("Sync Status:")
        print(f"  Total converted books: {sync_stats['total_converted']:,}")
        print(f"  Successfully synced: {sync_stats['synced']:,}")
        print(f"  Failed syncs: {sync_stats['failed']:,}")
        print(f"  Pending syncs: {sync_stats['pending']:,}")
        print(f"  Currently syncing: {sync_stats['syncing']:,}")
        print(f"  Books with decrypted archives: {sync_stats['decrypted']:,}")

        if sync_stats["total_converted"] > 0:
            sync_percentage = (sync_stats["synced"] / sync_stats["total_converted"]) * 100
            print(f"  Sync completion: {sync_percentage:.1f}%")

        print()

        # Show breakdown by storage type if not filtered
        if not storage_type:
            print("Storage Type Breakdown:")

            # Get books by storage type and extract bucket from storage_path
            async with aiosqlite.connect(db_path) as db:
                cursor = await db.execute("""
                    SELECT storage_type, storage_path, COUNT(*) as count
                    FROM books
                    WHERE storage_type IS NOT NULL AND storage_path IS NOT NULL
                    GROUP BY storage_type, storage_path
                    ORDER BY storage_type, count DESC
                """)
                storage_breakdown = await cursor.fetchall()

                if storage_breakdown:
                    # Group by storage type and extract bucket from path
                    storage_buckets: dict[str, int] = {}
                    for storage, path, count in storage_breakdown:
                        # Extract bucket from path (first part after removing prefix)
                        bucket = "unknown"
                        if path:
                            # For paths like "bucket/BARCODE/..." extract the bucket
                            parts = path.split("/")
                            if parts:
                                bucket = parts[0]

                        key = f"{storage}/{bucket}"
                        storage_buckets[key] = storage_buckets.get(key, 0) + count

                    for storage_bucket, count in sorted(storage_buckets.items()):
                        print(f"  {storage_bucket}: {count:,} books")
                else:
                    print("  No books have been synced to any storage yet")

                print()

        # Show recent sync activity
        print("Recent Sync Activity (last 10):")
        async with aiosqlite.connect(db_path) as db:
            query = """
                SELECT b.barcode, h.status_value, b.sync_timestamp, b.sync_error, b.storage_type
                FROM books b
                JOIN book_status_history h ON b.barcode = h.barcode
                WHERE h.status_type = 'sync'
                  AND h.id = (
                      SELECT MAX(h2.id)
                      FROM book_status_history h2
                      WHERE h2.barcode = b.barcode AND h2.status_type = 'sync'
                  )
                  AND b.sync_timestamp IS NOT NULL
            """
            params = []

            if storage_type:
                query += " AND b.storage_type = ?"
                params.append(storage_type)

            query += " ORDER BY b.sync_timestamp DESC LIMIT 10"

            cursor = await db.execute(query, params)
            recent_syncs = await cursor.fetchall()

            if recent_syncs:
                for barcode, status, timestamp, error, st_type in recent_syncs:
                    status_icon = "✅" if status == "completed" else "❌" if status == "failed" else "🔄"
                    print(f"  {status_icon} {barcode} ({st_type}) - {status} at {timestamp}")
                    if error:
                        print(f"      Error: {error}")
            else:
                print("  No recent sync activity found")

    except Exception as e:
        print(f"❌ Error reading database: {e}")
        return

    finally:
        # Clean up database connections
        try:
            if hasattr(tracker, "_db") and tracker._db:
                await tracker._db.close()
        except Exception:
            pass


def validate_database_file(db_path: str) -> None:
    """Validate that the database file exists and contains the required tables."""
    import sqlite3

    db_file = Path(db_path)

    if not db_file.exists():
        print(f"❌ Error: Database file does not exist: {db_path}")
        print("\nRun a book collection first:")
        print("python grin.py collect --run-name <your_run_name>")
        sys.exit(1)

    try:
        with sqlite3.connect(db_path) as conn:
            cursor = conn.cursor()
            cursor.execute("SELECT name FROM sqlite_master WHERE type='table';")
            tables = cursor.fetchall()

            table_names = [table[0] for table in tables]
            required_tables = ["books", "processed", "failed"]
            missing_tables = [table for table in required_tables if table not in table_names]

            if missing_tables:
                print(f"❌ Error: Database is missing required tables: {missing_tables}")
                sys.exit(1)

    except sqlite3.Error as e:
        print(f"❌ Error: Cannot read SQLite database: {e}")
        sys.exit(1)

    logger.debug(f"Using database: {db_path}")




async def cmd_pipeline(args) -> None:
    """Handle the 'pipeline' command."""
    # Set up database path and apply run configuration
    db_path = setup_run_database_path(args, args.run_name)
    logger.debug(f"Using run: {args.run_name}")
    print(f"Database: {db_path}")

    # Apply run configuration defaults
    apply_run_config_to_args(args, args.db_path)

    # Validate database
    validate_database_file(args.db_path)

    # Validate that we have required storage arguments
    if not args.storage:
        print("Error: --storage argument is required (or must be in run config)")
        sys.exit(1)

    missing_buckets = validate_bucket_arguments(args, args.storage)
    if missing_buckets:
        print(f"Error: The following bucket arguments are required for MinIO: {', '.join(missing_buckets)}")
        print("  For R2/S3: bucket names can be specified in credentials file")
        sys.exit(1)

    # Build storage configuration
    storage_config = build_storage_config_dict(args)

    # Update run configuration with storage parameters if they were provided
    config_path = Path(args.db_path).parent / "run_config.json"
    if config_path.exists():
        try:
            # Read existing config
            with open(config_path) as f:
                run_config = json.load(f)

            # Update storage configuration
            storage_config_dict = {
                "type": args.storage,
                "config": {k: v for k, v in storage_config.items() if v is not None},
                "prefix": storage_config.get("prefix", "grin-books"),
            }

            run_config["storage_config"] = storage_config_dict

            # Write back to config file
            with open(config_path, "w") as f:
                json.dump(run_config, f, indent=2)

            print(f"Updated storage configuration in {config_path}")

        except (json.JSONDecodeError, OSError) as e:
            print(f"Warning: Could not update run config: {e}")
    else:
        print(f"Note: No run config found at {config_path} to update")

    # Set up storage with auto-configuration and connectivity checks
    await setup_storage_with_checks(args.storage, storage_config)

    # Set up logging
    db_name = Path(args.db_path).stem
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    log_file = f"logs/sync_pipeline_{args.storage}_{db_name}_{timestamp}.log"
    setup_logging(args.log_level, log_file)

    # Create and run pipeline
    try:
        pipeline = SyncPipeline(
            db_path=args.db_path,
            storage_type=args.storage,
            storage_config=storage_config,
            library_directory=args.grin_library_directory,
            concurrent_downloads=args.concurrent,
            concurrent_uploads=args.concurrent_uploads,
            batch_size=args.batch_size,
            secrets_dir=args.secrets_dir,
            gpg_key_file=args.gpg_key_file,
            force=args.force,
            staging_dir=args.staging_dir,
            disk_space_threshold=args.disk_space_threshold,
        )

        # Set up signal handlers for graceful shutdown
        def signal_handler(signum: int, frame: Any) -> None:
            if pipeline._shutdown_requested:
                # Second interrupt - hard exit
                print(f"\nReceived second signal {signum}, forcing immediate exit...")
                import sys

                sys.exit(1)
            print(f"\nReceived signal {signum}, shutting down gracefully...")
            print("Press Control-C again to force immediate exit")
            pipeline._shutdown_requested = True

        signal.signal(signal.SIGINT, signal_handler)
        signal.signal(signal.SIGTERM, signal_handler)

        # Parse barcodes if provided
        specific_barcodes = None
        if hasattr(args, 'barcodes') and args.barcodes:
            try:
                specific_barcodes = validate_and_parse_barcodes(args.barcodes)
                print(f"Filtering to specific barcodes: {', '.join(specific_barcodes)}")
            except ValueError as e:
                print(f"Error: Invalid barcodes: {e}")
                sys.exit(1)

        # Auto-optimization for single barcode processing
        if specific_barcodes and len(specific_barcodes) == 1:
            print(f"Single barcode detected: {specific_barcodes[0]}")
            print("Auto-optimizing settings for single book processing...")

            # Create optimized pipeline for single book
            pipeline = SyncPipeline(
                db_path=args.db_path,
                storage_type=args.storage,
                storage_config=storage_config,
                library_directory=args.grin_library_directory,
                concurrent_downloads=1,  # Optimal for single book
                concurrent_uploads=1,    # Optimal for single book
                batch_size=1,           # Single book batch
                secrets_dir=args.secrets_dir,
                gpg_key_file=args.gpg_key_file,
                force=args.force,
                staging_dir=args.staging_dir,
                disk_space_threshold=args.disk_space_threshold,
            )
            print("  - Concurrent downloads: 1")
            print("  - Concurrent uploads: 1")
            print("  - Batch size: 1")
            print()

        await pipeline.run_sync(limit=args.limit, specific_barcodes=specific_barcodes)

    except KeyboardInterrupt:
        print("\nOperation cancelled by user")
    except Exception as e:
        print(f"Pipeline failed: {e}")
        sys.exit(1)


async def cmd_status(args) -> None:
    """Handle the 'status' command."""
    # Set up database path and apply run configuration
    db_path = setup_run_database_path(args, args.run_name)
    logger.debug(f"Using run: {args.run_name}")
    print(f"Database: {db_path}")

    try:
        await show_sync_status(args.db_path, args.storage_type)
    except KeyboardInterrupt:
        print("\nOperation cancelled by user")
        sys.exit(1)
    except Exception as e:
        print(f"Error: {e}")
        sys.exit(1)



async def cmd_catchup(args) -> None:
    """Handle the 'catchup' command."""
    # Set up database path and apply run configuration
    db_path = setup_run_database_path(args, args.run_name)
    logger.debug(f"Using run: {args.run_name}")
    print(f"Database: {db_path}")

    # Set up signal handlers for graceful shutdown
    def signal_handler(signum: int, frame: Any) -> None:
        print(f"\nReceived signal {signum}, shutting down gracefully...")
        raise KeyboardInterrupt()

    signal.signal(signal.SIGINT, signal_handler)
    signal.signal(signal.SIGTERM, signal_handler)

    # Validate database
    validate_database_file(args.db_path)

    # Build storage configuration from run config
    config_path = Path(args.db_path).parent / "run_config.json"
    if not config_path.exists():
        print(f"❌ Error: No run config found at {config_path}")
        print("Run collect_books.py first to create a run configuration")
        sys.exit(1)

    try:
        with open(config_path) as f:
            run_config = json.load(f)

        storage_config_dict = run_config.get("storage_config")
        if not storage_config_dict:
            print("❌ Error: No storage configuration found in run config")
            print("Run collect_books.py with storage configuration first")
            sys.exit(1)

        storage_type = storage_config_dict["type"]

        # For R2/S3, we need to load the credentials file to get bucket information
        # Create a minimal args object to use build_storage_config_dict
        from argparse import Namespace

        temp_args = Namespace(
            storage=storage_type,
            secrets_dir=run_config.get("secrets_dir"),
            bucket_raw=None,
            bucket_meta=None,
            bucket_full=None,
            storage_config=None,
        )

        # Build full storage config with credentials and bucket info
        full_storage_config = build_storage_config_dict(temp_args)
        storage_config = full_storage_config["config"]

        logger.debug(f"Using storage: {storage_type}")

        # Set up storage with auto-configuration and connectivity checks
        await setup_storage_with_checks(storage_type, storage_config)

    except (json.JSONDecodeError, KeyError, OSError) as e:
        print(f"❌ Error reading run config: {e}")
        sys.exit(1)

    # Set up logging
    db_name = Path(args.db_path).stem
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    log_file = f"logs/sync_catchup_{storage_type}_{db_name}_{timestamp}.log"
    setup_logging(args.log_level, log_file)

    # Create and run catchup
    try:
        print("=" * 60)
        print("GRIN Sync Catchup - Download Already-Converted Books")
        print("=" * 60)
        print("This will download any books in your database that are")
        print("already converted in GRIN, regardless of whether you")
        print("requested them for processing.")
        print()

        # Get list of converted books from GRIN
        print("Fetching converted books from GRIN...")

        # Create a simple GRIN client to get converted books

        grin_client = GRINClient(secrets_dir=run_config.get("secrets_dir"))

        try:
            response_text = await grin_client.fetch_resource(
                run_config.get("library_directory"), "_converted?format=text"
            )
            lines = response_text.strip().split("\n")
            converted_barcodes = set()
            for line in lines:
                if line.strip() and ".tar.gz.gpg" in line:
                    barcode = line.strip().replace(".tar.gz.gpg", "")
                    converted_barcodes.add(barcode)

            print(f"GRIN reports {len(converted_barcodes):,} total converted books available")

        except Exception as e:
            print(f"❌ Error fetching converted books from GRIN: {e}")
            sys.exit(1)
        finally:
            if hasattr(grin_client, "session") and grin_client.session:
                await grin_client.session.close()

        # Find books in our database that are converted but not yet synced
        db_tracker = SQLiteProgressTracker(args.db_path)
        await db_tracker.init_db()

        # Get all books in our database
        async with aiosqlite.connect(args.db_path) as db:
            cursor = await db.execute("SELECT barcode FROM books")
            all_books = {row[0] for row in await cursor.fetchall()}

        print(f"Database contains {len(all_books):,} books")

        # Find books that are both in our database and converted in GRIN
        catchup_candidates = all_books.intersection(converted_barcodes)
        print(f"Found {len(catchup_candidates):,} books that are converted and in our database")

        if not catchup_candidates:
            print("No books available for catchup")
            return

        # Filter out books already successfully synced using efficient batch query
        books_to_sync = await db_tracker.get_books_for_sync(
            storage_type=storage_type,
            limit=999999,  # Get all candidates
            status_filter=None,  # We want books that haven't been synced yet
            converted_barcodes=set(catchup_candidates),
            specific_barcodes=None,  # Not filtering to specific barcodes in catchup
        )

        print(f"Found {len(books_to_sync):,} books ready for catchup sync")

        if not books_to_sync:
            print("All converted books are already synced")
            return

        # Apply limit if specified
        if args.limit:
            books_to_sync = books_to_sync[: args.limit]
            print(f"Limited to {len(books_to_sync):,} books for this catchup run")

        # Handle dry-run mode
        if args.dry_run:
            print(f"\n📋 DRY RUN: Would sync {len(books_to_sync):,} books")
            print("=" * 50)

            if len(books_to_sync) <= 20:
                print("Books that would be synced:")
                for i, barcode in enumerate(books_to_sync, 1):
                    print(f"  {i:3d}. {barcode}")
            else:
                print("First 10 books that would be synced:")
                for i, barcode in enumerate(books_to_sync[:10], 1):
                    print(f"  {i:3d}. {barcode}")
                print(f"  ... and {len(books_to_sync) - 10:,} more books")

                print("\nLast 10 books that would be synced:")
                for i, barcode in enumerate(books_to_sync[-10:], len(books_to_sync) - 9):
                    print(f"  {i:3d}. {barcode}")

            print("\nTo actually sync these books, run without --dry-run")
            return

        # Confirm with user
        if not args.yes:
            response = input(f"\nDownload {len(books_to_sync):,} books? [y/N]: ").strip().lower()
            if response not in ("y", "yes"):
                print("Catchup cancelled")
                return

        print(f"Catchup sync: {len(books_to_sync):,} books")

        # Create a modified sync pipeline for catchup
        pipeline = SyncPipeline(
            db_path=args.db_path,
            storage_type=storage_type,
            storage_config=storage_config,
            library_directory=run_config.get("library_directory"),
            concurrent_downloads=args.concurrent,
            concurrent_uploads=args.concurrent_uploads,
            batch_size=args.batch_size,
            secrets_dir=run_config.get("secrets_dir"),
            gpg_key_file=args.gpg_key_file,
            force=args.force,
        )

        # Record that these books are being processed as part of catchup
        session_id = f"catchup_{timestamp}"
        for barcode in books_to_sync:
            # Mark processing as requested (catchup) and converted
            await db_tracker.add_status_change(
                barcode,
                "processing_request",
                "converted",
                session_id=session_id,
                metadata={"source": "catchup", "reason": "already_converted_in_grin"},
            )

        # Use a custom method to sync these specific books
        await pipeline._run_catchup_sync(books_to_sync, args.limit)

    except KeyboardInterrupt:
        print("\nCatchup cancelled by user")
    except Exception as e:
        print(f"Catchup failed: {e}")
        sys.exit(1)


async def main() -> None:
    """Main CLI entry point for sync commands."""
    parser = argparse.ArgumentParser(
        description="GRIN sync management",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Sync all converted books in collection
  python grin.py sync pipeline --run-name harvard_2024

  # Sync specific books only
  python grin.py sync pipeline --run-name harvard_2024 --barcodes "12345,67890,abcde"

  # Sync a single book (auto-optimized settings)
  python grin.py sync pipeline --run-name harvard_2024 --barcodes "39015123456789"

  # Check sync status
  python grin.py sync status --run-name harvard_2024

  # Download already-converted books
  python grin.py sync catchup --run-name harvard_2024
        """,
    )

    subparsers = parser.add_subparsers(dest="command", help="Commands")

    # Pipeline command
    pipeline_parser = subparsers.add_parser(
        "pipeline",
        help="Sync converted books to storage",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Basic sync (auto-detects storage config from run)
  python grin.py sync pipeline --run-name harvard_2024

  # Sync with explicit storage configuration
  python grin.py sync pipeline --run-name harvard_2024 --storage r2
      --bucket-raw grin-raw --bucket-meta grin-meta --bucket-full grin-full

  # Sync specific books only
  python grin.py sync pipeline --run-name harvard_2024 --barcodes "12345,67890,abcde"

  # Sync with custom concurrency
  python grin.py sync pipeline --run-name harvard_2024 --concurrent 5

  # Retry failed syncs only
  python grin.py sync pipeline --run-name harvard_2024 --status failed

  # Sync with limit and force overwrite
  python grin.py sync pipeline --run-name harvard_2024 --limit 100 --force
        """,
    )

    pipeline_parser.add_argument("--run-name", required=True, help="Run name (e.g., harvard_2024)")

    # Storage configuration
    pipeline_parser.add_argument(
        "--storage",
        choices=["minio", "r2", "s3"],
        help="Storage backend (auto-detected from run config if not specified)",
    )
    pipeline_parser.add_argument(
        "--bucket-raw", help="Raw data bucket (auto-detected from run config if not specified)"
    )
    pipeline_parser.add_argument(
        "--bucket-meta", help="Metadata bucket (auto-detected from run config if not specified)"
    )
    pipeline_parser.add_argument(
        "--bucket-full", help="Full-text bucket (auto-detected from run config if not specified)"
    )
    pipeline_parser.add_argument("--credentials-file", help="Custom credentials file path")

    # Pipeline options
    pipeline_parser.add_argument("--concurrent", type=int, default=5, help="Concurrent downloads (default: 5)")
    pipeline_parser.add_argument("--concurrent-uploads", type=int, default=10, help="Concurrent uploads (default: 10)")
    pipeline_parser.add_argument("--batch-size", type=int, default=100, help="Batch size for processing (default: 100)")
    pipeline_parser.add_argument("--limit", type=int, help="Limit number of books to sync")
    pipeline_parser.add_argument(
        "--barcodes", help="Comma-separated list of specific barcodes to sync (e.g., '12345,67890,abcde')"
    )
    pipeline_parser.add_argument("--force", action="store_true", help="Force download and overwrite existing files")
    pipeline_parser.add_argument(
        "--grin-library-directory", help="GRIN library directory name (required, from run config)"
    )

    # Staging directory options
    pipeline_parser.add_argument(
        "--staging-dir", help="Custom staging directory path (default: output/run-name/staging)"
    )
    pipeline_parser.add_argument(
        "--disk-space-threshold",
        type=float,
        default=0.9,
        help="Disk usage threshold to pause downloads (0.0-1.0, default: 0.9)",
    )

    # GRIN options
    pipeline_parser.add_argument(
        "--secrets-dir", help="Directory containing GRIN secrets (auto-detected from run config if not specified)"
    )
    pipeline_parser.add_argument("--gpg-key-file", help="Custom GPG key file path")

    # Logging
    pipeline_parser.add_argument("--log-level", choices=["DEBUG", "INFO", "WARNING", "ERROR"], default="INFO")

    # Status command
    status_parser = subparsers.add_parser(
        "status",
        help="Check sync status",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Check overall sync status
  python grin.py sync status --run-name harvard_2024

  # Check sync status for specific storage type
  python grin.py sync status --run-name harvard_2024 --storage-type r2
        """,
    )

    status_parser.add_argument("--run-name", required=True, help="Run name (e.g., harvard_2024)")
    status_parser.add_argument("--storage-type", choices=["local", "minio", "r2", "s3"], help="Filter by storage type")

    # Catchup command
    catchup_parser = subparsers.add_parser(
        "catchup",
        help="Download already-converted books from GRIN",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Check what books would be synced
  python grin.py sync catchup --run-name harvard_2024 --dry-run

  # Basic catchup (uses storage config from run)
  python grin.py sync catchup --run-name harvard_2024

  # Catchup with limit and auto-confirm
  python grin.py sync catchup --run-name harvard_2024 --limit 100 --yes

  # Catchup with custom concurrency
  python grin.py sync catchup --run-name harvard_2024 --concurrent 5
        """,
    )

    catchup_parser.add_argument("--run-name", required=True, help="Run name (e.g., harvard_2024)")
    catchup_parser.add_argument("--limit", type=int, help="Limit number of books to download")
    catchup_parser.add_argument("--concurrent", type=int, default=5, help="Concurrent downloads (default: 5)")
    catchup_parser.add_argument("--batch-size", type=int, default=100, help="Batch size for processing (default: 100)")
    catchup_parser.add_argument("--force", action="store_true", help="Force download and overwrite existing files")
    catchup_parser.add_argument("--yes", "-y", action="store_true", help="Auto-confirm without prompting")
    catchup_parser.add_argument(
        "--dry-run", action="store_true", help="Show what books would be synced without downloading"
    )
    catchup_parser.add_argument("--gpg-key-file", help="Custom GPG key file path")
    catchup_parser.add_argument("--log-level", choices=["DEBUG", "INFO", "WARNING", "ERROR"], default="INFO")


    args = parser.parse_args()

    if not args.command:
        parser.print_help()
        sys.exit(1)

    if args.command == "pipeline":
        await cmd_pipeline(args)
    elif args.command == "status":
        await cmd_status(args)
    elif args.command == "catchup":
        await cmd_catchup(args)


if __name__ == "__main__":
    asyncio.run(main())
