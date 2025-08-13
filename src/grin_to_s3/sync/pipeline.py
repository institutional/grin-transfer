#!/usr/bin/env python3
"""
Sync Pipeline Orchestration

Main pipeline orchestration for syncing books from GRIN to storage.
"""

import asyncio
import shutil
import time
from pathlib import Path
from typing import Any

from grin_to_s3.client import GRINClient
from grin_to_s3.collect_books.models import SQLiteProgressTracker
from grin_to_s3.common import (
    DEFAULT_DOWNLOAD_RETRIES,
    DEFAULT_DOWNLOAD_TIMEOUT,
    DEFAULT_MAX_SEQUENTIAL_FAILURES,
    RateLimiter,
    SlidingWindowRateCalculator,
    extract_bucket_config,
    format_duration,
    pluralize,
)
from grin_to_s3.constants import DEFAULT_CONVERSION_REQUEST_LIMIT, GRIN_RATE_LIMIT_DELAY
from grin_to_s3.database_utils import batch_write_status_updates
from grin_to_s3.extract.tracking import collect_status
from grin_to_s3.metadata.grin_enrichment import GRINEnrichmentPipeline
from grin_to_s3.processing import get_converted_books, get_in_process_set
from grin_to_s3.run_config import RunConfig
from grin_to_s3.storage import create_storage_from_config, get_storage_protocol
from grin_to_s3.storage.book_manager import BookManager
from grin_to_s3.storage.staging import StagingDirectoryManager
from grin_to_s3.sync.utils import logger

from .conversion_handler import ConversionRequestHandler
from .csv_export import CSVExportResult, export_and_upload_csv, export_csv_local
from .database_backup import create_local_database_backup, upload_database_to_storage
from .models import create_sync_stats
from .operations import (
    check_and_handle_etag_skip,
    download_book_to_filesystem,
    download_book_to_local,
    is_404_error,
    upload_book_from_staging,
)
from .utils import build_download_result, reset_bucket_cache

# Progress reporting intervals
INITIAL_PROGRESS_INTERVAL = 60  # 1 minute for first few reports
REGULAR_PROGRESS_INTERVAL = 600  # 10 minutes for subsequent reports
MAX_INITIAL_REPORTS = 3  # Number of initial reports before switching to regular interval


async def get_books_from_queue(grin_client, library_directory: str, queue_name: str, db_tracker) -> set[str]:
    """Get barcodes from specified queue.

    Args:
        grin_client: GRIN client instance
        library_directory: Library directory name
        queue_name: Queue name (converted, previous, changed, all)
        db_tracker: Database tracker instance

    Returns:
        set: Set of barcodes for the specified queue
    """
    if queue_name == "converted":
        return await get_converted_books(grin_client, library_directory)
    elif queue_name == "previous":
        # Get books with PREVIOUSLY_DOWNLOADED status
        previously_downloaded = await db_tracker.get_books_by_grin_state("PREVIOUSLY_DOWNLOADED")

        # Get in_process queue for filtering
        in_process = await get_in_process_set(grin_client, library_directory)

        # Get verified_unavailable books for filtering
        verified_unavailable = await db_tracker.get_books_with_status("verified_unavailable")

        # Return filtered set
        filtered_books = previously_downloaded - in_process - verified_unavailable
        logger.info(
            f"Previous queue: {len(previously_downloaded)} PREVIOUSLY_DOWNLOADED books, "
            f"filtered out {len(in_process)} in_process and {len(verified_unavailable)} unavailable, "
            f"returning {len(filtered_books)} books"
        )
        return filtered_books
    elif queue_name == "changed":
        # TODO: Implement changed queue (books with newer versions in GRIN)
        logger.warning("Changed queue not yet implemented")
        return set()
    elif queue_name == "all":
        # Union of converted and previous queues
        converted = await get_converted_books(grin_client, library_directory)
        previous = await get_books_from_queue(grin_client, library_directory, "previous", db_tracker)
        return converted | previous
    else:
        raise ValueError(f"Unknown queue name: {queue_name}")


class SyncPipeline:
    """Pipeline for syncing converted books from GRIN to storage with database tracking."""

    @classmethod
    def from_run_config(
        cls,
        config: RunConfig,
        process_summary_stage,
        force: bool = False,
        dry_run: bool = False,
        skip_extract_ocr: bool = False,
        skip_extract_marc: bool = False,
        skip_enrichment: bool = False,
        skip_csv_export: bool = False,
        skip_staging_cleanup: bool = False,
        skip_database_backup: bool = False,
        download_timeout: int = DEFAULT_DOWNLOAD_TIMEOUT,
        download_retries: int = DEFAULT_DOWNLOAD_RETRIES,
        max_sequential_failures: int = DEFAULT_MAX_SEQUENTIAL_FAILURES,
    ) -> "SyncPipeline":
        """Create SyncPipeline from RunConfig.

        Args:
            config: RunConfig containing all pipeline configuration
            process_summary_stage: Process summary stage for tracking
            force: Force re-download even if ETags match
            dry_run: Show what would be processed without downloading or uploading
            skip_extract_ocr: Skip OCR text extraction
            skip_extract_marc: Skip MARC metadata extraction
            skip_enrichment: Skip enrichment processing
            skip_csv_export: Skip CSV export after sync
            skip_staging_cleanup: Skip deletion of files in staging directory
            skip_database_backup: Skip database backup and upload
            download_timeout: Timeout for book downloads in seconds
            download_retries: Number of retry attempts for failed downloads
            max_sequential_failures: Exit pipeline after this many consecutive failures

        Returns:
            Configured SyncPipeline instance
        """
        return cls(
            config=config,
            process_summary_stage=process_summary_stage,
            force=force,
            dry_run=dry_run,
            skip_extract_ocr=skip_extract_ocr,
            skip_extract_marc=skip_extract_marc,
            skip_enrichment=skip_enrichment,
            skip_csv_export=skip_csv_export,
            skip_staging_cleanup=skip_staging_cleanup,
            skip_database_backup=skip_database_backup,
            download_timeout=download_timeout,
            download_retries=download_retries,
            max_sequential_failures=max_sequential_failures,
        )

    def __init__(
        self,
        config: RunConfig,
        process_summary_stage,
        force: bool = False,
        dry_run: bool = False,
        skip_extract_ocr: bool = False,
        skip_extract_marc: bool = False,
        skip_enrichment: bool = False,
        skip_csv_export: bool = False,
        skip_staging_cleanup: bool = False,
        skip_database_backup: bool = False,
        download_timeout: int = DEFAULT_DOWNLOAD_TIMEOUT,
        download_retries: int = DEFAULT_DOWNLOAD_RETRIES,
        max_sequential_failures: int = DEFAULT_MAX_SEQUENTIAL_FAILURES,
    ):
        # Store configuration and runtime parameters
        self.config = config
        self.force = force
        self.dry_run = dry_run
        self.skip_extract_ocr = skip_extract_ocr
        self.skip_extract_marc = skip_extract_marc
        self.enrichment_enabled = not skip_enrichment
        self.skip_csv_export = skip_csv_export
        self.skip_staging_cleanup = skip_staging_cleanup
        self.skip_database_backup = skip_database_backup
        self.download_timeout = download_timeout
        self.download_retries = download_retries
        self.max_sequential_failures = max_sequential_failures
        self.process_summary_stage = process_summary_stage

        # Extract commonly used config values
        self.db_path = config.sqlite_db_path
        self.full_storage_config = config.storage_config
        self.storage_type = self.full_storage_config["type"]
        self.storage_protocol = get_storage_protocol(self.storage_type)
        self.storage_config = self.full_storage_config["config"]
        self.library_directory = config.library_directory
        self.secrets_dir = config.secrets_dir

        # Sync configuration from RunConfig
        self.concurrent_downloads = config.sync_concurrent_downloads
        self.concurrent_uploads = config.sync_concurrent_uploads
        self.batch_size = config.sync_batch_size
        self.disk_space_threshold = config.sync_disk_space_threshold
        self.enrichment_workers = config.sync_enrichment_workers

        # Configure staging directory
        if config.sync_staging_dir is None:
            # Default to run directory + staging
            run_dir = Path(self.db_path).parent
            self.staging_dir = run_dir / "staging"
        else:
            self.staging_dir = Path(config.sync_staging_dir)

        # Initialize components
        self.db_tracker = SQLiteProgressTracker(self.db_path)
        self.grin_client = GRINClient(secrets_dir=self.secrets_dir)

        # Initialize staging directory manager only for non-local storage
        if self.storage_protocol != "local":
            self.staging_manager: StagingDirectoryManager | None = StagingDirectoryManager(
                staging_path=self.staging_dir, capacity_threshold=self.disk_space_threshold
            )
        else:
            self.staging_manager = None  # Not needed for local storage

        # Concurrency control
        self._download_semaphore = asyncio.Semaphore(self.concurrent_downloads)
        self._upload_semaphore = asyncio.Semaphore(self.concurrent_uploads)
        self._shutdown_requested = False
        self._fatal_error: str | None = None  # Store fatal errors that should stop the pipeline

        # Track actual active task counts for accurate reporting
        self._active_download_count = 0
        self._active_upload_count = 0
        self._pending_upload_count = 0  # Track pending uploads in queue

        # Simple gate to prevent race conditions in task creation
        self._task_creation_lock = asyncio.Lock()

        # Enrichment queue infrastructure
        if self.enrichment_enabled:
            self.enrichment_queue: asyncio.Queue[str] | None = asyncio.Queue()
        else:
            self.enrichment_queue = None

        # Statistics
        self.stats = create_sync_stats()
        self._processed_count = 0  # Track total processed (downloaded) books
        self._completed_count = 0  # Track fully synced books (upload completed/failed)
        self._has_started_work = False  # Track if any work has begun
        self._sequential_failures = 0  # Track consecutive failures for exit logic

        # Worker management
        self._enrichment_workers: list[asyncio.Task] = []

        # Initialize storage components once (now that tests provide complete configurations)
        self.storage = create_storage_from_config(self.full_storage_config)
        self.base_prefix = self.full_storage_config.get("prefix", "")
        self.bucket_config = extract_bucket_config(self.storage_type, self.storage_config)

        # Conversion request handling for previous queue
        self.current_queues: list[str] = []  # Track which queues are being processed
        self.conversion_handler: ConversionRequestHandler | None = None  # Lazy initialization
        self.conversion_request_limit = DEFAULT_CONVERSION_REQUEST_LIMIT
        self.book_manager = BookManager(self.storage, bucket_config=self.bucket_config, base_prefix=self.base_prefix)

    @property
    def uses_block_storage(self) -> bool:
        """Check if the pipeline uses block storage."""
        return self.staging_manager is not None and self.storage_protocol != "local"

    @property
    def uses_local_storage(self) -> bool:
        """Check if the pipeline uses local storage."""
        return self.storage_protocol == "local"

    def _handle_failure(self, barcode: str, error_msg: str) -> bool:
        """
        Handle a failure and check if pipeline should exit due to sequential failures.

        Args:
            barcode: The barcode that failed
            error_msg: Error message

        Returns:
            True if pipeline should exit, False otherwise
        """
        self.stats["failed"] += 1
        self._sequential_failures += 1

        logger.error(f"[{barcode}] ❌ Failed: {error_msg}")
        logger.warning(f"Sequential failures: {self._sequential_failures}/{self.max_sequential_failures}")

        # Live user reporting of failures
        print(f"❌ [{barcode}] Failed: {error_msg}")
        if self._sequential_failures > 1:
            print(f"⚠️  Sequential failures: {self._sequential_failures}/{self.max_sequential_failures}")

        if self._sequential_failures >= self.max_sequential_failures:
            logger.error(f"🛑 Exiting pipeline: {self.max_sequential_failures} consecutive failures reached")
            print(f"🛑 Exiting pipeline: {self.max_sequential_failures} consecutive failures reached")
            return True

        return False

    def _handle_success(self, barcode: str) -> None:
        """
        Handle a successful operation and reset sequential failure counter.

        Args:
            barcode: The barcode that succeeded
        """
        self._sequential_failures = 0  # Reset on any success
        logger.info(f"[{barcode}] ✅ Success (failure counter reset)")

    async def cleanup(self, sync_successful: bool = False) -> None:
        """Clean up resources and close connections safely.

        Args:
            sync_successful: Whether the sync completed successfully
        """
        if self._shutdown_requested:
            return

        self._shutdown_requested = True
        logger.info("Shutting down sync pipeline...")

        # Stop enrichment workers first
        await self.stop_enrichment_workers()

        # Final staging cleanup (only if sync was successful and not skipped)
        if self.uses_block_storage:
            if sync_successful and not self.skip_staging_cleanup:
                try:
                    logger.info("Performing final staging directory cleanup...")
                    shutil.rmtree(self.staging_manager.staging_path, ignore_errors=True)  # type: ignore[union-attr]
                    logger.info("Staging directory cleaned up")
                except Exception as e:
                    logger.warning(f"Error during final staging cleanup: {e}")

        try:
            if hasattr(self.db_tracker, "close"):
                await self.db_tracker.close()
                logger.debug("Closed database connection")
        except Exception as e:
            logger.warning(f"Error closing database connection: {e}")

        logger.info("Cleanup completed")

    async def _background_progress_reporter(
        self,
        start_time: float,
        books_to_process: int,
        rate_calculator: SlidingWindowRateCalculator,
    ) -> None:
        """Independent background progress reporter that runs throughout pipeline."""
        last_report_time = 0.0
        initial_reports_count = 0
        while not self._shutdown_requested:
            try:
                # Calculate timing for next report
                current_time = time.time()
                interval = (
                    INITIAL_PROGRESS_INTERVAL
                    if initial_reports_count < MAX_INITIAL_REPORTS
                    else REGULAR_PROGRESS_INTERVAL
                )

                # Check if it's time to report
                if current_time - last_report_time >= interval:
                    # Query current state
                    completed_count = await self._get_completed_count()

                    # Get active task counts
                    downloads_active = self._active_download_count
                    uploads_active = self._active_upload_count
                    uploads_queued = self._pending_upload_count
                    enrichment_queued = self.enrichment_queue.qsize() if self.enrichment_queue else 0

                    # Determine pipeline phase
                    sync_phase_active = downloads_active > 0 or uploads_active > 0
                    enrichment_phase_active = enrichment_queued > 0 and not sync_phase_active

                    # Show appropriate progress
                    await self._show_unified_progress(
                        completed_count,
                        books_to_process,
                        start_time,
                        current_time,
                        downloads_active,
                        uploads_active,
                        uploads_queued,
                        enrichment_queued,
                        sync_phase_active,
                        enrichment_phase_active,
                        rate_calculator,
                        interval,
                        last_report_time,
                    )

                    # Update tracking
                    last_report_time = current_time
                    if initial_reports_count < MAX_INITIAL_REPORTS:
                        initial_reports_count += 1

                # Sleep briefly to avoid busy-waiting, but check for shutdown more frequently
                for _ in range(10):  # Check every 0.1 seconds instead of every 1 second
                    if self._shutdown_requested:
                        break
                    await asyncio.sleep(0.1)

            except Exception as e:
                logger.error(f"Error in progress reporter: {e}", exc_info=True)
                await asyncio.sleep(1)

    async def _get_completed_count(self) -> int:
        """Get count of books fully synced in current session."""
        # Return the number of books that have been fully synced (upload completed or failed)
        return self._completed_count

    async def _show_unified_progress(
        self,
        completed_count: int,
        books_to_process: int,
        start_time: float,
        current_time: float,
        downloads_active: int,
        uploads_active: int,
        uploads_queued: int,
        enrichment_queued: int,
        sync_phase_active: bool,
        enrichment_phase_active: bool,
        rate_calculator: SlidingWindowRateCalculator,
        interval: int,
        last_report_time: float,
    ) -> None:
        """Display unified progress for all pipeline phases."""
        percentage = (completed_count / books_to_process) * 100 if books_to_process > 0 else 0
        elapsed = current_time - start_time
        rate = rate_calculator.get_rate(start_time, completed_count)

        # Calculate ETA
        remaining = books_to_process - completed_count
        eta_text = ""
        if rate > 0 and remaining > 0:
            eta_seconds = remaining / rate
            eta_text = f" (ETA: {format_duration(eta_seconds)})"

        # Calculate time until next update
        time_since_last_report = current_time - last_report_time
        time_until_next_update = max(0, interval - time_since_last_report)
        minutes_until_next = int(time_until_next_update // 60) + 1  # Round up to next minute
        minute_plural = "min" if minutes_until_next == 1 else "min"
        interval_desc = f"next update in {minutes_until_next} {minute_plural}"

        # Build status message based on phase
        if sync_phase_active:
            # Download/upload phase
            status_details = (
                f"[{downloads_active}/{self.concurrent_downloads} downloads, "
                f"{uploads_active}/{self.concurrent_uploads} uploads"
            )
            if uploads_queued > 0:
                upload_plural = "upload" if uploads_queued == 1 else "uploads"
                status_details += f", {uploads_queued} {upload_plural} queued"
            if enrichment_queued > 0:
                status_details += f", {enrichment_queued} enrichment queued"
            status_details += "]"
        elif enrichment_phase_active:
            # Pure enrichment phase
            status_details = f"[Enrichment phase: {enrichment_queued} books remaining]"
        else:
            # Either starting or finalizing
            if not self._has_started_work and completed_count == 0:
                status_details = "[Starting...]"
            else:
                status_details = "[Finalizing]"

        # Print progress
        print(
            f"{completed_count:,}/{books_to_process:,} "
            f"({percentage:.1f}%) - {rate:.1f} books/sec - "
            f"elapsed: {format_duration(elapsed)}{eta_text} "
            f"{status_details} [{interval_desc}]"
        )

    async def _mark_book_as_converted(self, barcode: str) -> None:
        """Mark a book as converted in our database after successful download."""
        try:
            # Use batched status change
            status_updates = [collect_status(barcode, "processing_request", "converted")]
            await batch_write_status_updates(str(self.db_tracker.db_path), status_updates)
        except Exception as e:
            logger.warning(f"[{barcode}] Failed to mark as converted: {e}")

    async def get_sync_status(self) -> dict:
        """Get current sync status and statistics."""
        stats = await self.db_tracker.get_sync_stats(self.storage_protocol)

        # Update enrichment queue size in session stats
        if self.enrichment_queue is not None:
            self.stats["enrichment_queue_size"] = self.enrichment_queue.qsize()

        return {
            **stats,
            "session_stats": self.stats,
        }

    async def enrichment_worker(self, worker_id: int = 0) -> None:
        """Background worker for enriching books from the enrichment queue."""
        if self.enrichment_queue is None:
            logger.warning(f"Enrichment worker {worker_id} started but enrichment is disabled")
            return

        worker_name = f"enrichment-worker-{worker_id}"
        logger.info(f"Starting {worker_name}")

        # Import enrichment components

        # Create shared rate limiter (5 QPS across all workers)
        rate_limiter = RateLimiter(requests_per_second=5.0)

        # Create enrichment pipeline for this worker
        enrichment_pipeline = GRINEnrichmentPipeline(
            directory=self.library_directory,
            db_path=self.db_path,
            rate_limit_delay=GRIN_RATE_LIMIT_DELAY,
            batch_size=100,  # Smaller batches for background processing
            max_concurrent_requests=1,  # Conservative for background work
            secrets_dir=self.secrets_dir,
            process_summary_stage=self.process_summary_stage,
        )

        try:
            while not self._shutdown_requested:
                try:
                    # Wait for a book to process (with timeout to check for shutdown)
                    try:
                        barcode = await asyncio.wait_for(self.enrichment_queue.get(), timeout=1.0)
                    except TimeoutError:
                        continue  # Check shutdown and continue waiting

                    logger.debug(f"[{worker_name}] Processing {barcode}")

                    # Collect enrichment status updates for batching
                    enrichment_status_updates = [
                        collect_status(barcode, "enrichment", "in_progress", metadata={"worker_id": worker_id})
                    ]

                    # Apply rate limiting
                    await rate_limiter.acquire()

                    # Enrich the book
                    try:
                        enrichment_results = await enrichment_pipeline.enrich_books_batch([barcode])

                        if enrichment_results > 0:
                            # Successfully enriched
                            enrichment_status_updates.append(
                                collect_status(barcode, "enrichment", "completed", metadata={"worker_id": worker_id})
                            )
                            logger.info(f"[{worker_name}] ✅ Enriched {barcode}")
                        else:
                            # No enrichment data found, but mark as processed
                            enrichment_status_updates.append(
                                collect_status(
                                    barcode,
                                    "enrichment",
                                    "completed",
                                    metadata={"worker_id": worker_id, "result": "no_data"},
                                )
                            )
                            logger.debug(f"[{worker_name}] No enrichment data for {barcode}")

                    except Exception as e:
                        # Mark as failed with error details
                        enrichment_status_updates.append(
                            collect_status(
                                barcode, "enrichment", "failed", metadata={"worker_id": worker_id, "error": str(e)}
                            )
                        )
                        logger.error(f"[{worker_name}] ❌ Failed to enrich {barcode}: {e}")

                    # Batch write all enrichment status updates
                    try:
                        await batch_write_status_updates(str(self.db_tracker.db_path), enrichment_status_updates)
                    except Exception as status_error:
                        logger.warning(
                            f"[{worker_name}] Failed to write enrichment status for {barcode}: {status_error}"
                        )

                    # Mark queue task as done
                    self.enrichment_queue.task_done()

                except asyncio.CancelledError:
                    logger.info(f"[{worker_name}] Cancelled")
                    break
                except Exception as e:
                    logger.error(f"[{worker_name}] Unexpected error: {e}")
                    await asyncio.sleep(1)  # Brief pause before continuing

        except Exception as e:
            logger.error(f"[{worker_name}] Fatal error: {e}")
        finally:
            # Clean up enrichment pipeline
            try:
                await enrichment_pipeline.cleanup()
            except Exception as e:
                logger.warning(f"[{worker_name}] Error during cleanup: {e}")

            logger.info(f"[{worker_name}] Stopped")

    async def start_enrichment_workers(self) -> None:
        """Start background enrichment workers."""
        if not self.enrichment_enabled or self.enrichment_queue is None:
            logger.debug("Enrichment disabled, not starting workers")
            return

        logger.info(f"Starting {self.enrichment_workers} enrichment {pluralize(self.enrichment_workers, 'worker')}")

        for worker_id in range(self.enrichment_workers):
            worker_task = asyncio.create_task(self.enrichment_worker(worker_id))
            self._enrichment_workers.append(worker_task)

    async def stop_enrichment_workers(self) -> None:
        """Stop background enrichment workers gracefully."""
        if not self._enrichment_workers:
            return

        logger.info(f"Stopping {len(self._enrichment_workers)} enrichment workers")

        # Cancel all workers
        for worker_task in self._enrichment_workers:
            worker_task.cancel()

        # Wait for all workers to finish
        if self._enrichment_workers:
            await asyncio.gather(*self._enrichment_workers, return_exceptions=True)

        self._enrichment_workers.clear()
        logger.info("All enrichment workers stopped")

    async def _export_csv_if_enabled(self) -> CSVExportResult:
        """Export CSV if enabled."""
        if self.skip_csv_export:
            logger.debug("CSV export skipped due to --skip-csv-export flag")
            return {"status": "skipped", "file_size": 0, "num_rows": 0, "export_time": 0.0}

        try:
            # Export CSV
            logger.info("Exporting CSV after sync completion")

            if self.storage_protocol == "local":
                # For local storage, write directly to final location
                result = await export_csv_local(self.book_manager, self.db_path)
            else:
                result = await export_and_upload_csv(
                    db_path=self.db_path,
                    staging_manager=self.staging_manager,  # type: ignore[arg-type]
                    book_manager=self.book_manager,
                    skip_export=False,
                )

            if result["status"] == "completed":
                logger.info(
                    f"CSV export completed successfully in {result['export_time']:.1f}s: "
                    f"{result['num_rows']} rows, {result['file_size']} bytes"
                )
            else:
                logger.error(f"CSV export failed: {result.get('status', 'unknown error')}")

            return result

        except Exception as e:
            logger.error(f"CSV export failed with exception: {e}", exc_info=True)
            return {"status": "failed", "file_size": 0, "num_rows": 0, "export_time": 0.0}

    async def _backup_database_at_start(self) -> None:
        """Create and upload database backup at start of sync process."""
        if self.skip_database_backup:
            logger.debug("Database backup skipped due to --skip-database-backup flag")
            return

        try:
            # Create local backup first
            logger.info("Creating database backup before sync...")
            local_backup_result = await create_local_database_backup(self.db_path)

            if local_backup_result["status"] == "completed":
                logger.info(f"Local backup created: {local_backup_result['backup_filename']}")

                # Upload timestamped backup
                upload_result = await upload_database_to_storage(
                    self.db_path,
                    self.book_manager,
                    self.staging_manager if hasattr(self, "staging_manager") else None,
                    upload_type="timestamped",
                )

                if upload_result["status"] == "completed":
                    logger.info(f"Database backup uploaded: {upload_result['backup_filename']}")

                    # Clean up local backup after successful upload if using block storage
                    if self.uses_block_storage and local_backup_result["backup_filename"] is not None:
                        await self._cleanup_local_backup(local_backup_result["backup_filename"])
                else:
                    logger.warning(f"Database backup upload failed: {upload_result['status']}")

            else:
                logger.warning(f"Local database backup failed: {local_backup_result['status']}")

        except Exception as e:
            logger.error(f"Database backup process failed: {e}", exc_info=True)

    async def _cleanup_local_backup(self, backup_filename: str) -> None:
        """Remove local database backup file after successful upload to block storage."""
        try:
            backup_file = Path(self.db_path).parent / "backups" / backup_filename
            backup_file.unlink(missing_ok=True)
        except Exception as e:
            logger.error(f"Failed to cleanup local backup {backup_filename}: {e}", exc_info=True)

    async def _upload_latest_database(self):
        """Upload current database state as 'latest' version."""
        if self.skip_database_backup:
            logger.debug("Database upload skipped due to --skip-database-backup flag")
            return {"status": "skipped", "file_size": 0, "backup_time": 0.0, "backup_filename": None}

        try:
            logger.info("Uploading current database as latest version...")

            # Upload latest database
            upload_result = await upload_database_to_storage(
                self.db_path,
                self.book_manager,
                self.staging_manager if hasattr(self, "staging_manager") else None,
                upload_type="latest",
            )

            if upload_result["status"] == "completed":
                logger.info(f"Latest database uploaded: {upload_result['backup_filename']}")
                return upload_result
            else:
                logger.warning(f"Latest database upload failed: {upload_result['status']}")
                return upload_result

        except Exception as e:
            logger.error(f"Latest database upload failed: {e}", exc_info=True)
            return {"status": "failed", "file_size": 0, "backup_time": 0.0, "backup_filename": None}

    async def _export_csv_and_upload_database_result(self) -> None:
        """Export CSV if enabled and upload database, print results to console."""
        csv_result = await self._export_csv_if_enabled()
        if csv_result["status"] == "completed":
            print(f"  CSV exported: {csv_result['num_rows']:,} rows ({csv_result['file_size']:,} bytes)")
        elif csv_result["status"] == "failed":
            print(f"  CSV export failed: {csv_result.get('error', 'unknown error')}")

        db_result = await self._upload_latest_database()
        if db_result and db_result["status"] == "completed":
            print(f"  Database backed up: {db_result['backup_filename']} ({db_result['file_size']:,} bytes)")
        elif db_result and db_result["status"] == "failed":
            print(f"  Database backup failed: {db_result.get('error', 'unknown error')}")

    async def queue_book_for_enrichment(self, barcode: str) -> None:
        """Add a book to the enrichment queue."""
        if self.enrichment_queue is None:
            logger.debug(f"Enrichment disabled, not queueing {barcode}")
            return

        # Add to queue and mark as pending
        try:
            await self.enrichment_queue.put(barcode)
            # Use batched status change
            status_updates = [collect_status(barcode, "enrichment", "pending")]
            await batch_write_status_updates(str(self.db_tracker.db_path), status_updates)
            logger.debug(f"Queued {barcode} for enrichment")
        except Exception as e:
            logger.error(f"Failed to queue {barcode} for enrichment: {e}")

    async def _print_final_stats_and_outputs(self, total_elapsed: float, books_processed: int) -> None:
        """Print comprehensive final statistics and output locations."""
        run_name = Path(self.db_path).parent.name

        print(f"\nSync completed for run: {run_name}")
        print(f"  Runtime: {format_duration(total_elapsed)}")
        print(f"  Books processed: {books_processed:,}")
        print(f"  Successfully synced: {self.stats['synced']:,}")
        print(f"  Failed: {self.stats['failed']:,}")

        # Show detailed breakdown of results
        if self.stats.get("conversion_requested", 0) > 0:
            print(f"  Conversion requested: {self.stats['conversion_requested']:,}")
        if self.stats.get("marked_unavailable", 0) > 0:
            print(f"  Marked unavailable: {self.stats['marked_unavailable']:,}")
        if self.stats.get("skipped_etag_match", 0) > 0:
            print(f"  Skipped (ETag match): {self.stats['skipped_etag_match']:,}")
        if self.stats.get("skipped_conversion_limit", 0) > 0:
            print(f"  Skipped (conversion limit): {self.stats['skipped_conversion_limit']:,}")

        if total_elapsed > 0 and self.stats["synced"] > 0:
            avg_rate = self.stats["synced"] / total_elapsed
            print(f"  Average rate: {avg_rate:.1f} books/second")

        # Export CSV and database
        await self._export_csv_and_upload_database_result()
        # Point to process summary file
        process_summary_path = f"output/{run_name}/process_summary.json"
        print(f"  Process summary: {process_summary_path}")
        # Print storage locations
        print("\nOutput locations:")

        if self.storage_protocol == "local":
            base_path = self.storage_config.get("base_path", "")
            print(f"  Raw data: {base_path}/raw/")
            print(f"  Metadata: {base_path}/meta/")
            print(f"  Full-text: {base_path}/full/")
            print(f"  CSV export: {base_path}/meta/books_latest.csv.gz")
        else:
            print(f"  Raw data bucket: {self.storage_config['bucket_raw']}")
            print(f"  Metadata bucket: {self.storage_config['bucket_meta']}")
            print(f"  Full-text bucket: {self.storage_config['bucket_full']}")
            print(f"  CSV export: {self.storage_config['bucket_meta']}/books_latest.csv.gz")

    async def _cancel_progress_reporter(self) -> None:
        """Cancel the background progress reporter with timeout."""
        if hasattr(self, "_progress_reporter_task") and not self._progress_reporter_task.done():
            self._progress_reporter_task.cancel()
            try:
                # Wait for cancellation with timeout to prevent hanging
                await asyncio.wait_for(self._progress_reporter_task, timeout=2.0)
            except (TimeoutError, asyncio.CancelledError):
                pass

    async def _download_book(self, barcode: str) -> dict[str, Any]:
        """Download a book"""
        if self.staging_manager:
            await self.staging_manager.wait_for_disk_space()
            logger.debug(f"[{barcode}] Disk space check passed, proceeding with download")

        async with self._download_semaphore:
            self._active_download_count += 1
            self._has_started_work = True
            try:
                logger.debug(
                    f"[{barcode}] Download task started "
                    f"(active: {self._active_download_count}/{self.concurrent_downloads})"
                )

                # Check ETag and handle skip scenario
                skip_result, encrypted_etag, _, sync_status_updates = await check_and_handle_etag_skip(
                    barcode,
                    self.grin_client,
                    self.library_directory,
                    self.storage_type,
                    self.storage_config,
                    self.db_tracker,
                    self._download_semaphore,
                    self.force,
                    self.current_queues,
                    self.conversion_handler,
                )

                if skip_result:
                    # Write sync status updates for books that don't need download
                    if sync_status_updates and self.db_tracker:
                        try:
                            await batch_write_status_updates(str(self.db_tracker.db_path), sync_status_updates)
                        except Exception as e:
                            logger.warning(f"[{barcode}] Failed to write status updates: {e}")

                    # Check the first status update to determine the result type
                    status_value = sync_status_updates[0].status_value if sync_status_updates else "unknown"
                    conversion_status = sync_status_updates[0].metadata.get("conversion_status") if sync_status_updates and sync_status_updates[0].metadata else None

                    if status_value == "completed" and conversion_status == "requested":
                        self.stats["conversion_requested"] += 1
                        return {"barcode": barcode, "download_success": False, "completed": True, "conversion_requested": True}
                    elif status_value == "completed" and conversion_status == "in_process":
                        self.stats["conversion_requested"] += 1  # Count in_process as conversion_requested in stats
                        return {"barcode": barcode, "download_success": False, "completed": True, "already_in_process": True}
                    elif status_value == "marked_unavailable":
                        self.stats["marked_unavailable"] += 1
                        return {"barcode": barcode, "download_success": False, "marked_unavailable": True}
                    elif status_value == "skipped" and sync_status_updates[0].metadata and sync_status_updates[0].metadata.get("skip_reason") == "conversion_limit_reached":
                        self.stats["skipped_conversion_limit"] += 1
                        self.stats["skipped"] += 1
                        return {"barcode": barcode, "download_success": False, "skipped": True, "conversion_limit_reached": True}
                    else:
                        # Default ETag match or other skip
                        self.stats["skipped_etag_match"] += 1
                        self.stats["skipped"] += 1
                        return {"barcode": barcode, "download_success": False, "skipped": True, "skip_result": skip_result}

                # We didn't skip, so do the download to either staging (if cloud storage) or local
                if self.storage_protocol == "local":
                    _, staging_file_path, metadata = await download_book_to_local(
                        barcode,
                        self.grin_client,
                        self.library_directory,
                        self.storage_config,
                        self.db_tracker,
                        None,  # No ETag for initial call
                        self.secrets_dir,
                        self.skip_extract_ocr,
                        self.skip_extract_marc,
                        self.download_timeout,
                        self.download_retries,
                    )

                else:
                    _, staging_file_path, metadata = await download_book_to_filesystem(
                        barcode,
                        self.grin_client,
                        self.library_directory,
                        encrypted_etag,
                        self.staging_manager,
                        self.secrets_dir,
                        self.download_timeout,
                        self.download_retries,
                    )

                return {
                    "barcode": barcode,
                    "download_success": True,
                    "staging_file_path": staging_file_path,
                    "encrypted_etag": encrypted_etag,
                    "metadata": metadata,
                }

            except Exception as e:
                # Check if this is a 404 error
                is_404 = is_404_error(e)

                if is_404:
                    logger.info(f"[{barcode}] Archive not found (404)")
                else:
                    logger.error(f"[{barcode}] Download failed: {e}", exc_info=True)
                return build_download_result(barcode, success=False, error=str(e), is_404=is_404)
            finally:
                self._active_download_count -= 1
                logger.info(
                    f"[{barcode}] Download task completed "
                    f"(active: {self._active_download_count}/{self.concurrent_downloads})"
                )

    async def _upload_book_from_staging(self, barcode: str, download_result: dict[str, Any]) -> dict[str, Any]:
        """Upload a book from staging directory to storage."""
        async with self._upload_semaphore:
            self._active_upload_count += 1
            try:
                logger.debug(
                    f"[{barcode}] Upload task started (active: {self._active_upload_count}/{self.concurrent_uploads})"
                )

                upload_result = await upload_book_from_staging(
                    barcode,
                    download_result["staging_file_path"],
                    self.storage_type,
                    self.storage_config,
                    self.staging_manager,
                    self.db_tracker,
                    download_result.get("encrypted_etag"),
                    self.secrets_dir,
                    self.skip_extract_ocr,
                    self.skip_extract_marc,
                    self.skip_staging_cleanup,
                )

                return {
                    "barcode": barcode,
                    "upload_success": upload_result.get("status") == "completed",
                    "result": upload_result,
                }

            except Exception as e:
                logger.error(f"[{barcode}] Upload failed: {e}", exc_info=True)
                return {"barcode": barcode, "upload_success": False, "error": str(e)}
            finally:
                self._active_upload_count -= 1
                logger.debug(
                    f"[{barcode}] Upload task completed (active: {self._active_upload_count}/{self.concurrent_uploads})"
                )

    async def setup_sync_loop(
        self, queues: list[str] | None = None, limit: int | None = None, specific_barcodes: list[str] | None = None
    ) -> None:
        """Run the complete sync pipeline.

        Args:
            limit: Optional limit on number of books to sync
            specific_barcodes: Optional list of specific barcodes to sync
            queues: List of queue types to process (converted, previous, changed, all)
        """
        # Store current queues for use in conversion request handling
        self.current_queues = queues or []

        # Initialize conversion handler if processing previous queue
        if "previous" in self.current_queues:
            self.conversion_handler = ConversionRequestHandler(
                library_directory=self.library_directory, db_tracker=self.db_tracker, secrets_dir=self.secrets_dir
            )
        print("Starting GRIN-to-Storage sync pipeline")
        if self.dry_run:
            print("🔍 DRY-RUN MODE: No files will be downloaded or uploaded")
        print(f"Database: {self.db_path}")

        # Display storage configuration details
        if self.storage_type == "local":
            base_path = self.storage_config.get("base_path")
            print(f"Storage: Local filesystem at {base_path or 'None'}")
        elif self.storage_type in ["s3", "r2", "minio"]:
            storage_names = {"s3": "AWS S3", "r2": "Cloudflare R2", "minio": "MinIO"}
            storage_name = storage_names[self.storage_type]

            if self.storage_type == "minio":
                endpoint = self.storage_config.get("endpoint_url", "unknown endpoint")
                print(f"Storage: {storage_name} at {endpoint}")
            else:
                print(f"Storage: {storage_name}")

            print(f"  Raw bucket: {self.storage_config.get('bucket_raw', 'unknown')}")
            print(f"  Meta bucket: {self.storage_config.get('bucket_meta', 'unknown')}")
            print(f"  Full bucket: {self.storage_config.get('bucket_full', 'unknown')}")
        else:
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

        # Create and upload database backup before starting sync (skip in dry-run)
        if not self.dry_run:
            await self._backup_database_at_start()

        sync_successful = False
        try:
            # Process queues to get available books (unless specific barcodes are provided)
            all_available_books: set[str] = set()

            if specific_barcodes:
                # When specific barcodes are provided, skip queue processing
                print(
                    f"Processing specific barcodes: {', '.join(specific_barcodes[:5])}{'...' if len(specific_barcodes) > 5 else ''}"
                )
                all_available_books.update(specific_barcodes)
            elif queues:
                # Queue-based processing
                print(f"Processing queues: {', '.join(queues)}")

                for queue_name in queues:
                    print(f"Fetching books from '{queue_name}' queue...")
                    queue_books = await get_books_from_queue(
                        self.grin_client, self.library_directory, queue_name, self.db_tracker
                    )

                    if len(queue_books) == 0:
                        print(f"  Warning: '{queue_name}' queue reports no books available")
                    else:
                        print(f"  '{queue_name}' queue: {len(queue_books):,} books available")

                    all_available_books.update(queue_books)
            else:
                # This should not happen due to validation, but handle it gracefully
                raise ValueError("Either queues or specific_barcodes must be provided")

            # Get initial status for reporting
            initial_status = await self.get_sync_status()
            total_converted = initial_status["total_converted"]
            already_synced = initial_status["synced"]
            failed_count = initial_status["failed"]
            pending_count = initial_status["pending"]

            print(
                f"Database sync status: {total_converted:,} total, {already_synced:,} synced, "
                f"{failed_count:,} failed, {pending_count:,} pending"
            )

            # Start enrichment workers if enabled
            if self.enrichment_enabled:
                print(
                    f"Starting {self.enrichment_workers} enrichment "
                    f"{pluralize(self.enrichment_workers, 'worker')} for background processing"
                )
                await self.start_enrichment_workers()

            # Determine books to sync
            if specific_barcodes:
                # When specific barcodes are provided, use them directly without database filtering
                available_to_sync = specific_barcodes
            else:
                # Standard mode: filter available books by those that need syncing
                available_to_sync = await self.db_tracker.get_books_for_sync(
                    storage_type=self.storage_protocol,
                    converted_barcodes=all_available_books,
                )

            print(f"Found {len(available_to_sync):,} books that need syncing")

            # Handle dry-run mode
            if self.dry_run:
                await self._show_dry_run_preview(available_to_sync, limit, specific_barcodes)
                sync_successful = True
                return

            if not available_to_sync:
                if len(all_available_books) == 0:
                    print("No books available from any queue")
                else:
                    print("No books found that need syncing (all may already be synced)")

                # Report on pending books
                pending_books = await self.db_tracker.get_books_for_sync(
                    storage_type=self.storage_protocol,
                    limit=999999,
                    converted_barcodes=None,  # Get all requested books regardless of conversion
                )

                if pending_books:
                    print("Status summary:")
                    print(f"  - {len(pending_books):,} books requested for processing but not yet converted")
                    print(f"  - {len(all_available_books):,} books available from queues")
                    print("  - 0 books ready to sync (no overlap between requested and available)")
                    print(
                        f"\nTip: Use 'python grin.py process monitor --run-name "
                        f"{Path(self.db_path).parent.name}' to check processing progress"
                    )

                sync_successful = True  # No books to process is considered successful
                return

            # Set up progress tracking
            books_to_process = min(limit or len(available_to_sync), len(available_to_sync))

            print(f"Starting sync of {books_to_process:,} books...")
            print(f"{self.concurrent_downloads} concurrent downloads")

            if self.storage_protocol != "local":
                print(f"{self.concurrent_uploads} uploads")

            print(
                f"Progress updates will be shown every {REGULAR_PROGRESS_INTERVAL // 60} minutes "
                f"(more frequent initially)"
            )
            print("---")

            await self._run_sync(available_to_sync, books_to_process, specific_barcodes)
            sync_successful = True

        except KeyboardInterrupt:
            print("\nOperation cancelled by user")
        except Exception as e:
            print(f"Pipeline failed: {e}")
            logger.error(f"Pipeline failed: {e}", exc_info=True)
        finally:
            await self.cleanup(sync_successful)

    def _should_exit_for_failure_limit(self) -> bool:
        """Check if pipeline should exit due to sequential failure limit."""
        return self._sequential_failures >= self.max_sequential_failures

    def _should_exit_for_shutdown(self) -> bool:
        """Check if pipeline should exit due to shutdown request."""
        return self._shutdown_requested

    async def _process_download_result(self, barcode: str, result: dict[str, Any], processed_count: int, rate_calculator, start_time: float) -> tuple[bool, int]:
        """Process download completion result and return (should_exit, updated_processed_count)."""
        if result.get("download_success"):
            # For local storage, successful downloads should be treated as completed
            if self.storage_protocol == "local":
                self._completed_count += 1
                self.stats["synced"] += 1
                self._handle_success(barcode)
                await self.queue_book_for_enrichment(barcode)
                self.process_summary_stage.increment_items(successful=1)
            # For cloud storage, downloads get uploaded separately
            processed_count += 1
            self._processed_count = processed_count
            rate_calculator.add_batch(time.time(), processed_count)
            self.process_summary_stage.increment_items(processed=1)
            return False, processed_count

        if result.get("completed"):
            self._completed_count += 1
            self._handle_success(barcode)

            if result.get("conversion_requested"):
                logger.info(f"[{barcode}] Conversion requested successfully")
                print(f"✅ [{barcode}] Conversion requested")
            elif result.get("already_in_process"):
                logger.info(f"[{barcode}] Already being processed")
                print(f"✅ [{barcode}] Already in process")
            elif result.get("marked_unavailable"):
                logger.info(f"[{barcode}] Marked as unavailable")
                print(f"✅ [{barcode}] Marked unavailable")

            self.process_summary_stage.increment_items(successful=1)
        elif result.get("skipped"):
            self._completed_count += 1
            self._handle_success(barcode)

            if result.get("conversion_limit_reached"):
                logger.warning(f"[{barcode}] Conversion limit reached")
                print(f"⚠️  [{barcode}] Conversion limit reached")
            else:
                logger.info(f"[{barcode}] Skipped (ETag match)")

            self.process_summary_stage.increment_items(successful=1)
        else:
            self._completed_count += 1

            if result.get("is_404"):
                self.stats["failed"] += 1

                if result.get("conversion_requested"):
                    logger.info(f"[{barcode}] Archive not found, conversion requested")
                    print(f"✓ [{barcode}] Archive not found, conversion requested")
                elif result.get("conversion_limit_reached"):
                    logger.error(f"[{barcode}] Failed: Archive not found, conversion request limit reached")
                    print(f"❌ [{barcode}] Failed: Archive not found, conversion request limit reached")
                else:
                    logger.error(f"[{barcode}] Failed: Archive not found (404)")
                    print(f"❌ [{barcode}] Failed: Archive not found (404)")
            else:
                error_detail = result.get("error", "Unknown error")
                should_exit = self._handle_failure(barcode, f"Download failed: {error_detail}")
                if should_exit:
                    return True, processed_count

            self.process_summary_stage.increment_items(failed=1)

        processed_count += 1
        self._processed_count = processed_count
        rate_calculator.add_batch(time.time(), processed_count)
        self.process_summary_stage.increment_items(processed=1)

        return False, processed_count

    async def _process_upload_result(self, barcode: str, result: dict[str, Any], processed_count: int, rate_calculator) -> tuple[bool, int]:
        """Process upload completion result and return should_exit."""
        self._completed_count += 1
        logger.info(f"[{barcode}] Upload completed (success: {result.get('upload_success', False)})")

        if result.get("upload_success"):
            self.stats["synced"] += 1
            self._handle_success(barcode)
            await self.queue_book_for_enrichment(barcode)
            self.process_summary_stage.increment_items(successful=1)
        else:
            error_detail = result.get("error", "Unknown error")
            should_exit = self._handle_failure(barcode, f"Upload failed: {error_detail}")
            if should_exit:
                return True, processed_count
            self.process_summary_stage.increment_items(failed=1)

        processed_count += 1
        self._processed_count = processed_count
        rate_calculator.add_batch(time.time(), processed_count)
        self.process_summary_stage.increment_items(processed=1)

        return False, processed_count

    async def _run_sync(self, available_to_sync: list[str], books_to_process: int, specific_barcodes: list[str] | None = None
) -> None:

        """Run sync pipeline"""
        start_time = time.time()
        processed_count = 0
        active_downloads: dict[str, asyncio.Task] = {}
        active_uploads: dict[str, asyncio.Task] = {}

        # Initialize sliding window rate calculator
        rate_calculator = SlidingWindowRateCalculator(window_size=20)

        # Start background progress reporter
        self._progress_reporter_task = asyncio.create_task(
            self._background_progress_reporter(start_time, books_to_process, rate_calculator)
        )

        try:
            # Create iterator for books
            book_iter = iter(available_to_sync[:books_to_process])

            # Process downloads and uploads
            while len(active_downloads) < self.concurrent_downloads or active_downloads or active_uploads:
                if self._should_exit_for_shutdown():
                    break

                # Refill download queue if under limit and more books available
                while len(active_downloads) < self.concurrent_downloads:
                    try:
                        barcode = next(book_iter)
                        if specific_barcodes is None or barcode in specific_barcodes:
                            task = asyncio.create_task(self._download_book(barcode))
                            active_downloads[barcode] = task
                            logger.debug(
                                f"Created new download task for {barcode} "
                                f"(queue: {len(active_downloads)}/{self.concurrent_downloads})"
                            )
                    except StopIteration:
                        break

                all_tasks = list(active_downloads.values()) + list(active_uploads.values())
                if not all_tasks:
                    break

                done, _ = await asyncio.wait(all_tasks, return_when=asyncio.FIRST_COMPLETED)

                for completed_task in done:
                    try:
                        result = await completed_task
                        if not result or not result.get("barcode"):
                            logger.warning("Upload/download task completed with no result")
                            continue

                        barcode = result.get("barcode")

                        # Handle download completion
                        if barcode in active_downloads and active_downloads[barcode] == completed_task:
                            del active_downloads[barcode]
                            logger.debug(f"[{barcode}] Download completed (success: {result.get('download_success', False)})")

                            if result.get("download_success") and self.storage_protocol != "local":
                                upload_task = asyncio.create_task(self._upload_book_from_staging(barcode, result))
                                active_uploads[barcode] = upload_task
                                self._pending_upload_count = len(active_uploads)
                                logger.debug(f"[{barcode}] Started upload task")
                            else:
                                should_exit, processed_count = await self._process_download_result(
                                    barcode, result, processed_count, rate_calculator, start_time
                                )
                                if should_exit:
                                    return

                        # Handle upload completion
                        elif barcode in active_uploads and active_uploads[barcode] == completed_task:
                            del active_uploads[barcode]
                            self._pending_upload_count = len(active_uploads)

                            should_exit, processed_count = await self._process_upload_result(barcode, result, processed_count, rate_calculator)
                            if should_exit:
                                return

                    except Exception as e:
                        logger.error(f"Error processing completed task: {e}", exc_info=True)
                        should_exit = self._handle_failure("unknown", f"Task processing exception: {e}")
                        if should_exit:
                            return

                        self.process_summary_stage.increment_items(failed=1)
                        self.process_summary_stage.add_error(type(e).__name__, str(e))

            # Main loop completed - stop the background progress reporter
            self._shutdown_requested = True

        finally:
            # Ensure shutdown is requested
            self._shutdown_requested = True

            # Cancel progress reporter
            await self._cancel_progress_reporter()

            # Cancel remaining tasks
            for task in active_downloads.values():
                if not task.done():
                    task.cancel()
            for task in active_uploads.values():
                if not task.done():
                    task.cancel()

            # Wait for cancellations
            all_tasks = list(active_downloads.values()) + list(active_uploads.values())
            if all_tasks:
                await asyncio.gather(*all_tasks, return_exceptions=True)

            # Final statistics and outputs
            total_elapsed = time.time() - start_time
            await self._print_final_stats_and_outputs(total_elapsed, books_to_process)

            logger.info("Sync completed")

    async def _show_dry_run_preview(
        self, available_to_sync: list[str], limit: int | None, specific_barcodes: list[str] | None
    ) -> None:
        """Show what would be processed in dry-run mode without actually doing it."""
        books_to_process = min(limit or len(available_to_sync), len(available_to_sync))

        print(f"\n{'=' * 60}")
        print("DRY-RUN PREVIEW: Books that would be processed")
        print(f"{'=' * 60}")

        if books_to_process == 0:
            print("No books would be processed.")
            return

        print(f"Total books that would be processed: {books_to_process:,}")
        print(f"Storage type: {self.storage_type}")
        print(f"Concurrent downloads: {self.concurrent_downloads}")
        print(f"Concurrent uploads: {self.concurrent_uploads}")
        print(f"Batch size: {self.batch_size}")

        if specific_barcodes:
            print(f"Filtered to specific barcodes: {len(specific_barcodes) if specific_barcodes else 0}")

        if limit and limit < len(available_to_sync):
            print(f"Limited to first {limit:,} books")

        print(f"\nAll {books_to_process} books that would be processed:")
        print("-" * 60)

        # Get book records for all barcodes to show titles
        try:
            for i, barcode in enumerate(available_to_sync[:books_to_process]):
                book = await self.db_tracker.get_book(barcode)
                if book and book.title:
                    title = book.title
                else:
                    title = "Unknown Title"

                print(f"{i + 1:3d}. {barcode} - {title}")
        except Exception:
            # Fallback if we can't get book records
            for i, barcode in enumerate(available_to_sync[:books_to_process]):
                print(f"{i + 1:3d}. {barcode} - Unknown Title")

        print("-" * 60)
        print("Operations that would be performed per book:")
        print("  1. Download encrypted archive from GRIN")
        print("  2. Decrypt and extract archive")
        if not self.skip_extract_ocr:
            print("  3. Extract OCR text to JSON/JSONL")
        if not self.skip_extract_marc:
            print("  4. Extract MARC metadata from METS XML")
        print("  5. Upload decrypted archive to storage")
        print("  6. Upload extracted files to storage")
        if self.enrichment_enabled:
            print("  7. Queue for background enrichment")
        if not self.skip_csv_export:
            print("  8. Update CSV export")
        if not self.skip_staging_cleanup:
            print("  9. Clean up staging files")

        print("\nDRY-RUN COMPLETE: No actual processing performed")
        print(f"{'=' * 60}")

        logger.info(f"DRY-RUN: Would process {books_to_process} books with storage type {self.storage_type}")

    async def _handle_404_with_conversion(self, barcode: str) -> dict[str, Any] | None:
        """Handle 404 error with possible conversion request for previous queue.

        Args:
            barcode: Book barcode that returned 404

        Returns:
            Result dict if conversion was attempted, None if no conversion needed
        """
        # Only attempt conversion for previous queue
        if "previous" not in self.current_queues or self.conversion_handler is None:
            return None

        try:
            logger.info(f"[{barcode}] Archive not found (404), requesting conversion for previous queue")
            conversion_status = await self.conversion_handler.handle_missing_archive(
                barcode, self.conversion_request_limit
            )

            if conversion_status == "requested":
                logger.info(f"[{barcode}] Conversion requested successfully")
                return build_download_result(
                    barcode,
                    success=False,
                    error="Archive not found, conversion requested",
                    is_404=True,
                    conversion_requested=True,
                )
            elif conversion_status == "limit_reached":
                logger.warning(f"[{barcode}] Conversion request limit reached")
                return build_download_result(
                    barcode,
                    success=False,
                    error="Archive not found, conversion request limit reached",
                    is_404=True,
                    conversion_limit_reached=True,
                )
            else:
                # unavailable or in_process
                logger.warning(f"[{barcode}] Conversion not possible: {conversion_status}")
                return None

        except Exception as conversion_error:
            logger.error(f"[{barcode}] Conversion request failed: {conversion_error}")
            return None

