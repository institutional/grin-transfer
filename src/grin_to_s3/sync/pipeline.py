#!/usr/bin/env python3
"""
Sync Pipeline Orchestration

Main pipeline orchestration for syncing books from GRIN to storage.
"""

import asyncio
import csv
import logging
import shutil
import time
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

from grin_to_s3.client import GRINClient
from grin_to_s3.collect_books.models import BookRecord, SQLiteProgressTracker
from grin_to_s3.common import (
    ProgressReporter,
    RateLimiter,
    SlidingWindowRateCalculator,
    format_duration,
    pluralize,
)
from grin_to_s3.metadata.grin_enrichment import GRINEnrichmentPipeline
from grin_to_s3.run_config import RunConfig
from grin_to_s3.storage import create_storage_from_config, get_storage_protocol
from grin_to_s3.storage.book_manager import BookManager, BucketConfig
from grin_to_s3.storage.staging import StagingDirectoryManager

from .csv_export import CSVExportResult, export_and_upload_csv
from .models import create_sync_stats
from .operations import (
    check_and_handle_etag_skip,
    download_book_to_staging,
    sync_book_to_local_storage,
    upload_book_from_staging,
)
from .utils import get_converted_books, reset_bucket_cache

logger = logging.getLogger(__name__)

# Progress reporting intervals
INITIAL_PROGRESS_INTERVAL = 60  # 1 minute for first few reports
REGULAR_PROGRESS_INTERVAL = 300  # 5 minutes for subsequent reports


class SyncPipeline:
    """Pipeline for syncing converted books from GRIN to storage with database tracking."""

    @classmethod
    def from_run_config(
        cls,
        config: RunConfig,
        process_summary_stage,
        force: bool = False,
        skip_extract_ocr: bool = False,
        skip_extract_marc: bool = False,
        skip_enrichment: bool = False,
        skip_csv_export: bool = False,
        skip_staging_cleanup: bool = False,
    ) -> "SyncPipeline":
        """Create SyncPipeline from RunConfig.

        Args:
            config: RunConfig containing all pipeline configuration
            process_summary_stage: Process summary stage for tracking
            force: Force re-download even if ETags match
            skip_extract_ocr: Skip OCR text extraction
            skip_extract_marc: Skip MARC metadata extraction
            skip_enrichment: Skip enrichment processing
            skip_csv_export: Skip CSV export after sync
            skip_staging_cleanup: Skip deletion of files in staging directory

        Returns:
            Configured SyncPipeline instance
        """
        return cls(
            config=config,
            process_summary_stage=process_summary_stage,
            force=force,
            skip_extract_ocr=skip_extract_ocr,
            skip_extract_marc=skip_extract_marc,
            skip_enrichment=skip_enrichment,
            skip_csv_export=skip_csv_export,
            skip_staging_cleanup=skip_staging_cleanup,
        )

    def __init__(
        self,
        config: RunConfig,
        process_summary_stage,
        force: bool = False,
        skip_extract_ocr: bool = False,
        skip_extract_marc: bool = False,
        skip_enrichment: bool = False,
        skip_csv_export: bool = False,
        skip_staging_cleanup: bool = False,
    ):
        # Store configuration and runtime parameters
        self.config = config
        self.force = force
        self.skip_extract_ocr = skip_extract_ocr
        self.skip_extract_marc = skip_extract_marc
        self.enrichment_enabled = not skip_enrichment
        self.skip_csv_export = skip_csv_export
        self.skip_staging_cleanup = skip_staging_cleanup
        self.process_summary_stage = process_summary_stage

        # Extract commonly used config values
        self.db_path = config.sqlite_db_path
        self.storage_type = config.storage_type or "local"  # Default to local if None
        self.storage_protocol = get_storage_protocol(self.storage_type)
        self.storage_config = config.storage_config.get("config", {}) if config.storage_config else {}
        self.library_directory = config.library_directory
        self.secrets_dir = config.secrets_dir

        # Sync configuration from RunConfig
        self.concurrent_downloads = config.sync_concurrent_downloads
        self.concurrent_uploads = config.sync_concurrent_uploads
        self.batch_size = config.sync_batch_size
        self.disk_space_threshold = config.sync_disk_space_threshold
        self.enrichment_workers = config.sync_enrichment_workers
        self.gpg_key_file = config.sync_gpg_key_file

        # Configure staging directory
        if config.sync_staging_dir is None:
            # Default to run directory + staging
            run_dir = Path(self.db_path).parent
            self.staging_dir = run_dir / "staging"
        else:
            self.staging_dir = Path(config.sync_staging_dir)

        # Initialize components
        self.db_tracker = SQLiteProgressTracker(self.db_path)
        self.progress_reporter = ProgressReporter("sync", None)
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

        # Simple gate to prevent race conditions in task creation
        self._task_creation_lock = asyncio.Lock()

        # Enrichment queue infrastructure
        if self.enrichment_enabled:
            self.enrichment_queue: asyncio.Queue[str] | None = asyncio.Queue()
        else:
            self.enrichment_queue = None

        # Statistics
        self.stats = create_sync_stats()

        # Worker management
        self._enrichment_workers: list[asyncio.Task] = []

    def _maybe_show_progress(
        self,
        processed_count: int,
        books_to_process: int,
        start_time: float,
        last_progress_report: float,
        initial_reports_count: int,
        max_initial_reports: int,
        rate_calculator,
        active_downloads: dict | None = None,
        active_uploads: dict | None = None,
        active_tasks: dict | None = None,
    ) -> tuple[float, int]:
        """Show detailed progress at intervals. Returns (new_last_report_time, new_initial_count)."""
        current_time = time.time()
        current_interval = (
            INITIAL_PROGRESS_INTERVAL if initial_reports_count < max_initial_reports else REGULAR_PROGRESS_INTERVAL
        )

        if current_time - last_progress_report >= current_interval:
            percentage = (processed_count / books_to_process) * 100
            remaining = books_to_process - processed_count
            elapsed = current_time - start_time

            rate = rate_calculator.get_rate(start_time, processed_count)

            # Show ETA only after enough batches for stable estimate
            eta_text = ""
            if len(rate_calculator.batch_times) >= 3 and rate > 0:
                eta_seconds = remaining / rate
                eta_text = f" (ETA: {format_duration(eta_seconds)})"

            interval_desc = (
                f"{INITIAL_PROGRESS_INTERVAL // 60} min"
                if initial_reports_count < max_initial_reports
                else f"{REGULAR_PROGRESS_INTERVAL // 60} min"
            )

            # Get enrichment status
            enrichment_info = ""
            if self.enrichment_enabled and self.enrichment_queue is not None:
                queue_size = self.enrichment_queue.qsize()
                enrichment_info = f", {queue_size} enrichment queued"

            # Format progress based on storage type
            if active_tasks is not None:  # Local storage
                active_count = len(active_tasks)
                print(
                    f"{processed_count:,}/{books_to_process:,} "
                    f"({percentage:.1f}%) - {rate:.1f} books/sec - "
                    f"elapsed: {format_duration(elapsed)}{eta_text} "
                    f"[{active_count}/{self.concurrent_downloads} active{enrichment_info}] [{interval_desc} update]"
                )
            else:  # Block storage
                downloads_running = self._active_download_count
                uploads_running = min(len(active_uploads or {}), self.concurrent_uploads)
                uploads_queued = len(active_uploads or {}) - uploads_running
                print(
                    f"{processed_count:,}/{books_to_process:,} "
                    f"({percentage:.1f}%) - {rate:.1f} books/sec - "
                    f"elapsed: {format_duration(elapsed)}{eta_text} "
                    f"[{downloads_running}/{self.concurrent_downloads} downloads, "
                    f"{uploads_running}/{self.concurrent_uploads} uploads, "
                    f"{uploads_queued} uploads queued{enrichment_info}] [{interval_desc} update]"
                )

            # Update tracking variables
            new_last_report = current_time
            new_initial_count = (
                initial_reports_count + 1 if initial_reports_count < max_initial_reports else initial_reports_count
            )
            return new_last_report, new_initial_count

        return last_progress_report, initial_reports_count

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
        if sync_successful and not self.skip_staging_cleanup and self.staging_manager is not None:
            try:
                logger.info("Performing final staging directory cleanup...")
                shutil.rmtree(self.staging_manager.staging_path, ignore_errors=True)
                logger.info("Staging directory cleaned up")
            except Exception as e:
                logger.warning(f"Error during final staging cleanup: {e}")
        elif not sync_successful and self.staging_manager is not None:
            logger.info("Staging directory preserved due to sync failure")

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

    async def _mark_book_as_converted(self, barcode: str) -> None:
        """Mark a book as converted in our database after successful download."""
        try:
            # Use atomic status change
            await self.db_tracker.add_status_change(barcode, "processing_request", "converted")
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
            rate_limit_delay=0.2,  # 5 QPS
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

                    # Update status to in_progress
                    await self.db_tracker.add_status_change(
                        barcode, "enrichment", "in_progress", metadata={"worker_id": worker_id}
                    )

                    # Apply rate limiting
                    await rate_limiter.acquire()

                    # Enrich the book
                    try:
                        enrichment_results = await enrichment_pipeline.enrich_books_batch([barcode])

                        if enrichment_results > 0:
                            # Successfully enriched
                            await self.db_tracker.add_status_change(
                                barcode, "enrichment", "completed", metadata={"worker_id": worker_id}
                            )
                            logger.info(f"[{worker_name}] ✅ Enriched {barcode}")
                        else:
                            # No enrichment data found, but mark as processed
                            await self.db_tracker.add_status_change(
                                barcode,
                                "enrichment",
                                "completed",
                                metadata={"worker_id": worker_id, "result": "no_data"},
                            )
                            logger.debug(f"[{worker_name}] No enrichment data for {barcode}")

                    except Exception as e:
                        # Mark as failed with error details
                        await self.db_tracker.add_status_change(
                            barcode, "enrichment", "failed", metadata={"worker_id": worker_id, "error": str(e)}
                        )
                        logger.error(f"[{worker_name}] ❌ Failed to enrich {barcode}: {e}")

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
            # Create book storage for CSV upload

            storage = create_storage_from_config(self.storage_type, self.storage_config or {})
            base_prefix = self.storage_config.get("prefix", "") if self.storage_config else ""

            bucket_config: BucketConfig = {
                "bucket_raw": self.storage_config.get("bucket_raw", "") if self.storage_config else "",
                "bucket_meta": self.storage_config.get("bucket_meta", "") if self.storage_config else "",
                "bucket_full": self.storage_config.get("bucket_full", "") if self.storage_config else "",
            }

            book_storage = BookManager(storage, bucket_config=bucket_config, base_prefix=base_prefix)

            # Export CSV
            logger.info("Exporting CSV after sync completion")

            if self.storage_protocol == "local":
                # For local storage, write directly to final location
                result = await self._export_csv_local(book_storage)
            else:
                # For cloud storage, use staging manager
                assert self.staging_manager is not None, "Staging manager required for cloud storage"
                result = await export_and_upload_csv(
                    db_path=self.db_path,
                    staging_manager=self.staging_manager,
                    book_storage=book_storage,
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

    async def _export_csv_and_print_result(self) -> None:
        """Export CSV if enabled and print result to console."""
        csv_result = await self._export_csv_if_enabled()
        if csv_result["status"] == "completed":
            print(f"  CSV exported: {csv_result['num_rows']:,} rows ({csv_result['file_size']:,} bytes)")
        elif csv_result["status"] == "failed":
            print(f"  CSV export failed: {csv_result.get('error', 'unknown error')}")

    async def _export_csv_local(self, book_storage) -> CSVExportResult:
        """Export CSV directly to local storage without temporary files."""
        start_time = time.time()
        try:
            # Get database data

            sqlite_tracker = SQLiteProgressTracker(self.db_path)
            books = await sqlite_tracker.get_all_books_csv_data()
            logger.info(f"Exporting {len(books)} books to local CSV")

            # Get final CSV paths directly from book storage
            timestamp = datetime.now(UTC).strftime("%Y%m%d_%H%M%S")
            latest_path = book_storage._meta_path("books_latest.csv")
            timestamped_path = book_storage._meta_path(f"timestamped/books_{timestamp}.csv")

            # Ensure directories exist
            latest_path = Path(latest_path)
            timestamped_path = Path(timestamped_path)

            latest_path.parent.mkdir(parents=True, exist_ok=True)
            timestamped_path.parent.mkdir(parents=True, exist_ok=True)

            # Write directly to latest CSV file
            with open(latest_path, "w", encoding="utf-8", newline="") as f:
                writer = csv.writer(f)
                writer.writerow(BookRecord.csv_headers())
                for book in books:
                    writer.writerow(book.to_csv_row())

            # Copy to timestamped version
            shutil.copy2(latest_path, timestamped_path)

            # Get file size
            file_size = latest_path.stat().st_size
            num_rows = len(books) + 1  # Include header

            logger.info(f"CSV written directly to {latest_path}")
            logger.info(f"CSV timestamped copy at {timestamped_path}")

            return {
                "status": "completed",
                "num_rows": num_rows,
                "file_size": file_size,
                "export_time": time.time() - start_time,
            }

        except Exception as e:
            logger.error(f"Local CSV export failed: {e}", exc_info=True)
            return {"status": "failed", "file_size": 0, "num_rows": 0, "export_time": time.time() - start_time}

    async def queue_book_for_enrichment(self, barcode: str) -> None:
        """Add a book to the enrichment queue."""
        if self.enrichment_queue is None:
            logger.debug(f"Enrichment disabled, not queueing {barcode}")
            return

        # Add to queue and mark as pending
        try:
            await self.enrichment_queue.put(barcode)
            await self.db_tracker.add_status_change(barcode, "enrichment", "pending")
            logger.debug(f"Queued {barcode} for enrichment")
        except Exception as e:
            logger.error(f"Failed to queue {barcode} for enrichment: {e}")

    async def _run_local_storage_sync(
        self, available_to_sync: list[str], books_to_process: int, specific_barcodes: list[str] | None = None
    ) -> None:
        """Run sync pipeline for local storage with direct processing."""
        print("Using optimized local storage sync (no staging directory)")
        print("---")

        start_time = time.time()
        processed_count = 0
        active_tasks: dict[str, asyncio.Task] = {}

        # Initialize sliding window rate calculator
        rate_calculator = SlidingWindowRateCalculator(window_size=20)

        # Progress reporting variables
        last_progress_report = start_time - INITIAL_PROGRESS_INTERVAL  # Force immediate first report
        initial_reports_count = 0
        max_initial_reports = 3

        try:
            # Create iterator for books
            book_iter = iter(available_to_sync[:books_to_process])

            # Fill initial processing queue
            for _ in range(self.concurrent_downloads):
                try:
                    barcode = next(book_iter)
                    task = asyncio.create_task(
                        sync_book_to_local_storage(
                            barcode,
                            self.grin_client,
                            self.library_directory,
                            self.storage_config,
                            self.db_tracker,
                            None,  # No ETag for initial call
                            self.gpg_key_file,
                            self.secrets_dir,
                            self.skip_extract_ocr,
                            self.skip_extract_marc,
                        )
                    )
                    active_tasks[barcode] = task
                    logger.info(f"[{barcode}] Started local storage sync")
                except StopIteration:
                    break

            # Process books
            while active_tasks:
                # Wait for any task to complete
                done, pending = await asyncio.wait(active_tasks.values(), return_when=asyncio.FIRST_COMPLETED)

                # Process completed tasks
                for task in done:
                    # Find which barcode this task belongs to
                    completed_barcode = None
                    for barcode, task_ref in active_tasks.items():
                        if task_ref == task:
                            completed_barcode = barcode
                            break

                    if completed_barcode:
                        del active_tasks[completed_barcode]
                        processed_count += 1

                        try:
                            result = await task
                            if result["status"] == "completed":
                                self.stats["completed"] += 1
                                self.stats["uploaded"] += 1
                                rate_calculator.add_batch(time.time(), processed_count)
                                logger.info(f"[{completed_barcode}] ✅ Local storage sync completed")
                                # Queue for enrichment after successful sync
                                await self.queue_book_for_enrichment(completed_barcode)
                            else:
                                self.stats["failed"] += 1
                                error_msg = result.get("error")
                                logger.error(f"[{completed_barcode}] ❌ Local storage sync failed: {error_msg}")

                        except Exception as e:
                            self.stats["failed"] += 1
                            logger.error(f"[{completed_barcode}] ❌ Local storage sync failed: {e}")

                        # Track progress (disable auto-reporting since we handle it manually)
                        # Don't call progress_reporter.increment() as we show manual progress updates

                        # Show detailed progress at intervals
                        last_progress_report, initial_reports_count = self._maybe_show_progress(
                            processed_count,
                            books_to_process,
                            start_time,
                            last_progress_report,
                            initial_reports_count,
                            max_initial_reports,
                            rate_calculator,
                            active_tasks=active_tasks,
                        )

                        # Start next book if available
                        try:
                            next_barcode = next(book_iter)
                            task = asyncio.create_task(
                                sync_book_to_local_storage(
                                    next_barcode,
                                    self.grin_client,
                                    self.library_directory,
                                    self.storage_config,
                                    self.db_tracker,
                                    None,
                                    self.gpg_key_file,
                                    self.secrets_dir,
                                    self.skip_extract_ocr,
                                    self.skip_extract_marc,
                                )
                            )
                            active_tasks[next_barcode] = task
                            logger.info(f"[{next_barcode}] Started local storage sync")
                        except StopIteration:
                            pass  # No more books

                # Check for shutdown request
                if self._shutdown_requested:
                    print("\nShutdown requested, stopping local storage sync...")
                    break

        except KeyboardInterrupt:
            print("\nLocal storage sync interrupted by user")
            logger.info("Local storage sync interrupted by user")

        except Exception as e:
            print(f"\nLocal storage sync failed: {e}")
            logger.error(f"Local storage sync failed: {e}", exc_info=True)

        finally:
            # Clean up resources
            await self.cleanup()

            # Final statistics
            self.progress_reporter.finish()
            total_elapsed = time.time() - start_time

            print("\nSync completed:")
            print(f"  Runtime: {format_duration(total_elapsed)}")
            print(f"  Books processed: {processed_count:,}")
            print(f"  Successfully synced: {self.stats['completed']:,}")
            print(f"  Failed: {self.stats['failed']:,}")

            if total_elapsed > 0 and self.stats["completed"] > 0:
                avg_rate = self.stats["completed"] / total_elapsed
                print(f"  Average rate: {avg_rate:.1f} books/second")

            # Export CSV if enabled
            await self._export_csv_and_print_result()

            logger.info("Sync completed")

    async def _run_block_storage_sync(
        self, available_to_sync: list[str], books_to_process: int, specific_barcodes: list[str] | None = None
    ) -> None:
        """Run sync pipeline for block storage (S3, R2, MinIO)."""
        self.progress_reporter = ProgressReporter("sync", books_to_process)
        self.progress_reporter.start()

        start_time = time.time()
        processed_count = 0
        active_downloads: dict[str, asyncio.Task] = {}
        active_uploads: dict[str, asyncio.Task] = {}

        # Initialize sliding window rate calculator
        rate_calculator = SlidingWindowRateCalculator(window_size=20)

        # Progress reporting variables
        last_progress_report = start_time - INITIAL_PROGRESS_INTERVAL  # Force immediate first report
        initial_reports_count = 0
        max_initial_reports = 3

        try:
            # Create iterator for books
            book_iter = iter(available_to_sync[:books_to_process])

            # Fill initial download queue - only up to concurrent_downloads limit
            while len(active_downloads) < self.concurrent_downloads:
                try:
                    barcode = next(book_iter)
                    if specific_barcodes is None or barcode in specific_barcodes:
                        task = asyncio.create_task(self._process_book_with_staging(barcode))
                        active_downloads[barcode] = task
                        logger.debug(
                            f"Created initial download task for {barcode} "
                            f"(queue: {len(active_downloads)}/{self.concurrent_downloads})"
                        )
                except StopIteration:
                    break

            # Process downloads and uploads
            while active_downloads or active_uploads:
                if self._shutdown_requested:
                    break

                # Refill download queue if under limit and more books available
                while len(active_downloads) < self.concurrent_downloads:
                    try:
                        barcode = next(book_iter)
                        if specific_barcodes is None or barcode in specific_barcodes:
                            task = asyncio.create_task(self._process_book_with_staging(barcode))
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

                done, pending = await asyncio.wait(all_tasks, return_when=asyncio.FIRST_COMPLETED)

                for completed_task in done:
                    try:
                        result = await completed_task

                        if result:
                            barcode = result.get("barcode")

                            # Handle download completion
                            if barcode in active_downloads and active_downloads[barcode] == completed_task:
                                del active_downloads[barcode]
                                logger.debug(
                                    f"[{barcode}] Download completed (success: {result.get('download_success', False)})"
                                )

                                # If download successful, start upload
                                if result.get("download_success"):
                                    upload_task = asyncio.create_task(self._upload_book_from_staging(barcode, result))
                                    active_uploads[barcode] = upload_task
                                    logger.debug(f"[{barcode}] Started upload task")
                                elif result.get("skipped"):
                                    # Download skipped due to ETag match, count as completed
                                    self.stats["skipped"] += 1
                                    logger.info(f"[{barcode}] Download skipped (already up to date)")

                                    # Track skipped as successful in process summary
                                    self.process_summary_stage.increment_items(successful=1)
                                else:
                                    # Download failed, update stats
                                    self.stats["failed"] += 1
                                    logger.warning(f"[{barcode}] Download failed, not starting upload")

                                    # Track download failure in process summary
                                    self.process_summary_stage.increment_items(failed=1)

                                # Update progress
                                processed_count += 1
                                rate_calculator.add_batch(time.time(), processed_count)

                                # Track in process summary
                                self.process_summary_stage.increment_items(processed=1)

                                # Show detailed progress at intervals
                                last_progress_report, initial_reports_count = self._maybe_show_progress(
                                    processed_count,
                                    books_to_process,
                                    start_time,
                                    last_progress_report,
                                    initial_reports_count,
                                    max_initial_reports,
                                    rate_calculator,
                                    active_downloads=active_downloads,
                                    active_uploads=active_uploads,
                                )

                            # Handle upload completion
                            elif barcode in active_uploads and active_uploads[barcode] == completed_task:
                                del active_uploads[barcode]
                                logger.info(
                                    f"[{barcode}] Upload completed (success: {result.get('upload_success', False)})"
                                )

                                # Update stats based on upload result
                                if result.get("upload_success"):
                                    self.stats["completed"] += 1
                                    logger.info(f"[{barcode}] Book sync fully completed")
                                    # Queue for enrichment after successful sync
                                    await self.queue_book_for_enrichment(barcode)

                                    # Track success in process summary
                                    self.process_summary_stage.increment_items(successful=1)
                                else:
                                    self.stats["failed"] += 1
                                    logger.warning(f"[{barcode}] Upload failed")

                                    # Track failure in process summary
                                    self.process_summary_stage.increment_items(failed=1)

                    except Exception as e:
                        logger.error(f"Error processing completed task: {e}", exc_info=True)
                        self.stats["failed"] += 1

                        # Track exception as failure in process summary
                        self.process_summary_stage.increment_items(failed=1)
                        self.process_summary_stage.add_error(type(e).__name__, str(e))

        finally:
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

            self.progress_reporter.finish()

            # Print final statistics
            total_elapsed = time.time() - start_time
            print(f"\nSync completed in {format_duration(total_elapsed)}")
            print(f"  Successfully synced: {self.stats['completed']:,}")
            print(f"  Failed: {self.stats['failed']:,}")
            print(f"  Skipped (ETag match): {self.stats['skipped']:,}")

            if total_elapsed > 0 and self.stats["completed"] > 0:
                avg_rate = self.stats["completed"] / total_elapsed
                print(f"  Average rate: {avg_rate:.1f} books/second")

            # Export CSV if enabled
            await self._export_csv_and_print_result()

            # Point to process summary file
            run_name = Path(self.db_path).parent.name
            process_summary_path = f"output/{run_name}/process_summary.json"
            print(f"\nProcess summary: {process_summary_path}")

            logger.info("Sync completed")

    async def _process_book_with_staging(self, barcode: str) -> dict[str, Any]:
        """Process a single book using staging directory."""
        # Wait for disk space BEFORE acquiring semaphore to prevent race condition
        if self.staging_manager:
            await self.staging_manager.wait_for_disk_space()
            logger.debug(f"[{barcode}] Disk space check passed, proceeding with download")

        async with self._download_semaphore:
            self._active_download_count += 1
            try:
                logger.debug(
                    f"[{barcode}] Download task started "
                    f"(active: {self._active_download_count}/{self.concurrent_downloads})"
                )

                # Check ETag and handle skip scenario
                skip_result, encrypted_etag, file_size = await check_and_handle_etag_skip(
                    barcode,
                    self.grin_client,
                    self.library_directory,
                    self.storage_type,
                    self.storage_config,
                    self.db_tracker,
                    self.force,
                )

                if skip_result:
                    self.stats["skipped"] += 1
                    return {"barcode": barcode, "download_success": False, "skipped": True, "skip_result": skip_result}

                # Download to staging
                _, staging_file_path, metadata = await download_book_to_staging(
                    barcode,
                    self.grin_client,
                    self.library_directory,
                    self.staging_manager,
                    encrypted_etag,
                    self.secrets_dir,
                )

                return {
                    "barcode": barcode,
                    "download_success": True,
                    "staging_file_path": staging_file_path,
                    "encrypted_etag": encrypted_etag,
                    "metadata": metadata,
                }

            except Exception as e:
                logger.error(f"[{barcode}] Download failed: {e}", exc_info=True)
                return {"barcode": barcode, "download_success": False, "error": str(e)}
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
                    self.gpg_key_file,
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

    async def run_sync(self, limit: int | None = None, specific_barcodes: list[str] | None = None) -> None:
        """Run the complete sync pipeline.

        Args:
            limit: Optional limit on number of books to sync
            specific_barcodes: Optional list of specific barcodes to sync
        """
        print("Starting GRIN-to-Storage sync pipeline")
        print(f"Database: {self.db_path}")

        # Display storage configuration details
        if self.storage_type == "local":
            base_path = self.storage_config.get("base_path") if self.storage_config else None
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

        sync_successful = False
        try:
            # Get list of converted books from GRIN
            print("Fetching list of converted books from GRIN...")
            converted_barcodes = await get_converted_books(self.grin_client, self.library_directory)
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

            # Start enrichment workers if enabled
            if self.enrichment_enabled:
                print(
                    f"Starting {self.enrichment_workers} enrichment "
                    f"{pluralize(self.enrichment_workers, 'worker')} for background processing"
                )
                await self.start_enrichment_workers()

            # Check how many requested books need syncing (only those actually converted by GRIN)
            available_to_sync = await self.db_tracker.get_books_for_sync(
                storage_type=self.storage_protocol,
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
                    storage_type=self.storage_protocol,
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

                sync_successful = True  # No books to process is considered successful
                return

            # Set up progress tracking
            books_to_process = min(limit or len(available_to_sync), len(available_to_sync))
            self.progress_reporter = ProgressReporter("sync", books_to_process)
            self.progress_reporter.start()

            # For local storage, use direct processing without staging
            if self.storage_protocol == "local":
                await self._run_local_storage_sync(available_to_sync, books_to_process, specific_barcodes)
                sync_successful = True
                return

            # For cloud storage, use the existing staging-based pipeline
            print(f"Starting sync of {books_to_process:,} books...")
            print(f"Concurrent limits: {self.concurrent_downloads} downloads, {self.concurrent_uploads} uploads")
            print(
                f"Progress updates will be shown every {REGULAR_PROGRESS_INTERVAL // 60} minutes "
                f"(more frequent initially)"
            )
            print("---")

            # Run block storage pipeline for cloud storage
            await self._run_block_storage_sync(available_to_sync, books_to_process, specific_barcodes)

            # If we get here, sync completed successfully
            sync_successful = True

        except KeyboardInterrupt:
            print("\nOperation cancelled by user")
        except Exception as e:
            print(f"Pipeline failed: {e}")
            logger.error(f"Pipeline failed: {e}", exc_info=True)
        finally:
            await self.cleanup(sync_successful)
