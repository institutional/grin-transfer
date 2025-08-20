#!/usr/bin/env python3
"""
Sync Pipeline Orchestration

Main pipeline orchestration for syncing books from GRIN to storage.
"""

import asyncio
import logging
import time
from pathlib import Path
from typing import Any, TypedDict

from grin_to_s3.client import GRINClient
from grin_to_s3.collect_books.models import SQLiteProgressTracker
from grin_to_s3.common import (
    DEFAULT_DOWNLOAD_RETRIES,
    DEFAULT_DOWNLOAD_TIMEOUT,
    DEFAULT_MAX_SEQUENTIAL_FAILURES,
    pluralize,
)
from grin_to_s3.constants import DEFAULT_CONVERSION_REQUEST_LIMIT
from grin_to_s3.queue_utils import get_converted_books, get_in_process_set
from grin_to_s3.run_config import RunConfig
from grin_to_s3.storage import create_storage_from_config
from grin_to_s3.storage.book_manager import BookManager
from grin_to_s3.storage.staging import LocalDirectoryManager, StagingDirectoryManager
from grin_to_s3.sync.progress_reporter import SlidingWindowRateCalculator
from grin_to_s3.sync.task_manager import (
    TaskManager,
    process_books_batch,
)
from grin_to_s3.sync.tasks import (
    check,
    cleanup,
    decrypt,
    download,
    export_csv,
    extract_marc,
    extract_ocr,
    request_conversion,
    unpack,
    upload,
)
from grin_to_s3.sync.tasks.task_types import TaskType

from .barcode_filtering import create_filtering_summary, filter_barcodes_pipeline
from .conversion_handler import ConversionRequestHandler
from .preflight import run_preflight_operations
from .progress_reporter import SyncProgressReporter
from .utils import reset_bucket_cache

logger = logging.getLogger(__name__)


class SyncStats(TypedDict):
    """Statistics for sync operations."""

    processed: int
    synced: int  # Actually downloaded and uploaded to storage
    failed: int
    skipped: int
    skipped_etag_match: int
    skipped_conversion_limit: int
    conversion_requested: int
    marked_unavailable: int
    uploaded: int
    total_bytes: int


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
        skip_csv_export: bool = False,
        skip_staging_cleanup: bool = False,
        skip_database_backup: bool = False,
        download_timeout: int = DEFAULT_DOWNLOAD_TIMEOUT,
        download_retries: int = DEFAULT_DOWNLOAD_RETRIES,
        max_sequential_failures: int = DEFAULT_MAX_SEQUENTIAL_FAILURES,
        task_concurrency_overrides: dict[str, int] | None = None,
    ) -> "SyncPipeline":
        """Create SyncPipeline from RunConfig.

        Args:
            config: RunConfig containing all pipeline configuration
            process_summary_stage: Process summary stage for tracking
            force: Force re-download even if ETags match
            dry_run: Show what would be processed without downloading or uploading
            skip_extract_ocr: Skip OCR text extraction
            skip_extract_marc: Skip MARC metadata extraction
            skip_csv_export: Skip CSV export after sync
            skip_staging_cleanup: Skip deletion of files in staging directory
            skip_database_backup: Skip database backup and upload
            download_timeout: Timeout for book downloads in seconds
            download_retries: Number of retry attempts for failed downloads
            max_sequential_failures: Exit pipeline after this many consecutive failures
            task_concurrency_overrides: Override task concurrency limits from CLI

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
            skip_csv_export=skip_csv_export,
            skip_staging_cleanup=skip_staging_cleanup,
            skip_database_backup=skip_database_backup,
            download_timeout=download_timeout,
            download_retries=download_retries,
            max_sequential_failures=max_sequential_failures,
            task_concurrency_overrides=task_concurrency_overrides,
        )

    def __init__(
        self,
        config: RunConfig,
        process_summary_stage,
        force: bool = False,
        dry_run: bool = False,
        skip_extract_ocr: bool = False,
        skip_extract_marc: bool = False,
        skip_csv_export: bool = False,
        skip_staging_cleanup: bool = False,
        skip_database_backup: bool = False,
        download_timeout: int = DEFAULT_DOWNLOAD_TIMEOUT,
        download_retries: int = DEFAULT_DOWNLOAD_RETRIES,
        max_sequential_failures: int = DEFAULT_MAX_SEQUENTIAL_FAILURES,
        task_concurrency_overrides: dict[str, int] | None = None,
    ):
        # Store configuration and runtime parameters
        self.config = config
        self.force = force
        self.dry_run = dry_run
        self.skip_extract_ocr = skip_extract_ocr
        self.skip_extract_marc = skip_extract_marc
        self.skip_csv_export = skip_csv_export
        self.skip_staging_cleanup = skip_staging_cleanup
        self.skip_database_backup = skip_database_backup
        self.download_timeout = download_timeout
        self.download_retries = download_retries
        self.max_sequential_failures = max_sequential_failures
        self.process_summary_stage = process_summary_stage

        # Extract commonly used config values
        self.db_path = config.sqlite_db_path
        self.library_directory = config.library_directory
        self.secrets_dir = config.secrets_dir

        # Sync configuration from RunConfig
        self.batch_size = config.sync_batch_size
        self.disk_space_threshold = config.sync_disk_space_threshold

        # Build task concurrency limits from config and overrides
        self.task_concurrency_limits = self._build_task_concurrency_limits(config, task_concurrency_overrides)

        self.filesystem_manager: LocalDirectoryManager | StagingDirectoryManager

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
        if self.config.storage_config["protocol"] != "local":
            self.filesystem_manager = StagingDirectoryManager(
                staging_path=self.staging_dir, capacity_threshold=self.disk_space_threshold
            )
        else:
            self.filesystem_manager = LocalDirectoryManager(
                staging_path=config.storage_config["config"]["base_path"],  # pyright: ignore[reportTypedDictNotRequiredAccess]
            )

        # Concurrency control
        self._shutdown_requested = False
        self._fatal_error: str | None = None  # Store fatal errors that should stop the pipeline

        # Database update accumulator for atomic commits
        self.book_record_updates: dict[str, dict[str, Any]] = {}

        # Track active postprocessing tasks
        self._active_postprocessing_count = 0

        # Simple gate to prevent race conditions in task creation
        self._task_creation_lock = asyncio.Lock()

        # Statistics
        self.stats = SyncStats(
            processed=0,
            synced=0,
            failed=0,
            skipped=0,
            skipped_etag_match=0,
            skipped_conversion_limit=0,
            conversion_requested=0,
            marked_unavailable=0,
            uploaded=0,
            total_bytes=0,
        )
        self.start_time = time.time()
        self._sequential_failures = 0  # Track consecutive failures for exit logic

        # Initialize storage components once (now that tests provide complete configurations)
        self.storage = create_storage_from_config(self.config.storage_config)
        self.base_prefix = self.config.storage_config.get("prefix", "")

        # Conversion request handling for previous queue
        self.current_queues: list[str] = []  # Track which queues are being processed
        self.conversion_handler: ConversionRequestHandler | None = None  # Lazy initialization
        self.conversion_request_limit = DEFAULT_CONVERSION_REQUEST_LIMIT
        self.conversion_requests_made = 0
        self.book_manager = BookManager(
            self.storage, storage_config=self.config.storage_config, base_prefix=self.base_prefix
        )

    def _build_task_concurrency_limits(
        self, config: RunConfig, overrides: dict[str, int] | None = None
    ) -> dict[TaskType, int]:
        """Build task concurrency limits from config and CLI overrides."""
        from grin_to_s3.sync.tasks.task_types import TaskType

        limits = {
            # CHECK tasks use shared GRIN request semaphore (no separate limit)
            TaskType.REQUEST_CONVERSION: 2,  # Limit concurrent conversion requests
            TaskType.DOWNLOAD: config.sync_task_download_concurrency,
            TaskType.DECRYPT: config.sync_task_decrypt_concurrency,
            TaskType.UPLOAD: config.sync_task_upload_concurrency,
            TaskType.UNPACK: config.sync_task_unpack_concurrency,
            TaskType.EXTRACT_MARC: config.sync_task_extract_marc_concurrency,
            TaskType.EXTRACT_OCR: config.sync_task_extract_ocr_concurrency,
            TaskType.EXPORT_CSV: config.sync_task_export_csv_concurrency,
            TaskType.CLEANUP: config.sync_task_cleanup_concurrency,
        }

        # Apply CLI overrides if provided
        if overrides:
            task_type_mapping = {
                "task_download_concurrency": TaskType.DOWNLOAD,
                "task_decrypt_concurrency": TaskType.DECRYPT,
                "task_upload_concurrency": TaskType.UPLOAD,
                "task_unpack_concurrency": TaskType.UNPACK,
                "task_extract_marc_concurrency": TaskType.EXTRACT_MARC,
                "task_extract_ocr_concurrency": TaskType.EXTRACT_OCR,
                "task_export_csv_concurrency": TaskType.EXPORT_CSV,
                "task_cleanup_concurrency": TaskType.CLEANUP,
            }

            for config_key, value in overrides.items():
                if config_key in task_type_mapping:
                    limits[task_type_mapping[config_key]] = value

        return limits

    @property
    def uses_block_storage(self) -> bool:
        """Check if the pipeline uses block storage."""
        return self.config.storage_config["protocol"] != "local"

    @property
    def uses_local_storage(self) -> bool:
        """Check if the pipeline uses local storage."""
        return self.config.storage_config["protocol"] == "local"

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

    async def get_sync_status(self) -> dict:
        """Get current sync status and statistics."""
        stats = await self.db_tracker.get_sync_stats(self.config.storage_config["protocol"])

        return {
            **stats,
            "session_stats": self.stats,
        }

    async def setup_sync_loop(
        self, queues: list[str] | None = None, limit: int | None = None, specific_barcodes: list[str] | None = None
    ) -> None:
        """Run the complete sync pipeline.

        Args:
            limit: Optional limit on number of books to sync
            specific_barcodes: Optional list of specific barcodes to sync
            queues: List of queue types to process (converted, previous, changed, all)
        """
        # Initialize progress reporter variables
        progress_reporter = None
        progress_reporter_task = None

        # Store current queues for use in conversion request handling
        self.current_queues = queues or []

        # Initialize conversion handler if processing previous queue
        if "previous" in self.current_queues:
            self.conversion_handler = ConversionRequestHandler(
                library_directory=self.library_directory, db_tracker=self.db_tracker, secrets_dir=self.secrets_dir
            )
        if self.dry_run:
            print("🔍 DRY-RUN MODE: No files will be downloaded or uploaded")

        # Display storage configuration details
        if self.uses_local_storage:
            base_path = self.config.storage_config["config"].get("base_path")
            print(f"Storage: Local filesystem at {base_path or 'None'}")
        elif self.config.storage_config["type"] in ["s3", "r2", "minio"]:
            storage_names = {"s3": "AWS S3", "r2": "Cloudflare R2", "minio": "MinIO"}
            storage_name = storage_names.get(self.config.storage_config["type"])

            if self.config.storage_config["type"] == "minio":
                endpoint = self.config.storage_config["config"].get("endpoint_url", "unknown endpoint")
                print(f"Storage: {storage_name} at {endpoint}")
            else:
                print(f"Storage: {storage_name}")

            print(f"  Raw bucket: {self.config.storage_config['config'].get('bucket_raw', 'unknown')}")
            print(f"  Meta bucket: {self.config.storage_config['config'].get('bucket_meta', 'unknown')}")
            print(f"  Full bucket: {self.config.storage_config['config'].get('bucket_full', 'unknown')}")

        if limit:
            print(f"Limit: {limit:,} {pluralize(limit, 'book')}")
        print()

        logger.info("Starting sync pipeline")
        logger.info(f"Database: {self.db_path}")

        # Reset bucket cache at start of sync
        reset_bucket_cache()

        # Run preflight operations (including database backup)
        preflight_results = await run_preflight_operations(self)
        for operation, result in preflight_results.items():
            if result.action.value == "failed":
                logger.error(f"Preflight operation {operation} failed: {result.error}")
            elif result.action.value == "completed":
                logger.info(f"Preflight operation {operation} completed successfully")
            elif result.action.value == "skipped":
                logger.info(f"Preflight operation {operation} skipped")

        try:
            # Collect books from queues if not using specific barcodes
            queue_books = None
            if not specific_barcodes and queues:
                print(f"Processing queues: {', '.join(queues)}")
                queue_books = set()
                for queue_name in queues:
                    print(f"Fetching books from '{queue_name}' queue...")
                    books = await get_books_from_queue(
                        self.grin_client, self.library_directory, queue_name, self.db_tracker
                    )
                    if len(books) == 0:
                        print(f"  Warning: '{queue_name}' queue reports no books available")
                    else:
                        print(f"  '{queue_name}' queue: {len(books):,} books available")
                    queue_books.update(books)

            # Get already synced books from database
            if specific_barcodes:
                books_already_synced = set()  # Skip DB check for specific barcodes
            else:
                # Get books that are already synced
                books_already_synced = await self.db_tracker.get_synced_books(
                    storage_type=self.config.storage_config["protocol"]
                )

            # Run the filtering pipeline
            filtering_result = filter_barcodes_pipeline(
                specific_barcodes=specific_barcodes,
                queue_books=queue_books,
                books_already_synced=books_already_synced,
                limit=limit,
            )

            # Print the filtering summary
            for line in create_filtering_summary(filtering_result):
                print(line)

            # Get database sync status for additional context
            initial_status = await self.get_sync_status()
            print(
                f"\nDatabase sync status: {initial_status['total_converted']:,} total, "
                f"{initial_status['synced']:,} synced, "
                f"{initial_status['failed']:,} failed, "
                f"{initial_status['pending']:,} pending"
            )

            # Handle case where no books to process
            if not filtering_result.books_after_limit:
                if len(filtering_result.source_books) == 0:
                    print("No books available from any queue")
                else:
                    print("No books found that need syncing (all may already be synced)")
                return

            # Set up progress tracking and task management
            books_to_process_count = len(filtering_result.books_after_limit)

            # Define task functions and limits
            task_funcs = {
                TaskType.CHECK: check.main,
                TaskType.REQUEST_CONVERSION: request_conversion.main,
                TaskType.DOWNLOAD: download.main,
                TaskType.DECRYPT: decrypt.main,
                TaskType.UPLOAD: upload.main,
                TaskType.UNPACK: unpack.main,
                TaskType.EXTRACT_MARC: extract_marc.main,
                TaskType.EXTRACT_OCR: extract_ocr.main,
                TaskType.EXPORT_CSV: export_csv.main,
                TaskType.CLEANUP: cleanup.main,
            }

            limits = self.task_concurrency_limits

            # Handle dry-run mode
            if self.dry_run:
                await self._show_dry_run_preview(filtering_result.books_after_limit, limit, specific_barcodes)
                return

            # Handle case where no books to process (already handled above, but keep for safety)
            if not filtering_result.books_after_limit:
                return  # No books to process is considered successful

            # Create progress reporter with task manager integration
            task_manager = TaskManager(limits)
            rate_calculator = SlidingWindowRateCalculator(window_size=20)

            progress_reporter = SyncProgressReporter(
                task_manager,
                books_to_process_count,
                self.task_concurrency_limits[TaskType.DOWNLOAD],
                self.task_concurrency_limits[TaskType.UPLOAD],
            )

            print(f"Starting sync of {books_to_process_count:,} {pluralize(books_to_process_count, 'book')}...")

            print("Progress updates will be shown every 10 minutes (more frequent initially)")
            print("---")

            # Start background progress reporter
            progress_reporter_task = asyncio.create_task(progress_reporter.run(time.time(), rate_calculator))

            # Process the filtered books using the task manager
            await process_books_batch(
                filtering_result.books_after_limit,
                self,
                task_funcs,
                task_manager,
            )

        except KeyboardInterrupt:
            # KeyboardInterrupt is handled by signal handler, just log and continue to cleanup
            logger.info("Processing interrupted by user signal, proceeding to cleanup")
        except Exception as e:
            print(f"Pipeline failed: {e}")
            logger.error(f"Pipeline failed: {e}", exc_info=True)
        finally:
            # Ensure progress reporter is shut down gracefully
            if (
                progress_reporter is not None
                and progress_reporter_task is not None
                and not progress_reporter_task.done()
            ):
                # Request graceful shutdown first
                progress_reporter.request_shutdown()
                try:
                    # Wait for graceful shutdown with timeout
                    await asyncio.wait_for(progress_reporter_task, timeout=2.0)
                except TimeoutError:
                    # Force cancellation if graceful shutdown takes too long
                    progress_reporter_task.cancel()
                    try:
                        await asyncio.wait_for(progress_reporter_task, timeout=1.0)
                    except (TimeoutError, asyncio.CancelledError):
                        pass

    def _should_exit_for_failure_limit(self) -> bool:
        """Check if pipeline should exit due to sequential failure limit."""
        return self._sequential_failures >= self.max_sequential_failures

    def _should_exit_for_shutdown(self) -> bool:
        """Check if pipeline should exit due to shutdown request."""
        return self._shutdown_requested

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
        print(f"Storage type: {self.config.storage_config['type']}")
        print("Task concurrency limits:")
        for task_type, task_limit in self.task_concurrency_limits.items():
            print(f"  {task_type.name.lower()}: {task_limit}")
        print(f"Batch size: {self.batch_size}")

        if specific_barcodes:
            print(f"Filtered to specific barcodes: {len(specific_barcodes) if specific_barcodes else 0}")

        if limit and limit < len(available_to_sync):
            print(f"Limited to first {limit:,} {pluralize(limit, 'book')}")

        print(f"\nAll {books_to_process:,} {pluralize(books_to_process, 'book')} that would be processed:")
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
        if not self.skip_csv_export:
            print("  7. Update CSV export")
        if not self.skip_staging_cleanup:
            print("  8. Clean up staging files")

        print("\nDRY-RUN COMPLETE: No actual processing performed")
        print(f"{'=' * 60}")

        logger.info(
            f"DRY-RUN: Would process {books_to_process} books with storage type {self.config.storage_config['type']}"
        )
