#!/usr/bin/env python3
"""
Book Collection main orchestrator.

Contains the BookCollector class responsible for coordinating the entire book collection process.
"""

import asyncio
import csv
import json
import logging
import os
import signal
import socket
import sys
import time
from collections.abc import AsyncGenerator
from datetime import UTC, datetime
from pathlib import Path

import aiofiles

from grin_to_s3.client import GRINClient, GRINRow
from grin_to_s3.common import (
    RateLimiter,
    format_duration,
    pluralize,
    print_oauth_setup_instructions,
)
from grin_to_s3.storage import BookManager, create_storage_from_config
from grin_to_s3.sync.progress_reporter import SlidingWindowRateCalculator, show_progress

from .config import ExportConfig, PaginationConfig
from .grin_parser import parse_grin_row
from .models import BookRecord, BoundedSet, SQLiteProgressTracker

# Set up module logger
logger = logging.getLogger(__name__)

# Progress display frequency (books per progress update)
PROGRESS_UPDATE_FREQUENCY = 2000


class BookCollector:
    """Main book collection orchestrator."""

    def __init__(
        self,
        directory: str,
        process_summary_stage,
        storage_config: dict,
        rate_limit: float = 1.0,
        resume_file: str = "output/default/progress.json",
        config: ExportConfig | None = None,
        secrets_dir: str | None = None,
    ):
        # Load or use provided configuration
        self.config = config or ExportConfig(
            library_directory=directory, rate_limit=rate_limit, resume_file=resume_file
        )

        self.directory = self.config.library_directory
        self.process_summary_stage = process_summary_stage

        # Initialize client
        self.grin_client = GRINClient(secrets_dir=secrets_dir)
        self.rate_limiter = RateLimiter(self.config.rate_limit)
        self.storage_config = storage_config
        self.resume_file = Path(self.config.resume_file)

        # Progress tracking
        self.rate_calculator = SlidingWindowRateCalculator(window_size=5)
        self.start_time: float | None = None
        self.sqlite_tracker = SQLiteProgressTracker(self.config.sqlite_db_path)
        # Keep small in-memory sets for recent items only (performance optimization)
        self.recent_processed = BoundedSet(max_size=self.config.recent_cache_size)
        self.recent_failed = BoundedSet(max_size=self.config.recent_failed_cache_size)

        # Job metadata
        self.job_metadata = self._create_job_metadata(rate_limit, storage_config)

        # Progress tracking state (moved from ProgressTracker)
        self.resume_count = 0
        self.accumulated_runtime = 0.0
        self.session_start_time: datetime | None = None

        # Storage (required)
        if not storage_config:
            raise ValueError("storage_config is required")
        storage = create_storage_from_config(storage_config)
        prefix = storage_config.get("prefix", "")
        self.book_manager: BookManager = BookManager(storage, storage_config=storage_config, base_prefix=prefix)

    def _create_job_metadata(self, rate_limit: float, storage_config: dict | None) -> dict:
        """Create comprehensive job metadata for progress tracking."""
        now = datetime.now(UTC)

        return {
            "job_started": now.isoformat(),
            "started_by_user": os.getenv("USER") or os.getenv("USERNAME") or "unknown",
            "hostname": socket.gethostname(),
            "python_version": f"{sys.version_info.major}.{sys.version_info.minor}.{sys.version_info.micro}",
            "working_directory": str(Path.cwd()),
            "export_parameters": {
                "directory": self.directory,
                "rate_limit": rate_limit,
                "storage_config": storage_config,
            },
            "system_info": {"platform": sys.platform, "pid": os.getpid()},
        }

    async def archive_progress_file(self) -> bool:
        """Archive existing progress file with timestamp before execution.

        Returns True if archiving was successful or not needed, False if failed.
        """
        if not self.resume_file.exists():
            return True  # Nothing to archive

        try:
            # Create archive filename with clean timestamp
            now = datetime.now(UTC)
            timestamp = now.strftime("%Y%m%d_%H%M%S")
            archive_name = f"{self.resume_file.stem}_backup_{timestamp}.json"
            archive_path = self.resume_file.parent / archive_name

            # Copy the file to archive location
            async with aiofiles.open(self.resume_file) as src:
                content = await src.read()

            async with aiofiles.open(archive_path, "w") as dst:
                await dst.write(content)

            logger.debug(f"Progress file archived: {archive_name}")
            return True

        except Exception as e:
            print(f"⚠️  Failed to archive progress file: {e}")
            print("   Proceeding with execution, but progress file corruption risk exists")
            return False

    async def load_progress(self) -> dict:
        """Load progress from resume file."""
        if not self.resume_file.exists():
            return {"processed": [], "failed": []}

        try:
            async with aiofiles.open(self.resume_file) as f:
                content = await f.read()
                progress_data = json.loads(content)

                # Initialize SQLite tracker
                await self.sqlite_tracker.init_db()

                # Load additional metadata if available
                if "job_metadata" in progress_data:
                    existing_metadata = progress_data["job_metadata"]

                    # Preserve original job start time and user
                    self.job_metadata["job_started"] = existing_metadata.get(
                        "job_started", self.job_metadata["job_started"]
                    )
                    self.job_metadata["started_by_user"] = existing_metadata.get(
                        "started_by_user", self.job_metadata["started_by_user"]
                    )

                # Load resume count and increment
                self.resume_count = progress_data.get("resume_count", 0) + 1

                # Get current counts from SQLite
                processed_count = await self.sqlite_tracker.get_processed_count()
                failed_count = await self.sqlite_tracker.get_failed_count()

                # Print resume summary
                elapsed_time = "unknown"
                if "job_metadata" in progress_data:
                    job_started = progress_data["job_metadata"].get("job_started")
                    if job_started:
                        try:
                            start_dt = datetime.fromisoformat(job_started.replace("Z", "+00:00"))
                            elapsed_seconds = (datetime.now(UTC) - start_dt).total_seconds()
                            elapsed_time = format_duration(elapsed_seconds)
                        except Exception:
                            pass

                print(f"Resumed (attempt #{self.resume_count})")
                print(f"  Progress: {processed_count} processed, {failed_count} failed")
                print(f"  Running time: {elapsed_time}")
                if "job_metadata" in progress_data:
                    started_by = progress_data["job_metadata"].get("started_by_user", "unknown")
                    hostname = progress_data["job_metadata"].get("hostname", "unknown")
                    print(f"  Started by: {started_by}@{hostname}")

                # Load accumulated runtime for tracking
                if "runtime_tracking" in progress_data:
                    self.accumulated_runtime = progress_data["runtime_tracking"].get("total_runtime_seconds", 0.0)

                return progress_data

        except (json.JSONDecodeError, Exception) as e:
            print(f"Warning: Could not load progress file: {e}")
            return {"processed": [], "failed": []}

    async def save_progress(self):
        """Save current progress to resume file."""
        now = datetime.now(UTC)

        # Calculate performance metrics based on actual runtime, not wall-clock time
        if hasattr(self, "session_start_time") and self.session_start_time:
            # Current session runtime
            session_elapsed = (now - self.session_start_time).total_seconds()
            # Add to accumulated runtime from previous sessions
            total_runtime = self.accumulated_runtime + session_elapsed
        else:
            # Fallback: estimate based on processing rate
            total_processed = await self.sqlite_tracker.get_processed_count()
            if total_processed > 0:
                # Assume reasonable processing rate for estimates
                estimated_rate = 15  # books per minute (conservative estimate)
                total_runtime = (total_processed / estimated_rate) * 60
            else:
                total_runtime = 0.0

        # Also calculate wall-clock time for reference
        job_start_time = datetime.fromisoformat(self.job_metadata["job_started"].replace("Z", "+00:00"))
        wall_clock_elapsed = (now - job_start_time).total_seconds()

        # Get current counts from SQLite
        total_processed = await self.sqlite_tracker.get_processed_count()
        total_failed = await self.sqlite_tracker.get_failed_count()

        # Build minimal progress data (metadata only, no barcode lists)
        progress_data = {
            "updated": now.isoformat(),
            # Enhanced metadata
            "job_metadata": self.job_metadata,
            "resume_count": self.resume_count,
            # Current status
            "current_status": {
                "total_processed": total_processed,
                "total_failed": total_failed,
                "actual_runtime_seconds": round(total_runtime, 1),
                "actual_runtime_formatted": format_duration(total_runtime),
                "wall_clock_elapsed_seconds": round(wall_clock_elapsed, 1),
                "wall_clock_elapsed_formatted": format_duration(wall_clock_elapsed),
                "last_update": now.isoformat(),
            },
            # Error summary
            "error_summary": {
                "failure_rate_percent": round(total_failed / max(1, total_processed + total_failed) * 100, 2),
                "total_errors": total_failed,
            },
            # Progress tracking
            "progress_tracking": {
                "completion_percentage": None,
                "total_estimate_method": "unknown",
            },
            # Runtime tracking
            "runtime_tracking": {
                "total_runtime_seconds": round(total_runtime, 1),
                "wall_clock_elapsed_seconds": round(wall_clock_elapsed, 1),
                "resume_count": self.resume_count,
                "explanation": (
                    "total_runtime tracks actual processing time across sessions; "
                    "wall_clock tracks time since first start"
                ),
            },
            # SQLite database info
            "sqlite_info": {
                "database_path": str(self.sqlite_tracker.db_path),
                "session_id": self.sqlite_tracker.session_id,
                "note": "Processed/failed barcodes stored in SQLite database, not in this JSON file",
            },
        }

        # Ensure progress directory exists
        self.resume_file.parent.mkdir(parents=True, exist_ok=True)

        async with aiofiles.open(self.resume_file, "w") as f:
            await f.write(json.dumps(progress_data, indent=2))

    def start_session(self):
        """Mark the start of a new session."""
        self.session_start_time = datetime.now(UTC)

    async def get_converted_books_html(
        self,
    ) -> AsyncGenerator[tuple[GRINRow, set[str]], None]:
        """Stream converted books from GRIN using HTML pagination with full metadata."""
        logger.info("Streaming converted books from GRIN...")

        # Use same pagination settings as _all_books
        pagination_config = self.config.pagination or PaginationConfig()

        book_count = 0
        phase1_start_time = time.time()
        phase1_rate_calculator = SlidingWindowRateCalculator(window_size=5)

        async for (
            book_row,
            known_barcodes,
        ) in self.grin_client.stream_book_list_html_prefetch(
            self.directory,
            list_type="_converted",
            page_size=pagination_config.page_size,
            max_pages=pagination_config.max_pages,
            start_page=1,
            sqlite_tracker=self.sqlite_tracker,
        ):
            yield book_row, known_barcodes
            book_count += 1

            # Show progress every N items to match main collection frequency
            if book_count % PROGRESS_UPDATE_FREQUENCY == 0:
                extra_info = {"current": book_row.get("barcode", "unknown")} if book_row.get("barcode") else None
                show_progress(
                    start_time=phase1_start_time,
                    total_items=None,  # Unknown total for converted books
                    rate_calculator=phase1_rate_calculator,
                    completed_count=book_count,
                    operation_name="barcode records",
                    extra_info=extra_info,
                )

    async def get_all_books_html(
        self,
    ) -> AsyncGenerator[tuple[GRINRow, set[str]], None]:
        """Stream non-converted books from GRIN using HTML pagination with large page sizes.

        Note: _all_books endpoint actually returns 'all books except converted', not truly all books.
        Always starts from page 1 for idempotent collection.
        """
        logger.info("Streaming non-converted books from GRIN...")

        pagination_config = self.config.pagination or PaginationConfig()
        book_count = 0

        async for (
            book_row,
            known_barcodes,
        ) in self.grin_client.stream_book_list_html_prefetch(
            directory=self.directory,
            list_type="_all_books",
            page_size=pagination_config.page_size,
            max_pages=pagination_config.max_pages,
            start_page=1,
            sqlite_tracker=self.sqlite_tracker,
        ):
            yield book_row, known_barcodes
            book_count += 1

            if book_count % 5000 == 0:
                logger.info(f"Streamed {book_count:,} non-converted {pluralize(book_count, 'book')}...")

    async def get_all_books(self, limit: int | None = None) -> AsyncGenerator[tuple[GRINRow, set[str]], None]:
        """Stream all book data from GRIN using two-pass collection with full metadata.

        First pass: Collect converted books with full metadata from _converted endpoint
        Second pass: Collect non-converted books from _all_books endpoint

        Note: _all_books endpoint actually returns 'all books except converted', not truly all books.

        Args:
            limit: Maximum number of books to yield across both phases

        Yields:
            tuple[GRINRow, set[str]]: (book_row, known_barcodes)
        """
        total_yielded = 0
        # First pass: Get converted books with full metadata
        print("Phase 1: Collecting converted books with full metadata...")
        converted_count = 0
        async for book_row, known_barcodes in self.get_converted_books_html():
            if limit and total_yielded >= limit:
                break
            yield book_row, known_barcodes
            converted_count += 1
            total_yielded += 1

        if converted_count > 0:
            print(f"Phase 1 complete: {converted_count:,} converted books collected")

        # Second pass: Get non-converted books from _all_books (only if limit not reached)
        if not limit or total_yielded < limit:
            print("Phase 2: Collecting non-converted books...")
            non_converted_count = 0
            async for book_row, known_barcodes in self.get_all_books_html():
                if limit and total_yielded >= limit:
                    break
                yield book_row, known_barcodes
                non_converted_count += 1
                total_yielded += 1

            if non_converted_count > 0:
                print(f"Phase 2 complete: {non_converted_count:,} non-converted books collected")
        else:
            non_converted_count = 0

        total_books = converted_count + non_converted_count
        print(
            f"Two-pass collection complete: {total_books:,} total books ({converted_count:,} converted + {non_converted_count:,} non-converted)"
        )

    async def collect_books(self, output_file: str, limit: int | None = None) -> bool:
        """
        Book collection with pagination.

        Processes books one at a time with reliable pagination and resume capability.
        Writes CSV at the end from the SQLite database.

        Returns:
            True if collection completed successfully, False if interrupted or incomplete
        """
        print("Starting book collection")
        if limit:
            print(f"Limit: {limit:,} {pluralize(limit, 'book')}")
            print("\n⚠️  WARNING: Using --limit will result in an incomplete collection only suitable for quick tests.")
            print("   For production sync, a full collect (without --limit) is recommended.\n")

        # Validate credentials before starting export
        logger.debug("Validating GRIN credentials...")
        try:
            await self.grin_client.auth.validate_credentials(self.directory)
        except Exception as e:
            print(f"Credential validation failed: {e}")
            print("Collection cannot continue without valid credentials.")
            print_oauth_setup_instructions()
            return False

        # Archive existing progress file before starting execution
        logger.debug("Backing up progress file...")
        await self.archive_progress_file()

        # Set up async-friendly signal handling
        loop = asyncio.get_running_loop()
        stop_event = asyncio.Event()
        completed_successfully = True

        def handle_interrupt():
            print("\nInterrupt received - saving progress and exiting gracefully...")
            stop_event.set()

        # Install signal handler
        loop.add_signal_handler(signal.SIGINT, handle_interrupt)

        try:
            # Load progress and initialize session tracking
            await self.load_progress()

            # Initialize session timing
            self.start_session()

            # Start progress tracking
            self.start_time = time.time()
            processed_count = 0

            # Process books one by one using configured data mode
            last_book_time = time.time()
            book_count_in_loop = 0
            async for grin_row, known_barcodes_on_page in self.get_all_books(limit=limit):
                current_time = time.time()
                time_since_last = current_time - last_book_time
                book_count_in_loop += 1

                # Log if there's a significant gap between book yields
                if time_since_last > 10.0:  # More than 10 seconds between books
                    gap_time = datetime.now().strftime("%H:%M:%S.%f")[:-3]
                    logger.warning(
                        f"Long gap detected: {time_since_last:.1f}s between books at {gap_time} "
                        f"(book #{book_count_in_loop})"
                    )
                elif book_count_in_loop % 5000 == 0:
                    receive_time = datetime.now().strftime("%H:%M:%S.%f")[:-3]
                    logger.debug(
                        f"Main loop received book #{book_count_in_loop} at {receive_time} (gap: {time_since_last:.2f}s)"
                    )

                last_book_time = current_time

                # Check for interrupt
                if stop_event.is_set():
                    print("Collection interrupted - saving progress...")
                    completed_successfully = False
                    break

                # Extract barcode for checking
                barcode = grin_row.get("barcode", "")
                if not barcode:
                    continue

                # Skip if already processed (using batch SQLite results)
                if barcode in known_barcodes_on_page:
                    continue

                # Process the book
                try:
                    record = await self.process_book(grin_row)
                    if record:
                        processed_count += 1

                        # Show progress every N items
                        if processed_count % PROGRESS_UPDATE_FREQUENCY == 0:
                            extra_info = {"current": record.barcode} if record else None
                            show_progress(
                                start_time=self.start_time,
                                total_items=None,
                                rate_calculator=self.rate_calculator,
                                completed_count=processed_count,
                                operation_name="barcode records",
                                extra_info=extra_info,
                            )

                        # Track in process summary
                        self.process_summary_stage.increment_items(processed=1, successful=1)

                        # Save progress periodically
                        if processed_count % 100 == 0:
                            await self.save_progress()

                    else:
                        # Book was processed but no record created (already exists, etc.)
                        self.process_summary_stage.increment_items(processed=1)

                except Exception as e:
                    print(f"Error processing {barcode}: {e}")
                    await self.sqlite_tracker.mark_failed(barcode, str(e))
                    self.recent_failed.add(barcode)

                    # Track failed item in process summary
                    self.process_summary_stage.increment_items(processed=1, failed=1)
                    continue

            print(f"Processed {processed_count} new books")

            # Save final progress
            await self.save_progress()

            # Export CSV from database if collection completed successfully
            if completed_successfully or processed_count > 0:
                print(f"\nExporting collection to CSV: {output_file}")
                await self.export_csv_from_database(output_file)

        finally:
            # Remove signal handler
            loop.remove_signal_handler(signal.SIGINT)

            # Show final completion message
            print(f"✓ Completed Book Collection: {processed_count:,} barcode records")

        # Return completion status
        return completed_successfully

    async def export_csv_from_database(self, output_file: str) -> None:
        """Export all books from SQLite database to CSV file.

        Args:
            output_file: Path to output CSV file
        """
        # Get all books from database
        books = await self.sqlite_tracker.get_all_books_csv_data()

        # Write CSV file
        with open(output_file, "w", newline="", encoding="utf-8") as f:
            writer = csv.writer(f)
            writer.writerow(BookRecord.csv_headers())

            for book in books:
                writer.writerow(book.to_csv_row())

        print(f"✅ CSV export completed: {output_file}")
        print(f"   Exported {len(books):,} books with all available metadata")

    async def process_book(self, grin_row: GRINRow) -> BookRecord | None:
        """Process a single GRINRow from GRIN and return its record.
        Args:
            grin_row: GRINRow dict from client with parsed book data
        """

        # Process GRIN data directly from the row
        parsed_data = parse_grin_row(grin_row)
        if not parsed_data or not parsed_data.get("barcode"):
            return None

        barcode = parsed_data["barcode"]

        if await self.sqlite_tracker.is_processed(barcode):
            return None  # Already processed

        try:
            # Create record
            record = BookRecord(**parsed_data)

            # Warn if GRIN returned empty title
            if not record.title or record.title.strip() == "":
                logger.warning(
                    f"[{barcode}] GRIN returned empty title field; okay only if book is not available for download"
                )

            # Save book record to SQLite database (blocking to ensure data integrity)
            # Note: Main network/processing work remains parallel; only DB writes are synchronous
            # to prevent race condition where limit is reached before all books are saved
            await self.sqlite_tracker.save_book(record)

            # Mark as processed for progress tracking
            await self.sqlite_tracker.mark_processed(barcode)
            self.recent_processed.add(barcode)
            return record

        except Exception as e:
            print(f"Error processing {barcode}: {e}")
            await self.sqlite_tracker.mark_failed(barcode, str(e))
            self.recent_failed.add(barcode)
            return None

    async def cleanup(self):
        """Clean up resources."""
        if hasattr(self, "grin_client"):
            await self.grin_client.close()
        if hasattr(self, "sqlite_tracker"):
            await self.sqlite_tracker.close()
