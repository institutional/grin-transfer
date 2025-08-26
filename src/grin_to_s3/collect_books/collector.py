#!/usr/bin/env python3
"""
Book Collection main orchestrator.

Contains the BookCollector class responsible for coordinating the entire book collection process.
"""

import asyncio
import csv
import hashlib
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
from typing import TypedDict

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
from .models import BookRecord, BoundedSet, SQLiteProgressTracker


class PaginationState(TypedDict):
    """Type for pagination state dictionary."""

    current_page: int
    next_url: str | None
    page_size: int


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
        test_mode: bool = False,
        config: ExportConfig | None = None,
        secrets_dir: str | None = None,
    ):
        # Load or use provided configuration
        self.config = config or ExportConfig(
            library_directory=directory, rate_limit=rate_limit, resume_file=resume_file
        )

        self.directory = self.config.library_directory
        self.test_mode = test_mode
        self.process_summary_stage = process_summary_stage

        # Initialize client (will be replaced with mock if in test mode)
        self.grin_client = GRINClient(secrets_dir=secrets_dir)
        self.rate_limiter = RateLimiter(self.config.rate_limit)
        self.storage_config = storage_config
        self.resume_file = Path(self.config.resume_file)

        # Set up test mode if requested
        if test_mode:
            self._setup_test_mode()

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
        self.total_books_estimate: int | None = None
        self.accumulated_runtime = 0.0
        self.session_start_time: datetime | None = None

        # Initialize pagination state
        pagination_config = self.config.pagination or PaginationConfig()
        self.pagination_state: PaginationState = {
            "current_page": pagination_config.start_page,
            "next_url": None,
            "page_size": pagination_config.page_size,
        }

        # Storage (required)
        if not storage_config:
            raise ValueError("storage_config is required")
        storage = create_storage_from_config(storage_config)
        prefix = storage_config.get("prefix", "")
        self.book_manager: BookManager = BookManager(storage, storage_config=storage_config, base_prefix=prefix)

    def _setup_test_mode(self):
        """Set up test mode with mock data and clients"""
        # Import mocks locally to avoid dependency issues when not in test mode
        try:
            from tests.mocks import (
                MockBookStorage,
                MockGRINClient,
                MockStorage,
                get_test_data,
            )

            # Replace client with mock
            self.grin_client = MockGRINClient(get_test_data())  # type: ignore[assignment]

            # Replace storage with mock if storage config exists
            if self.storage_config:
                mock_book_manager = MockBookStorage()
                mock_book_manager.storage = MockStorage()  # type: ignore[attr-defined]
                self.book_manager = mock_book_manager  # type: ignore[assignment]

            logger.info("Test mode enabled - using mock data (no network calls)")

        except ImportError:
            logger.warning("Test mode requested but mocks not available")
            logger.warning("Falling back to regular mode")

    def _create_job_metadata(self, rate_limit: float, storage_config: dict | None) -> dict:
        """Create comprehensive job metadata for progress tracking."""
        now = datetime.now(UTC)

        # Create configuration hash for detecting parameter changes
        config_str = json.dumps(
            {
                "directory": self.directory,
                "rate_limit": rate_limit,
                "storage_config": storage_config,
            },
            sort_keys=True,
        )
        config_hash = hashlib.md5(config_str.encode()).hexdigest()[:8]

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
                "config_hash": config_hash,
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
                self.total_books_estimate = progress_data.get("total_books_estimate")

                # Load pagination state if available
                if "pagination_state" in progress_data:
                    self.pagination_state = progress_data["pagination_state"]
                    print(
                        f"  Pagination: Resume from page {self.pagination_state.get('current_page', 1)} (Phase 2: non-converted books)"
                    )

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
            "total_books_estimate": self.total_books_estimate,
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
            # Performance metrics (empty now since we removed the calculator)
            "performance_metrics": {},
            # Error summary
            "error_summary": {
                "failure_rate_percent": round(total_failed / max(1, total_processed + total_failed) * 100, 2),
                "total_errors": total_failed,
            },
            # Progress tracking
            "progress_tracking": {
                "completion_percentage": round(
                    total_processed / max(1, self.total_books_estimate or total_processed) * 100, 2
                )
                if self.total_books_estimate and self.total_books_estimate > total_processed
                else None,
                "total_estimate_method": "streaming" if self.total_books_estimate else "unknown",
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
            # Pagination state for resume functionality
            "pagination_state": self.pagination_state,
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

    async def save_pagination_state(self, pagination_state: PaginationState):
        """Save pagination state for resume functionality."""
        self.pagination_state.update(pagination_state)
        # Save progress immediately to persist pagination state
        await self.save_progress()

    def start_session(self):
        """Mark the start of a new session."""
        self.session_start_time = datetime.now(UTC)

    def update_total_books_estimate(self, estimate: int):
        """Update the total books estimate."""
        self.total_books_estimate = estimate

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
            pagination_callback=None,  # Don't save pagination state for converted books
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
        """
        logger.info("Streaming non-converted books from GRIN...")

        # Determine starting point for pagination
        start_page = self.pagination_state.get("current_page", 1)
        start_url = self.pagination_state.get("next_url")
        pagination_config = self.config.pagination or PaginationConfig()
        page_size = self.pagination_state.get("page_size", pagination_config.page_size)

        if start_page is not None and start_page > 1:
            print(f"Resuming pagination from page {start_page}")

        book_count = 0
        # Explicitly handle the start_url parameter to satisfy type checking
        kwargs = {
            "directory": self.directory,
            "list_type": "_all_books",
            "page_size": page_size or pagination_config.page_size,
            "max_pages": pagination_config.max_pages,
            "start_page": start_page or 1,
            "pagination_callback": self.save_pagination_state,
            "sqlite_tracker": self.sqlite_tracker,
        }
        if start_url is not None:
            kwargs["start_url"] = start_url

        async for (
            book_row,
            known_barcodes,
        ) in self.grin_client.stream_book_list_html_prefetch(**kwargs):
            yield book_row, known_barcodes
            book_count += 1

            # Update total estimate as we stream
            if not self.total_books_estimate or book_count > self.total_books_estimate:
                self.update_total_books_estimate(book_count + 50000)  # Conservative estimate

            if book_count % 5000 == 0:
                logger.info(f"Streamed {book_count:,} non-converted {pluralize(book_count, 'book')}...")
                # Update total estimate more aggressively during large streams
                if book_count > 50000:
                    self.update_total_books_estimate(book_count + 100000)

    async def get_converted_books(self) -> AsyncGenerator[tuple[str, set[str]], None]:
        """Stream books from GRIN's _converted list."""
        try:
            print("Fetching converted books from GRIN...")
            response_text = await self.grin_client.fetch_resource(self.directory, "_converted?format=text")
            lines = response_text.strip().split("\n")

            converted_count = 0
            for line in lines:
                if line.strip() and ".tar.gz.gpg" in line:
                    barcode = line.strip().replace(".tar.gz.gpg", "")
                    if barcode:
                        # Create a minimal GRIN line format - just the barcode
                        # The process_book method will enrich this with metadata
                        grin_line = barcode
                        converted_count += 1
                        yield grin_line, set()

            print(f"Found {converted_count:,} converted books from GRIN")

        except Exception as e:
            print(f"Warning: Could not fetch converted books: {e}")
            # Continue without converted books rather than failing

    async def get_all_books(self) -> AsyncGenerator[tuple[GRINRow, set[str]], None]:
        """Stream all book data from GRIN using two-pass collection with full metadata.

        First pass: Collect converted books with full metadata from _converted endpoint
        Second pass: Collect non-converted books from _all_books endpoint

        Note: _all_books endpoint actually returns 'all books except converted', not truly all books.

        Yields:
            tuple[GRINRow, set[str]]: (book_row, known_barcodes)
        """
        # First pass: Get converted books with full metadata
        print("Phase 1: Collecting converted books with full metadata...")
        converted_count = 0
        async for book_row, known_barcodes in self.get_converted_books_html():
            yield book_row, known_barcodes
            converted_count += 1

        if converted_count > 0:
            print(f"Phase 1 complete: {converted_count:,} converted books collected")

        # Second pass: Get non-converted books from _all_books
        print("Phase 2: Collecting non-converted books...")
        non_converted_count = 0
        async for book_row, known_barcodes in self.get_all_books_html():
            yield book_row, known_barcodes
            non_converted_count += 1

        if non_converted_count > 0:
            print(f"Phase 2 complete: {non_converted_count:,} non-converted books collected")

        total_books = converted_count + non_converted_count
        print(
            f"Two-pass collection complete: {total_books:,} total books ({converted_count:,} converted + {non_converted_count:,} non-converted)"
        )

    def _looks_like_date(self, text: str) -> bool:
        """Check if text looks like a date string."""
        if not text:
            return False
        # Check for common date patterns
        return (
            ("/" in text and any(c.isdigit() for c in text))
            or ("-" in text and any(c.isdigit() for c in text))
            or text.lower().startswith("http")
        )  # URLs are not dates

    def _parse_grin_date(self, date_str: str) -> str | None:
        """Parse a date string from GRIN format to ISO format."""
        if not date_str:
            return None
        try:
            # GRIN format: YYYY/MM/DD HH:MM
            dt = datetime.strptime(date_str, "%Y/%m/%d %H:%M")
            return dt.isoformat()
        except ValueError:
            return date_str  # Return as-is if parsing fails

    def _get_field_or_none(self, fields: list[str], index: int) -> str | None:
        """Get field at index or None if index is out of bounds or field is empty."""
        if index < len(fields) and fields[index]:
            return fields[index]
        return None

    def _parse_date_field(self, fields: list[str], index: int) -> str | None:
        """Parse date field at index or return None if missing/empty."""
        field = self._get_field_or_none(fields, index)
        return self._parse_grin_date(field) if field else None

    def process_grin_row(self, row: GRINRow) -> dict:
        """Process a GRINRow dict from the client and map to BookRecord field names.

        Args:
            row: GRINRow dict from client with unstructured field names from HTML
        """
        if not row or not row.get("barcode"):
            return {}

        barcode = row.get("barcode", "unknown")

        # Create result with standard BookRecord field names
        result = {
            "barcode": barcode,
            "title": "",
            "scanned_date": None,
            "converted_date": None,
            "downloaded_date": None,
            "processed_date": None,
            "analyzed_date": None,
            "ocr_date": None,
            "google_books_link": "",
            "grin_state": None,
        }

        # Map unstructured field names to standard ones
        # Try different possible field names that might contain title
        for key, value in row.items():
            if not value:
                continue

            key_lower = key.lower()

            # Map title field (usually first column after barcode)
            if "title" in key_lower:
                result["title"] = str(value).strip()
            # Map various date fields
            elif "scanned" in key_lower:
                result["scanned_date"] = self._parse_date_field([value], 0)
            elif "converted" in key_lower:
                result["converted_date"] = self._parse_date_field([value], 0)
            elif "downloaded" in key_lower:
                result["downloaded_date"] = self._parse_date_field([value], 0)
            elif "processed" in key_lower:
                result["processed_date"] = self._parse_date_field([value], 0)
            elif "analyzed" in key_lower:
                result["analyzed_date"] = self._parse_date_field([value], 0)
            elif "ocr" in key_lower:
                result["ocr_date"] = self._parse_date_field([value], 0)
            # Map Google Books link
            elif "google" in key_lower or "books" in key_lower:
                if "http" in str(value):
                    result["google_books_link"] = str(value).strip()
            # Map state field
            elif "state" in key_lower or "status" in key_lower:
                result["grin_state"] = str(value).strip()

        return result

    async def collect_books(self, output_file: str, limit: int | None = None) -> bool:
        """
        Book collection with pagination.

        Processes books one at a time with reliable pagination and resume capability.

        Returns:
            True if collection completed successfully, False if interrupted or incomplete
        """
        print(f"Starting book collection to {output_file}")
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

            # Prepare CSV file
            output_path = Path(output_file)
            file_exists = output_path.exists()

            # Read existing barcodes if file exists
            existing_barcodes = set()
            if file_exists:
                try:
                    with open(output_path, newline="") as f:
                        reader = csv.reader(f)
                        next(reader)  # Skip header
                        for row in reader:
                            if row:
                                existing_barcodes.add(row[0])  # First column is barcode
                    existing_count = len(existing_barcodes)
                    print(f"Found {existing_count} existing {pluralize(existing_count, 'record')} in CSV")
                except Exception as e:
                    print(f"Warning: Could not read existing CSV: {e}")

            # Open CSV file for writing
            write_mode = "a" if file_exists else "w"

            async with aiofiles.open(output_file, mode=write_mode) as f:  # type: ignore[call-overload]
                # Write header if new file
                if not file_exists:
                    header_line = ",".join(BookRecord.csv_headers()) + "\n"
                    await f.write(header_line)

                # Start progress tracking
                self.start_time = time.time()
                processed_count = 0

                # Process books one by one using configured data mode
                last_book_time = time.time()
                book_count_in_loop = 0
                async for grin_row, known_barcodes_on_page in self.get_all_books():
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
                            f"Main loop received book #{book_count_in_loop} at {receive_time} "
                            f"(gap: {time_since_last:.2f}s)"
                        )

                    last_book_time = current_time

                    # Check for interrupt
                    if stop_event.is_set():
                        print("Collection interrupted - saving progress...")
                        completed_successfully = False
                        break

                    if limit and processed_count >= limit:
                        print(f"Reached limit of {limit} books")
                        break

                    # Extract barcode for checking
                    barcode = grin_row.get("barcode", "")
                    if not barcode:
                        continue

                    # Skip if already in CSV or processed (using batch SQLite results)
                    if barcode in existing_barcodes or barcode in known_barcodes_on_page:
                        continue

                    # Process the book
                    try:
                        record = await self.process_book(grin_row)
                        if record:
                            # Write to CSV immediately
                            csv_line = ",".join(f'"{field}"' for field in record.to_csv_row()) + "\n"
                            io_start = time.time()
                            await f.write(csv_line)
                            await f.flush()  # Ensure data is written immediately
                            io_elapsed = time.time() - io_start

                            # Log slow I/O operations
                            if io_elapsed > 1.0:  # More than 1 second for I/O
                                io_time = datetime.now().strftime("%H:%M:%S.%f")[:-3]
                                logger.warning(f"Slow CSV I/O: {io_elapsed:.2f}s at {io_time}")

                            processed_count += 1

                            # Show progress every N items
                            if processed_count % PROGRESS_UPDATE_FREQUENCY == 0:
                                extra_info = {"current": record.barcode} if record else None
                                show_progress(
                                    start_time=self.start_time,
                                    total_items=self.total_books_estimate,
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

                            # Force garbage collection periodically to prevent memory buildup
                            if processed_count % 10000 == 0:
                                import gc

                                gc.collect()
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

        finally:
            # Remove signal handler
            loop.remove_signal_handler(signal.SIGINT)

            # Show final completion message
            print(f"✓ Completed Book Collection: {processed_count:,} barcode records")

        # Return completion status
        return completed_successfully

    async def process_book(self, grin_row: GRINRow) -> BookRecord | None:
        """Process a single GRINRow from GRIN and return its record.
        Args:
            grin_row: GRINRow dict from client with parsed book data
        """

        # Process GRIN data directly from the row
        parsed_data = self.process_grin_row(grin_row)
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
