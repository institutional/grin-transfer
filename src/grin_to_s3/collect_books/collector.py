#!/usr/bin/env python3
"""
Book Collection main orchestrator.

Contains the BookCollector class responsible for coordinating the entire book collection process.
"""

import asyncio
import csv
import errno
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

import aiofiles

from grin_to_s3.client import GRINClient
from grin_to_s3.common import (
    BackupManager,
    ProgressReporter,
    RateLimiter,
    format_bytes,
    format_duration,
    pluralize,
)
from grin_to_s3.storage import BookManager, create_storage_from_config
from grin_to_s3.storage.book_manager import BucketConfig

from .config import ExportConfig, PaginationConfig
from .models import BookRecord, BoundedSet, SQLiteProgressTracker
from .progress import PaginationState, ProgressTracker

# Set up module logger
logger = logging.getLogger(__name__)


# PaginationState moved to progress.py


class BookCollector:
    """Main book collection orchestrator."""

    def __init__(
        self,
        directory: str,
        process_summary_stage,
        rate_limit: float = 1.0,
        storage_config: dict | None = None,
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
        self.client = GRINClient(secrets_dir=secrets_dir)
        self.rate_limiter = RateLimiter(self.config.rate_limit)
        self.storage_config = storage_config
        self.resume_file = Path(self.config.resume_file)

        # Set up test mode if requested
        if test_mode:
            self._setup_test_mode()

        # Progress tracking
        self.progress = ProgressReporter("Book Collection")
        self.sqlite_tracker = SQLiteProgressTracker(self.config.sqlite_db_path)
        # Keep small in-memory sets for recent items only (performance optimization)
        self.recent_processed = BoundedSet(max_size=self.config.recent_cache_size)
        self.recent_failed = BoundedSet(max_size=self.config.recent_failed_cache_size)

        # File locking for concurrent session prevention
        self._lock_file: Path | None = None
        self._lock_fd: int | None = None

        # Job metadata
        self.job_metadata = self._create_job_metadata(rate_limit, storage_config)
        # Initialize progress tracker after job_metadata is available
        self.progress_tracker = ProgressTracker(self.resume_file, self.job_metadata, self.sqlite_tracker)
        # Progress tracking attributes for backward compatibility
        self.resume_count = 0
        self.total_books_estimate: int | None = None
        self.processing_rates: list[float] = []  # Track books/second over time

        # Pagination state for resume functionality
        pagination_config = self.config.pagination or PaginationConfig()
        self.pagination_state: PaginationState = {
            "current_page": pagination_config.start_page,
            "next_url": None,
            "page_size": pagination_config.page_size,
        }
        # Initialize progress tracker pagination state
        self.progress_tracker.pagination_state = self.pagination_state

        # Storage (optional)
        self.book_storage: BookManager | None = None
        if storage_config:
            storage = create_storage_from_config(storage_config["type"], storage_config.get("config", {}))
            # Create bucket config for BookStorage constructor
            bucket_config: BucketConfig = {
                "bucket_raw": storage_config.get("bucket_raw", ""),
                "bucket_meta": storage_config.get("bucket_meta", ""),
                "bucket_full": storage_config.get("bucket_full", ""),
            }
            prefix = storage_config.get("prefix", "")
            self.book_storage = BookManager(storage, bucket_config=bucket_config, base_prefix=prefix)

    def _setup_test_mode(self):
        """Set up test mode with mock data and clients"""
        # Import mocks locally to avoid dependency issues when not in test mode
        try:
            from tests.mocks import MockBookStorage, MockGRINClient, MockStorage, get_test_data

            # Replace client with mock
            self.client = MockGRINClient(get_test_data())  # type: ignore[assignment]

            # Replace storage with mock if storage config exists
            if self.storage_config:
                mock_book_storage = MockBookStorage()
                mock_book_storage.storage = MockStorage()  # type: ignore[attr-defined]
                self.book_storage = mock_book_storage  # type: ignore[assignment]

            logger.info("Test mode enabled - using mock data (no network calls)")

        except ImportError:
            logger.warning("Test mode requested but mocks not available")
            logger.warning("Falling back to regular mode")

    def _create_job_metadata(self, rate_limit: float, storage_config: dict | None) -> dict:
        """Create comprehensive job metadata for progress tracking."""
        now = datetime.now(UTC)

        # Create configuration hash for detecting parameter changes
        config_str = json.dumps(
            {"directory": self.directory, "rate_limit": rate_limit, "storage_config": storage_config}, sort_keys=True
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

    async def _calculate_performance_metrics(self, elapsed_seconds: float) -> dict:
        """Calculate current performance metrics."""
        total_processed = await self.sqlite_tracker.get_processed_count()

        if elapsed_seconds > 0 and total_processed > 0:
            books_per_second = total_processed / elapsed_seconds
            books_per_hour = books_per_second * 3600

            # Estimate completion time if we have total estimate
            eta_hours = None
            if self.total_books_estimate and books_per_hour > 0:
                remaining = self.total_books_estimate - total_processed
                eta_hours = remaining / books_per_hour
        else:
            books_per_second = 0.0
            books_per_hour = 0.0
            eta_hours = None

        return {
            "books_per_second": round(books_per_second, 3),
            "books_per_hour": round(books_per_hour, 1),
            "estimated_completion_hours": round(eta_hours, 1) if eta_hours else None,
        }

    def _acquire_session_lock(self, output_file: str) -> bool:
        """Acquire exclusive lock to prevent concurrent sessions.

        Creates lock files for both the output CSV and progress file to prevent
        multiple sessions from corrupting each other's work.

        Returns True if lock acquired successfully, False if another session is running.
        """
        try:
            # Create lock file paths
            output_path = Path(output_file)
            progress_lock_file = self.resume_file.with_suffix(".lock")
            output_path.with_suffix(".lock")

            # Ensure the progress directory exists for lock files
            progress_lock_file.parent.mkdir(parents=True, exist_ok=True)

            # Try to acquire progress file lock first
            try:
                self._lock_fd = os.open(str(progress_lock_file), os.O_CREAT | os.O_EXCL | os.O_WRONLY)
                self._lock_file = progress_lock_file
            except OSError as e:
                if e.errno == errno.EEXIST:
                    # Check if the process is still running
                    if self._is_lock_stale(progress_lock_file):
                        logger.info(f"Removing stale lock file: {progress_lock_file}")
                        progress_lock_file.unlink(missing_ok=True)
                        # Retry lock acquisition
                        self._lock_fd = os.open(str(progress_lock_file), os.O_CREAT | os.O_EXCL | os.O_WRONLY)
                        self._lock_file = progress_lock_file
                    else:
                        return False
                else:
                    raise

            # Write lock file content with session info
            lock_info = {
                "pid": os.getpid(),
                "hostname": socket.gethostname(),
                "started_at": datetime.now(UTC).isoformat(),
                "user": os.getenv("USER") or os.getenv("USERNAME") or "unknown",
                "output_file": str(output_path.absolute()),
                "progress_file": str(self.resume_file.absolute()),
            }

            os.write(self._lock_fd, json.dumps(lock_info, indent=2).encode())
            os.fsync(self._lock_fd)

            logger.info(f"Session lock acquired: {progress_lock_file.name}")
            return True

        except Exception as e:
            logger.warning(f"Failed to acquire session lock: {e}")
            return False

    def _is_lock_stale(self, lock_file: Path) -> bool:
        """Check if a lock file is stale (process no longer running)."""
        try:
            with open(lock_file) as f:
                lock_info = json.load(f)

            pid = lock_info.get("pid")
            if not pid:
                return True

            # Check if process is still running
            try:
                os.kill(pid, 0)  # Signal 0 checks if process exists
                return False  # Process still running
            except OSError:
                return True  # Process not found

        except (json.JSONDecodeError, FileNotFoundError, KeyError):
            return True  # Invalid lock file

    def _release_session_lock(self):
        """Release the session lock."""
        if self._lock_fd is not None:
            try:
                os.close(self._lock_fd)
                self._lock_fd = None
            except OSError:
                pass

        if self._lock_file and self._lock_file.exists():
            try:
                self._lock_file.unlink()
                logger.info(f"Session lock released: {self._lock_file.name}")
            except OSError:
                pass
            self._lock_file = None

    def _check_concurrent_session(self, output_file: str) -> bool:
        """Check if another session is already running with same files.

        Returns True if it's safe to proceed, False if another session detected.
        """
        Path(output_file)
        progress_lock_file = self.resume_file.with_suffix(".lock")

        # Check progress file lock
        if progress_lock_file.exists():
            if not self._is_lock_stale(progress_lock_file):
                try:
                    with open(progress_lock_file) as f:
                        lock_info = json.load(f)

                    logger.error("Another CSV export session is already running:")
                    logger.error(f"  PID: {lock_info.get('pid', 'unknown')}")
                    logger.error(f"  User: {lock_info.get('user', 'unknown')}")
                    logger.error(f"  Host: {lock_info.get('hostname', 'unknown')}")
                    logger.error(f"  Started: {lock_info.get('started_at', 'unknown')}")
                    logger.error(f"  Progress file: {lock_info.get('progress_file', 'unknown')}")
                    logger.error(f"  Output file: {lock_info.get('output_file', 'unknown')}")
                    logger.error("Wait for the other session to complete or stop it before starting a new one.")
                    return False

                except (json.JSONDecodeError, FileNotFoundError):
                    # Invalid lock file, remove it
                    progress_lock_file.unlink(missing_ok=True)

        return True

    # Progress file archiving moved to progress.py

    async def _backup_database(self) -> bool:
        """Create a timestamped backup of the SQLite database before starting work.

        Returns True if backup was successful or not needed, False if failed.
        """
        db_path = Path(self.sqlite_tracker.db_path)
        backup_dir = db_path.parent / "backups"

        # Use shared backup manager
        backup_manager = BackupManager(backup_dir)
        return await backup_manager.backup_file(db_path, "database")

    async def load_progress(self) -> dict:
        """Load progress from resume file."""
        return await self.progress_tracker.load_progress()

    async def save_progress(self):
        """Save current progress to resume file."""
        await self.progress_tracker.save_progress(self._calculate_performance_metrics)

    async def get_processing_states(self) -> dict[str, set[str]]:
        """Get book processing states from GRIN endpoints."""
        states: dict[str, set[str]] = {"converted": set(), "failed": set(), "all_books": set()}

        logger.info("Fetching processing states from GRIN...")

        # Get converted books
        try:
            await self.rate_limiter.acquire()
            converted_text = await self.client.fetch_resource(self.directory, "_converted?format=text")
            for line in converted_text.strip().split("\n"):
                if line.strip():
                    barcode = line.split("\t")[0]
                    states["converted"].add(barcode)
            converted_count = len(states["converted"])
            logger.info(f"Found {converted_count} converted {pluralize(converted_count, 'book')}")
        except Exception as e:
            logger.warning(f"Could not fetch converted books: {e}")

        # Get failed books
        try:
            await self.rate_limiter.acquire()
            failed_text = await self.client.fetch_resource(self.directory, "_failed?format=text")
            for line in failed_text.strip().split("\n"):
                if line.strip():
                    barcode = line.split("\t")[0]
                    states["failed"].add(barcode)
            failed_count = len(states["failed"])
            logger.info(f"Found {failed_count} failed {pluralize(failed_count, 'book')}")
        except Exception as e:
            logger.warning(f"Could not fetch failed books: {e}")

        return states

    async def save_pagination_state(self, pagination_state: PaginationState):
        """Save pagination state for resume functionality."""
        await self.progress_tracker.save_pagination_state(pagination_state)
        # Update local state for backward compatibility
        self.pagination_state.update(pagination_state)

    async def get_converted_books_html(self) -> AsyncGenerator[tuple[str, set[str]], None]:
        """Stream converted books from GRIN using HTML pagination with full metadata."""
        logger.info("Streaming converted books from GRIN...")

        # Use same pagination settings as _all_books
        pagination_config = self.config.pagination or PaginationConfig()

        book_count = 0
        async for book_line, known_barcodes in self.client.stream_book_list_html_prefetch(
            self.directory,
            list_type="_converted",
            page_size=pagination_config.page_size,
            max_pages=pagination_config.max_pages,
            start_page=1,
            pagination_callback=None,  # Don't save pagination state for converted books
            sqlite_tracker=self.sqlite_tracker,
        ):
            yield book_line.strip(), known_barcodes
            book_count += 1

            if book_count % 1000 == 0:
                logger.info(f"Streamed {book_count:,} converted {pluralize(book_count, 'book')}...")

    async def get_all_books_html(self) -> AsyncGenerator[tuple[str, set[str]], None]:
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
        async for book_line, known_barcodes in self.client.stream_book_list_html_prefetch(
            self.directory,
            list_type="_all_books",
            page_size=page_size or pagination_config.page_size,
            max_pages=pagination_config.max_pages,
            start_page=start_page or 1,
            start_url=start_url,
            pagination_callback=self.save_pagination_state,
            sqlite_tracker=self.sqlite_tracker,
        ):
            yield book_line.strip(), known_barcodes
            book_count += 1

            # Update total estimate as we stream
            if not self.total_books_estimate or book_count > self.total_books_estimate:
                self.total_books_estimate = book_count + 50000  # Conservative estimate

            if book_count % 5000 == 0:
                logger.info(f"Streamed {book_count:,} non-converted {pluralize(book_count, 'book')}...")
                # Update total estimate more aggressively during large streams
                if book_count > 50000:
                    self.total_books_estimate = book_count + 100000


    async def get_converted_books(self) -> AsyncGenerator[tuple[str, set[str]], None]:
        """Stream books from GRIN's _converted list."""
        try:
            print("Fetching converted books from GRIN...")
            response_text = await self.client.fetch_resource(self.directory, "_converted?format=text")
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

    async def get_all_books(self) -> AsyncGenerator[tuple[str, set[str]], None]:
        """Stream all book data from GRIN using two-pass collection with full metadata.

        First pass: Collect converted books with full metadata from _converted endpoint
        Second pass: Collect non-converted books from _all_books endpoint

        Note: _all_books endpoint actually returns 'all books except converted', not truly all books.
        """
        # First pass: Get converted books with full metadata
        print("Phase 1: Collecting converted books with full metadata...")
        converted_count = 0
        async for book_line, known_barcodes in self.get_converted_books_html():
            yield book_line, known_barcodes
            converted_count += 1

        if converted_count > 0:
            print(f"Phase 1 complete: {converted_count:,} converted books collected")

        # Second pass: Get non-converted books from _all_books
        print("Phase 2: Collecting non-converted books...")
        non_converted_count = 0
        async for book_line, known_barcodes in self.get_all_books_html():
            yield book_line, known_barcodes
            non_converted_count += 1

        if non_converted_count > 0:
            print(f"Phase 2 complete: {non_converted_count:,} non-converted books collected")

        total_books = converted_count + non_converted_count
        print(f"Two-pass collection complete: {total_books:,} total books ({converted_count:,} converted + {non_converted_count:,} non-converted)")

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

    def _has_status_field(self, text: str) -> bool:
        """Check if text contains a GRIN status field."""
        return any(status in text for status in ["NOT_AVAILABLE", "PREVIOUSLY_DOWNLOADED", "AVAILABLE"])

    def _extract_google_books_link(self, text: str) -> str:
        """Extract Google Books link if valid."""
        if text and ("books.google.com" in text or text.startswith("http")):
            return text
        return ""

    def _detect_grin_format(self, fields: list[str]) -> str:
        """Detect the GRIN line format type."""
        # Check if this is _converted format (barcode in field 0, title in field 1)
        # The _converted format has 9 fields, a non-empty title in field 1, and scanned_date in field 2
        if (
            len(fields) >= 9 and
            len(fields[1]) > 0 and
            not self._looks_like_date(fields[1]) and
            len(fields[2]) > 0 and
            self._looks_like_date(fields[2])
        ):
            return "converted"

        # Check if this is _all_books format without checkbox (barcode in field 0, title in field 1, status in field 2)
        if (
            len(fields) >= 3 and
            len(fields[1]) > 0 and
            not self._looks_like_date(fields[1]) and
            len(fields[2]) > 0 and
            self._has_status_field(fields[2])
        ):
            return "all_books_no_checkbox"

        # Check if this is _all_books HTML format (has title in field 2)
        if len(fields) > 2 and fields[2] and not self._looks_like_date(fields[2]):
            return "html_table"

        # Default to plain text format
        return "text"

    def _parse_converted_format(self, fields: list[str]) -> dict:
        """Parse _converted HTML format."""
        # _converted HTML format:
        # fields[0] = barcode
        # fields[1] = title
        # fields[2] = scanned_date
        # fields[3] = processed_date
        # fields[4] = analyzed_date
        # fields[5] = ? (additional date)
        # fields[6] = ? (additional date)
        # fields[7] = ? (additional date)
        # fields[8] = google_books_link
        return {
            "barcode": fields[0],
            "title": self._get_field_or_none(fields, 1) or "",
            "scanned_date": self._parse_date_field(fields, 2),
            "processed_date": self._parse_date_field(fields, 3),
            "analyzed_date": self._parse_date_field(fields, 4),
            "converted_date": None,  # Not directly available in _converted format
            "downloaded_date": None,
            "ocr_date": None,
            "google_books_link": self._get_field_or_none(fields, 8) or "",
        }

    def _parse_all_books_no_checkbox(self, fields: list[str]) -> dict:
        """Parse _all_books HTML format without checkbox."""
        # _all_books HTML format without checkbox:
        # fields[0] = barcode
        # fields[1] = title
        # fields[2] = status
        # fields[3] = dates (if present)
        # Limited date information available
        return {
            "barcode": fields[0],
            "title": self._get_field_or_none(fields, 1) or "",
            "grin_state": self._get_field_or_none(fields, 2),
            "scanned_date": self._parse_date_field(fields, 3),
            "analyzed_date": self._parse_date_field(fields, 4),
            "converted_date": self._parse_date_field(fields, 5),
            "downloaded_date": self._parse_date_field(fields, 6),
            "processed_date": self._parse_date_field(fields, 7),
            "ocr_date": self._parse_date_field(fields, 8),
            "google_books_link": self._get_field_or_none(fields, 9) or "",
        }

    def _parse_html_table_format(self, fields: list[str]) -> dict:
        """Parse HTML table format."""
        # HTML table format - title in field[2]
        # Extract Google Books link from field 10
        google_link = self._extract_google_books_link(self._get_field_or_none(fields, 10) or "")

        return {
            "barcode": fields[0],  # Barcode from checkbox input
            "title": fields[2],
            "grin_state": self._get_field_or_none(fields, 3),  # Status field
            "scanned_date": self._parse_date_field(fields, 4),
            "analyzed_date": self._parse_date_field(fields, 5),
            "converted_date": self._parse_date_field(fields, 6),
            "downloaded_date": self._parse_date_field(fields, 7),
            "processed_date": self._parse_date_field(fields, 8),
            "ocr_date": self._parse_date_field(fields, 9),
            "google_books_link": google_link,
        }

    def _parse_text_format(self, fields: list[str]) -> dict:
        """Parse plain text format."""
        return {
            "barcode": fields[0],
            "title": "",
            "scanned_date": self._parse_date_field(fields, 1),
            "converted_date": self._parse_date_field(fields, 2),
            "downloaded_date": self._parse_date_field(fields, 3),
            "processed_date": self._parse_date_field(fields, 4),
            "analyzed_date": self._parse_date_field(fields, 5),
            # field[6] is unknown/blank
            "ocr_date": self._parse_date_field(fields, 7),
            "google_books_link": self._get_field_or_none(fields, 8) or "",
        }

    def _create_barcode_only_record(self, barcode: str) -> dict:
        """Create minimal record for barcode-only input."""
        return {
            "barcode": barcode,
            "title": "",
            "scanned_date": None,
            "converted_date": None,
            "downloaded_date": None,
            "processed_date": None,
            "analyzed_date": None,
            "ocr_date": None,
            "google_books_link": "",
        }

    def parse_grin_line(self, line: str) -> dict:
        """Parse a line from GRIN _all_books output or a simple barcode."""
        fields = line.strip().split("\t")
        if len(fields) < 1 or not fields[0]:  # Must have at least barcode
            return {}

        # Handle barcode-only input (from converted books list)
        if len(fields) == 1 and not any("\t" in line for line in [line]):
            return self._create_barcode_only_record(fields[0])

        # Pad fields to ensure we have at least 11 elements
        while len(fields) < 11:
            fields.append("")

        # Detect format and parse accordingly
        format_type = self._detect_grin_format(fields)
        if format_type == "converted":
            return self._parse_converted_format(fields)
        elif format_type == "all_books_no_checkbox":
            return self._parse_all_books_no_checkbox(fields)
        elif format_type == "html_table":
            return self._parse_html_table_format(fields)
        else:  # text
            return self._parse_text_format(fields)

    async def enrich_book_record(self, record: BookRecord) -> BookRecord:
        """Enrich book record with storage and metadata information."""

        # Note: Storage checks removed since BookRecord doesn't have
        # archive_exists, archive_retrieved, text_json_exists attributes
        # Storage functionality should be handled separately if needed

        # Set timestamps
        now = datetime.now(UTC).isoformat()
        if not record.csv_exported:
            record.csv_exported = now
        record.csv_updated = now

        return record

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

        # Validate credentials before starting export
        logger.debug("Validating GRIN credentials...")
        try:
            await self.client.auth.validate_credentials(self.directory)
        except Exception as e:
            print(f"Credential validation failed: {e}")
            print("Collection cannot continue without valid credentials.")
            return False

        # Check for concurrent sessions before starting
        print("Checking for concurrent sessions...")
        if not self._check_concurrent_session(output_file):
            return False

        # Acquire session lock to prevent concurrent access
        print("Acquiring session lock...")
        if not self._acquire_session_lock(output_file):
            print("❌ Failed to acquire session lock. Another session may be running.")
            return False

        # Archive existing progress file before starting execution
        logger.debug("Backing up progress file...")
        await self.progress_tracker.archive_progress_file()

        # Backup database before starting work
        logger.debug("Backing up SQLite database...")
        await self._backup_database()

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
            self.progress_tracker.start_session()
            self.session_start_time = datetime.now(UTC)  # Keep for backward compatibility

            # Runtime tracking is now handled by progress_tracker

            # Book conversion status is determined from converted_date field in all_books data

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
                self.progress.start()
                processed_count = 0

                # Process books one by one using configured data mode
                last_book_time = time.time()
                book_count_in_loop = 0
                async for grin_line, known_barcodes_on_page in self.get_all_books():
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
                    barcode = grin_line.split("\t")[0] if grin_line else ""
                    if not barcode:
                        continue

                    # Skip if already in CSV or processed (using batch SQLite results)
                    if barcode in existing_barcodes or barcode in known_barcodes_on_page:
                        continue

                    # Process the book
                    try:
                        record = await self.process_book(grin_line)
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
                            self.progress.increment(1, record_id=record.barcode)

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

            # Release session lock
            self._release_session_lock()

            # Finish progress tracking
            self.progress.finish()

        # Final summary
        actual_runtime = self.progress_tracker.accumulated_runtime
        if hasattr(self, "session_start_time"):
            session_elapsed = (datetime.now(UTC) - self.session_start_time).total_seconds()
            actual_runtime += session_elapsed

        job_start_time = datetime.fromisoformat(self.job_metadata["job_started"].replace("Z", "+00:00"))
        wall_clock_elapsed = (datetime.now(UTC) - job_start_time).total_seconds()
        performance_metrics = await self._calculate_performance_metrics(actual_runtime)

        print()
        print("=" * 60)
        print("Book Collection Summary")
        print("=" * 60)

        # Basic stats from SQLite
        total_books = await self.sqlite_tracker.get_processed_count()
        print(f"{pluralize(total_books, 'Record')} processed: {total_books:,}")
        failed_count = await self.sqlite_tracker.get_failed_count()
        print(f"Failed {pluralize(failed_count, 'record')}: {failed_count:,}")
        print(f"Success rate: {((total_books / max(1, total_books + failed_count)) * 100):.1f}%")

        # Output info
        output_path = Path(output_file)
        print(f"Output file: {output_path}")
        if output_path.exists():
            print(f"File size: {format_bytes(output_path.stat().st_size)}")

            # Count total records in final CSV file
            try:
                with open(output_path) as f:
                    csv_reader = csv.reader(f)
                    next(csv_reader)  # Skip header
                    total_csv_records = sum(1 for row in csv_reader if row)
                print(f"Total records in CSV: {total_csv_records:,}")
            except Exception as e:
                print(f"Could not count CSV records: {e}")

        # Performance metrics
        print()
        print("Performance:")
        print(f"  Actual runtime: {format_duration(actual_runtime)}")
        print(f"  Wall-clock elapsed: {format_duration(wall_clock_elapsed)}")
        print(f"  Processing rate: {performance_metrics['books_per_hour']:.1f} books/hour")
        print(f"  Resume count: {self.resume_count}")

        print(f"\nProgress file: {self.resume_file}")

        # Return completion status
        return completed_successfully

    async def process_book(self, grin_line: str) -> BookRecord | None:
        """Process a single book line from GRIN and return its record."""

        # Parse GRIN data directly from the line
        parsed_data = self.parse_grin_line(grin_line)
        if not parsed_data or not parsed_data.get("barcode"):
            return None

        barcode = parsed_data["barcode"]

        if await self.sqlite_tracker.is_processed(barcode):
            return None  # Already processed

        try:
            # Create record
            record = BookRecord(**parsed_data)

            # Book conversion status is available in converted_date field

            # Enrich record with timestamps and storage info
            record = await self.enrich_book_record(record)

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
