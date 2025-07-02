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
from grin_to_s3.storage import BookStorage, create_storage_from_config

from .config import ExportConfig, PaginationConfig
from .models import BookRecord, BoundedSet, SQLiteProgressTracker

# Set up module logger
logger = logging.getLogger(__name__)


class BookCollector:
    """Main book collection orchestrator."""

    def __init__(
        self,
        directory: str,
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
        self.resume_count = 0
        self.total_books_estimate: int | None = None
        self.processing_rates: list[float] = []  # Track books/second over time

        # Pagination state for resume functionality
        pagination_config = self.config.pagination or PaginationConfig()
        self.pagination_state = {
            "current_page": pagination_config.start_page,
            "next_url": None,
            "page_size": pagination_config.page_size,
        }

        # Storage (optional)
        self.book_storage: BookStorage | None = None
        if storage_config:
            storage = create_storage_from_config(storage_config["type"], storage_config.get("config", {}))
            # Create bucket config for BookStorage constructor
            bucket_config = {
                "bucket_raw": storage_config.get("bucket_raw", ""),
                "bucket_meta": storage_config.get("bucket_meta", ""),
                "bucket_full": storage_config.get("bucket_full", ""),
            }
            self.book_storage = BookStorage(storage, bucket_config, base_prefix=storage_config.get("prefix", ""))

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

    async def _archive_progress_file(self) -> bool:
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
        if not self.resume_file.exists():
            return {"processed": [], "failed": []}

        try:
            async with aiofiles.open(self.resume_file) as f:
                content = await f.read()
                progress_data = json.loads(content)

                # Initialize SQLite tracker (no need to load barcode lists from JSON)
                await self.sqlite_tracker.init_db()

                # Load additional metadata if available
                if "job_metadata" in progress_data:
                    existing_metadata = progress_data["job_metadata"]

                    # Check for configuration changes
                    existing_config_hash = existing_metadata.get("export_parameters", {}).get("config_hash")
                    current_config_hash = self.job_metadata["export_parameters"]["config_hash"]

                    if existing_config_hash != current_config_hash:
                        print(f"""Warning: Export configuration has changed since last run
  Previous: {existing_config_hash}
  Current:  {current_config_hash}""")

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
                    print(f"  Pagination: Resume from page {self.pagination_state.get('current_page', 1)}")

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

                print(f"Resumed export (attempt #{self.resume_count})")
                print(f"  Progress: {processed_count} processed, {failed_count} failed")
                print(f"  Running time: {elapsed_time}")
                if "job_metadata" in progress_data:
                    started_by = progress_data["job_metadata"].get("started_by_user", "unknown")
                    hostname = progress_data["job_metadata"].get("hostname", "unknown")
                    print(f"  Started by: {started_by}@{hostname}")

                return progress_data

        except (json.JSONDecodeError, Exception) as e:
            print(f"Warning: Could not load progress file: {e}")
            return {"processed": [], "failed": []}

    async def save_progress(self):
        """Save current progress to resume file."""
        now = datetime.now(UTC)

        # Calculate performance metrics based on actual runtime, not wall-clock time
        if hasattr(self, "session_start_time"):
            # Current session runtime
            session_elapsed = (now - self.session_start_time).total_seconds()
            # Add to accumulated runtime from previous sessions
            total_runtime = getattr(self, "accumulated_runtime", 0.0) + session_elapsed
        else:
            # Fallback: estimate based on processing rate
            total_processed = await self.sqlite_tracker.get_processed_count()
            if total_processed > 0:
                # Assume reasonable processing rate for estimates
                estimated_rate = 15  # books per minute (conservative estimate)
                total_runtime = (total_processed / estimated_rate) * 60
            else:
                total_runtime = 0.0

        performance_metrics = await self._calculate_performance_metrics(total_runtime)

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
            # Performance metrics
            "performance_metrics": performance_metrics,
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

    async def save_pagination_state(self, pagination_state: dict):
        """Save pagination state for resume functionality."""
        self.pagination_state.update(pagination_state)
        # Save progress immediately to persist pagination state
        await self.save_progress()

    async def get_all_books_html(self) -> AsyncGenerator[tuple[str, set[str]], None]:
        """Stream all book data from GRIN using HTML pagination with large page sizes."""
        logger.info("Streaming all books from GRIN...")

        # Determine starting point for pagination
        start_page = self.pagination_state.get("current_page", 1)
        start_url = self.pagination_state.get("next_url")
        if start_url is not None and not isinstance(start_url, str):
            start_url = None  # Reset invalid start_url
        pagination_config = self.config.pagination or PaginationConfig()
        page_size = self.pagination_state.get("page_size", pagination_config.page_size)

        if start_page is not None and start_page > 1:
            print(f"Resuming pagination from page {start_page}")

        book_count = 0
        # Use prefetch version if enabled
        if self.config.enable_prefetch:
            stream_method = self.client.stream_book_list_html_prefetch
            async for book_line, known_barcodes in stream_method(
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
                    logger.info(f"Streamed {book_count:,} {pluralize(book_count, 'book')}...")
                    # Update total estimate more aggressively during large streams
                    if book_count > 50000:
                        self.total_books_estimate = book_count + 100000
        else:
            # Fall back to non-prefetch version - yield with empty known set
            async for book_line in self.client.stream_book_list_html(
                self.directory,
                list_type="_all_books",
                page_size=page_size or pagination_config.page_size,
                max_pages=pagination_config.max_pages,
                start_page=start_page or 1,
                start_url=start_url,
                pagination_callback=self.save_pagination_state,
            ):
                yield book_line.strip(), set()
                book_count += 1

                # Update total estimate as we stream
                if not self.total_books_estimate or book_count > self.total_books_estimate:
                    self.total_books_estimate = book_count + 50000  # Conservative estimate

                if book_count % 5000 == 0:
                    logger.info(f"Streamed {book_count:,} {pluralize(book_count, 'book')}...")
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
        """Stream all book data from GRIN using HTML pagination, plus converted books."""
        # First, yield books from the _converted list
        converted_count = 0
        async for book_line, known_barcodes in self.get_converted_books():
            converted_count += 1
            yield book_line, known_barcodes

        if converted_count > 0:
            print(f"Finished processing {converted_count:,} converted books, now continuing with full catalog...")

        # Then, yield books from the main _all_books catalog
        async for book_line, known_barcodes in self.get_all_books_html():
            yield book_line, known_barcodes

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

    def parse_grin_line(self, line: str) -> dict:
        """Parse a line from GRIN _all_books output or a simple barcode."""
        fields = line.strip().split("\t")
        if len(fields) < 1 or not fields[0]:  # Must have at least barcode
            return {}

        # Handle barcode-only input (from converted books list)
        if len(fields) == 1 and not any("\t" in line for line in [line]):
            # This is just a barcode, create minimal record
            barcode = fields[0]
            return {
                "barcode": barcode,
                "title": "",  # Will be enriched later if needed
                "scanned_date": None,
                "converted_date": None,
                "downloaded_date": None,
                "processed_date": None,
                "analyzed_date": None,
                "ocr_date": None,
                "google_books_link": "",
            }

        # Pad fields to ensure we have at least 9 elements
        while len(fields) < 9:
            fields.append("")

        def parse_date(date_str: str) -> str | None:
            if not date_str:
                return None
            try:
                # GRIN format: YYYY/MM/DD HH:MM
                dt = datetime.strptime(date_str, "%Y/%m/%d %H:%M")
                return dt.isoformat()
            except ValueError:
                return date_str  # Return as-is if parsing fails

        # GRIN HTML output format (from GRINTableParser):
        # fields[0] = barcode (from input checkbox value)
        # fields[1] = barcode (duplicate from table cell)
        # fields[2] = title
        # fields[3] = unknown/empty field
        # fields[4] = scanned_date
        # fields[5] = analyzed_date
        # fields[6] = converted_date
        # fields[7] = downloaded_date
        # fields[8+] = additional fields if present (might include Google Books link)

        # Check if this is HTML format (has title in field 2) vs test format (has date in field 2)
        if len(fields) > 2 and fields[2] and not self._looks_like_date(fields[2]):
            # HTML table format - title in field[2]
            # Look for Google Books link in additional fields
            google_link = ""
            for i in range(8, len(fields)):
                if fields[i] and ("books.google.com" in fields[i] or fields[i].startswith("http")):
                    google_link = fields[i]
                    break

            return {
                "barcode": fields[0],
                "title": fields[2],
                "scanned_date": parse_date(fields[4]) if len(fields) > 4 else None,
                "converted_date": parse_date(fields[6]) if len(fields) > 6 else None,
                "downloaded_date": parse_date(fields[7]) if len(fields) > 7 else None,
                "processed_date": None,  # Not available in HTML format
                "analyzed_date": parse_date(fields[5]) if len(fields) > 5 else None,
                "ocr_date": None,  # Not available in HTML format
                "google_books_link": google_link,
            }
        else:
            # Test/legacy text format
            return {
                "barcode": fields[0],
                "title": "",  # Title not available in text format
                "scanned_date": parse_date(fields[1]),
                "converted_date": parse_date(fields[2]),
                "downloaded_date": parse_date(fields[3]),
                "processed_date": parse_date(fields[4]),
                "analyzed_date": parse_date(fields[5]),
                # field[6] is unknown/blank
                "ocr_date": parse_date(fields[7]) if len(fields) > 7 else None,
                "google_books_link": fields[8] if len(fields) > 8 else "",
            }

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
        await self._archive_progress_file()

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
            progress_data = await self.load_progress()

            # Initialize session timing
            self.session_start_time = datetime.now(UTC)

            # Load accumulated runtime from previous sessions
            if "runtime_tracking" in progress_data:
                self.accumulated_runtime = progress_data["runtime_tracking"].get("total_runtime_seconds", 0.0)
            else:
                self.accumulated_runtime = 0.0

            # Get processing states
            processing_states = await self.get_processing_states()

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
                        record = await self.process_book(grin_line, processing_states)
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

                            # Save progress periodically
                            if processed_count % 100 == 0:
                                await self.save_progress()

                            # Force garbage collection periodically to prevent memory buildup
                            if processed_count % 10000 == 0:
                                import gc

                                gc.collect()

                    except Exception as e:
                        print(f"Error processing {barcode}: {e}")
                        await self.sqlite_tracker.mark_failed(barcode, str(e))
                        self.recent_failed.add(barcode)
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
        actual_runtime = getattr(self, "accumulated_runtime", 0.0)
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

    async def process_book(self, grin_line: str, processing_states: dict[str, set[str]]) -> BookRecord | None:
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

            # Determine processing state
            # Processing state is now tracked in status history table
            # No need to set processing_state field

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
