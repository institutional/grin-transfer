"""
Progress tracking for book collection operations.

This module handles persistence of collection progress including:
- Progress file archiving and backup
- Loading and saving progress data
- Pagination state management
- Runtime tracking and performance metrics
"""

import json
import logging
from datetime import UTC, datetime
from pathlib import Path
from typing import TypedDict

import aiofiles

from grin_to_s3.common import format_duration

logger = logging.getLogger(__name__)


class PaginationState(TypedDict):
    """Type for pagination state dictionary."""

    current_page: int
    next_url: str | None
    page_size: int


class ProgressTracker:
    """Manages progress tracking for book collection operations."""

    def __init__(self, resume_file: Path, job_metadata: dict, sqlite_tracker):
        self.resume_file = resume_file
        self.job_metadata = job_metadata
        self.sqlite_tracker = sqlite_tracker

        # Progress tracking state
        self.resume_count = 0
        self.total_books_estimate: int | None = None
        self.pagination_state: PaginationState = {"current_page": 1, "next_url": None, "page_size": 5000}
        self.accumulated_runtime = 0.0
        self.session_start_time: datetime | None = None

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

                # Initialize SQLite tracker (no need to load barcode lists from JSON)
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

    async def save_progress(self, performance_calculator=None):
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

        # Calculate performance metrics if calculator provided
        if performance_calculator:
            performance_metrics = await performance_calculator(total_runtime)
        else:
            performance_metrics = {}

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
