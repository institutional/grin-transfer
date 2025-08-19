#!/usr/bin/env python3
"""
Progress Reporter for Sync Pipeline

Provides real-time progress reporting for long-running sync operations
with ETA calculations and task monitoring.
"""

import asyncio
import logging
import time
from typing import TYPE_CHECKING

from grin_to_s3.common import SlidingWindowRateCalculator, format_duration

if TYPE_CHECKING:
    from grin_to_s3.sync.task_manager import TaskManager

from .tasks.task_types import TaskType

logger = logging.getLogger(__name__)

# Progress reporting intervals
INITIAL_PROGRESS_INTERVAL = 60  # 1 minute for first few reports
REGULAR_PROGRESS_INTERVAL = 600  # 10 minutes for subsequent reports
MAX_INITIAL_REPORTS = 3  # Number of initial reports before switching to regular interval


class SyncProgressReporter:
    """Reports progress for sync pipeline operations."""

    def __init__(
        self,
        task_manager: "TaskManager",
        books_to_process: int,
        concurrent_downloads: int,
        concurrent_uploads: int,
    ):
        """Initialize progress reporter.

        Args:
            task_manager: TaskManager instance to query for statistics
            books_to_process: Total number of books to process
            concurrent_downloads: Max concurrent downloads for display
            concurrent_uploads: Max concurrent uploads for display
        """
        self.manager = task_manager
        self.books_to_process = books_to_process
        self.concurrent_downloads = concurrent_downloads
        self.concurrent_uploads = concurrent_uploads
        self.shutdown_event = asyncio.Event()

    async def run(self, start_time: float, rate_calculator: SlidingWindowRateCalculator) -> None:
        """Main reporter loop that displays progress until shutdown."""
        last_report_time = 0.0
        initial_reports_count = 0

        while not self.shutdown_event.is_set():
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
                    await self._show_progress(start_time, current_time, rate_calculator, interval, last_report_time)

                    # Update tracking
                    last_report_time = current_time
                    if initial_reports_count < MAX_INITIAL_REPORTS:
                        initial_reports_count += 1

                # Sleep briefly to avoid busy-waiting, but check for shutdown more frequently
                for _ in range(10):  # Check every 0.1 seconds instead of every 1 second
                    if self.shutdown_event.is_set():
                        break
                    await asyncio.sleep(0.1)

            except Exception as e:
                logger.error(f"Error in progress reporter: {e}", exc_info=True)
                await asyncio.sleep(1)

    def request_shutdown(self) -> None:
        """Signal the reporter to stop gracefully."""
        self.shutdown_event.set()

    async def _show_progress(
        self,
        start_time: float,
        current_time: float,
        rate_calculator: SlidingWindowRateCalculator,
        interval: int,
        last_report_time: float,
    ) -> None:
        """Display current progress with statistics from TaskManager."""
        # Get active task counts from TaskManager
        downloads_active = self.manager.get_active_task_count(TaskType.DOWNLOAD)
        uploads_active = self.manager.get_active_task_count(TaskType.UPLOAD)

        # Calculate completed count from upload task statistics (upload is the final task)
        upload_stats = self.manager.stats[TaskType.UPLOAD]
        completed_count = upload_stats["completed"] + upload_stats["failed"]

        # Calculate progress metrics
        percentage = (completed_count / self.books_to_process) * 100 if self.books_to_process > 0 else 0
        elapsed = current_time - start_time
        rate = rate_calculator.get_rate(start_time, completed_count)

        # Calculate ETA
        remaining = self.books_to_process - completed_count
        eta_text = ""
        if rate > 0 and remaining > 0:
            eta_seconds = remaining / rate
            eta_text = f" (ETA: {format_duration(eta_seconds)})"

        # Calculate time until next update
        time_since_last_report = current_time - last_report_time
        time_until_next_update = max(0, interval - time_since_last_report)
        minutes_until_next = int(time_until_next_update // 60) + 1  # Round up to next minute
        interval_desc = f"next update in {minutes_until_next} min"

        # Build task status
        task_status = f"[{downloads_active}/{self.concurrent_downloads} downloads, {uploads_active}/{self.concurrent_uploads} uploads]"

        # Print progress line
        print(
            f"{completed_count:,}/{self.books_to_process:,} "
            f"({percentage:.1f}%) - {rate:.1f} books/sec - "
            f"elapsed: {format_duration(elapsed)}{eta_text} "
            f"{task_status} [{interval_desc}]"
        )
