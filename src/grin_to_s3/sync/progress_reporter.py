#!/usr/bin/env python3
"""
Progress Reporter for Sync Pipeline

Provides rate calculation utilities for sync operations.
"""

import logging
import time

from grin_to_s3.common import format_duration

logger = logging.getLogger(__name__)


class SlidingWindowRateCalculator:
    """
    Calculate processing rates using a sliding window for more accurate ETAs.

    This prevents ETAs from being skewed by startup overhead or early slow batches
    by using only the most recent batch completions for rate calculation.
    """

    def __init__(self, window_size: int = 5):
        """
        Initialize the rate calculator.

        Args:
            window_size: Number of recent batches to consider for rate calculation
        """
        self.window_size = window_size
        self.batch_times: list[tuple[float, int]] = []  # (timestamp, processed_count)

    def add_batch(self, timestamp: float, processed_count: int) -> None:
        """
        Add a batch completion record.

        Args:
            timestamp: Time when batch was completed
            processed_count: Cumulative number of items processed
        """
        self.batch_times.append((timestamp, processed_count))

        # Keep only recent batches for rate calculation
        if len(self.batch_times) > self.window_size:
            self.batch_times.pop(0)

    def get_rate(self, fallback_start_time: float, fallback_processed_count: int) -> float:
        """
        Calculate current processing rate based on sliding window.

        Args:
            fallback_start_time: Start time for fallback rate calculation
            fallback_processed_count: Total processed count for fallback

        Returns:
            Processing rate in items per second
        """
        if len(self.batch_times) >= 2:
            # Use time and count span from oldest to newest batch in window
            oldest_time, oldest_count = self.batch_times[0]
            newest_time, newest_count = self.batch_times[-1]

            time_span = newest_time - oldest_time
            count_span = newest_count - oldest_count

            return count_span / max(1, time_span)
        else:
            # Fallback to overall rate for first batch
            current_time = time.time()
            overall_elapsed = current_time - fallback_start_time
            return fallback_processed_count / max(1, overall_elapsed)


def show_progress(
    start_time: float,
    total_items: int | None,
    rate_calculator: "SlidingWindowRateCalculator",
    completed_count: int,
    operation_name: str = "items",
    extra_info: dict[str, str] | None = None,
) -> None:
    """Show generic progress with ETA calculation."""
    # Skip showing progress if no items completed yet
    if completed_count == 0:
        return

    current_time = time.time()
    percentage = (completed_count / total_items) * 100 if total_items and total_items > 0 else None
    elapsed = current_time - start_time

    # Update rate calculator
    rate_calculator.add_batch(current_time, completed_count)
    rate = rate_calculator.get_rate(start_time, completed_count)

    # Calculate ETA
    eta_text = ""
    if rate > 0 and total_items and total_items > completed_count:
        remaining = total_items - completed_count
        eta_seconds = remaining / rate
        eta_text = f" (ETA: {format_duration(eta_seconds)})"

    # Build progress display
    if percentage is not None:
        progress_part = f"{completed_count:,}/{total_items:,} ({percentage:.1f}%)"
    else:
        progress_part = f"{completed_count:,} {operation_name}"

    base_info = f"{progress_part} - {rate:.1f} {operation_name}/sec - elapsed: {format_duration(elapsed)}{eta_text}"

    # Add extra info (queue depths for sync, current record for collect)
    if extra_info:
        extra_parts = [f"{k}: {v}" for k, v in extra_info.items()]
        full_info = f"{base_info} [{', '.join(extra_parts)}]"
        print(full_info)
        logger.info(f"Progress: {full_info}")
    else:
        print(base_info)
        logger.info(f"Progress: {base_info}")


def show_queue_progress(
    start_time: float,
    total_books: int,
    rate_calculator: "SlidingWindowRateCalculator",
    completed_count: int,
    download_queue_depth: int,
    processing_queue_depth: int,
    active_tasks: dict[str, int],
) -> None:
    """Show progress for dual-queue processing."""
    downloads = active_tasks.get("downloads", 0)
    download_limit = active_tasks.get("download_limit", 5)
    processing = active_tasks.get("processing", 0)

    extra_info = {
        "downloads": f"{downloads}/{download_limit}",
        "processing": str(processing),
        "waiting to download": str(download_queue_depth),
        "waiting to be processed": str(processing_queue_depth),
    }

    show_progress(
        start_time=start_time,
        total_items=total_books,
        rate_calculator=rate_calculator,
        completed_count=completed_count,
        operation_name="books",
        extra_info=extra_info,
    )
