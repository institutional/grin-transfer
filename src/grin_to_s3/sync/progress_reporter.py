#!/usr/bin/env python3
"""
Progress Reporter for Sync Pipeline

Provides rate calculation utilities for sync operations.
"""

import time


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
