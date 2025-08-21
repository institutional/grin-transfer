#!/usr/bin/env python3
"""
Tests for Progress Reporting Components
"""

import time

import pytest

from grin_to_s3.sync.progress_reporter import SlidingWindowRateCalculator


class TestSlidingWindowRateCalculator:
    """Test the SlidingWindowRateCalculator class."""

    @pytest.fixture
    def rate_calculator(self):
        """Create SlidingWindowRateCalculator instance."""
        return SlidingWindowRateCalculator(window_size=5)

    def test_initialization(self):
        """SlidingWindowRateCalculator should initialize correctly."""
        calculator = SlidingWindowRateCalculator(window_size=10)

        assert calculator.window_size == 10
        assert calculator.batch_times == []

    def test_add_batch_single(self, rate_calculator):
        """Should handle adding a single batch."""
        timestamp = 1000.0
        count = 10

        rate_calculator.add_batch(timestamp, count)

        assert len(rate_calculator.batch_times) == 1
        assert rate_calculator.batch_times[0] == (timestamp, count)

    def test_add_batch_multiple(self, rate_calculator):
        """Should handle adding multiple batches."""
        batches = [
            (1000.0, 10),
            (1002.0, 25),
            (1005.0, 45),
        ]

        for timestamp, count in batches:
            rate_calculator.add_batch(timestamp, count)

        assert len(rate_calculator.batch_times) == 3
        assert rate_calculator.batch_times == batches

    def test_window_size_limit(self, rate_calculator):
        """Should maintain only window_size batches."""
        # Add more batches than window size
        for i in range(8):
            rate_calculator.add_batch(1000.0 + i, i * 10)

        # Should only keep the last 5 batches
        assert len(rate_calculator.batch_times) == 5
        assert rate_calculator.batch_times[0] == (1003.0, 30)
        assert rate_calculator.batch_times[-1] == (1007.0, 70)

    def test_get_rate_fallback_single_batch(self, rate_calculator):
        """Should use fallback calculation for single batch."""
        start_time = 1000.0
        current_count = 25

        # Add single batch
        rate_calculator.add_batch(1002.0, current_count)

        with pytest.MonkeyPatch().context() as mp:
            mp.setattr(time, "time", lambda: 1003.0)

            rate = rate_calculator.get_rate(start_time, current_count)

            # Should use fallback: 25 items / 3 seconds = 8.33 items/sec
            expected_rate = 25 / 3
            assert abs(rate - expected_rate) < 0.01

    def test_get_rate_sliding_window(self, rate_calculator):
        """Should calculate rate using sliding window with multiple batches."""
        # Add multiple batches
        rate_calculator.add_batch(1000.0, 10)  # 10 items at time 1000
        rate_calculator.add_batch(1002.0, 25)  # 15 more items in 2 seconds = 7.5/sec
        rate_calculator.add_batch(1004.0, 45)  # 20 more items in 2 seconds = 10/sec

        rate = rate_calculator.get_rate(999.0, 45)

        # Should use sliding window: oldest to newest
        # From 1000.0 to 1004.0: 45 - 10 = 35 items in 4 seconds = 8.75/sec
        expected_rate = 35 / 4
        assert abs(rate - expected_rate) < 0.01

    def test_get_rate_zero_time_span(self, rate_calculator):
        """Should handle zero time span gracefully."""
        same_time = 1000.0

        rate_calculator.add_batch(same_time, 10)
        rate_calculator.add_batch(same_time, 20)

        rate = rate_calculator.get_rate(999.0, 20)

        # Should handle division by zero (max(1, time_span))
        expected_rate = 10 / 1  # count_span / max(1, 0)
        assert rate == expected_rate

    def test_get_rate_no_batches_fallback(self, rate_calculator):
        """Should use fallback when no batches added."""
        start_time = 1000.0
        current_count = 15

        with pytest.MonkeyPatch().context() as mp:
            mp.setattr(time, "time", lambda: 1005.0)

            rate = rate_calculator.get_rate(start_time, current_count)

            # Should use fallback: 15 items / 5 seconds = 3.0 items/sec
            expected_rate = 15 / 5
            assert rate == expected_rate

    def test_get_rate_zero_elapsed_time_fallback(self, rate_calculator):
        """Should handle zero elapsed time in fallback calculation."""
        start_time = 1000.0
        current_count = 10

        with pytest.MonkeyPatch().context() as mp:
            mp.setattr(time, "time", lambda: 1000.0)  # Same as start time

            rate = rate_calculator.get_rate(start_time, current_count)

            # Should handle division by zero: max(1, overall_elapsed)
            expected_rate = 10 / 1
            assert rate == expected_rate

    def test_realistic_usage_pattern(self, rate_calculator):
        """Test with realistic batch processing pattern."""
        start_time = 1000.0

        # Simulate processing batches over time
        batches = [
            (1010.0, 100),   # First 100 items in 10 seconds
            (1025.0, 250),   # Next 150 items in 15 seconds
            (1040.0, 400),   # Next 150 items in 15 seconds
            (1050.0, 500),   # Next 100 items in 10 seconds
        ]

        for timestamp, count in batches:
            rate_calculator.add_batch(timestamp, count)

        rate = rate_calculator.get_rate(start_time, 500)

        # Should use sliding window from first to last batch
        # From 1010.0 to 1050.0: 500 - 100 = 400 items in 40 seconds = 10.0/sec
        expected_rate = 400 / 40
        assert abs(rate - expected_rate) < 0.01
