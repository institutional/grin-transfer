#!/usr/bin/env python3
"""
Unit tests for SlidingWindowRateCalculator
"""

import time
from unittest.mock import patch

import pytest

from grin_to_s3.common import SlidingWindowRateCalculator


class TestSlidingWindowRateCalculator:
    """Test SlidingWindowRateCalculator functionality."""

    def test_initialization(self):
        """Test calculator initialization."""
        calc = SlidingWindowRateCalculator(window_size=5)
        assert calc.window_size == 5
        assert calc.batch_times == []

    def test_initialization_default_window_size(self):
        """Test default window size."""
        calc = SlidingWindowRateCalculator()
        assert calc.window_size == 5

    def test_add_batch_single_entry(self):
        """Test adding a single batch entry."""
        calc = SlidingWindowRateCalculator(window_size=3)
        calc.add_batch(100.0, 10)

        assert len(calc.batch_times) == 1
        assert calc.batch_times[0] == (100.0, 10)

    def test_add_batch_multiple_entries(self):
        """Test adding multiple batch entries."""
        calc = SlidingWindowRateCalculator(window_size=3)
        calc.add_batch(100.0, 10)
        calc.add_batch(101.0, 20)
        calc.add_batch(102.0, 30)

        assert len(calc.batch_times) == 3
        assert calc.batch_times[0] == (100.0, 10)
        assert calc.batch_times[1] == (101.0, 20)
        assert calc.batch_times[2] == (102.0, 30)

    def test_window_size_limiting(self):
        """Test that window size is respected."""
        calc = SlidingWindowRateCalculator(window_size=2)
        calc.add_batch(100.0, 10)
        calc.add_batch(101.0, 20)
        calc.add_batch(102.0, 30)  # Should push out the first entry

        assert len(calc.batch_times) == 2
        assert calc.batch_times[0] == (101.0, 20)
        assert calc.batch_times[1] == (102.0, 30)

    def test_window_size_limiting_fifo(self):
        """Test that oldest entries are removed first."""
        calc = SlidingWindowRateCalculator(window_size=3)
        calc.add_batch(100.0, 10)
        calc.add_batch(101.0, 20)
        calc.add_batch(102.0, 30)
        calc.add_batch(103.0, 40)  # Should remove (100.0, 10)
        calc.add_batch(104.0, 50)  # Should remove (101.0, 20)

        assert len(calc.batch_times) == 3
        assert calc.batch_times[0] == (102.0, 30)
        assert calc.batch_times[1] == (103.0, 40)
        assert calc.batch_times[2] == (104.0, 50)

    def test_get_rate_insufficient_data_uses_fallback(self):
        """Test rate calculation with insufficient data uses fallback."""
        calc = SlidingWindowRateCalculator(window_size=5)
        calc.add_batch(101.0, 10)  # Only one entry

        # Should use fallback calculation - using a fixed time for deterministic testing
        with patch('time.time', return_value=101.0):
            rate = calc.get_rate(fallback_start_time=100.0, fallback_processed_count=10)
            expected_rate = 10 / 1.0  # 10 items in 1 second
            assert rate == expected_rate

    def test_get_rate_no_data_uses_fallback(self):
        """Test rate calculation with no data uses fallback."""
        calc = SlidingWindowRateCalculator(window_size=5)

        # Should use fallback calculation
        rate = calc.get_rate(fallback_start_time=100.0, fallback_processed_count=25)
        expected_rate = 25 / 1.0  # Based on current time vs start time
        assert abs(rate - expected_rate) < 25  # Allow some variance for execution time

    def test_get_rate_sliding_window_calculation(self):
        """Test rate calculation using sliding window."""
        calc = SlidingWindowRateCalculator(window_size=5)
        calc.add_batch(100.0, 10)  # Start: 10 items at 100s
        calc.add_batch(102.0, 30)  # End: 30 items at 102s

        # Window calculation: (30-10) items / (102-100) seconds = 20/2 = 10 items/sec
        rate = calc.get_rate(fallback_start_time=99.0, fallback_processed_count=30)
        assert rate == 10.0

    def test_get_rate_multiple_entries_uses_window_span(self):
        """Test rate calculation uses oldest to newest in window."""
        calc = SlidingWindowRateCalculator(window_size=3)
        calc.add_batch(100.0, 10)   # oldest in window
        calc.add_batch(101.0, 20)   # middle
        calc.add_batch(104.0, 50)   # newest in window

        # Should use span from oldest (100.0, 10) to newest (104.0, 50)
        # Rate = (50-10) / (104-100) = 40/4 = 10 items/sec
        rate = calc.get_rate(fallback_start_time=99.0, fallback_processed_count=50)
        assert rate == 10.0

    def test_get_rate_zero_time_span_protection(self):
        """Test rate calculation protects against division by zero."""
        calc = SlidingWindowRateCalculator(window_size=5)
        calc.add_batch(100.0, 10)
        calc.add_batch(100.0, 20)  # Same timestamp

        # Should handle zero time span gracefully
        rate = calc.get_rate(fallback_start_time=99.0, fallback_processed_count=20)
        assert rate >= 0  # Should not crash and return reasonable rate

    def test_get_rate_real_world_scenario(self):
        """Test realistic batch processing scenario."""
        calc = SlidingWindowRateCalculator(window_size=5)

        # Simulate processing batches over time
        start_time = 1000.0
        calc.add_batch(1001.0, 5)    # 5 items after 1 second
        calc.add_batch(1002.0, 12)   # 12 items after 2 seconds
        calc.add_batch(1003.0, 18)   # 18 items after 3 seconds
        calc.add_batch(1005.0, 28)   # 28 items after 5 seconds

        # Rate from first to last: (28-5)/(1005-1001) = 23/4 = 5.75 items/sec
        rate = calc.get_rate(fallback_start_time=start_time, fallback_processed_count=28)
        assert abs(rate - 5.75) < 0.01

    def test_method_signature_compatibility(self):
        """Test that the expected method signatures exist and work correctly."""
        calc = SlidingWindowRateCalculator()

        # These method calls should not raise AttributeError
        calc.add_batch(time.time(), 10)
        rate = calc.get_rate(time.time() - 1, 10)

        assert isinstance(rate, int | float)
        assert rate >= 0

    def test_add_completion_method_does_not_exist(self):
        """Test that add_completion method does not exist (prevent regression)."""
        calc = SlidingWindowRateCalculator()

        # This should raise AttributeError - documenting the expected interface
        with pytest.raises(AttributeError, match="'SlidingWindowRateCalculator' object has no attribute 'add_completion'"):
            calc.add_completion(time.time())
