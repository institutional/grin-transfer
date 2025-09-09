#!/usr/bin/env python3
"""
Unit tests for SlidingWindowRateCalculator
"""

from unittest.mock import patch

from grin_to_s3.sync.progress_reporter import SlidingWindowRateCalculator


class TestSlidingWindowRateCalculator:
    """Test SlidingWindowRateCalculator functionality."""

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
        with patch("time.time", return_value=101.0):
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
        calc.add_batch(100.0, 10)  # oldest in window
        calc.add_batch(101.0, 20)  # middle
        calc.add_batch(104.0, 50)  # newest in window

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
        calc.add_batch(1001.0, 5)  # 5 items after 1 second
        calc.add_batch(1002.0, 12)  # 12 items after 2 seconds
        calc.add_batch(1003.0, 18)  # 18 items after 3 seconds
        calc.add_batch(1005.0, 28)  # 28 items after 5 seconds

        # Rate from first to last: (28-5)/(1005-1001) = 23/4 = 5.75 items/sec
        rate = calc.get_rate(fallback_start_time=start_time, fallback_processed_count=28)
        assert abs(rate - 5.75) < 0.01

    def test_eta_stability_with_minor_variations(self):
        """Test that ETAs remain stable despite minor processing variations."""
        calc = SlidingWindowRateCalculator(window_size=5)

        # Simulate processing at 0.8 books/sec
        # Progress every 20 books, so each batch takes 25 seconds (20/0.8)
        base_time = 1000.0

        # Build up a full window of 5 batches (100 books total)
        # Each batch represents 20 books completed at regular intervals
        calc.add_batch(base_time + 25, 20)  # 20 books at 25 seconds
        calc.add_batch(base_time + 50, 40)  # 40 books at 50 seconds
        calc.add_batch(base_time + 75, 60)  # 60 books at 75 seconds
        calc.add_batch(base_time + 100, 80)  # 80 books at 100 seconds
        calc.add_batch(base_time + 125, 100)  # 100 books at 125 seconds

        # Get stable rate (should be 0.8 books/sec)
        initial_rate = calc.get_rate(base_time, 100)

        # Now add one slower batch that takes 50% longer (37.5 seconds vs 25)
        # This simulates a temporary network slowdown
        calc.add_batch(base_time + 162.5, 120)  # Next 20 books took 37.5 seconds

        # Due to sliding window, the oldest entry (base_time + 25, 20) gets dropped
        # New window spans from (base_time + 50, 40) to (base_time + 162.5, 120)
        slower_rate = calc.get_rate(base_time, 120)

        # Calculate how much the rate changed
        rate_change_percent = abs(slower_rate - initial_rate) / initial_rate * 100

        # With exponential smoothing, ETAs should remain very stable
        # Rate changes should be minimal even with temporary slowdowns
        assert rate_change_percent < 2, (
            f"Processing rate changed by {rate_change_percent:.1f}% from one slow batch. "
            f"Rate went from {initial_rate:.2f} to {slower_rate:.2f} books/sec. "
            f"This would cause ETA to swing wildly in real usage. "
            f"Expected < 2% change for stable ETAs."
        )
