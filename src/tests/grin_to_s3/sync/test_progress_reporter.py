#!/usr/bin/env python3
"""
Tests for SyncProgressReporter
"""

import time
from unittest.mock import AsyncMock, Mock, patch

import pytest

from grin_to_s3.common import SlidingWindowRateCalculator
from grin_to_s3.sync.progress_reporter import (
    INITIAL_PROGRESS_INTERVAL,
    REGULAR_PROGRESS_INTERVAL,
    SyncProgressReporter,
)
from grin_to_s3.sync.tasks.task_types import TaskType


class TestSyncProgressReporter:
    """Test the SyncProgressReporter class."""

    @pytest.fixture
    def mock_task_manager(self):
        """Mock TaskManager for testing."""
        mock_manager = Mock()
        mock_manager.get_active_task_count = Mock(return_value=0)
        mock_manager.stats = {
            TaskType.UPLOAD: {"completed": 0, "failed": 0},
            TaskType.DOWNLOAD: {"completed": 0, "failed": 0},
        }
        return mock_manager

    @pytest.fixture
    def reporter(self, mock_task_manager):
        """Create SyncProgressReporter instance."""
        return SyncProgressReporter(
            task_manager=mock_task_manager,
            books_to_process=100,
            concurrent_downloads=5,
            concurrent_uploads=3,
        )

    @pytest.fixture
    def rate_calculator(self):
        """Create SlidingWindowRateCalculator instance."""
        return SlidingWindowRateCalculator(window_size=20)

    @pytest.mark.asyncio
    async def test_initialization(self, mock_task_manager):
        """SyncProgressReporter should initialize correctly."""
        reporter = SyncProgressReporter(
            task_manager=mock_task_manager,
            books_to_process=200,
            concurrent_downloads=10,
            concurrent_uploads=8,
        )

        assert reporter.manager is mock_task_manager
        assert reporter.books_to_process == 200
        assert reporter.concurrent_downloads == 10
        assert reporter.concurrent_uploads == 8
        assert not reporter.shutdown_event.is_set()

    @pytest.mark.asyncio
    async def test_shutdown_request(self, reporter):
        """Reporter should respond to shutdown requests."""
        assert not reporter.shutdown_event.is_set()

        reporter.request_shutdown()

        assert reporter.shutdown_event.is_set()

    @pytest.mark.asyncio
    async def test_progress_display_initial_state(self, reporter, mock_task_manager, rate_calculator):
        """_show_progress should display correct initial state."""
        # Setup task manager stats
        mock_task_manager.get_active_task_count.side_effect = lambda task_type: {
            TaskType.DOWNLOAD: 2,
            TaskType.UPLOAD: 1,
        }.get(task_type, 0)

        mock_task_manager.stats[TaskType.UPLOAD] = {"completed": 0, "failed": 0}

        with patch("builtins.print") as mock_print:
            start_time = time.time()
            current_time = start_time + 60  # 1 minute elapsed

            await reporter._show_progress(
                start_time=start_time,
                current_time=current_time,
                rate_calculator=rate_calculator,
                interval=INITIAL_PROGRESS_INTERVAL,
                last_report_time=start_time,
            )

            mock_print.assert_called_once()
            printed_output = mock_print.call_args[0][0]

            # Check that output contains expected elements
            assert "0/100" in printed_output  # completed/total
            assert "(0.0%)" in printed_output  # percentage
            assert "[2/5 downloads, 1/3 uploads]" in printed_output  # task status
            assert "elapsed:" in printed_output
            assert "next update" in printed_output

    @pytest.mark.asyncio
    async def test_progress_display_with_progress(self, reporter, mock_task_manager, rate_calculator):
        """_show_progress should display progress correctly."""
        # Setup task manager stats with some completed work
        mock_task_manager.get_active_task_count.side_effect = lambda task_type: {
            TaskType.DOWNLOAD: 3,
            TaskType.UPLOAD: 2,
        }.get(task_type, 0)

        mock_task_manager.stats[TaskType.UPLOAD] = {"completed": 25, "failed": 2}

        with patch("builtins.print") as mock_print:
            start_time = time.time()
            current_time = start_time + 300  # 5 minutes elapsed

            await reporter._show_progress(
                start_time=start_time,
                current_time=current_time,
                rate_calculator=rate_calculator,
                interval=REGULAR_PROGRESS_INTERVAL,
                last_report_time=start_time,
            )

            mock_print.assert_called_once()
            printed_output = mock_print.call_args[0][0]

            # Check progress calculations
            assert "27/100" in printed_output  # 25 completed + 2 failed = 27 total
            assert "(27.0%)" in printed_output  # percentage
            assert "[3/5 downloads, 2/3 uploads]" in printed_output  # task status

    @pytest.mark.asyncio
    async def test_progress_display_with_eta(self, reporter, mock_task_manager):
        """_show_progress should calculate and display ETA."""
        # Setup completed work and mock rate calculator
        mock_task_manager.stats[TaskType.UPLOAD] = {"completed": 20, "failed": 0}

        mock_rate_calculator = Mock()
        mock_rate_calculator.get_rate.return_value = 2.5  # 2.5 books/sec

        with patch("builtins.print") as mock_print:
            start_time = time.time()
            current_time = start_time + 120  # 2 minutes elapsed

            await reporter._show_progress(
                start_time=start_time,
                current_time=current_time,
                rate_calculator=mock_rate_calculator,
                interval=INITIAL_PROGRESS_INTERVAL,
                last_report_time=start_time,
            )

            printed_output = mock_print.call_args[0][0]

            # With 20 completed out of 100, and rate of 2.5/sec, ETA should be calculated
            assert "20/100" in printed_output
            assert "2.5 books/sec" in printed_output
            assert "ETA:" in printed_output

    @pytest.mark.asyncio
    async def test_progress_display_no_eta_when_zero_rate(self, reporter, mock_task_manager):
        """_show_progress should not display ETA when rate is zero."""
        mock_task_manager.stats[TaskType.UPLOAD] = {"completed": 5, "failed": 0}

        mock_rate_calculator = Mock()
        mock_rate_calculator.get_rate.return_value = 0.0  # No progress rate

        with patch("builtins.print") as mock_print:
            start_time = time.time()
            current_time = start_time + 60

            await reporter._show_progress(
                start_time=start_time,
                current_time=current_time,
                rate_calculator=mock_rate_calculator,
                interval=INITIAL_PROGRESS_INTERVAL,
                last_report_time=start_time,
            )

            printed_output = mock_print.call_args[0][0]

            # Should not contain ETA when rate is zero
            assert "ETA:" not in printed_output
            assert "0.0 books/sec" in printed_output

    @pytest.mark.asyncio
    async def test_run_initial_vs_regular_intervals(self, reporter, mock_task_manager, rate_calculator):
        """Reporter should use different intervals for initial vs regular reports."""
        # Mock time to control timing
        start_time = 1000.0
        times_called = [start_time]

        def mock_time():
            # Return progressing times to trigger reports
            if len(times_called) == 1:
                times_called.append(start_time + INITIAL_PROGRESS_INTERVAL)
                return times_called[-1]
            elif len(times_called) == 2:
                times_called.append(start_time + INITIAL_PROGRESS_INTERVAL * 2)
                return times_called[-1]
            else:
                # After MAX_INITIAL_REPORTS, should switch to regular interval
                times_called.append(start_time + INITIAL_PROGRESS_INTERVAL * 3 + REGULAR_PROGRESS_INTERVAL)
                return times_called[-1]

        with (
            patch("time.time", side_effect=mock_time),
            patch("builtins.print") as mock_print,
            patch("asyncio.sleep", new_callable=AsyncMock) as mock_sleep,
        ):
            # Mock the sleep to exit after a few iterations
            sleep_call_count = 0

            async def mock_sleep_with_exit(*args):
                nonlocal sleep_call_count
                sleep_call_count += 1
                if sleep_call_count > 30:  # Exit after reasonable number of sleep calls
                    reporter.request_shutdown()

            mock_sleep.side_effect = mock_sleep_with_exit

            await reporter.run(start_time, rate_calculator)

            # Should have made progress reports
            assert mock_print.call_count > 0

    @pytest.mark.asyncio
    async def test_run_handles_exceptions(self, reporter, mock_task_manager, rate_calculator):
        """Reporter should handle exceptions gracefully and continue running."""
        exception_count = 0

        def mock_show_progress(*args, **kwargs):
            nonlocal exception_count
            exception_count += 1
            if exception_count == 1:
                raise ValueError("Test exception")
            # After first exception, request shutdown to exit loop
            reporter.request_shutdown()

        with (
            patch.object(reporter, "_show_progress", side_effect=mock_show_progress),
            patch("asyncio.sleep", new_callable=AsyncMock),
            patch("time.time", return_value=1000.0),
        ):
            await reporter.run(1000.0, rate_calculator)

            # Should have attempted to show progress twice (once failed, once succeeded)
            assert exception_count == 2

    @pytest.mark.asyncio
    async def test_run_exits_on_shutdown_signal(self, reporter, rate_calculator):
        """Reporter should exit gracefully when shutdown is requested."""
        # Request shutdown immediately
        reporter.request_shutdown()

        with patch("builtins.print") as mock_print, patch("time.time", return_value=1000.0):
            await reporter.run(1000.0, rate_calculator)

            # Should not have printed anything since shutdown was immediate
            mock_print.assert_not_called()

    @pytest.mark.asyncio
    async def test_run_checks_shutdown_during_sleep(self, reporter, rate_calculator):
        """Reporter should check for shutdown signal during sleep intervals."""
        sleep_call_count = 0

        async def mock_sleep_with_shutdown(*args):
            nonlocal sleep_call_count
            sleep_call_count += 1
            if sleep_call_count == 5:  # Shutdown after a few sleep calls
                reporter.request_shutdown()

        with (
            patch("asyncio.sleep", side_effect=mock_sleep_with_shutdown),
            patch("time.time", return_value=1000.0),
            patch("builtins.print"),
        ):
            await reporter.run(1000.0, rate_calculator)

            # Should have called sleep multiple times before shutdown
            assert sleep_call_count >= 5

    @pytest.mark.asyncio
    async def test_zero_books_to_process(self, mock_task_manager, rate_calculator):
        """Reporter should handle zero books gracefully."""
        reporter = SyncProgressReporter(
            task_manager=mock_task_manager,
            books_to_process=0,
            concurrent_downloads=5,
            concurrent_uploads=3,
        )

        with patch("builtins.print") as mock_print:
            start_time = time.time()

            await reporter._show_progress(
                start_time=start_time,
                current_time=start_time + 60,
                rate_calculator=rate_calculator,
                interval=INITIAL_PROGRESS_INTERVAL,
                last_report_time=start_time,
            )

            printed_output = mock_print.call_args[0][0]

            # Should handle division by zero gracefully
            assert "0/0" in printed_output
            assert "(0.0%)" in printed_output or "(0%" in printed_output

    @pytest.mark.asyncio
    async def test_interval_calculation_timing(self, reporter, rate_calculator):
        """Reporter should calculate time until next update correctly."""
        with patch("builtins.print") as mock_print:
            start_time = 1000.0
            current_time = start_time + 300  # 5 minutes after start
            last_report_time = start_time + 240  # Last report was 4 minutes after start (1 minute ago)

            await reporter._show_progress(
                start_time=start_time,
                current_time=current_time,
                rate_calculator=rate_calculator,
                interval=REGULAR_PROGRESS_INTERVAL,  # 10 minutes
                last_report_time=last_report_time,
            )

            printed_output = mock_print.call_args[0][0]

            # With 10-minute interval and 1 minute since last report,
            # should show approximately 9 minutes until next update
            assert "next update" in printed_output
            assert "9 min" in printed_output or "10 min" in printed_output

    @pytest.mark.asyncio
    async def test_pipeline_integration_progress_reporter_lifecycle(self):
        """Test that progress reporter integrates correctly with sync pipeline lifecycle."""
        # This is an integration test to ensure the progress reporter works with the sync pipeline
        from grin_to_s3.common import SlidingWindowRateCalculator

        # Create mocks for TaskManager and pipeline dependencies
        mock_task_manager = Mock()
        mock_task_manager.get_active_task_count = Mock(return_value=0)
        mock_task_manager.stats = {
            TaskType.UPLOAD: {"completed": 0, "failed": 0},
            TaskType.DOWNLOAD: {"completed": 0, "failed": 0},
        }

        # Test the integration points that the pipeline uses
        reporter = SyncProgressReporter(
            task_manager=mock_task_manager,
            books_to_process=50,
            concurrent_downloads=3,
            concurrent_uploads=2,
        )
        rate_calculator = SlidingWindowRateCalculator(window_size=20)

        # Test the lifecycle that would occur in the pipeline
        start_time = time.time()

        # 1. Test initial progress display (would happen at pipeline start)
        with patch("builtins.print") as mock_print:
            await reporter._show_progress(
                start_time=start_time,
                current_time=start_time + 30,
                rate_calculator=rate_calculator,
                interval=INITIAL_PROGRESS_INTERVAL,
                last_report_time=start_time,
            )

            assert mock_print.called
            output = mock_print.call_args[0][0]
            assert "0/50" in output
            assert "[0/3 downloads, 0/2 uploads]" in output

        # 2. Test progress during processing (simulate some completed uploads)
        mock_task_manager.stats[TaskType.UPLOAD] = {"completed": 10, "failed": 1}
        mock_task_manager.get_active_task_count.side_effect = lambda task_type: {
            TaskType.DOWNLOAD: 2,
            TaskType.UPLOAD: 1,
        }.get(task_type, 0)

        with patch("builtins.print") as mock_print:
            await reporter._show_progress(
                start_time=start_time,
                current_time=start_time + 120,
                rate_calculator=rate_calculator,
                interval=INITIAL_PROGRESS_INTERVAL,
                last_report_time=start_time + 60,
            )

            output = mock_print.call_args[0][0]
            assert "11/50" in output  # 10 completed + 1 failed = 11 total processed
            assert "[2/3 downloads, 1/2 uploads]" in output

        # 3. Test graceful shutdown (what happens in pipeline cleanup)
        assert not reporter.shutdown_event.is_set()
        reporter.request_shutdown()
        assert reporter.shutdown_event.is_set()

    @pytest.mark.asyncio
    async def test_rate_calculator_integration(self, reporter, mock_task_manager):
        """Test that progress reporter correctly integrates with SlidingWindowRateCalculator."""
        # Test integration between progress reporter and rate calculator
        rate_calculator = SlidingWindowRateCalculator(window_size=5)

        # Simulate rate calculator having received some batch data
        rate_calculator.add_batch(1000.0, 10)  # 10 items at time 1000
        rate_calculator.add_batch(1002.0, 25)  # 25 items at time 1002 (rate: 15/2 = 7.5/sec)

        mock_task_manager.stats[TaskType.UPLOAD] = {"completed": 25, "failed": 0}

        with patch("builtins.print") as mock_print:
            await reporter._show_progress(
                start_time=999.0,
                current_time=1002.0,
                rate_calculator=rate_calculator,
                interval=INITIAL_PROGRESS_INTERVAL,
                last_report_time=999.0,
            )

            output = mock_print.call_args[0][0]

            # Should show the rate calculated by the rate calculator
            assert "7.5 books/sec" in output

            # Should show ETA calculation based on the rate
            # Remaining: 100 - 25 = 75, Rate: 7.5/sec, ETA: 75/7.5 = 10 seconds
            assert "ETA:" in output

    @pytest.mark.asyncio
    async def test_format_duration_integration(self, reporter, mock_task_manager, rate_calculator):
        """Test that format_duration utility is used correctly in progress display."""
        from grin_to_s3.common import format_duration

        mock_task_manager.stats[TaskType.UPLOAD] = {"completed": 0, "failed": 0}

        with patch("builtins.print") as mock_print:
            start_time = 1000.0
            current_time = 1000.0 + (3 * 3600) + (25 * 60) + 42  # 3h 25m 42s elapsed

            await reporter._show_progress(
                start_time=start_time,
                current_time=current_time,
                rate_calculator=rate_calculator,
                interval=INITIAL_PROGRESS_INTERVAL,
                last_report_time=start_time,
            )

            output = mock_print.call_args[0][0]

            # Should display formatted elapsed time
            elapsed = current_time - start_time
            expected_formatted = format_duration(elapsed)
            assert expected_formatted in output
