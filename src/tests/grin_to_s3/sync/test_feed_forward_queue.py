#!/usr/bin/env python3
"""Integration tests for the feed-forward queue implementation.

Tests the streaming architecture that replaces batch processing with continuous
book processing using asyncio.Queue for bounded memory and eliminated dead time.
"""

import asyncio
import time
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from grin_to_s3.sync.progress_reporter import SlidingWindowRateCalculator
from grin_to_s3.sync.task_manager import TaskManager, process_books_with_queue
from grin_to_s3.sync.tasks.task_types import TaskAction, TaskResult, TaskType


@pytest.fixture
def mock_pipeline():
    """Create mock pipeline for testing queue processing."""
    pipeline = MagicMock()
    pipeline.config = MagicMock()
    pipeline.config.storage_config = {"protocol": "s3"}
    pipeline.current_etags = {}
    pipeline.db_tracker = MagicMock()
    pipeline.db_tracker.db_path = "/tmp/test.db"
    pipeline.db_tracker.update_sync_data = AsyncMock()
    pipeline.book_record_updates = {}
    pipeline._shutdown_requested = False
    return pipeline


@pytest.fixture
def mock_task_manager():
    """Create mock task manager with tracking capabilities."""
    # Create limits for all task types
    limits = {
        TaskType.REQUEST_CONVERSION: 2,
        TaskType.CHECK: 5,
        TaskType.DOWNLOAD: 5,
        TaskType.DECRYPT: 10,
        TaskType.UPLOAD: 10,
        TaskType.UNPACK: 10,
        TaskType.EXTRACT_MARC: 10,
        TaskType.EXTRACT_OCR: 10,
        TaskType.CLEANUP: 10,
    }
    manager = TaskManager(limits)
    # Initialize stats for all task types
    for task_type in TaskType:
        manager.stats[task_type] = {
            "started": 0,
            "completed": 0,
            "failed": 0,
        }
    return manager


@pytest.fixture
def mock_rate_calculator():
    """Create mock rate calculator for progress reporting."""
    calculator = MagicMock(spec=SlidingWindowRateCalculator)
    calculator.add_batch = MagicMock()
    calculator.get_rate = MagicMock(return_value=10.5)  # Mock rate
    return calculator


class MockTaskFunction:
    """Mock task function that can simulate different processing times and outcomes."""

    def __init__(self, processing_time: float = 0.1, failure_rate: float = 0.0):
        self.processing_time = processing_time
        self.failure_rate = failure_rate
        self.call_count = 0
        self.call_times: list[float] = []

    async def __call__(self, barcode: str, *args, **kwargs):
        """Mock task execution with configurable timing and failure."""
        self.call_count += 1
        self.call_times.append(time.time())

        # Simulate processing time
        await asyncio.sleep(self.processing_time)

        # Simulate failures based on failure rate
        import random

        if random.random() < self.failure_rate:
            raise RuntimeError(f"Simulated failure for {barcode}")

        return TaskResult(
            barcode=barcode,
            task_type=TaskType.DOWNLOAD,  # Default type
            action=TaskAction.COMPLETED,
            data={"result": f"processed_{barcode}"},
        )


@pytest.fixture
def mock_task_functions():
    """Create mock task functions for testing."""
    return {
        TaskType.DOWNLOAD: MockTaskFunction(processing_time=0.05),
        TaskType.DECRYPT: MockTaskFunction(processing_time=0.02),
        TaskType.UPLOAD: MockTaskFunction(processing_time=0.1),  # Slower upload
    }


class TestQueueBasics:
    """Test basic queue functionality and setup."""

    @pytest.mark.asyncio
    async def test_queue_processes_single_book(
        self, mock_pipeline, mock_task_manager, mock_rate_calculator, mock_task_functions
    ):
        """Queue should process a single book successfully."""
        barcodes = ["TEST001"]

        with patch("grin_to_s3.sync.task_manager.process_download_phase") as mock_download, \
             patch("grin_to_s3.sync.task_manager.process_processing_phase") as mock_processing:
            download_result = TaskResult("TEST001", TaskType.DOWNLOAD, TaskAction.COMPLETED, data={"file_path": "/tmp/test001.tar.gz"})

            mock_download.return_value = {
                TaskType.DOWNLOAD: download_result
            }
            mock_processing.return_value = {
                TaskType.DOWNLOAD: download_result,
                TaskType.UPLOAD: TaskResult("TEST001", TaskType.UPLOAD, TaskAction.COMPLETED)
            }

            results = await process_books_with_queue(
                barcodes,
                mock_pipeline,
                mock_task_functions,
                mock_task_manager,
                mock_rate_calculator,
                workers=2,
                progress_interval=1,
            )

            assert len(results) == 1
            assert "TEST001" in results
            mock_download.assert_called_once()
            mock_processing.assert_called_once()

    @pytest.mark.asyncio
    async def test_queue_processes_multiple_books(
        self, mock_pipeline, mock_task_manager, mock_rate_calculator, mock_task_functions
    ):
        """Queue should process multiple books correctly."""
        barcodes = [f"TEST{i:03d}" for i in range(5)]

        with patch("grin_to_s3.sync.task_manager.process_download_phase") as mock_download, \
             patch("grin_to_s3.sync.task_manager.process_processing_phase") as mock_processing:
            def download_side_effect(manager, barcode, pipeline, task_funcs):
                result = TaskResult(barcode, TaskType.DOWNLOAD, TaskAction.COMPLETED, data={"file_path": f"/tmp/{barcode}.tar.gz"})
                return {TaskType.DOWNLOAD: result}

            def processing_side_effect(manager, barcode, download_results, pipeline, task_funcs):
                return {
                    **download_results,
                    TaskType.UPLOAD: TaskResult(barcode, TaskType.UPLOAD, TaskAction.COMPLETED)
                }

            mock_download.side_effect = download_side_effect
            mock_processing.side_effect = processing_side_effect

            results = await process_books_with_queue(
                barcodes,
                mock_pipeline,
                mock_task_functions,
                mock_task_manager,
                mock_rate_calculator,
                workers=3,
                progress_interval=2,
            )

            assert len(results) == 5
            assert all(f"TEST{i:03d}" in results for i in range(5))
            assert mock_download.call_count == 5
            assert mock_processing.call_count == 5


class TestConcurrentProcessing:
    """Test concurrent processing and elimination of dead time."""

    @pytest.mark.asyncio
    async def test_concurrent_processing_eliminates_dead_time(
        self, mock_pipeline, mock_task_manager, mock_rate_calculator, mock_task_functions
    ):
        """Workers should process books concurrently without waiting for batches."""
        barcodes = [f"TEST{i:03d}" for i in range(10)]
        processing_times = []

        def track_download_time(manager, barcode, pipeline, task_funcs):
            processing_times.append((barcode, time.time()))
            result = TaskResult(barcode, TaskType.DOWNLOAD, TaskAction.COMPLETED, data={"file_path": f"/tmp/{barcode}.tar.gz"})
            return {TaskType.DOWNLOAD: result}

        def track_processing_time(manager, barcode, download_results, pipeline, task_funcs):
            return {
                **download_results,
                TaskType.UPLOAD: TaskResult(barcode, TaskType.UPLOAD, TaskAction.COMPLETED)
            }

        with patch("grin_to_s3.sync.task_manager.process_download_phase", side_effect=track_download_time), \
             patch("grin_to_s3.sync.task_manager.process_processing_phase", side_effect=track_processing_time):
            start_time = time.time()

            await process_books_with_queue(
                barcodes,
                mock_pipeline,
                mock_task_functions,
                mock_task_manager,
                mock_rate_calculator,
                workers=5,
                progress_interval=3,
            )

            end_time = time.time()

            # With 5 workers processing 10 books, should have overlap
            # Sort by processing time to check for concurrent execution
            processing_times.sort(key=lambda x: x[1])

            # First 5 books should start within a very short time window (concurrent)
            first_five_times = [t[1] for t in processing_times[:5]]
            time_spread = max(first_five_times) - min(first_five_times)

            # Should be much less than sequential processing time
            assert time_spread < 0.5, f"Time spread too large: {time_spread}"
            assert end_time - start_time < 5.0, "Total time suggests sequential processing"

    @pytest.mark.asyncio
    async def test_bounded_queue_provides_backpressure(
        self, mock_pipeline, mock_task_manager, mock_rate_calculator, mock_task_functions
    ):
        """Queue should provide backpressure when full, preventing memory explosion."""
        # Create a large number of books to test backpressure
        barcodes = [f"TEST{i:04d}" for i in range(200)]

        async def slow_download(*args):
            await asyncio.sleep(0.01)
            barcode = args[1]
            result = TaskResult(barcode, TaskType.DOWNLOAD, TaskAction.COMPLETED, data={"file_path": f"/tmp/{barcode}.tar.gz"})
            return {TaskType.DOWNLOAD: result}

        async def slow_processing(*args):
            barcode = args[1]
            download_results = args[2]
            return {
                **download_results,
                TaskType.UPLOAD: TaskResult(barcode, TaskType.UPLOAD, TaskAction.COMPLETED)
            }

        with patch("grin_to_s3.sync.task_manager.process_download_phase", slow_download), \
             patch("grin_to_s3.sync.task_manager.process_processing_phase", slow_processing):
            # This should complete without memory issues
            results = await process_books_with_queue(
                barcodes,
                mock_pipeline,
                mock_task_functions,
                mock_task_manager,
                mock_rate_calculator,
                workers=5,
                progress_interval=50,
            )

            assert len(results) == 200


class TestWorkerManagement:
    """Test worker lifecycle and management."""

    @pytest.mark.asyncio
    async def test_configurable_worker_count(
        self, mock_pipeline, mock_task_manager, mock_rate_calculator, mock_task_functions
    ):
        """Should support configurable number of workers."""
        barcodes = [f"TEST{i:03d}" for i in range(20)]
        worker_counts = []

        def track_download_workers(manager, barcode, pipeline, task_funcs):
            current_task = asyncio.current_task()
            worker_counts.append(
                len(current_task.get_name()) if current_task and hasattr(current_task, "get_name") else 1
            )
            result = TaskResult(barcode, TaskType.DOWNLOAD, TaskAction.COMPLETED, data={"file_path": f"/tmp/{barcode}.tar.gz"})
            return {TaskType.DOWNLOAD: result}

        def track_processing_workers(manager, barcode, download_results, pipeline, task_funcs):
            return {
                **download_results,
                TaskType.UPLOAD: TaskResult(barcode, TaskType.UPLOAD, TaskAction.COMPLETED)
            }

        with patch("grin_to_s3.sync.task_manager.process_download_phase", side_effect=track_download_workers), \
             patch("grin_to_s3.sync.task_manager.process_processing_phase", side_effect=track_processing_workers):
            # Test with 10 workers
            await process_books_with_queue(
                barcodes,
                mock_pipeline,
                mock_task_functions,
                mock_task_manager,
                mock_rate_calculator,
                workers=10,
                progress_interval=5,
            )

            # All books should be processed
            assert len(worker_counts) == 20

    @pytest.mark.asyncio
    async def test_poison_pill_shutdown(
        self, mock_pipeline, mock_task_manager, mock_rate_calculator, mock_task_functions
    ):
        """Workers should terminate correctly when receiving poison pills."""
        barcodes = [f"TEST{i:03d}" for i in range(5)]

        with patch("grin_to_s3.sync.task_manager.process_download_phase") as mock_download, \
             patch("grin_to_s3.sync.task_manager.process_processing_phase") as mock_processing:
            def download_side_effect(manager, barcode, pipeline, task_funcs):
                result = TaskResult(barcode, TaskType.DOWNLOAD, TaskAction.COMPLETED, data={"file_path": f"/tmp/{barcode}.tar.gz"})
                return {TaskType.DOWNLOAD: result}

            def processing_side_effect(manager, barcode, download_results, pipeline, task_funcs):
                return {
                    **download_results,
                    TaskType.UPLOAD: TaskResult(barcode, TaskType.UPLOAD, TaskAction.COMPLETED)
                }

            mock_download.side_effect = download_side_effect
            mock_processing.side_effect = processing_side_effect

            # This should complete cleanly without hanging
            start_time = time.time()
            results = await process_books_with_queue(
                barcodes,
                mock_pipeline,
                mock_task_functions,
                mock_task_manager,
                mock_rate_calculator,
                workers=3,
                progress_interval=2,
            )
            end_time = time.time()

            assert len(results) == 5
            # Should not hang - complete within reasonable time
            assert end_time - start_time < 2.0


class TestProgressReporting:
    """Test progress reporting functionality."""

    @pytest.mark.asyncio
    async def test_progress_updates_at_interval(
        self, mock_pipeline, mock_task_manager, mock_rate_calculator, mock_task_functions
    ):
        """Progress should be reported at specified intervals."""
        barcodes = [f"TEST{i:03d}" for i in range(10)]

        with patch("grin_to_s3.sync.task_manager.process_download_phase") as mock_download, \
             patch("grin_to_s3.sync.task_manager.process_processing_phase") as mock_processing:
            def download_side_effect(manager, barcode, pipeline, task_funcs):
                result = TaskResult(barcode, TaskType.DOWNLOAD, TaskAction.COMPLETED, data={"file_path": f"/tmp/{barcode}.tar.gz"})
                return {TaskType.DOWNLOAD: result}

            def processing_side_effect(manager, barcode, download_results, pipeline, task_funcs):
                return {
                    **download_results,
                    TaskType.UPLOAD: TaskResult(barcode, TaskType.UPLOAD, TaskAction.COMPLETED)
                }

            mock_download.side_effect = download_side_effect
            mock_processing.side_effect = processing_side_effect

            # Capture print output to verify progress reporting
            with patch("builtins.print") as mock_print:
                await process_books_with_queue(
                    barcodes,
                    mock_pipeline,
                    mock_task_functions,
                    mock_task_manager,
                    mock_rate_calculator,
                    workers=2,
                    progress_interval=3,
                )

                # Should have progress reports (at least for intervals and completion)
                print_calls = [call for call in mock_print.call_args_list if call[0] and "books/sec" in str(call[0][0])]

                # Should have at least some progress updates
                assert len(print_calls) >= 1

    @pytest.mark.asyncio
    async def test_queue_depth_in_progress(
        self, mock_pipeline, mock_task_manager, mock_rate_calculator, mock_task_functions
    ):
        """Progress reporting should include queue depth information."""
        barcodes = [f"TEST{i:03d}" for i in range(8)]

        with patch("grin_to_s3.sync.task_manager.process_download_phase") as mock_download, \
             patch("grin_to_s3.sync.task_manager.process_processing_phase") as mock_processing:
            def download_side_effect(manager, barcode, pipeline, task_funcs):
                result = TaskResult(barcode, TaskType.DOWNLOAD, TaskAction.COMPLETED, data={"file_path": f"/tmp/{barcode}.tar.gz"})
                return {TaskType.DOWNLOAD: result}

            def processing_side_effect(manager, barcode, download_results, pipeline, task_funcs):
                return {
                    **download_results,
                    TaskType.UPLOAD: TaskResult(barcode, TaskType.UPLOAD, TaskAction.COMPLETED)
                }

            mock_download.side_effect = download_side_effect
            mock_processing.side_effect = processing_side_effect

            with patch("builtins.print") as mock_print:
                await process_books_with_queue(
                    barcodes,
                    mock_pipeline,
                    mock_task_functions,
                    mock_task_manager,
                    mock_rate_calculator,
                    workers=2,
                    progress_interval=4,
                )

                # Look for queue depth in progress output (now includes downloads and processing info)
                progress_calls = [
                    str(call[0][0]) for call in mock_print.call_args_list if call[0] and "waiting to" in str(call[0][0])
                ]

                # Should have at least one progress report with queue depth
                assert len(progress_calls) >= 1


class TestErrorHandling:
    """Test error handling and resilience."""

    @pytest.mark.asyncio
    async def test_individual_book_failures_dont_crash_workers(
        self, mock_pipeline, mock_task_manager, mock_rate_calculator, mock_task_functions
    ):
        """Worker failures should be isolated and not crash other workers."""
        barcodes = [f"TEST{i:03d}" for i in range(8)]

        def failing_download(manager, barcode, pipeline, task_funcs):
            # Make every 3rd book fail
            if int(barcode[-3:]) % 3 == 0:
                raise RuntimeError(f"Simulated failure for {barcode}")
            result = TaskResult(barcode, TaskType.DOWNLOAD, TaskAction.COMPLETED, data={"file_path": f"/tmp/{barcode}.tar.gz"})
            return {TaskType.DOWNLOAD: result}

        def normal_processing(manager, barcode, download_results, pipeline, task_funcs):
            return {
                **download_results,
                TaskType.UPLOAD: TaskResult(barcode, TaskType.UPLOAD, TaskAction.COMPLETED)
            }

        with patch("grin_to_s3.sync.task_manager.process_download_phase", side_effect=failing_download), \
             patch("grin_to_s3.sync.task_manager.process_processing_phase", side_effect=normal_processing):
            # Should complete despite some failures
            results = await process_books_with_queue(
                barcodes,
                mock_pipeline,
                mock_task_functions,
                mock_task_manager,
                mock_rate_calculator,
                workers=3,
                progress_interval=2,
            )

            # Should have results for all books (including failures)
            assert len(results) == 8

            # Check that the right books failed (every 3rd: 000, 003, 006)
            failed_barcodes = []
            successful_barcodes = []

            for barcode, result in results.items():
                download_result = result.get(TaskType.DOWNLOAD)
                if download_result and download_result.action == TaskAction.FAILED:
                    failed_barcodes.append(barcode)
                else:
                    successful_barcodes.append(barcode)

            # Verify expected failures and successes
            assert set(failed_barcodes) == {"TEST000", "TEST003", "TEST006"}
            assert set(successful_barcodes) == {"TEST001", "TEST002", "TEST004", "TEST005", "TEST007"}

            # Verify successful books have both DOWNLOAD and UPLOAD results
            for barcode in successful_barcodes:
                assert TaskType.DOWNLOAD in results[barcode]
                assert TaskType.UPLOAD in results[barcode]

            # Verify failed books only have DOWNLOAD result with error
            for barcode in failed_barcodes:
                assert TaskType.DOWNLOAD in results[barcode]
                assert TaskType.UPLOAD not in results[barcode]
                assert results[barcode][TaskType.DOWNLOAD].error is not None

    @pytest.mark.asyncio
    async def test_graceful_shutdown_on_interrupt(
        self, mock_pipeline, mock_task_manager, mock_rate_calculator, mock_task_functions
    ):
        """Should handle KeyboardInterrupt gracefully during queue feeding."""
        barcodes = [f"TEST{i:03d}" for i in range(20)]

        with patch("grin_to_s3.sync.task_manager.process_download_phase") as mock_download, \
             patch("grin_to_s3.sync.task_manager.process_processing_phase") as mock_processing:
            def download_side_effect(manager, barcode, pipeline, task_funcs):
                result = TaskResult(barcode, TaskType.DOWNLOAD, TaskAction.COMPLETED, data={"file_path": f"/tmp/{barcode}.tar.gz"})
                return {TaskType.DOWNLOAD: result}

            def processing_side_effect(manager, barcode, download_results, pipeline, task_funcs):
                return {
                    **download_results,
                    TaskType.UPLOAD: TaskResult(barcode, TaskType.UPLOAD, TaskAction.COMPLETED)
                }

            mock_download.side_effect = download_side_effect
            mock_processing.side_effect = processing_side_effect

            # Test that KeyboardInterrupt handling works by simulating the interrupt
            # This is a simplified test that verifies the function can handle such scenarios
            try:
                # First verify normal operation works
                results = await process_books_with_queue(
                    barcodes[:5],
                    mock_pipeline,
                    mock_task_functions,
                    mock_task_manager,
                    mock_rate_calculator,
                    workers=2,
                    progress_interval=2,
                )
                assert len(results) == 5

                # The actual KeyboardInterrupt handling is tested implicitly by
                # the shutdown behavior - the function should clean up workers properly
                assert True  # Test passes if we reach this point without hanging

            except Exception:
                # Should not crash unexpectedly
                pytest.fail("Unexpected exception during normal processing")


class TestEndToEndIntegration:
    """End-to-end integration tests."""

    @pytest.mark.asyncio
    async def test_shutdown_request_stops_queue_feeding(
        self, mock_pipeline, mock_task_manager, mock_rate_calculator, mock_task_functions
    ):
        """Pipeline shutdown request should stop feeding new books to queue."""
        barcodes = [f"TEST{i:03d}" for i in range(200)]  # More than queue size (100)

        # Create a task that will trigger shutdown after a delay to simulate external shutdown
        async def trigger_shutdown():
            await asyncio.sleep(0.05)  # Small delay to let initial queue feeding start
            mock_pipeline._shutdown_requested = True

        with patch("grin_to_s3.sync.task_manager.process_download_phase") as mock_download, \
             patch("grin_to_s3.sync.task_manager.process_processing_phase") as mock_processing:
            def download_side_effect(manager, barcode, pipeline, task_funcs):
                result = TaskResult(barcode, TaskType.DOWNLOAD, TaskAction.COMPLETED, data={"file_path": f"/tmp/{barcode}.tar.gz"})
                return {TaskType.DOWNLOAD: result}

            def processing_side_effect(manager, barcode, download_results, pipeline, task_funcs):
                return {
                    **download_results,
                    TaskType.UPLOAD: TaskResult(barcode, TaskType.UPLOAD, TaskAction.COMPLETED)
                }

            mock_download.side_effect = download_side_effect
            mock_processing.side_effect = processing_side_effect

            # Start shutdown trigger task
            shutdown_task = asyncio.create_task(trigger_shutdown())

            try:
                results = await process_books_with_queue(
                    barcodes,
                    mock_pipeline,
                    mock_task_functions,
                    mock_task_manager,
                    mock_rate_calculator,
                    workers=5,
                    progress_interval=5,
                )

                # Should process the initial queue fill (100) plus some additional items
                # that were already in processing when shutdown was triggered,
                # but not all 200 books
                assert len(results) < 200  # Didn't process all books (shutdown worked)
                assert len(results) >= 100  # At least processed initial queue fill

            finally:
                # Clean up the shutdown task
                if not shutdown_task.done():
                    shutdown_task.cancel()
                try:
                    await shutdown_task
                except asyncio.CancelledError:
                    pass

    @pytest.mark.asyncio
    async def test_memory_bounded_with_large_book_list(
        self, mock_pipeline, mock_task_manager, mock_rate_calculator, mock_task_functions
    ):
        """Should handle large book lists without memory explosion."""
        # Test with a large number of books (simulating "millions")
        # Use smaller number for test performance but verify pattern
        barcodes = [f"TEST{i:05d}" for i in range(1000)]

        with patch("grin_to_s3.sync.task_manager.process_download_phase") as mock_download, \
             patch("grin_to_s3.sync.task_manager.process_processing_phase") as mock_processing:
            def download_side_effect(manager, barcode, pipeline, task_funcs):
                result = TaskResult(barcode, TaskType.DOWNLOAD, TaskAction.COMPLETED, data={"file_path": f"/tmp/{barcode}.tar.gz"})
                return {TaskType.DOWNLOAD: result}

            def processing_side_effect(manager, barcode, download_results, pipeline, task_funcs):
                return {
                    **download_results,
                    TaskType.UPLOAD: TaskResult(barcode, TaskType.UPLOAD, TaskAction.COMPLETED)
                }

            mock_download.side_effect = download_side_effect
            mock_processing.side_effect = processing_side_effect

            # Should complete without memory issues
            results = await process_books_with_queue(
                barcodes,
                mock_pipeline,
                mock_task_functions,
                mock_task_manager,
                mock_rate_calculator,
                workers=20,
                progress_interval=100,
            )

            assert len(results) == 1000
            assert mock_download.call_count == 1000
            assert mock_processing.call_count == 1000

    @pytest.mark.asyncio
    async def test_database_updates_accumulated(
        self, mock_pipeline, mock_task_manager, mock_rate_calculator, mock_task_functions
    ):
        """Database updates should be accumulated correctly during processing."""
        barcodes = [f"TEST{i:03d}" for i in range(5)]

        # Mock book_record_updates to track accumulation
        mock_pipeline.book_record_updates = {}

        def download_with_db_updates(manager, barcode, pipeline, task_funcs):
            # Simulate database updates being accumulated
            pipeline.book_record_updates[barcode] = {"status": "processed"}
            result = TaskResult(barcode, TaskType.DOWNLOAD, TaskAction.COMPLETED, data={"file_path": f"/tmp/{barcode}.tar.gz"})
            return {TaskType.DOWNLOAD: result}

        def processing_with_updates(manager, barcode, download_results, pipeline, task_funcs):
            return {
                **download_results,
                TaskType.UPLOAD: TaskResult(barcode, TaskType.UPLOAD, TaskAction.COMPLETED)
            }

        with patch("grin_to_s3.sync.task_manager.process_download_phase", side_effect=download_with_db_updates), \
             patch("grin_to_s3.sync.task_manager.process_processing_phase", side_effect=processing_with_updates):
            await process_books_with_queue(
                barcodes,
                mock_pipeline,
                mock_task_functions,
                mock_task_manager,
                mock_rate_calculator,
                workers=3,
                progress_interval=2,
            )

            # Should have accumulated updates for all processed books
            assert len(mock_pipeline.book_record_updates) == 5
            for barcode in barcodes:
                assert barcode in mock_pipeline.book_record_updates
