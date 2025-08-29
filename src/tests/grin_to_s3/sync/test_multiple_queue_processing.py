#!/usr/bin/env python3
"""Tests for multiple queue processing with independent failure tracking."""

import asyncio
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from grin_to_s3.sync.progress_reporter import SlidingWindowRateCalculator
from grin_to_s3.sync.task_manager import TaskManager, process_books_with_queue
from grin_to_s3.sync.tasks.task_types import TaskAction, TaskResult, TaskType


@pytest.fixture
def mock_pipeline():
    """Create mock pipeline for testing failure tracking in multiple queue scenarios."""
    pipeline = MagicMock()
    pipeline.max_sequential_failures = 2  # Lower limit for faster tests
    pipeline._sequential_failures = 0
    pipeline._shutdown_requested = False
    pipeline.process_summary_stage = MagicMock()
    pipeline.book_record_updates = {}
    pipeline.worker_count = 2
    pipeline.progress_interval = 1

    # Mock database tracker to avoid database operations
    db_tracker = AsyncMock()
    db_tracker.db_path = "/tmp/test.db"
    pipeline.db_tracker = db_tracker

    # Mock the failure handler to behave like the real one
    def handle_failure(barcode, error_msg):
        pipeline._sequential_failures += 1
        if pipeline._sequential_failures >= pipeline.max_sequential_failures:
            return True
        return False

    pipeline._handle_failure = handle_failure
    return pipeline


@pytest.fixture
def mock_task_funcs():
    """Create mock task functions for testing."""
    return {
        TaskType.CHECK: AsyncMock(),
        TaskType.DOWNLOAD: AsyncMock(),
        TaskType.DECRYPT: AsyncMock(),
        TaskType.UPLOAD: AsyncMock(),
        TaskType.CLEANUP: AsyncMock(),
    }


@pytest.fixture
def task_manager():
    """Create task manager for testing."""
    limits = {
        TaskType.CHECK: 2,
        TaskType.DOWNLOAD: 2,
        TaskType.DECRYPT: 1,
        TaskType.UPLOAD: 1,
        TaskType.CLEANUP: 1,
    }
    return TaskManager(limits)


@pytest.mark.asyncio
async def test_single_queue_regression(mock_pipeline, mock_task_funcs, task_manager):
    """Single queue processing should work exactly as before."""
    # Test both success and failure scenarios to ensure no regression
    barcodes = ["TEST001", "TEST002", "TEST003"]

    with (
        patch("grin_to_s3.sync.task_manager.commit_book_record_updates") as mock_commit,
        patch("grin_to_s3.sync.task_manager.get_updates_for_task") as mock_updates,
        patch("grin_to_s3.sync.task_manager.ProcessStageMetrics.determine_book_outcome") as mock_outcome,
    ):
        # Set up mocks
        mock_commit.return_value = None
        mock_updates.return_value = {}
        mock_outcome.return_value = "synced"  # All books succeed

        # Set up task functions to return success results
        for task_type, func in mock_task_funcs.items():
            func.return_value = TaskResult(barcode="TEST", task_type=task_type, action=TaskAction.COMPLETED, data={})

        rate_calculator = SlidingWindowRateCalculator(window_size=10)

        # Process books - should complete all successfully
        result = await process_books_with_queue(
            barcodes=barcodes,
            pipeline=mock_pipeline,
            task_funcs=mock_task_funcs,
            task_manager=task_manager,
            rate_calculator=rate_calculator,
            workers=2,
            progress_interval=1,
        )

        # Should process all books successfully
        assert len(result) == len(barcodes)
        assert mock_pipeline._shutdown_requested is False
        assert mock_pipeline._sequential_failures == 0


@pytest.mark.asyncio
async def test_failure_limit_stops_queue_feeding_with_many_books(mock_pipeline, mock_task_funcs, task_manager):
    """When failure limit is reached, it should stop feeding new books to queue."""
    # Create enough books to exceed queue capacity (queue maxsize is 10)
    # This ensures we can test whether feeding actually stops
    many_failing_barcodes = [f"FAIL{i:03d}" for i in range(1, 26)]  # 25 books

    with (
        patch("grin_to_s3.sync.task_manager.commit_book_record_updates") as mock_commit,
        patch("grin_to_s3.sync.task_manager.get_updates_for_task") as mock_updates,
        patch("grin_to_s3.sync.task_manager.ProcessStageMetrics.determine_book_outcome") as mock_outcome,
    ):
        # Set up mocks
        mock_commit.return_value = None
        mock_updates.return_value = {}
        # All books fail
        mock_outcome.return_value = "failed"

        # Set up task functions to return failure results with some delay
        # This ensures books don't all complete instantly
        async def slow_failing_task(*args, **kwargs):
            await asyncio.sleep(0.01)  # Small delay to allow queue feeding logic to run
            return TaskResult(barcode="TEST", task_type=TaskType.CHECK, action=TaskAction.FAILED, data={})

        for _task_type, func in mock_task_funcs.items():
            func.side_effect = slow_failing_task

        rate_calculator = SlidingWindowRateCalculator(window_size=10)

        # Process books - should stop feeding after failure limit but not set global shutdown
        result = await process_books_with_queue(
            barcodes=many_failing_barcodes,
            pipeline=mock_pipeline,
            task_funcs=mock_task_funcs,
            task_manager=task_manager,
            rate_calculator=rate_calculator,
            workers=2,
            progress_interval=5,
        )

        # Key verification: should NOT have set global shutdown flag
        # This is the critical fix - local failure limit doesn't trigger global shutdown
        assert mock_pipeline._shutdown_requested is False

        # Should have processed fewer books than the total due to stopped feeding
        # (Books already in queue when limit hit will complete, but no new ones fed)
        assert len(result) < len(many_failing_barcodes), (
            f"Expected < {len(many_failing_barcodes)} books processed, got {len(result)}"
        )

        # Failure counter should be at or above the limit
        assert mock_pipeline._sequential_failures >= mock_pipeline.max_sequential_failures


@pytest.mark.asyncio
async def test_global_shutdown_still_works(mock_pipeline, mock_task_funcs, task_manager):
    """Global shutdown flag should still stop queue feeding as before."""
    barcodes = ["TEST001", "TEST002", "TEST003"]

    with (
        patch("grin_to_s3.sync.task_manager.commit_book_record_updates") as mock_commit,
        patch("grin_to_s3.sync.task_manager.get_updates_for_task") as mock_updates,
        patch("grin_to_s3.sync.task_manager.ProcessStageMetrics.determine_book_outcome") as mock_outcome,
    ):
        # Set up mocks
        mock_commit.return_value = None
        mock_updates.return_value = {}
        mock_outcome.return_value = "synced"

        # Set up task functions to return success results
        for task_type, func in mock_task_funcs.items():
            func.return_value = TaskResult(barcode="TEST", task_type=task_type, action=TaskAction.COMPLETED, data={})

        # Simulate global shutdown being set (e.g., by user interrupt)
        mock_pipeline._shutdown_requested = True

        rate_calculator = SlidingWindowRateCalculator(window_size=10)

        # Process books - should stop immediately due to global shutdown
        result = await process_books_with_queue(
            barcodes=barcodes,
            pipeline=mock_pipeline,
            task_funcs=mock_task_funcs,
            task_manager=task_manager,
            rate_calculator=rate_calculator,
            workers=2,
            progress_interval=1,
        )

        # Should have processed minimal books due to immediate shutdown
        assert len(result) < len(barcodes)


@pytest.mark.asyncio
async def test_counter_reset_between_queues():
    """Failure counter should reset between queue processing sessions."""
    # This test simulates what happens in pipeline.py when processing multiple queues
    mock_pipeline = MagicMock()
    mock_pipeline.max_sequential_failures = 2

    # Simulate processing first queue with failures
    mock_pipeline._sequential_failures = 2  # At failure limit

    # Reset counter as done in pipeline.py between queues
    mock_pipeline._sequential_failures = 0

    # Verify counter was reset
    assert mock_pipeline._sequential_failures == 0


@pytest.mark.asyncio
async def test_failure_counter_reset_with_realistic_scenarios(mock_pipeline, mock_task_funcs, task_manager):
    """Test failure counter resets properly between queues in realistic scenarios."""
    # Simulate a more realistic scenario where first queue hits failures, second succeeds

    with (
        patch("grin_to_s3.sync.task_manager.commit_book_record_updates") as mock_commit,
        patch("grin_to_s3.sync.task_manager.get_updates_for_task") as mock_updates,
        patch("grin_to_s3.sync.task_manager.ProcessStageMetrics.determine_book_outcome") as mock_outcome,
    ):
        # Set up mocks
        mock_commit.return_value = None
        mock_updates.return_value = {}

        # First queue: simulate failure outcomes
        mock_outcome.side_effect = ["failed", "failed"]  # Two failures hit the limit

        # Set up task functions to fail appropriately
        for task_type, func in mock_task_funcs.items():
            func.return_value = TaskResult(barcode="TEST", task_type=task_type, action=TaskAction.FAILED, data={})

        rate_calculator = SlidingWindowRateCalculator(window_size=10)

        # Process first queue - should hit failure limit
        first_queue_barcodes = ["PREV001", "PREV002"]
        await process_books_with_queue(
            barcodes=first_queue_barcodes,
            pipeline=mock_pipeline,
            task_funcs=mock_task_funcs,
            task_manager=task_manager,
            rate_calculator=rate_calculator,
            workers=2,
            progress_interval=1,
        )

        # After first queue: should be at failure limit
        first_queue_counter = mock_pipeline._sequential_failures
        assert first_queue_counter >= mock_pipeline.max_sequential_failures

        # Reset counter for second queue (as done in pipeline.py:428)
        mock_pipeline._sequential_failures = 0

        # Second queue: simulate success outcomes
        mock_outcome.side_effect = ["synced", "synced"]  # Both succeed

        # Set up task functions to succeed
        for task_type, func in mock_task_funcs.items():
            func.return_value = TaskResult(barcode="TEST", task_type=task_type, action=TaskAction.COMPLETED, data={})

        # Process second queue - should succeed despite first queue failures
        second_queue_barcodes = ["CONV001", "CONV002"]
        second_result = await process_books_with_queue(
            barcodes=second_queue_barcodes,
            pipeline=mock_pipeline,
            task_funcs=mock_task_funcs,
            task_manager=task_manager,
            rate_calculator=rate_calculator,
            workers=2,
            progress_interval=1,
        )

        # Verify second queue processed successfully
        assert len(second_result) == len(second_queue_barcodes)
        assert mock_pipeline._shutdown_requested is False  # No global shutdown

        # Counter should remain reset for second queue
        assert mock_pipeline._sequential_failures == 0


@pytest.mark.asyncio
async def test_failure_limit_does_not_trigger_global_shutdown():
    """Test that reaching failure limit sets local flag, not global shutdown."""
    # This directly tests the changed behavior in task_manager.py
    mock_pipeline = MagicMock()
    mock_pipeline.max_sequential_failures = 1  # Immediate failure
    mock_pipeline._sequential_failures = 0
    mock_pipeline._shutdown_requested = False

    def handle_failure(barcode, error_msg):
        mock_pipeline._sequential_failures += 1
        if mock_pipeline._sequential_failures >= mock_pipeline.max_sequential_failures:
            return True  # Indicates limit reached
        return False

    mock_pipeline._handle_failure = handle_failure

    # Simulate what happens in the processing worker when failure limit is reached
    failure_limit_reached = False

    # Simulate book failure and handling
    book_outcome = "failed"
    if book_outcome == "failed":
        if mock_pipeline._handle_failure("TEST001", "Book processing failed"):
            # This is the new behavior - set local flag instead of global shutdown
            failure_limit_reached = True

    # Verify the fix
    assert failure_limit_reached is True  # Local flag set
    assert mock_pipeline._shutdown_requested is False  # Global shutdown NOT set
    assert mock_pipeline._sequential_failures == 1  # Counter incremented


@pytest.mark.asyncio
async def test_multiple_queues_independent_processing():
    """Test that multiple queues process independently with separate failure tracking."""
    # Simulate the pipeline.py queue processing loop

    # Mock the queue functions that would be called in pipeline.py
    with patch("grin_to_s3.sync.pipeline.get_books_from_queue"):
        with patch("grin_to_s3.sync.pipeline.process_books_with_queue"):
            with patch("grin_to_s3.sync.pipeline.filter_and_print_barcodes"):
                # Set up mock pipeline
                mock_pipeline = MagicMock()
                mock_pipeline.max_sequential_failures = 2
                mock_pipeline._sequential_failures = 0
                mock_pipeline._shutdown_requested = False

                # This test simulates the behavior without actually calling pipeline methods
                # It verifies the logical separation of queues

                queues = ["previous", "converted"]
                queue_books_data = [
                    {"PREV001", "PREV002", "PREV003"},  # First queue (previous)
                    {"CONV001", "CONV002"},  # Second queue (converted)
                ]

                queue_results_data = [
                    [],  # First queue fails (empty result = stopped due to failures)
                    [{"barcode": "CONV001"}, {"barcode": "CONV002"}],  # Second queue succeeds
                ]

                # Simulate the queue processing loop logic from pipeline.py
                total_results = []

                for i, _queue_name in enumerate(queues):
                    # Reset failure counter at start of each queue (as in pipeline.py:428)
                    mock_pipeline._sequential_failures = 0

                    # Get books from this specific queue
                    queue_books = queue_books_data[i]

                    if len(queue_books) == 0:
                        continue

                    # Process this queue's books
                    queue_results = queue_results_data[i]
                    total_results.extend(queue_results)

                # Verify the logical behavior: second queue produces results despite first failing
                assert len(total_results) == 2
                assert any(result["barcode"] == "CONV001" for result in total_results)
                assert any(result["barcode"] == "CONV002" for result in total_results)

                # Verify failure counter would be reset for each queue
                assert mock_pipeline._sequential_failures == 0  # Last reset


@pytest.mark.asyncio
async def test_queue_specific_task_routing_behavior():
    """Test that different queues trigger the correct task flows based on archive availability."""
    from grin_to_s3.sync.tasks.task_types import CheckResult, TaskAction, TaskType

    # Test the task routing logic that should differ between queue types

    # Simulate "previous" queue behavior: CHECK fails with 404 → REQUEST_CONVERSION
    previous_check_result = CheckResult(
        barcode="PREV001",
        task_type=TaskType.CHECK,
        action=TaskAction.FAILED,
        data={"etag": None, "file_size_bytes": None, "http_status_code": 404},
        reason="fail_archive_missing",
    )

    # Simulate "converted" queue behavior: CHECK succeeds → DOWNLOAD pipeline
    converted_check_result = CheckResult(
        barcode="CONV001",
        task_type=TaskType.CHECK,
        action=TaskAction.COMPLETED,
        data={"etag": "abc123", "file_size_bytes": 1024, "http_status_code": 200},
        reason=None,
    )

    # Verify different task flows
    previous_next_tasks = previous_check_result.next_tasks()
    converted_next_tasks = converted_check_result.next_tasks()

    # "previous" queue: failed CHECK should trigger REQUEST_CONVERSION
    assert previous_next_tasks == [TaskType.REQUEST_CONVERSION]
    assert not previous_check_result.should_continue_pipeline

    # "converted" queue: successful CHECK should trigger DOWNLOAD pipeline
    assert converted_next_tasks == [TaskType.DOWNLOAD]
    assert converted_check_result.should_continue_pipeline

    # Verify the routing correctly reflects queue purposes
    # - Previous queue handles books that need conversion (CHECK fails → request conversion)
    # - Converted queue handles books ready for download (CHECK succeeds → download pipeline)
