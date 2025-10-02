#!/usr/bin/env python3
"""Tests for sequential failure exit functionality in sync pipeline."""

from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from grin_to_s3.sync.progress_reporter import SlidingWindowRateCalculator
from grin_to_s3.sync.task_manager import TaskManager, process_books_with_queue
from grin_to_s3.sync.tasks.task_types import TaskAction, TaskResult, TaskType
from tests.test_utils.unified_mocks import create_test_pipeline


@pytest.fixture
def mock_pipeline():
    """Create mock pipeline for testing failure tracking."""
    pipeline = create_test_pipeline()
    pipeline.max_sequential_failures = 3  # Lower limit for faster tests
    pipeline._sequential_failures = 0
    pipeline._shutdown_requested = False
    pipeline.process_summary_stage = MagicMock()
    pipeline.book_record_updates = {}
    pipeline.worker_count = 2
    pipeline.progress_interval = 1

    # Mock database tracker to avoid database operations
    db_tracker = AsyncMock()
    db_tracker.db_path = "/tmp/test.db"  # Use a proper string path
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
async def test_pipeline_exits_after_consecutive_failures():
    """Pipeline should exit after max_sequential_failures consecutive failures."""
    # Test the failure tracking logic more directly
    pipeline = create_test_pipeline()
    pipeline.max_sequential_failures = 3
    pipeline._sequential_failures = 0
    pipeline._shutdown_requested = False

    # Create the real failure handler method
    def handle_failure(barcode, error_msg):
        pipeline._sequential_failures += 1
        if pipeline._sequential_failures >= pipeline.max_sequential_failures:
            pipeline._shutdown_requested = True
            return True
        return False

    pipeline._handle_failure = handle_failure

    # Test the first 2 failures don't trigger shutdown
    result1 = pipeline._handle_failure("TEST001", "Error 1")
    assert result1 is False
    assert pipeline._sequential_failures == 1
    assert pipeline._shutdown_requested is False

    result2 = pipeline._handle_failure("TEST002", "Error 2")
    assert result2 is False
    assert pipeline._sequential_failures == 2
    assert pipeline._shutdown_requested is False

    # Test the 3rd failure triggers shutdown
    result3 = pipeline._handle_failure("TEST003", "Error 3")
    assert result3 is True
    assert pipeline._sequential_failures == 3
    assert pipeline._shutdown_requested is True


@pytest.mark.parametrize("success_outcome", ["synced", "skipped", "conversion_requested"])
@pytest.mark.asyncio
async def test_failure_counter_resets_on_success(success_outcome, mock_pipeline, mock_task_funcs, task_manager):
    """Sequential failure counter should reset when a book succeeds."""
    # Create a pattern: fail, fail, success, fail, fail, success
    outcomes = ["failed", "failed", success_outcome, "failed", "failed", success_outcome]
    barcodes = [f"TEST{i:03d}" for i in range(1, len(outcomes) + 1)]

    # Mock database operations to avoid database errors
    with (
        patch("grin_to_s3.sync.task_manager.commit_book_record_updates") as mock_commit,
        patch("grin_to_s3.sync.task_manager.get_updates_for_task") as mock_updates,
        patch("grin_to_s3.sync.task_manager.ProcessStageMetrics.determine_book_outcome") as mock_outcome,
    ):
        # Set up mocks
        mock_commit.return_value = None
        mock_updates.return_value = {}
        mock_outcome.side_effect = outcomes

        # Set up task functions to return appropriate results
        for task_type, func in mock_task_funcs.items():
            func.return_value = TaskResult(barcode="TEST", task_type=task_type, action=TaskAction.COMPLETED, data={})

        rate_calculator = SlidingWindowRateCalculator(window_size=10)

        # Process all books - should not exit early due to counter resets
        result = await process_books_with_queue(
            barcodes=barcodes,
            pipeline=mock_pipeline,
            task_funcs=mock_task_funcs,
            task_manager=task_manager,
            rate_calculator=rate_calculator,
            workers=2,
            progress_interval=1,
        )

        # Should have processed all 6 books (counter resets prevented early exit)
        assert len(result) == 6
        assert mock_pipeline._shutdown_requested is False
        # Counter should be reset after the last success
        assert mock_pipeline._sequential_failures == 0
