"""
Unit tests for process summary infrastructure.
"""

import tempfile
import time
from datetime import UTC, datetime
from pathlib import Path
from unittest.mock import patch

import pytest

from grin_to_s3.constants import OUTPUT_DIR
from grin_to_s3.process_summary import (
    ProcessStageMetrics,
    RunSummary,
    create_process_summary,
    save_process_summary,
)


class TestRunSummary:
    """Test RunSummary class functionality."""

    def test_init_default_values(self):
        """Test RunSummary initialization with default values."""
        summary = RunSummary(run_name="test_run")

        assert summary.run_name == "test_run"
        assert summary.session_id > 0
        # Check that run_start_time is a valid UTC ISO timestamp
        datetime.fromisoformat(summary.run_start_time.replace("Z", "+00:00"))
        assert summary.run_end_time is None
        assert summary.total_duration_seconds is None
        assert summary.interruption_count == 0
        assert summary.last_checkpoint_time is None
        assert summary.stages == {}

    def test_get_or_create_stage(self):
        """Test getting or creating a stage."""
        summary = RunSummary(run_name="test_run")

        # Create new stage
        stage = summary.get_or_create_stage("test_stage")
        assert stage.stage_name == "test_stage"
        assert "test_stage" in summary.stages

        # Get existing stage
        same_stage = summary.get_or_create_stage("test_stage")
        assert same_stage is stage

    def test_start_stage(self):
        """Test starting a stage."""
        summary = RunSummary(run_name="test_run")

        stage = summary.start_stage("test_stage")
        assert stage.stage_name == "test_stage"
        assert stage.start_time is not None
        assert summary.last_checkpoint_time is not None

    def test_end_stage(self):
        """Test ending a stage."""
        summary = RunSummary(run_name="test_run")

        # Start and end a stage
        summary.start_stage("test_stage")
        summary.end_stage("test_stage")

        stage = summary.stages["test_stage"]
        assert stage.end_time is not None
        assert stage.duration_seconds is not None

    def test_end_run(self):
        """Test ending the run."""
        summary = RunSummary(run_name="test_run")

        summary.end_run()
        assert summary.run_end_time is not None
        assert summary.total_duration_seconds is not None

    def test_detect_interruption_no_interruption(self):
        """Test interruption detection when no interruption occurred."""
        summary = RunSummary(run_name="test_run")
        summary.checkpoint()

        # No interruption should be detected immediately
        assert not summary.detect_interruption()

    def test_detect_interruption_with_interruption(self):
        """Test interruption detection when interruption occurred."""
        summary = RunSummary(run_name="test_run")
        summary._last_checkpoint_perf_time = time.perf_counter() - 400  # 400 seconds ago
        summary.last_checkpoint_time = datetime.now(UTC).isoformat()  # Set a UTC timestamp

        # Should detect interruption
        assert summary.detect_interruption()
        assert summary.interruption_count == 1

    def test_checkpoint(self):
        """Test checkpoint functionality."""
        summary = RunSummary(run_name="test_run")

        original_time = summary.last_checkpoint_time
        summary.checkpoint()

        assert summary.last_checkpoint_time != original_time
        assert summary.last_checkpoint_time is not None

    def test_get_summary_dict(self):
        """Test getting summary dict."""
        summary = RunSummary(run_name="test_run")

        # Add a collect stage with some data
        stage = summary.start_stage("collect")
        stage.books_collected = 8
        stage.collection_failed = 2
        summary.end_stage("collect")
        summary.end_run()

        summary_dict = summary.get_summary_dict()

        # Check basic fields
        assert summary_dict["run_name"] == "test_run"
        assert summary_dict["total_items_processed"] == 10
        assert summary_dict["total_items_successful"] == 8
        assert summary_dict["total_items_failed"] == 2
        assert summary_dict["is_completed"] is True

        # Check stage data
        assert "collect" in summary_dict["stages"]
        stage_data = summary_dict["stages"]["collect"]
        assert stage_data["books_collected"] == 8
        assert stage_data["collection_failed"] == 2
        assert stage_data["is_completed"] is True


class TestProcessStageMetricsQueueTracking:
    """Test queue tracking features in ProcessStageMetrics."""

    def test_increment_by_outcome_conversion_failure_tracked(self):
        """Test that increment_by_outcome properly increments conversion_failures_tracked counter."""
        stage = ProcessStageMetrics("sync")

        # Initial value should be 0
        assert stage.conversion_failures_tracked == 0

        # Increment by conversion_failure_tracked outcome
        stage.increment_by_outcome("conversion_failure_tracked")
        assert stage.conversion_failures_tracked == 1

        # Increment again
        stage.increment_by_outcome("conversion_failure_tracked")
        assert stage.conversion_failures_tracked == 2

        # Other outcomes should not affect this counter
        stage.increment_by_outcome("synced")
        stage.increment_by_outcome("failed")
        assert stage.conversion_failures_tracked == 2

    def test_progress_updates_with_queue_data(self):
        """Test adding progress updates with queue tracking data."""
        stage = ProcessStageMetrics("sync")
        stage.start_stage()

        # Add progress update with queue counts
        stage.add_progress_update(
            "Fetched books from converted queue",
            queue_name="converted",
            queue_counts={"converted": {"books_available": 100, "count_returned": 100}},
        )

        # Add progress update with queue outcomes
        stage.add_progress_update(
            "Queue outcomes for converted",
            queue_name="converted",
            queue_outcomes={"converted": {"synced": 2, "skipped": 0, "conversion_requested": 0, "failed": 1}},
        )

        # Check progress updates contain queue data
        assert len(stage.progress_updates) == 2

        # Check first update
        first_update = stage.progress_updates[0]
        assert first_update["message"] == "Fetched books from converted queue"
        assert first_update["queue_name"] == "converted"
        assert "queue_counts" in first_update
        assert first_update["queue_counts"]["converted"]["books_available"] == 100

        # Check second update
        second_update = stage.progress_updates[1]
        assert second_update["message"] == "Queue outcomes for converted"
        assert second_update["queue_name"] == "converted"
        assert "queue_outcomes" in second_update
        assert second_update["queue_outcomes"]["converted"]["synced"] == 2

    def test_get_summary_dict_includes_queue_data_in_progress_updates(self):
        """Test that get_summary_dict includes queue tracking data in progress_updates."""
        summary = RunSummary(run_name="test_run")
        stage = summary.start_stage("sync")

        # Add queue data via progress updates
        stage.add_progress_update(
            "Fetched books",
            queue_name="converted",
            queue_counts={"converted": {"books_available": 100, "books_processed": 50}},
        )
        stage.add_progress_update(
            "Queue outcomes",
            queue_name="converted",
            queue_outcomes={"converted": {"synced": 1, "skipped": 0, "conversion_requested": 0, "failed": 0}},
        )

        summary.end_stage("sync")
        summary_dict = summary.get_summary_dict()

        # Check that queue data is included in progress_updates
        sync_stage = summary_dict["stages"]["sync"]
        assert "progress_updates" in sync_stage
        assert len(sync_stage["progress_updates"]) == 2

        # Find the progress update with queue_counts
        queue_counts_update = None
        queue_outcomes_update = None
        for update in sync_stage["progress_updates"]:
            if "queue_counts" in update:
                queue_counts_update = update
            if "queue_outcomes" in update:
                queue_outcomes_update = update

        # Check specific values
        assert queue_counts_update is not None
        assert queue_counts_update["queue_counts"]["converted"]["books_available"] == 100
        assert queue_outcomes_update is not None
        assert queue_outcomes_update["queue_outcomes"]["converted"]["synced"] == 1


@pytest.mark.asyncio
class TestProcessSummaryFunctions:
    """Test process summary convenience functions."""

    @pytest.fixture
    def temp_dir(self):
        """Create temporary directory for test files."""
        with tempfile.TemporaryDirectory() as temp_dir:
            yield Path(temp_dir)

    async def test_create_process_summary(self, temp_dir):
        """Test create_process_summary function."""
        with patch("grin_to_s3.process_summary.Path") as mock_path:
            mock_path.return_value = temp_dir / "test_run"

            summary = await create_process_summary("test_run", "test_stage")

            assert summary.run_name == "test_run"
            assert "test_stage" in summary.stages
            assert summary.stages["test_stage"].start_time is not None

    async def test_save_process_summary(self, temp_dir):
        """Test save_process_summary function."""
        import os

        # Save current directory
        original_cwd = os.getcwd()

        try:
            # Change to temp directory so relative paths work
            os.chdir(temp_dir)

            summary = RunSummary(run_name="test_run")
            stage = summary.start_stage("collect")
            stage.books_collected = 5

            # This should not raise an exception
            await save_process_summary(summary)

            # Check that file was created
            summary_file = temp_dir / OUTPUT_DIR / "test_run" / "process_summary.json"
            assert summary_file.exists()
        finally:
            # Restore original directory
            os.chdir(original_cwd)


class TestDetermineBookOutcome:
    """Test ProcessStageMetrics.determine_book_outcome static method."""

    def test_request_conversion_skipped_returns_skipped_outcome(self):
        """REQUEST_CONVERSION SKIPPED (already in process) should return 'skipped' outcome."""
        from grin_to_s3.sync.tasks.task_types import TaskAction, TaskResult, TaskType

        task_results = {
            TaskType.CHECK: TaskResult(
                barcode="TEST",
                task_type=TaskType.CHECK,
                action=TaskAction.FAILED,
                reason="fail_archive_missing",
                error="Archive not available in GRIN",
            ),
            TaskType.REQUEST_CONVERSION: TaskResult(
                barcode="TEST",
                task_type=TaskType.REQUEST_CONVERSION,
                action=TaskAction.SKIPPED,
                reason="skip_already_in_process",
            ),
        }

        outcome = ProcessStageMetrics.determine_book_outcome(task_results)
        assert outcome == "skipped"

    def test_request_conversion_completed_returns_conversion_requested(self):
        """REQUEST_CONVERSION COMPLETED should return 'conversion_requested' outcome."""
        from grin_to_s3.sync.tasks.task_types import TaskAction, TaskResult, TaskType

        task_results = {
            TaskType.CHECK: TaskResult(
                barcode="TEST",
                task_type=TaskType.CHECK,
                action=TaskAction.FAILED,
                reason="fail_archive_missing",
                error="Archive not available in GRIN",
            ),
            TaskType.REQUEST_CONVERSION: TaskResult(
                barcode="TEST",
                task_type=TaskType.REQUEST_CONVERSION,
                action=TaskAction.COMPLETED,
                reason="success_conversion_requested",
            ),
        }

        outcome = ProcessStageMetrics.determine_book_outcome(task_results)
        assert outcome == "conversion_requested"

    def test_check_skipped_returns_skipped_outcome(self):
        """CHECK SKIPPED should return 'skipped' outcome."""
        from grin_to_s3.sync.tasks.task_types import TaskAction, TaskResult, TaskType

        task_results = {
            TaskType.CHECK: TaskResult(
                barcode="TEST",
                task_type=TaskType.CHECK,
                action=TaskAction.SKIPPED,
                reason="skip_etag_match",
            ),
        }

        outcome = ProcessStageMetrics.determine_book_outcome(task_results)
        assert outcome == "skipped"

    def test_upload_completed_returns_synced_outcome(self):
        """UPLOAD COMPLETED should return 'synced' outcome."""
        from grin_to_s3.sync.tasks.task_types import TaskAction, TaskResult, TaskType

        task_results = {
            TaskType.CHECK: TaskResult(barcode="TEST", task_type=TaskType.CHECK, action=TaskAction.COMPLETED),
            TaskType.DOWNLOAD: TaskResult(barcode="TEST", task_type=TaskType.DOWNLOAD, action=TaskAction.COMPLETED),
            TaskType.UPLOAD: TaskResult(barcode="TEST", task_type=TaskType.UPLOAD, action=TaskAction.COMPLETED),
        }

        outcome = ProcessStageMetrics.determine_book_outcome(task_results)
        assert outcome == "synced"

    def test_track_conversion_failure_returns_conversion_failure_tracked(self):
        """TRACK_CONVERSION_FAILURE COMPLETED should return 'conversion_failure_tracked' outcome."""
        from grin_to_s3.sync.tasks.task_types import TaskAction, TaskResult, TaskType

        task_results = {
            TaskType.CHECK: TaskResult(
                barcode="TEST",
                task_type=TaskType.CHECK,
                action=TaskAction.FAILED,
                reason="fail_known_conversion_failure",
            ),
            TaskType.TRACK_CONVERSION_FAILURE: TaskResult(
                barcode="TEST",
                task_type=TaskType.TRACK_CONVERSION_FAILURE,
                action=TaskAction.COMPLETED,
            ),
        }

        outcome = ProcessStageMetrics.determine_book_outcome(task_results)
        assert outcome == "conversion_failure_tracked"

    def test_no_matching_outcome_returns_failed(self):
        """When no matching outcome pattern, should return 'failed'."""
        from grin_to_s3.sync.tasks.task_types import TaskAction, TaskResult, TaskType

        task_results = {
            TaskType.CHECK: TaskResult(
                barcode="TEST",
                task_type=TaskType.CHECK,
                action=TaskAction.FAILED,
                reason="fail_unexpected_http_status_code",
                error="HTTP 500",
            ),
        }

        outcome = ProcessStageMetrics.determine_book_outcome(task_results)
        assert outcome == "failed"
