"""
Unit tests for process summary infrastructure.
"""

import tempfile
import time
from pathlib import Path
from unittest.mock import patch

import pytest

from grin_to_s3.process_summary import (
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
        assert summary.run_start_time > 0
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
        summary.last_checkpoint_time = time.perf_counter() - 400  # 400 seconds ago

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

        # Add a stage with some data
        stage = summary.start_stage("test_stage")
        stage.increment_items(processed=10, successful=8, failed=2)
        summary.end_stage("test_stage")
        summary.end_run()

        summary_dict = summary.get_summary_dict()

        # Check basic fields
        assert summary_dict["run_name"] == "test_run"
        assert summary_dict["total_items_processed"] == 10
        assert summary_dict["total_items_successful"] == 8
        assert summary_dict["total_items_failed"] == 2
        assert summary_dict["is_completed"] is True

        # Check stage data
        assert "test_stage" in summary_dict["stages"]
        stage_data = summary_dict["stages"]["test_stage"]
        assert stage_data["items_processed"] == 10
        assert stage_data["is_completed"] is True


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
            stage = summary.start_stage("test_stage")
            stage.increment_items(processed=5, successful=5)

            # This should not raise an exception
            await save_process_summary(summary)

            # Check that file was created
            summary_file = temp_dir / "output" / "test_run" / "process_summary.json"
            assert summary_file.exists()
        finally:
            # Restore original directory
            os.chdir(original_cwd)
