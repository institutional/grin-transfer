"""
Unit tests for process summary infrastructure.
"""

import tempfile
import time
from pathlib import Path
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from grin_to_s3.process_summary import (
    ProcessSummary,
    ProcessSummaryManager,
    create_process_summary,
    save_process_summary,
)


class TestProcessSummary:
    """Test ProcessSummary class functionality."""

    def test_init_default_values(self):
        """Test ProcessSummary initialization with default values."""
        summary = ProcessSummary(process_name="test", run_name="test_run")

        assert summary.process_name == "test"
        assert summary.run_name == "test_run"
        assert summary.session_id > 0
        assert summary.start_time > 0
        assert summary.end_time is None
        assert summary.duration_seconds is None
        assert summary.items_processed == 0
        assert summary.items_successful == 0
        assert summary.items_failed == 0
        assert summary.items_retried == 0
        assert summary.bytes_processed == 0
        assert summary.error_count == 0
        assert summary.error_types == {}
        assert summary.last_error_message is None
        assert summary.last_error_time is None
        assert summary.interruption_count == 0
        assert summary.last_checkpoint_time is None
        assert summary.progress_updates == []
        assert summary.custom_metrics == {}

    def test_start_process(self):
        """Test start_process method."""
        summary = ProcessSummary(process_name="test", run_name="test_run")
        original_start_time = summary.start_time

        time.sleep(0.01)  # Small delay to ensure time difference
        summary.start_process()

        assert summary.start_time > original_start_time
        assert summary.last_checkpoint_time == summary.start_time

    def test_end_process(self):
        """Test end_process method."""
        summary = ProcessSummary(process_name="test", run_name="test_run")
        summary.start_process()

        time.sleep(0.01)  # Small delay to ensure duration > 0
        summary.end_process()

        assert summary.end_time is not None
        assert summary.duration_seconds is not None
        assert summary.duration_seconds > 0
        assert summary.end_time > summary.start_time

    def test_add_progress_update(self):
        """Test adding progress updates."""
        summary = ProcessSummary(process_name="test", run_name="test_run")
        summary.start_process()

        summary.add_progress_update("Test message", custom_field="value")

        assert len(summary.progress_updates) == 1
        update = summary.progress_updates[0]
        assert update["message"] == "Test message"
        assert update["custom_field"] == "value"
        assert update["timestamp"] > 0
        assert update["elapsed_seconds"] >= 0
        assert update["items_processed"] == 0
        assert update["items_successful"] == 0
        assert update["items_failed"] == 0

    def test_increment_items(self):
        """Test incrementing item counters."""
        summary = ProcessSummary(process_name="test", run_name="test_run")

        summary.increment_items(processed=10, successful=8, failed=2, retried=1, bytes_count=1024)

        assert summary.items_processed == 10
        assert summary.items_successful == 8
        assert summary.items_failed == 2
        assert summary.items_retried == 1
        assert summary.bytes_processed == 1024

        # Test multiple increments
        summary.increment_items(processed=5, successful=4, failed=1, bytes_count=512)

        assert summary.items_processed == 15
        assert summary.items_successful == 12
        assert summary.items_failed == 3
        assert summary.items_retried == 1
        assert summary.bytes_processed == 1536

    def test_add_error(self):
        """Test adding error records."""
        summary = ProcessSummary(process_name="test", run_name="test_run")

        summary.add_error("NetworkError", "Connection timeout")

        assert summary.error_count == 1
        assert summary.error_types["NetworkError"] == 1
        assert summary.last_error_message == "Connection timeout"
        assert summary.last_error_time > 0

        # Test multiple errors of same type
        summary.add_error("NetworkError", "Another timeout")

        assert summary.error_count == 2
        assert summary.error_types["NetworkError"] == 2
        assert summary.last_error_message == "Another timeout"

        # Test different error type
        summary.add_error("ValidationError", "Invalid data")

        assert summary.error_count == 3
        assert summary.error_types["NetworkError"] == 2
        assert summary.error_types["ValidationError"] == 1
        assert summary.last_error_message == "Invalid data"

    def test_detect_interruption_no_interruption(self):
        """Test interruption detection when no interruption occurred."""
        summary = ProcessSummary(process_name="test", run_name="test_run")
        summary.start_process()

        # No interruption should be detected immediately
        assert not summary.detect_interruption()
        assert summary.interruption_count == 0

    def test_detect_interruption_with_interruption(self):
        """Test interruption detection when interruption occurred."""
        summary = ProcessSummary(process_name="test", run_name="test_run")
        summary.start_process()

        # Simulate old checkpoint time (more than 5 minutes ago)
        summary.last_checkpoint_time = time.perf_counter() - 400  # 400 seconds ago

        assert summary.detect_interruption()
        assert summary.interruption_count == 1
        assert len(summary.progress_updates) == 1
        assert "Interruption detected" in summary.progress_updates[0]["message"]

    def test_checkpoint(self):
        """Test checkpoint functionality."""
        summary = ProcessSummary(process_name="test", run_name="test_run")
        summary.start_process()

        original_checkpoint = summary.last_checkpoint_time
        time.sleep(0.01)  # Small delay
        summary.checkpoint()

        assert summary.last_checkpoint_time > original_checkpoint

    def test_set_custom_metric(self):
        """Test setting custom metrics."""
        summary = ProcessSummary(process_name="test", run_name="test_run")

        summary.set_custom_metric("books_collected", 100)
        summary.set_custom_metric("storage_type", "minio")

        assert summary.custom_metrics["books_collected"] == 100
        assert summary.custom_metrics["storage_type"] == "minio"

    def test_get_summary_dict_running_process(self):
        """Test getting summary dict for running process."""
        summary = ProcessSummary(process_name="test", run_name="test_run")
        summary.start_process()
        summary.increment_items(processed=50, successful=45, failed=5)
        summary.add_error("TestError", "Test message")
        summary.set_custom_metric("test_metric", "value")

        summary_dict = summary.get_summary_dict()

        assert summary_dict["process_name"] == "test"
        assert summary_dict["run_name"] == "test_run"
        assert summary_dict["session_id"] > 0
        assert summary_dict["start_time"] > 0
        assert summary_dict["end_time"] is None
        assert summary_dict["duration_seconds"] is None
        assert not summary_dict["is_completed"]
        assert summary_dict["items_processed"] == 50
        assert summary_dict["items_successful"] == 45
        assert summary_dict["items_failed"] == 5
        assert summary_dict["success_rate_percent"] == 90.0
        assert summary_dict["processing_rate_per_second"] > 0
        assert summary_dict["error_count"] == 1
        assert summary_dict["error_types"]["TestError"] == 1
        assert summary_dict["custom_metrics"]["test_metric"] == "value"
        assert "generated_at" in summary_dict

    def test_get_summary_dict_completed_process(self):
        """Test getting summary dict for completed process."""
        summary = ProcessSummary(process_name="test", run_name="test_run")
        summary.start_process()
        summary.increment_items(processed=100, successful=90, failed=10)
        time.sleep(0.01)  # Small delay
        summary.end_process()

        summary_dict = summary.get_summary_dict()

        assert summary_dict["end_time"] > summary_dict["start_time"]
        assert summary_dict["duration_seconds"] > 0
        assert summary_dict["is_completed"]
        assert summary_dict["success_rate_percent"] == 90.0
        assert summary_dict["processing_rate_per_second"] > 0

    def test_get_progress_summary_empty(self):
        """Test getting progress summary with no updates."""
        summary = ProcessSummary(process_name="test", run_name="test_run")

        progress_summary = summary.get_progress_summary()

        assert progress_summary == {}

    def test_get_progress_summary_with_updates(self):
        """Test getting progress summary with updates."""
        summary = ProcessSummary(process_name="test", run_name="test_run")
        summary.start_process()

        # Add several progress updates
        for i in range(10):
            summary.add_progress_update(f"Update {i}")

        progress_summary = summary.get_progress_summary()

        assert progress_summary["total_updates"] == 10
        assert len(progress_summary["recent_updates"]) == 5  # Last 5 updates
        assert progress_summary["first_update"]["message"] == "Update 0"
        assert progress_summary["latest_update"]["message"] == "Update 9"
        assert progress_summary["recent_updates"][0]["message"] == "Update 5"
        assert progress_summary["recent_updates"][-1]["message"] == "Update 9"


class TestProcessSummaryManager:
    """Test ProcessSummaryManager class functionality."""

    @pytest.fixture
    def temp_dir(self):
        """Create temporary directory for test files."""
        with tempfile.TemporaryDirectory() as temp_dir:
            yield Path(temp_dir)

    @pytest.fixture
    def summary_manager(self, temp_dir):
        """Create ProcessSummaryManager with temp directory."""
        manager = ProcessSummaryManager("test_run", "test_process")
        manager.summary_file = temp_dir / "process_summary_test_process.json"
        return manager

    @pytest.mark.asyncio
    async def test_load_or_create_summary_new(self, summary_manager):
        """Test loading or creating summary when no file exists."""
        summary = await summary_manager.load_or_create_summary()

        assert isinstance(summary, ProcessSummary)
        assert summary.process_name == "test_process"
        assert summary.run_name == "test_run"
        assert summary.start_time > 0
        assert summary.last_checkpoint_time > 0

    @pytest.mark.asyncio
    async def test_save_and_load_summary(self, summary_manager):
        """Test saving and loading summary."""
        # Create and save summary
        original_summary = ProcessSummary(process_name="test_process", run_name="test_run")
        original_summary.start_process()
        original_summary.increment_items(processed=10, successful=8, failed=2)
        original_summary.add_error("TestError", "Test message")
        original_summary.set_custom_metric("test_key", "test_value")

        await summary_manager.save_summary(original_summary)

        # Verify file was created
        assert summary_manager.summary_file.exists()

        # Load summary
        loaded_summary = await summary_manager._load_summary()

        assert loaded_summary.process_name == original_summary.process_name
        assert loaded_summary.run_name == original_summary.run_name
        assert loaded_summary.session_id == original_summary.session_id
        assert loaded_summary.start_time == original_summary.start_time
        assert loaded_summary.items_processed == original_summary.items_processed
        assert loaded_summary.items_successful == original_summary.items_successful
        assert loaded_summary.items_failed == original_summary.items_failed
        assert loaded_summary.error_count == original_summary.error_count
        assert loaded_summary.error_types == original_summary.error_types
        assert loaded_summary.custom_metrics == original_summary.custom_metrics

    @pytest.mark.asyncio
    async def test_load_or_create_summary_existing(self, summary_manager):
        """Test loading existing summary with interruption detection."""
        # Create and save summary with old checkpoint
        original_summary = ProcessSummary(process_name="test_process", run_name="test_run")
        original_summary.start_process()
        original_summary.last_checkpoint_time = time.perf_counter() - 400  # 400 seconds ago

        await summary_manager.save_summary(original_summary)

        # Load summary (should detect interruption)
        with patch.object(ProcessSummary, "detect_interruption", return_value=True) as mock_detect:
            loaded_summary = await summary_manager.load_or_create_summary()

        mock_detect.assert_called_once()
        assert loaded_summary.process_name == "test_process"
        assert loaded_summary.run_name == "test_run"

    @pytest.mark.asyncio
    async def test_load_or_create_summary_corrupted_file(self, summary_manager):
        """Test handling corrupted summary file."""
        # Create corrupted file
        summary_manager.summary_file.parent.mkdir(parents=True, exist_ok=True)
        with open(summary_manager.summary_file, "w") as f:
            f.write("invalid json")

        # Should create new summary when file is corrupted
        summary = await summary_manager.load_or_create_summary()

        assert isinstance(summary, ProcessSummary)
        assert summary.process_name == "test_process"
        assert summary.run_name == "test_run"

    @pytest.mark.asyncio
    async def test_cleanup_summary(self, summary_manager):
        """Test cleanup of summary file."""
        # Create summary file
        summary_manager.summary_file.parent.mkdir(parents=True, exist_ok=True)
        summary_manager.summary_file.write_text("{}")

        assert summary_manager.summary_file.exists()

        await summary_manager.cleanup_summary()

        assert not summary_manager.summary_file.exists()

    @pytest.mark.asyncio
    async def test_cleanup_summary_nonexistent(self, summary_manager):
        """Test cleanup when summary file doesn't exist."""
        # Should not raise error
        await summary_manager.cleanup_summary()

        assert not summary_manager.summary_file.exists()


class TestModuleFunctions:
    """Test module-level functions."""

    @pytest.mark.asyncio
    async def test_create_process_summary(self):
        """Test create_process_summary function."""
        with patch("grin_to_s3.process_summary.ProcessSummaryManager") as mock_manager_class:
            mock_manager = MagicMock()
            mock_summary = ProcessSummary(process_name="test", run_name="test_run")
            mock_manager.load_or_create_summary = AsyncMock(return_value=mock_summary)
            mock_manager_class.return_value = mock_manager

            result = await create_process_summary("test_run", "test")

            mock_manager_class.assert_called_once_with("test_run", "test")
            mock_manager.load_or_create_summary.assert_called_once()
            assert result == mock_summary

    @pytest.mark.asyncio
    async def test_save_process_summary(self):
        """Test save_process_summary function."""
        summary = ProcessSummary(process_name="test", run_name="test_run")

        with patch("grin_to_s3.process_summary.ProcessSummaryManager") as mock_manager_class:
            mock_manager = MagicMock()
            mock_manager.save_summary = AsyncMock()
            mock_manager_class.return_value = mock_manager

            await save_process_summary(summary)

            mock_manager_class.assert_called_once_with("test_run", "test")
            mock_manager.save_summary.assert_called_once_with(summary)


class TestIntegration:
    """Integration tests for process summary functionality."""

    @pytest.mark.asyncio
    async def test_full_process_lifecycle(self):
        """Test complete process lifecycle with real file operations."""
        with tempfile.TemporaryDirectory() as temp_dir:
            # Override the runs directory for testing
            test_runs_path = Path(temp_dir) / "runs"

            def path_side_effect(x):
                return test_runs_path / x.replace("runs/", "")

            with patch("grin_to_s3.process_summary.Path", side_effect=path_side_effect):
                # Create process summary
                summary = await create_process_summary("test_run", "test_process")

                # Simulate process operations
                summary.increment_items(processed=5, successful=5)
                summary.add_progress_update("Processing batch 1")
                summary.checkpoint()

                # Save summary
                await save_process_summary(summary)

                # Simulate process interruption and restart
                summary2 = await create_process_summary("test_run", "test_process")

                # Should have loaded previous data
                assert summary2.items_processed == 5
                assert summary2.items_successful == 5
                assert len(summary2.progress_updates) >= 1

                # Continue processing
                summary2.increment_items(processed=5, successful=4, failed=1)
                summary2.add_error("TestError", "Test error message")
                summary2.end_process()

                # Final save
                await save_process_summary(summary2)

                # Verify final state
                final_dict = summary2.get_summary_dict()
                assert final_dict["items_processed"] == 10
                assert final_dict["items_successful"] == 9
                assert final_dict["items_failed"] == 1
                assert final_dict["error_count"] == 1
                assert final_dict["is_completed"]
                assert final_dict["success_rate_percent"] == 90.0
