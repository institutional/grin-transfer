"""
Integration tests for process summary upload functionality.
"""

import json
import tempfile
from pathlib import Path
from unittest.mock import patch

import pytest

from grin_to_s3.process_summary import (
    RunSummary,
    RunSummaryManager,
    create_process_summary,
    save_process_summary,
)
from tests.test_utils.unified_mocks import create_book_manager_mock, create_storage_mock


class TestProcessSummaryUpload:
    """Test process summary upload functionality."""

    @pytest.fixture
    def temp_dir(self):
        """Create temporary directory for test files."""
        with tempfile.TemporaryDirectory() as temp_dir:
            yield Path(temp_dir)

    @pytest.fixture
    def mock_run_name(self):
        """Provide a test run name."""
        return "test_run_upload"

    @pytest.fixture
    def mock_storage(self):
        """Create a mock storage instance."""
        return create_storage_mock(storage_type="s3")

    @pytest.fixture
    def mock_book_manager(self):
        """Create a mock BookStorage instance."""
        return create_book_manager_mock(base_prefix="test_run")

    @pytest.mark.asyncio
    async def test_run_summary_manager_storage_upload(self, temp_dir, mock_run_name, mock_book_manager):
        """Test RunSummaryManager storage upload functionality."""
        # Create manager with temporary directory
        with patch("grin_to_s3.process_summary.Path") as mock_path:
            mock_path.return_value = temp_dir / mock_run_name
            manager = RunSummaryManager(mock_run_name)

            # Enable storage upload
            manager.enable_storage_upload(mock_book_manager)

            # Create a test summary
            summary = RunSummary(run_name=mock_run_name)
            stage = summary.start_stage("collect")
            stage.books_collected = 9
            stage.collection_failed = 1
            stage.add_progress_update("Test progress")
            summary.end_stage("collect")

            # Save summary (should trigger upload)
            await manager.save_summary(summary)

            # Verify local file was created
            assert manager.summary_file.exists()

            # Verify storage upload was called
            mock_book_manager.storage.write_file.assert_called_once()

            # Verify the storage path is correct (now compressed)
            call_args = mock_book_manager.storage.write_file.call_args
            storage_path, local_path = call_args[0]
            assert storage_path == "test-meta/test_run/process_summary.json.gz"
            # local_path should be a compressed temp file, not the original summary file
            assert local_path.endswith(".gz")

    @pytest.mark.asyncio
    async def test_run_summary_manager_upload_error_handling(self, temp_dir, mock_run_name, mock_book_manager):
        """Test error handling during storage upload."""
        # Mock storage to raise exception
        mock_book_manager.storage.write_file.side_effect = Exception("Storage error")

        with patch("grin_to_s3.process_summary.Path") as mock_path:
            mock_path.return_value = temp_dir / mock_run_name
            manager = RunSummaryManager(mock_run_name)
            manager.enable_storage_upload(mock_book_manager)

            # Create a test summary
            summary = RunSummary(run_name=mock_run_name)

            # Save summary (should handle upload error gracefully)
            await manager.save_summary(summary)

            # Local file should still be created
            assert manager.summary_file.exists()

            # Upload should have been attempted
            mock_book_manager.storage.write_file.assert_called_once()

    @pytest.mark.asyncio
    async def test_create_process_summary_with_storage(self, temp_dir, mock_run_name, mock_book_manager):
        """Test create_process_summary with storage upload enabled."""
        with patch("grin_to_s3.process_summary.Path") as mock_path:
            mock_path.return_value = temp_dir / mock_run_name

            # Create process summary with storage
            summary = await create_process_summary(mock_run_name, "test_stage", mock_book_manager)

            # Verify summary was created
            assert summary.run_name == mock_run_name
            assert "test_stage" in summary.stages
            assert summary.stages["test_stage"].start_time is not None

    @pytest.mark.asyncio
    async def test_save_process_summary_with_storage(self, temp_dir, mock_run_name, mock_book_manager):
        """Test save_process_summary with storage upload enabled."""
        with patch("grin_to_s3.process_summary.Path") as mock_path:
            mock_path.return_value = temp_dir / mock_run_name

            # Create summary
            summary = RunSummary(run_name=mock_run_name)
            stage = summary.start_stage("collect")
            stage.books_collected = 5

            # Save with storage
            await save_process_summary(summary, mock_book_manager)

            # Verify upload was called
            mock_book_manager.storage.write_file.assert_called_once()

    @pytest.mark.asyncio
    async def test_upload_path_generation(self, temp_dir, mock_run_name, mock_book_manager):
        """Test that upload paths are generated correctly."""
        with patch("grin_to_s3.process_summary.Path") as mock_path:
            summary_dir = temp_dir / mock_run_name
            summary_dir.mkdir(parents=True, exist_ok=True)
            summary_file = summary_dir / "process_summary.json"
            mock_path.return_value = summary_file

            manager = RunSummaryManager(mock_run_name)
            manager.enable_storage_upload(mock_book_manager)

            # Create a summary file first
            summary = RunSummary(mock_run_name)
            await manager.save_summary(summary)

            # Test the private method directly
            await manager._upload_to_storage()

            # Verify the path structure
            mock_book_manager.storage.write_file.assert_called()
            call_args = mock_book_manager.storage.write_file.call_args
            storage_path = call_args[0][0]

            # Should follow the pattern: bucket_meta/base_prefix/process_summary.json.gz (now compressed)
            expected_path = "test-meta/test_run/process_summary.json.gz"
            assert storage_path == expected_path

    @pytest.mark.asyncio
    async def test_upload_disabled_by_default(self, temp_dir, mock_run_name):
        """Test that upload is disabled by default."""
        with patch("grin_to_s3.process_summary.Path") as mock_path:
            mock_path.return_value = temp_dir / mock_run_name
            manager = RunSummaryManager(mock_run_name)

            # Create summary
            summary = RunSummary(run_name=mock_run_name)

            # Save without enabling storage
            await manager.save_summary(summary)

            # Local file should be created
            assert manager.summary_file.exists()

            # Storage upload should not be enabled
            assert not manager._storage_upload_enabled
            assert manager._book_manager is None

    @pytest.mark.asyncio
    async def test_summary_content_structure(self, temp_dir, mock_run_name, mock_book_manager):
        """Test that uploaded summary contains expected structure."""
        with patch("grin_to_s3.process_summary.Path") as mock_path:
            mock_path.return_value = temp_dir / mock_run_name
            manager = RunSummaryManager(mock_run_name)
            manager.enable_storage_upload(mock_book_manager)

            # Create comprehensive summary
            summary = RunSummary(run_name=mock_run_name)

            # Add collect stage
            collect_stage = summary.start_stage("collect")
            collect_stage.set_command_arg("storage_type", "s3")
            collect_stage.set_command_arg("limit", 1000)
            collect_stage.books_collected = 98
            collect_stage.collection_failed = 2
            collect_stage.add_progress_update("Collection started")
            collect_stage.add_error("NetworkError", "Connection timeout")
            summary.end_stage("collect")

            # Add sync stage
            sync_stage = summary.start_stage("sync")
            sync_stage.set_command_arg("force_mode", False)
            sync_stage.books_synced = 50
            sync_stage.add_progress_update("Sync completed")
            summary.end_stage("sync")

            # End run
            summary.end_run()

            # Save summary
            await manager.save_summary(summary)

            # Verify file content structure
            with open(manager.summary_file) as f:
                content = json.load(f)

            # Check required fields
            assert content["run_name"] == mock_run_name
            assert content["total_items_processed"] == 150
            assert content["total_items_successful"] == 148
            assert content["total_items_failed"] == 2
            assert content["overall_success_rate_percent"] == pytest.approx(98.67, rel=1e-2)
            assert content["is_completed"] is True
            assert content["has_errors"] is True

            # Check stage details
            assert "collect" in content["stages"]
            assert "sync" in content["stages"]

            collect_data = content["stages"]["collect"]
            assert collect_data["books_collected"] == 98
            assert collect_data["collection_failed"] == 2
            assert collect_data["error_count"] == 1
            assert collect_data["command_args"]["storage_type"] == "s3"
            assert collect_data["command_args"]["limit"] == 1000
            assert collect_data["is_completed"] is True

            sync_data = content["stages"]["sync"]
            assert sync_data["books_synced"] == 50
            assert sync_data["command_args"]["force_mode"] is False
            assert sync_data["is_completed"] is True
