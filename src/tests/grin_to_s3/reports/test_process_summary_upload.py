"""
Integration tests for process summary upload functionality.
"""

import json
import tempfile
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest

from grin_to_s3.process_summary import (
    RunSummary,
    RunSummaryManager,
    create_book_manager_for_uploads,
    create_process_summary,
    save_process_summary,
)
from tests.test_utils.unified_mocks import MockStorageFactory


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
        return MockStorageFactory.create_storage(storage_type="s3")

    @pytest.fixture
    def mock_book_storage(self, mock_storage):
        """Create a mock BookStorage instance."""
        return MockStorageFactory.create_book_manager(base_prefix="test_run")

    @pytest.mark.asyncio
    async def test_run_summary_manager_storage_upload(self, temp_dir, mock_run_name, mock_book_storage):
        """Test RunSummaryManager storage upload functionality."""
        # Create manager with temporary directory
        with patch("grin_to_s3.process_summary.Path") as mock_path:
            mock_path.return_value = temp_dir / mock_run_name
            manager = RunSummaryManager(mock_run_name)

            # Enable storage upload
            manager.enable_storage_upload(mock_book_storage)

            # Create a test summary
            summary = RunSummary(run_name=mock_run_name)
            stage = summary.start_stage("test_stage")
            stage.increment_items(processed=10, successful=9, failed=1)
            stage.add_progress_update("Test progress")
            summary.end_stage("test_stage")

            # Save summary (should trigger upload)
            await manager.save_summary(summary)

            # Verify local file was created
            assert manager.summary_file.exists()

            # Verify storage upload was called
            mock_book_storage.storage.write_file.assert_called_once()

            # Verify the storage path is correct
            call_args = mock_book_storage.storage.write_file.call_args
            storage_path, local_path = call_args[0]
            assert storage_path == "test-meta/test_run/process_summary_test_run_upload.json"
            assert local_path == str(manager.summary_file)

    @pytest.mark.asyncio
    async def test_run_summary_manager_upload_error_handling(self, temp_dir, mock_run_name, mock_book_storage):
        """Test error handling during storage upload."""
        # Mock storage to raise exception
        mock_book_storage.storage.write_file.side_effect = Exception("Storage error")

        with patch("grin_to_s3.process_summary.Path") as mock_path:
            mock_path.return_value = temp_dir / mock_run_name
            manager = RunSummaryManager(mock_run_name)
            manager.enable_storage_upload(mock_book_storage)

            # Create a test summary
            summary = RunSummary(run_name=mock_run_name)

            # Save summary (should handle upload error gracefully)
            await manager.save_summary(summary)

            # Local file should still be created
            assert manager.summary_file.exists()

            # Upload should have been attempted
            mock_book_storage.storage.write_file.assert_called_once()

    @pytest.mark.asyncio
    async def test_create_process_summary_with_storage(self, temp_dir, mock_run_name, mock_book_storage):
        """Test create_process_summary with storage upload enabled."""
        with patch("grin_to_s3.process_summary.Path") as mock_path:
            mock_path.return_value = temp_dir / mock_run_name

            # Create process summary with storage
            summary = await create_process_summary(mock_run_name, "test_stage", mock_book_storage)

            # Verify summary was created
            assert summary.run_name == mock_run_name
            assert "test_stage" in summary.stages
            assert summary.stages["test_stage"].start_time is not None

    @pytest.mark.asyncio
    async def test_save_process_summary_with_storage(self, temp_dir, mock_run_name, mock_book_storage):
        """Test save_process_summary with storage upload enabled."""
        with patch("grin_to_s3.process_summary.Path") as mock_path:
            mock_path.return_value = temp_dir / mock_run_name

            # Create summary
            summary = RunSummary(run_name=mock_run_name)
            stage = summary.start_stage("test_stage")
            stage.increment_items(processed=5, successful=5)

            # Save with storage
            await save_process_summary(summary, mock_book_storage)

            # Verify upload was called
            mock_book_storage.storage.write_file.assert_called_once()

    @pytest.mark.asyncio
    async def test_create_book_manager_for_uploads(self, temp_dir, mock_run_name):
        """Test creating BookStorage for uploads."""
        # Mock run configuration
        mock_run_config = MagicMock()
        mock_run_config.storage_type = "s3"
        mock_run_config.storage_config = {
            "config": {
                "bucket_raw": "test-raw",
                "bucket_meta": "test-meta",
                "bucket_full": "test-full",
            }
        }

        # Mock storage creation
        mock_storage = MagicMock()

        with patch("grin_to_s3.run_config.find_run_config", return_value=mock_run_config):
            with patch("grin_to_s3.storage.factories.create_storage_from_config", return_value=mock_storage):
                book_storage = await create_book_manager_for_uploads(mock_run_name)

                # Verify BookStorage was created
                assert book_storage is not None
                assert book_storage.bucket_raw == "test-raw"
                assert book_storage.bucket_meta == "test-meta"
                assert book_storage.bucket_full == "test-full"
                assert book_storage.base_prefix == mock_run_name

    @pytest.mark.asyncio
    async def test_create_book_manager_for_uploads_no_config(self, mock_run_name):
        """Test creating BookStorage when no config is found."""
        with patch("grin_to_s3.run_config.find_run_config", return_value=None):
            book_storage = await create_book_manager_for_uploads(mock_run_name)

            # Should return None when no config is found
            assert book_storage is None

    @pytest.mark.asyncio
    async def test_create_book_manager_for_uploads_no_storage_type(self, mock_run_name):
        """Test creating BookStorage when storage type is missing."""
        # Mock run configuration with no storage type
        mock_run_config = MagicMock()
        mock_run_config.storage_type = None
        mock_run_config.storage_config = {"bucket_raw": "test-raw"}

        with patch("grin_to_s3.run_config.find_run_config", return_value=mock_run_config):
            book_storage = await create_book_manager_for_uploads(mock_run_name)

            # Should return None when storage type is missing
            assert book_storage is None

    @pytest.mark.asyncio
    async def test_create_book_manager_for_uploads_exception(self, mock_run_name):
        """Test creating BookStorage when exception occurs."""
        with patch("grin_to_s3.run_config.find_run_config", side_effect=Exception("Config error")):
            book_storage = await create_book_manager_for_uploads(mock_run_name)

            # Should return None when exception occurs
            assert book_storage is None

    @pytest.mark.asyncio
    async def test_upload_path_generation(self, mock_run_name, mock_book_storage):
        """Test that upload paths are generated correctly."""
        manager = RunSummaryManager(mock_run_name)
        manager.enable_storage_upload(mock_book_storage)

        # Test the private method directly
        await manager._upload_to_storage()

        # Verify the path structure
        mock_book_storage.storage.write_file.assert_called_once()
        call_args = mock_book_storage.storage.write_file.call_args
        storage_path = call_args[0][0]

        # Should follow the pattern: bucket_meta/base_prefix/process_summary_run_name.json
        expected_path = f"test-meta/test_run/process_summary_{mock_run_name}.json"
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
            assert manager._book_storage is None

    @pytest.mark.asyncio
    async def test_summary_content_structure(self, temp_dir, mock_run_name, mock_book_storage):
        """Test that uploaded summary contains expected structure."""
        with patch("grin_to_s3.process_summary.Path") as mock_path:
            mock_path.return_value = temp_dir / mock_run_name
            manager = RunSummaryManager(mock_run_name)
            manager.enable_storage_upload(mock_book_storage)

            # Create comprehensive summary
            summary = RunSummary(run_name=mock_run_name)

            # Add collect stage
            collect_stage = summary.start_stage("collect")
            collect_stage.set_command_arg("storage_type", "s3")
            collect_stage.set_command_arg("limit", 1000)
            collect_stage.increment_items(processed=100, successful=98, failed=2)
            collect_stage.add_progress_update("Collection started")
            collect_stage.add_error("NetworkError", "Connection timeout")
            summary.end_stage("collect")

            # Add sync stage
            sync_stage = summary.start_stage("sync")
            sync_stage.set_command_arg("force_mode", False)
            sync_stage.increment_items(processed=50, successful=50, bytes_count=1048576)
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
            assert collect_data["items_processed"] == 100
            assert collect_data["error_count"] == 1
            assert collect_data["command_args"]["storage_type"] == "s3"
            assert collect_data["command_args"]["limit"] == 1000
            assert collect_data["is_completed"] is True

            sync_data = content["stages"]["sync"]
            assert sync_data["items_processed"] == 50
            assert sync_data["bytes_processed"] == 1048576
            assert sync_data["command_args"]["force_mode"] is False
            assert sync_data["is_completed"] is True
