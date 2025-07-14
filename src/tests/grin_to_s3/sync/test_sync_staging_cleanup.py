"""Tests for sync pipeline staging cleanup behavior."""

import tempfile
from pathlib import Path
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from grin_to_s3.sync.pipeline import SyncPipeline


class TestSyncStagingCleanup:
    """Test staging cleanup behavior in sync pipeline."""

    @pytest.fixture
    def mock_run_config(self, test_config_builder):
        """Create a mock RunConfig for testing."""
        return (
            test_config_builder.with_library_directory("TestLib")
            .minio_storage(bucket_raw="test-raw")
            .with_concurrent_downloads(2)
            .with_concurrent_uploads(1)
            .with_batch_size(10)
            .build()
        )

    @pytest.fixture
    def mock_pipeline_dependencies(self):
        """Mock all pipeline dependencies."""
        with patch("grin_to_s3.sync.pipeline.GRINClient") as mock_client:
            mock_client.return_value.auth = MagicMock()
            with patch("grin_to_s3.sync.pipeline.SQLiteProgressTracker") as mock_tracker:
                mock_tracker.return_value.get_sync_stats = AsyncMock(return_value={
                    "total_books": 100,
                    "synced_books": 50,
                    "failed_books": 0,
                    "pending_books": 50
                })
                mock_tracker.return_value.get_books_for_sync = AsyncMock(return_value=[])
                mock_tracker.return_value.add_status_change = AsyncMock()
                mock_tracker.return_value.update_sync_data = AsyncMock()

                with patch("grin_to_s3.sync.pipeline.StagingDirectoryManager") as mock_staging:
                    mock_staging.return_value.staging_path = Path("/tmp/staging")
                    mock_staging.return_value.check_disk_space = MagicMock(return_value=True)

                    yield {
                        "client": mock_client,
                        "tracker": mock_tracker,
                        "staging": mock_staging
                    }

    @pytest.fixture
    def pipeline(self, mock_run_config, mock_pipeline_dependencies):
        """Create a sync pipeline for testing."""
        with tempfile.TemporaryDirectory() as temp_dir:
            db_path = str(Path(temp_dir) / "test.db")

            pipeline = SyncPipeline.from_run_config(
                config=mock_run_config,
                process_summary_stage=MagicMock(),
                skip_staging_cleanup=False  # Test default behavior
            )
            pipeline.db_path = db_path
            pipeline.staging_manager = mock_pipeline_dependencies["staging"].return_value

            yield pipeline

    @pytest.mark.asyncio
    async def test_successful_sync_cleans_up_staging(self, pipeline, mock_pipeline_dependencies):
        """Test that successful sync cleans up staging directory."""
        # Mock shutil.rmtree to track calls
        with patch("grin_to_s3.sync.pipeline.shutil.rmtree") as mock_rmtree:
            # Mock successful completion
            await pipeline.cleanup(sync_successful=True)

            # Verify staging directory was cleaned up
            mock_rmtree.assert_called_once_with(
                pipeline.staging_manager.staging_path,
                ignore_errors=True
            )

    @pytest.mark.asyncio
    async def test_failed_sync_preserves_staging(self, pipeline, mock_pipeline_dependencies):
        """Test that failed sync preserves staging directory."""
        # Mock shutil.rmtree to track calls
        with patch("grin_to_s3.sync.pipeline.shutil.rmtree") as mock_rmtree:
            # Mock failed completion
            await pipeline.cleanup(sync_successful=False)

            # Verify staging directory was NOT cleaned up
            mock_rmtree.assert_not_called()

    @pytest.mark.asyncio
    async def test_skip_staging_cleanup_flag_prevents_cleanup(self, mock_run_config, mock_pipeline_dependencies):
        """Test that --skip-staging-cleanup flag prevents cleanup even on success."""
        with tempfile.TemporaryDirectory() as temp_dir:
            db_path = str(Path(temp_dir) / "test.db")

            # Create pipeline with skip_staging_cleanup=True
            pipeline = SyncPipeline.from_run_config(
                config=mock_run_config,
                process_summary_stage=MagicMock(),
                skip_staging_cleanup=True  # Skip cleanup
            )
            pipeline.db_path = db_path
            pipeline.staging_manager = mock_pipeline_dependencies["staging"].return_value

            with patch("grin_to_s3.sync.pipeline.shutil.rmtree") as mock_rmtree:
                # Mock successful completion
                await pipeline.cleanup(sync_successful=True)

                # Verify staging directory was NOT cleaned up due to flag
                mock_rmtree.assert_not_called()

    @pytest.mark.asyncio
    async def test_no_staging_manager_handles_gracefully(self, pipeline, mock_pipeline_dependencies):
        """Test that cleanup handles missing staging manager gracefully."""
        # Set staging manager to None
        pipeline.staging_manager = None

        with patch("grin_to_s3.sync.pipeline.shutil.rmtree") as mock_rmtree:
            # Mock successful completion
            await pipeline.cleanup(sync_successful=True)

            # Verify no cleanup was attempted
            mock_rmtree.assert_not_called()

    @pytest.mark.asyncio
    async def test_cleanup_error_handling(self, pipeline, mock_pipeline_dependencies):
        """Test that cleanup handles errors gracefully."""
        with patch("grin_to_s3.sync.pipeline.shutil.rmtree") as mock_rmtree:
            # Mock rmtree to raise an exception
            mock_rmtree.side_effect = OSError("Permission denied")

            with patch("grin_to_s3.sync.pipeline.logger") as mock_logger:
                # Mock successful completion
                await pipeline.cleanup(sync_successful=True)

                # Verify cleanup was attempted
                mock_rmtree.assert_called_once()

                # Verify error was logged (check for specific error message)
                mock_logger.warning.assert_any_call("Error during final staging cleanup: Permission denied")

    @pytest.mark.asyncio
    async def test_cleanup_called_with_success_status_in_run_sync(self, pipeline, mock_pipeline_dependencies):
        """Test that run_sync calls cleanup with correct success status."""
        # Mock the pipeline methods to avoid actual sync work
        with patch.object(pipeline, "_run_block_storage_sync"):
            with patch.object(pipeline, "cleanup") as mock_cleanup:
                with patch("grin_to_s3.sync.pipeline.get_converted_books") as mock_get_books:
                    mock_get_books.return_value = []  # No books to process

                    # Mock the progress tracker methods - include all required fields
                    pipeline.db_tracker.get_sync_stats = AsyncMock(return_value={
                        "total_books": 0,
                        "synced_books": 0,
                        "failed_books": 0,
                        "pending_books": 0
                    })
                    pipeline.db_tracker.get_books_for_sync = AsyncMock(return_value=[])

                    # Mock get_sync_status to return the correct format
                    with patch.object(pipeline, "get_sync_status") as mock_get_status:
                        mock_get_status.return_value = {
                            "total_converted": 0,
                            "synced": 0,
                            "failed": 0,
                            "pending": 0
                        }

                        await pipeline.run_sync()

                        # Verify cleanup was called with success=True (no books to process is success)
                        mock_cleanup.assert_called_once_with(True)

    @pytest.mark.asyncio
    async def test_cleanup_called_with_failure_status_on_exception(self, pipeline, mock_pipeline_dependencies):
        """Test that run_sync calls cleanup with failure status on exception."""
        with patch.object(pipeline, "cleanup") as mock_cleanup:
            with patch("grin_to_s3.sync.pipeline.get_converted_books") as mock_get_books:
                # Mock an exception during sync
                mock_get_books.side_effect = Exception("Test error")

                # Run sync and expect it to handle the exception
                await pipeline.run_sync()

                # Verify cleanup was called with success=False
                mock_cleanup.assert_called_once_with(False)

    @pytest.mark.asyncio
    async def test_cleanup_called_with_failure_status_on_keyboard_interrupt(self, pipeline, mock_pipeline_dependencies):
        """Test that run_sync calls cleanup with failure status on keyboard interrupt."""
        with patch.object(pipeline, "cleanup") as mock_cleanup:
            with patch("grin_to_s3.sync.pipeline.get_converted_books") as mock_get_books:
                # Mock a keyboard interrupt during sync
                mock_get_books.side_effect = KeyboardInterrupt()

                # Run sync and expect it to handle the interrupt
                await pipeline.run_sync()

                # Verify cleanup was called with success=False
                mock_cleanup.assert_called_once_with(False)

    @pytest.mark.asyncio
    async def test_cleanup_logs_preservation_message_on_failure(self, pipeline, mock_pipeline_dependencies):
        """Test that cleanup logs preservation message when sync fails."""
        with patch("grin_to_s3.sync.pipeline.logger") as mock_logger:
            # Mock failed completion
            await pipeline.cleanup(sync_successful=False)

            # Verify preservation message was logged
            mock_logger.info.assert_any_call("Staging directory preserved due to sync failure")

    @pytest.mark.asyncio
    async def test_cleanup_logs_cleanup_message_on_success(self, pipeline, mock_pipeline_dependencies):
        """Test that cleanup logs cleanup message when sync succeeds."""
        with patch("grin_to_s3.sync.pipeline.shutil.rmtree"):
            with patch("grin_to_s3.sync.pipeline.logger") as mock_logger:
                # Mock successful completion
                await pipeline.cleanup(sync_successful=True)

                # Verify cleanup messages were logged
                mock_logger.info.assert_any_call("Performing final staging directory cleanup...")
                mock_logger.info.assert_any_call("Staging directory cleaned up")
