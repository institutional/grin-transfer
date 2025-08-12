"""Tests for sync pipeline staging cleanup behavior."""

from pathlib import Path
from unittest.mock import AsyncMock, MagicMock, patch

import pytest


class TestSyncStagingCleanup:
    """Test staging cleanup behavior in sync pipeline."""

    def _setup_staging_cleanup_test(self, sync_pipeline, skip_cleanup=False):
        """Helper to set up staging manager for cleanup tests."""
        sync_pipeline.staging_manager = MagicMock()
        sync_pipeline.staging_manager.staging_path = Path("/tmp/staging")
        sync_pipeline.skip_staging_cleanup = skip_cleanup

    @pytest.mark.asyncio
    async def test_successful_sync_cleans_up_staging(self, sync_pipeline):
        """Test that successful sync cleans up staging directory."""
        self._setup_staging_cleanup_test(sync_pipeline)

        # Mock shutil.rmtree to track calls
        with patch("grin_to_s3.sync.pipeline.shutil.rmtree") as mock_rmtree:
            # Mock successful completion
            await sync_pipeline.cleanup(sync_successful=True)

            # Verify staging directory was cleaned up
            mock_rmtree.assert_called_once_with(
                sync_pipeline.staging_manager.staging_path,
                ignore_errors=True
            )

    @pytest.mark.asyncio
    async def test_failed_sync_preserves_staging(self, sync_pipeline):
        """Test that failed sync preserves staging directory."""
        self._setup_staging_cleanup_test(sync_pipeline)

        # Mock shutil.rmtree to track calls
        with patch("grin_to_s3.sync.pipeline.shutil.rmtree") as mock_rmtree:
            # Mock failed completion
            await sync_pipeline.cleanup(sync_successful=False)

            # Verify staging directory was NOT cleaned up
            mock_rmtree.assert_not_called()

    @pytest.mark.asyncio
    async def test_skip_staging_cleanup_flag_prevents_cleanup(self, sync_pipeline):
        """Test that --skip-staging-cleanup flag prevents cleanup even on success."""
        self._setup_staging_cleanup_test(sync_pipeline, skip_cleanup=True)

        with patch("grin_to_s3.sync.pipeline.shutil.rmtree") as mock_rmtree:
            # Mock successful completion
            await sync_pipeline.cleanup(sync_successful=True)

            # Verify staging directory was NOT cleaned up due to flag
            mock_rmtree.assert_not_called()

    @pytest.mark.asyncio
    async def test_no_staging_manager_handles_gracefully(self, sync_pipeline):
        """Test that cleanup handles missing staging manager gracefully."""
        # Set staging manager to None
        sync_pipeline.staging_manager = None
        sync_pipeline.skip_staging_cleanup = False

        with patch("grin_to_s3.sync.pipeline.shutil.rmtree") as mock_rmtree:
            # Mock successful completion
            await sync_pipeline.cleanup(sync_successful=True)

            # Verify no cleanup was attempted
            mock_rmtree.assert_not_called()

    @pytest.mark.asyncio
    async def test_cleanup_error_handling(self, sync_pipeline):
        """Test that cleanup handles errors gracefully."""
        self._setup_staging_cleanup_test(sync_pipeline)

        with patch("grin_to_s3.sync.pipeline.shutil.rmtree") as mock_rmtree:
            # Mock rmtree to raise an exception
            mock_rmtree.side_effect = OSError("Permission denied")

            with patch("grin_to_s3.sync.pipeline.logger") as mock_logger:
                # Mock successful completion
                await sync_pipeline.cleanup(sync_successful=True)

                # Verify cleanup was attempted
                mock_rmtree.assert_called_once()

                # Verify error was logged (check for specific error message)
                mock_logger.warning.assert_any_call("Error during final staging cleanup: Permission denied")

    @pytest.mark.asyncio
    async def test_cleanup_called_with_success_status_in_run_sync(self, sync_pipeline):
        """Test that setup_sync_loop calls cleanup with correct success status."""
        # Mock the pipeline methods to avoid actual sync work
        with patch.object(sync_pipeline, "_run_sync"):
            with patch.object(sync_pipeline, "cleanup") as mock_cleanup:
                with patch("grin_to_s3.processing.get_converted_books") as mock_get_books:
                    mock_get_books.return_value = []  # No books to process

                    # Mock the progress tracker methods - include all required fields
                    sync_pipeline.db_tracker.get_sync_stats = AsyncMock(return_value={
                        "total_books": 0,
                        "synced_books": 0,
                        "failed_books": 0,
                        "pending_books": 0
                    })
                    sync_pipeline.db_tracker.get_books_for_sync = AsyncMock(return_value=[])

                    # Mock get_sync_status to return the correct format
                    with patch.object(sync_pipeline, "get_sync_status") as mock_get_status:
                        mock_get_status.return_value = {
                            "total_converted": 0,
                            "synced": 0,
                            "failed": 0,
                            "pending": 0
                        }

                        await sync_pipeline.setup_sync_loop(queues=["converted"])

                        # Verify cleanup was called with success=True (no books to process is success)
                        mock_cleanup.assert_called_once_with(True)

    @pytest.mark.asyncio
    async def test_cleanup_called_with_failure_status_on_exception(self, sync_pipeline):
        """Test that setup_sync_loop calls cleanup with failure status on exception."""
        with patch.object(sync_pipeline, "cleanup") as mock_cleanup:
            with patch("grin_to_s3.sync.pipeline.get_books_from_queue") as mock_get_books:
                # Mock an exception during sync
                mock_get_books.side_effect = Exception("Test error")

                # Run sync and expect it to handle the exception
                await sync_pipeline.setup_sync_loop(queues=["converted"])

                # Verify cleanup was called with success=False
                mock_cleanup.assert_called_once_with(False)

    @pytest.mark.asyncio
    async def test_cleanup_called_with_failure_status_on_keyboard_interrupt(self, sync_pipeline):
        """Test that setup_sync_loop calls cleanup with failure status on keyboard interrupt."""
        with patch.object(sync_pipeline, "cleanup") as mock_cleanup:
            with patch("grin_to_s3.sync.pipeline.get_books_from_queue") as mock_get_books:
                # Mock a keyboard interrupt during sync
                mock_get_books.side_effect = KeyboardInterrupt()

                # Run sync and expect it to handle the interrupt
                await sync_pipeline.setup_sync_loop(queues=["converted"])

                # Verify cleanup was called with success=False
                mock_cleanup.assert_called_once_with(False)

    @pytest.mark.asyncio
    async def test_cleanup_no_staging_cleanup_on_failure(self, sync_pipeline):
        """Test that staging directory is not cleaned up when sync fails."""
        self._setup_staging_cleanup_test(sync_pipeline)

        with patch("grin_to_s3.sync.pipeline.shutil.rmtree") as mock_rmtree:
            # Mock failed completion
            await sync_pipeline.cleanup(sync_successful=False)

            # Verify staging cleanup was not called for failed sync
            mock_rmtree.assert_not_called()

    @pytest.mark.asyncio
    async def test_cleanup_logs_cleanup_message_on_success(self, sync_pipeline):
        """Test that cleanup logs cleanup message when sync succeeds."""
        self._setup_staging_cleanup_test(sync_pipeline)

        with patch("grin_to_s3.sync.pipeline.shutil.rmtree"):
            with patch("grin_to_s3.sync.pipeline.logger") as mock_logger:
                # Mock successful completion
                await sync_pipeline.cleanup(sync_successful=True)

                # Verify cleanup messages were logged
                mock_logger.info.assert_any_call("Performing final staging directory cleanup...")
                mock_logger.info.assert_any_call("Staging directory cleaned up")
