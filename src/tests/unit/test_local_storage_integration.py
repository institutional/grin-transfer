"""Integration tests for local storage sync pipeline."""

import tempfile
from pathlib import Path
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from grin_to_s3.sync import SyncPipeline


class TestLocalStorageIntegration:
    """Integration tests that verify the complete local storage pipeline flow."""

    @pytest.mark.asyncio
    async def test_local_storage_sync_pipeline_flow(self):
        """Test the complete local storage sync pipeline flow.

        This test would have caught the ProgressReporter.stop() issue and
        target directory display issue.
        """
        with tempfile.TemporaryDirectory() as temp_dir:
            # Create test database
            db_path = Path(temp_dir) / "test.db"

            # Create sync pipeline with local storage
            storage_config = {"base_path": temp_dir}
            pipeline = SyncPipeline(
                db_path=str(db_path),
                storage_type="local",
                storage_config=storage_config,
                library_directory="test_library",
                concurrent_downloads=1,
            )

            # Mock all external dependencies
            pipeline.db_tracker = AsyncMock()
            pipeline.grin_client = MagicMock()

            # Mock database methods
            pipeline.db_tracker.get_books_for_sync = AsyncMock(return_value=["TEST123"])
            pipeline.db_tracker.add_status_change = AsyncMock()
            pipeline.db_tracker.update_sync_data = AsyncMock()

            # Mock ETag and download methods to simulate skip scenario
            with patch.object(pipeline, '_check_google_etag', return_value=("test-etag", 1000)):
                with patch.object(pipeline, '_should_skip_download', return_value=True):
                    # This should exercise the complete pipeline including ProgressReporter
                    await pipeline._run_local_storage_sync(["TEST123"], 1)

            # Verify database updates were called (shows the pipeline ran)
            pipeline.db_tracker.update_sync_data.assert_called()

    @pytest.mark.asyncio
    async def test_local_storage_target_directory_display(self):
        """Test that target directory is displayed correctly for various storage configs."""
        with tempfile.TemporaryDirectory() as temp_dir:
            db_path = Path(temp_dir) / "test.db"

            # Test case 1: Valid storage config
            storage_config = {"base_path": "/valid/path"}
            pipeline = SyncPipeline(
                db_path=str(db_path),
                storage_type="local",
                storage_config=storage_config,
                library_directory="test_library",
            )

            # Mock dependencies
            pipeline.db_tracker = AsyncMock()
            pipeline.db_tracker.get_books_for_sync = AsyncMock(return_value=[])

            # Capture print output to verify directory display
            with patch('builtins.print') as mock_print:
                await pipeline._run_local_storage_sync([], 0)

                # Check that target directory was printed correctly
                print_calls = [str(call) for call in mock_print.call_args_list]
                target_dir_printed = any("/valid/path" in call for call in print_calls)
                assert target_dir_printed, "Target directory should be displayed correctly"

            # Test case 2: None storage config (edge case)
            pipeline.storage_config = None

            with patch('builtins.print') as mock_print:
                await pipeline._run_local_storage_sync([], 0)

                # Should handle None gracefully
                print_calls = [str(call) for call in mock_print.call_args_list]
                unknown_printed = any("Unknown" in call for call in print_calls)
                assert unknown_printed, "Should display 'Unknown' for missing config"

    @pytest.mark.asyncio
    async def test_progress_reporter_methods_exist(self):
        """Test that ProgressReporter has expected methods and they work correctly."""
        with tempfile.TemporaryDirectory() as temp_dir:
            db_path = Path(temp_dir) / "test.db"
            storage_config = {"base_path": temp_dir}

            pipeline = SyncPipeline(
                db_path=str(db_path),
                storage_type="local",
                storage_config=storage_config,
                library_directory="test_library",
            )

            # Verify ProgressReporter has the methods we expect
            reporter = pipeline.progress_reporter

            # These should exist and be callable
            assert hasattr(reporter, 'start'), "ProgressReporter should have start() method"
            assert hasattr(reporter, 'increment'), "ProgressReporter should have increment() method"
            assert hasattr(reporter, 'finish'), "ProgressReporter should have finish() method"

            # This should NOT exist (would have caught the .stop() bug)
            assert not hasattr(reporter, 'stop'), "ProgressReporter should NOT have stop() method"

            # Test that the methods actually work
            reporter.start()
            reporter.increment(1)
            reporter.finish()  # This is what should be called, not stop()

    @pytest.mark.asyncio
    async def test_etag_refactoring_integration(self):
        """Test that ETag refactoring works in the context of local storage pipeline."""
        with tempfile.TemporaryDirectory() as temp_dir:
            db_path = Path(temp_dir) / "test.db"
            storage_config = {"base_path": temp_dir}

            pipeline = SyncPipeline(
                db_path=str(db_path),
                storage_type="local",
                storage_config=storage_config,
                library_directory="test_library",
            )

            # Mock database
            pipeline.db_tracker = AsyncMock()
            pipeline.db_tracker.update_sync_data = AsyncMock()

            # Test the refactored ETag method directly
            with patch.object(pipeline, '_check_google_etag', return_value=("etag123", 5000)):
                with patch.object(pipeline, '_should_skip_download', return_value=True):
                    # This exercises the refactored method
                    skip_result, google_etag, file_size = await pipeline._check_and_handle_etag_skip("TEST456")

                    # Verify return format is correct
                    assert skip_result is not None
                    assert skip_result["status"] == "completed"
                    assert skip_result["skipped"] is True
                    assert google_etag == "etag123"
                    assert file_size == 5000

                    # Verify database was updated
                    pipeline.db_tracker.update_sync_data.assert_called_once()

    @pytest.mark.asyncio
    async def test_staging_manager_none_handling(self):
        """Test that local storage properly handles None staging_manager."""
        with tempfile.TemporaryDirectory() as temp_dir:
            db_path = Path(temp_dir) / "test.db"
            storage_config = {"base_path": temp_dir}

            pipeline = SyncPipeline(
                db_path=str(db_path),
                storage_type="local",
                storage_config=storage_config,
                library_directory="test_library",
            )

            # Verify staging_manager is None for local storage
            assert pipeline.staging_manager is None, "Local storage should not have staging manager"

            # Verify staging directory doesn't exist
            staging_dir = Path(temp_dir) / "staging"
            assert not staging_dir.exists(), "Staging directory should not be created for local storage"

