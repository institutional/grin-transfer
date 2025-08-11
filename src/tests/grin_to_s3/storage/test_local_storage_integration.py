"""Integration tests for local storage sync pipeline."""

import tempfile
from pathlib import Path
from unittest.mock import AsyncMock, patch

import pytest

from grin_to_s3.collect_books.models import SQLiteProgressTracker
from grin_to_s3.sync.pipeline import SyncPipeline


class TestLocalStorageIntegration:
    """Integration tests that verify the complete local storage pipeline flow."""

    @pytest.mark.asyncio
    async def test_local_storage_sync_pipeline_flow(self, mock_process_stage, test_config_builder):
        """Test the complete local storage sync pipeline flow."""
        with tempfile.TemporaryDirectory() as temp_dir:
            # Create test database
            db_path = Path(temp_dir) / "test.db"

            # Initialize database
            tracker = SQLiteProgressTracker(str(db_path))
            await tracker.init_db()
            await tracker.close()

            # Create sync pipeline with local storage
            config = (
                test_config_builder.with_db_path(str(db_path))
                .local_storage(temp_dir)
                .with_concurrent_downloads(1)
                .build()
            )

            pipeline = SyncPipeline.from_run_config(
                config=config,
                process_summary_stage=mock_process_stage,
            )

            # Mock database methods
            pipeline.db_tracker.get_books_for_sync = AsyncMock(return_value=["TEST123"])
            pipeline.db_tracker.update_sync_data = AsyncMock()

            # Mock converted books
            mock_converted_books = {"TEST123"}

            with patch("grin_to_s3.processing.get_converted_books", return_value=mock_converted_books):
                # This should exercise the complete pipeline
                status = await pipeline.get_sync_status()

                # Verify pipeline can be initialized and provide status
                assert "session_stats" in status

    @pytest.mark.asyncio
    async def test_local_storage_startup_configuration_display(self, mock_process_stage, test_config_builder):
        """Test that storage configuration is displayed at pipeline startup."""
        with tempfile.TemporaryDirectory() as temp_dir:
            db_path = Path(temp_dir) / "test.db"

            # Initialize database
            tracker = SQLiteProgressTracker(str(db_path))
            await tracker.init_db()
            await tracker.close()

            # Test case 1: Valid storage config
            config = test_config_builder.with_db_path(str(db_path)).local_storage("/valid/path").build()

            pipeline = SyncPipeline.from_run_config(
                config=config,
                process_summary_stage=mock_process_stage,
            )

            # Mock dependencies for run_sync startup
            pipeline.db_tracker.get_books_for_sync = AsyncMock(return_value=[])

            # Mock get_converted_books to return empty list so sync exits early
            with patch("grin_to_s3.processing.get_converted_books", return_value=set()):
                # Capture print output to verify storage configuration display
                with patch("builtins.print") as mock_print:
                    await pipeline.run_sync(queues=["converted"], limit=0)

                    # Check that target directory was printed in startup configuration
                    print_calls = [str(call) for call in mock_print.call_args_list]
                    target_dir_printed = any("/valid/path" in call for call in print_calls)
                    assert target_dir_printed, "Target directory should be displayed in startup configuration"

            # Test case 2: None storage config (edge case)
            pipeline.storage_config = None

            with patch("grin_to_s3.processing.get_converted_books", return_value=set()):
                with patch("builtins.print") as mock_print:
                    await pipeline.run_sync(queues=["converted"], limit=0)

                    # Should handle None gracefully by showing "None"
                    print_calls = [str(call) for call in mock_print.call_args_list]
                    none_printed = any("None" in call for call in print_calls)
                    assert none_printed, "Should display 'None' for missing storage config"

    @pytest.mark.asyncio
    async def test_staging_manager_none_handling(self, mock_process_stage, test_config_builder):
        """Test that local storage properly handles None staging_manager."""
        with tempfile.TemporaryDirectory() as temp_dir:
            db_path = Path(temp_dir) / "test.db"

            # Initialize database
            tracker = SQLiteProgressTracker(str(db_path))
            await tracker.init_db()
            await tracker.close()

            config = test_config_builder.with_db_path(str(db_path)).local_storage(temp_dir).build()

            pipeline = SyncPipeline.from_run_config(
                config=config,
                process_summary_stage=mock_process_stage,
            )

            # Verify staging_manager is None for local storage
            assert pipeline.staging_manager is None, "Local storage should not have staging manager"

            # Verify staging directory doesn't exist (since it's not created for local storage)
            staging_dir = pipeline.staging_dir
            assert not staging_dir.exists(), "Staging directory should not be created for local storage"
