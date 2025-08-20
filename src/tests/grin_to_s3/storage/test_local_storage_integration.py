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
                assert "total_converted" in status
                assert "storage_type" in status

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
            valid_path = Path(temp_dir) / "valid_path"
            valid_path.mkdir(parents=True, exist_ok=True)
            config = test_config_builder.with_db_path(str(db_path)).local_storage(str(valid_path)).build()

            pipeline = SyncPipeline.from_run_config(
                config=config,
                process_summary_stage=mock_process_stage,
            )

            # Mock dependencies for setup_sync_loop startup
            pipeline.db_tracker.get_books_for_sync = AsyncMock(return_value=[])

            # Mock get_converted_books to return empty list so sync exits early
            with patch("grin_to_s3.processing.get_converted_books", return_value=set()):
                # Capture print output to verify storage configuration display
                with patch("builtins.print") as mock_print:
                    await pipeline.setup_sync_loop(queues=["converted"], limit=0)

                    # Check that target directory was printed in startup configuration
                    print_calls = [str(call) for call in mock_print.call_args_list]
                    target_dir_printed = any("valid_path" in call for call in print_calls)
                    assert target_dir_printed, "Target directory should be displayed in startup configuration"

            # Test case 2: Minimal storage config (no base_path)
            # Remove base_path from the existing pipeline's config to simulate missing base_path
            pipeline.config.storage_config["config"] = {}  # Clear the config to remove base_path

            with patch("grin_to_s3.processing.get_converted_books", return_value=set()):
                with patch("builtins.print") as mock_print:
                    await pipeline.setup_sync_loop(queues=["converted"], limit=0)

                    # Should handle missing base_path gracefully by showing "None"
                    print_calls = [str(call) for call in mock_print.call_args_list]
                    none_printed = any("None" in call for call in print_calls)
                    assert none_printed, f"Should display 'None' for missing base_path. Print calls: {print_calls}"

    @pytest.mark.asyncio
    async def test_staging_manager_local_handling(self, mock_process_stage, test_config_builder):
        """Test that local storage properly creates LocalDirectoryManager."""
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

            # Verify filesystem_manager is LocalDirectoryManager for local storage
            from grin_to_s3.storage.staging import LocalDirectoryManager

            assert isinstance(pipeline.filesystem_manager, LocalDirectoryManager), (
                "Local storage should have LocalDirectoryManager"
            )

            # Verify staging directory path is configured correctly
            staging_dir = pipeline.staging_dir
            assert staging_dir.parent == Path(db_path).parent, "Staging directory should be under run directory"
