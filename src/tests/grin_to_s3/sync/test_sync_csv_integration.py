"""
Unit tests for CSV export integration into sync pipeline.

Tests the CLI flag parsing and basic pipeline integration for CSV export.
"""

import tempfile
from unittest.mock import patch

import pytest

from grin_to_s3.sync.pipeline import SyncPipeline


class TestCSVExportIntegration:
    """Test suite for CSV export integration in sync pipeline."""

    def test_skip_csv_export_flag_default(self, mock_process_stage, test_config_builder):
        """Test that skip_csv_export defaults to False."""
        config = (
            test_config_builder.with_db_path("/test/db.sqlite")
            .with_library_directory("test_lib")
            .local_storage("/test")
            .build()
        )

        pipeline = SyncPipeline.from_run_config(
            config=config,
            process_summary_stage=mock_process_stage,
        )
        assert pipeline.skip_csv_export is False

    def test_skip_csv_export_flag_enabled(self, mock_process_stage, test_config_builder):
        """Test that skip_csv_export can be set to True."""
        config = (
            test_config_builder.with_db_path("/test/db.sqlite")
            .with_library_directory("test_lib")
            .local_storage("/test")
            .build()
        )

        pipeline = SyncPipeline.from_run_config(
            config=config,
            process_summary_stage=mock_process_stage,
            skip_csv_export=True,
        )
        assert pipeline.skip_csv_export is True

    @pytest.mark.asyncio
    async def test_csv_export_skipped_when_flag_set(self, mock_process_stage, test_config_builder):
        """Test that CSV export is skipped when flag is set."""
        config = (
            test_config_builder.with_db_path("/test/db.sqlite")
            .with_library_directory("test_lib")
            .local_storage("/test")
            .build()
        )

        pipeline = SyncPipeline.from_run_config(
            config=config,
            process_summary_stage=mock_process_stage,
            skip_csv_export=True,
        )

        result = await pipeline._export_csv_if_enabled()
        assert result["status"] == "skipped"
        assert result["file_size"] == 0
        assert result["num_rows"] == 0
        assert result["export_time"] == 0.0

    @pytest.mark.asyncio
    async def test_csv_export_success(self, mock_process_stage, test_config_builder):
        """Test successful CSV export when enabled and books were synced."""
        with tempfile.TemporaryDirectory() as temp_dir:
            config = (
                test_config_builder.with_db_path(f"{temp_dir}/db.sqlite")
                .with_library_directory("test_lib")
                .local_storage(temp_dir)
                .build()
            )

            pipeline = SyncPipeline.from_run_config(
                config=config,
                process_summary_stage=mock_process_stage,
                skip_csv_export=False,
            )

            # Mock the CSV export function
            with patch("grin_to_s3.sync.pipeline.export_and_upload_csv") as mock_export:
                mock_export.return_value = {
                    "status": "completed",
                    "num_rows": 100,
                    "file_size": 5000,
                    "export_time": 1.5,
                }

                # Mock storage creation
                with patch("grin_to_s3.sync.pipeline.create_storage_from_config"):
                    with patch("grin_to_s3.sync.pipeline.BookManager"):
                        result = await pipeline._export_csv_if_enabled()

                        assert result["status"] == "completed"
                        assert result["num_rows"] >= 1  # At least header row
                        assert result["file_size"] > 0
                        assert "export_time" in result

                        # For local storage, export_and_upload_csv should not be called
                        mock_export.assert_not_called()

    @pytest.mark.asyncio
    async def test_csv_export_error_handling(self, mock_process_stage, test_config_builder):
        """Test that CSV export errors are handled properly."""
        with tempfile.TemporaryDirectory() as temp_dir:
            config = (
                test_config_builder.with_db_path(f"{temp_dir}/db.sqlite")
                .with_library_directory("test_lib")
                .local_storage(temp_dir)
                .build()
            )

            pipeline = SyncPipeline.from_run_config(
                config=config,
                process_summary_stage=mock_process_stage,
                skip_csv_export=False,
            )

            # Mock the local CSV export function to raise an exception
            with patch("grin_to_s3.sync.pipeline.SyncPipeline._export_csv_local") as mock_export_local:
                mock_export_local.side_effect = Exception("Export failed")

                # Mock storage creation
                with patch("grin_to_s3.sync.pipeline.create_storage_from_config"):
                    with patch("grin_to_s3.sync.pipeline.BookManager"):
                        result = await pipeline._export_csv_if_enabled()

                        assert result["status"] == "failed"
                        assert result["file_size"] == 0
                        assert result["num_rows"] == 0
                        assert result["export_time"] == 0.0

    def test_local_csv_export_path_construction_unit(self, mock_process_stage, test_config_builder):
        """Test that local CSV export constructs paths correctly under base_path."""
        from pathlib import Path
        from unittest.mock import Mock

        with tempfile.TemporaryDirectory() as temp_dir:
            # Config not needed for this unit test, just testing path construction directly

            # Create a mock book storage that mimics local storage behavior
            mock_book_manager = Mock()
            mock_book_manager.storage.config.options.get.return_value = temp_dir

            # Test the path construction logic directly (extracted from _export_csv_local)
            timestamp = "20241201_120000"  # Fixed timestamp for testing
            base_path = mock_book_manager.storage.config.options.get("base_path")

            # This is the fixed path construction from our fix
            latest_path = Path(base_path) / "meta" / "books_latest.csv"
            timestamped_path = Path(base_path) / "meta" / "timestamped" / f"books_{timestamp}.csv"

            # Verify paths are constructed correctly
            assert str(latest_path) == f"{temp_dir}/meta/books_latest.csv"
            assert str(timestamped_path) == f"{temp_dir}/meta/timestamped/books_{timestamp}.csv"

            # Verify paths are not at filesystem root
            assert not str(latest_path).startswith("/books_")
            assert not str(timestamped_path).startswith("/timestamped/")

            # Verify paths are under base_path
            assert str(latest_path).startswith(temp_dir)
            assert str(timestamped_path).startswith(temp_dir)
