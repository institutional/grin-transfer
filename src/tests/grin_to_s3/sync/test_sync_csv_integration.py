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
                .r2_storage(bucket_meta="test-meta")
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
                        assert result["num_rows"] == 100
                        assert result["file_size"] == 5000
                        assert "export_time" in result

                        # Verify CSV export was called
                        mock_export.assert_called_once()

    @pytest.mark.asyncio
    async def test_csv_export_error_handling(self, mock_process_stage, test_config_builder):
        """Test that CSV export errors are handled properly."""
        with tempfile.TemporaryDirectory() as temp_dir:
            config = (
                test_config_builder.with_db_path(f"{temp_dir}/db.sqlite")
                .with_library_directory("test_lib")
                .r2_storage(bucket_meta="test-meta")
                .build()
            )

            pipeline = SyncPipeline.from_run_config(
                config=config,
                process_summary_stage=mock_process_stage,
                skip_csv_export=False,
            )

            # Mock the CSV export function to raise an exception
            with patch("grin_to_s3.sync.pipeline.export_and_upload_csv") as mock_export:
                mock_export.side_effect = Exception("Export failed")

                # Mock storage creation
                with patch("grin_to_s3.sync.pipeline.create_storage_from_config"):
                    with patch("grin_to_s3.sync.pipeline.BookManager"):
                        result = await pipeline._export_csv_if_enabled()

                        assert result["status"] == "failed"
                        assert result["file_size"] == 0
                        assert result["num_rows"] == 0
                        assert result["export_time"] == 0.0
                        assert result["file_size"] == 0
                        assert result["num_rows"] == 0
                        assert result["export_time"] == 0.0
