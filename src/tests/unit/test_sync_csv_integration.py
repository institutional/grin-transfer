"""
Unit tests for CSV export integration into sync pipeline.

Tests the CLI flag parsing and basic pipeline integration for CSV export.
"""

from unittest.mock import patch

import pytest

from grin_to_s3.sync.pipeline import SyncPipeline


class TestCSVExportIntegration:
    """Test suite for CSV export integration in sync pipeline."""

    def test_skip_csv_export_flag_default(self):
        """Test that skip_csv_export defaults to False."""
        pipeline = SyncPipeline(
            db_path="/test/db.sqlite",
            storage_type="local",
            storage_config={"base_path": "/test"},
            library_directory="test_lib",
        )
        assert pipeline.skip_csv_export is False

    def test_skip_csv_export_flag_enabled(self):
        """Test that skip_csv_export can be set to True."""
        pipeline = SyncPipeline(
            db_path="/test/db.sqlite",
            storage_type="local",
            storage_config={"base_path": "/test"},
            library_directory="test_lib",
            skip_csv_export=True,
        )
        assert pipeline.skip_csv_export is True

    @pytest.mark.asyncio
    async def test_csv_export_skipped_when_flag_set(self):
        """Test that CSV export is skipped when flag is set."""
        pipeline = SyncPipeline(
            db_path="/test/db.sqlite",
            storage_type="local",
            storage_config={"base_path": "/test"},
            library_directory="test_lib",
            skip_csv_export=True,
        )

        result = await pipeline._export_csv_if_enabled()
        assert result["status"] == "skipped"
        assert result["file_size"] == 0
        assert result["num_rows"] == 0
        assert result["export_time"] == 0.0

    @pytest.mark.asyncio
    async def test_csv_export_success(self):
        """Test successful CSV export when enabled and books were synced."""
        import tempfile

        with tempfile.TemporaryDirectory() as temp_dir:
            pipeline = SyncPipeline(
                db_path=f"{temp_dir}/db.sqlite",
                storage_type="r2",  # Use non-local storage to test staging path
                storage_config={"bucket_meta": "test-meta"},
                library_directory="test_lib",
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
                with patch("grin_to_s3.storage.create_storage_from_config"):
                    with patch("grin_to_s3.storage.book_storage.BookStorage"):
                        result = await pipeline._export_csv_if_enabled()

                        assert result["status"] == "completed"
                        assert result["num_rows"] == 100
                        assert result["file_size"] == 5000
                        assert "export_time" in result

                        # Verify CSV export was called
                        mock_export.assert_called_once()

    @pytest.mark.asyncio
    async def test_csv_export_error_handling(self):
        """Test that CSV export errors are handled properly."""
        import tempfile

        with tempfile.TemporaryDirectory() as temp_dir:
            pipeline = SyncPipeline(
                db_path=f"{temp_dir}/db.sqlite",
                storage_type="r2",  # Use non-local storage to test staging path
                storage_config={"bucket_meta": "test-meta"},
                library_directory="test_lib",
                skip_csv_export=False,
            )

            # Mock the CSV export function to raise an exception
            with patch("grin_to_s3.sync.pipeline.export_and_upload_csv") as mock_export:
                mock_export.side_effect = Exception("Export failed")

                # Mock storage creation
                with patch("grin_to_s3.storage.create_storage_from_config"):
                    with patch("grin_to_s3.storage.book_storage.BookStorage"):
                        result = await pipeline._export_csv_if_enabled()

                        assert result["status"] == "failed"
                        assert result["file_size"] == 0
                        assert result["num_rows"] == 0
                        assert result["export_time"] == 0.0

