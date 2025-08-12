#!/usr/bin/env python3
"""
Integration tests for block storage sync pipeline
"""

import tempfile
from pathlib import Path
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from grin_to_s3.collect_books.models import SQLiteProgressTracker
from grin_to_s3.common import SlidingWindowRateCalculator
from grin_to_s3.sync.pipeline import SyncPipeline


class TestBlockStorageSyncIntegration:
    """Integration tests for block storage sync pipeline interface compatibility."""

    @pytest.mark.asyncio
    async def test_rate_calculator_method_compatibility(self, mock_process_stage, test_config_builder):
        """Test that SlidingWindowRateCalculator interface is used correctly in sync pipeline."""
        with tempfile.TemporaryDirectory() as temp_dir:
            db_path = Path(temp_dir) / "test.db"
            staging_dir = Path(temp_dir) / "staging"
            staging_dir.mkdir()

            tracker = SQLiteProgressTracker(str(db_path))
            await tracker.init_db()
            await tracker.close()

            config = (
                test_config_builder.with_db_path(str(db_path))
                .s3_storage(access_key="test", secret_key="test", bucket_raw="test")
                .with_concurrent_downloads(1)
                .with_staging_dir(str(staging_dir))
                .build()
            )

            pipeline = SyncPipeline.from_run_config(
                config=config,
                process_summary_stage=mock_process_stage,
            )

            # Use REAL SlidingWindowRateCalculator to catch method name issues
            real_calc = SlidingWindowRateCalculator(window_size=5)

            # Mock minimal dependencies but keep rate calculator real
            pipeline.db_tracker.get_books_for_sync = AsyncMock(return_value=["TEST123"])
            pipeline.grin_client = MagicMock()
            pipeline.staging_manager = MagicMock()
            pipeline.staging_manager.wait_for_disk_space = AsyncMock()

            # Mock the pipeline processing methods but use real rate calculator
            with (
                patch.object(pipeline, "_download_book", new=AsyncMock()) as mock_download,
                patch.object(pipeline, "_upload_book_from_staging", new=AsyncMock()) as mock_upload,
                patch("grin_to_s3.sync.pipeline.SlidingWindowRateCalculator", return_value=real_calc),
            ):
                mock_download.return_value = {
                    "barcode": "TEST123",
                    "download_success": True,
                    "staging_file_path": "/tmp/staging/TEST123.tar.gz",
                    "encrypted_etag": "test-etag",
                    "metadata": {"size": 1000},
                }
                mock_upload.return_value = {"barcode": "TEST123", "upload_success": True, "result": {"success": True}}

                # This should work without errors if method names are correct
                try:
                    await pipeline._run_sync(["TEST123"], 1)
                finally:
                    # Ensure pipeline is properly shut down
                    await pipeline.cleanup()

                # Verify batch was added to rate calculator (called once: download completion)
                assert len(real_calc.batch_times) == 1
                assert real_calc.batch_times[0][1] == 1  # Download completion (processed_count=1)

    @pytest.mark.asyncio
    async def test_rate_calculator_interface_validation(self):
        """Test that SlidingWindowRateCalculator has the expected interface."""
        calc = SlidingWindowRateCalculator()

        # Test expected interface methods exist and work

        # Test correct interface methods
        calc.add_batch(12345.0, 10)  # timestamp, processed_count
        rate = calc.get_rate(12344.0, 10)  # fallback_start_time, fallback_processed_count
        assert isinstance(rate, int | float)

    @pytest.mark.asyncio
    async def test_staging_vs_local_sync_selection(self, mock_process_stage, test_config_builder):
        """Test that storage type determines sync method selection."""
        with tempfile.TemporaryDirectory() as temp_dir:
            db_path = Path(temp_dir) / "test.db"

            tracker = SQLiteProgressTracker(str(db_path))
            await tracker.init_db()
            await tracker.close()

            # Test that cloud storage creates staging manager
            cloud_config = (
                test_config_builder.with_db_path(str(db_path))
                .s3_storage(access_key="test", secret_key="test", bucket_raw="test")
                .build()
            )

            cloud_pipeline = SyncPipeline.from_run_config(
                config=cloud_config,
                process_summary_stage=mock_process_stage,
            )

            # Test that local storage doesn't create staging manager
            local_config = test_config_builder.with_db_path(str(db_path)).local_storage(temp_dir).build()

            local_pipeline = SyncPipeline.from_run_config(
                config=local_config,
                process_summary_stage=mock_process_stage,
            )

            # Verify correct staging manager setup
            assert cloud_pipeline.staging_manager is not None
            assert local_pipeline.staging_manager is None

    @pytest.mark.asyncio
    async def test_concurrent_semaphore_limits(self, mock_process_stage, test_config_builder):
        """Test that concurrent limits are respected in staging sync."""
        with tempfile.TemporaryDirectory() as temp_dir:
            db_path = Path(temp_dir) / "test.db"
            staging_dir = Path(temp_dir) / "staging"
            staging_dir.mkdir()

            tracker = SQLiteProgressTracker(str(db_path))
            await tracker.init_db()
            await tracker.close()

            # Create pipeline with specific concurrency limits
            config = (
                test_config_builder.with_db_path(str(db_path))
                .s3_storage(access_key="test", secret_key="test", bucket_raw="test")
                .with_concurrent_downloads(2)
                .with_concurrent_uploads(3)
                .with_staging_dir(str(staging_dir))
                .build()
            )

            pipeline = SyncPipeline.from_run_config(
                config=config,
                process_summary_stage=mock_process_stage,
            )

            # Verify semaphore limits are set correctly
            assert pipeline._download_semaphore._value == 2
            assert pipeline._upload_semaphore._value == 3

    @pytest.mark.asyncio
    async def test_pipeline_cleanup_and_shutdown(self, mock_process_stage, test_config_builder):
        """Test pipeline cleanup and shutdown behavior."""
        with tempfile.TemporaryDirectory() as temp_dir:
            db_path = Path(temp_dir) / "test.db"
            staging_dir = Path(temp_dir) / "staging"
            staging_dir.mkdir()

            tracker = SQLiteProgressTracker(str(db_path))
            await tracker.init_db()
            await tracker.close()

            config = (
                test_config_builder.with_db_path(str(db_path))
                .s3_storage(access_key="test", secret_key="test", bucket_raw="test")
                .with_staging_dir(str(staging_dir))
                .build()
            )

            pipeline = SyncPipeline.from_run_config(
                config=config,
                process_summary_stage=mock_process_stage,
            )

            # Test cleanup doesn't error
            await pipeline.cleanup()

            # Verify shutdown flag is set
            assert pipeline._shutdown_requested is True

    @pytest.mark.asyncio
    async def test_statistics_tracking(self, mock_process_stage, test_config_builder):
        """Test that statistics are tracked correctly during staging sync."""
        with tempfile.TemporaryDirectory() as temp_dir:
            db_path = Path(temp_dir) / "test.db"
            staging_dir = Path(temp_dir) / "staging"
            staging_dir.mkdir()

            tracker = SQLiteProgressTracker(str(db_path))
            await tracker.init_db()
            await tracker.close()

            config = (
                test_config_builder.with_db_path(str(db_path))
                .s3_storage(access_key="test", secret_key="test", bucket_raw="test")
                .with_concurrent_downloads(1)
                .with_staging_dir(str(staging_dir))
                .build()
            )

            pipeline = SyncPipeline.from_run_config(
                config=config,
                process_summary_stage=mock_process_stage,
            )

            # Mock dependencies
            pipeline.db_tracker.get_books_for_sync = AsyncMock(return_value=["TEST1", "TEST2"])
            pipeline.grin_client = MagicMock()
            pipeline.staging_manager = MagicMock()
            pipeline.staging_manager.wait_for_disk_space = AsyncMock()

            # Mock mixed success/failure scenario
            results = [
                {"barcode": "TEST1", "download_success": True, "staging_file_path": "/tmp/test1.tar.gz"},
                {"barcode": "TEST2", "download_success": False, "error": "Download failed"},
            ]
            upload_results = [{"barcode": "TEST1", "upload_success": True, "result": {"success": True}}]

            with (
                patch.object(pipeline, "_download_book", new=AsyncMock(side_effect=results)),
                patch.object(pipeline, "_upload_book_from_staging", new=AsyncMock(side_effect=upload_results)),
            ):
                try:
                    await pipeline._run_sync(["TEST1", "TEST2"], 2)
                finally:
                    # Ensure pipeline is properly shut down
                    await pipeline.cleanup()

                # Verify statistics are correct
                assert pipeline.stats["synced"] == 1  # TEST1 succeeded
                assert pipeline.stats["failed"] == 1  # TEST2 failed
                assert pipeline.stats["skipped"] == 0
