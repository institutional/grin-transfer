"""Tests for sync pipeline concurrency control."""

import asyncio
import tempfile
from pathlib import Path
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from grin_to_s3.sync.pipeline import SyncPipeline


class TestSyncPipelineConcurrency:
    """Test concurrency control in sync pipeline."""

    @pytest.fixture
    def mock_run_config(self, test_config_builder):
        """Create a mock RunConfig for testing."""
        return (test_config_builder
                .with_library_directory("TestLib")
                .minio_storage(bucket_raw="test-raw")
                .with_concurrent_downloads(2)  # Low limit for testing
                .with_concurrent_uploads(1)
                .with_batch_size(10)
                .build())

    @pytest.fixture
    def mock_pipeline_dependencies(self):
        """Mock all pipeline dependencies."""
        with tempfile.TemporaryDirectory() as temp_dir:
            with (
                patch("grin_to_s3.sync.pipeline.SQLiteProgressTracker") as mock_tracker,
                patch("grin_to_s3.sync.pipeline.ProgressReporter") as mock_reporter,
                patch("grin_to_s3.sync.pipeline.GRINClient") as mock_client,
                patch("grin_to_s3.storage.StagingDirectoryManager") as mock_staging,
            ):
                # Configure mocks
                mock_tracker.return_value = MagicMock()
                mock_reporter.return_value = MagicMock()
                mock_client.return_value = MagicMock()

                # Configure staging manager mock with real temporary directory
                staging_manager_mock = MagicMock()
                staging_manager_mock.staging_path = Path(temp_dir)
                staging_manager_mock.get_staging_path = MagicMock(return_value=Path(temp_dir) / "test_file")
                staging_manager_mock.cleanup_file = AsyncMock()
                staging_manager_mock.check_and_wait_for_space = AsyncMock()
                mock_staging.return_value = staging_manager_mock

                yield {
                    "tracker": mock_tracker.return_value,
                    "reporter": mock_reporter.return_value,
                    "client": mock_client.return_value,
                    "staging": staging_manager_mock,
                }

    @pytest.fixture
    def pipeline(self, mock_pipeline_dependencies, mock_process_stage, mock_run_config):
        """Create a test pipeline with low concurrency limits."""
        return SyncPipeline.from_run_config(
            config=mock_run_config,
            process_summary_stage=mock_process_stage,
        )

    async def test_download_concurrency_limit_respected(self, pipeline, mock_pipeline_dependencies):
        """Test that download concurrency limit is respected."""
        download_started = []
        download_completed = []
        max_concurrent = 0
        current_concurrent = 0

        # Track actual concurrent downloads
        async def mock_download_with_delay(*args, **kwargs):
            nonlocal current_concurrent, max_concurrent
            current_concurrent += 1
            max_concurrent = max(max_concurrent, current_concurrent)
            download_started.append(args[0] if args else "unknown")

            # Simulate download time
            await asyncio.sleep(0.1)

            current_concurrent -= 1
            download_completed.append(args[0] if args else "unknown")
            return {"barcode": args[0], "download_success": True, "staging_file_path": "/tmp/test"}

        # Mock the actual download operation
        with (
            patch("grin_to_s3.sync.pipeline.download_book_to_staging", side_effect=mock_download_with_delay),
            patch("grin_to_s3.sync.pipeline.check_and_handle_etag_skip", return_value=(None, "etag123", 1000)),
            patch("grin_to_s3.sync.pipeline.upload_book_from_staging", return_value={"success": True}),
        ):
            # Mock get_converted_books to return test books
            with patch(
                "grin_to_s3.sync.pipeline.get_converted_books",
                return_value={"book1", "book2", "book3", "book4", "book5"},
            ):
                # Mock database methods
                mock_pipeline_dependencies["tracker"].get_books_for_sync = AsyncMock(
                    return_value=["book1", "book2", "book3", "book4", "book5"]
                )
                mock_pipeline_dependencies["tracker"].get_sync_stats = AsyncMock(
                    return_value={"total_converted": 5, "synced": 0, "failed": 0, "pending": 5}
                )
                mock_pipeline_dependencies["tracker"].add_status_change = AsyncMock()

                # Run sync with limit
                await pipeline.run_sync(limit=5)

        # Verify concurrency was respected
        assert max_concurrent <= pipeline.concurrent_downloads, (
            f"Max concurrent downloads ({max_concurrent}) exceeded limit ({pipeline.concurrent_downloads})"
        )
        assert len(download_started) >= 5, f"Expected at least 5 downloads, got {len(download_started)}"
        assert len(download_completed) >= 5, f"Expected at least 5 completed downloads, got {len(download_completed)}"

    async def test_progress_reporting_accuracy(self, pipeline, mock_pipeline_dependencies):
        """Test that progress reporting shows accurate task counts."""
        progress_reports = []

        # Mock progress reporting to capture data
        original_maybe_show_progress = pipeline._maybe_show_progress

        def mock_progress_reporter(*args, **kwargs):
            # Capture the active counts from the progress report
            if len(args) >= 15:  # Check we have enough args
                active_downloads = kwargs.get("active_downloads", {})
                active_uploads = kwargs.get("active_uploads", {})
                progress_reports.append(
                    {
                        "active_downloads": len(active_downloads) if active_downloads else 0,
                        "active_uploads": len(active_uploads) if active_uploads else 0,
                        "reported_download_count": pipeline._active_download_count,
                        "reported_upload_count": pipeline._active_upload_count,
                    }
                )
            return original_maybe_show_progress(*args, **kwargs)

        pipeline._maybe_show_progress = mock_progress_reporter

        # Mock download/upload operations
        with (
            patch("grin_to_s3.sync.pipeline.download_book_to_staging", return_value=("book", "/tmp/test", {})),
            patch("grin_to_s3.sync.pipeline.check_and_handle_etag_skip", return_value=(None, "etag123", 1000)),
            patch("grin_to_s3.sync.pipeline.upload_book_from_staging", return_value={"success": True}),
        ):
            # Mock get_converted_books
            with patch("grin_to_s3.sync.pipeline.get_converted_books", return_value={"book1", "book2"}):
                # Mock database methods
                mock_pipeline_dependencies["tracker"].get_books_for_sync = AsyncMock(return_value=["book1", "book2"])
                mock_pipeline_dependencies["tracker"].get_sync_stats = AsyncMock(
                    return_value={"total_converted": 2, "synced": 0, "failed": 0, "pending": 2}
                )
                mock_pipeline_dependencies["tracker"].add_status_change = AsyncMock()

                # Run sync
                await pipeline.run_sync(limit=2)

        # Verify progress reports had consistent data
        for report in progress_reports:
            assert report["reported_download_count"] <= pipeline.concurrent_downloads, (
                f"Reported download count ({report['reported_download_count']}) exceeded limit"
            )
            assert report["reported_upload_count"] <= pipeline.concurrent_uploads, (
                f"Reported upload count ({report['reported_upload_count']}) exceeded limit"
            )

    async def test_semaphore_acquisition_order(self, pipeline):
        """Test that semaphore is properly acquired and released."""
        acquisition_order = []
        release_order = []

        # Track semaphore acquisition
        original_acquire = pipeline._download_semaphore.acquire
        original_release = pipeline._download_semaphore.release

        async def track_acquire():
            result = await original_acquire()
            acquisition_order.append(len(acquisition_order))
            return result

        def track_release():
            release_order.append(len(release_order))
            return original_release()

        pipeline._download_semaphore.acquire = track_acquire
        pipeline._download_semaphore.release = track_release

        # Test semaphore with multiple tasks
        tasks = []
        for i in range(5):  # More than concurrency limit
            task = asyncio.create_task(pipeline._process_book_with_staging(f"book{i}"))
            tasks.append(task)

        # Mock the actual operations to avoid real network calls
        with (
            patch("grin_to_s3.sync.pipeline.check_and_handle_etag_skip", return_value=(None, "etag", 1000)),
            patch("grin_to_s3.sync.pipeline.download_book_to_staging", return_value=("book", "/tmp/test", {})),
        ):
            # Wait for all tasks
            await asyncio.gather(*tasks, return_exceptions=True)

        # Verify semaphore was respected
        assert len(acquisition_order) == 5, f"Expected 5 acquisitions, got {len(acquisition_order)}"
        assert len(release_order) == 5, f"Expected 5 releases, got {len(release_order)}"

        # At any point, concurrent acquisitions should not exceed limit
        for i in range(len(acquisition_order)):
            concurrent_at_point = len(list(acquisition_order[: i + 1])) - len(list(release_order[:i]))
            assert concurrent_at_point <= pipeline.concurrent_downloads, (
                f"Concurrent acquisitions ({concurrent_at_point}) exceeded limit at point {i}"
            )


class TestSyncPipelineEnrichmentQueue:
    """Test enrichment queue infrastructure in sync pipeline."""

    @pytest.fixture
    def mock_local_run_config(self, test_config_builder):
        """Create a mock RunConfig for local storage testing."""
        return (test_config_builder
                .with_library_directory("TestLib")
                .local_storage()
                .build())

    @pytest.fixture
    def mock_pipeline_dependencies(self):
        """Mock all pipeline dependencies."""
        with (
            patch("grin_to_s3.sync.pipeline.SQLiteProgressTracker") as mock_tracker,
            patch("grin_to_s3.sync.pipeline.ProgressReporter") as mock_reporter,
            patch("grin_to_s3.sync.pipeline.GRINClient") as mock_client,
        ):
            # Configure mocks
            mock_tracker.return_value = MagicMock()
            mock_reporter.return_value = MagicMock()
            mock_client.return_value = MagicMock()

            yield {
                "tracker": mock_tracker.return_value,
                "reporter": mock_reporter.return_value,
                "client": mock_client.return_value,
            }

    def test_enrichment_queue_enabled_by_default(self, mock_pipeline_dependencies, mock_process_stage, mock_local_run_config):
        """Test that enrichment queue is enabled by default."""
        pipeline = SyncPipeline.from_run_config(
            config=mock_local_run_config,
            process_summary_stage=mock_process_stage,
        )

        # Enrichment should be enabled by default
        assert pipeline.enrichment_enabled is True
        assert pipeline.enrichment_workers == 1
        assert pipeline.skip_csv_export is False

        # Queue should be initialized
        assert pipeline.enrichment_queue is not None
        assert pipeline.enrichment_queue.qsize() == 0

    def test_enrichment_queue_can_be_disabled(self, mock_pipeline_dependencies, mock_process_stage, mock_local_run_config):
        """Test that enrichment queue can be disabled."""
        pipeline = SyncPipeline.from_run_config(
            config=mock_local_run_config,
            process_summary_stage=mock_process_stage,
            skip_enrichment=True,
        )

        # Enrichment should be disabled
        assert pipeline.enrichment_enabled is False

        # Queue should be None when disabled
        assert pipeline.enrichment_queue is None

    def test_enrichment_configuration_options(self, mock_pipeline_dependencies, mock_process_stage, mock_local_run_config):
        """Test enrichment configuration options."""
        # Modify the config to test custom enrichment workers
        mock_local_run_config.config_dict["sync_config"]["enrichment_workers"] = 3

        pipeline = SyncPipeline.from_run_config(
            config=mock_local_run_config,
            process_summary_stage=mock_process_stage,
            skip_csv_export=True,
        )

        # Configuration should be set correctly
        assert pipeline.enrichment_enabled is True
        assert pipeline.enrichment_workers == 3
        assert pipeline.skip_csv_export is True

        # Queue should still be initialized
        assert pipeline.enrichment_queue is not None

    async def test_enrichment_queue_size_in_stats(self, mock_pipeline_dependencies, mock_process_stage, mock_local_run_config):
        """Test that enrichment queue size is tracked in stats."""
        pipeline = SyncPipeline.from_run_config(
            config=mock_local_run_config,
            process_summary_stage=mock_process_stage,
        )

        # Mock database stats
        mock_tracker = mock_pipeline_dependencies["tracker"]
        mock_tracker.get_sync_stats = AsyncMock(return_value={"total_books": 100})

        # Initial stats should show queue size 0
        status = await pipeline.get_sync_status()
        assert status["session_stats"]["enrichment_queue_size"] == 0

        # Add items to queue
        await pipeline.enrichment_queue.put("barcode1")
        await pipeline.enrichment_queue.put("barcode2")

        # Stats should reflect queue size
        status = await pipeline.get_sync_status()
        assert status["session_stats"]["enrichment_queue_size"] == 2

    async def test_enrichment_queue_size_zero_when_disabled(self, mock_pipeline_dependencies, mock_process_stage, mock_local_run_config):
        """Test that enrichment queue size is 0 when enrichment is disabled."""
        pipeline = SyncPipeline.from_run_config(
            config=mock_local_run_config,
            process_summary_stage=mock_process_stage,
            skip_enrichment=True,
        )

        # Mock database stats
        mock_tracker = mock_pipeline_dependencies["tracker"]
        mock_tracker.get_sync_stats = AsyncMock(return_value={"total_books": 100})

        # Stats should show queue size 0 when disabled
        status = await pipeline.get_sync_status()
        assert status["session_stats"]["enrichment_queue_size"] == 0
