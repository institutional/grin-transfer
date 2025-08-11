"""Tests for sync pipeline concurrency control."""

import asyncio
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from grin_to_s3.sync.pipeline import SyncPipeline


class TestSyncPipelineConcurrency:
    """Test concurrency control in sync pipeline."""


    async def test_download_concurrency_limit_respected(self, sync_pipeline):
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

        # Set concurrency limit for test
        sync_pipeline.concurrent_downloads = 2

        # Mock the actual download operation
        with (
            patch("grin_to_s3.sync.pipeline.download_book_to_staging", side_effect=mock_download_with_delay),
            patch("grin_to_s3.sync.pipeline.check_and_handle_etag_skip", return_value=(None, "etag123", 1000, [])),
            patch("grin_to_s3.sync.pipeline.upload_book_from_staging", return_value={"success": True}),
        ):
            # Mock get_converted_books to return test books
            with patch(
                "grin_to_s3.processing.get_converted_books",
                return_value={"book1", "book2", "book3", "book4", "book5"},
            ):
                # Mock database methods
                sync_pipeline.db_tracker.get_books_for_sync = AsyncMock(
                    return_value=["book1", "book2", "book3", "book4", "book5"]
                )
                sync_pipeline.db_tracker.get_sync_stats = AsyncMock(
                    return_value={"total_converted": 5, "synced": 0, "failed": 0, "pending": 5}
                )
                # Mock batch_write_status_updates instead of add_status_change
                with patch("grin_to_s3.database_utils.batch_write_status_updates", new_callable=AsyncMock) as mock_batch_write:
                    mock_batch_write.return_value = None

                    # Run sync with limit
                    await sync_pipeline.run_sync(queues=["converted"], limit=5)

        # Verify concurrency was respected
        assert max_concurrent <= sync_pipeline.concurrent_downloads, (
            f"Max concurrent downloads ({max_concurrent}) exceeded limit ({sync_pipeline.concurrent_downloads})"
        )
        assert len(download_started) >= 5, f"Expected at least 5 downloads, got {len(download_started)}"
        assert len(download_completed) >= 5, f"Expected at least 5 completed downloads, got {len(download_completed)}"

    @pytest.mark.skip(reason="Progress reporting implementation changed to background task")
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
            patch("grin_to_s3.sync.pipeline.check_and_handle_etag_skip", return_value=(None, "etag123", 1000, [])),
            patch("grin_to_s3.sync.pipeline.upload_book_from_staging", return_value={"success": True}),
        ):
            # Mock get_converted_books
            with patch("grin_to_s3.processing.get_converted_books", return_value={"book1", "book2"}):
                # Mock database methods
                mock_pipeline_dependencies["tracker"].get_books_for_sync = AsyncMock(return_value=["book1", "book2"])
                mock_pipeline_dependencies["tracker"].get_sync_stats = AsyncMock(
                    return_value={"total_converted": 2, "synced": 0, "failed": 0, "pending": 2}
                )
                # Mock batch_write_status_updates instead of add_status_change
                with patch("grin_to_s3.database_utils.batch_write_status_updates", new_callable=AsyncMock) as mock_batch_write:
                    mock_batch_write.return_value = None

                    # Run sync
                    await pipeline.run_sync(queues=["converted"], limit=2)

        # Verify progress reports had consistent data
        for report in progress_reports:
            assert report["reported_download_count"] <= pipeline.concurrent_downloads, (
                f"Reported download count ({report['reported_download_count']}) exceeded limit"
            )
            assert report["reported_upload_count"] <= pipeline.concurrent_uploads, (
                f"Reported upload count ({report['reported_upload_count']}) exceeded limit"
            )

    async def test_semaphore_acquisition_order(self, sync_pipeline):
        """Test that semaphore is properly acquired and released."""
        acquisition_order = []
        release_order = []

        # Track semaphore acquisition
        original_acquire = sync_pipeline._download_semaphore.acquire
        original_release = sync_pipeline._download_semaphore.release

        async def track_acquire():
            result = await original_acquire()
            acquisition_order.append(len(acquisition_order))
            return result

        def track_release():
            release_order.append(len(release_order))
            return original_release()

        sync_pipeline._download_semaphore.acquire = track_acquire
        sync_pipeline._download_semaphore.release = track_release

        # Test semaphore with multiple tasks
        tasks = []
        for i in range(5):  # More than concurrency limit
            task = asyncio.create_task(sync_pipeline._process_book_with_staging(f"book{i}"))
            tasks.append(task)

        # Mock the actual operations to avoid real network calls
        with (
            patch("grin_to_s3.sync.pipeline.check_and_handle_etag_skip", return_value=(None, "etag", 1000, [])),
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
            assert concurrent_at_point <= sync_pipeline.concurrent_downloads, (
                f"Concurrent acquisitions ({concurrent_at_point}) exceeded limit at point {i}"
            )


class TestSyncPipelineEnrichmentQueue:
    """Test enrichment queue infrastructure in sync pipeline."""

    @pytest.fixture
    def mock_local_run_config(self, test_config_builder):
        """Create a mock RunConfig for local storage testing."""
        return test_config_builder.with_library_directory("TestLib").local_storage().build()

    @pytest.fixture
    def mock_pipeline_dependencies(self):
        """Mock all pipeline dependencies."""
        with (
            patch("grin_to_s3.sync.pipeline.SQLiteProgressTracker") as mock_tracker,
            patch("grin_to_s3.sync.pipeline.GRINClient") as mock_client,
        ):
            # Configure mocks
            mock_tracker.return_value = MagicMock()
            mock_client.return_value = MagicMock()

            yield {
                "tracker": mock_tracker.return_value,
                "client": mock_client.return_value,
            }

    def test_enrichment_queue_enabled_by_default(
        self, mock_pipeline_dependencies, mock_process_stage, mock_local_run_config
    ):
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

    def test_enrichment_queue_can_be_disabled(
        self, mock_pipeline_dependencies, mock_process_stage, mock_local_run_config
    ):
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

    def test_enrichment_configuration_options(
        self, mock_pipeline_dependencies, mock_process_stage, mock_local_run_config
    ):
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

    async def test_enrichment_queue_size_in_stats(
        self, mock_pipeline_dependencies, mock_process_stage, mock_local_run_config
    ):
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

    async def test_enrichment_queue_size_zero_when_disabled(
        self, mock_pipeline_dependencies, mock_process_stage, mock_local_run_config
    ):
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


class TestDiskSpaceRaceConditionFix:
    """Test the disk space race condition fix."""

    @pytest.fixture
    def mock_run_config(self, test_config_builder):
        """Create a mock RunConfig for testing."""
        return (
            test_config_builder.with_library_directory("TestLib")
            .minio_storage(bucket_raw="test-raw")
            .with_concurrent_downloads(3)  # Multiple downloads to test race condition
            .with_batch_size(5)
            .build()
        )

    @pytest.mark.asyncio
    async def test_disk_space_checked_before_semaphore_acquisition(self, mock_run_config):
        """Test that disk space is checked BEFORE semaphore acquisition to prevent race condition."""
        # Track the order of operations
        operation_order = []

        with patch("grin_to_s3.sync.pipeline.SQLiteProgressTracker") as mock_tracker:
            with patch("grin_to_s3.sync.pipeline.GRINClient") as mock_client:
                # Configure mocks
                mock_tracker.return_value = MagicMock()
                mock_client.return_value = MagicMock()

                # Create pipeline
                mock_stage = MagicMock()
                pipeline = SyncPipeline.from_run_config(config=mock_run_config, process_summary_stage=mock_stage)

                # Mock staging manager with space checking
                space_calls = 0
                async def mock_wait_for_disk_space():
                    nonlocal space_calls
                    space_calls += 1
                    operation_order.append(f"space_check_{space_calls}")
                    await asyncio.sleep(0.01)  # Simulate brief wait

                pipeline.staging_manager.wait_for_disk_space = mock_wait_for_disk_space

                # Mock semaphore acquisition to track when it happens
                original_acquire = pipeline._download_semaphore.acquire
                semaphore_calls = 0
                async def mock_semaphore_acquire():
                    nonlocal semaphore_calls
                    semaphore_calls += 1
                    operation_order.append(f"semaphore_acquire_{semaphore_calls}")
                    return await original_acquire()

                pipeline._download_semaphore.acquire = mock_semaphore_acquire

                # Mock the actual operations to avoid network calls
                with patch("grin_to_s3.sync.pipeline.check_and_handle_etag_skip", return_value=(None, "etag123", 1000, [])):
                    with patch("grin_to_s3.sync.pipeline.download_book_to_staging", return_value=("book1", "/tmp/test", {})):

                            # Start multiple concurrent tasks
                            tasks = []
                            for i in range(3):
                                task = asyncio.create_task(pipeline._process_book_with_staging(f"book{i}"))
                                tasks.append(task)

                            # Wait for all tasks to complete
                            await asyncio.gather(*tasks, return_exceptions=True)

        # Verify that disk space checks happened BEFORE semaphore acquisitions
        space_operations = [op for op in operation_order if op.startswith("space_check")]
        semaphore_operations = [op for op in operation_order if op.startswith("semaphore_acquire")]

        assert len(space_operations) == 3, f"Expected 3 space checks, got {len(space_operations)}"
        assert len(semaphore_operations) == 3, f"Expected 3 semaphore acquisitions, got {len(semaphore_operations)}"

        # Find positions of first space check and first semaphore acquire
        first_space_pos = operation_order.index(space_operations[0])
        first_semaphore_pos = operation_order.index(semaphore_operations[0])

        # Space check should happen before semaphore acquisition
        assert first_space_pos < first_semaphore_pos, (
            f"Disk space check should happen before semaphore acquisition. "
            f"Order: {operation_order}"
        )

    @pytest.mark.asyncio
    async def test_disk_space_blocks_all_new_downloads(self, mock_run_config):
        """Test that when disk space is insufficient, ALL new downloads are blocked."""
        with patch("grin_to_s3.sync.pipeline.SQLiteProgressTracker") as mock_tracker:
            with patch("grin_to_s3.sync.pipeline.GRINClient") as mock_client:
                # Configure mocks
                mock_tracker.return_value = MagicMock()
                mock_client.return_value = MagicMock()

                # Create pipeline
                mock_stage = MagicMock()
                pipeline = SyncPipeline.from_run_config(config=mock_run_config, process_summary_stage=mock_stage)

                # Mock staging manager to block on disk space initially
                wait_calls = 0
                async def mock_wait_for_disk_space():
                    nonlocal wait_calls
                    wait_calls += 1
                    if wait_calls <= 2:  # First 2 calls blocked
                        await asyncio.sleep(0.1)  # Simulate waiting for space
                    # Third call and beyond proceed immediately

                pipeline.staging_manager.wait_for_disk_space = mock_wait_for_disk_space

                # Track when semaphore is acquired (actual download starts)
                semaphore_acquired = []
                original_acquire = pipeline._download_semaphore.acquire
                async def track_semaphore_acquire():
                    semaphore_acquired.append(asyncio.get_event_loop().time())
                    return await original_acquire()
                pipeline._download_semaphore.acquire = track_semaphore_acquire

                # Mock the actual operations
                with patch("grin_to_s3.sync.pipeline.check_and_handle_etag_skip", return_value=(None, "etag123", 1000, [])):
                    with patch("grin_to_s3.sync.pipeline.download_book_to_staging", return_value=("book1", "/tmp/test", {})):

                            # Start multiple concurrent tasks
                            tasks = []
                            for i in range(3):
                                task = asyncio.create_task(pipeline._process_book_with_staging(f"book{i}"))
                                tasks.append(task)

                            # Wait for all tasks
                            await asyncio.gather(*tasks, return_exceptions=True)

        # Verify that downloads were delayed due to disk space blocking
        assert wait_calls >= 3, f"Expected at least 3 disk space checks, got {wait_calls}"
        assert len(semaphore_acquired) == 3, f"Expected 3 semaphore acquisitions, got {len(semaphore_acquired)}"

        # The semaphore acquisitions should have been spread out due to disk space blocking
        if len(semaphore_acquired) >= 2:
            time_diff = semaphore_acquired[1] - semaphore_acquired[0]
            assert time_diff >= 0.05, f"Semaphore acquisitions too close together: {time_diff}s (expected delay due to disk space blocking)"
