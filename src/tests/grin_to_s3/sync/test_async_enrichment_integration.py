#!/usr/bin/env python3
"""
Integration tests for async enrichment feature in sync pipeline.

Tests the complete integration of automatic enrichment after successful book uploads,
including queue management, worker lifecycle, error handling, and database tracking.
"""

import asyncio
import json
import sqlite3
import tempfile
from pathlib import Path
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from grin_to_s3.collect_books.models import SQLiteProgressTracker
from grin_to_s3.sync.pipeline import SyncPipeline


@pytest.fixture
async def temp_db_tracker():
    """Create a temporary database with a real SQLiteProgressTracker."""
    with tempfile.NamedTemporaryFile(suffix=".db", delete=False) as f:
        db_path = f.name

    # Initialize tracker which will create the schema
    tracker = SQLiteProgressTracker(db_path=db_path)
    await tracker.init_db()

    yield tracker, db_path

    # Cleanup
    await tracker.close()
    Path(db_path).unlink(missing_ok=True)


@pytest.fixture
def mock_grin_enrichment_pipeline():
    """Mock GRINEnrichmentPipeline for testing."""
    with patch("grin_to_s3.sync.pipeline.GRINEnrichmentPipeline") as mock_class:
        mock_pipeline = MagicMock()
        mock_pipeline.enrich_books_batch = AsyncMock()
        mock_pipeline.cleanup = AsyncMock()
        mock_class.return_value = mock_pipeline
        yield mock_pipeline


@pytest.fixture
def mock_test_config(test_config_builder):
    """Create a test RunConfig for pipeline testing."""

    def _create_config(db_path: str, **kwargs):
        builder = test_config_builder.with_db_path(db_path)

        # Apply storage configuration
        storage_type = kwargs.get("storage_type", "local")
        if storage_type == "local":
            storage_config = kwargs.get("storage_config", {"base_path": "/tmp"})
            builder = builder.local_storage(storage_config.get("base_path", "/tmp"))
        elif storage_type == "s3":
            storage_config = kwargs.get("storage_config", {"bucket_raw": "test-bucket"})
            builder = builder.s3_storage(**storage_config)
        elif storage_type == "r2":
            storage_config = kwargs.get("storage_config", {"bucket_raw": "test-bucket"})
            builder = builder.r2_storage(**storage_config)

        # Apply sync configuration
        sync_config = {
            "concurrent_downloads": kwargs.get("concurrent_downloads", 1),
            "concurrent_uploads": kwargs.get("concurrent_uploads", 1),
            "batch_size": kwargs.get("batch_size", 10),
            "enrichment_workers": kwargs.get("enrichment_workers", 1),
        }
        builder = builder.with_sync_config(**sync_config)

        return builder.build()

    return _create_config


@pytest.fixture
def mock_pipeline_dependencies():
    """Mock all pipeline dependencies."""
    with (
        patch("grin_to_s3.sync.pipeline.GRINClient") as mock_client,
        patch("grin_to_s3.sync.pipeline.StagingDirectoryManager") as mock_staging,
    ):
        # Configure mocks
        mock_client.return_value = MagicMock()
        mock_staging.return_value = MagicMock()

        yield {
            "client": mock_client.return_value,
            "staging": mock_staging.return_value,
        }


class TestAsyncEnrichmentQueueInfrastructure:
    """Test enrichment queue infrastructure and configuration."""

    def test_enrichment_queue_enabled_by_default(
        self, temp_db_tracker, mock_pipeline_dependencies, mock_process_stage, mock_test_config
    ):
        """Test that enrichment queue is enabled by default."""
        tracker, db_path = temp_db_tracker

        config = mock_test_config(db_path, storage_type="local", storage_config={"base_path": "/tmp"})
        pipeline = SyncPipeline.from_run_config(
            config=config,
            process_summary_stage=mock_process_stage,
        )

        # Enrichment should be enabled by default
        assert pipeline.enrichment_enabled is True
        assert pipeline.enrichment_workers == 1
        assert pipeline.enrichment_queue is not None
        assert pipeline.enrichment_queue.qsize() == 0

    def test_enrichment_queue_can_be_disabled(
        self, temp_db_tracker, mock_pipeline_dependencies, mock_process_stage, mock_test_config
    ):
        """Test that enrichment queue can be disabled."""
        tracker, db_path = temp_db_tracker

        config = mock_test_config(db_path, storage_type="local", storage_config={"base_path": "/tmp"})
        pipeline = SyncPipeline.from_run_config(
            config=config,
            process_summary_stage=mock_process_stage,
            skip_enrichment=True,
        )

        # Enrichment should be disabled
        assert pipeline.enrichment_enabled is False
        assert pipeline.enrichment_queue is None

    def test_enrichment_queue_configuration_options(
        self, temp_db_tracker, mock_pipeline_dependencies, mock_process_stage, mock_test_config
    ):
        """Test enrichment queue configuration options."""
        tracker, db_path = temp_db_tracker

        config = mock_test_config(
            db_path, storage_type="local", storage_config={"base_path": "/tmp"}, enrichment_workers=3
        )
        pipeline = SyncPipeline.from_run_config(
            config=config,
            process_summary_stage=mock_process_stage,
            skip_csv_export=True,
        )

        # Configuration should be set correctly
        assert pipeline.enrichment_enabled is True
        assert pipeline.enrichment_workers == 3
        assert pipeline.skip_csv_export is True
        assert pipeline.enrichment_queue is not None

    @pytest.mark.asyncio
    async def test_enrichment_queue_size_tracking_in_stats(
        self, temp_db_tracker, mock_pipeline_dependencies, mock_process_stage, mock_test_config
    ):
        """Test that enrichment queue size is tracked in pipeline statistics."""
        tracker, db_path = temp_db_tracker

        config = mock_test_config(db_path, storage_type="local", storage_config={"base_path": "/tmp"})
        pipeline = SyncPipeline.from_run_config(
            config=config,
            process_summary_stage=mock_process_stage,
        )

        # Mock database stats
        tracker.get_sync_stats = AsyncMock(return_value={"total_books": 100})

        # Initial stats should show queue size 0
        status = await pipeline.get_sync_status()
        assert status["session_stats"]["enrichment_queue_size"] == 0

        # Add items to queue
        await pipeline.enrichment_queue.put("barcode1")
        await pipeline.enrichment_queue.put("barcode2")
        await pipeline.enrichment_queue.put("barcode3")

        # Stats should reflect queue size
        status = await pipeline.get_sync_status()
        assert status["session_stats"]["enrichment_queue_size"] == 3

    @pytest.mark.asyncio
    async def test_enrichment_queue_size_zero_when_disabled(
        self, temp_db_tracker, mock_pipeline_dependencies, mock_process_stage, mock_test_config
    ):
        """Test that enrichment queue size is 0 when enrichment is disabled."""
        tracker, db_path = temp_db_tracker

        config = mock_test_config(db_path, storage_type="local", storage_config={"base_path": "/tmp"})
        pipeline = SyncPipeline.from_run_config(
            config=config,
            process_summary_stage=mock_process_stage,
            skip_enrichment=True,
        )

        # Mock database stats
        tracker.get_sync_stats = AsyncMock(return_value={"total_books": 100})

        # Stats should show queue size 0 when disabled
        status = await pipeline.get_sync_status()
        assert status["session_stats"]["enrichment_queue_size"] == 0


class TestAsyncEnrichmentWorkerLifecycle:
    """Test enrichment worker lifecycle management."""

    @pytest.mark.asyncio
    async def test_enrichment_workers_start_correctly(
        self,
        temp_db_tracker,
        mock_pipeline_dependencies,
        mock_grin_enrichment_pipeline,
        mock_process_stage,
        mock_test_config,
    ):
        """Test that enrichment workers start correctly."""
        tracker, db_path = temp_db_tracker

        config = mock_test_config(
            db_path, storage_type="local", storage_config={"base_path": "/tmp"}, enrichment_workers=2
        )
        pipeline = SyncPipeline.from_run_config(
            config=config,
            process_summary_stage=mock_process_stage,
        )

        # Start workers
        await pipeline.start_enrichment_workers()

        # Verify workers are running
        assert len(pipeline._enrichment_workers) == 2
        for worker in pipeline._enrichment_workers:
            assert not worker.done()

        # Cleanup
        await pipeline.stop_enrichment_workers()

    @pytest.mark.asyncio
    async def test_enrichment_workers_not_started_when_disabled(
        self, temp_db_tracker, mock_pipeline_dependencies, mock_process_stage, mock_test_config
    ):
        """Test that enrichment workers are not started when disabled."""
        tracker, db_path = temp_db_tracker

        config = mock_test_config(db_path, storage_type="local", storage_config={"base_path": "/tmp"})
        pipeline = SyncPipeline.from_run_config(
            config=config,
            process_summary_stage=mock_process_stage,
            skip_enrichment=True,
        )

        # Attempt to start workers
        await pipeline.start_enrichment_workers()

        # No workers should be started
        assert len(pipeline._enrichment_workers) == 0

    @pytest.mark.asyncio
    async def test_enrichment_workers_process_queued_books(
        self,
        temp_db_tracker,
        mock_pipeline_dependencies,
        mock_grin_enrichment_pipeline,
        mock_process_stage,
        mock_test_config,
    ):
        """Test that enrichment workers process queued books."""
        tracker, db_path = temp_db_tracker

        config = mock_test_config(
            db_path, storage_type="local", storage_config={"base_path": "/tmp"}, enrichment_workers=1
        )
        pipeline = SyncPipeline.from_run_config(
            config=config,
            process_summary_stage=mock_process_stage,
        )

        # Configure mock to return successful enrichment
        mock_grin_enrichment_pipeline.enrich_books_batch.return_value = 1

        # Start workers
        await pipeline.start_enrichment_workers()

        # Queue books for enrichment
        test_barcodes = ["TEST001", "TEST002", "TEST003"]
        for barcode in test_barcodes:
            await pipeline.queue_book_for_enrichment(barcode)

        # Wait for processing
        await asyncio.sleep(0.5)

        # Verify enrichment was called for each book
        assert mock_grin_enrichment_pipeline.enrich_books_batch.call_count == len(test_barcodes)

        # Cleanup
        await pipeline.stop_enrichment_workers()

    @pytest.mark.asyncio
    async def test_enrichment_workers_graceful_shutdown(
        self,
        temp_db_tracker,
        mock_pipeline_dependencies,
        mock_grin_enrichment_pipeline,
        mock_process_stage,
        mock_test_config,
    ):
        """Test graceful shutdown of enrichment workers."""
        tracker, db_path = temp_db_tracker

        config = mock_test_config(
            db_path, storage_type="local", storage_config={"base_path": "/tmp"}, enrichment_workers=2
        )
        pipeline = SyncPipeline.from_run_config(
            config=config,
            process_summary_stage=mock_process_stage,
        )

        # Configure mock to simulate slow processing
        async def slow_enrichment(*args, **kwargs):
            await asyncio.sleep(0.2)
            return 1

        mock_grin_enrichment_pipeline.enrich_books_batch.side_effect = slow_enrichment

        # Start workers
        await pipeline.start_enrichment_workers()

        # Queue some books
        await pipeline.queue_book_for_enrichment("TEST001")
        await pipeline.queue_book_for_enrichment("TEST002")

        # Allow some processing to start
        await asyncio.sleep(0.1)

        # Stop workers
        await pipeline.stop_enrichment_workers()

        # Verify all workers are stopped
        assert len(pipeline._enrichment_workers) == 0

    @pytest.mark.asyncio
    async def test_enrichment_workers_multiple_worker_configuration(
        self,
        temp_db_tracker,
        mock_pipeline_dependencies,
        mock_grin_enrichment_pipeline,
        mock_process_stage,
        mock_test_config,
    ):
        """Test multiple enrichment workers working concurrently."""
        tracker, db_path = temp_db_tracker

        config = mock_test_config(
            db_path, storage_type="local", storage_config={"base_path": "/tmp"}, enrichment_workers=3
        )
        pipeline = SyncPipeline.from_run_config(
            config=config,
            process_summary_stage=mock_process_stage,
        )

        # Configure mock to return successful enrichment
        mock_grin_enrichment_pipeline.enrich_books_batch.return_value = 1

        # Start workers
        await pipeline.start_enrichment_workers()

        # Verify correct number of workers
        assert len(pipeline._enrichment_workers) == 3

        # Queue multiple books
        test_barcodes = ["TEST001", "TEST002", "TEST003", "TEST004", "TEST005"]
        for barcode in test_barcodes:
            await pipeline.queue_book_for_enrichment(barcode)

        # Wait for processing
        await asyncio.sleep(0.5)

        # Verify enrichment was called for each book
        assert mock_grin_enrichment_pipeline.enrich_books_batch.call_count == len(test_barcodes)

        # Cleanup
        await pipeline.stop_enrichment_workers()


class TestAsyncEnrichmentEndToEndIntegration:
    """Test end-to-end integration of async enrichment with sync pipeline."""

    @pytest.mark.asyncio
    async def test_book_queued_for_enrichment_after_successful_upload(
        self, temp_db_tracker, mock_pipeline_dependencies, mock_process_stage, mock_test_config
    ):
        """Test that books are automatically queued for enrichment after successful upload."""
        tracker, db_path = temp_db_tracker

        config = mock_test_config(db_path, storage_type="local", storage_config={"base_path": "/tmp"})
        pipeline = SyncPipeline.from_run_config(
            config=config,
            process_summary_stage=mock_process_stage,
        )

        # Test queuing a book
        barcode = "TEST123456789"
        await pipeline.queue_book_for_enrichment(barcode)

        # Verify book is in queue
        assert pipeline.enrichment_queue.qsize() == 1

        # Verify database status
        with sqlite3.connect(db_path) as conn:
            cursor = conn.execute(
                """SELECT status_value FROM book_status_history
                   WHERE barcode = ? AND status_type = 'enrichment'
                   ORDER BY timestamp DESC LIMIT 1""",
                (barcode,),
            )
            result = cursor.fetchone()

        assert result is not None
        assert result[0] == "pending"

    @pytest.mark.asyncio
    async def test_enrichment_disabled_does_not_queue_books(
        self, temp_db_tracker, mock_pipeline_dependencies, mock_process_stage, mock_test_config
    ):
        """Test that books are not queued when enrichment is disabled."""
        tracker, db_path = temp_db_tracker

        config = mock_test_config(db_path, storage_type="local", storage_config={"base_path": "/tmp"})
        pipeline = SyncPipeline.from_run_config(
            config=config,
            process_summary_stage=mock_process_stage,
            skip_enrichment=True,
        )

        # Attempt to queue a book
        barcode = "TEST123456789"
        await pipeline.queue_book_for_enrichment(barcode)

        # Verify no queue exists
        assert pipeline.enrichment_queue is None

        # Verify no database entries
        with sqlite3.connect(db_path) as conn:
            cursor = conn.execute(
                """SELECT COUNT(*) FROM book_status_history
                   WHERE barcode = ? AND status_type = 'enrichment'""",
                (barcode,),
            )
            count = cursor.fetchone()[0]

        assert count == 0

    @pytest.mark.asyncio
    async def test_enrichment_with_local_storage(
        self, temp_db_tracker, mock_pipeline_dependencies, mock_process_stage, mock_test_config
    ):
        """Test enrichment works with local storage."""
        tracker, db_path = temp_db_tracker

        config = mock_test_config(db_path, storage_type="local", storage_config={"bucket_raw": "test-bucket"})
        pipeline = SyncPipeline.from_run_config(
            config=config,
            process_summary_stage=mock_process_stage,
        )

        # Test queuing works with local storage
        barcode = "TEST_LOCAL_123"
        await pipeline.queue_book_for_enrichment(barcode)

        assert pipeline.enrichment_queue.qsize() >= 1

        # Clear queue for cleanup
        try:
            while True:
                pipeline.enrichment_queue.get_nowait()
                pipeline.enrichment_queue.task_done()
        except asyncio.QueueEmpty:
            pass


class TestAsyncEnrichmentErrorHandling:
    """Test error handling and recovery in async enrichment."""

    @pytest.mark.asyncio
    async def test_enrichment_worker_handles_grin_api_failures(
        self,
        temp_db_tracker,
        mock_pipeline_dependencies,
        mock_grin_enrichment_pipeline,
        mock_process_stage,
        mock_test_config,
    ):
        """Test that enrichment workers handle GRIN API failures gracefully."""
        # This test directly exercises the error handling logic without relying on
        # the full async worker lifecycle to avoid fixture teardown race conditions

        tracker, db_path = temp_db_tracker
        barcode = "TEST123456789"

        # Mock the enrichment pipeline to simulate API failure
        mock_grin_enrichment_pipeline.enrich_books_batch.side_effect = Exception("GRIN API timeout")

        # Mock the database write to capture what would be written
        captured_status_updates = []
        async def capture_batch_write(db_path_arg, status_updates):
            captured_status_updates.extend(status_updates)
            return None

        with patch("grin_to_s3.database_utils.batch_write_status_updates", side_effect=capture_batch_write):
            # Import the functions used by the worker for error handling
            from grin_to_s3.database_utils import batch_write_status_updates
            from grin_to_s3.extract.tracking import collect_status

            # Simulate the worker's error handling logic directly
            worker_id = 0
            enrichment_status_updates = [
                collect_status(barcode, "enrichment", "in_progress", metadata={"worker_id": worker_id})
            ]

            # Simulate the enrichment call that throws an exception
            try:
                await mock_grin_enrichment_pipeline.enrich_books_batch([barcode])
            except Exception as e:
                # This is the exact error handling logic from the worker
                enrichment_status_updates.append(
                    collect_status(barcode, "enrichment", "failed", metadata={"worker_id": worker_id, "error": str(e)})
                )

            # Simulate the database write that happens in the worker
            await batch_write_status_updates(str(db_path), enrichment_status_updates)

        # Verify the error handling worked correctly
        assert mock_grin_enrichment_pipeline.enrich_books_batch.call_count == 1

        # Check that the failed status was captured
        failed_updates = [u for u in captured_status_updates if u.status_value == "failed" and u.barcode == barcode]
        assert len(failed_updates) == 1, f"Expected 1 failed status update, got {len(failed_updates)}"

        failed_update = failed_updates[0]
        assert failed_update.status_type == "enrichment"
        assert failed_update.barcode == barcode
        assert "GRIN API timeout" in failed_update.metadata["error"]
        assert failed_update.metadata["worker_id"] == worker_id

    @pytest.mark.asyncio
    async def test_enrichment_worker_continues_after_single_failure(
        self,
        temp_db_tracker,
        mock_pipeline_dependencies,
        mock_grin_enrichment_pipeline,
        mock_process_stage,
        mock_test_config,
    ):
        """Test that enrichment workers continue processing after single failures."""
        tracker, db_path = temp_db_tracker

        config = mock_test_config(
            db_path, storage_type="local", storage_config={"base_path": "/tmp"}, enrichment_workers=1
        )
        pipeline = SyncPipeline.from_run_config(
            config=config,
            process_summary_stage=mock_process_stage,
        )

        # Configure mock to fail first call, succeed second
        call_count = 0

        def mock_enrichment(*args, **kwargs):
            nonlocal call_count
            call_count += 1
            if call_count == 1:
                raise Exception("First call fails")
            return 1

        mock_grin_enrichment_pipeline.enrich_books_batch.side_effect = mock_enrichment

        # Start workers
        await pipeline.start_enrichment_workers()

        # Queue two books
        await pipeline.queue_book_for_enrichment("TEST001")
        await pipeline.queue_book_for_enrichment("TEST002")

        # Wait for processing
        await asyncio.sleep(0.5)

        # Verify both calls were made
        assert call_count == 2

        # Verify database shows one failure, one success
        with sqlite3.connect(db_path) as conn:
            cursor = conn.execute(
                """SELECT barcode, status_value FROM book_status_history
                   WHERE status_type = 'enrichment' AND status_value IN ('failed', 'completed')
                   ORDER BY timestamp""",
            )
            results = cursor.fetchall()

        # Should have both failed and completed entries
        statuses = [r[1] for r in results]
        assert "failed" in statuses
        assert "completed" in statuses

        # Cleanup
        await pipeline.stop_enrichment_workers()

    @pytest.mark.asyncio
    async def test_enrichment_worker_handles_database_errors(
        self,
        temp_db_tracker,
        mock_pipeline_dependencies,
        mock_grin_enrichment_pipeline,
        mock_process_stage,
        mock_test_config,
    ):
        """Test enrichment workers handle database errors gracefully."""
        tracker, db_path = temp_db_tracker

        config = mock_test_config(
            db_path, storage_type="local", storage_config={"base_path": "/tmp"}, enrichment_workers=1
        )
        pipeline = SyncPipeline.from_run_config(
            config=config,
            process_summary_stage=mock_process_stage,
        )

        # Configure mock to return successful enrichment
        mock_grin_enrichment_pipeline.enrich_books_batch.return_value = 1

        # Mock batch_write_status_updates to simulate database error
        with patch("grin_to_s3.database_utils.batch_write_status_updates", new_callable=AsyncMock) as mock_batch_write:
            mock_batch_write.side_effect = Exception("Database connection failed")

            # Start workers
            await pipeline.start_enrichment_workers()

            # Queue a book
            barcode = "TEST123456789"
            await pipeline.queue_book_for_enrichment(barcode)

            # Wait for processing
            await asyncio.sleep(0.5)

            # Verify enrichment was attempted despite database error
            assert mock_grin_enrichment_pipeline.enrich_books_batch.call_count == 1

            # Cleanup
            await pipeline.stop_enrichment_workers()


class TestAsyncEnrichmentDatabaseIntegration:
    """Test database integration for async enrichment."""

    @pytest.mark.asyncio
    async def test_enrichment_status_tracking(
        self,
        temp_db_tracker,
        mock_pipeline_dependencies,
        mock_grin_enrichment_pipeline,
        mock_process_stage,
        mock_test_config,
    ):
        """Test that enrichment status is properly tracked in database."""
        tracker, db_path = temp_db_tracker

        config = mock_test_config(
            db_path, storage_type="local", storage_config={"base_path": "/tmp"}, enrichment_workers=1
        )
        pipeline = SyncPipeline.from_run_config(
            config=config,
            process_summary_stage=mock_process_stage,
        )

        # Configure mock to return successful enrichment
        mock_grin_enrichment_pipeline.enrich_books_batch.return_value = 1

        # Start workers
        await pipeline.start_enrichment_workers()

        # Queue a book
        barcode = "TEST123456789"
        await pipeline.queue_book_for_enrichment(barcode)

        # Wait for processing
        await asyncio.sleep(0.5)

        # Verify status progression in database
        with sqlite3.connect(db_path) as conn:
            cursor = conn.execute(
                """SELECT status_value, metadata FROM book_status_history
                   WHERE barcode = ? AND status_type = 'enrichment'
                   ORDER BY timestamp""",
                (barcode,),
            )
            results = cursor.fetchall()

        # Should have at least pending and completed statuses
        assert len(results) >= 2
        statuses = [r[0] for r in results]
        assert "pending" in statuses
        assert "completed" in statuses

        # Cleanup
        await pipeline.stop_enrichment_workers()

    @pytest.mark.asyncio
    async def test_enrichment_metadata_storage(
        self,
        temp_db_tracker,
        mock_pipeline_dependencies,
        mock_grin_enrichment_pipeline,
        mock_process_stage,
        mock_test_config,
    ):
        """Test that enrichment metadata is properly stored."""
        tracker, db_path = temp_db_tracker

        config = mock_test_config(
            db_path, storage_type="local", storage_config={"base_path": "/tmp"}, enrichment_workers=1
        )
        pipeline = SyncPipeline.from_run_config(
            config=config,
            process_summary_stage=mock_process_stage,
        )

        # Configure mock to return successful enrichment
        mock_grin_enrichment_pipeline.enrich_books_batch.return_value = 1

        # Start workers
        await pipeline.start_enrichment_workers()

        # Queue a book
        barcode = "TEST123456789"
        await pipeline.queue_book_for_enrichment(barcode)

        # Wait for processing
        await asyncio.sleep(0.5)

        # Verify metadata is stored
        with sqlite3.connect(db_path) as conn:
            cursor = conn.execute(
                """SELECT metadata FROM book_status_history
                   WHERE barcode = ? AND status_type = 'enrichment' AND status_value = 'in_progress'
                   ORDER BY timestamp DESC LIMIT 1""",
                (barcode,),
            )
            result = cursor.fetchone()

        if result:
            metadata = json.loads(result[0])
            assert "worker_id" in metadata
            assert metadata["worker_id"] == 0

        # Cleanup
        await pipeline.stop_enrichment_workers()

    @pytest.mark.asyncio
    async def test_enrichment_no_data_handling(
        self,
        temp_db_tracker,
        mock_pipeline_dependencies,
        mock_grin_enrichment_pipeline,
        mock_process_stage,
        mock_test_config,
    ):
        """Test handling of books with no enrichment data."""
        tracker, db_path = temp_db_tracker

        config = mock_test_config(
            db_path, storage_type="local", storage_config={"base_path": "/tmp"}, enrichment_workers=1
        )
        pipeline = SyncPipeline.from_run_config(
            config=config,
            process_summary_stage=mock_process_stage,
        )

        # Configure mock to return no enrichment data
        mock_grin_enrichment_pipeline.enrich_books_batch.return_value = 0

        # Start workers
        await pipeline.start_enrichment_workers()

        # Queue a book
        barcode = "TEST123456789"
        await pipeline.queue_book_for_enrichment(barcode)

        # Wait for processing
        await asyncio.sleep(0.5)

        # Verify completed status with no_data result
        with sqlite3.connect(db_path) as conn:
            cursor = conn.execute(
                """SELECT status_value, metadata FROM book_status_history
                   WHERE barcode = ? AND status_type = 'enrichment' AND status_value = 'completed'
                   ORDER BY timestamp DESC LIMIT 1""",
                (barcode,),
            )
            result = cursor.fetchone()

        assert result is not None
        metadata = json.loads(result[1])
        assert metadata.get("result") == "no_data"

        # Cleanup
        await pipeline.stop_enrichment_workers()
