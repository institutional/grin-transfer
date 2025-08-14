#!/usr/bin/env python3
"""Tests for sync db_updates module."""

import json
import tempfile
from datetime import UTC, datetime
from pathlib import Path
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from grin_to_s3.collect_books.models import SQLiteProgressTracker
from grin_to_s3.database import connect_async
from grin_to_s3.sync.db_updates import UPDATE_HANDLERS, download_failed, on, update_database_for_task, upload_completed
from grin_to_s3.sync.tasks.task_types import TaskAction, TaskResult, TaskType


@pytest.fixture
def mock_pipeline():
    """Mock pipeline with database tracker."""
    pipeline = MagicMock()
    pipeline.config = MagicMock()
    pipeline.config.storage_config = {"protocol": "s3"}
    pipeline.current_etags = {"TEST123": "stored_etag_value"}
    pipeline.db_tracker = MagicMock()
    pipeline.db_tracker.db_path = "/tmp/test.db"
    pipeline.db_tracker.update_sync_data = AsyncMock()
    return pipeline


class TestHandlerRegistration:
    """Test the @on decorator registration mechanism."""

    def test_handler_registration(self):
        """@on decorator should register handlers in UPDATE_HANDLERS registry."""
        # Clear any existing handlers for this test
        test_key = (TaskType.CHECK, TaskAction.COMPLETED)
        if test_key in UPDATE_HANDLERS:
            del UPDATE_HANDLERS[test_key]

        @on(TaskType.CHECK, TaskAction.COMPLETED, "test_status", "test_value")
        def test_handler(result, pipeline_data):
            return {"metadata": {"test": True}}

        # Verify handler was registered
        assert test_key in UPDATE_HANDLERS
        handlers = UPDATE_HANDLERS[test_key]
        assert len(handlers) == 1
        handler_func, status_type, status_value = handlers[0]
        assert handler_func == test_handler
        assert status_type == "test_status"
        assert status_value == "test_value"


class TestHandlerBehavior:
    """Test individual handler functions."""

    @pytest.mark.asyncio
    async def test_handler_returns_data(self):
        """upload_completed handler should return metadata and sync_data structures."""
        result = TaskResult(
            barcode="TEST123",
            task_type=TaskType.UPLOAD,
            action=TaskAction.COMPLETED,
            data={"upload_path": "/bucket/TEST123.tar.gz"},
        )
        pipeline_data = {"storage_protocol": "s3", "etags": {"TEST123": "abc123"}}

        handler_result = await upload_completed(result, pipeline_data)

        # Should return both metadata and sync_data
        assert "metadata" in handler_result
        assert "sync_data" in handler_result
        assert handler_result["metadata"]["path"] == "/bucket/TEST123.tar.gz"
        assert handler_result["sync_data"]["storage_type"] == "s3"
        assert handler_result["sync_data"]["is_decrypted"] is True

    @pytest.mark.asyncio
    async def test_handler_with_error(self):
        """download_failed handler should include error message in metadata."""
        result = TaskResult(
            barcode="TEST123", task_type=TaskType.DOWNLOAD, action=TaskAction.FAILED, error="Connection timeout"
        )
        pipeline_data = {}

        handler_result = await download_failed(result, pipeline_data)

        assert handler_result["metadata"] is not None
        assert handler_result["metadata"]["error"] == "Connection timeout"


class TestDatabaseUpdateOrchestration:
    """Test the main update_database_for_task orchestration."""

    @patch("grin_to_s3.sync.db_updates.batch_write_status_updates")
    @pytest.mark.asyncio
    async def test_update_database_basic_flow(self, mock_batch_write, mock_pipeline):
        """update_database_for_task should write status updates and sync_data for completed tasks."""
        result = TaskResult(
            barcode="TEST123",
            task_type=TaskType.UPLOAD,
            action=TaskAction.COMPLETED,
            data={"upload_path": "/bucket/TEST123.tar.gz"},
        )

        await update_database_for_task(result, mock_pipeline)

        # Verify status update was written
        mock_batch_write.assert_called_once()
        call_args = mock_batch_write.call_args[0]
        db_path = call_args[0]
        status_updates = call_args[1]

        assert db_path == str(mock_pipeline.db_tracker.db_path)
        assert len(status_updates) == 1
        assert status_updates[0].barcode == "TEST123"
        assert status_updates[0].status_type == "sync"
        assert status_updates[0].status_value == "uploaded"

        # Verify sync_data was updated
        mock_pipeline.db_tracker.update_sync_data.assert_called_once_with(
            "TEST123",
            {
                "storage_type": "s3",
                "storage_path": "/bucket/TEST123.tar.gz",
                "is_decrypted": True,
                "sync_timestamp": mock_pipeline.db_tracker.update_sync_data.call_args[0][1]["sync_timestamp"],
                "encrypted_etag": "stored_etag_value",
            },
        )

    @patch("grin_to_s3.sync.db_updates.batch_write_status_updates")
    @pytest.mark.asyncio
    async def test_multiple_handlers_for_same_task(self, mock_batch_write, mock_pipeline):
        """update_database_for_task should execute all registered handlers for a task/action."""
        # Register a second handler for the same task/action
        test_key = (TaskType.UPLOAD, TaskAction.COMPLETED)
        original_handlers = UPDATE_HANDLERS.get(test_key, [])

        @on(TaskType.UPLOAD, TaskAction.COMPLETED, "custom_status", "custom_uploaded")
        async def custom_upload_handler(result, pipeline_data):
            return {"metadata": {"custom_field": "custom_value"}}

        try:
            result = TaskResult(
                barcode="TEST123",
                task_type=TaskType.UPLOAD,
                action=TaskAction.COMPLETED,
                data={"upload_path": "/bucket/TEST123.tar.gz"},
            )

            await update_database_for_task(result, mock_pipeline)

            # Should have called batch_write with multiple status updates
            mock_batch_write.assert_called_once()
            status_updates = mock_batch_write.call_args[0][1]
            assert len(status_updates) == 2  # Original + custom handler

            # Find the custom status update
            custom_update = next(u for u in status_updates if u.status_type == "custom_status")
            assert custom_update.status_value == "custom_uploaded"
            assert custom_update.metadata["custom_field"] == "custom_value"

        finally:
            # Clean up the test handler
            UPDATE_HANDLERS[test_key] = original_handlers

    @patch("grin_to_s3.sync.db_updates.batch_write_status_updates")
    @pytest.mark.asyncio
    async def test_failed_task_handling(self, mock_batch_write, mock_pipeline):
        """update_database_for_task should capture error messages in status updates for failed tasks."""
        result = TaskResult(
            barcode="TEST123",
            task_type=TaskType.DOWNLOAD,
            action=TaskAction.FAILED,
            error="Network timeout after 3 retries",
        )

        await update_database_for_task(result, mock_pipeline)

        # Verify error was captured in status update
        mock_batch_write.assert_called_once()
        status_updates = mock_batch_write.call_args[0][1]
        assert len(status_updates) == 1

        status_update = status_updates[0]
        assert status_update.barcode == "TEST123"
        assert status_update.status_type == "sync"
        assert status_update.status_value == "download_failed"
        assert status_update.metadata["error"] == "Network timeout after 3 retries"


@pytest.fixture
async def real_db_pipeline():
    """Create a pipeline with real SQLite database for testing."""
    with tempfile.TemporaryDirectory() as temp_dir:
        db_path = Path(temp_dir) / "test.db"

        # Initialize database with schema
        db_tracker = SQLiteProgressTracker(str(db_path))
        await db_tracker.init_db()

        # Insert a test book record with required fields
        now = datetime.now(UTC).isoformat()
        async with connect_async(str(db_path)) as db:
            await db.execute(
                "INSERT INTO books (barcode, title, created_at, updated_at) VALUES (?, ?, ?, ?)",
                ("TEST123", "Test Book Title", now, now),
            )
            await db.commit()

        # Create mock pipeline with real database tracker
        pipeline = MagicMock()
        pipeline.config = MagicMock()
        pipeline.config.storage_config = {"protocol": "s3"}
        pipeline.current_etags = {"TEST123": "real_etag_value"}
        pipeline.db_tracker = db_tracker

        yield pipeline


class TestRealDatabaseIntegration:
    """Test database updates with real SQLite database."""

    @pytest.mark.asyncio
    async def test_status_updates_written_to_database(self, real_db_pipeline):
        """Status updates should be written to book_status_history table."""
        result = TaskResult(
            barcode="TEST123",
            task_type=TaskType.UPLOAD,
            action=TaskAction.COMPLETED,
            data={"upload_path": "/bucket/TEST123.tar.gz"},
        )

        await update_database_for_task(result, real_db_pipeline)

        # Verify status was written to database
        async with connect_async(real_db_pipeline.db_tracker.db_path) as db:
            cursor = await db.execute(
                "SELECT barcode, status_type, status_value, metadata FROM book_status_history WHERE barcode = ?",
                ("TEST123",),
            )
            row = await cursor.fetchone()

            assert row is not None
            assert row[0] == "TEST123"  # barcode
            assert row[1] == "sync"  # status_type
            assert row[2] == "uploaded"  # status_value

            # Verify metadata contains upload path
            metadata = json.loads(row[3]) if row[3] else {}
            assert metadata.get("path") == "/bucket/TEST123.tar.gz"

    @pytest.mark.asyncio
    async def test_sync_data_updated_in_books_table(self, real_db_pipeline):
        """Sync data should be updated in the books table."""
        result = TaskResult(
            barcode="TEST123",
            task_type=TaskType.UPLOAD,
            action=TaskAction.COMPLETED,
            data={"upload_path": "/bucket/TEST123.tar.gz"},
        )

        await update_database_for_task(result, real_db_pipeline)

        # Verify sync data was updated in books table
        async with connect_async(real_db_pipeline.db_tracker.db_path) as db:
            cursor = await db.execute(
                "SELECT storage_type, storage_path, is_decrypted, encrypted_etag FROM books WHERE barcode = ?",
                ("TEST123",),
            )
            row = await cursor.fetchone()

            assert row is not None
            assert row[0] == "s3"  # storage_type
            assert row[1] == "/bucket/TEST123.tar.gz"  # storage_path
            assert row[2] == 1  # is_decrypted (SQLite stores as integer)
            assert row[3] == "real_etag_value"  # encrypted_etag

    @pytest.mark.asyncio
    async def test_failed_task_error_captured_in_database(self, real_db_pipeline):
        """Failed task errors should be captured in status history."""
        result = TaskResult(
            barcode="TEST123",
            task_type=TaskType.DOWNLOAD,
            action=TaskAction.FAILED,
            error="Connection refused after 3 retries",
        )

        await update_database_for_task(result, real_db_pipeline)

        # Verify error was captured in database
        async with connect_async(real_db_pipeline.db_tracker.db_path) as db:
            cursor = await db.execute(
                "SELECT status_type, status_value, metadata FROM book_status_history WHERE barcode = ? AND status_type = ?",
                ("TEST123", "sync"),
            )
            row = await cursor.fetchone()

            assert row is not None
            assert row[0] == "sync"  # status_type
            assert row[1] == "download_failed"  # status_value

            # Verify error message in metadata
            metadata = json.loads(row[2]) if row[2] else {}
            assert metadata.get("error") == "Connection refused after 3 retries"
