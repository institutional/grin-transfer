#!/usr/bin/env python3
"""Tests for sync db_updates module."""

import json
import tempfile
from datetime import UTC, datetime
from pathlib import Path
from unittest.mock import AsyncMock, MagicMock

import pytest

from grin_to_s3.collect_books.models import SQLiteProgressTracker
from grin_to_s3.database import connect_async
from grin_to_s3.sync.db_updates import (
    UPDATE_HANDLERS,
    commit_book_record_updates,
    download_failed,
    get_updates_for_task,
    on,
    upload_completed,
)
from grin_to_s3.sync.task_manager import TaskManager
from grin_to_s3.sync.tasks.task_types import RequestConversionResult, TaskAction, TaskResult, TaskType


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
    # Add book_record_updates for accumulating database changes
    pipeline.book_record_updates = {}
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
        """upload_completed handler should return status and books structures."""
        result = TaskResult(
            barcode="TEST123",
            task_type=TaskType.UPLOAD,
            action=TaskAction.COMPLETED,
            data={"upload_path": "/bucket/TEST123.tar.gz"},
        )
        previous_results = {
            TaskType.DOWNLOAD: TaskResult(
                barcode="TEST123", task_type=TaskType.DOWNLOAD, action=TaskAction.COMPLETED, data={"etag": "abc123"}
            )
        }

        handler_result = await upload_completed(result, previous_results)

        # Should return both status and books
        assert "status" in handler_result
        assert "books" in handler_result

        # Verify status tuple structure (type, value, metadata)
        status_type, status_value, metadata = handler_result["status"]
        assert status_type == "sync"
        assert status_value == "uploaded"
        assert metadata["path"] == "/bucket/TEST123.tar.gz"

        # Verify books data
        assert handler_result["books"]["storage_path"] == "/bucket/TEST123.tar.gz"
        assert handler_result["books"]["is_decrypted"] is True
        assert handler_result["books"]["encrypted_etag"] == "abc123"

    @pytest.mark.asyncio
    async def test_handler_with_error(self):
        """download_failed handler should include error message in status metadata."""
        result = TaskResult(
            barcode="TEST123", task_type=TaskType.DOWNLOAD, action=TaskAction.FAILED, error="Connection timeout"
        )
        previous_results = {}

        handler_result = await download_failed(result, previous_results)

        # Should return status with error in metadata
        assert "status" in handler_result
        status_type, status_value, metadata = handler_result["status"]
        assert status_type == "sync"
        assert status_value == "download_failed"
        assert metadata is not None
        assert metadata["error"] == "Connection timeout"


class TestDatabaseUpdateOrchestration:
    """Test the main update_database_for_task orchestration."""

    @pytest.mark.asyncio
    async def test_get_updates_basic_flow(self):
        """get_updates_for_task should return correct update data structure for completed tasks."""
        result = TaskResult(
            barcode="TEST123",
            task_type=TaskType.UPLOAD,
            action=TaskAction.COMPLETED,
            data={"upload_path": "/bucket/TEST123.tar.gz"},
        )

        # Mock previous results with download containing etag
        previous_results = {
            TaskType.DOWNLOAD: TaskResult(
                barcode="TEST123", task_type=TaskType.DOWNLOAD, action=TaskAction.COMPLETED, data={"etag": "abc123"}
            )
        }

        updates = await get_updates_for_task(result, previous_results)

        # Verify correct structure is returned
        assert "status" in updates
        assert "books" in updates

        # Verify status tuple
        status_type, status_value, metadata = updates["status"]
        assert status_type == "sync"
        assert status_value == "uploaded"
        assert metadata == {"path": "/bucket/TEST123.tar.gz"}

        # Verify books update contains etag from download
        books_updates = updates["books"]
        assert books_updates["storage_path"] == "/bucket/TEST123.tar.gz"
        assert books_updates["is_decrypted"] is True
        assert books_updates["encrypted_etag"] == "abc123"  # From download result
        assert "sync_timestamp" in books_updates

    def test_duplicate_handler_prevention(self):
        """System should prevent duplicate handlers for the same task/action."""
        # Try to register a duplicate handler for UPLOAD+COMPLETED (which already exists)
        with pytest.raises(ValueError, match="Handler for UPLOAD\\+completed already exists"):

            @on(TaskType.UPLOAD, TaskAction.COMPLETED, "custom_status", "custom_uploaded")
            async def duplicate_upload_handler(result, previous_results):
                return {"status": ("custom_status", "custom_uploaded", {"custom_field": "custom_value"}), "books": {}}

    @pytest.mark.asyncio
    async def test_failed_task_handling(self, mock_pipeline):
        """TaskManager.run_task should capture error messages in status updates for failed tasks."""
        task_manager = TaskManager({TaskType.DOWNLOAD: 1})

        async def mock_failed_download():
            return TaskResult(
                barcode="TEST123",
                task_type=TaskType.DOWNLOAD,
                action=TaskAction.FAILED,
                error="Network timeout after 3 retries",
            )

        previous_results = {}
        await task_manager.run_task(TaskType.DOWNLOAD, "TEST123", mock_failed_download, mock_pipeline, previous_results)

        # Verify error was captured in accumulated updates
        assert "TEST123" in mock_pipeline.book_record_updates
        book_updates = mock_pipeline.book_record_updates["TEST123"]
        assert "status_history" in book_updates

        status_updates = book_updates["status_history"]
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
        # Add book_record_updates for accumulating database changes
        pipeline.book_record_updates = {}

        # Add storage protocol info for handlers
        pipeline.storage_protocol = "s3"

        yield pipeline


class TestRealDatabaseIntegration:
    """Test database updates with real SQLite database."""

    @pytest.mark.asyncio
    async def test_status_updates_written_to_database(self, real_db_pipeline):
        """Status updates should be written to book_status_history table."""
        task_manager = TaskManager({TaskType.UPLOAD: 1})

        async def mock_upload_task():
            return TaskResult(
                barcode="TEST123",
                task_type=TaskType.UPLOAD,
                action=TaskAction.COMPLETED,
                data={"upload_path": "/bucket/TEST123.tar.gz", "storage_type": "s3"},
            )

        previous_results = {
            TaskType.DOWNLOAD: TaskResult(
                barcode="TEST123",
                task_type=TaskType.DOWNLOAD,
                action=TaskAction.COMPLETED,
                data={"etag": "download_etag_value"},
            )
        }
        await task_manager.run_task(TaskType.UPLOAD, "TEST123", mock_upload_task, real_db_pipeline, previous_results)

        # Commit accumulated updates to the real database
        conn = await real_db_pipeline.db_tracker.get_connection()
        await commit_book_record_updates(real_db_pipeline, "TEST123", conn)

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
        task_manager = TaskManager({TaskType.UPLOAD: 1})

        async def mock_upload_task():
            return TaskResult(
                barcode="TEST123",
                task_type=TaskType.UPLOAD,
                action=TaskAction.COMPLETED,
                data={"upload_path": "/bucket/TEST123.tar.gz", "storage_type": "s3"},
            )

        previous_results = {
            TaskType.DOWNLOAD: TaskResult(
                barcode="TEST123",
                task_type=TaskType.DOWNLOAD,
                action=TaskAction.COMPLETED,
                data={"etag": "real_etag_value"},
            )
        }
        await task_manager.run_task(TaskType.UPLOAD, "TEST123", mock_upload_task, real_db_pipeline, previous_results)

        # Commit accumulated updates to the real database
        conn = await real_db_pipeline.db_tracker.get_connection()
        await commit_book_record_updates(real_db_pipeline, "TEST123", conn)

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
        task_manager = TaskManager({TaskType.DOWNLOAD: 1})

        async def mock_failed_download():
            return TaskResult(
                barcode="TEST123",
                task_type=TaskType.DOWNLOAD,
                action=TaskAction.FAILED,
                error="Connection refused after 3 retries",
            )

        previous_results = {}
        await task_manager.run_task(
            TaskType.DOWNLOAD, "TEST123", mock_failed_download, real_db_pipeline, previous_results
        )

        # Commit accumulated updates to the real database
        conn = await real_db_pipeline.db_tracker.get_connection()
        await commit_book_record_updates(real_db_pipeline, "TEST123", conn)

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

    @pytest.mark.asyncio
    async def test_request_conversion_updates_processing_timestamp(self, real_db_pipeline):
        """Request conversion should update processing_request_timestamp in books table via batching."""
        # Create a request conversion result for successful conversion request
        result = RequestConversionResult(
            barcode="TEST123",
            task_type=TaskType.REQUEST_CONVERSION,
            action=TaskAction.SKIPPED,
            data={"conversion_status": "requested", "request_count": 1},
            reason="skip_conversion_requested",
        )

        # Get the database updates that would be applied
        updates = await get_updates_for_task(result, {})

        # Verify that processing_request_timestamp is included in books table updates
        assert "books" in updates
        books_updates = updates["books"]
        assert "processing_request_timestamp" in books_updates

        # Verify timestamp is a valid ISO format
        timestamp = books_updates["processing_request_timestamp"]
        parsed_time = datetime.fromisoformat(timestamp.replace("Z", "+00:00"))
        assert parsed_time is not None

        # Verify timestamp is recent (within last minute)
        now = datetime.now(UTC)
        time_diff = abs((now - parsed_time).total_seconds())
        assert time_diff < 60, f"Timestamp should be recent, but was {time_diff} seconds ago"

        # Simulate the batching process - accumulate updates
        barcode = "TEST123"
        if barcode not in real_db_pipeline.book_record_updates:
            real_db_pipeline.book_record_updates[barcode] = {"status_history": [], "books_fields": {}}

        # Add status update if present
        if updates.get("status"):
            from grin_to_s3.sync.db_updates import StatusUpdate

            status_type, status_value, metadata = updates["status"]
            status_record = StatusUpdate(barcode, status_type, status_value, metadata)
            real_db_pipeline.book_record_updates[barcode]["status_history"].append(status_record)

        # Add books table field updates
        if updates.get("books"):
            real_db_pipeline.book_record_updates[barcode]["books_fields"].update(updates["books"])

        # Commit accumulated updates to the real database
        conn = await real_db_pipeline.db_tracker.get_connection()
        await commit_book_record_updates(real_db_pipeline, barcode, conn)

        # Verify the processing_request_timestamp was updated in the database
        async with connect_async(real_db_pipeline.db_tracker.db_path) as db:
            cursor = await db.execute("SELECT processing_request_timestamp FROM books WHERE barcode = ?", ("TEST123",))
            row = await cursor.fetchone()

            assert row is not None
            db_timestamp = row[0]
            assert db_timestamp is not None
            assert db_timestamp == timestamp  # Should match the generated timestamp

            # Also verify status was recorded
            cursor = await db.execute(
                "SELECT status_type, status_value, metadata FROM book_status_history WHERE barcode = ? AND status_type = ?",
                ("TEST123", "conversion"),
            )
            status_row = await cursor.fetchone()

            assert status_row is not None
            assert status_row[0] == "conversion"  # status_type
            assert status_row[1] == "requested"  # status_value

            # Verify metadata contains conversion details
            metadata = json.loads(status_row[2]) if status_row[2] else {}
            assert metadata.get("conversion_status") == "requested"
            assert metadata.get("request_count") == 1
            assert metadata.get("reason") == "skip_conversion_requested"
