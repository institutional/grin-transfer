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
    extract_marc_completed,
    extract_ocr_completed,
    get_updates_for_task,
    on,
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
        # Test that existing handlers are properly registered
        test_key = (TaskType.CHECK, TaskAction.COMPLETED)

        # Verify the production handler exists
        assert test_key in UPDATE_HANDLERS, f"Expected production handler for {test_key} to be registered"
        handlers = UPDATE_HANDLERS[test_key]
        assert len(handlers) == 1, f"Expected exactly one handler for {test_key}"

        handler_func, status_type, status_value = handlers[0]
        assert callable(handler_func), "Handler function should be callable"
        assert status_type == "sync", f"Expected status_type 'sync', got '{status_type}'"
        assert status_value == "checked", f"Expected status_value 'checked', got '{status_value}'"


class TestHandlerBehavior:
    """Test individual handler functions."""

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

    @pytest.mark.asyncio
    @pytest.mark.parametrize(
        "task_type,handler_func,task_data,expected_status,expected_metadata_keys",
        [
            (
                TaskType.EXTRACT_OCR,
                extract_ocr_completed,
                {
                    "page_count": 5,
                    "extraction_time_ms": 2500,
                    "json_file_path": "/path/to/file.jsonl",
                },
                ("text_extraction", "completed"),
                ["page_count", "extraction_time_ms"],
            ),
            (
                TaskType.EXTRACT_MARC,
                extract_marc_completed,
                {
                    "marc_metadata": {
                        "title": "Test Book",
                        "author": "Test Author",
                        "publisher": "Test Publisher",
                    },
                    "field_count": 3,
                },
                ("marc_extraction", "completed"),
                ["field_count"],
            ),
        ],
    )
    async def test_extraction_handlers_include_metadata(
        self, task_type, handler_func, task_data, expected_status, expected_metadata_keys
    ):
        """Extraction completion handlers should include specific metadata fields."""
        result = TaskResult(
            barcode="TEST123",
            task_type=task_type,
            action=TaskAction.COMPLETED,
            data=task_data,
        )
        previous_results = {}

        updates = await handler_func(result, previous_results)

        assert updates["status"][0] == expected_status[0]
        assert updates["status"][1] == expected_status[1]

        metadata = updates["status"][2]
        assert metadata is not None

        # Verify all expected metadata keys are present with correct values
        for key in expected_metadata_keys:
            assert key in metadata
            assert metadata[key] == task_data[key]

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

    async def _commit_updates(self, pipeline, barcode="TEST123"):
        """Helper to commit accumulated database updates."""
        conn = await pipeline.db_tracker.get_connection()
        await commit_book_record_updates(pipeline, barcode, conn)

    def _create_task_result(self, task_type, action, data, error=None, reason=None):
        """Helper to create TaskResult objects."""
        result = TaskResult(barcode="TEST123", task_type=task_type, action=action, data=data)
        if error:
            result.error = error
        if reason:
            result.reason = reason
        return result

    def _create_download_result(self, etag="real_etag_value"):
        """Helper for common download result pattern."""
        return {TaskType.DOWNLOAD: self._create_task_result(TaskType.DOWNLOAD, TaskAction.COMPLETED, {"etag": etag})}

    async def _query_status_history(self, pipeline, status_type="sync", barcode="TEST123"):
        """Helper to query status history and return row."""
        async with connect_async(pipeline.db_tracker.db_path) as db:
            cursor = await db.execute(
                "SELECT status_type, status_value, metadata FROM book_status_history WHERE barcode = ? AND status_type = ?",
                (barcode, status_type),
            )
            return await cursor.fetchone()

    async def _query_books_fields(self, pipeline, *field_names, barcode="TEST123"):
        """Helper to query multiple fields from books table."""
        async with connect_async(pipeline.db_tracker.db_path) as db:
            fields_str = ", ".join(field_names)
            cursor = await db.execute(f"SELECT {fields_str} FROM books WHERE barcode = ?", (barcode,))
            row = await cursor.fetchone()
            return row if row else tuple(None for _ in field_names)

    def _verify_timestamp_recent(self, timestamp_str, test_description=""):
        """Helper to verify timestamp is valid and recent."""
        parsed_time = datetime.fromisoformat(timestamp_str.replace("Z", "+00:00"))
        time_diff = abs((datetime.now(UTC) - parsed_time).total_seconds())
        assert time_diff < 60, f"Timestamp should be recent: {test_description}"

    async def _run_upload_task(self, pipeline):
        """Helper to run upload task with standard setup."""
        task_manager = TaskManager({TaskType.UPLOAD: 1})

        async def upload_task():
            return self._create_task_result(
                TaskType.UPLOAD, TaskAction.COMPLETED, {"upload_path": "/bucket/TEST123.tar.gz"}
            )

        await task_manager.run_task(TaskType.UPLOAD, "TEST123", upload_task, pipeline, self._create_download_result())
        await self._commit_updates(pipeline)

    @pytest.mark.asyncio
    async def test_upload_task_database_integration(self, real_db_pipeline):
        """UPLOAD task should write status to history and update books table."""
        await self._run_upload_task(real_db_pipeline)

        # Verify status history update
        status_type, status_value, metadata_json = await self._query_status_history(real_db_pipeline)
        assert status_type == "sync"
        assert status_value == "uploaded"

        metadata = json.loads(metadata_json) if metadata_json else {}
        assert metadata.get("path") == "/bucket/TEST123.tar.gz"

        # Verify books table update
        storage_path, is_decrypted, encrypted_etag = await self._query_books_fields(
            real_db_pipeline, "storage_path", "is_decrypted", "encrypted_etag"
        )

        assert storage_path == "/bucket/TEST123.tar.gz"
        assert is_decrypted == 1  # SQLite stores as integer
        assert encrypted_etag == "real_etag_value"

    @pytest.mark.asyncio
    @pytest.mark.parametrize(
        "task_type,error_message,expected_status_value",
        [
            (TaskType.DOWNLOAD, "Connection refused after 3 retries", "download_failed"),
            (TaskType.UPLOAD, "S3 upload timeout", "upload_failed"),
        ],
    )
    async def test_failed_task_error_captured_in_database(
        self, real_db_pipeline, task_type, error_message, expected_status_value
    ):
        """Failed task errors should be captured in status history."""
        task_manager = TaskManager({task_type: 1})

        async def failed_task():
            return self._create_task_result(task_type, TaskAction.FAILED, {}, error=error_message)

        await task_manager.run_task(task_type, "TEST123", failed_task, real_db_pipeline, {})
        await self._commit_updates(real_db_pipeline)

        status_type, status_value, metadata_json = await self._query_status_history(real_db_pipeline)
        assert status_type == "sync"
        assert status_value == expected_status_value

        metadata = json.loads(metadata_json) if metadata_json else {}
        assert metadata.get("error") == error_message

    async def _apply_updates_to_pipeline(self, pipeline, updates, barcode="TEST123"):
        """Helper to apply updates to pipeline and commit to database."""
        from grin_to_s3.sync.db_updates import StatusUpdate

        if barcode not in pipeline.book_record_updates:
            pipeline.book_record_updates[barcode] = {"status_history": [], "books_fields": {}}

        if updates.get("status"):
            status_type, status_value, metadata = updates["status"]
            status_record = StatusUpdate(barcode, status_type, status_value, metadata)
            pipeline.book_record_updates[barcode]["status_history"].append(status_record)

        if updates.get("books"):
            pipeline.book_record_updates[barcode]["books_fields"].update(updates["books"])

        await self._commit_updates(pipeline, barcode)

    @pytest.mark.asyncio
    async def test_request_conversion_updates_processing_timestamp(self, real_db_pipeline):
        """Request conversion should update processing_request_timestamp in books table."""
        result = RequestConversionResult(
            barcode="TEST123",
            task_type=TaskType.REQUEST_CONVERSION,
            action=TaskAction.COMPLETED,
            data={"conversion_status": "requested", "request_count": 1},
            reason="success_conversion_requested",
        )

        updates = await get_updates_for_task(result, {})
        assert "processing_request_timestamp" in updates["books"]

        timestamp = updates["books"]["processing_request_timestamp"]
        self._verify_timestamp_recent(timestamp)

        await self._apply_updates_to_pipeline(real_db_pipeline, updates)

        # Verify timestamp was written to database
        (db_timestamp,) = await self._query_books_fields(real_db_pipeline, "processing_request_timestamp")
        assert db_timestamp == timestamp

        # Verify status was recorded
        status_type, status_value, metadata_json = await self._query_status_history(real_db_pipeline, "conversion")
        assert status_type == "conversion"
        assert status_value == "requested"

        metadata = json.loads(metadata_json) if metadata_json else {}
        assert metadata.get("reason") == "success_conversion_requested"

    @pytest.mark.asyncio
    async def test_request_conversion_429_limit_reached_database_tracking(self, real_db_pipeline):
        """429 responses from GRIN should be tracked as conversion limit_reached in database."""
        task_manager = TaskManager({TaskType.REQUEST_CONVERSION: 1})

        async def mock_429_request_conversion():
            return RequestConversionResult(
                barcode="TEST123",
                task_type=TaskType.REQUEST_CONVERSION,
                action=TaskAction.FAILED,
                data={"conversion_status": "queue_limit_reached", "request_count": 50},
                reason="fail_queue_limit_reached",
            )

        await task_manager.run_task(
            TaskType.REQUEST_CONVERSION, "TEST123", mock_429_request_conversion, real_db_pipeline, {}
        )
        await self._commit_updates(real_db_pipeline)

        # Verify limit_reached status was recorded
        status_type, status_value, metadata_json = await self._query_status_history(real_db_pipeline, "conversion")
        assert status_type == "conversion"
        assert status_value == "limit_reached"

        metadata = json.loads(metadata_json) if metadata_json else {}
        assert metadata.get("conversion_status") == "queue_limit_reached"
        assert metadata.get("reason") == "fail_queue_limit_reached"

        # Verify no processing timestamp was updated for failed requests
        (processing_timestamp,) = await self._query_books_fields(real_db_pipeline, "processing_request_timestamp")
        assert processing_timestamp is None

    @pytest.mark.asyncio
    @pytest.mark.parametrize(
        "reason,expected_status_value",
        [
            ("skip_already_in_process", "in_process"),
            ("skip_verified_unavailable", "unavailable"),
            ("skip_already_available", "skipped"),
            ("fail_generic_error", "failed"),
        ],
    )
    async def test_request_conversion_database_write_cases(self, real_db_pipeline, reason, expected_status_value):
        """Test REQUEST_CONVERSION database write cases with different reasons."""
        task_manager = TaskManager({TaskType.REQUEST_CONVERSION: 1})

        async def mock_request_conversion():
            action = TaskAction.FAILED if reason.startswith("fail_") else TaskAction.SKIPPED
            return RequestConversionResult(
                barcode="TEST123",
                task_type=TaskType.REQUEST_CONVERSION,
                action=action,
                data={"conversion_status": reason.replace("skip_", "").replace("fail_", ""), "request_count": 25},
                reason=reason,
            )

        await task_manager.run_task(
            TaskType.REQUEST_CONVERSION, "TEST123", mock_request_conversion, real_db_pipeline, {}
        )
        await self._commit_updates(real_db_pipeline)

        # Verify expected status was recorded
        status_type, status_value, metadata_json = await self._query_status_history(real_db_pipeline, "conversion")
        assert status_type == "conversion"
        assert status_value == expected_status_value

        metadata = json.loads(metadata_json) if metadata_json else {}
        assert metadata.get("reason") == reason

        # All these cases should leave processing_request_timestamp as NULL
        (processing_timestamp,) = await self._query_books_fields(real_db_pipeline, "processing_request_timestamp")
        assert processing_timestamp is None

    @pytest.mark.asyncio
    @pytest.mark.parametrize(
        "task_action,task_reason",
        [
            (TaskAction.COMPLETED, None),
            (TaskAction.SKIPPED, "skip_etag_match"),
        ],
    )
    async def test_check_task_updates_last_etag_check_timestamp(self, real_db_pipeline, task_action, task_reason):
        """CHECK tasks should update last_etag_check timestamp in books table."""
        task_manager = TaskManager({TaskType.CHECK: 1})

        async def mock_check_task():
            return self._create_task_result(
                TaskType.CHECK,
                task_action,
                {"etag": "test-etag", "file_size_bytes": 2048, "http_status_code": 200},
                reason=task_reason,
            )

        await task_manager.run_task(TaskType.CHECK, "TEST123", mock_check_task, real_db_pipeline, {})
        await self._commit_updates(real_db_pipeline)

        # Verify timestamp was populated and is recent
        (last_etag_check,) = await self._query_books_fields(real_db_pipeline, "last_etag_check")
        assert last_etag_check is not None
        self._verify_timestamp_recent(last_etag_check)
