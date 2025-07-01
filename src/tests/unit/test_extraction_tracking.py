#!/usr/bin/env python3
"""
Tests for OCR text extraction database tracking functionality.
"""

import json
import sqlite3
import tempfile
from pathlib import Path
from unittest.mock import AsyncMock, MagicMock

import pytest

from grin_to_s3.collect_books.models import SQLiteProgressTracker
from grin_to_s3.extract.tracking import (
    TEXT_EXTRACTION_STATUS_TYPE,
    ExtractionMethod,
    ExtractionStatus,
    get_extraction_progress,
    get_extraction_status_summary,
    get_failed_extractions,
    track_extraction_completion,
    track_extraction_failure,
    track_extraction_progress,
    track_extraction_start,
)


@pytest.fixture
async def temp_db():
    """Create a temporary database for testing."""
    with tempfile.NamedTemporaryFile(suffix=".db", delete=False) as f:
        db_path = f.name

    # Initialize with basic schema
    conn = sqlite3.connect(db_path)
    conn.execute("""
        CREATE TABLE IF NOT EXISTS book_status_history (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            barcode TEXT NOT NULL,
            status_type TEXT NOT NULL,
            status_value TEXT NOT NULL,
            timestamp TEXT NOT NULL,
            session_id TEXT,
            metadata TEXT
        )
    """)
    conn.execute("""
        CREATE TABLE IF NOT EXISTS books (
            barcode TEXT PRIMARY KEY,
            updated_at TEXT
        )
    """)
    conn.commit()
    conn.close()

    yield db_path

    # Cleanup
    Path(db_path).unlink(missing_ok=True)


@pytest.fixture
async def mock_db_tracker():
    """Create a mock SQLiteProgressTracker for testing."""
    tracker = MagicMock(spec=SQLiteProgressTracker)
    tracker.add_status_change = AsyncMock(return_value=True)
    return tracker


class TestTrackingFunctions:
    """Test individual tracking functions."""

    @pytest.mark.asyncio
    async def test_track_extraction_start(self, mock_db_tracker):
        """Test tracking extraction start."""
        barcode = "test_barcode_001"
        session_id = "session_123"

        await track_extraction_start(mock_db_tracker, barcode, session_id)

        # Verify add_status_change was called correctly
        mock_db_tracker.add_status_change.assert_called_once()
        call_args = mock_db_tracker.add_status_change.call_args

        assert call_args[0][0] == barcode  # barcode
        assert call_args[0][1] == TEXT_EXTRACTION_STATUS_TYPE  # status_type
        assert call_args[0][2] == ExtractionStatus.STARTING.value  # status_value
        assert call_args[1]["session_id"] == session_id

        # Check metadata structure
        metadata = call_args[1]["metadata"]
        assert "started_at" in metadata
        assert metadata["extraction_stage"] == "initialization"

    @pytest.mark.asyncio
    async def test_track_extraction_progress(self, mock_db_tracker):
        """Test tracking extraction progress."""
        barcode = "test_barcode_002"
        page_count = 150

        await track_extraction_progress(mock_db_tracker, barcode, page_count)

        mock_db_tracker.add_status_change.assert_called_once()
        call_args = mock_db_tracker.add_status_change.call_args

        assert call_args[0][2] == ExtractionStatus.EXTRACTING.value

        metadata = call_args[1]["metadata"]
        assert metadata["page_count"] == page_count
        assert metadata["extraction_stage"] == "processing_pages"
        assert "progress_at" in metadata

    @pytest.mark.asyncio
    async def test_track_extraction_completion(self, mock_db_tracker):
        """Test tracking successful extraction completion."""
        barcode = "test_barcode_003"
        page_count = 200
        extraction_time_ms = 5000
        jsonl_file_size = 1024000
        output_path = "/tmp/test.jsonl"

        await track_extraction_completion(
            mock_db_tracker,
            barcode,
            page_count,
            extraction_time_ms,
            jsonl_file_size,
            output_path,
        )

        mock_db_tracker.add_status_change.assert_called_once()
        call_args = mock_db_tracker.add_status_change.call_args

        assert call_args[0][2] == ExtractionStatus.COMPLETED.value

        metadata = call_args[1]["metadata"]
        assert metadata["page_count"] == page_count
        assert metadata["extraction_time_ms"] == extraction_time_ms
        assert metadata["jsonl_file_size"] == jsonl_file_size
        assert metadata["output_path"] == output_path
        assert metadata["extraction_method"] == ExtractionMethod.DISK.value  # default
        assert "completed_at" in metadata

    @pytest.mark.asyncio
    async def test_track_extraction_failure(self, mock_db_tracker):
        """Test tracking extraction failure."""
        barcode = "test_barcode_004"
        error = ValueError("Test error message")
        partial_page_count = 50

        await track_extraction_failure(
            mock_db_tracker,
            barcode,
            error,
            partial_page_count,
            extraction_method=ExtractionMethod.DISK,
        )

        mock_db_tracker.add_status_change.assert_called_once()
        call_args = mock_db_tracker.add_status_change.call_args

        assert call_args[0][2] == ExtractionStatus.FAILED.value

        metadata = call_args[1]["metadata"]
        assert metadata["error_type"] == "ValueError"
        assert metadata["error_message"] == "Test error message"
        assert metadata["partial_page_count"] == partial_page_count
        assert metadata["extraction_method"] == "disk"
        assert "failed_at" in metadata
        assert "error_details" in metadata

    @pytest.mark.asyncio
    async def test_tracking_with_db_failure(self, mock_db_tracker):
        """Test that tracking failures are handled gracefully."""
        # Mock database failure
        mock_db_tracker.add_status_change.side_effect = Exception("Database error")

        barcode = "test_barcode_005"

        # Should not raise exception, just log warning
        await track_extraction_start(mock_db_tracker, barcode)

        # Verify it tried to call the database
        mock_db_tracker.add_status_change.assert_called_once()


class TestQueryFunctions:
    """Test database query functions."""

    @pytest.mark.asyncio
    async def test_get_extraction_status_summary_empty(self, temp_db):
        """Test status summary with empty database."""
        summary = await get_extraction_status_summary(temp_db)

        expected = {
            ExtractionStatus.STARTING: 0,
            ExtractionStatus.EXTRACTING: 0,
            ExtractionStatus.COMPLETED: 0,
            ExtractionStatus.FAILED: 0,
            "total": 0,
        }
        assert summary == expected

    @pytest.mark.asyncio
    async def test_get_extraction_status_summary_with_data(self, temp_db):
        """Test status summary with sample data."""
        # Insert test data
        conn = sqlite3.connect(temp_db)
        test_data = [
            ("book1", TEXT_EXTRACTION_STATUS_TYPE, ExtractionStatus.COMPLETED.value, "2024-01-01T10:00:00Z"),
            ("book2", TEXT_EXTRACTION_STATUS_TYPE, ExtractionStatus.COMPLETED.value, "2024-01-01T11:00:00Z"),
            ("book3", TEXT_EXTRACTION_STATUS_TYPE, ExtractionStatus.FAILED.value, "2024-01-01T12:00:00Z"),
            ("book4", TEXT_EXTRACTION_STATUS_TYPE, ExtractionStatus.EXTRACTING.value, "2024-01-01T13:00:00Z"),
        ]

        for barcode, status_type, status_value, timestamp in test_data:
            conn.execute(
                """INSERT INTO book_status_history
                   (barcode, status_type, status_value, timestamp)
                   VALUES (?, ?, ?, ?)""",
                (barcode, status_type, status_value, timestamp),
            )
        conn.commit()
        conn.close()

        summary = await get_extraction_status_summary(temp_db)

        assert summary[ExtractionStatus.COMPLETED.value] == 2
        assert summary[ExtractionStatus.FAILED.value] == 1
        assert summary[ExtractionStatus.EXTRACTING.value] == 1
        assert summary[ExtractionStatus.STARTING.value] == 0
        assert summary["total"] == 4

    @pytest.mark.asyncio
    async def test_get_failed_extractions(self, temp_db):
        """Test retrieving failed extraction details."""
        # Insert test data
        conn = sqlite3.connect(temp_db)

        failure_metadata = {
            "error_type": "TextExtractionError",
            "error_message": "Archive corrupted",
            "partial_page_count": 25,
            "extraction_method": ExtractionMethod.DISK.value,
        }

        conn.execute(
            """INSERT INTO book_status_history
               (barcode, status_type, status_value, timestamp, metadata)
               VALUES (?, ?, ?, ?, ?)""",
            (
                "failed_book",
                TEXT_EXTRACTION_STATUS_TYPE,
                ExtractionStatus.FAILED.value,
                "2024-01-01T10:00:00Z",
                json.dumps(failure_metadata),
            ),
        )
        conn.commit()
        conn.close()

        failures = await get_failed_extractions(temp_db, limit=10)

        assert len(failures) == 1
        failure = failures[0]
        assert failure["barcode"] == "failed_book"
        assert failure["error_type"] == "TextExtractionError"
        assert failure["error_message"] == "Archive corrupted"
        assert failure["partial_page_count"] == 25
        assert failure["extraction_method"] == ExtractionMethod.DISK.value

    @pytest.mark.asyncio
    async def test_get_extraction_progress(self, temp_db):
        """Test getting overall extraction progress statistics."""
        # Insert test data
        conn = sqlite3.connect(temp_db)

        completed_metadata1 = {
            "page_count": 100,
            "extraction_time_ms": 5000,
        }
        completed_metadata2 = {
            "page_count": 200,
            "extraction_time_ms": 7000,
        }

        test_data = [
            ("book1", ExtractionStatus.COMPLETED, json.dumps(completed_metadata1)),
            ("book2", ExtractionStatus.COMPLETED, json.dumps(completed_metadata2)),
            ("book3", ExtractionStatus.FAILED, '{"error_type": "TestError"}'),
        ]

        for barcode, status_value, metadata in test_data:
            conn.execute(
                """INSERT INTO book_status_history
                   (barcode, status_type, status_value, timestamp, metadata)
                   VALUES (?, ?, ?, ?, ?)""",
                (barcode, TEXT_EXTRACTION_STATUS_TYPE, status_value, "2024-01-01T10:00:00Z", metadata),
            )
        conn.commit()
        conn.close()

        progress = await get_extraction_progress(temp_db)

        assert progress["total_pages_extracted"] == 300  # 100 + 200
        assert progress["avg_pages_per_book"] == 150.0  # 300 / 2
        assert progress["avg_extraction_time_ms"] == 6000.0  # (5000 + 7000) / 2
        assert progress["status_summary"]["total"] == 3
        assert len(progress["recent_failures"]) == 1


class TestMetadataSerialization:
    """Test metadata handling and serialization."""

    @pytest.mark.asyncio
    async def test_metadata_with_none_values(self, mock_db_tracker):
        """Test tracking with None values in metadata."""
        barcode = "test_barcode_006"

        await track_extraction_completion(
            mock_db_tracker,
            barcode,
            page_count=0,  # Edge case: zero pages
            extraction_time_ms=0,
            jsonl_file_size=0,
            output_path="",
        )

        mock_db_tracker.add_status_change.assert_called_once()
        call_args = mock_db_tracker.add_status_change.call_args
        metadata = call_args[1]["metadata"]

        # Verify all fields are present even with zero values
        assert metadata["page_count"] == 0
        assert metadata["extraction_time_ms"] == 0
        assert metadata["jsonl_file_size"] == 0
        assert metadata["output_path"] == ""

    @pytest.mark.asyncio
    async def test_session_id_handling(self, mock_db_tracker):
        """Test session ID handling in tracking functions."""
        barcode = "test_barcode_007"
        session_id = "test_session_456"

        # Test with session ID
        await track_extraction_start(mock_db_tracker, barcode, session_id)
        call_args = mock_db_tracker.add_status_change.call_args
        assert call_args[1]["session_id"] == session_id

        mock_db_tracker.reset_mock()

        # Test without session ID (None)
        await track_extraction_start(mock_db_tracker, barcode)
        call_args = mock_db_tracker.add_status_change.call_args
        assert call_args[1]["session_id"] is None
