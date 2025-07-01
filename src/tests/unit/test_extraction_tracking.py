#!/usr/bin/env python3
"""
Tests for OCR text extraction database tracking functionality.
"""

import json
import sqlite3
import tempfile
from pathlib import Path

import pytest

from grin_to_s3.extract.tracking import (
    TEXT_EXTRACTION_STATUS_TYPE,
    ExtractionMethod,
    ExtractionStatus,
    get_extraction_progress,
    get_failed_extractions,
    get_status_summary,
    track_completion,
    track_failure,
    track_progress,
    track_start,
    write_status,
)


@pytest.fixture
def temp_db():
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
    conn.commit()
    conn.close()

    yield db_path

    # Cleanup
    Path(db_path).unlink(missing_ok=True)


class TestTrackingFunctions:
    """Test individual tracking functions."""

    def test_track_start(self, temp_db):
        """Test tracking extraction start."""
        barcode = "test_barcode_001"
        session_id = "session_123"

        track_start(temp_db, barcode, session_id)

        # Verify record was written
        conn = sqlite3.connect(temp_db)
        cursor = conn.execute(
            "SELECT barcode, status_type, status_value, session_id, metadata FROM book_status_history"
        )
        record = cursor.fetchone()
        conn.close()

        assert record is not None
        assert record[0] == barcode
        assert record[1] == TEXT_EXTRACTION_STATUS_TYPE
        assert record[2] == ExtractionStatus.STARTING.value
        assert record[3] == session_id

        metadata = json.loads(record[4])
        assert "started_at" in metadata
        assert metadata["extraction_stage"] == "initialization"

    def test_track_progress(self, temp_db):
        """Test tracking extraction progress."""
        barcode = "test_barcode_002"
        page_count = 150

        track_progress(temp_db, barcode, page_count)

        conn = sqlite3.connect(temp_db)
        cursor = conn.execute(
            "SELECT status_value, metadata FROM book_status_history WHERE barcode = ?"
            , (barcode,)
        )
        record = cursor.fetchone()
        conn.close()

        assert record[0] == ExtractionStatus.EXTRACTING.value
        metadata = json.loads(record[1])
        assert metadata["page_count"] == page_count
        assert metadata["extraction_stage"] == "processing_pages"
        assert "progress_at" in metadata

    def test_track_completion(self, temp_db):
        """Test tracking successful extraction completion."""
        barcode = "test_barcode_003"
        page_count = 200
        extraction_time_ms = 5000
        method = ExtractionMethod.DISK

        track_completion(temp_db, barcode, page_count, extraction_time_ms, method)

        conn = sqlite3.connect(temp_db)
        cursor = conn.execute(
            "SELECT status_value, metadata FROM book_status_history WHERE barcode = ?"
            , (barcode,)
        )
        record = cursor.fetchone()
        conn.close()

        assert record[0] == ExtractionStatus.COMPLETED.value
        metadata = json.loads(record[1])
        assert metadata["page_count"] == page_count
        assert metadata["extraction_time_ms"] == extraction_time_ms
        assert metadata["extraction_method"] == method.value
        assert "completed_at" in metadata

    def test_track_failure(self, temp_db):
        """Test tracking extraction failure."""
        barcode = "test_barcode_004"
        error = ValueError("Test error message")
        method = ExtractionMethod.MEMORY

        track_failure(temp_db, barcode, error, method)

        conn = sqlite3.connect(temp_db)
        cursor = conn.execute(
            "SELECT status_value, metadata FROM book_status_history WHERE barcode = ?"
            , (barcode,)
        )
        record = cursor.fetchone()
        conn.close()

        assert record[0] == ExtractionStatus.FAILED.value
        metadata = json.loads(record[1])
        assert metadata["error_type"] == "ValueError"
        assert metadata["error_message"] == "Test error message"
        assert metadata["extraction_method"] == method.value
        assert "failed_at" in metadata

    def test_write_status_with_invalid_db(self, temp_db):
        """Test that tracking failures are handled gracefully."""
        # Use invalid database path
        invalid_path = "/invalid/path/that/cannot/be/created.db"

        # Should not raise exception, just log warning
        write_status(invalid_path, "test", ExtractionStatus.STARTING)


class TestQueryFunctions:
    """Test database query functions."""

    def test_get_status_summary_empty(self, temp_db):
        """Test status summary with empty database."""
        summary = get_status_summary(temp_db)

        expected = {
            ExtractionStatus.STARTING.value: 0,
            ExtractionStatus.EXTRACTING.value: 0,
            ExtractionStatus.COMPLETED.value: 0,
            ExtractionStatus.FAILED.value: 0,
            "total": 0,
        }
        assert summary == expected

    def test_get_status_summary_with_data(self, temp_db):
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

        summary = get_status_summary(temp_db)

        assert summary[ExtractionStatus.COMPLETED.value] == 2
        assert summary[ExtractionStatus.FAILED.value] == 1
        assert summary[ExtractionStatus.EXTRACTING.value] == 1
        assert summary[ExtractionStatus.STARTING.value] == 0
        assert summary["total"] == 4

    def test_get_failed_extractions(self, temp_db):
        """Test retrieving failed extraction details."""
        # Insert test data
        conn = sqlite3.connect(temp_db)

        failure_metadata = {
            "error_type": "TextExtractionError",
            "error_message": "Archive corrupted",
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

        failures = get_failed_extractions(temp_db, limit=10)

        assert len(failures) == 1
        failure = failures[0]
        assert failure["barcode"] == "failed_book"
        assert failure["error_type"] == "TextExtractionError"
        assert failure["error_message"] == "Archive corrupted"
        assert failure["extraction_method"] == ExtractionMethod.DISK.value

    def test_get_extraction_progress(self, temp_db):
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
            ("book1", ExtractionStatus.COMPLETED.value, json.dumps(completed_metadata1)),
            ("book2", ExtractionStatus.COMPLETED.value, json.dumps(completed_metadata2)),
            ("book3", ExtractionStatus.FAILED.value, '{"error_type": "TestError"}'),
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

        progress = get_extraction_progress(temp_db)

        assert progress["total_pages_extracted"] == 300  # 100 + 200
        assert progress["avg_pages_per_book"] == 150.0  # 300 / 2
        assert progress["avg_extraction_time_ms"] == 6000.0  # (5000 + 7000) / 2
        assert progress["status_summary"]["total"] == 3
        assert len(progress["recent_failures"]) == 1


class TestSessionTracking:
    """Test session-based tracking functionality."""

    def test_session_tracking_isolation(self, temp_db):
        """Test that session IDs properly isolate batch operations."""
        session1 = "batch_session_1"
        session2 = "batch_session_2"

        # Track with different session IDs
        track_start(temp_db, "book1", session1)
        track_start(temp_db, "book2", session2)

        # Verify session isolation in database
        conn = sqlite3.connect(temp_db)

        # Check session1 records
        cursor = conn.execute(
            """SELECT COUNT(*) FROM book_status_history
               WHERE status_type = ? AND session_id = ?""",
            (TEXT_EXTRACTION_STATUS_TYPE, session1),
        )
        session1_count = cursor.fetchone()[0]

        # Check session2 records
        cursor = conn.execute(
            """SELECT COUNT(*) FROM book_status_history
               WHERE status_type = ? AND session_id = ?""",
            (TEXT_EXTRACTION_STATUS_TYPE, session2),
        )
        session2_count = cursor.fetchone()[0]

        conn.close()

        # Each session should have its own records
        assert session1_count == 1
        assert session2_count == 1

