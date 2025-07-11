#!/usr/bin/env python3
"""
Tests for OCR text extraction database tracking functionality.
"""

import json
import sqlite3

import pytest

from grin_to_s3.extract.tracking import (
    TEXT_EXTRACTION_STATUS_TYPE,
    ExtractionMethod,
    ExtractionStatus,
    get_extraction_progress,
    get_failed_extractions,
    get_status_summary,
    track_start,
)
from tests.test_utils.unified_mocks import create_progress_tracker_with_db_mock


@pytest.fixture
async def mock_db_tracker():
    """Create a mock SQLiteProgressTracker for testing."""
    return create_progress_tracker_with_db_mock("/tmp/test.db")


class TestTrackingFunctions:
    """Test individual tracking functions."""

    async def test_track_start(self, temp_db):
        """Test tracking extraction start."""
        barcode = "test_barcode_001"
        session_id = "session_123"

        await track_start(temp_db, barcode, session_id)

        # Verify data was written to database
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

        # Check metadata structure
        metadata = json.loads(record[4])
        assert "started_at" in metadata
        assert metadata["extraction_stage"] == "initialization"


class TestQueryFunctions:
    """Test database query functions."""

    async def test_get_status_summary_empty(self, temp_db):
        """Test status summary with empty database."""
        summary = await get_status_summary(temp_db)

        expected = {
            ExtractionStatus.STARTING.value: 0,
            ExtractionStatus.EXTRACTING.value: 0,
            ExtractionStatus.COMPLETED.value: 0,
            ExtractionStatus.FAILED.value: 0,
            "total": 0,
        }
        assert summary == expected

    async def test_get_status_summary_with_data(self, temp_db):
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

        summary = await get_status_summary(temp_db)

        assert summary[ExtractionStatus.COMPLETED.value] == 2
        assert summary[ExtractionStatus.FAILED.value] == 1
        assert summary[ExtractionStatus.EXTRACTING.value] == 1
        assert summary[ExtractionStatus.STARTING.value] == 0
        assert summary["total"] == 4

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

        progress = await get_extraction_progress(temp_db)

        assert progress["total_pages_extracted"] == 300  # 100 + 200
        assert progress["avg_pages_per_book"] == 150.0  # 300 / 2
        assert progress["avg_extraction_time_ms"] == 6000.0  # (5000 + 7000) / 2
        assert progress["status_summary"]["total"] == 3
        assert len(progress["recent_failures"]) == 1
