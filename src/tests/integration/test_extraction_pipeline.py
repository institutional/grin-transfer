#!/usr/bin/env python3
"""
Integration tests for OCR text extraction with database tracking.

Tests the full extraction pipeline including database status tracking,
ensuring that the text extraction and database operations work together
correctly in realistic scenarios.
"""

import json
import sqlite3
import tempfile
from pathlib import Path

import pytest

from grin_to_s3.collect_books.models import SQLiteProgressTracker
from grin_to_s3.extract.text_extraction import (
    extract_text_from_archive,
    extract_text_to_jsonl_file,
)
from grin_to_s3.extract.tracking import (
    TEXT_EXTRACTION_STATUS_TYPE,
    ExtractionStatus,
    get_extraction_progress,
    get_extraction_status_summary,
    get_failed_extractions,
)
from tests.utils import create_test_archive


@pytest.fixture
async def temp_db_tracker():
    """Create a temporary database with a real SQLiteProgressTracker."""
    with tempfile.NamedTemporaryFile(suffix=".db", delete=False) as f:
        db_path = f.name

    # Initialize tracker which will create the schema
    tracker = SQLiteProgressTracker(db_path=db_path)
    await tracker.init_db()

    yield tracker

    # Cleanup
    await tracker.close()
    Path(db_path).unlink(missing_ok=True)


@pytest.fixture
def test_archive_with_content():
    """Create a test archive with realistic page content."""
    with tempfile.TemporaryDirectory() as temp_dir:
        temp_path = Path(temp_dir)

        # Create archive with multiple pages and various content
        pages = {
            "00000001.txt": "Chapter 1: Introduction\n\nThis is the first page of the book.",
            "00000002.txt": "Chapter 1 continued\n\nMore content on page two.",
            "00000003.txt": "Chapter 2: Methods\n\nDetailed methodology section.",
            "00000005.txt": "Chapter 3: Results\n\nPage 5 content (page 4 is missing).",
            "00000006.txt": "Conclusion\n\nFinal thoughts and summary.",
        }

        archive_path = create_test_archive(pages, temp_path)

        yield str(archive_path)


class TestExtractionWithTracking:
    """Test text extraction with full database tracking."""

    @pytest.mark.asyncio
    async def test_successful_extraction_with_tracking(self, temp_db_tracker, test_archive_with_content):
        """Test successful text extraction with complete status tracking."""
        # Get the actual barcode from the archive path
        from grin_to_s3.extract.text_extraction import get_barcode_from_path
        barcode = get_barcode_from_path(test_archive_with_content)
        session_id = "test_session_123"

        # Run extraction with tracking
        result = extract_text_from_archive(
            test_archive_with_content,
            db_tracker=temp_db_tracker,
            session_id=session_id,
        )

        # Verify extraction results
        assert len(result) == 6  # Pages 1-6 (including gap at page 4)
        assert result[0] == "Chapter 1: Introduction\n\nThis is the first page of the book."
        assert result[1] == "Chapter 1 continued\n\nMore content on page two."
        assert result[2] == "Chapter 2: Methods\n\nDetailed methodology section."
        assert result[3] == ""  # Missing page 4
        assert result[4] == "Chapter 3: Results\n\nPage 5 content (page 4 is missing)."
        assert result[5] == "Conclusion\n\nFinal thoughts and summary."

        # Verify database tracking was recorded
        # Allow some time for async tasks to complete
        import asyncio
        await asyncio.sleep(0.1)

        # Check status history entries
        conn = sqlite3.connect(temp_db_tracker.db_path)
        cursor = conn.execute(
            """SELECT status_value, metadata FROM book_status_history
               WHERE barcode = ? AND status_type = ?
               ORDER BY timestamp""",
            (barcode, TEXT_EXTRACTION_STATUS_TYPE),
        )
        records = cursor.fetchall()
        conn.close()

        # Should have at least starting and completed status
        assert len(records) >= 2

        # Check starting status
        start_record = records[0]
        assert start_record[0] == ExtractionStatus.STARTING
        start_metadata = json.loads(start_record[1])
        assert start_metadata["extraction_stage"] == "initialization"

        # Check completion status (last record)
        completion_record = records[-1]
        assert completion_record[0] == ExtractionStatus.COMPLETED
        completion_metadata = json.loads(completion_record[1])
        assert completion_metadata["page_count"] == 6
        assert completion_metadata["extraction_method"] == "disk"
        assert completion_metadata["extraction_time_ms"] >= 0  # Can be 0 for very fast extractions

    @pytest.mark.asyncio
    async def test_jsonl_extraction_with_tracking(self, temp_db_tracker, test_archive_with_content):
        """Test JSONL extraction with database tracking."""
        # Get the actual barcode from the archive path
        from grin_to_s3.extract.text_extraction import get_barcode_from_path
        barcode = get_barcode_from_path(test_archive_with_content)
        session_id = "test_session_456"

        with tempfile.NamedTemporaryFile(suffix=".jsonl", delete=False) as f:
            output_path = f.name

        try:
            # Run JSONL extraction with tracking
            page_count = extract_text_to_jsonl_file(
                test_archive_with_content,
                output_path,
                db_tracker=temp_db_tracker,
                session_id=session_id,
            )

            # Verify extraction results
            assert page_count == 6

            # Verify JSONL file content
            with open(output_path, encoding="utf-8") as f:
                lines = f.readlines()

            assert len(lines) == 6

            # Check first and last pages
            page1 = json.loads(lines[0].strip())
            assert page1 == "Chapter 1: Introduction\n\nThis is the first page of the book."

            page4 = json.loads(lines[3].strip())
            assert page4 == ""  # Missing page

            page6 = json.loads(lines[5].strip())
            assert page6 == "Conclusion\n\nFinal thoughts and summary."

            # Allow time for async tasks
            import asyncio
            await asyncio.sleep(0.1)

            # Verify tracking in database
            conn = sqlite3.connect(temp_db_tracker.db_path)
            cursor = conn.execute(
                """SELECT status_value, metadata FROM book_status_history
                   WHERE barcode = ? AND status_type = ? AND status_value = ?""",
                (barcode, TEXT_EXTRACTION_STATUS_TYPE, ExtractionStatus.COMPLETED),
            )
            record = cursor.fetchone()
            conn.close()

            assert record is not None
            metadata = json.loads(record[1])
            assert metadata["page_count"] == 6
            assert metadata["extraction_method"] == "streaming"
            assert metadata["jsonl_file_size"] > 0
            assert metadata["output_path"] == output_path

        finally:
            Path(output_path).unlink(missing_ok=True)

    @pytest.mark.asyncio
    async def test_extraction_failure_tracking(self, temp_db_tracker):
        """Test that extraction failures are properly tracked in database."""
        # Create a file with known barcode pattern for predictable tracking
        with tempfile.NamedTemporaryFile(suffix=".tar.gz", delete=False) as f:
            nonexistent_path = f.name
        # Remove the file so extraction will fail but we know the barcode
        Path(nonexistent_path).unlink()

        barcode = Path(nonexistent_path).name.split(".")[0]  # Extract barcode from filename
        session_id = "test_session_789"

        # Try to extract from nonexistent file
        from grin_to_s3.extract.text_extraction import TextExtractionError
        with pytest.raises(TextExtractionError):
            extract_text_from_archive(
                nonexistent_path,
                db_tracker=temp_db_tracker,
                session_id=session_id,
            )

        # Allow time for async tasks
        import asyncio
        await asyncio.sleep(0.1)

        # Verify failure was tracked
        conn = sqlite3.connect(temp_db_tracker.db_path)
        cursor = conn.execute(
            """SELECT status_value, metadata FROM book_status_history
               WHERE barcode = ? AND status_type = ? AND status_value = ?""",
            (barcode, TEXT_EXTRACTION_STATUS_TYPE, ExtractionStatus.FAILED),
        )
        record = cursor.fetchone()
        conn.close()

        assert record is not None
        metadata = json.loads(record[1])
        assert "error_type" in metadata
        assert "error_message" in metadata
        assert metadata["partial_page_count"] == 0
        assert metadata["extraction_method"] in ["memory", "disk"]

    @pytest.mark.asyncio
    async def test_tracking_resilience_to_db_failures(self, test_archive_with_content):
        """Test that extraction continues even when database tracking fails."""
        # Create tracker with invalid database path
        invalid_tracker = SQLiteProgressTracker(db_path="/invalid/path/that/cannot/be/created.db")

        # Extraction should still work despite tracking failures
        result = extract_text_from_archive(
            test_archive_with_content,
            db_tracker=invalid_tracker,
            session_id="test_session",
        )

        # Verify extraction succeeded despite tracking failure
        assert len(result) == 6
        assert result[0] == "Chapter 1: Introduction\n\nThis is the first page of the book."


class TestQueryFunctionsIntegration:
    """Test query functions with real database data."""

    @pytest.mark.asyncio
    async def test_status_summary_with_mixed_extractions(self, temp_db_tracker, test_archive_with_content):
        """Test status summary query with multiple extraction records."""
        # Perform several extractions with different outcomes

        # Successful extraction
        extract_text_from_archive(
            test_archive_with_content,
            db_tracker=temp_db_tracker,
            session_id="session1",
        )

        # Another successful extraction (different barcode)
        # We'll simulate this by directly adding to database since we need different barcodes
        await temp_db_tracker.add_status_change(
            "book2",
            TEXT_EXTRACTION_STATUS_TYPE,
            ExtractionStatus.COMPLETED,
            metadata={"page_count": 100, "extraction_time_ms": 2000},
        )

        # Failed extraction
        await temp_db_tracker.add_status_change(
            "book3",
            TEXT_EXTRACTION_STATUS_TYPE,
            ExtractionStatus.FAILED,
            metadata={"error_type": "CorruptedArchiveError", "error_message": "Archive damaged"},
        )

        # In-progress extraction
        await temp_db_tracker.add_status_change(
            "book4",
            TEXT_EXTRACTION_STATUS_TYPE,
            ExtractionStatus.EXTRACTING,
            metadata={"page_count": 50},
        )

        # Allow time for async tasks
        import asyncio
        await asyncio.sleep(0.1)

        # Test status summary
        summary = await get_extraction_status_summary(temp_db_tracker.db_path)

        assert summary[ExtractionStatus.COMPLETED] >= 2
        assert summary[ExtractionStatus.FAILED] >= 1
        assert summary[ExtractionStatus.EXTRACTING] >= 1
        assert summary["total"] >= 4

    @pytest.mark.asyncio
    async def test_failed_extractions_query(self, temp_db_tracker):
        """Test retrieving failed extraction details."""
        # Add some failed extractions
        failure_cases = [
            {
                "barcode": "corrupt_book_1",
                "error_type": "CorruptedArchiveError",
                "error_message": "Cannot open tar.gz file",
                "partial_page_count": 0,
            },
            {
                "barcode": "invalid_book_2",
                "error_type": "InvalidPageFormatError",
                "error_message": "No valid page files found",
                "partial_page_count": 5,
            },
        ]

        for case in failure_cases:
            await temp_db_tracker.add_status_change(
                case["barcode"],
                TEXT_EXTRACTION_STATUS_TYPE,
                ExtractionStatus.FAILED,
                metadata=case,
            )

        # Query failed extractions
        failures = await get_failed_extractions(temp_db_tracker.db_path, limit=10)

        assert len(failures) >= 2

        # Verify failure details are correctly retrieved
        barcodes = [f["barcode"] for f in failures]
        assert "corrupt_book_1" in barcodes
        assert "invalid_book_2" in barcodes

        for failure in failures:
            if failure["barcode"] == "corrupt_book_1":
                assert failure["error_type"] == "CorruptedArchiveError"
                assert failure["partial_page_count"] == 0
            elif failure["barcode"] == "invalid_book_2":
                assert failure["error_type"] == "InvalidPageFormatError"
                assert failure["partial_page_count"] == 5

    @pytest.mark.asyncio
    async def test_extraction_progress_statistics(self, temp_db_tracker):
        """Test overall progress statistics calculation."""
        # Add completed extractions with various statistics
        completed_cases = [
            {"page_count": 150, "extraction_time_ms": 3000},
            {"page_count": 200, "extraction_time_ms": 4000},
            {"page_count": 100, "extraction_time_ms": 2000},
        ]

        for i, case in enumerate(completed_cases, 1):
            await temp_db_tracker.add_status_change(
                f"completed_book_{i}",
                TEXT_EXTRACTION_STATUS_TYPE,
                ExtractionStatus.COMPLETED,
                metadata=case,
            )

        # Add a failure for comprehensive stats
        await temp_db_tracker.add_status_change(
            "failed_book",
            TEXT_EXTRACTION_STATUS_TYPE,
            ExtractionStatus.FAILED,
            metadata={"error_type": "TestError", "partial_page_count": 10},
        )

        # Get progress statistics
        progress = await get_extraction_progress(temp_db_tracker.db_path)

        # Verify statistics
        assert progress["total_pages_extracted"] == 450  # 150 + 200 + 100
        assert progress["avg_pages_per_book"] == 150.0  # 450 / 3
        assert progress["avg_extraction_time_ms"] == 3000.0  # (3000 + 4000 + 2000) / 3

        # Check status summary is included
        assert "status_summary" in progress
        assert progress["status_summary"][ExtractionStatus.COMPLETED] >= 3
        assert progress["status_summary"][ExtractionStatus.FAILED] >= 1

        # Check recent failures
        assert len(progress["recent_failures"]) >= 1
        assert progress["recent_failures"][0]["barcode"] == "failed_book"


class TestSessionTracking:
    """Test session-based tracking functionality."""

    @pytest.mark.asyncio
    async def test_session_tracking_isolation(self, temp_db_tracker, test_archive_with_content):
        """Test that session IDs properly isolate batch operations."""
        session1 = "batch_session_1"
        session2 = "batch_session_2"

        # Extract with different session IDs
        extract_text_from_archive(
            test_archive_with_content,
            db_tracker=temp_db_tracker,
            session_id=session1,
        )

        # Add another book manually for session2
        await temp_db_tracker.add_status_change(
            "book_session2",
            TEXT_EXTRACTION_STATUS_TYPE,
            ExtractionStatus.COMPLETED,
            session_id=session2,
            metadata={"page_count": 75, "extraction_time_ms": 1500},
        )

        # Allow time for async tasks
        import asyncio
        await asyncio.sleep(0.1)

        # Verify session isolation in database
        conn = sqlite3.connect(temp_db_tracker.db_path)

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
        assert session1_count >= 1  # At least completion record from extraction
        assert session2_count >= 1  # At least the manual record

        # Verify session IDs are properly stored
        assert session1_count > 0
        assert session2_count > 0
