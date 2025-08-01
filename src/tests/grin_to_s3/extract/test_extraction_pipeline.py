#!/usr/bin/env python3
"""
Integration tests for OCR text extraction with database tracking.

Tests the full extraction pipeline including database status tracking,
ensuring that the text extraction and database operations work together
correctly in realistic scenarios.
"""

import asyncio
import json
import sqlite3
import tempfile
from pathlib import Path

import pytest

from grin_to_s3.collect_books.models import SQLiteProgressTracker
from grin_to_s3.database_utils import batch_write_status_updates
from grin_to_s3.extract.text_extraction import (
    TextExtractionError,
    extract_ocr_pages,
)
from grin_to_s3.extract.tracking import (
    TEXT_EXTRACTION_STATUS_TYPE,
    ExtractionStatus,
    collect_status,
    get_extraction_progress,
    get_failed_extractions,
    get_status_summary,
)
from tests.utils import create_extracted_directory


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
def test_extracted_directory_with_content():
    """Create a test extracted directory with realistic page content."""
    with tempfile.TemporaryDirectory() as temp_dir:
        temp_path = Path(temp_dir)

        # Create extracted directory with multiple pages and various content
        pages = {
            "00000001.txt": "Chapter 1: Introduction\n\nThis is the first page of the book.",
            "00000002.txt": "Chapter 1 continued\n\nMore content on page two.",
            "00000003.txt": "Chapter 2: Methods\n\nDetailed methodology section.",
            "00000005.txt": "Chapter 3: Results\n\nPage 5 content (page 4 is missing).",
            "00000006.txt": "Conclusion\n\nFinal thoughts and summary.",
        }

        extracted_dir = create_extracted_directory(pages, temp_path, "test_archive_extracted")

        yield str(extracted_dir)


class TestExtractionWithTracking:
    """Test text extraction with full database tracking."""


    @pytest.mark.asyncio
    async def test_jsonl_extraction_with_tracking(self, temp_db_tracker, test_extracted_directory_with_content):
        """Test JSONL extraction with database tracking."""
        # Get the actual barcode from the archive path

        barcode = Path(test_extracted_directory_with_content).name.replace("_extracted", "")
        session_id = "test_session_456"

        with tempfile.NamedTemporaryFile(suffix=".jsonl", delete=False) as f:
            output_path = f.name

        try:
            # Run JSONL extraction with tracking
            page_count = await extract_ocr_pages(
                test_extracted_directory_with_content,
                temp_db_tracker.db_path,
                session_id,
                output_file=output_path,
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

            await asyncio.sleep(0.1)

            # Verify tracking in database
            conn = sqlite3.connect(temp_db_tracker.db_path)
            cursor = conn.execute(
                """SELECT status_value, metadata FROM book_status_history
                   WHERE barcode = ? AND status_type = ? AND status_value = ?""",
                (barcode, TEXT_EXTRACTION_STATUS_TYPE, ExtractionStatus.COMPLETED.value),
            )
            record = cursor.fetchone()
            conn.close()

            assert record is not None
            metadata = json.loads(record[1])
            assert metadata["page_count"] == 6
            assert metadata["jsonl_file_size"] > 0
            assert metadata["output_path"] == output_path

        finally:
            Path(output_path).unlink(missing_ok=True)

    @pytest.mark.asyncio
    async def test_extraction_failure_tracking(self, temp_db_tracker, temp_jsonl_file):
        """Test that extraction failures are properly tracked in database."""
        # Create a nonexistent directory path for predictable tracking
        nonexistent_path = "/tmp/test_barcode_extracted"

        # Extract barcode the same way the function does
        barcode = Path(nonexistent_path).name.replace("_extracted", "")  # Should be "test_barcode"
        session_id = "test_session_789"

        # Try to extract from nonexistent directory
        with pytest.raises(TextExtractionError):
            await extract_ocr_pages(
                nonexistent_path,
                temp_db_tracker.db_path,
                session_id,
                output_file=temp_jsonl_file,
            )

        # Allow time for async tasks

        await asyncio.sleep(0.1)

        # Verify failure was tracked
        conn = sqlite3.connect(temp_db_tracker.db_path)
        cursor = conn.execute(
            """SELECT status_value, metadata FROM book_status_history
               WHERE barcode = ? AND status_type = ? AND status_value = ?""",
            (barcode, TEXT_EXTRACTION_STATUS_TYPE, ExtractionStatus.FAILED.value),
        )
        record = cursor.fetchone()
        conn.close()

        assert record is not None
        metadata = json.loads(record[1])
        assert "error_type" in metadata
        assert "error_message" in metadata
        assert metadata["partial_page_count"] == 0


class TestQueryFunctionsIntegration:
    """Test query functions with real database data."""

    @pytest.mark.asyncio
    async def test_status_summary_with_mixed_extractions(self, temp_db_tracker, test_extracted_directory_with_content, temp_jsonl_file):
        """Test status summary query with multiple extraction records."""
        # Perform several extractions with different outcomes

        # Successful extraction
        await extract_ocr_pages(
            test_extracted_directory_with_content,
            temp_db_tracker.db_path,
            "session1",
            output_file=temp_jsonl_file,
        )

        # Another successful extraction (different barcode)
        # We'll simulate this by directly adding to database since we need different barcodes
        status_updates = [collect_status(
            "book2",
            TEXT_EXTRACTION_STATUS_TYPE,
            ExtractionStatus.COMPLETED.value,
            metadata={"page_count": 100, "extraction_time_ms": 2000},
        )]
        await batch_write_status_updates(temp_db_tracker.db_path, status_updates)

        # Failed extraction
        status_updates = [collect_status(
            "book3",
            TEXT_EXTRACTION_STATUS_TYPE,
            ExtractionStatus.FAILED.value,
            metadata={"error_type": "CorruptedArchiveError", "error_message": "Archive damaged"},
        )]
        await batch_write_status_updates(temp_db_tracker.db_path, status_updates)

        # In-progress extraction
        status_updates = [collect_status(
            "book4",
            TEXT_EXTRACTION_STATUS_TYPE,
            ExtractionStatus.EXTRACTING.value,
            metadata={"page_count": 50},
        )]
        await batch_write_status_updates(temp_db_tracker.db_path, status_updates)

        # Allow time for async tasks

        await asyncio.sleep(0.1)

        # Test status summary
        summary = await get_status_summary(temp_db_tracker.db_path)

        assert summary[ExtractionStatus.COMPLETED.value] >= 2
        assert summary[ExtractionStatus.FAILED.value] >= 1
        assert summary[ExtractionStatus.EXTRACTING.value] >= 1
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
            status_updates = [collect_status(
                case["barcode"],
                TEXT_EXTRACTION_STATUS_TYPE,
                ExtractionStatus.FAILED.value,
                metadata=case,
            )]
            await batch_write_status_updates(temp_db_tracker.db_path, status_updates)

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
            status_updates = [collect_status(
                f"completed_book_{i}",
                TEXT_EXTRACTION_STATUS_TYPE,
                ExtractionStatus.COMPLETED.value,
                metadata=case,
            )]
            await batch_write_status_updates(temp_db_tracker.db_path, status_updates)

        # Add a failure for comprehensive stats
        status_updates = [collect_status(
            "failed_book",
            TEXT_EXTRACTION_STATUS_TYPE,
            ExtractionStatus.FAILED.value,
            metadata={"error_type": "TestError", "partial_page_count": 10},
        )]
        await batch_write_status_updates(temp_db_tracker.db_path, status_updates)

        # Get progress statistics
        progress = await get_extraction_progress(temp_db_tracker.db_path)

        # Verify statistics
        assert progress["total_pages_extracted"] == 450  # 150 + 200 + 100
        assert progress["avg_pages_per_book"] == 150.0  # 450 / 3
        assert progress["avg_extraction_time_ms"] == 3000.0  # (3000 + 4000 + 2000) / 3

        # Check status summary is included
        assert "status_summary" in progress
        assert progress["status_summary"][ExtractionStatus.COMPLETED.value] >= 3
        assert progress["status_summary"][ExtractionStatus.FAILED.value] >= 1

        # Check recent failures
        assert len(progress["recent_failures"]) >= 1
        assert progress["recent_failures"][0]["barcode"] == "failed_book"


class TestSessionTracking:
    """Test session-based tracking functionality."""

    @pytest.mark.asyncio
    async def test_session_tracking_isolation(self, temp_db_tracker, test_extracted_directory_with_content, temp_jsonl_file):
        """Test that session IDs properly isolate batch operations."""
        session1 = "batch_session_1"
        session2 = "batch_session_2"

        # Extract with different session IDs
        await extract_ocr_pages(
            test_extracted_directory_with_content,
            temp_db_tracker.db_path,
            session1,
            output_file=temp_jsonl_file,
        )

        # Add another book manually for session2
        status_updates = [collect_status(
            "book_session2",
            TEXT_EXTRACTION_STATUS_TYPE,
            ExtractionStatus.COMPLETED.value,
            session_id=session2,
            metadata={"page_count": 75, "extraction_time_ms": 1500},
        )]
        await batch_write_status_updates(temp_db_tracker.db_path, status_updates)

        # Allow time for async tasks

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
