#!/usr/bin/env python3
"""
Tests for sync tasks extract_marc module.
"""

import tempfile
from pathlib import Path

import pytest

from grin_to_s3.collect_books.models import BookRecord, SQLiteProgressTracker
from grin_to_s3.database import connect_async
from grin_to_s3.sync.tasks import extract_marc
from grin_to_s3.sync.tasks.task_types import TaskAction, UnpackData


@pytest.mark.asyncio
async def test_main_successful_extraction(mock_pipeline):
    """Extract MARC task should complete successfully."""
    from tests.test_utils.unified_mocks import mock_extract_marc_operations

    unpack_data: UnpackData = {
        "unpacked_path": Path("/tmp/TEST123"),
    }

    marc_metadata = {"title": "Test Book", "author": "Test Author"}
    normalized_metadata = {"title_display": "Test Book", "author_display": "Test Author"}

    with mock_extract_marc_operations(marc_metadata=marc_metadata, normalized_metadata=normalized_metadata):
        result = await extract_marc.main("TEST123", unpack_data, mock_pipeline)

        assert result.action == TaskAction.COMPLETED
        assert result.data
        assert result.data["marc_metadata"] == marc_metadata
        mock_pipeline.db_tracker.update_book_marc_metadata.assert_called_once_with("TEST123", normalized_metadata)


@pytest.mark.asyncio
async def test_extract_marc_calls_extraction_with_path(mock_pipeline):
    """Extract MARC should call extraction function with unpacked path."""
    from tests.test_utils.unified_mocks import mock_extract_marc_operations

    unpack_data: UnpackData = {
        "unpacked_path": Path("/tmp/TEST123"),
    }

    with mock_extract_marc_operations(
        marc_metadata={"test": "data"}, normalized_metadata={"converted": "data"}
    ) as mocks:
        await extract_marc.main("TEST123", unpack_data, mock_pipeline)

        mocks.extract_marc_metadata.assert_called_once_with(unpack_data["unpacked_path"])


@pytest.mark.asyncio
async def test_handles_empty_metadata(mock_pipeline):
    """Extract MARC should fail when no metadata found."""
    from tests.test_utils.unified_mocks import mock_extract_marc_operations

    unpack_data: UnpackData = {
        "unpacked_path": Path("/tmp/TEST123"),
    }

    with mock_extract_marc_operations(marc_metadata={}, normalized_metadata={}):
        result = await extract_marc.main("TEST123", unpack_data, mock_pipeline)

        assert result.action == TaskAction.FAILED
        assert result.reason == "fail_no_marc_metadata"
        assert result.data
        assert result.data["marc_metadata"] == {}
        mock_pipeline.db_tracker.update_book_marc_metadata.assert_not_called()


@pytest.mark.asyncio
async def test_marc_data_written_to_real_database():
    """Integration test: MARC data should be written to real SQLite database."""
    unpack_data: UnpackData = {
        "unpacked_path": Path("/tmp/TEST123"),
    }

    # Create temporary database
    with tempfile.NamedTemporaryFile(suffix=".db", delete=False) as f:
        db_path = f.name

    # Initialize real database tracker
    tracker = SQLiteProgressTracker(db_path=db_path)
    await tracker.init_db()

    # Create mock pipeline with real database tracker
    pipeline = object.__new__(type("MockPipeline", (), {}))
    pipeline.db_tracker = tracker  # type: ignore

    try:
        from tests.test_utils.unified_mocks import mock_extract_marc_operations

        # Setup test data
        marc_metadata = {
            "title": "Test Book Title",
            "author": "Test Author",
            "publisher": "Test Publisher",
            "isbn": "978-0123456789",
        }
        normalized_metadata = {
            "marc_title": "Test Book Title",
            "marc_author_personal": "Test Author",
            "marc_isbn": "978-0123456789",
        }

        with mock_extract_marc_operations(marc_metadata=marc_metadata, normalized_metadata=normalized_metadata):
            # First ensure the book exists in the database
            book = BookRecord(barcode="TEST123", title="Test Book")
            await tracker.save_book(book)

            # Run the extract_marc task
            result = await extract_marc.main("TEST123", unpack_data, pipeline)  # type: ignore

            # Verify task completed successfully
            assert result.action == TaskAction.COMPLETED
            assert result.data
            assert result.data["marc_metadata"] == marc_metadata

            # Verify MARC data was actually written to database
            async with connect_async(tracker.db_path) as db:
                cursor = await db.execute(
                    "SELECT marc_title, marc_author_personal, marc_isbn FROM books WHERE barcode = ?", ("TEST123",)
                )
                row = await cursor.fetchone()

            assert row is not None
            assert row[0] == "Test Book Title"  # marc_title
            assert row[1] == "Test Author"  # marc_author_personal
            assert row[2] == "978-0123456789"  # marc_isbn

    finally:
        # Cleanup
        await tracker.close()
        Path(db_path).unlink(missing_ok=True)


@pytest.mark.asyncio
async def test_extract_marc_includes_field_count(mock_pipeline):
    """Extract MARC task should include field_count in result data."""
    from tests.test_utils.unified_mocks import mock_extract_marc_operations

    unpack_data: UnpackData = {
        "unpacked_path": Path("/tmp/TEST123"),
    }

    marc_metadata = {
        "title": "Test Book",
        "author": "Test Author",
        "publisher": "Test Publisher",
        "isbn": "978-0123456789",
        "language": "en",
    }
    normalized_metadata = {"title_display": "Test Book", "author_display": "Test Author"}

    with mock_extract_marc_operations(marc_metadata=marc_metadata, normalized_metadata=normalized_metadata):
        result = await extract_marc.main("TEST123", unpack_data, mock_pipeline)

        assert result.action == TaskAction.COMPLETED
        assert result.data
        assert result.data["marc_metadata"] == marc_metadata
        assert result.data["field_count"] == 5  # 5 fields in marc_metadata
        mock_pipeline.db_tracker.update_book_marc_metadata.assert_called_once_with("TEST123", normalized_metadata)
