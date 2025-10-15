#!/usr/bin/env python3
"""
Tests for unconverted queue functionality.

Tests the logic for fetching books from the "unconverted" queue which includes
books that have never been requested for processing or converted by GRIN,
filtered by in_process and verified_unavailable books.
"""

from datetime import UTC, datetime
from unittest.mock import patch

import pytest

from grin_transfer.collect_books.models import BookRecord, SQLiteProgressTracker
from grin_transfer.queue_utils import get_unconverted_books
from grin_transfer.sync.pipeline import get_books_from_queue
from tests.test_utils.database_helpers import StatusUpdate
from tests.utils import batch_write_status_updates


async def insert_book(
    tracker: SQLiteProgressTracker,
    barcode: str,
    processing_request_timestamp: str | None = None,
    converted_date: str | None = None,
    grin_check_in_date: str | None = None,
    grin_state: str | None = None,
) -> None:
    """Helper to insert a book with specified processing, conversion, and check-in status."""
    now = datetime.now(UTC).isoformat()
    book = BookRecord(
        barcode=barcode,
        processing_request_timestamp=processing_request_timestamp,
        converted_date=converted_date,
        grin_check_in_date=grin_check_in_date,
        grin_state=grin_state,
        created_at=now,
        updated_at=now,
    )

    # Use the full insert SQL to include all fields
    await tracker.init_db()
    conn = await tracker.get_connection()
    await conn.execute(BookRecord.build_insert_sql(), book.to_tuple())
    await conn.commit()


@pytest.mark.asyncio
async def test_get_unconverted_books_basic(db_tracker):
    """get_unconverted_books should return books with neither processing timestamp nor converted date"""
    # Insert test books
    await insert_book(db_tracker, "book1")  # Neither timestamp - should be returned
    await insert_book(
        db_tracker, "book2", processing_request_timestamp="2024-01-01T00:00:00Z"
    )  # Has processing timestamp - should NOT be returned
    await insert_book(
        db_tracker, "book3", converted_date="2024-01-01T00:00:00Z"
    )  # Has converted date - should NOT be returned
    await insert_book(
        db_tracker, "book4", processing_request_timestamp="2024-01-01T00:00:00Z", converted_date="2024-01-01T00:00:00Z"
    )  # Has both - should NOT be returned

    result = await get_unconverted_books(db_tracker)

    assert result == {"book1"}


@pytest.mark.asyncio
async def test_get_unconverted_books_empty_database(db_tracker):
    """get_unconverted_books should return empty set when no books exist"""
    result = await get_unconverted_books(db_tracker)
    assert result == set()


@pytest.mark.asyncio
async def test_get_unconverted_books_no_unconverted(db_tracker):
    """get_unconverted_books should return empty set when all books have processing timestamps or conversion dates"""
    # Insert books that all have either processing timestamp or converted date
    await insert_book(db_tracker, "book1", processing_request_timestamp="2024-01-01T00:00:00Z")
    await insert_book(db_tracker, "book2", converted_date="2024-01-01T00:00:00Z")
    await insert_book(
        db_tracker, "book3", processing_request_timestamp="2024-01-01T00:00:00Z", converted_date="2024-01-01T00:00:00Z"
    )

    result = await get_unconverted_books(db_tracker)
    assert result == set()


@pytest.mark.asyncio
async def test_get_unconverted_books_excludes_checked_in(db_tracker):
    """get_unconverted_books should exclude books that are checked in or not available for download"""
    # Insert test books
    await insert_book(db_tracker, "book1")  # Neither timestamp nor checked in - should be returned
    await insert_book(
        db_tracker, "book2", grin_check_in_date="2024-01-01T00:00:00Z"
    )  # Has check-in date - should NOT be returned
    await insert_book(db_tracker, "book3", grin_state="CHECKED_IN")  # Has CHECKED_IN state - should NOT be returned
    await insert_book(
        db_tracker, "book4", grin_state="NOT_AVAILABLE_FOR_DOWNLOAD"
    )  # Has NOT_AVAILABLE_FOR_DOWNLOAD state - should NOT be returned
    await insert_book(
        db_tracker, "book5", grin_check_in_date="2024-01-01T00:00:00Z", grin_state="CHECKED_IN"
    )  # Has both - should NOT be returned

    result = await get_unconverted_books(db_tracker)

    assert result == {"book1"}


@pytest.mark.asyncio
async def test_unconverted_queue_basic_filtering(mock_grin_client, db_tracker):
    """Unconverted queue should filter out in_process books"""
    # Insert test books
    await insert_book(db_tracker, "book1")  # Unconverted - should be returned
    await insert_book(db_tracker, "book2")  # Unconverted but in process - should be filtered out
    await insert_book(db_tracker, "book3")  # Unconverted - should be returned
    await insert_book(
        db_tracker, "book4", processing_request_timestamp="2024-01-01T00:00:00Z"
    )  # Has processing timestamp - not unconverted

    with patch("grin_transfer.sync.pipeline.get_in_process_set") as mock_get_in_process:
        mock_get_in_process.return_value = {"book2"}
        result = await get_books_from_queue(mock_grin_client, "test_library", "unconverted", db_tracker)

    # book1 and book3 should remain after filtering (book2 filtered out by in_process, book4 not unconverted)
    assert result == {"book1", "book3"}


@pytest.mark.asyncio
async def test_unconverted_queue_empty_sets(mock_grin_client, db_tracker):
    """Unconverted queue should handle empty in_process sets"""
    # Insert test books
    await insert_book(db_tracker, "book1")
    await insert_book(db_tracker, "book2")

    with patch("grin_transfer.sync.pipeline.get_in_process_set") as mock_get_in_process:
        mock_get_in_process.return_value = set()
        result = await get_books_from_queue(mock_grin_client, "test_library", "unconverted", db_tracker)

    # Both books should be returned since nothing filters them out
    assert result == {"book1", "book2"}


@pytest.mark.asyncio
async def test_unconverted_queue_all_filtered_out(mock_grin_client, db_tracker):
    """Unconverted queue should return empty set when all books are filtered out"""
    # Insert test books that will all be filtered out
    await insert_book(db_tracker, "book1")  # Will be in process
    await insert_book(db_tracker, "book2")  # Will be in process

    with patch("grin_transfer.sync.pipeline.get_in_process_set") as mock_get_in_process:
        mock_get_in_process.return_value = {"book1", "book2"}
        result = await get_books_from_queue(mock_grin_client, "test_library", "unconverted", db_tracker)

    # No books should remain after filtering
    assert result == set()


@pytest.mark.asyncio
async def test_get_unconverted_books_excludes_conversion_failed(db_tracker, temp_db):
    """get_unconverted_books should exclude books marked as conversion_failed."""
    # Insert test books
    await insert_book(db_tracker, "book1")  # Should be included
    await insert_book(db_tracker, "book2")  # Should be excluded (conversion_failed)
    await insert_book(db_tracker, "book3")  # Should be excluded (unavailable)
    await insert_book(db_tracker, "book4")  # Should be included

    # Mark book2 as conversion_failed and book3 as unavailable
    status_updates = [
        StatusUpdate("book2", "conversion", "conversion_failed"),
        StatusUpdate("book3", "conversion", "unavailable"),
    ]
    await batch_write_status_updates(temp_db, status_updates)

    # Test the function
    result = await get_unconverted_books(db_tracker)

    # Only book1 and book4 should be returned (book2 and book3 excluded)
    assert result == {"book1", "book4"}
