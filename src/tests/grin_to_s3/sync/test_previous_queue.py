#!/usr/bin/env python3
"""
Tests for previous queue functionality.

Tests the logic for fetching books from the "previous" queue which includes
books with PREVIOUSLY_DOWNLOADED status filtered by in_process and verified_unavailable books.
"""

from unittest.mock import patch

import pytest

from grin_to_s3.collect_books.models import BookRecord, SQLiteProgressTracker
from grin_to_s3.queue_utils import get_previous_queue_books
from grin_to_s3.sync.pipeline import get_books_from_queue
from tests.test_utils.database_helpers import StatusUpdate
from tests.utils import batch_write_status_updates


async def add_book_with_grin_state(
    tracker: SQLiteProgressTracker, db_path: str, barcode: str, grin_state: str, sync_statuses: list[str] = None
) -> None:
    """Helper to add a book with given GRIN state and optional sync statuses."""
    book = BookRecord(
        barcode=barcode,
        title=f"Test Book {barcode}",
        grin_state=grin_state,
    )
    await tracker.save_book(book)

    # Add sync status history if provided
    if sync_statuses:
        status_updates = []
        for status in sync_statuses:
            status_updates.append(StatusUpdate(barcode, "sync", status))

        if status_updates:
            await batch_write_status_updates(db_path, status_updates)


@pytest.mark.asyncio
async def test_previous_queue_basic_filtering(mock_grin_client, db_tracker, temp_db):
    """Test basic previous queue filtering logic."""
    # Create test books with different GRIN states
    await add_book_with_grin_state(db_tracker, temp_db, "PREV001", "PREVIOUSLY_DOWNLOADED")
    await add_book_with_grin_state(db_tracker, temp_db, "PREV002", "PREVIOUSLY_DOWNLOADED")
    await add_book_with_grin_state(db_tracker, temp_db, "PREV003", "CONVERTED")  # Should not be included
    await add_book_with_grin_state(db_tracker, temp_db, "PREV004", "PREVIOUSLY_DOWNLOADED")

    # Mock in_process and verified_unavailable filtering
    with patch("grin_to_s3.sync.pipeline.get_in_process_set") as mock_in_process:
        mock_in_process.return_value = {"PREV002"}  # PREV002 is in process

        # PREV004 marked as verified_unavailable
        await add_book_with_grin_state(db_tracker, temp_db, "PREV004", "PREVIOUSLY_DOWNLOADED")
        # Add conversion unavailable status to match real system behavior
        status_updates = [StatusUpdate("PREV004", "conversion", "unavailable")]
        await batch_write_status_updates(temp_db, status_updates)

        # Get previous queue books
        result = await get_books_from_queue(mock_grin_client, "test_library", "previous", db_tracker)

    # Should only return PREV001 (PREV002 filtered by in_process, PREV003 wrong state, PREV004 unavailable)
    assert result == {"PREV001"}


@pytest.mark.asyncio
async def test_previous_queue_empty_sets(mock_grin_client, db_tracker, temp_db):
    """Test previous queue with various empty scenarios."""
    # No PREVIOUSLY_DOWNLOADED books
    await add_book_with_grin_state(db_tracker, temp_db, "CONV001", "CONVERTED")
    await add_book_with_grin_state(db_tracker, temp_db, "CONV002", "CONVERTED")

    with patch("grin_to_s3.sync.pipeline.get_in_process_set") as mock_in_process:
        mock_in_process.return_value = set()

        result = await get_books_from_queue(mock_grin_client, "test_library", "previous", db_tracker)

    assert result == set()


@pytest.mark.asyncio
async def test_previous_queue_all_filtered_out(mock_grin_client, db_tracker, temp_db):
    """Test previous queue where all books are filtered out."""
    # Create PREVIOUSLY_DOWNLOADED books
    await add_book_with_grin_state(db_tracker, temp_db, "PREV001", "PREVIOUSLY_DOWNLOADED")
    await add_book_with_grin_state(db_tracker, temp_db, "PREV002", "PREVIOUSLY_DOWNLOADED")

    # Mock all books being in process
    with patch("grin_to_s3.sync.pipeline.get_in_process_set") as mock_in_process:
        mock_in_process.return_value = {"PREV001", "PREV002"}

        result = await get_books_from_queue(mock_grin_client, "test_library", "previous", db_tracker)

    assert result == set()


@pytest.mark.asyncio
async def test_previous_queue_verified_unavailable_filtering(mock_grin_client, db_tracker, temp_db):
    """Test that verified_unavailable books are filtered out."""
    # Create test books
    await add_book_with_grin_state(db_tracker, temp_db, "PREV001", "PREVIOUSLY_DOWNLOADED")
    await add_book_with_grin_state(db_tracker, temp_db, "PREV002", "PREVIOUSLY_DOWNLOADED")
    await add_book_with_grin_state(db_tracker, temp_db, "PREV003", "PREVIOUSLY_DOWNLOADED")

    # Add conversion unavailable status to match real system behavior
    status_updates = [StatusUpdate("PREV002", "conversion", "unavailable")]
    await batch_write_status_updates(temp_db, status_updates)

    with patch("grin_to_s3.sync.pipeline.get_in_process_set") as mock_in_process:
        mock_in_process.return_value = set()

        result = await get_books_from_queue(mock_grin_client, "test_library", "previous", db_tracker)

    # PREV002 should be filtered out due to verified_unavailable status
    assert result == {"PREV001", "PREV003"}


@pytest.mark.asyncio
async def test_previous_queue_mixed_filtering(mock_grin_client, db_tracker, temp_db):
    """Test previous queue with both in_process and verified_unavailable filtering."""
    # Create mix of books
    await add_book_with_grin_state(db_tracker, temp_db, "PREV001", "PREVIOUSLY_DOWNLOADED")
    await add_book_with_grin_state(db_tracker, temp_db, "PREV002", "PREVIOUSLY_DOWNLOADED")
    await add_book_with_grin_state(db_tracker, temp_db, "PREV003", "PREVIOUSLY_DOWNLOADED")
    await add_book_with_grin_state(db_tracker, temp_db, "PREV004", "PREVIOUSLY_DOWNLOADED")
    await add_book_with_grin_state(db_tracker, temp_db, "PREV005", "CONVERTED")  # Wrong state

    # Add conversion unavailable status to match real system behavior
    status_updates = [StatusUpdate("PREV003", "conversion", "unavailable")]
    await batch_write_status_updates(temp_db, status_updates)

    with patch("grin_to_s3.sync.pipeline.get_in_process_set") as mock_in_process:
        # PREV002 and PREV004 are in process
        mock_in_process.return_value = {"PREV002", "PREV004"}

        result = await get_books_from_queue(mock_grin_client, "test_library", "previous", db_tracker)

    # Only PREV001 should remain (PREV002/PREV004 in_process, PREV003 unavailable, PREV005 wrong state)
    assert result == {"PREV001"}


@pytest.mark.asyncio
async def test_previous_queue_database_queries(mock_grin_client, db_tracker, temp_db):
    """Test that correct database queries are made for previous queue."""
    # Create test data
    await add_book_with_grin_state(db_tracker, temp_db, "PREV001", "PREVIOUSLY_DOWNLOADED")
    await add_book_with_grin_state(db_tracker, temp_db, "PREV002", "CONVERTED")
    await add_book_with_grin_state(db_tracker, temp_db, "PREV003", "PREVIOUSLY_DOWNLOADED")

    # Add conversion unavailable status to match real system behavior
    status_updates = [StatusUpdate("PREV003", "conversion", "unavailable")]
    await batch_write_status_updates(temp_db, status_updates)

    # Mock in_process set
    with patch("grin_to_s3.sync.pipeline.get_in_process_set") as mock_in_process:
        mock_in_process.return_value = set()

        # Mock the new get_previous_queue_books function
        with patch("grin_to_s3.sync.pipeline.get_previous_queue_books") as mock_previous_queue:
            mock_previous_queue.return_value = {"PREV001"}  # Only PREV001 should be returned

            result = await get_books_from_queue(mock_grin_client, "test_library", "previous", db_tracker)

            # Verify the new function was called with correct parameters
            mock_previous_queue.assert_called_once_with(db_tracker)

            # Result should be the filtered set
            assert result == {"PREV001"}


@pytest.mark.asyncio
async def test_get_books_by_grin_state_method(db_tracker, temp_db):
    """Test the get_books_by_grin_state method directly."""
    # Create books with different GRIN states
    await add_book_with_grin_state(db_tracker, temp_db, "PREV001", "PREVIOUSLY_DOWNLOADED")
    await add_book_with_grin_state(db_tracker, temp_db, "PREV002", "PREVIOUSLY_DOWNLOADED")
    await add_book_with_grin_state(db_tracker, temp_db, "CONV001", "CONVERTED")
    await add_book_with_grin_state(db_tracker, temp_db, "PROC001", "IN_PROCESS")

    # Test querying for PREVIOUSLY_DOWNLOADED
    result = await db_tracker.get_books_by_grin_state("PREVIOUSLY_DOWNLOADED")
    assert result == {"PREV001", "PREV002"}

    # Test querying for CONVERTED
    result = await db_tracker.get_books_by_grin_state("CONVERTED")
    assert result == {"CONV001"}

    # Test querying for non-existent state
    result = await db_tracker.get_books_by_grin_state("NON_EXISTENT")
    assert result == set()


@pytest.mark.asyncio
async def test_get_books_with_status_method(db_tracker, temp_db):
    """Test the get_books_with_status method directly."""
    # Create books with different sync statuses
    await add_book_with_grin_state(db_tracker, temp_db, "BOOK001", "CONVERTED", ["completed"])
    await add_book_with_grin_state(db_tracker, temp_db, "BOOK002", "CONVERTED", ["failed"])
    # Add books with conversion unavailable status to match real system behavior
    await add_book_with_grin_state(db_tracker, temp_db, "BOOK003", "CONVERTED")
    await add_book_with_grin_state(db_tracker, temp_db, "BOOK004", "CONVERTED")

    # Add conversion unavailable status records
    status_updates = [
        StatusUpdate("BOOK003", "conversion", "unavailable"),
        StatusUpdate("BOOK004", "conversion", "unavailable"),
    ]
    await batch_write_status_updates(temp_db, status_updates)

    # Test querying for verified_unavailable status
    result = await db_tracker.get_books_with_status("unavailable", "conversion")
    assert result == {"BOOK003", "BOOK004"}

    # Test querying for failed status
    result = await db_tracker.get_books_with_status("failed")
    assert result == {"BOOK002"}

    # Test querying for non-existent status
    result = await db_tracker.get_books_with_status("non_existent")
    assert result == set()


@pytest.mark.asyncio
async def test_previous_queue_caching_behavior(mock_grin_client, db_tracker, temp_db):
    """Test that in_process caching doesn't interfere with previous queue logic."""
    await add_book_with_grin_state(db_tracker, temp_db, "PREV001", "PREVIOUSLY_DOWNLOADED")
    await add_book_with_grin_state(db_tracker, temp_db, "PREV002", "PREVIOUSLY_DOWNLOADED")

    # First call should cache the in_process set
    with patch("grin_to_s3.sync.pipeline.get_in_process_set") as mock_in_process:
        mock_in_process.return_value = {"PREV001"}

        result1 = await get_books_from_queue(mock_grin_client, "test_library", "previous", db_tracker)

        # Second call should use cached data
        result2 = await get_books_from_queue(mock_grin_client, "test_library", "previous", db_tracker)

    # Results should be consistent
    assert result1 == {"PREV002"}
    assert result2 == {"PREV002"}

    # get_in_process_set should be called for both since each call is independent
    # (caching is within get_in_process_set itself, not at the queue level)
    assert mock_in_process.call_count == 2


@pytest.mark.asyncio
async def test_get_previous_queue_books_filtering(db_tracker, temp_db):
    """Test the get_previous_queue_books function directly."""
    # Create books with different states and processing status
    await add_book_with_grin_state(db_tracker, temp_db, "PREV001", "PREVIOUSLY_DOWNLOADED")  # Should be included
    await add_book_with_grin_state(
        db_tracker, temp_db, "PREV002", "PREVIOUSLY_DOWNLOADED"
    )  # Should be excluded (has processing_request_timestamp)
    await add_book_with_grin_state(
        db_tracker, temp_db, "PREV003", "PREVIOUSLY_DOWNLOADED"
    )  # Should be excluded (unavailable)
    await add_book_with_grin_state(db_tracker, temp_db, "PREV004", "CONVERTED")  # Should be excluded (wrong state)

    # Add processing_request_timestamp to PREV002
    book_with_timestamp = BookRecord(
        barcode="PREV002",
        title="Test Book PREV002",
        grin_state="PREVIOUSLY_DOWNLOADED",
        processing_request_timestamp="2024-01-01T00:00:00Z",
    )
    await db_tracker.save_book(book_with_timestamp, refresh_mode=True)

    # Mark PREV003 as unavailable
    status_updates = [StatusUpdate("PREV003", "conversion", "unavailable")]
    await batch_write_status_updates(temp_db, status_updates)

    # Test the function
    result = await get_previous_queue_books(db_tracker)

    # Only PREV001 should be returned
    assert result == {"PREV001"}


@pytest.mark.asyncio
async def test_get_previous_queue_books_excludes_conversion_failed(db_tracker, temp_db):
    """get_previous_queue_books should exclude books marked as conversion_failed."""
    # Create books with different states
    await add_book_with_grin_state(db_tracker, temp_db, "PREV001", "PREVIOUSLY_DOWNLOADED")  # Should be included
    await add_book_with_grin_state(
        db_tracker, temp_db, "PREV002", "PREVIOUSLY_DOWNLOADED"
    )  # Should be excluded (conversion_failed)
    await add_book_with_grin_state(
        db_tracker, temp_db, "PREV003", "PREVIOUSLY_DOWNLOADED"
    )  # Should be excluded (unavailable)
    await add_book_with_grin_state(db_tracker, temp_db, "PREV004", "PREVIOUSLY_DOWNLOADED")  # Should be included

    # Mark PREV002 as conversion_failed
    status_updates = [
        StatusUpdate("PREV002", "conversion", "conversion_failed"),
        StatusUpdate("PREV003", "conversion", "unavailable"),
    ]
    await batch_write_status_updates(temp_db, status_updates)

    # Test the function
    result = await get_previous_queue_books(db_tracker)

    # Only PREV001 and PREV004 should be returned (PREV002 and PREV003 excluded)
    assert result == {"PREV001", "PREV004"}
