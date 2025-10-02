#!/usr/bin/env python3
"""
Unit tests for status history system.

Tests the atomic status tracking functionality including:
- Latest status retrieval
- Status history queries
- Batched status updates integration
"""

import asyncio
import json
from datetime import UTC, datetime

import aiosqlite
import pytest

from grin_to_s3.collect_books.models import BookRecord
from tests.test_utils.database_helpers import (
    StatusUpdate,
    get_all_barcodes_for_testing,
    get_books_with_latest_status_for_testing,
    get_latest_status_for_testing,
)
from tests.utils import batch_write_status_updates


@pytest.mark.asyncio
async def test_batch_status_change_basic(db_tracker, temp_db):
    """Test basic batched status change recording."""
    barcode = "TEST001"

    # Add initial book record
    book = BookRecord(
        barcode=barcode,
        title="Test Book",
        created_at=datetime.now(UTC).isoformat(),
        updated_at=datetime.now(UTC).isoformat(),
    )
    await db_tracker.save_book(book)

    # Add status change using batched approach
    status_updates = [StatusUpdate(barcode, "processing_request", "requested")]
    await batch_write_status_updates(temp_db, status_updates)

    # Verify status was recorded
    latest_status = await get_latest_status_for_testing(db_tracker, barcode, "processing_request")
    assert latest_status == "requested"


@pytest.mark.asyncio
async def test_batch_status_change_with_metadata(db_tracker, temp_db):
    """Test batched status change with metadata."""
    barcode = "TEST002"

    # Add initial book record
    book = BookRecord(
        barcode=barcode,
        title="Test Book 2",
        created_at=datetime.now(UTC).isoformat(),
        updated_at=datetime.now(UTC).isoformat(),
    )
    await db_tracker.save_book(book)

    # Add status change with metadata using batched approach
    metadata = {"batch_id": "batch_001", "retry_count": 1}
    status_updates = [
        StatusUpdate(barcode, "processing_request", "requested", metadata=metadata, session_id="session_123")
    ]
    await batch_write_status_updates(temp_db, status_updates)

    # Verify the metadata was stored correctly
    async with aiosqlite.connect(temp_db) as db:
        cursor = await db.execute(
            """
            SELECT metadata, session_id FROM book_status_history
            WHERE barcode = ? AND status_type = ?
            """,
            (barcode, "processing_request"),
        )
        row = await cursor.fetchone()

    assert row is not None
    stored_metadata = json.loads(row[0])
    assert stored_metadata == metadata
    assert row[1] == "session_123"


@pytest.mark.asyncio
async def test_status_progression(db_tracker, temp_db):
    """Test multiple status changes over time."""
    barcode = "TEST003"

    # Add initial book record
    book = BookRecord(
        barcode=barcode,
        title="Test Book 3",
        created_at=datetime.now(UTC).isoformat(),
        updated_at=datetime.now(UTC).isoformat(),
    )
    await db_tracker.save_book(book)

    # Add sequence of status changes
    statuses = ["requested", "in_process", "converted"]

    # Add sequence of status changes using batched approach
    status_updates = []
    for status in statuses:
        status_updates.append(StatusUpdate(barcode, "processing_request", status))
        # Small delay to ensure different timestamps
        await asyncio.sleep(0.001)

    await batch_write_status_updates(temp_db, status_updates)

    # Verify latest status is correct
    latest_status = await get_latest_status_for_testing(db_tracker, barcode, "processing_request")
    assert latest_status == "converted"

    # Verify all history is preserved
    async with aiosqlite.connect(temp_db) as db:
        cursor = await db.execute(
            """
            SELECT status_value FROM book_status_history
            WHERE barcode = ? AND status_type = ?
            ORDER BY timestamp ASC, id ASC
            """,
            (barcode, "processing_request"),
        )
        rows = await cursor.fetchall()

    recorded_statuses = [row[0] for row in rows]
    assert recorded_statuses == statuses


@pytest.mark.asyncio
async def test_multiple_status_types(db_tracker, temp_db):
    """Test different status types for same book."""
    barcode = "TEST004"

    # Add initial book record
    book = BookRecord(
        barcode=barcode,
        title="Test Book 4",
        created_at=datetime.now(UTC).isoformat(),
        updated_at=datetime.now(UTC).isoformat(),
    )
    await db_tracker.save_book(book)

    # Add different types of status changes using batched approach
    status_updates = [
        StatusUpdate(barcode, "processing_request", "requested"),
        StatusUpdate(barcode, "sync", "pending"),
        StatusUpdate(barcode, "enrichment", "completed"),
    ]
    await batch_write_status_updates(temp_db, status_updates)

    # Verify each status type has correct latest value
    processing_status = await get_latest_status_for_testing(db_tracker, barcode, "processing_request")
    sync_status = await get_latest_status_for_testing(db_tracker, barcode, "sync")
    enrichment_status = await get_latest_status_for_testing(db_tracker, barcode, "enrichment")

    assert processing_status == "requested"
    assert sync_status == "pending"
    assert enrichment_status == "completed"


@pytest.mark.asyncio
async def test_get_books_with_latest_status(db_tracker, temp_db):
    """Test querying books by latest status."""
    # Create multiple books with different statuses
    books = [
        ("BOOK001", "requested"),
        ("BOOK002", "in_process"),
        ("BOOK003", "converted"),
        ("BOOK004", "requested"),
        ("BOOK005", "failed"),
    ]

    for barcode, status in books:
        # Add book record
        book = BookRecord(
            barcode=barcode,
            title=f"Test Book {barcode}",
            created_at=datetime.now(UTC).isoformat(),
            updated_at=datetime.now(UTC).isoformat(),
        )
        await db_tracker.save_book(book)

        # Add status using batched approach
        status_updates = [StatusUpdate(barcode, "processing_request", status)]
        await batch_write_status_updates(temp_db, status_updates)

    # Query books with specific statuses
    requested_books = await get_books_with_latest_status_for_testing(
        db_tracker, status_type="processing_request", status_values=["requested"]
    )

    processing_books = await get_books_with_latest_status_for_testing(
        db_tracker, status_type="processing_request", status_values=["in_process", "converted"]
    )

    all_books = await get_books_with_latest_status_for_testing(db_tracker, status_type="processing_request")

    # Verify results
    requested_barcodes = [book[0] for book in requested_books]
    assert "BOOK001" in requested_barcodes
    assert "BOOK004" in requested_barcodes
    assert len(requested_books) == 2

    processing_barcodes = [book[0] for book in processing_books]
    assert "BOOK002" in processing_barcodes
    assert "BOOK003" in processing_barcodes
    assert len(processing_books) == 2

    assert len(all_books) == 5


@pytest.mark.asyncio
async def test_backwards_compatibility(db_tracker, temp_db):
    """Test that status changes update legacy fields."""
    barcode = "TEST005"

    # Add initial book record
    book = BookRecord(
        barcode=barcode,
        title="Test Book 5",
        created_at=datetime.now(UTC).isoformat(),
        updated_at=datetime.now(UTC).isoformat(),
    )
    await db_tracker.save_book(book)

    # Add processing_request status change using batched approach
    status_updates = [StatusUpdate(barcode, "processing_request", "converted")]
    await batch_write_status_updates(temp_db, status_updates)

    # Status changes only update status history now
    # Verify processing status is in history
    processing_status = await get_latest_status_for_testing(db_tracker, barcode, "processing_request")
    assert processing_status == "converted"

    # Add sync status change using batched approach
    status_updates = [StatusUpdate(barcode, "sync", "completed")]
    await batch_write_status_updates(temp_db, status_updates)

    # Verify sync status is in history
    sync_status = await get_latest_status_for_testing(db_tracker, barcode, "sync")
    assert sync_status == "completed"


@pytest.mark.asyncio
async def test_get_latest_status_nonexistent(db_tracker):
    """Test getting status for non-existent book/status type."""
    result = await get_latest_status_for_testing(db_tracker, "NONEXISTENT", "processing_request")
    assert result is None


@pytest.mark.asyncio
async def test_status_change_order_consistency(db_tracker, temp_db):
    """Test that status changes are ordered correctly by timestamp and ID."""
    barcode = "TEST006"

    # Add initial book record
    book = BookRecord(
        barcode=barcode,
        title="Test Book 6",
        created_at=datetime.now(UTC).isoformat(),
        updated_at=datetime.now(UTC).isoformat(),
    )
    await db_tracker.save_book(book)

    # Add multiple status changes using batched approach
    status_updates = [
        StatusUpdate(barcode, "processing_request", "requested"),
        StatusUpdate(barcode, "processing_request", "in_process"),
        StatusUpdate(barcode, "processing_request", "converted"),
    ]
    await batch_write_status_updates(temp_db, status_updates)

    # The latest status should be "converted" (last added)
    latest_status = await get_latest_status_for_testing(db_tracker, barcode, "processing_request")
    assert latest_status == "converted"


@pytest.mark.asyncio
async def test_get_books_for_sync_with_status_history(db_tracker, temp_db):
    """Test that get_books_for_sync works with status history."""
    # Create books with different statuses in history
    test_books = [
        ("SYNC001", "converted"),
        ("SYNC002", "in_process"),
        ("SYNC003", "requested"),
        ("SYNC004", "failed"),
    ]

    for barcode, final_status in test_books:
        # Add book record
        book = BookRecord(
            barcode=barcode,
            title=f"Sync Test {barcode}",
            created_at=datetime.now(UTC).isoformat(),
            updated_at=datetime.now(UTC).isoformat(),
        )
        await db_tracker.save_book(book)

        # Add progression: requested -> (in_process) -> final_status
        status_updates = [StatusUpdate(barcode, "processing_request", "requested")]

        if final_status != "requested":
            if final_status in ("converted", "in_process"):
                status_updates.append(StatusUpdate(barcode, "processing_request", "in_process"))

            if final_status == "converted":
                status_updates.append(StatusUpdate(barcode, "processing_request", "converted"))
            elif final_status == "failed":
                status_updates.append(StatusUpdate(barcode, "processing_request", "failed"))

        await batch_write_status_updates(temp_db, status_updates)

    # Test get_books_for_sync - should find books that have been requested
    # and are in valid processing states (excludes failed books)
    sync_books = await get_all_barcodes_for_testing(db_tracker, limit=10)

    sync_barcodes = set(sync_books)
    expected_barcodes = {"SYNC001", "SYNC002", "SYNC003", "SYNC004"}
    assert sync_barcodes == expected_barcodes


@pytest.mark.asyncio
async def test_limit_functionality(db_tracker, temp_db):
    """Test limit parameter in get_books_with_latest_status."""
    # Create multiple books
    for i in range(10):
        barcode = f"LIMIT{i:03d}"
        book = BookRecord(
            barcode=barcode,
            title=f"Limit Test {barcode}",
            created_at=datetime.now(UTC).isoformat(),
            updated_at=datetime.now(UTC).isoformat(),
        )
        await db_tracker.save_book(book)

        status_updates = [StatusUpdate(barcode, "processing_request", "requested")]
        await batch_write_status_updates(temp_db, status_updates)

    # Test with limit
    limited_books = await get_books_with_latest_status_for_testing(
        db_tracker, status_type="processing_request", status_values=["requested"], limit=5
    )

    assert len(limited_books) == 5

    # Test without limit
    all_books = await get_books_with_latest_status_for_testing(
        db_tracker, status_type="processing_request", status_values=["requested"]
    )

    assert len(all_books) == 10
