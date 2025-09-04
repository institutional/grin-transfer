#!/usr/bin/env python3
"""
Integration tests for sync pipeline with status history.

Tests that the sync pipeline correctly works with the new status history system.
"""

import tempfile
from datetime import UTC, datetime
from pathlib import Path
from typing import NamedTuple
from unittest import IsolatedAsyncioTestCase

from grin_to_s3.collect_books.models import BookRecord, SQLiteProgressTracker
from grin_to_s3.database.database_utils import batch_write_status_updates


class StatusUpdate(NamedTuple):
    """Status update tuple for collecting updates before writing."""

    barcode: str
    status_type: str
    status_value: str
    metadata: dict | None = None
    session_id: str | None = None


class TestSyncStatusIntegration(IsolatedAsyncioTestCase):
    """Test sync pipeline integration with status history."""

    async def asyncSetUp(self):
        """Set up test database."""
        self.temp_dir = tempfile.TemporaryDirectory()
        self.db_path = Path(self.temp_dir.name) / "test_books.db"
        self.tracker = SQLiteProgressTracker(str(self.db_path))
        await self.tracker.init_db()

    async def asyncTearDown(self):
        """Clean up test database."""
        self.temp_dir.cleanup()

    async def test_get_books_for_sync_with_converted_status(self):
        """Test that books with 'converted' status are eligible for sync."""
        # Create test books with different status progressions
        test_data = [
            ("SYNC001", ["requested", "in_process", "converted"]),
            ("SYNC002", ["requested", "in_process"]),
            ("SYNC003", ["requested", "failed"]),
            ("SYNC004", ["requested"]),
        ]

        for barcode, status_progression in test_data:
            # Add book record
            book = BookRecord(
                barcode=barcode,
                title=f"Test Book {barcode}",
                processing_request_timestamp=datetime.now(UTC).isoformat(),
                created_at=datetime.now(UTC).isoformat(),
                updated_at=datetime.now(UTC).isoformat(),
            )
            await self.tracker.save_book(book)

            # Add status progression using batched approach
            status_updates = [StatusUpdate(barcode, "processing_request", status) for status in status_progression]
            await batch_write_status_updates(str(self.db_path), status_updates)

        # Test get_books_for_sync - should include books with valid processing states
        sync_books = await self.tracker.get_books_for_sync(limit=10)

        sync_barcodes = set(sync_books)
        expected_barcodes = {"SYNC001", "SYNC002", "SYNC003", "SYNC004"}
        self.assertEqual(sync_barcodes, expected_barcodes)

    async def test_get_books_for_sync_with_converted_filter(self):
        """Test get_books_for_sync with converted_barcodes filter."""
        # Create test books
        test_books = ["FILT001", "FILT002", "FILT003", "FILT004"]

        for barcode in test_books:
            # Add book record
            book = BookRecord(
                barcode=barcode,
                title=f"Test Book {barcode}",
                processing_request_timestamp=datetime.now(UTC).isoformat(),
                created_at=datetime.now(UTC).isoformat(),
                updated_at=datetime.now(UTC).isoformat(),
            )
            await self.tracker.save_book(book)

            # Add processing status using batched approach
            status_updates = [StatusUpdate(barcode, "processing_request", "converted")]
            await batch_write_status_updates(str(self.db_path), status_updates)

        # Test with converted_barcodes filter (simulating GRIN's converted list)
        converted_barcodes = {"FILT001", "FILT003"}  # Only some books are actually converted

        sync_books = await self.tracker.get_books_for_sync(limit=10, converted_barcodes=converted_barcodes)

        # Only books in the converted_barcodes set should be returned
        sync_barcodes = set(sync_books)
        self.assertEqual(sync_barcodes, converted_barcodes)

    async def test_get_books_for_sync_excludes_non_requested(self):
        """Test that books without processing history are not included."""
        # Create a book without any processing request status
        barcode = "NOREQ001"
        book = BookRecord(
            barcode=barcode,
            title=f"Test Book {barcode}",
            created_at=datetime.now(UTC).isoformat(),
            updated_at=datetime.now(UTC).isoformat(),
        )
        await self.tracker.save_book(book)
        # Note: No processing_request status added

        # Create a book with processing request status
        barcode2 = "HASREQ001"
        book2 = BookRecord(
            barcode=barcode2,
            title=f"Test Book {barcode2}",
            processing_request_timestamp=datetime.now(UTC).isoformat(),
            created_at=datetime.now(UTC).isoformat(),
            updated_at=datetime.now(UTC).isoformat(),
        )
        await self.tracker.save_book(book2)

        status_updates = [StatusUpdate(barcode2, "processing_request", "requested")]
        await batch_write_status_updates(str(self.db_path), status_updates)

        # Test get_books_for_sync
        sync_books = await self.tracker.get_books_for_sync(limit=10)

        self.assertEqual(set(sync_books), {barcode, barcode2})

    async def test_sync_status_tracking(self):
        """Test that sync status changes are tracked properly."""
        barcode = "SYNCSTAT001"

        # Add book record
        book = BookRecord(
            barcode=barcode,
            title=f"Test Book {barcode}",
            processing_request_timestamp=datetime.now(UTC).isoformat(),
            created_at=datetime.now(UTC).isoformat(),
            updated_at=datetime.now(UTC).isoformat(),
        )
        await self.tracker.save_book(book)

        # Add processing status using batched approach
        status_updates = [StatusUpdate(barcode, "processing_request", "converted")]
        await batch_write_status_updates(str(self.db_path), status_updates)

        # Add sync status changes using batched approach
        sync_statuses = ["pending", "syncing", "completed"]
        sync_status_updates = [StatusUpdate(barcode, "sync", status) for status in sync_statuses]
        await batch_write_status_updates(str(self.db_path), sync_status_updates)

        # Verify latest sync status
        latest_sync_status = await self.tracker.get_latest_status(barcode, "sync")
        self.assertEqual(latest_sync_status, "completed")

    async def test_get_books_for_sync_with_status_filter(self):
        """Test get_books_for_sync with sync status filter."""
        # Create test books with different sync statuses
        test_data = [
            ("STAT001", None),  # No sync status
            ("STAT002", "pending"),  # Pending sync
            ("STAT003", "completed"),  # Already synced
            ("STAT004", "failed"),  # Failed sync
        ]

        for barcode, sync_status in test_data:
            # Add book record
            book = BookRecord(
                barcode=barcode,
                title=f"Test Book {barcode}",
                processing_request_timestamp=datetime.now(UTC).isoformat(),
                created_at=datetime.now(UTC).isoformat(),
                updated_at=datetime.now(UTC).isoformat(),
            )
            await self.tracker.save_book(book)

            # Add processing status using batched approach
            status_updates = [StatusUpdate(barcode, "processing_request", "converted")]

            # Add sync status if specified
            if sync_status:
                status_updates.append(StatusUpdate(barcode, "sync", sync_status))

            await batch_write_status_updates(str(self.db_path), status_updates)

        # Test with no status filter (default: pending or NULL)
        all_sync_books = await self.tracker.get_books_for_sync(limit=10)

        # Should include books with no sync status or failed status
        expected_all = {"STAT001", "STAT004"}  # NULL or failed
        self.assertEqual(set(all_sync_books), expected_all)

        # Test with specific status filter
        failed_sync_books = await self.tracker.get_books_for_sync(limit=10, status_filter="failed")

        # Should only include books with failed status
        self.assertEqual(set(failed_sync_books), {"STAT004"})

    async def test_sync_book_eligibility(self):
        """Test that books with processing history are eligible for sync."""
        barcode = "ELIGIBLE001"

        # Add book record
        book = BookRecord(
            barcode=barcode,
            title=f"Test Book {barcode}",
            processing_request_timestamp=datetime.now(UTC).isoformat(),
            created_at=datetime.now(UTC).isoformat(),
            updated_at=datetime.now(UTC).isoformat(),
        )
        await self.tracker.save_book(book)

        # Add processing status using batched approach
        status_updates = [StatusUpdate(barcode, "processing_request", "converted")]
        await batch_write_status_updates(str(self.db_path), status_updates)

        # Test sync eligibility
        sync_books = await self.tracker.get_books_for_sync(limit=10)

        # Book should be eligible for sync
        self.assertIn(barcode, sync_books)

    async def test_backwards_compatibility_legacy_status(self):
        """Test that books with only legacy status fields still work."""
        barcode = "LEGACY001"

        # Add book record without status history
        book = BookRecord(
            barcode=barcode,
            title=f"Test Book {barcode}",
            processing_request_timestamp=datetime.now(UTC).isoformat(),
            created_at=datetime.now(UTC).isoformat(),
            updated_at=datetime.now(UTC).isoformat(),
        )
        await self.tracker.save_book(book)

        # Test get_books_for_sync - should include this book even without status history
        sync_books = await self.tracker.get_books_for_sync(limit=10)

        self.assertIn(barcode, sync_books)

    async def test_status_change_ordering(self):
        """Test that status changes are properly ordered and latest is used."""
        barcode = "ORDER001"

        # Add book record
        book = BookRecord(
            barcode=barcode,
            title=f"Test Book {barcode}",
            processing_request_timestamp=datetime.now(UTC).isoformat(),
            created_at=datetime.now(UTC).isoformat(),
            updated_at=datetime.now(UTC).isoformat(),
        )
        await self.tracker.save_book(book)

        # Add status changes in sequence using batched approach
        statuses = ["requested", "in_process", "converted", "failed", "converted"]
        status_updates = [StatusUpdate(barcode, "processing_request", status) for status in statuses]
        await batch_write_status_updates(str(self.db_path), status_updates)

        # The latest status should be "converted" (last in sequence)
        latest_status = await self.tracker.get_latest_status(barcode, "processing_request")
        self.assertEqual(latest_status, "converted")

        # Book should be eligible for sync since it has processing history
        sync_books = await self.tracker.get_books_for_sync(limit=10)

        self.assertIn(barcode, sync_books)
