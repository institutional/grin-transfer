#!/usr/bin/env python3
"""
Unit tests for status history system.

Tests the atomic status tracking functionality including:
- Status change recording
- Latest status retrieval
- Status history queries
- Backwards compatibility
"""

import asyncio
import json
import tempfile
from datetime import UTC, datetime
from pathlib import Path
from unittest import IsolatedAsyncioTestCase

from collect_books.models import BookRecord, SQLiteProgressTracker


class TestStatusHistory(IsolatedAsyncioTestCase):
    """Test status history tracking functionality."""

    async def asyncSetUp(self):
        """Set up test database."""
        self.temp_dir = tempfile.TemporaryDirectory()
        self.db_path = Path(self.temp_dir.name) / "test_books.db"
        self.tracker = SQLiteProgressTracker(str(self.db_path))
        await self.tracker.init_db()

    async def asyncTearDown(self):
        """Clean up test database."""
        self.temp_dir.cleanup()

    async def test_add_status_change_basic(self):
        """Test basic status change recording."""
        barcode = "TEST001"

        # Add initial book record
        book = BookRecord(
            barcode=barcode,
            title="Test Book",
            created_at=datetime.now(UTC).isoformat(),
            updated_at=datetime.now(UTC).isoformat()
        )
        await self.tracker.save_book(book)

        # Add status change
        result = await self.tracker.add_status_change(
            barcode=barcode,
            status_type="processing_request",
            status_value="requested"
        )

        self.assertTrue(result)

        # Verify status was recorded
        latest_status = await self.tracker.get_latest_status(barcode, "processing_request")
        self.assertEqual(latest_status, "requested")

    async def test_add_status_change_with_metadata(self):
        """Test status change with metadata."""
        barcode = "TEST002"

        # Add initial book record
        book = BookRecord(
            barcode=barcode,
            title="Test Book 2",
            created_at=datetime.now(UTC).isoformat(),
            updated_at=datetime.now(UTC).isoformat()
        )
        await self.tracker.save_book(book)

        # Add status change with metadata
        metadata = {"batch_id": "batch_001", "retry_count": 1}
        result = await self.tracker.add_status_change(
            barcode=barcode,
            status_type="processing_request",
            status_value="requested",
            session_id="session_123",
            metadata=metadata
        )

        self.assertTrue(result)

        # Verify the metadata was stored correctly
        import aiosqlite
        async with aiosqlite.connect(self.db_path) as db:
            cursor = await db.execute(
                """
                SELECT metadata, session_id FROM book_status_history
                WHERE barcode = ? AND status_type = ?
                """,
                (barcode, "processing_request")
            )
            row = await cursor.fetchone()

        self.assertIsNotNone(row)
        stored_metadata = json.loads(row[0])
        self.assertEqual(stored_metadata, metadata)
        self.assertEqual(row[1], "session_123")

    async def test_status_progression(self):
        """Test multiple status changes over time."""
        barcode = "TEST003"

        # Add initial book record
        book = BookRecord(
            barcode=barcode,
            title="Test Book 3",
            created_at=datetime.now(UTC).isoformat(),
            updated_at=datetime.now(UTC).isoformat()
        )
        await self.tracker.save_book(book)

        # Add sequence of status changes
        statuses = ["requested", "in_process", "converted"]

        for status in statuses:
            await self.tracker.add_status_change(
                barcode=barcode,
                status_type="processing_request",
                status_value=status
            )
            # Small delay to ensure different timestamps
            await asyncio.sleep(0.001)

        # Verify latest status is correct
        latest_status = await self.tracker.get_latest_status(barcode, "processing_request")
        self.assertEqual(latest_status, "converted")

        # Verify all history is preserved
        import aiosqlite
        async with aiosqlite.connect(self.db_path) as db:
            cursor = await db.execute(
                """
                SELECT status_value FROM book_status_history
                WHERE barcode = ? AND status_type = ?
                ORDER BY timestamp ASC, id ASC
                """,
                (barcode, "processing_request")
            )
            rows = await cursor.fetchall()

        recorded_statuses = [row[0] for row in rows]
        self.assertEqual(recorded_statuses, statuses)

    async def test_multiple_status_types(self):
        """Test different status types for same book."""
        barcode = "TEST004"

        # Add initial book record
        book = BookRecord(
            barcode=barcode,
            title="Test Book 4",
            created_at=datetime.now(UTC).isoformat(),
            updated_at=datetime.now(UTC).isoformat()
        )
        await self.tracker.save_book(book)

        # Add different types of status changes
        await self.tracker.add_status_change(
            barcode=barcode,
            status_type="processing_request",
            status_value="requested"
        )

        await self.tracker.add_status_change(
            barcode=barcode,
            status_type="sync",
            status_value="pending"
        )

        await self.tracker.add_status_change(
            barcode=barcode,
            status_type="enrichment",
            status_value="completed"
        )

        # Verify each status type has correct latest value
        processing_status = await self.tracker.get_latest_status(barcode, "processing_request")
        sync_status = await self.tracker.get_latest_status(barcode, "sync")
        enrichment_status = await self.tracker.get_latest_status(barcode, "enrichment")

        self.assertEqual(processing_status, "requested")
        self.assertEqual(sync_status, "pending")
        self.assertEqual(enrichment_status, "completed")

    async def test_get_books_with_latest_status(self):
        """Test querying books by latest status."""
        # Create multiple books with different statuses
        books = [
            ("BOOK001", "requested"),
            ("BOOK002", "in_process"),
            ("BOOK003", "converted"),
            ("BOOK004", "requested"),
            ("BOOK005", "failed")
        ]

        for barcode, status in books:
            # Add book record
            book = BookRecord(
                barcode=barcode,
                title=f"Test Book {barcode}",
                created_at=datetime.now(UTC).isoformat(),
                updated_at=datetime.now(UTC).isoformat()
            )
            await self.tracker.save_book(book)

            # Add status
            await self.tracker.add_status_change(
                barcode=barcode,
                status_type="processing_request",
                status_value=status
            )

        # Query books with specific statuses
        requested_books = await self.tracker.get_books_with_latest_status(
            status_type="processing_request",
            status_values=["requested"]
        )

        processing_books = await self.tracker.get_books_with_latest_status(
            status_type="processing_request",
            status_values=["in_process", "converted"]
        )

        all_books = await self.tracker.get_books_with_latest_status(
            status_type="processing_request"
        )

        # Verify results
        requested_barcodes = [book[0] for book in requested_books]
        self.assertIn("BOOK001", requested_barcodes)
        self.assertIn("BOOK004", requested_barcodes)
        self.assertEqual(len(requested_books), 2)

        processing_barcodes = [book[0] for book in processing_books]
        self.assertIn("BOOK002", processing_barcodes)
        self.assertIn("BOOK003", processing_barcodes)
        self.assertEqual(len(processing_books), 2)

        self.assertEqual(len(all_books), 5)

    async def test_backwards_compatibility(self):
        """Test that status changes update legacy fields."""
        barcode = "TEST005"

        # Add initial book record
        book = BookRecord(
            barcode=barcode,
            title="Test Book 5",
            created_at=datetime.now(UTC).isoformat(),
            updated_at=datetime.now(UTC).isoformat()
        )
        await self.tracker.save_book(book)

        # Add processing_request status change
        await self.tracker.add_status_change(
            barcode=barcode,
            status_type="processing_request",
            status_value="converted"
        )

        # Status changes only update status history now
        # Verify processing status is in history
        processing_status = await self.tracker.get_latest_status(barcode, "processing_request")
        self.assertEqual(processing_status, "converted")

        # Add sync status change
        await self.tracker.add_status_change(
            barcode=barcode,
            status_type="sync",
            status_value="completed"
        )

        # Verify sync status is in history
        sync_status = await self.tracker.get_latest_status(barcode, "sync")
        self.assertEqual(sync_status, "completed")

    async def test_get_latest_status_nonexistent(self):
        """Test getting status for non-existent book/status type."""
        result = await self.tracker.get_latest_status("NONEXISTENT", "processing_request")
        self.assertIsNone(result)

    async def test_status_change_order_consistency(self):
        """Test that status changes are ordered correctly by timestamp and ID."""
        barcode = "TEST006"

        # Add initial book record
        book = BookRecord(
            barcode=barcode,
            title="Test Book 6",
            created_at=datetime.now(UTC).isoformat(),
            updated_at=datetime.now(UTC).isoformat()
        )
        await self.tracker.save_book(book)

        # Add multiple status changes with same timestamp (edge case)
        await self.tracker.add_status_change(
            barcode=barcode,
            status_type="processing_request",
            status_value="requested"
        )

        await self.tracker.add_status_change(
            barcode=barcode,
            status_type="processing_request",
            status_value="in_process"
        )

        await self.tracker.add_status_change(
            barcode=barcode,
            status_type="processing_request",
            status_value="converted"
        )

        # The latest status should be "converted" (last added)
        latest_status = await self.tracker.get_latest_status(barcode, "processing_request")
        self.assertEqual(latest_status, "converted")

    async def test_get_books_for_sync_with_status_history(self):
        """Test that get_books_for_sync works with status history."""
        # Create books with different statuses in history
        test_books = [
            ("SYNC001", "converted"),
            ("SYNC002", "in_process"),
            ("SYNC003", "requested"),
            ("SYNC004", "failed")
        ]

        for barcode, final_status in test_books:
            # Add book record
            book = BookRecord(
                barcode=barcode,
                title=f"Sync Test {barcode}",
                created_at=datetime.now(UTC).isoformat(),
                updated_at=datetime.now(UTC).isoformat()
            )
            await self.tracker.save_book(book)

            # Add progression: requested -> (in_process) -> final_status
            await self.tracker.add_status_change(
                barcode=barcode,
                status_type="processing_request",
                status_value="requested"
            )

            if final_status != "requested":
                if final_status in ("converted", "in_process"):
                    await self.tracker.add_status_change(
                        barcode=barcode,
                        status_type="processing_request",
                        status_value="in_process"
                    )

                if final_status == "converted":
                    await self.tracker.add_status_change(
                        barcode=barcode,
                        status_type="processing_request",
                        status_value="converted"
                    )
                elif final_status == "failed":
                    await self.tracker.add_status_change(
                        barcode=barcode,
                        status_type="processing_request",
                        status_value="failed"
                    )

        # Test get_books_for_sync - should find books that have been requested
        # and are in valid processing states (excludes failed books)
        sync_books = await self.tracker.get_books_for_sync(
            storage_type="test",
            limit=10
        )

        # Should include all books except SYNC004 (failed status is excluded from sync)
        sync_barcodes = set(sync_books)
        expected_barcodes = {"SYNC001", "SYNC002", "SYNC003"}  # SYNC004 excluded (failed)
        self.assertEqual(sync_barcodes, expected_barcodes)

    async def test_limit_functionality(self):
        """Test limit parameter in get_books_with_latest_status."""
        # Create multiple books
        for i in range(10):
            barcode = f"LIMIT{i:03d}"
            book = BookRecord(
                barcode=barcode,
                title=f"Limit Test {barcode}",
                created_at=datetime.now(UTC).isoformat(),
                updated_at=datetime.now(UTC).isoformat()
            )
            await self.tracker.save_book(book)

            await self.tracker.add_status_change(
                barcode=barcode,
                status_type="processing_request",
                status_value="requested"
            )

        # Test with limit
        limited_books = await self.tracker.get_books_with_latest_status(
            status_type="processing_request",
            status_values=["requested"],
            limit=5
        )

        self.assertEqual(len(limited_books), 5)

        # Test without limit
        all_books = await self.tracker.get_books_with_latest_status(
            status_type="processing_request",
            status_values=["requested"]
        )

        self.assertEqual(len(all_books), 10)
