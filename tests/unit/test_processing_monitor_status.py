#!/usr/bin/env python3
"""
Integration tests for ProcessingMonitor status update functionality.

Tests the automatic status updating when monitoring GRIN processing status.
"""

import tempfile
from datetime import UTC, datetime
from pathlib import Path
from unittest import IsolatedAsyncioTestCase
from unittest.mock import AsyncMock, patch

from collect_books.models import SQLiteProgressTracker, BookRecord
from processing import ProcessingMonitor


class TestProcessingMonitorStatus(IsolatedAsyncioTestCase):
    """Test ProcessingMonitor status update functionality."""

    async def asyncSetUp(self):
        """Set up test database and monitor."""
        self.temp_dir = tempfile.TemporaryDirectory()
        self.db_path = Path(self.temp_dir.name) / "test_books.db"
        self.tracker = SQLiteProgressTracker(str(self.db_path))
        await self.tracker.init_db()
        
        # Create monitor with mocked GRIN client
        self.monitor = ProcessingMonitor("TestDirectory")
        self.monitor.db_path = str(self.db_path)

    async def asyncTearDown(self):
        """Clean up test database."""
        await self.monitor.cleanup()
        self.temp_dir.cleanup()

    async def test_update_book_statuses_to_converted(self):
        """Test updating books from in_process to converted."""
        # Create test books in database
        test_books = ["CONV001", "CONV002", "CONV003"]
        
        for barcode in test_books:
            # Add book record
            book = BookRecord(
                barcode=barcode,
                title=f"Test Book {barcode}",
                processing_request_timestamp=datetime.now(UTC).isoformat(),
                created_at=datetime.now(UTC).isoformat(),
                updated_at=datetime.now(UTC).isoformat()
            )
            await self.tracker.save_book(book)
            
            # Add initial status
            await self.tracker.add_status_change(
                barcode=barcode,
                status_type="processing_request",
                status_value="requested"
            )
            
            # Move to in_process
            await self.tracker.add_status_change(
                barcode=barcode,
                status_type="processing_request",
                status_value="in_process"
            )
        
        # Mock GRIN responses - some books are now converted
        converted_books = ["CONV001", "CONV003"]
        in_process_books = ["CONV002"]
        failed_books = []
        
        with patch.object(self.monitor, 'get_converted_books', new_callable=AsyncMock) as mock_converted, \
             patch.object(self.monitor, 'get_in_process_books', new_callable=AsyncMock) as mock_in_process, \
             patch.object(self.monitor, 'get_failed_books', new_callable=AsyncMock) as mock_failed:
            
            mock_converted.return_value = converted_books
            mock_in_process.return_value = in_process_books
            mock_failed.return_value = failed_books
            
            # Update statuses
            updates = await self.monitor.update_book_statuses()
        
        # Verify updates
        self.assertEqual(updates["converted"], 2)  # CONV001, CONV003
        self.assertEqual(updates["in_process"], 0)  # CONV002 already in_process
        self.assertEqual(updates["failed"], 0)
        
        # Verify database status
        conv001_status = await self.tracker.get_latest_status("CONV001", "processing_request")
        conv002_status = await self.tracker.get_latest_status("CONV002", "processing_request")
        conv003_status = await self.tracker.get_latest_status("CONV003", "processing_request")
        
        self.assertEqual(conv001_status, "converted")
        self.assertEqual(conv002_status, "in_process")
        self.assertEqual(conv003_status, "converted")

    async def test_update_book_statuses_to_in_process(self):
        """Test updating books from requested to in_process."""
        # Create test books in database
        test_books = ["PROC001", "PROC002"]
        
        for barcode in test_books:
            # Add book record
            book = BookRecord(
                barcode=barcode,
                title=f"Test Book {barcode}",
                processing_request_timestamp=datetime.now(UTC).isoformat(),
                created_at=datetime.now(UTC).isoformat(),
                updated_at=datetime.now(UTC).isoformat()
            )
            await self.tracker.save_book(book)
            
            # Add initial status
            await self.tracker.add_status_change(
                barcode=barcode,
                status_type="processing_request",
                status_value="requested"
            )
        
        # Mock GRIN responses - books are now in process
        converted_books = []
        in_process_books = ["PROC001", "PROC002"]
        failed_books = []
        
        with patch.object(self.monitor, 'get_converted_books', new_callable=AsyncMock) as mock_converted, \
             patch.object(self.monitor, 'get_in_process_books', new_callable=AsyncMock) as mock_in_process, \
             patch.object(self.monitor, 'get_failed_books', new_callable=AsyncMock) as mock_failed:
            
            mock_converted.return_value = converted_books
            mock_in_process.return_value = in_process_books
            mock_failed.return_value = failed_books
            
            # Update statuses
            updates = await self.monitor.update_book_statuses()
        
        # Verify updates
        self.assertEqual(updates["converted"], 0)
        self.assertEqual(updates["in_process"], 2)  # Both books moved to in_process
        self.assertEqual(updates["failed"], 0)
        
        # Verify database status
        for barcode in test_books:
            status = await self.tracker.get_latest_status(barcode, "processing_request")
            self.assertEqual(status, "in_process")

    async def test_update_book_statuses_to_failed(self):
        """Test updating books to failed status."""
        # Create test books in database
        test_books = ["FAIL001", "FAIL002"]
        
        for barcode in test_books:
            # Add book record
            book = BookRecord(
                barcode=barcode,
                title=f"Test Book {barcode}",
                processing_request_timestamp=datetime.now(UTC).isoformat(),
                created_at=datetime.now(UTC).isoformat(),
                updated_at=datetime.now(UTC).isoformat()
            )
            await self.tracker.save_book(book)
            
            # Add initial status
            await self.tracker.add_status_change(
                barcode=barcode,
                status_type="processing_request",
                status_value="requested"
            )
        
        # Mock GRIN responses - books have failed
        converted_books = []
        in_process_books = []
        failed_books = ["FAIL001", "FAIL002"]
        
        with patch.object(self.monitor, 'get_converted_books', new_callable=AsyncMock) as mock_converted, \
             patch.object(self.monitor, 'get_in_process_books', new_callable=AsyncMock) as mock_in_process, \
             patch.object(self.monitor, 'get_failed_books', new_callable=AsyncMock) as mock_failed:
            
            mock_converted.return_value = converted_books
            mock_in_process.return_value = in_process_books
            mock_failed.return_value = failed_books
            
            # Update statuses
            updates = await self.monitor.update_book_statuses()
        
        # Verify updates
        self.assertEqual(updates["converted"], 0)
        self.assertEqual(updates["in_process"], 0)
        self.assertEqual(updates["failed"], 2)  # Both books failed
        
        # Verify database status
        for barcode in test_books:
            status = await self.tracker.get_latest_status(barcode, "processing_request")
            self.assertEqual(status, "failed")

    async def test_no_duplicate_status_updates(self):
        """Test that status updates don't create duplicates."""
        # Create test book
        barcode = "NODUP001"
        
        # Add book record
        book = BookRecord(
            barcode=barcode,
            title=f"Test Book {barcode}",
            processing_request_timestamp=datetime.now(UTC).isoformat(),
            created_at=datetime.now(UTC).isoformat(),
            updated_at=datetime.now(UTC).isoformat()
        )
        await self.tracker.save_book(book)
        
        # Add initial status
        await self.tracker.add_status_change(
            barcode=barcode,
            status_type="processing_request",
            status_value="converted"
        )
        
        # Mock GRIN responses - book is still converted
        with patch.object(self.monitor, 'get_converted_books', new_callable=AsyncMock) as mock_converted, \
             patch.object(self.monitor, 'get_in_process_books', new_callable=AsyncMock) as mock_in_process, \
             patch.object(self.monitor, 'get_failed_books', new_callable=AsyncMock) as mock_failed:
            
            mock_converted.return_value = [barcode]
            mock_in_process.return_value = []
            mock_failed.return_value = []
            
            # Update statuses (should not create duplicate)
            updates = await self.monitor.update_book_statuses()
        
        # No updates should be made since book is already converted
        self.assertEqual(updates["converted"], 0)
        self.assertEqual(updates["in_process"], 0)
        self.assertEqual(updates["failed"], 0)
        
        # Verify only one status entry exists
        import aiosqlite
        async with aiosqlite.connect(self.db_path) as db:
            cursor = await db.execute(
                """
                SELECT COUNT(*) FROM book_status_history 
                WHERE barcode = ? AND status_type = ? AND status_value = ?
                """,
                (barcode, "processing_request", "converted")
            )
            count = (await cursor.fetchone())[0]
        
        self.assertEqual(count, 1)

    async def test_mixed_status_updates(self):
        """Test updating multiple books with different status changes."""
        # Create test books with different current statuses
        books_data = [
            ("MIX001", "requested", "in_process"),
            ("MIX002", "in_process", "converted"),
            ("MIX003", "requested", "failed"),
            ("MIX004", "converted", "converted"),  # No change
        ]
        
        for barcode, initial_status, final_status in books_data:
            # Add book record
            book = BookRecord(
                barcode=barcode,
                title=f"Test Book {barcode}",
                processing_request_timestamp=datetime.now(UTC).isoformat(),
                created_at=datetime.now(UTC).isoformat(),
                updated_at=datetime.now(UTC).isoformat()
            )
            await self.tracker.save_book(book)
            
            # Add initial status
            await self.tracker.add_status_change(
                barcode=barcode,
                status_type="processing_request",
                status_value=initial_status
            )
        
        # Mock GRIN responses based on final statuses
        converted_books = ["MIX002", "MIX004"]
        in_process_books = ["MIX001"]
        failed_books = ["MIX003"]
        
        with patch.object(self.monitor, 'get_converted_books', new_callable=AsyncMock) as mock_converted, \
             patch.object(self.monitor, 'get_in_process_books', new_callable=AsyncMock) as mock_in_process, \
             patch.object(self.monitor, 'get_failed_books', new_callable=AsyncMock) as mock_failed:
            
            mock_converted.return_value = converted_books
            mock_in_process.return_value = in_process_books
            mock_failed.return_value = failed_books
            
            # Update statuses
            updates = await self.monitor.update_book_statuses()
        
        # Verify update counts
        self.assertEqual(updates["converted"], 1)   # MIX002: in_process -> converted
        self.assertEqual(updates["in_process"], 1)  # MIX001: requested -> in_process
        self.assertEqual(updates["failed"], 1)      # MIX003: requested -> failed
        # MIX004 should not be updated (already converted)
        
        # Verify final statuses
        mix001_status = await self.tracker.get_latest_status("MIX001", "processing_request")
        mix002_status = await self.tracker.get_latest_status("MIX002", "processing_request")
        mix003_status = await self.tracker.get_latest_status("MIX003", "processing_request")
        mix004_status = await self.tracker.get_latest_status("MIX004", "processing_request")
        
        self.assertEqual(mix001_status, "in_process")
        self.assertEqual(mix002_status, "converted")
        self.assertEqual(mix003_status, "failed")
        self.assertEqual(mix004_status, "converted")

    async def test_get_requested_books_with_status_history(self):
        """Test that get_requested_books works with status history."""
        # Create books with different status progressions
        test_books = [
            ("REQ001", ["requested"]),
            ("REQ002", ["requested", "in_process"]),
            ("REQ003", ["requested", "in_process", "converted"]),
            ("REQ004", ["requested", "failed"]),
        ]
        
        for barcode, status_progression in test_books:
            # Add book record
            book = BookRecord(
                barcode=barcode,
                title=f"Test Book {barcode}",
                processing_request_timestamp=datetime.now(UTC).isoformat(),
                created_at=datetime.now(UTC).isoformat(),
                updated_at=datetime.now(UTC).isoformat()
            )
            await self.tracker.save_book(book)
            
            # Add status progression
            for status in status_progression:
                await self.tracker.add_status_change(
                    barcode=barcode,
                    status_type="processing_request",
                    status_value=status
                )
        
        # Get requested books (should include all since they all have processing_request_timestamp)
        requested_books = await self.monitor.get_requested_books()
        
        # All books should be returned since they all have processing timestamps
        expected_barcodes = {"REQ001", "REQ002", "REQ003", "REQ004"}
        self.assertEqual(requested_books, expected_barcodes)

    async def test_status_update_error_handling(self):
        """Test error handling in status updates."""
        # Create a monitor with invalid database path
        bad_monitor = ProcessingMonitor("TestDirectory")
        bad_monitor.db_path = "/nonexistent/path/to/database.db"
        
        # Mock GRIN responses
        with patch.object(bad_monitor, 'get_converted_books', new_callable=AsyncMock) as mock_converted, \
             patch.object(bad_monitor, 'get_in_process_books', new_callable=AsyncMock) as mock_in_process, \
             patch.object(bad_monitor, 'get_failed_books', new_callable=AsyncMock) as mock_failed:
            
            mock_converted.return_value = ["TEST001"]
            mock_in_process.return_value = []
            mock_failed.return_value = []
            
            # Should handle error gracefully
            updates = await bad_monitor.update_book_statuses()
        
        # Should return empty updates dict on error
        self.assertEqual(updates, {})
        
        await bad_monitor.cleanup()