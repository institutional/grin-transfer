#!/usr/bin/env python3
"""
Tests for unconverted queue functionality.

Tests the logic for fetching books from the "unconverted" queue which includes
books that have never been requested for processing or converted by GRIN,
filtered by in_process and verified_unavailable books.
"""

import tempfile
from pathlib import Path
from typing import NamedTuple
from unittest import IsolatedAsyncioTestCase
from unittest.mock import AsyncMock, patch

from grin_to_s3.collect_books.models import BookRecord, SQLiteProgressTracker
from grin_to_s3.queue_utils import get_unconverted_books
from grin_to_s3.sync.pipeline import get_books_from_queue
from tests.utils import batch_write_status_updates


class StatusUpdate(NamedTuple):
    """Status update tuple for collecting updates before writing."""

    barcode: str
    status_type: str
    status_value: str
    metadata: dict | None = None
    session_id: str | None = None


class TestUnconvertedQueue(IsolatedAsyncioTestCase):
    """Test unconverted queue functionality."""

    def _create_async_return_value(self, value):
        """Helper to create properly resolved async return values for mocks."""
        return value

    async def asyncSetUp(self):
        """Set up test database and tracker."""
        self.temp_dir = tempfile.TemporaryDirectory()
        self.db_path = Path(self.temp_dir.name) / "test_books.db"
        self.tracker = SQLiteProgressTracker(str(self.db_path))
        await self.tracker.init_db()

        # Mock GRIN client
        self.mock_grin_client = AsyncMock()
        # Configure fetch_resource to return a valid string response
        self.mock_grin_client.fetch_resource.return_value = ""

    async def asyncTearDown(self):
        """Clean up resources."""
        await self.tracker.close()
        self.temp_dir.cleanup()

    async def _insert_book(
        self,
        barcode: str,
        processing_request_timestamp: str | None = None,
        converted_date: str | None = None,
        grin_check_in_date: str | None = None,
        grin_state: str | None = None,
    ) -> None:
        """Helper to insert a book with specified processing, conversion, and check-in status."""
        from datetime import UTC, datetime

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
        await self.tracker.init_db()
        conn = await self.tracker.get_connection()
        await conn.execute(BookRecord.build_insert_sql(), book.to_tuple())
        await conn.commit()

    async def test_get_unconverted_books_basic(self):
        """get_unconverted_books should return books with neither processing timestamp nor converted date"""
        # Insert test books
        await self._insert_book("book1")  # Neither timestamp - should be returned
        await self._insert_book(
            "book2", processing_request_timestamp="2024-01-01T00:00:00Z"
        )  # Has processing timestamp - should NOT be returned
        await self._insert_book(
            "book3", converted_date="2024-01-01T00:00:00Z"
        )  # Has converted date - should NOT be returned
        await self._insert_book(
            "book4", processing_request_timestamp="2024-01-01T00:00:00Z", converted_date="2024-01-01T00:00:00Z"
        )  # Has both - should NOT be returned

        result = await get_unconverted_books(self.tracker)

        self.assertEqual(result, {"book1"})

    async def test_get_unconverted_books_empty_database(self):
        """get_unconverted_books should return empty set when no books exist"""
        result = await get_unconverted_books(self.tracker)
        self.assertEqual(result, set())

    async def test_get_unconverted_books_no_unconverted(self):
        """get_unconverted_books should return empty set when all books have processing timestamps or conversion dates"""
        # Insert books that all have either processing timestamp or converted date
        await self._insert_book("book1", processing_request_timestamp="2024-01-01T00:00:00Z")
        await self._insert_book("book2", converted_date="2024-01-01T00:00:00Z")
        await self._insert_book(
            "book3", processing_request_timestamp="2024-01-01T00:00:00Z", converted_date="2024-01-01T00:00:00Z"
        )

        result = await get_unconverted_books(self.tracker)
        self.assertEqual(result, set())

    async def test_get_unconverted_books_excludes_checked_in(self):
        """get_unconverted_books should exclude books that are checked in or not available for download"""
        # Insert test books
        await self._insert_book("book1")  # Neither timestamp nor checked in - should be returned
        await self._insert_book(
            "book2", grin_check_in_date="2024-01-01T00:00:00Z"
        )  # Has check-in date - should NOT be returned
        await self._insert_book("book3", grin_state="CHECKED_IN")  # Has CHECKED_IN state - should NOT be returned
        await self._insert_book(
            "book4", grin_state="NOT_AVAILABLE_FOR_DOWNLOAD"
        )  # Has NOT_AVAILABLE_FOR_DOWNLOAD state - should NOT be returned
        await self._insert_book(
            "book5", grin_check_in_date="2024-01-01T00:00:00Z", grin_state="CHECKED_IN"
        )  # Has both - should NOT be returned

        result = await get_unconverted_books(self.tracker)

        self.assertEqual(result, {"book1"})

    @patch("grin_to_s3.sync.pipeline.get_in_process_set")
    async def test_unconverted_queue_basic_filtering(self, mock_get_in_process):
        """Unconverted queue should filter out in_process books"""
        # Set up mock return values
        mock_get_in_process.return_value = {"book2"}

        # Insert test books
        await self._insert_book("book1")  # Unconverted - should be returned
        await self._insert_book("book2")  # Unconverted but in process - should be filtered out
        await self._insert_book("book3")  # Unconverted - should be returned
        await self._insert_book(
            "book4", processing_request_timestamp="2024-01-01T00:00:00Z"
        )  # Has processing timestamp - not unconverted

        result = await get_books_from_queue(self.mock_grin_client, "test_library", "unconverted", self.tracker)

        # book1 and book3 should remain after filtering (book2 filtered out by in_process, book4 not unconverted)
        self.assertEqual(result, {"book1", "book3"})

    @patch("grin_to_s3.sync.pipeline.get_in_process_set")
    async def test_unconverted_queue_empty_sets(self, mock_get_in_process):
        """Unconverted queue should handle empty in_process sets"""
        # Set up mock return values for empty sets
        mock_get_in_process.return_value = set()

        # Insert test books
        await self._insert_book("book1")
        await self._insert_book("book2")

        result = await get_books_from_queue(self.mock_grin_client, "test_library", "unconverted", self.tracker)

        # Both books should be returned since nothing filters them out
        self.assertEqual(result, {"book1", "book2"})

    @patch("grin_to_s3.sync.pipeline.get_in_process_set")
    async def test_unconverted_queue_all_filtered_out(self, mock_get_in_process):
        """Unconverted queue should return empty set when all books are filtered out"""
        # Set up mock return values
        mock_get_in_process.return_value = {"book1", "book2"}

        # Insert test books that will all be filtered out
        await self._insert_book("book1")  # Will be in process
        await self._insert_book("book2")  # Will be in process

        result = await get_books_from_queue(self.mock_grin_client, "test_library", "unconverted", self.tracker)

        # No books should remain after filtering
        self.assertEqual(result, set())

    async def test_get_unconverted_books_excludes_conversion_failed(self):
        """get_unconverted_books should exclude books marked as conversion_failed."""
        # Insert test books
        await self._insert_book("book1")  # Should be included
        await self._insert_book("book2")  # Should be excluded (conversion_failed)
        await self._insert_book("book3")  # Should be excluded (unavailable)
        await self._insert_book("book4")  # Should be included

        # Mark book2 as conversion_failed and book3 as unavailable
        status_updates = [
            StatusUpdate("book2", "conversion", "conversion_failed"),
            StatusUpdate("book3", "conversion", "unavailable"),
        ]
        await batch_write_status_updates(str(self.db_path), status_updates)

        # Test the function
        result = await get_unconverted_books(self.tracker)

        # Only book1 and book4 should be returned (book2 and book3 excluded)
        self.assertEqual(result, {"book1", "book4"})
