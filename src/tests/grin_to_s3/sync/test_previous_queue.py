#!/usr/bin/env python3
"""
Tests for previous queue functionality.

Tests the logic for fetching books from the "previous" queue which includes
books with PREVIOUSLY_DOWNLOADED status filtered by in_process and verified_unavailable books.
"""

import asyncio
import tempfile
from pathlib import Path
from typing import NamedTuple
from unittest import IsolatedAsyncioTestCase
from unittest.mock import AsyncMock, patch

from grin_to_s3.collect_books.models import BookRecord, SQLiteProgressTracker
from grin_to_s3.queue_utils import get_previous_queue_books
from grin_to_s3.sync.pipeline import get_books_from_queue
from tests.utils import batch_write_status_updates


class StatusUpdate(NamedTuple):
    """Status update tuple for collecting updates before writing."""

    barcode: str
    status_type: str
    status_value: str
    metadata: dict | None = None
    session_id: str | None = None


class TestPreviousQueue(IsolatedAsyncioTestCase):
    """Test previous queue functionality."""

    def _create_async_return_value(self, value):
        """Helper to create properly resolved async return values for mocks."""
        future = asyncio.Future()
        future.set_result(value)
        return future

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
        """Clean up test database."""
        await self.tracker.close()
        self.temp_dir.cleanup()

    async def _add_book_with_grin_state(self, barcode: str, grin_state: str, sync_statuses: list[str] = None) -> None:
        """Helper to add a book with given GRIN state and optional sync statuses."""
        book = BookRecord(
            barcode=barcode,
            title=f"Test Book {barcode}",
            grin_state=grin_state,
        )
        await self.tracker.save_book(book)

        # Add sync status history if provided
        if sync_statuses:
            status_updates = []
            for status in sync_statuses:
                status_updates.append(StatusUpdate(barcode, "sync", status))

            if status_updates:
                await batch_write_status_updates(str(self.db_path), status_updates)

    async def test_previous_queue_basic_filtering(self):
        """Test basic previous queue filtering logic."""
        # Create test books with different GRIN states
        await self._add_book_with_grin_state("PREV001", "PREVIOUSLY_DOWNLOADED")
        await self._add_book_with_grin_state("PREV002", "PREVIOUSLY_DOWNLOADED")
        await self._add_book_with_grin_state("PREV003", "CONVERTED")  # Should not be included
        await self._add_book_with_grin_state("PREV004", "PREVIOUSLY_DOWNLOADED")

        # Mock in_process and verified_unavailable filtering
        with patch("grin_to_s3.sync.pipeline.get_in_process_set") as mock_in_process:
            mock_in_process.return_value = {"PREV002"}  # PREV002 is in process

            # PREV004 marked as verified_unavailable
            await self._add_book_with_grin_state("PREV004", "PREVIOUSLY_DOWNLOADED")
            # Add conversion unavailable status to match real system behavior
            status_updates = [StatusUpdate("PREV004", "conversion", "unavailable")]
            await batch_write_status_updates(str(self.db_path), status_updates)

            # Get previous queue books
            result = await get_books_from_queue(self.mock_grin_client, "test_library", "previous", self.tracker)

        # Should only return PREV001 (PREV002 filtered by in_process, PREV003 wrong state, PREV004 unavailable)
        self.assertEqual(result, {"PREV001"})

    async def test_previous_queue_empty_sets(self):
        """Test previous queue with various empty scenarios."""
        # No PREVIOUSLY_DOWNLOADED books
        await self._add_book_with_grin_state("CONV001", "CONVERTED")
        await self._add_book_with_grin_state("CONV002", "CONVERTED")

        with patch("grin_to_s3.sync.pipeline.get_in_process_set") as mock_in_process:
            mock_in_process.return_value = set()

            result = await get_books_from_queue(self.mock_grin_client, "test_library", "previous", self.tracker)

        self.assertEqual(result, set())

    async def test_previous_queue_all_filtered_out(self):
        """Test previous queue where all books are filtered out."""
        # Create PREVIOUSLY_DOWNLOADED books
        await self._add_book_with_grin_state("PREV001", "PREVIOUSLY_DOWNLOADED")
        await self._add_book_with_grin_state("PREV002", "PREVIOUSLY_DOWNLOADED")

        # Mock all books being in process
        with patch("grin_to_s3.sync.pipeline.get_in_process_set") as mock_in_process:
            mock_in_process.return_value = {"PREV001", "PREV002"}

            result = await get_books_from_queue(self.mock_grin_client, "test_library", "previous", self.tracker)

        self.assertEqual(result, set())

    async def test_previous_queue_verified_unavailable_filtering(self):
        """Test that verified_unavailable books are filtered out."""
        # Create test books
        await self._add_book_with_grin_state("PREV001", "PREVIOUSLY_DOWNLOADED")
        await self._add_book_with_grin_state("PREV002", "PREVIOUSLY_DOWNLOADED")
        await self._add_book_with_grin_state("PREV003", "PREVIOUSLY_DOWNLOADED")

        # Add conversion unavailable status to match real system behavior
        status_updates = [StatusUpdate("PREV002", "conversion", "unavailable")]
        await batch_write_status_updates(str(self.db_path), status_updates)

        with patch("grin_to_s3.sync.pipeline.get_in_process_set") as mock_in_process:
            mock_in_process.return_value = set()

            result = await get_books_from_queue(self.mock_grin_client, "test_library", "previous", self.tracker)

        # PREV002 should be filtered out due to verified_unavailable status
        self.assertEqual(result, {"PREV001", "PREV003"})

    async def test_previous_queue_mixed_filtering(self):
        """Test previous queue with both in_process and verified_unavailable filtering."""
        # Create mix of books
        await self._add_book_with_grin_state("PREV001", "PREVIOUSLY_DOWNLOADED")
        await self._add_book_with_grin_state("PREV002", "PREVIOUSLY_DOWNLOADED")
        await self._add_book_with_grin_state("PREV003", "PREVIOUSLY_DOWNLOADED")
        await self._add_book_with_grin_state("PREV004", "PREVIOUSLY_DOWNLOADED")
        await self._add_book_with_grin_state("PREV005", "CONVERTED")  # Wrong state

        # Add conversion unavailable status to match real system behavior
        status_updates = [StatusUpdate("PREV003", "conversion", "unavailable")]
        await batch_write_status_updates(str(self.db_path), status_updates)

        with patch("grin_to_s3.sync.pipeline.get_in_process_set") as mock_in_process:
            # PREV002 and PREV004 are in process
            mock_in_process.return_value = {"PREV002", "PREV004"}

            result = await get_books_from_queue(self.mock_grin_client, "test_library", "previous", self.tracker)

        # Only PREV001 should remain (PREV002/PREV004 in_process, PREV003 unavailable, PREV005 wrong state)
        self.assertEqual(result, {"PREV001"})

    async def test_previous_queue_database_queries(self):
        """Test that correct database queries are made for previous queue."""
        # Create test data
        await self._add_book_with_grin_state("PREV001", "PREVIOUSLY_DOWNLOADED")
        await self._add_book_with_grin_state("PREV002", "CONVERTED")
        await self._add_book_with_grin_state("PREV003", "PREVIOUSLY_DOWNLOADED")

        # Add conversion unavailable status to match real system behavior
        status_updates = [StatusUpdate("PREV003", "conversion", "unavailable")]
        await batch_write_status_updates(str(self.db_path), status_updates)

        # Mock in_process set
        with patch("grin_to_s3.sync.pipeline.get_in_process_set") as mock_in_process:
            mock_in_process.return_value = set()

            # Mock the new get_previous_queue_books function
            with patch("grin_to_s3.sync.pipeline.get_previous_queue_books") as mock_previous_queue:
                mock_previous_queue.return_value = {"PREV001"}  # Only PREV001 should be returned

                result = await get_books_from_queue(self.mock_grin_client, "test_library", "previous", self.tracker)

                # Verify the new function was called with correct parameters
                mock_previous_queue.assert_called_once_with(self.tracker)

                # Result should be the filtered set
                self.assertEqual(result, {"PREV001"})

    async def test_get_books_by_grin_state_method(self):
        """Test the get_books_by_grin_state method directly."""
        # Create books with different GRIN states
        await self._add_book_with_grin_state("PREV001", "PREVIOUSLY_DOWNLOADED")
        await self._add_book_with_grin_state("PREV002", "PREVIOUSLY_DOWNLOADED")
        await self._add_book_with_grin_state("CONV001", "CONVERTED")
        await self._add_book_with_grin_state("PROC001", "IN_PROCESS")

        # Test querying for PREVIOUSLY_DOWNLOADED
        result = await self.tracker.get_books_by_grin_state("PREVIOUSLY_DOWNLOADED")
        self.assertEqual(result, {"PREV001", "PREV002"})

        # Test querying for CONVERTED
        result = await self.tracker.get_books_by_grin_state("CONVERTED")
        self.assertEqual(result, {"CONV001"})

        # Test querying for non-existent state
        result = await self.tracker.get_books_by_grin_state("NON_EXISTENT")
        self.assertEqual(result, set())

    async def test_get_books_with_status_method(self):
        """Test the get_books_with_status method directly."""
        # Create books with different sync statuses
        await self._add_book_with_grin_state("BOOK001", "CONVERTED", ["completed"])
        await self._add_book_with_grin_state("BOOK002", "CONVERTED", ["failed"])
        # Add books with conversion unavailable status to match real system behavior
        await self._add_book_with_grin_state("BOOK003", "CONVERTED")
        await self._add_book_with_grin_state("BOOK004", "CONVERTED")

        # Add conversion unavailable status records
        status_updates = [
            StatusUpdate("BOOK003", "conversion", "unavailable"),
            StatusUpdate("BOOK004", "conversion", "unavailable"),
        ]
        await batch_write_status_updates(str(self.db_path), status_updates)

        # Test querying for verified_unavailable status
        result = await self.tracker.get_books_with_status("unavailable", "conversion")
        self.assertEqual(result, {"BOOK003", "BOOK004"})

        # Test querying for failed status
        result = await self.tracker.get_books_with_status("failed")
        self.assertEqual(result, {"BOOK002"})

        # Test querying for non-existent status
        result = await self.tracker.get_books_with_status("non_existent")
        self.assertEqual(result, set())

    async def test_previous_queue_caching_behavior(self):
        """Test that in_process caching doesn't interfere with previous queue logic."""
        await self._add_book_with_grin_state("PREV001", "PREVIOUSLY_DOWNLOADED")
        await self._add_book_with_grin_state("PREV002", "PREVIOUSLY_DOWNLOADED")

        # First call should cache the in_process set
        with patch("grin_to_s3.sync.pipeline.get_in_process_set") as mock_in_process:
            mock_in_process.return_value = {"PREV001"}

            result1 = await get_books_from_queue(self.mock_grin_client, "test_library", "previous", self.tracker)

            # Second call should use cached data
            result2 = await get_books_from_queue(self.mock_grin_client, "test_library", "previous", self.tracker)

        # Results should be consistent
        self.assertEqual(result1, {"PREV002"})
        self.assertEqual(result2, {"PREV002"})

        # get_in_process_set should be called for both since each call is independent
        # (caching is within get_in_process_set itself, not at the queue level)
        self.assertEqual(mock_in_process.call_count, 2)

    async def test_get_previous_queue_books_filtering(self):
        """Test the get_previous_queue_books function directly."""
        # Create books with different states and processing status
        await self._add_book_with_grin_state("PREV001", "PREVIOUSLY_DOWNLOADED")  # Should be included
        await self._add_book_with_grin_state(
            "PREV002", "PREVIOUSLY_DOWNLOADED"
        )  # Should be excluded (has processing_request_timestamp)
        await self._add_book_with_grin_state("PREV003", "PREVIOUSLY_DOWNLOADED")  # Should be excluded (unavailable)
        await self._add_book_with_grin_state("PREV004", "CONVERTED")  # Should be excluded (wrong state)

        # Add processing_request_timestamp to PREV002
        book_with_timestamp = BookRecord(
            barcode="PREV002",
            title="Test Book PREV002",
            grin_state="PREVIOUSLY_DOWNLOADED",
            processing_request_timestamp="2024-01-01T00:00:00Z",
        )
        await self.tracker.save_book(book_with_timestamp, refresh_mode=True)

        # Mark PREV003 as unavailable
        status_updates = [StatusUpdate("PREV003", "conversion", "unavailable")]
        await batch_write_status_updates(str(self.db_path), status_updates)

        # Test the function
        result = await get_previous_queue_books(self.tracker)

        # Only PREV001 should be returned
        self.assertEqual(result, {"PREV001"})

    async def test_get_previous_queue_books_excludes_conversion_failed(self):
        """get_previous_queue_books should exclude books marked as conversion_failed."""
        # Create books with different states
        await self._add_book_with_grin_state("PREV001", "PREVIOUSLY_DOWNLOADED")  # Should be included
        await self._add_book_with_grin_state(
            "PREV002", "PREVIOUSLY_DOWNLOADED"
        )  # Should be excluded (conversion_failed)
        await self._add_book_with_grin_state("PREV003", "PREVIOUSLY_DOWNLOADED")  # Should be excluded (unavailable)
        await self._add_book_with_grin_state("PREV004", "PREVIOUSLY_DOWNLOADED")  # Should be included

        # Mark PREV002 as conversion_failed
        status_updates = [
            StatusUpdate("PREV002", "conversion", "conversion_failed"),
            StatusUpdate("PREV003", "conversion", "unavailable"),
        ]
        await batch_write_status_updates(str(self.db_path), status_updates)

        # Test the function
        result = await get_previous_queue_books(self.tracker)

        # Only PREV001 and PREV004 should be returned (PREV002 and PREV003 excluded)
        self.assertEqual(result, {"PREV001", "PREV004"})
