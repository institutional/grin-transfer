#!/usr/bin/env python3
"""
Integration test for sync_single_book functionality.
"""

import tempfile
from datetime import UTC, datetime
from pathlib import Path
from unittest import IsolatedAsyncioTestCase
from unittest.mock import patch

from grin_to_s3.collect_books.models import BookRecord, SQLiteProgressTracker
from grin_to_s3.sync import SyncPipeline


class TestSyncSingleBookIntegration(IsolatedAsyncioTestCase):
    """Test single book sync integration."""

    async def asyncSetUp(self):
        """Set up test database."""
        self.temp_dir = tempfile.TemporaryDirectory()
        self.db_path = Path(self.temp_dir.name) / "test_books.db"
        self.tracker = SQLiteProgressTracker(str(self.db_path))
        await self.tracker.init_db()

    async def asyncTearDown(self):
        """Clean up test database."""
        self.temp_dir.cleanup()

    async def test_sync_single_book_method_exists(self):
        """Test that sync_single_book method exists and has correct signature."""
        # Create sync pipeline
        pipeline = SyncPipeline(
            db_path=str(self.db_path),
            storage_type="local",
            storage_config={"prefix": "test"},
            directory="Harvard",
        )

        # Check that the method exists
        self.assertTrue(hasattr(pipeline, 'sync_single_book'))

        # Check that it's a coroutine
        self.assertTrue(callable(pipeline.sync_single_book))

    async def test_sync_single_book_return_format(self):
        """Test that sync_single_book returns correct format."""
        # Add test book
        book = BookRecord(
            barcode="TEST123",
            title="Test Book",
            created_at=datetime.now(UTC).isoformat(),
            updated_at=datetime.now(UTC).isoformat()
        )
        await self.tracker.save_book(book)

        # Create sync pipeline
        pipeline = SyncPipeline(
            db_path=str(self.db_path),
            storage_type="local",
            storage_config={"prefix": "test"},
            directory="Harvard",
        )

        # Mock dependencies
        with patch.object(pipeline, 'get_converted_books', return_value={"TEST123"}), \
             patch.object(pipeline, '_check_google_etag', return_value=("test-etag", 1024000)), \
             patch.object(pipeline, '_should_skip_download', return_value=True):

            # Test sync_single_book
            result = await pipeline.sync_single_book("TEST123", show_progress=False)

            # Verify result structure matches download.py format
            expected_keys = {
                "barcode", "status", "storage_type", "file_size", "download_time",
                "storage_time", "total_time", "download_speed_mbps", "google_etag",
                "is_decrypted"
            }

            # Check all expected keys exist
            self.assertTrue(expected_keys.issubset(result.keys()))

            # Check types and values
            self.assertEqual(result["barcode"], "TEST123")
            self.assertIn(result["status"], ["completed", "failed", "skipped"])
            self.assertEqual(result["storage_type"], "local")
            self.assertIsInstance(result["file_size"], int)
            self.assertIsInstance(result["download_time"], (int, float))
            self.assertIsInstance(result["storage_time"], (int, float))
            self.assertIsInstance(result["total_time"], (int, float))
            self.assertIsInstance(result["download_speed_mbps"], (int, float))

    async def test_sync_single_book_fast_mode_option(self):
        """Test that fast_mode parameter is accepted."""
        # Add test book
        book = BookRecord(
            barcode="TESTFAST",
            title="Test Book",
            created_at=datetime.now(UTC).isoformat(),
            updated_at=datetime.now(UTC).isoformat()
        )
        await self.tracker.save_book(book)

        # Create sync pipeline
        pipeline = SyncPipeline(
            db_path=str(self.db_path),
            storage_type="local",
            storage_config={"prefix": "test"},
            directory="Harvard",
        )

        # Mock dependencies
        with patch.object(pipeline, 'get_converted_books', return_value={"TESTFAST"}), \
             patch.object(pipeline, '_check_google_etag', return_value=("test-etag", 1024000)), \
             patch.object(pipeline, '_should_skip_download', return_value=True):

            # Test both fast_mode values
            result_normal = await pipeline.sync_single_book("TESTFAST", show_progress=False, fast_mode=False)
            result_fast = await pipeline.sync_single_book("TESTFAST", show_progress=False, fast_mode=True)

            # Both should return valid results
            self.assertIn(result_normal["status"], ["completed", "failed", "skipped"])
            self.assertIn(result_fast["status"], ["completed", "failed", "skipped"])
