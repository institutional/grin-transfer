#!/usr/bin/env python3
"""
Tests for database ETag tracking functionality.
"""

import asyncio
import tempfile
from pathlib import Path
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from grin_to_s3.collect_books.models import SQLiteProgressTracker
from grin_to_s3.database_utils import batch_write_status_updates
from grin_to_s3.extract.tracking import collect_status
from grin_to_s3.sync.operations import check_and_handle_etag_skip
from grin_to_s3.sync.utils import should_skip_download


@pytest.fixture
def mock_semaphore():
    """Mock semaphore for GRIN API concurrency testing."""
    return asyncio.Semaphore(5)


class TestDatabaseETagTracking:
    async def make_tracker(self):
        temp_dir = tempfile.mkdtemp()
        db_path = Path(temp_dir) / "test.db"
        tracker = SQLiteProgressTracker(str(db_path))
        await tracker.init_db()
        return tracker

    @pytest.mark.asyncio
    async def test_skip_download_scenarios(self):
        tracker = await self.make_tracker()

        # No metadata - should download
        should_skip, reason = await should_skip_download("TEST1", '"abc"', "local", {}, tracker, False)
        assert not should_skip and reason == "no_metadata"

        # Metadata without ETag - should download
        status_updates = [collect_status("TEST2", "sync", "completed", metadata={"storage_type": "local"})]
        await batch_write_status_updates(tracker.db_path, status_updates)
        should_skip, reason = await should_skip_download("TEST2", '"abc"', "local", {}, tracker, False)
        assert not should_skip and reason == "no_stored_etag"

        # Matching ETag - should skip
        status_updates = [collect_status("TEST3", "sync", "completed", metadata={"encrypted_etag": '"abc"'})]
        await batch_write_status_updates(tracker.db_path, status_updates)
        should_skip, reason = await should_skip_download("TEST3", '"abc"', "local", {}, tracker, False)
        assert should_skip and reason == "database_etag_match"

        # Different ETag - should download
        status_updates = [collect_status("TEST4", "sync", "completed", metadata={"encrypted_etag": '"xyz"'})]
        await batch_write_status_updates(tracker.db_path, status_updates)
        should_skip, reason = await should_skip_download("TEST4", '"abc"', "local", {}, tracker, False)
        assert not should_skip and reason == "database_etag_mismatch"

        # Force flag - should download even with matching ETag
        should_skip, reason = await should_skip_download("TEST3", '"abc"', "local", {}, tracker, True)
        assert not should_skip and reason == "force_flag"

        # No Google ETag - should download
        should_skip, reason = await should_skip_download("TEST3", None, "local", {}, tracker, False)
        assert not should_skip and reason == "no_etag"

        await tracker.close()

    @pytest.mark.asyncio
    async def test_etag_quote_handling(self):
        tracker = await self.make_tracker()

        cases = [
            ('"abc"', '"abc"', True),
            ("abc", '"abc"', True),
            ('"abc"', "abc", True),
            ("abc", "abc", True),
            ('"abc"', '"xyz"', False),
        ]

        for i, (db_etag, encrypted_etag, expected) in enumerate(cases):
            barcode = f"TEST{i:03d}"
            status_updates = [collect_status(barcode, "sync", "completed", metadata={"encrypted_etag": db_etag})]
            await batch_write_status_updates(tracker.db_path, status_updates)
            should_skip, _ = await should_skip_download(barcode, encrypted_etag, "local", {}, tracker, False)
            assert should_skip == expected

        await tracker.close()


class TestETagSkipHandling:
    async def make_tracker(self):
        temp_dir = tempfile.mkdtemp()
        db_path = Path(temp_dir) / "test.db"
        tracker = SQLiteProgressTracker(str(db_path))
        await tracker.init_db()
        return tracker

    @pytest.mark.asyncio
    async def test_check_and_handle_etag_skip_scenarios(self, mock_semaphore):
        tracker = await self.make_tracker()

        # Test ETag match - should skip
        status_updates = [collect_status("TEST1", "sync", "completed", metadata={"encrypted_etag": '"abc"'})]
        await batch_write_status_updates(tracker.db_path, status_updates)

        # Set up a properly mocked grin_client
        mock_grin_client = AsyncMock()
        mock_response = MagicMock()
        mock_response.headers = {"ETag": '"abc"', "Content-Length": "1000"}
        mock_grin_client.auth.make_authenticated_request.return_value = mock_response

        with patch("grin_to_s3.common.create_http_session") as mock_session:
            mock_session.return_value.__aenter__.return_value = MagicMock()

            skip_result, etag, size, status_updates = await check_and_handle_etag_skip(
                "TEST1", mock_grin_client, "Harvard", "local", {}, tracker, mock_semaphore, False
            )

            assert skip_result is not None and skip_result["skipped"]
            assert etag == "abc" and size == 1000

            # Write the status updates to database
            if status_updates:
                await batch_write_status_updates(tracker.db_path, status_updates)

            status, metadata = await tracker.get_latest_status_with_metadata("TEST1", "sync")
            assert status == "skipped" and metadata and metadata.get("skipped")

        # Test ETag mismatch - should not skip
        status_updates = [collect_status("TEST2", "sync", "completed", metadata={"encrypted_etag": '"xyz"'})]
        await batch_write_status_updates(tracker.db_path, status_updates)

        # Set up another properly mocked grin_client with different ETag
        mock_grin_client2 = AsyncMock()
        mock_response2 = MagicMock()
        mock_response2.headers = {"ETag": '"abc"', "Content-Length": "1000"}
        mock_grin_client2.auth.make_authenticated_request.return_value = mock_response2

        with patch("grin_to_s3.common.create_http_session") as mock_session:
            mock_session.return_value.__aenter__.return_value = MagicMock()

            skip_result, etag, size, status_updates = await check_and_handle_etag_skip(
                "TEST2", mock_grin_client2, "Harvard", "local", {}, tracker, mock_semaphore, False
            )

            assert skip_result is None
            assert etag == "abc" and size == 1000

        await tracker.close()


class TestGetLatestStatusWithMetadata:
    async def make_tracker(self):
        temp_dir = tempfile.mkdtemp()
        db_path = Path(temp_dir) / "test.db"
        tracker = SQLiteProgressTracker(str(db_path))
        await tracker.init_db()
        return tracker

    @pytest.mark.asyncio
    async def test_get_latest_status_with_metadata_scenarios(self):
        tracker = await self.make_tracker()

        # Test with metadata
        metadata = {"encrypted_etag": '"abc"', "storage_type": "local"}
        status_updates = [collect_status("TEST1", "sync", "completed", metadata=metadata)]
        await batch_write_status_updates(tracker.db_path, status_updates)
        status, retrieved_metadata = await tracker.get_latest_status_with_metadata("TEST1", "sync")
        assert status == "completed" and retrieved_metadata == metadata

        # Test without metadata
        status_updates = [collect_status("TEST2", "sync", "completed")]
        await batch_write_status_updates(tracker.db_path, status_updates)
        status, metadata = await tracker.get_latest_status_with_metadata("TEST2", "sync")
        assert status == "completed" and metadata is None

        # Test not found
        status, metadata = await tracker.get_latest_status_with_metadata("NONEXISTENT", "sync")
        assert status is None and metadata is None

        # Test multiple entries - should get latest
        status_updates = [
            collect_status("TEST3", "sync", "started", metadata={"encrypted_etag": '"old"'}),
            collect_status("TEST3", "sync", "completed", metadata={"encrypted_etag": '"new"'}),
        ]
        await batch_write_status_updates(tracker.db_path, status_updates)
        status, metadata = await tracker.get_latest_status_with_metadata("TEST3", "sync")
        assert status == "completed" and metadata and metadata.get("encrypted_etag") == '"new"'

        await tracker.close()
