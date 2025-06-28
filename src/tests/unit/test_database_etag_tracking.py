#!/usr/bin/env python3
"""
Tests for database ETag tracking functionality.
"""

import tempfile
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest

from grin_to_s3.collect_books.models import SQLiteProgressTracker
from grin_to_s3.sync.operations import check_and_handle_etag_skip
from grin_to_s3.sync.utils import should_skip_download


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
        should_skip, reason = await should_skip_download("TEST1", '"abc"', tracker, False)
        assert not should_skip and reason == "no_metadata"

        # Metadata without ETag - should download
        await tracker.add_status_change("TEST2", "sync", "completed", metadata={"storage_type": "local"})
        should_skip, reason = await should_skip_download("TEST2", '"abc"', tracker, False)
        assert not should_skip and reason == "no_stored_etag"

        # Matching ETag - should skip
        await tracker.add_status_change("TEST3", "sync", "completed", metadata={"grin_etag": '"abc"'})
        should_skip, reason = await should_skip_download("TEST3", '"abc"', tracker, False)
        assert should_skip and reason == "etag_match"

        # Different ETag - should download
        await tracker.add_status_change("TEST4", "sync", "completed", metadata={"grin_etag": '"xyz"'})
        should_skip, reason = await should_skip_download("TEST4", '"abc"', tracker, False)
        assert not should_skip and reason == "etag_mismatch"

        # Force flag - should download even with matching ETag
        should_skip, reason = await should_skip_download("TEST3", '"abc"', tracker, True)
        assert not should_skip and reason == "force_flag"

        # No Google ETag - should download
        should_skip, reason = await should_skip_download("TEST3", None, tracker, False)
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

        for i, (db_etag, google_etag, expected) in enumerate(cases):
            barcode = f"TEST{i:03d}"
            await tracker.add_status_change(barcode, "sync", "completed", metadata={"grin_etag": db_etag})
            should_skip, _ = await should_skip_download(barcode, google_etag, tracker, False)
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
    async def test_check_and_handle_etag_skip_scenarios(self):
        tracker = await self.make_tracker()

        # Test ETag match - should skip
        await tracker.add_status_change("TEST1", "sync", "completed", metadata={"grin_etag": '"abc"'})

        with patch("grin_to_s3.sync.operations.check_google_etag") as mock_check:
            mock_check.return_value = ('"abc"', 1000)

            skip_result, etag, size = await check_and_handle_etag_skip(
                "TEST1", MagicMock(), "Harvard", "local", {}, tracker, False
            )

            assert skip_result is not None and skip_result["skipped"]
            assert etag == '"abc"' and size == 1000

            status, metadata = await tracker.get_latest_status_with_metadata("TEST1", "sync")
            assert status == "skipped" and metadata["skipped"]

        # Test ETag mismatch - should not skip
        await tracker.add_status_change("TEST2", "sync", "completed", metadata={"grin_etag": '"xyz"'})

        with patch("grin_to_s3.sync.operations.check_google_etag") as mock_check:
            mock_check.return_value = ('"abc"', 1000)

            skip_result, etag, size = await check_and_handle_etag_skip(
                "TEST2", MagicMock(), "Harvard", "local", {}, tracker, False
            )

            assert skip_result is None
            assert etag == '"abc"' and size == 1000

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
        metadata = {"grin_etag": '"abc"', "storage_type": "local"}
        await tracker.add_status_change("TEST1", "sync", "completed", metadata=metadata)
        status, retrieved_metadata = await tracker.get_latest_status_with_metadata("TEST1", "sync")
        assert status == "completed" and retrieved_metadata == metadata

        # Test without metadata
        await tracker.add_status_change("TEST2", "sync", "completed")
        status, metadata = await tracker.get_latest_status_with_metadata("TEST2", "sync")
        assert status == "completed" and metadata is None

        # Test not found
        status, metadata = await tracker.get_latest_status_with_metadata("NONEXISTENT", "sync")
        assert status is None and metadata is None

        # Test multiple entries - should get latest
        await tracker.add_status_change("TEST3", "sync", "started", metadata={"grin_etag": '"old"'})
        await tracker.add_status_change("TEST3", "sync", "completed", metadata={"grin_etag": '"new"'})
        status, metadata = await tracker.get_latest_status_with_metadata("TEST3", "sync")
        assert status == "completed" and metadata["grin_etag"] == '"new"'

        await tracker.close()