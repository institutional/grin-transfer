#!/usr/bin/env python3
"""
Tests for ETag storage metadata functionality with fresh database scenarios.
"""

import tempfile
from pathlib import Path
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from grin_to_s3.collect_books.models import SQLiteProgressTracker
from grin_to_s3.sync.utils import should_skip_download


class TestStorageMetadataETagTracking:
    """Test ETag tracking with storage metadata (S3/R2/MinIO scenarios)."""

    async def make_fresh_tracker(self):
        """Create a fresh tracker with empty database."""
        temp_dir = tempfile.mkdtemp()
        db_path = Path(temp_dir) / "fresh_test.db"
        tracker = SQLiteProgressTracker(str(db_path))
        await tracker.init_db()
        return tracker

    @pytest.mark.asyncio
    async def test_fresh_database_with_storage_metadata_match(self):
        """Test that storage metadata check works even with fresh database."""
        tracker = await self.make_fresh_tracker()

        storage_config = {
            "bucket_raw": "test-bucket",
            "access_key": "test-key",
            "secret_key": "test-secret",
        }

        with (
            patch("grin_to_s3.common.create_storage_from_config") as mock_storage_factory,
            patch("grin_to_s3.storage.BookStorage") as mock_book_storage_class,
        ):
            # Mock storage and book storage
            mock_storage = MagicMock()
            mock_storage_factory.return_value = mock_storage
            mock_book_storage = AsyncMock()
            mock_book_storage_class.return_value = mock_book_storage

            # Mock that decrypted archive exists and ETag matches
            mock_book_storage.decrypted_archive_exists.return_value = True
            mock_book_storage.archive_matches_encrypted_etag.return_value = True

            # Test S3 storage with fresh database - should skip based on storage metadata
            should_skip, reason = await should_skip_download(
                "TEST123", '"etag123"', "s3", storage_config, tracker, False
            )

            assert should_skip is True
            assert reason == "storage_etag_match"

            # Verify that storage metadata was checked
            mock_book_storage.decrypted_archive_exists.assert_called_once_with("TEST123")
            mock_book_storage.archive_matches_encrypted_etag.assert_called_once_with("TEST123", '"etag123"')

        await tracker.close()

    @pytest.mark.asyncio
    async def test_fresh_database_with_storage_metadata_mismatch(self):
        """Test that storage metadata mismatch triggers download even with fresh database."""
        tracker = await self.make_fresh_tracker()

        storage_config = {
            "bucket_raw": "test-bucket",
            "access_key": "test-key",
            "secret_key": "test-secret",
        }

        with (
            patch("grin_to_s3.common.create_storage_from_config") as mock_storage_factory,
            patch("grin_to_s3.storage.BookStorage") as mock_book_storage_class,
        ):
            # Mock storage and book storage
            mock_storage = MagicMock()
            mock_storage_factory.return_value = mock_storage
            mock_book_storage = AsyncMock()
            mock_book_storage_class.return_value = mock_book_storage

            # Mock that decrypted archive exists but ETag doesn't match
            mock_book_storage.decrypted_archive_exists.return_value = True
            mock_book_storage.archive_matches_encrypted_etag.return_value = False

            # Test S3 storage with fresh database - should not skip due to ETag mismatch
            should_skip, reason = await should_skip_download(
                "TEST123", '"new_etag"', "s3", storage_config, tracker, False
            )

            assert should_skip is False
            assert reason == "storage_etag_mismatch"

        await tracker.close()

    @pytest.mark.asyncio
    async def test_fresh_database_no_decrypted_archive(self):
        """Test that missing decrypted archive triggers download with fresh database."""
        tracker = await self.make_fresh_tracker()

        storage_config = {
            "bucket_raw": "test-bucket",
            "access_key": "test-key",
            "secret_key": "test-secret",
        }

        with (
            patch("grin_to_s3.common.create_storage_from_config") as mock_storage_factory,
            patch("grin_to_s3.storage.BookStorage") as mock_book_storage_class,
        ):
            # Mock storage and book storage
            mock_storage = MagicMock()
            mock_storage_factory.return_value = mock_storage
            mock_book_storage = AsyncMock()
            mock_book_storage_class.return_value = mock_book_storage

            # Mock that decrypted archive doesn't exist
            mock_book_storage.decrypted_archive_exists.return_value = False

            # Test S3 storage with fresh database - should not skip since no archive exists
            should_skip, reason = await should_skip_download(
                "TEST123", '"etag123"', "s3", storage_config, tracker, False
            )

            assert should_skip is False
            assert reason == "no_decrypted_archive"

            # Verify that only existence check was called (not ETag check)
            mock_book_storage.decrypted_archive_exists.assert_called_once_with("TEST123")
            mock_book_storage.archive_matches_encrypted_etag.assert_not_called()

        await tracker.close()

    @pytest.mark.asyncio
    async def test_fresh_database_fallback_to_database_check(self):
        """Test fallback to database check when storage metadata fails."""
        tracker = await self.make_fresh_tracker()

        storage_config = {
            "bucket_raw": "test-bucket",
            "access_key": "test-key",
            "secret_key": "test-secret",
        }

        with (
            patch("grin_to_s3.common.create_storage_from_config") as mock_storage_factory,
            patch("grin_to_s3.storage.BookStorage") as mock_book_storage_class,
        ):
            # Mock storage and book storage
            mock_storage = MagicMock()
            mock_storage_factory.return_value = mock_storage
            mock_book_storage = AsyncMock()
            mock_book_storage_class.return_value = mock_book_storage

            # Mock that storage metadata check fails
            mock_book_storage.decrypted_archive_exists.side_effect = Exception("Storage error")

            # Test S3 storage with storage error - should fall back to database check
            should_skip, reason = await should_skip_download(
                "TEST123", '"etag123"', "s3", storage_config, tracker, False
            )

            # With fresh database, should not skip since no metadata is in DB
            assert should_skip is False
            assert reason == "no_metadata"

        await tracker.close()

    @pytest.mark.asyncio
    async def test_s3_protocol_storage_configuration(self):
        """Test that S3 protocol storage uses metadata approach."""
        tracker = await self.make_fresh_tracker()

        # Test with S3 protocol (covers AWS S3, MinIO, R2 which all use 's3' protocol)
        storage_config = {"bucket_raw": "test-bucket"}

        with (
            patch("grin_to_s3.common.create_storage_from_config") as mock_storage_factory,
            patch("grin_to_s3.storage.BookStorage") as mock_book_storage_class,
        ):
            # Mock storage and book storage
            mock_storage = MagicMock()
            mock_storage_factory.return_value = mock_storage
            mock_book_storage = AsyncMock()
            mock_book_storage_class.return_value = mock_book_storage

            # Mock that decrypted archive exists and ETag matches
            mock_book_storage.decrypted_archive_exists.return_value = True
            mock_book_storage.archive_matches_encrypted_etag.return_value = True

            # Test S3 protocol uses metadata approach
            should_skip, reason = await should_skip_download(
                "TEST123", '"etag123"', "s3", storage_config, tracker, False
            )

            assert should_skip is True
            assert reason == "storage_etag_match"

        await tracker.close()

    async def test_storage_config_protocol_mapping(self):
        """Test that StorageConfig maps R2/MinIO to 's3' protocol."""
        from grin_to_s3.storage import StorageConfig

        # Test that all S3-compatible configs use 's3' protocol
        s3_config = StorageConfig.s3("bucket")
        assert s3_config.protocol == "s3"

        r2_config = StorageConfig.r2("account", "key", "secret")
        assert r2_config.protocol == "s3"  # R2 uses S3 protocol

        minio_config = StorageConfig.minio("http://localhost:9000", "key", "secret")
        assert minio_config.protocol == "s3"  # MinIO uses S3 protocol

        local_config = StorageConfig.local("/tmp")
        assert local_config.protocol == "file"  # Local uses file protocol

    @pytest.mark.asyncio
    async def test_local_storage_uses_database_approach(self):
        """Test that local storage still uses database approach with fresh database."""
        tracker = await self.make_fresh_tracker()

        storage_config = {"base_path": "/tmp/test"}

        # Test local storage with fresh database - should not skip since no metadata in DB
        should_skip, reason = await should_skip_download(
            "TEST123", '"etag123"', "local", storage_config, tracker, False
        )

        assert should_skip is False
        assert reason == "no_metadata"

        await tracker.close()


class TestHybridETagApproach:
    """Test the hybrid ETag approach with different scenarios."""

    async def make_tracker_with_metadata(self):
        """Create a tracker with some ETag metadata."""
        temp_dir = tempfile.mkdtemp()
        db_path = Path(temp_dir) / "test_with_metadata.db"
        tracker = SQLiteProgressTracker(str(db_path))
        await tracker.init_db()

        # Add some test metadata
        await tracker.add_status_change(
            "BOOK_WITH_ETAG", "sync", "completed", metadata={"encrypted_etag": '"stored_etag"'}
        )

        return tracker

    @pytest.mark.asyncio
    async def test_block_storage_bypasses_database_when_available(self):
        """Test that block storage uses metadata even when database has data."""
        tracker = await self.make_tracker_with_metadata()

        storage_config = {"bucket_raw": "test-bucket"}

        with (
            patch("grin_to_s3.common.create_storage_from_config") as mock_storage_factory,
            patch("grin_to_s3.storage.BookStorage") as mock_book_storage_class,
        ):
            # Mock storage and book storage
            mock_storage = MagicMock()
            mock_storage_factory.return_value = mock_storage
            mock_book_storage = AsyncMock()
            mock_book_storage_class.return_value = mock_book_storage

            # Mock that decrypted archive exists and ETag matches in storage
            mock_book_storage.decrypted_archive_exists.return_value = True
            mock_book_storage.archive_matches_encrypted_etag.return_value = True

            # Test that S3 uses storage metadata, not database
            should_skip, reason = await should_skip_download(
                "BOOK_WITH_ETAG", '"current_etag"', "s3", storage_config, tracker, False
            )

            assert should_skip is True
            assert reason == "storage_etag_match"  # Storage metadata was used

            # Verify storage methods were called
            mock_book_storage.decrypted_archive_exists.assert_called_once()
            mock_book_storage.archive_matches_encrypted_etag.assert_called_once()

        await tracker.close()

    @pytest.mark.asyncio
    async def test_local_storage_uses_database_when_available(self):
        """Test that local storage uses database when data is available."""
        tracker = await self.make_tracker_with_metadata()

        storage_config = {"base_path": "/tmp/test"}

        # Test that local storage uses database metadata for matching ETag
        should_skip, reason = await should_skip_download(
            "BOOK_WITH_ETAG", '"stored_etag"', "local", storage_config, tracker, False
        )

        assert should_skip is True
        assert reason == "database_etag_match"  # Database was used

        await tracker.close()
