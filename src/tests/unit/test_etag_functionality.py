#!/usr/bin/env python3
"""
Unit tests for ETag functionality in SyncPipeline

Tests the new ETag checking and optimization methods added to sync.py
"""

import tempfile
import unittest
from pathlib import Path
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from grin_to_s3.sync import SyncPipeline


class TestETagFunctionality(unittest.TestCase):
    """Test ETag infrastructure in SyncPipeline class."""

    def setUp(self):
        """Set up test environment."""
        self.temp_dir = tempfile.mkdtemp()
        self.db_path = Path(self.temp_dir) / "test.db"

        # Create minimal pipeline instance for testing
        self.pipeline = SyncPipeline(
            db_path=str(self.db_path),
            storage_type="r2",
            storage_config={
                "bucket_raw": "test-bucket",
                "prefix": "test-prefix",
                "key": "test-key",
                "secret": "test-secret",
            },
            library_directory="Harvard",
            force=False,
        )

    @pytest.mark.asyncio
    async def test_check_google_etag_success(self):
        """Test successful Google ETag check via HEAD request."""
        mock_response = MagicMock()
        mock_response.headers = {
            'ETag': '"abc123def456"',
            'Content-Length': '1024000'
        }

        with patch('grin_to_s3.common.create_http_session') as mock_session:
            mock_session.return_value.__aenter__.return_value = MagicMock()
            self.pipeline.grin_client = MagicMock()
            self.pipeline.grin_client.auth.make_authenticated_request = AsyncMock(return_value=mock_response)

            etag, file_size = await self.pipeline._check_google_etag("12345")

            self.assertEqual(etag, "abc123def456")  # Should strip quotes
            self.assertEqual(file_size, 1024000)

    @pytest.mark.asyncio
    async def test_check_google_etag_no_etag(self):
        """Test Google ETag check when no ETag header is present."""
        mock_response = MagicMock()
        mock_response.headers = {
            'Content-Length': '2048000'
        }

        with patch('grin_to_s3.common.create_http_session') as mock_session:
            mock_session.return_value.__aenter__.return_value = MagicMock()
            self.pipeline.grin_client = MagicMock()
            self.pipeline.grin_client.auth.make_authenticated_request = AsyncMock(return_value=mock_response)

            etag, file_size = await self.pipeline._check_google_etag("12345")

            self.assertIsNone(etag)
            self.assertEqual(file_size, 2048000)

    @pytest.mark.asyncio
    async def test_check_google_etag_failure(self):
        """Test Google ETag check when request fails."""
        with patch('grin_to_s3.common.create_http_session') as mock_session:
            mock_session.return_value.__aenter__.return_value = MagicMock()
            self.pipeline.grin_client = MagicMock()
            self.pipeline.grin_client.auth.make_authenticated_request = AsyncMock(
                side_effect=Exception("Network error")
            )

            etag, file_size = await self.pipeline._check_google_etag("12345")

            self.assertIsNone(etag)
            self.assertIsNone(file_size)

    @pytest.mark.asyncio
    async def test_should_skip_download_force_flag(self):
        """Test that force flag bypasses ETag checks."""
        self.pipeline.force = True

        result = await self.pipeline._should_skip_download("12345", "abc123")

        self.assertFalse(result)

    @pytest.mark.asyncio
    async def test_should_skip_download_no_etag(self):
        """Test that missing ETag prevents skipping."""
        self.pipeline.force = False

        result = await self.pipeline._should_skip_download("12345", None)

        self.assertFalse(result)

    @pytest.mark.asyncio
    async def test_should_skip_download_local_storage(self):
        """Test that local storage doesn't support ETag checking."""
        self.pipeline.storage_type = "local"
        self.pipeline.force = False

        result = await self.pipeline._should_skip_download("12345", "abc123")

        self.assertFalse(result)

    @pytest.mark.asyncio
    async def test_should_skip_download_etag_match(self):
        """Test skipping download when ETag matches."""
        self.pipeline.force = False

        with patch('grin_to_s3.common.create_storage_from_config') as mock_create_storage:
            mock_storage = MagicMock()
            mock_create_storage.return_value = mock_storage

            with patch('grin_to_s3.storage.BookStorage') as mock_book_storage_class:
                mock_book_storage = mock_book_storage_class.return_value
                mock_book_storage.archive_exists = AsyncMock(return_value=True)
                mock_book_storage.archive_matches_google_etag = AsyncMock(return_value=True)

                result = await self.pipeline._should_skip_download("12345", "abc123")

                self.assertTrue(result)
                mock_book_storage.archive_exists.assert_called_once_with("12345")
                mock_book_storage.archive_matches_google_etag.assert_called_once_with("12345", "abc123")

    @pytest.mark.asyncio
    async def test_should_skip_download_etag_mismatch(self):
        """Test not skipping download when ETag differs."""
        self.pipeline.force = False

        with patch('grin_to_s3.common.create_storage_from_config') as mock_create_storage:
            mock_storage = MagicMock()
            mock_create_storage.return_value = mock_storage

            with patch('grin_to_s3.storage.BookStorage') as mock_book_storage_class:
                mock_book_storage = mock_book_storage_class.return_value
                mock_book_storage.archive_exists = AsyncMock(return_value=True)
                mock_book_storage.archive_matches_google_etag = AsyncMock(return_value=False)

                result = await self.pipeline._should_skip_download("12345", "abc123")

                self.assertFalse(result)

    @pytest.mark.asyncio
    async def test_should_skip_download_file_not_exists(self):
        """Test not skipping download when file doesn't exist."""
        self.pipeline.force = False

        with patch('grin_to_s3.common.create_storage_from_config') as mock_create_storage:
            mock_storage = MagicMock()
            mock_create_storage.return_value = mock_storage

            with patch('grin_to_s3.storage.BookStorage') as mock_book_storage_class:
                mock_book_storage = mock_book_storage_class.return_value
                mock_book_storage.archive_exists = AsyncMock(return_value=False)

                result = await self.pipeline._should_skip_download("12345", "abc123")

                self.assertFalse(result)
                # Should not check ETag if file doesn't exist
                mock_book_storage.archive_matches_google_etag.assert_not_called()

    @pytest.mark.asyncio
    async def test_should_skip_download_error_handling(self):
        """Test error handling during ETag check."""
        self.pipeline.force = False

        with patch('grin_to_s3.common.create_storage_from_config') as mock_create_storage:
            mock_create_storage.side_effect = Exception("Storage creation failed")

            result = await self.pipeline._should_skip_download("12345", "abc123")

            self.assertFalse(result)

    @pytest.mark.asyncio
    async def test_download_only_skip_scenario(self):
        """Test _download_only returns skip metadata when ETag matches."""
        with patch.object(self.pipeline, '_check_google_etag') as mock_check_etag:
            mock_check_etag.return_value = ("abc123", 1024000)

            with patch.object(self.pipeline, '_should_skip_download') as mock_should_skip:
                mock_should_skip.return_value = True

                barcode, staging_file, metadata = await self.pipeline._download_only("12345")

                self.assertEqual(barcode, "12345")
                self.assertEqual(staging_file, "SKIP_DOWNLOAD")
                self.assertEqual(metadata["file_size"], 1024000)
                self.assertEqual(metadata["google_etag"], "abc123")
                self.assertTrue(metadata["skipped"])
                self.assertEqual(self.pipeline.stats["skipped"], 1)

    @pytest.mark.asyncio
    async def test_upload_book_skip_scenario(self):
        """Test _upload_book handles skip scenario correctly."""
        with patch.object(self.pipeline.db_tracker, 'update_sync_data') as mock_update_sync:
            mock_update_sync.return_value = True

            result = await self.pipeline._upload_book("12345", "SKIP_DOWNLOAD", "abc123")

            self.assertEqual(result["status"], "completed")
            self.assertTrue(result["skipped"])
            self.assertTrue(result["encrypted_success"])
            self.assertTrue(result["decrypted_success"])

            # Should update sync data with ETag info
            mock_update_sync.assert_called_once()
            call_args = mock_update_sync.call_args[0]
            self.assertEqual(call_args[0], "12345")  # barcode
            sync_data = call_args[1]
            self.assertEqual(sync_data["google_etag"], "abc123")
            self.assertIn("last_etag_check", sync_data)

    def tearDown(self):
        """Clean up test environment."""
        import shutil
        shutil.rmtree(self.temp_dir, ignore_errors=True)


if __name__ == "__main__":
    # Run tests
    unittest.main()
