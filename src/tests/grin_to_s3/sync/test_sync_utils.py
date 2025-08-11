#!/usr/bin/env python3
"""
Tests for sync utility functions.
"""

import asyncio
import tempfile
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from grin_to_s3.processing import get_converted_books
from grin_to_s3.sync.utils import (
    check_encrypted_etag,
    should_skip_download,
)


@pytest.fixture
def mock_semaphore():
    """Mock semaphore for GRIN API concurrency testing."""
    return asyncio.Semaphore(5)


class TestETagOperations:
    """Test ETag-related utility functions."""

    @pytest.mark.asyncio
    async def test_check_encrypted_etag_success(self, mock_grin_client, mock_semaphore):
        """Test successful ETag retrieval from Google."""
        # Mock HTTP session and response
        mock_response = MagicMock()
        mock_response.headers = {"ETag": '"abc123def"', "Content-Length": "1024"}

        mock_grin_client.auth.make_authenticated_request.return_value = mock_response

        with patch("grin_to_s3.common.create_http_session") as mock_session:
            mock_session.return_value.__aenter__.return_value = MagicMock()

            etag, file_size, status_code = await check_encrypted_etag(mock_grin_client, "Harvard", "TEST123", mock_semaphore)

            assert etag == "abc123def"  # ETag should be stripped of quotes
            assert file_size == 1024
            assert status_code == 200

    @pytest.mark.asyncio
    async def test_check_encrypted_etag_no_etag(self, mock_grin_client, mock_semaphore):
        """Test ETag check when no ETag is present."""
        # Mock HTTP session and response without ETag
        mock_response = MagicMock()
        mock_response.headers = {"Content-Length": "2048"}

        mock_grin_client.auth.make_authenticated_request.return_value = mock_response

        with patch("grin_to_s3.common.create_http_session") as mock_session:
            mock_session.return_value.__aenter__.return_value = MagicMock()

            etag, file_size, status_code = await check_encrypted_etag(mock_grin_client, "Harvard", "TEST123", mock_semaphore)

            assert etag is None
            assert file_size == 2048
            assert status_code == 200

    @pytest.mark.asyncio
    async def test_check_encrypted_etag_error(self, mock_grin_client, mock_semaphore):
        """Test ETag check when request fails with non-HTTP error."""
        mock_grin_client.auth.make_authenticated_request.side_effect = Exception("Network error")

        with patch("grin_to_s3.common.create_http_session") as mock_session:
            mock_session.return_value.__aenter__.return_value = MagicMock()

            # Non-HTTP errors should bubble up for retry handling
            with pytest.raises(Exception, match="Network error"):
                await check_encrypted_etag(mock_grin_client, "Harvard", "TEST123", mock_semaphore)

    @pytest.mark.asyncio
    async def test_check_encrypted_etag_404_error(self, mock_grin_client, mock_semaphore):
        """Test ETag check when HEAD returns 404."""
        import aiohttp

        mock_grin_client.auth.make_authenticated_request.side_effect = aiohttp.ClientResponseError(
            request_info=None, history=None, status=404, message="Not Found"
        )

        with patch("grin_to_s3.common.create_http_session") as mock_session:
            mock_session.return_value.__aenter__.return_value = MagicMock()

            etag, file_size, status_code = await check_encrypted_etag(mock_grin_client, "Harvard", "TEST123", mock_semaphore)

            assert etag is None
            assert file_size is None
            assert status_code == 404

    @pytest.mark.asyncio
    async def test_should_skip_download_force_flag(self, mock_storage_config):
        """Test that force flag prevents skipping."""

        mock_tracker = AsyncMock()
        should_skip, reason = await should_skip_download("TEST123", "abc123", "local", {}, mock_tracker, force=True)
        assert should_skip is False
        assert reason == "force_flag"

    @pytest.mark.asyncio
    async def test_should_skip_download_no_etag(self, mock_storage_config):
        """Test that missing ETag prevents skipping."""

        mock_tracker = AsyncMock()
        should_skip, reason = await should_skip_download("TEST123", None, "local", {}, mock_tracker, force=False)
        assert should_skip is False
        assert reason == "no_etag"

    @pytest.mark.asyncio
    async def test_should_skip_download_database_error(self, mock_storage_config):
        """Test that database errors are handled gracefully."""

        mock_tracker = AsyncMock()
        mock_tracker.db_path = "/fake/path"

        with patch("grin_to_s3.sync.utils.connect_async") as mock_connect:
            mock_connect.side_effect = Exception("Database error")
            should_skip, reason = await should_skip_download(
                "TEST123", "abc123", "local", {}, mock_tracker, force=False
            )
            assert should_skip is False
            assert "error" in reason


class TestConvertedBooks:
    """Test converted books retrieval."""

    @pytest.mark.asyncio
    async def test_get_converted_books_success(self, mock_grin_client):
        """Test successful retrieval of converted books."""
        mock_response = "TEST123.tar.gz.gpg\nTEST456.tar.gz.gpg\nTEST789.tar.gz.gpg\n"
        mock_grin_client.fetch_resource.return_value = mock_response

        result = await get_converted_books(mock_grin_client, "Harvard")

        assert result == {"TEST123", "TEST456", "TEST789"}
        mock_grin_client.fetch_resource.assert_called_once_with("Harvard", "_converted?format=text")

    @pytest.mark.asyncio
    async def test_get_converted_books_empty_response(self, mock_grin_client):
        """Test handling of empty response."""
        mock_grin_client.fetch_resource.return_value = ""

        result = await get_converted_books(mock_grin_client, "Harvard")

        assert result == set()

    @pytest.mark.asyncio
    async def test_get_converted_books_mixed_content(self, mock_grin_client):
        """Test filtering of mixed content."""
        mock_response = """
        TEST123.tar.gz.gpg
        some_other_file.txt
        TEST456.tar.gz.gpg

        TEST789.tar.gz.gpg
        """
        mock_grin_client.fetch_resource.return_value = mock_response

        result = await get_converted_books(mock_grin_client, "Harvard")

        assert result == {"TEST123", "TEST456", "TEST789"}

    @pytest.mark.asyncio
    async def test_get_converted_books_error(self, mock_grin_client):
        """Test error handling in converted books retrieval."""
        mock_grin_client.fetch_resource.side_effect = Exception("Network error")

        result = await get_converted_books(mock_grin_client, "Harvard")

        assert result == set()


class TestBookManagerInitializationInUtils:
    """Test that sync utils correctly initialize BookManager with bucket_config."""

    @pytest.mark.asyncio
    async def test_should_skip_download_book_manager_initialization(self):
        """Test that should_skip_download correctly initializes BookManager."""

        with tempfile.TemporaryDirectory() as temp_dir:
            # Create proper storage config with bucket information
            storage_config = {
                "bucket_raw": "test-raw",
                "bucket_meta": "test-meta",
                "bucket_full": "test-full",
                "prefix": "",
                "config": {"base_path": temp_dir},
            }

            # Mock database tracker
            mock_db_tracker = MagicMock()

            with (
                patch("grin_to_s3.storage.create_storage_from_config") as mock_create_storage,
                patch("grin_to_s3.storage.BookManager") as mock_book_manager_class,
            ):
                # Mock storage creation
                mock_storage = MagicMock()
                mock_create_storage.return_value = mock_storage

                # Mock BookStorage instance
                mock_book_manager = MagicMock()
                mock_book_manager.decrypted_archive_exists = AsyncMock(return_value=False)
                mock_book_manager_class.return_value = mock_book_manager

                # This should NOT raise "missing bucket_config argument" error
                should_skip, reason = await should_skip_download(
                    "TEST123",
                    "test_etag",
                    "minio",  # S3-compatible storage to trigger the problematic code path
                    storage_config,
                    mock_db_tracker,
                    force=False,
                )

                # Verify the function completed without error
                assert should_skip is False
                assert reason == "no_decrypted_archive"

                # Verify BookStorage was called with correct arguments
                mock_book_manager_class.assert_called_once()
                call_args = mock_book_manager_class.call_args

                # Should be called with (storage, bucket_config=..., base_prefix=...)
                assert len(call_args[0]) == 1  # One positional arg: storage
                assert call_args[0][0] == mock_storage  # First arg is storage

                # bucket_config should be keyword argument
                assert "bucket_config" in call_args[1]
                bucket_config = call_args[1]["bucket_config"]
                assert isinstance(bucket_config, dict)
                assert "bucket_raw" in bucket_config
                assert "bucket_meta" in bucket_config
                assert "bucket_full" in bucket_config
