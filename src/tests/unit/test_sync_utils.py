#!/usr/bin/env python3
"""
Tests for sync utility functions.
"""

from unittest.mock import MagicMock, patch

import pytest

from grin_to_s3.sync.utils import (
    check_encrypted_etag,
    ensure_bucket_exists,
    get_converted_books,
    reset_bucket_cache,
    should_skip_download,
)


class TestBucketOperations:
    """Test bucket creation and management utilities."""

    def test_reset_bucket_cache(self):
        """Test that bucket cache can be reset."""
        # Add something to cache
        from grin_to_s3.sync.utils import _bucket_checked_cache

        _bucket_checked_cache.add("test:bucket")
        assert len(_bucket_checked_cache) > 0

        # Reset and verify empty
        reset_bucket_cache()
        assert len(_bucket_checked_cache) == 0

    @pytest.mark.asyncio
    async def test_ensure_bucket_exists_local_storage(self, mock_storage_config):
        """Test bucket existence check for local storage."""
        result = await ensure_bucket_exists("local", mock_storage_config, "test-bucket")
        assert result is True

    @pytest.mark.asyncio
    async def test_ensure_bucket_exists_cached(self, mock_storage_config):
        """Test bucket existence check with cached result."""
        # Add to cache
        from grin_to_s3.sync.utils import _bucket_checked_cache

        _bucket_checked_cache.add("minio:test-bucket")

        result = await ensure_bucket_exists("minio", mock_storage_config, "test-bucket")
        assert result is True

    @pytest.mark.asyncio
    @patch("boto3.client")
    async def test_ensure_bucket_exists_s3_success(self, mock_boto3_client, mock_storage_config):
        """Test successful bucket creation for S3-compatible storage."""
        # Mock S3 client
        mock_s3 = MagicMock()
        mock_boto3_client.return_value = mock_s3
        mock_s3.head_bucket.return_value = None  # Bucket exists

        reset_bucket_cache()  # Clear cache
        result = await ensure_bucket_exists("s3", mock_storage_config, "test-bucket")
        assert result is True

        # Verify correct client configuration (S3 without custom endpoint)
        mock_boto3_client.assert_called_once_with(
            "s3",
            aws_access_key_id="test-access",
            aws_secret_access_key="test-secret",
        )

    @pytest.mark.asyncio
    @patch("boto3.client")
    async def test_ensure_bucket_exists_create_bucket(self, mock_boto3_client, mock_storage_config):
        """Test bucket creation when bucket doesn't exist."""
        from botocore.exceptions import ClientError

        # Mock S3 client
        mock_s3 = MagicMock()
        mock_boto3_client.return_value = mock_s3

        # Mock bucket doesn't exist, then creation succeeds
        mock_s3.head_bucket.side_effect = ClientError(
            error_response={"Error": {"Code": "404"}}, operation_name="HeadBucket"
        )
        mock_s3.create_bucket.return_value = None
        mock_s3.list_buckets.return_value = {"Buckets": [{"Name": "test-bucket"}]}

        reset_bucket_cache()  # Clear cache
        result = await ensure_bucket_exists("s3", mock_storage_config, "test-bucket")
        assert result is True

        mock_s3.create_bucket.assert_called_once_with(Bucket="test-bucket")


class TestETagOperations:
    """Test ETag-related utility functions."""

    @pytest.mark.asyncio
    async def test_check_encrypted_etag_success(self, mock_grin_client):
        """Test successful ETag retrieval from Google."""
        # Mock HTTP session and response
        mock_response = MagicMock()
        mock_response.headers = {"ETag": '"abc123def"', "Content-Length": "1024"}

        mock_grin_client.auth.make_authenticated_request.return_value = mock_response

        with patch("grin_to_s3.common.create_http_session") as mock_session:
            mock_session.return_value.__aenter__.return_value = MagicMock()

            etag, file_size = await check_encrypted_etag(mock_grin_client, "Harvard", "TEST123")

            assert etag == "abc123def"  # ETag should be stripped of quotes
            assert file_size == 1024

    @pytest.mark.asyncio
    async def test_check_encrypted_etag_no_etag(self, mock_grin_client):
        """Test ETag check when no ETag is present."""
        # Mock HTTP session and response without ETag
        mock_response = MagicMock()
        mock_response.headers = {"Content-Length": "2048"}

        mock_grin_client.auth.make_authenticated_request.return_value = mock_response

        with patch("grin_to_s3.common.create_http_session") as mock_session:
            mock_session.return_value.__aenter__.return_value = MagicMock()

            etag, file_size = await check_encrypted_etag(mock_grin_client, "Harvard", "TEST123")

            assert etag is None
            assert file_size == 2048

    @pytest.mark.asyncio
    async def test_check_encrypted_etag_error(self, mock_grin_client):
        """Test ETag check when request fails."""
        mock_grin_client.auth.make_authenticated_request.side_effect = Exception("Network error")

        with patch("grin_to_s3.common.create_http_session") as mock_session:
            mock_session.return_value.__aenter__.return_value = MagicMock()

            etag, file_size = await check_encrypted_etag(mock_grin_client, "Harvard", "TEST123")

            assert etag is None
            assert file_size is None

    @pytest.mark.asyncio
    async def test_should_skip_download_force_flag(self, mock_storage_config):
        """Test that force flag prevents skipping."""
        from unittest.mock import AsyncMock
        mock_tracker = AsyncMock()
        should_skip, reason = await should_skip_download("TEST123", "abc123", "local", {}, mock_tracker, force=True)
        assert should_skip is False
        assert reason == "force_flag"

    @pytest.mark.asyncio
    async def test_should_skip_download_no_etag(self, mock_storage_config):
        """Test that missing ETag prevents skipping."""
        from unittest.mock import AsyncMock
        mock_tracker = AsyncMock()
        should_skip, reason = await should_skip_download("TEST123", None, "local", {}, mock_tracker, force=False)
        assert should_skip is False
        assert reason == "no_etag"

    @pytest.mark.asyncio
    async def test_should_skip_download_database_error(self, mock_storage_config):
        """Test that database errors are handled gracefully."""
        from unittest.mock import AsyncMock, patch
        mock_tracker = AsyncMock()
        mock_tracker.db_path = "/fake/path"

        with patch("grin_to_s3.sync.utils.aiosqlite.connect") as mock_connect:
            mock_connect.side_effect = Exception("Database error")
            should_skip, reason = await should_skip_download("TEST123", "abc123", "local", {}, mock_tracker, force=False)
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
