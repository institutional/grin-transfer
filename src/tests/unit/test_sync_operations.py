#!/usr/bin/env python3
"""
Tests for core sync functions.
"""

from unittest.mock import patch

import pytest

from grin_to_s3.sync.operations import (
    check_and_handle_etag_skip,
    sync_book_to_local_storage,
    upload_book_from_staging,
)


class TestETagSkipHandling:
    """Test ETag skip handling functionality."""

    @pytest.mark.asyncio
    async def test_check_and_handle_etag_skip_no_skip(
        self, mock_grin_client, mock_progress_tracker, mock_storage_config
    ):
        """Test ETag check when file should not be skipped."""
        with patch('grin_to_s3.sync.operations.check_google_etag') as mock_check_etag, \
             patch('grin_to_s3.sync.operations.should_skip_download') as mock_should_skip:

            mock_check_etag.return_value = ("abc123", 1024)
            mock_should_skip.return_value = False

            result, etag, file_size = await check_and_handle_etag_skip(
                "TEST123", mock_grin_client, "Harvard", "minio", mock_storage_config, mock_progress_tracker
            )

            assert result is None  # No skip
            assert etag == "abc123"
            assert file_size == 1024

    @pytest.mark.asyncio
    async def test_check_and_handle_etag_skip_with_skip(
        self, mock_grin_client, mock_progress_tracker, mock_storage_config
    ):
        """Test ETag check when file should be skipped."""
        with patch('grin_to_s3.sync.operations.check_google_etag') as mock_check_etag, \
             patch('grin_to_s3.sync.operations.should_skip_download') as mock_should_skip:

            mock_check_etag.return_value = ("abc123", 1024)
            mock_should_skip.return_value = True

            result, etag, file_size = await check_and_handle_etag_skip(
                "TEST123", mock_grin_client, "Harvard", "minio", mock_storage_config, mock_progress_tracker
            )

            assert result is not None  # Skip result returned
            assert result["barcode"] == "TEST123"
            assert result["status"] == "completed"
            assert result["skipped"] is True
            assert result["google_etag"] == "abc123"
            assert result["file_size"] == 1024
            assert etag == "abc123"
            assert file_size == 1024


class TestBookDownload:
    """Test book download functionality."""

    # TODO: Fix async mocking issues with GRIN client
    # These tests need proper async mocking setup for the GRIN client's HTTP session


class TestBookUpload:
    """Test book upload functionality."""

    @pytest.mark.asyncio
    async def test_upload_book_from_staging_skip_scenario(
        self, mock_storage_config, mock_staging_manager, mock_progress_tracker
    ):
        """Test upload handling skip download scenario."""
        result = await upload_book_from_staging(
            "TEST123", "SKIP_DOWNLOAD", "minio", mock_storage_config,
            mock_staging_manager, mock_progress_tracker
        )

        assert result["barcode"] == "TEST123"
        assert result["status"] == "completed"
        assert result["skipped"] is True

    # TODO: Fix async mocking issues
    # @pytest.mark.asyncio
    # async def test_upload_book_from_staging_success(
    #     self, mock_storage_config, mock_staging_manager, mock_progress_tracker
    # ):
    #     """Test successful book upload from staging."""
    #     # This test needs fixing for async mocking
    #     pass


class TestLocalStorageSync:
    """Test local storage sync functionality."""

    # TODO: Fix async mocking issues
    # @pytest.mark.asyncio
    # async def test_sync_book_to_local_storage_success(self, mock_grin_client, mock_progress_tracker):
    #     """Test successful local storage sync."""
    #     # This test needs fixing for async mocking
    #     pass

    @pytest.mark.asyncio
    async def test_sync_book_to_local_storage_missing_base_path(self, mock_grin_client, mock_progress_tracker):
        """Test local storage sync with missing base_path."""
        storage_config = {}  # No base_path

        result = await sync_book_to_local_storage(
            "TEST123", mock_grin_client, "Harvard", storage_config, mock_progress_tracker
        )

        assert result["barcode"] == "TEST123"
        assert result["status"] == "failed"
        assert "Local storage requires" in result["error"]
