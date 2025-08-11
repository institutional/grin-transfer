#!/usr/bin/env python3
"""
Test archive availability checking and ETag handling for previous queue.

Tests the HEAD request functionality for checking archive availability
and ETag comparison for previously downloaded books.
"""

import asyncio
from unittest.mock import AsyncMock, Mock, patch

import aiohttp
import pytest

from grin_to_s3.client import GRINClient
from grin_to_s3.sync.operations import check_archive_availability_with_etag


@pytest.fixture
def mock_semaphore():
    """Mock semaphore for GRIN API concurrency testing."""
    return asyncio.Semaphore(5)


class TestArchiveAvailabilityChecking:
    """Test archive availability checking with HEAD requests."""

    @pytest.fixture
    def mock_grin_client(self):
        """Create mock GRIN client."""
        client = Mock(spec=GRINClient)
        client.auth = Mock()
        client.auth.make_authenticated_request = AsyncMock()
        return client

    @pytest.mark.asyncio
    async def test_archive_available_with_etag(self, mock_grin_client, mock_semaphore):
        """Test successful HEAD request with ETag returned."""
        # Mock successful HEAD response with ETag and Content-Length
        with patch("grin_to_s3.sync.operations.check_encrypted_etag") as mock_check:
            mock_check.return_value = ("test-etag-123", 1234567, 200)

            result = await check_archive_availability_with_etag("test_barcode", mock_grin_client, "test_library", mock_semaphore)

            assert result["available"] is True
            assert result["etag"] == "test-etag-123"
            assert result["file_size"] == 1234567
            assert result["http_status"] == 200
            assert result["needs_conversion"] is False

            # Verify the existing function was called correctly
            mock_check.assert_called_once_with(mock_grin_client, "test_library", "test_barcode", mock_semaphore)

    @pytest.mark.asyncio
    async def test_archive_available_no_etag(self, mock_grin_client, mock_semaphore):
        """Test successful HEAD request without ETag but with file size."""
        with patch("grin_to_s3.sync.operations.check_encrypted_etag") as mock_check:
            mock_check.return_value = (None, 1234567, 200)

            result = await check_archive_availability_with_etag("test_barcode", mock_grin_client, "test_library", mock_semaphore)

            assert result["available"] is True
            assert result["etag"] is None
            assert result["file_size"] == 1234567
            assert result["http_status"] == 200
            assert result["needs_conversion"] is False

    @pytest.mark.asyncio
    async def test_archive_not_available_404(self, mock_grin_client, mock_semaphore):
        """Test HEAD request returning 404 (archive not available)."""
        with patch("grin_to_s3.sync.operations.check_encrypted_etag") as mock_check:
            mock_check.return_value = (None, None, 404)

            result = await check_archive_availability_with_etag("test_barcode", mock_grin_client, "test_library", mock_semaphore)

            assert result["available"] is False
            assert result["etag"] is None
            assert result["file_size"] is None
            assert result["http_status"] == 404
            assert result["needs_conversion"] is True

    @pytest.mark.asyncio
    async def test_archive_check_exception(self, mock_grin_client, mock_semaphore):
        """Test exception during archive availability check bubbles up for retry."""
        with patch("grin_to_s3.sync.operations.check_encrypted_etag") as mock_check:
            mock_check.side_effect = Exception("Network error")

            # Non-HTTP errors should bubble up for upstream retry handling
            with pytest.raises(Exception, match="Network error"):
                await check_archive_availability_with_etag("test_barcode", mock_grin_client, "test_library", mock_semaphore)

    @pytest.mark.asyncio
    async def test_multiple_books_mixed_availability(self, mock_grin_client, mock_semaphore):
        """Test checking multiple books with different availability states."""
        test_cases = [
            ("available_book", ("etag-123", 1000000, 200)),
            ("unavailable_book", (None, None, 404)),
            ("partial_book", (None, 500000, 200)),
        ]

        with patch("grin_to_s3.sync.operations.check_encrypted_etag") as mock_check:
            for barcode, return_value in test_cases:
                mock_check.return_value = return_value

                result = await check_archive_availability_with_etag(barcode, mock_grin_client, "test_library", mock_semaphore)

                if barcode == "available_book":
                    assert result["available"] is True
                    assert result["etag"] == "etag-123"
                    assert result["needs_conversion"] is False
                elif barcode == "unavailable_book":
                    assert result["available"] is False
                    assert result["needs_conversion"] is True
                elif barcode == "partial_book":
                    # File size but no ETag still counts as available
                    assert result["available"] is True
                    assert result["etag"] is None
                    assert result["file_size"] == 500000
                    assert result["needs_conversion"] is False

    @pytest.mark.asyncio
    async def test_etag_comparison_logic(self, mock_grin_client, mock_semaphore):
        """Test ETag comparison logic for determining if download is needed."""
        # This tests the basic availability check - ETag comparison will be
        # handled in the sync pipeline logic
        with patch("grin_to_s3.sync.operations.check_encrypted_etag") as mock_check:
            test_etag = 'W/"abc123def456"'
            mock_check.return_value = (test_etag, 2000000, 200)

            result = await check_archive_availability_with_etag("test_barcode", mock_grin_client, "test_library", mock_semaphore)

            # Function should return the raw ETag for comparison by caller
            assert result["etag"] == test_etag
            assert result["available"] is True
            assert result["file_size"] == 2000000

    @pytest.mark.asyncio
    async def test_logging_behavior(self, mock_grin_client, caplog):
        """Test that appropriate log messages are generated."""
        import logging

        caplog.set_level(logging.DEBUG, logger="grin_to_s3.sync.operations")

        with patch("grin_to_s3.sync.operations.check_encrypted_etag") as mock_check:
            # Test unavailable case
            mock_check.return_value = (None, None, 404)

            await check_archive_availability_with_etag("test_barcode", mock_grin_client, "test_library", mock_semaphore)

            # Check debug log for unavailable archive
            assert "Archive not available (404), may need conversion" in caplog.text

    @pytest.mark.asyncio
    async def test_error_logging(self, mock_grin_client, caplog):
        """Test error logging when check_encrypted_etag raises non-HTTP exception."""
        with patch("grin_to_s3.sync.operations.check_encrypted_etag") as mock_check:
            mock_check.side_effect = aiohttp.ClientError("Connection failed")

            # Non-HTTP errors should bubble up for upstream retry handling
            with pytest.raises(aiohttp.ClientError, match="Connection failed"):
                await check_archive_availability_with_etag("test_barcode", mock_grin_client, "test_library", mock_semaphore)

