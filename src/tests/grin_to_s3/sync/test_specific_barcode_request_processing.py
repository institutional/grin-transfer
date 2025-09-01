#!/usr/bin/env python3
"""
Integration tests for request processing with specific barcodes.

Tests that when running sync pipeline with specific barcodes that aren't available
in GRIN, the request processing loop is activated to trigger conversion requests.
"""

from unittest.mock import AsyncMock, MagicMock, patch

import aiohttp
import pytest

from grin_to_s3.sync.tasks import check, request_conversion
from grin_to_s3.sync.tasks.task_types import TaskAction, TaskType


@pytest.mark.asyncio
async def test_specific_barcode_triggers_conversion_request(mock_pipeline):
    """Specific barcode returning 404 should trigger REQUEST_CONVERSION task."""

    barcode = "TEST_BARCODE_404"

    # Mock GRIN client to return 404 for HEAD request
    mock_grin_client = AsyncMock()
    mock_grin_client.head_archive.side_effect = aiohttp.ClientResponseError(
        request_info=None, history=(), status=404, message="Not Found"
    )

    # Mock pipeline components
    mock_pipeline.grin_client = mock_grin_client
    mock_pipeline.library_directory = "test_library"
    mock_pipeline.secrets_dir = None
    mock_pipeline.conversion_requests_made = 0

    # Mock the book manager async methods
    mock_pipeline.book_manager.get_decrypted_archive_metadata = AsyncMock(return_value=None)
    mock_pipeline.book_manager.get_archive_etag_from_s3 = AsyncMock(return_value=None)

    # Test the CHECK task first
    check_result = await check.main(barcode, mock_pipeline)

    # Verify CHECK task failed with the correct reason
    assert check_result.task_type == TaskType.CHECK
    assert check_result.action == TaskAction.FAILED
    assert check_result.reason == "fail_archive_missing"
    assert check_result.data["http_status_code"] == 404

    # Verify that CHECK task indicates REQUEST_CONVERSION should run next
    next_tasks = check_result.next_tasks()
    assert TaskType.REQUEST_CONVERSION in next_tasks

    # Mock the request_conversion function to simulate successful request
    with patch("grin_to_s3.sync.tasks.request_conversion.request_conversion") as mock_request:
        mock_request.return_value = "Success"

        # Test the REQUEST_CONVERSION task
        conversion_result = await request_conversion.main(barcode, mock_pipeline)

        # Verify conversion request was made
        assert conversion_result.task_type == TaskType.REQUEST_CONVERSION
        assert conversion_result.action == TaskAction.SKIPPED
        assert conversion_result.reason == "skip_conversion_requested"
        assert conversion_result.data["conversion_status"] == "requested"

        # Verify the request_conversion function was called
        mock_request.assert_called_once_with(barcode, "test_library", None)


@pytest.mark.asyncio
async def test_mixed_barcode_list_handles_available_and_missing(mock_pipeline):
    """Mixed list of barcodes should handle available and missing ones correctly."""

    # Mock pipeline components
    mock_grin_client = AsyncMock()
    mock_pipeline.grin_client = mock_grin_client
    mock_pipeline.library_directory = "test_library"

    # Mock book manager async methods
    mock_pipeline.book_manager.get_decrypted_archive_metadata = AsyncMock(return_value=None)
    mock_pipeline.book_manager.get_archive_etag_from_s3 = AsyncMock(return_value=None)

    # Test barcode that exists in GRIN (200 response)
    available_barcode = "AVAILABLE_BOOK"
    mock_grin_client.head_archive.return_value = MagicMock(
        status=200, headers={"ETag": '"abc123"', "Content-Length": "12345"}
    )

    available_result = await check.main(available_barcode, mock_pipeline)
    assert available_result.action == TaskAction.COMPLETED
    assert available_result.data["http_status_code"] == 200
    assert TaskType.REQUEST_CONVERSION not in available_result.next_tasks()

    # Test barcode that doesn't exist in GRIN (404 response)
    missing_barcode = "MISSING_BOOK"
    mock_grin_client.head_archive.side_effect = aiohttp.ClientResponseError(
        request_info=None, history=(), status=404, message="Not Found"
    )

    missing_result = await check.main(missing_barcode, mock_pipeline)
    assert missing_result.action == TaskAction.FAILED
    assert missing_result.reason == "fail_archive_missing"
    assert missing_result.data["http_status_code"] == 404
    assert TaskType.REQUEST_CONVERSION in missing_result.next_tasks()


@pytest.mark.asyncio
async def test_barcode_already_in_storage_but_not_in_grin(mock_pipeline):
    """Barcode in storage but not in GRIN should be skipped (not request conversion)."""

    barcode = "IN_STORAGE_NOT_GRIN"

    # Mock GRIN client to return 404
    mock_grin_client = AsyncMock()
    mock_grin_client.head_archive.side_effect = aiohttp.ClientResponseError(
        request_info=None, history=(), status=404, message="Not Found"
    )
    mock_pipeline.grin_client = mock_grin_client

    # Mock book manager async methods - return existing etag (book exists in storage)
    mock_pipeline.book_manager.get_decrypted_archive_metadata = AsyncMock(
        return_value={"encrypted_etag": '"stored_etag"'}
    )
    mock_pipeline.book_manager.get_archive_etag_from_s3 = AsyncMock(return_value='"stored_etag"')

    result = await check.main(barcode, mock_pipeline)

    # Should be skipped, not failed (so no conversion request)
    assert result.action == TaskAction.SKIPPED
    assert result.reason == "skip_found_in_storage_not_grin"
    assert result.data["http_status_code"] == 404
    assert TaskType.REQUEST_CONVERSION not in result.next_tasks()


@pytest.mark.asyncio
async def test_request_conversion_updates_counter(mock_pipeline):
    """REQUEST_CONVERSION task should increment the conversion requests counter."""

    barcode = "TEST_COUNTER"
    mock_pipeline.conversion_requests_made = 5
    mock_pipeline.library_directory = "test_library"
    mock_pipeline.secrets_dir = None

    with patch("grin_to_s3.sync.tasks.request_conversion.request_conversion") as mock_request:
        mock_request.return_value = "Success"

        result = await request_conversion.main(barcode, mock_pipeline)

        # Verify counter was incremented
        assert mock_pipeline.conversion_requests_made == 6
        assert result.data["request_count"] == 6
