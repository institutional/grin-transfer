#!/usr/bin/env python3
"""
Tests for sync tasks check module.
"""

from unittest.mock import AsyncMock, MagicMock

import aiohttp
import pytest
from botocore.exceptions import ClientError

from grin_to_s3.sync.tasks import check
from grin_to_s3.sync.tasks.task_types import TaskAction


@pytest.mark.asyncio
async def test_main_needs_download(mock_pipeline):
    """Check task should complete when file needs download."""
    # Mock HEAD response
    response = MagicMock()
    response.status = 200
    response.headers = {"ETag": '"abc123"', "Content-Length": "1024"}
    mock_pipeline.grin_client.head_archive.return_value = response

    # Mock no storage
    mock_pipeline.book_manager.get_decrypted_archive_metadata = AsyncMock(return_value={})

    result = await check.main("TEST123", mock_pipeline)

    assert result.action == TaskAction.COMPLETED
    assert result.data is not None
    assert result.data["etag"] == "abc123"
    assert result.data["file_size_bytes"] == 1024


@pytest.mark.asyncio
async def test_main_etag_match_skips(mock_pipeline):
    """Check task should skip when etag matches existing file."""
    response = MagicMock()
    response.status = 200
    response.headers = {"ETag": '"abc123"', "Content-Length": "1024"}
    mock_pipeline.grin_client.head_archive.return_value = response

    # Mock storage with matching etag
    mock_pipeline.book_manager.get_decrypted_archive_metadata = AsyncMock(return_value={"encrypted_etag": "abc123"})

    result = await check.main("TEST123", mock_pipeline)

    assert result.action == TaskAction.SKIPPED
    assert result.reason == "skip_etag_match"


@pytest.mark.asyncio
async def test_main_etag_continue_if_force(mock_pipeline):
    """Check task should return success even if etag matches, if force is provided."""
    response = MagicMock()
    response.status = 200
    response.headers = {"ETag": '"abc123"', "Content-Length": "1024"}
    mock_pipeline.grin_client.head_archive.return_value = response
    mock_pipeline.force = True

    # Mock storage with matching etag
    mock_pipeline.book_manager.get_decrypted_archive_metadata = AsyncMock(return_value={"encrypted_etag": "abc123"})

    result = await check.main("TEST123", mock_pipeline)

    assert result.action == TaskAction.COMPLETED
    assert result.reason == "completed_match_with_force"


@pytest.mark.asyncio
async def test_main_404_fail_no_storage(mock_pipeline):
    """Check task should fail when file not found in GRIN and not in storage."""
    error_404 = aiohttp.ClientResponseError(request_info=MagicMock(), history=(), status=404, message="Not Found")
    mock_pipeline.grin_client.head_archive.side_effect = error_404

    # Mock no storage
    mock_pipeline.book_manager.get_decrypted_archive_metadata = AsyncMock(return_value={})

    result = await check.main("TEST123", mock_pipeline)

    assert result.action == TaskAction.FAILED
    assert result.reason == "fail_archive_missing"


@pytest.mark.asyncio
async def test_main_storage_exists_grin_404_skip(mock_pipeline):
    """Check task should skip when book exists in storage but GRIN returns 404."""
    error_404 = aiohttp.ClientResponseError(request_info=MagicMock(), history=(), status=404, message="Not Found")
    mock_pipeline.grin_client.head_archive.side_effect = error_404

    # Mock storage has book
    mock_pipeline.book_manager.get_decrypted_archive_metadata = AsyncMock(
        return_value={"encrypted_etag": "stored-etag"}
    )

    result = await check.main("TEST123", mock_pipeline)

    assert result.action == TaskAction.SKIPPED
    assert result.reason == "skip_found_in_storage_not_grin"
    assert result.data is not None
    assert result.data["http_status_code"] == 404


@pytest.mark.asyncio
async def test_main_etag_mismatch_redownload(mock_pipeline):
    """Check task should complete when etags differ (re-download needed)."""
    response = MagicMock()
    response.status = 200
    response.headers = {"ETag": '"new-etag"', "Content-Length": "2048"}
    mock_pipeline.grin_client.head_archive.return_value = response

    # Mock storage has book with different etag
    mock_pipeline.book_manager.get_decrypted_archive_metadata = AsyncMock(return_value={"encrypted_etag": "old-etag"})

    result = await check.main("TEST123", mock_pipeline)

    assert result.action == TaskAction.COMPLETED
    assert result.data is not None
    assert result.data["etag"] == "new-etag"
    assert result.data["file_size_bytes"] == 2048


@pytest.mark.asyncio
async def test_main_storage_404_grin_available(mock_pipeline):
    """Check task should complete when storage throws 404 but GRIN has the book."""
    # Mock storage 404 error
    storage_error = ClientError(error_response={"Error": {"Code": "404"}}, operation_name="HeadObject")
    mock_pipeline.book_manager.get_decrypted_archive_metadata = AsyncMock(side_effect=storage_error)

    # Mock GRIN has the book
    response = MagicMock()
    response.status = 200
    response.headers = {"ETag": '"grin-etag"', "Content-Length": "2048"}
    mock_pipeline.grin_client.head_archive.return_value = response

    result = await check.main("TEST123", mock_pipeline)

    assert result.action == TaskAction.COMPLETED
    assert result.data is not None
    assert result.data["etag"] == "grin-etag"
    assert result.data["file_size_bytes"] == 2048


@pytest.mark.asyncio
async def test_main_storage_404_grin_404_fail(mock_pipeline):
    """Check task should fail when both storage and GRIN return 404."""
    # Mock storage 404 error
    storage_error = ClientError(error_response={"Error": {"Code": "404"}}, operation_name="HeadObject")
    mock_pipeline.book_manager.get_decrypted_archive_metadata = AsyncMock(side_effect=storage_error)

    # Mock GRIN 404 error
    grin_error = aiohttp.ClientResponseError(request_info=MagicMock(), history=(), status=404, message="Not Found")
    mock_pipeline.grin_client.head_archive.side_effect = grin_error

    result = await check.main("TEST123", mock_pipeline)

    assert result.action == TaskAction.FAILED
    assert result.reason == "fail_archive_missing"
    assert result.data is not None
    assert result.data["http_status_code"] == 404


@pytest.mark.asyncio
async def test_head_request():
    """HEAD request should return file metadata."""
    response = MagicMock()
    response.status = 200
    response.headers = {"ETag": '"test-etag"', "Content-Length": "2048"}

    grin_client = MagicMock()
    grin_client.auth.make_authenticated_request = AsyncMock(return_value=response)
    grin_client.head_archive = AsyncMock(return_value=response)

    result = await check.grin_head_request("HEAD123", grin_client, "TestLib")

    assert result["etag"] == "test-etag"
    assert result["file_size_bytes"] == 2048
    assert result["http_status_code"] == 200


@pytest.mark.asyncio
async def test_429_error_not_retried():
    """429 errors should not be retried by the grin_head_request function."""
    # Create a 429 error
    error_429 = aiohttp.ClientResponseError(
        request_info=MagicMock(), history=(), status=429, message="Too Many Requests"
    )

    grin_client = MagicMock()
    grin_client.head_archive = AsyncMock(side_effect=error_429)

    # Should not retry and raise the error immediately
    with pytest.raises(aiohttp.ClientResponseError) as exc_info:
        await check.grin_head_request("TEST123", grin_client, "TestLib")

    assert exc_info.value.status == 429
    # Verify head_archive was called only once (no retries)
    assert grin_client.head_archive.call_count == 1
