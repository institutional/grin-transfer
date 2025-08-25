#!/usr/bin/env python3
"""
Tests for sync tasks check module.
"""

from unittest.mock import AsyncMock, MagicMock, patch

import aiohttp
import pytest

from grin_to_s3.sync.tasks import check
from grin_to_s3.sync.tasks.task_types import TaskAction


@pytest.mark.asyncio
async def test_main_needs_download(mock_pipeline):
    """Check task should complete when file needs download."""
    # Mock HEAD response
    response = MagicMock()
    response.status = 200
    response.headers = {"ETag": '"abc123"', "Content-Length": "1024"}
    mock_pipeline.grin_client.auth.make_authenticated_request.return_value = response
    mock_pipeline.grin_client.head_archive.return_value = response

    with (
        patch("grin_to_s3.sync.tasks.check.etag_matches") as mock_etag_matches,
        patch("grin_to_s3.sync.tasks.check.BookManager"),
    ):
        mock_etag_matches.return_value = {"matched": False, "reason": "no_archive"}

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
    mock_pipeline.grin_client.auth.make_authenticated_request.return_value = response
    mock_pipeline.grin_client.head_archive.return_value = response

    with (
        patch("grin_to_s3.sync.tasks.check.etag_matches") as mock_etag_matches,
        patch("grin_to_s3.sync.tasks.check.BookManager"),
    ):
        mock_etag_matches.return_value = {"matched": True, "reason": "etag_match"}

        result = await check.main("TEST123", mock_pipeline)

        assert result.action == TaskAction.SKIPPED
        assert result.reason == "skip_etag_match"


@pytest.mark.asyncio
async def test_main_etag_continue_if_force(mock_pipeline):
    """Check task should return success even if etag matches, if force is provided."""
    response = MagicMock()
    response.status = 200
    response.headers = {"ETag": '"abc123"', "Content-Length": "1024"}
    mock_pipeline.grin_client.auth.make_authenticated_request.return_value = response
    mock_pipeline.grin_client.head_archive.return_value = response
    mock_pipeline.force = True
    with (
        patch("grin_to_s3.sync.tasks.check.etag_matches") as mock_etag_matches,
        patch("grin_to_s3.sync.tasks.check.BookManager"),
    ):
        mock_etag_matches.return_value = {"matched": True, "reason": "etag_match"}

        result = await check.main("TEST123", mock_pipeline)

        assert result.action == TaskAction.COMPLETED
        assert result.reason == "completed_match_with_force"


@pytest.mark.asyncio
async def test_main_404_fail(mock_pipeline):
    """Check task should fail when file not found in GRIN."""
    error_404 = aiohttp.ClientResponseError(request_info=MagicMock(), history=(), status=404, message="Not Found")
    mock_pipeline.grin_client.auth.make_authenticated_request.side_effect = error_404
    mock_pipeline.grin_client.head_archive.side_effect = error_404

    result = await check.main("TEST123", mock_pipeline)

    assert result.action == TaskAction.FAILED
    assert result.reason == "fail_archive_missing"


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
async def test_etag_matches():
    """ETag should match when stored and GRIN ETags are identical."""
    book_manager = MagicMock()
    book_manager.get_decrypted_archive_metadata = AsyncMock(return_value={"encrypted_etag": "matching-etag"})

    result = await check.etag_matches("MATCH123", "matching-etag", book_manager, MagicMock())

    assert result["matched"] is True
    assert result["reason"] == "etag_match"


@pytest.mark.asyncio
async def test_etag_mismatch():
    """ETag should not match when stored and GRIN ETags differ."""
    book_manager = MagicMock()
    book_manager.get_decrypted_archive_metadata = AsyncMock(return_value={"encrypted_etag": "old-etag"})

    result = await check.etag_matches("MISMATCH123", "new-etag", book_manager, MagicMock())

    assert result["matched"] is False
    assert result["reason"] == "etag_mismatch"
