#!/usr/bin/env python3
"""Tests for sync tasks check module."""

from unittest.mock import AsyncMock

import pytest

from grin_to_s3.sync.tasks import check
from grin_to_s3.sync.tasks.task_types import TaskAction
from tests.test_utils.unified_mocks import (
    create_client_response_error,
    create_grin_head_response,
    create_storage_client_error,
)


def setup_storage_metadata(mock_pipeline, etag: str | None) -> None:
    """Configure mock storage metadata response."""
    mock_pipeline.book_manager.get_decrypted_archive_metadata = AsyncMock(
        return_value={} if etag is None else {"encrypted_etag": etag}
    )


@pytest.mark.parametrize(
    "storage_etag,grin_etag,size",
    [
        (None, "abc123", 1024),
        ("old-etag", "new-etag", 2048),
    ],
    ids=["needs_download", "redownload"],
)
@pytest.mark.asyncio
async def test_main_needs_download(storage_etag, grin_etag, size, mock_pipeline):
    """Check task should complete when download is needed."""
    mock_pipeline.grin_client.head_archive.return_value = create_grin_head_response(grin_etag, size)
    setup_storage_metadata(mock_pipeline, storage_etag)

    result = await check.main("TEST123", mock_pipeline)

    assert result.action == TaskAction.COMPLETED
    assert result.data["etag"] == grin_etag
    assert result.data["file_size_bytes"] == size


@pytest.mark.asyncio
async def test_main_storage_404_grin_available(mock_pipeline):
    """Check task should complete when storage throws 404 but GRIN has the book."""
    mock_pipeline.book_manager.get_decrypted_archive_metadata = AsyncMock(side_effect=create_storage_client_error())
    mock_pipeline.grin_client.head_archive.return_value = create_grin_head_response("grin-etag", 2048)

    result = await check.main("TEST123", mock_pipeline)

    assert result.action == TaskAction.COMPLETED
    assert result.data["etag"] == "grin-etag"
    assert result.data["file_size_bytes"] == 2048


@pytest.mark.parametrize(
    "force,expected_action,expected_reason",
    [
        (False, TaskAction.SKIPPED, "skip_etag_match"),
        (True, TaskAction.COMPLETED, "completed_match_with_force"),
    ],
    ids=["skip_on_match", "force_redownload"],
)
@pytest.mark.asyncio
async def test_main_etag_match(force, expected_action, expected_reason, mock_pipeline):
    """Check task should skip on etag match unless force flag is set."""
    mock_pipeline.grin_client.head_archive.return_value = create_grin_head_response("abc123", 1024)
    setup_storage_metadata(mock_pipeline, "abc123")
    mock_pipeline.force = force

    result = await check.main("TEST123", mock_pipeline)

    assert result.action == expected_action
    assert result.reason == expected_reason


@pytest.mark.asyncio
async def test_main_storage_exists_grin_404_skip(mock_pipeline):
    """Check task should skip when book exists in storage but GRIN returns 404."""
    mock_pipeline.grin_client.head_archive.side_effect = create_client_response_error(404, "Not Found")
    setup_storage_metadata(mock_pipeline, "stored-etag")

    result = await check.main("TEST123", mock_pipeline)

    assert result.action == TaskAction.SKIPPED
    assert result.reason == "skip_found_in_storage_not_grin"
    assert result.data["http_status_code"] == 404


@pytest.mark.asyncio
async def test_main_storage_404_grin_404_fail(mock_pipeline):
    """Check task should fail when both storage and GRIN return 404."""
    mock_pipeline.book_manager.get_decrypted_archive_metadata = AsyncMock(side_effect=create_storage_client_error())
    mock_pipeline.grin_client.head_archive.side_effect = create_client_response_error(404, "Not Found")

    result = await check.main("TEST123", mock_pipeline)

    assert result.action == TaskAction.FAILED
    assert result.reason == "fail_archive_missing"
    assert result.error == "Archive not available in GRIN"
    assert result.data["http_status_code"] == 404


@pytest.mark.parametrize(
    "has_known_failure,force,expected_reason",
    [
        (True, False, "fail_known_conversion_failure"),
        (True, True, "fail_archive_missing"),
        (False, False, "fail_archive_missing"),
    ],
    ids=["known_failure_no_force", "known_failure_with_force", "unknown_failure"],
)
@pytest.mark.asyncio
async def test_main_404_failure_reasons(has_known_failure, force, expected_reason, mock_pipeline):
    """Check task should use appropriate failure reason based on known failures and force flag."""
    if has_known_failure:
        mock_pipeline.conversion_failure_metadata = {
            "TEST123": {
                "grin_convert_failed_date": "2024-01-15",
                "grin_convert_failed_info": "Error: Timeout",
                "grin_detailed_convert_failed_info": "Process exceeded 3600s limit",
            }
        }
    else:
        mock_pipeline.conversion_failure_metadata = {}

    mock_pipeline.force = force
    mock_pipeline.grin_client.head_archive.side_effect = create_client_response_error(404, "Not Found")
    setup_storage_metadata(mock_pipeline, None)

    result = await check.main("TEST123", mock_pipeline)

    assert result.action == TaskAction.FAILED
    assert result.reason == expected_reason
    assert result.error == "Archive not available in GRIN"
    assert result.data["http_status_code"] == 404
