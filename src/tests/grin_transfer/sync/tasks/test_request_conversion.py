#!/usr/bin/env python3
"""
Tests for sync tasks request_conversion module.
"""

# Mock setup is done in conftest.py and test functions

import aiohttp
import pytest

from grin_transfer.sync.tasks import request_conversion
from grin_transfer.sync.tasks.task_types import TaskAction, TaskType


@pytest.mark.asyncio
async def test_429_error_returns_failed_status(mock_pipeline):
    """429 errors should return FAILED status to trigger sequential failure counter."""
    mock_pipeline.conversion_requests_made = 10

    # Mock grin_client.fetch_resource to raise 429 error
    error = aiohttp.ClientResponseError(request_info=None, history=(), status=429, message="Too Many Requests")
    # Mock the problematic __str__ method to avoid request_info.real_url access
    error.__str__ = lambda: "429, message='Too Many Requests'"
    mock_pipeline.grin_client.fetch_resource.side_effect = error

    result = await request_conversion.main("TEST123", mock_pipeline)

    assert result.barcode == "TEST123"
    assert result.task_type == TaskType.REQUEST_CONVERSION
    assert result.action == TaskAction.FAILED  # FAILED triggers sequential failure counter
    assert "Queue limit reached" in result.error
    assert result.reason == "fail_queue_limit_reached"
    assert result.data["conversion_status"] == "queue_limit_reached"
    assert result.data["request_count"] == 10  # Should not increment on failure


@pytest.mark.asyncio
async def test_successful_conversion_request(mock_pipeline):
    """Successful conversion requests should return SKIPPED status."""
    mock_pipeline.conversion_requests_made = 42

    # Mock grin_client.fetch_resource to return successful TSV response
    mock_pipeline.grin_client.fetch_resource.return_value = "Barcode\tStatus\nTEST789\tSuccess"

    result = await request_conversion.main("TEST789", mock_pipeline)

    assert result.barcode == "TEST789"
    assert result.task_type == TaskType.REQUEST_CONVERSION
    assert result.action == TaskAction.COMPLETED  # Success cases are COMPLETED
    assert result.reason == "success_conversion_requested"
    assert result.data["conversion_status"] == "requested"
    assert result.data["request_count"] == 43  # Should increment after success
    assert mock_pipeline.conversion_requests_made == 43


@pytest.mark.asyncio
async def test_already_in_process_response(mock_pipeline):
    """Books already in process should return SKIPPED status."""
    mock_pipeline.conversion_requests_made = 20

    # Mock grin_client.fetch_resource to return already in process TSV response
    mock_pipeline.grin_client.fetch_resource.return_value = (
        "Barcode\tStatus\nTEST999\tBook is already in process for conversion"
    )

    result = await request_conversion.main("TEST999", mock_pipeline)

    assert result.action == TaskAction.SKIPPED
    assert result.reason == "skip_already_in_process"
    assert result.data["conversion_status"] == "in_process"
    assert result.data["request_count"] == 21  # Should increment
    assert mock_pipeline.conversion_requests_made == 21


@pytest.mark.asyncio
async def test_unavailable_book_response(mock_pipeline):
    """Books that cannot be converted should return SKIPPED with unavailable status."""
    mock_pipeline.conversion_requests_made = 15

    # Mock grin_client.fetch_resource to return unavailable TSV response
    mock_pipeline.grin_client.fetch_resource.return_value = (
        "Barcode\tStatus\nUNAVAILABLE123\tBook not available for conversion"
    )

    result = await request_conversion.main("UNAVAILABLE123", mock_pipeline)

    assert result.action == TaskAction.SKIPPED
    assert result.reason == "skip_verified_unavailable"
    assert result.data["conversion_status"] == "unavailable"
    assert result.data["request_count"] == 16  # Should increment
    assert mock_pipeline.conversion_requests_made == 16


@pytest.mark.asyncio
async def test_non_429_errors_propagate(mock_pipeline):
    """Non-429 errors should propagate naturally (not be caught)."""
    mock_pipeline.conversion_requests_made = 8

    # Mock grin_client.fetch_resource to raise a different error (not 429)
    # Use a simple ValueError to test error propagation
    mock_pipeline.grin_client.fetch_resource.side_effect = ValueError("Simulated processing error")

    # Should raise the exception (not catch it)
    with pytest.raises(ValueError) as exc_info:
        await request_conversion.main("ERROR123", mock_pipeline)

    assert "Simulated processing error" in str(exc_info.value)
    # Conversion counter should not increment on error
    assert mock_pipeline.conversion_requests_made == 8


@pytest.mark.asyncio
async def test_generic_exception_propagates(mock_pipeline):
    """Generic exceptions should propagate normally."""
    # Mock grin_client.fetch_resource to raise generic exception
    mock_pipeline.grin_client.fetch_resource.side_effect = ValueError("Some unexpected error")

    with pytest.raises(ValueError) as exc_info:
        await request_conversion.main("ERROR456", mock_pipeline)

    assert "Some unexpected error" in str(exc_info.value)


@pytest.mark.asyncio
async def test_429_error_logging_and_data_structure(mock_pipeline):
    """Verify 429 error creates proper log messages and data structure."""
    mock_pipeline.conversion_requests_made = 100

    # Mock grin_client.fetch_resource to raise 429 error
    mock_pipeline.grin_client.fetch_resource.side_effect = aiohttp.ClientResponseError(
        request_info=None, history=(), status=429, message="Too Many Requests", headers={}
    )

    result = await request_conversion.main("HARVARD123", mock_pipeline)

    # Verify all expected fields are present
    assert "conversion_status" in result.data
    assert "request_count" in result.data
    assert result.data["conversion_status"] == "queue_limit_reached"
    assert result.data["request_count"] == 100

    # Verify task metadata
    assert result.barcode == "HARVARD123"
    assert result.task_type == TaskType.REQUEST_CONVERSION
    assert result.action == TaskAction.FAILED
    assert result.reason == "fail_queue_limit_reached"
