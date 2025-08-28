#!/usr/bin/env python3
"""
Tests for sync tasks request_conversion module.
"""

from unittest.mock import patch

import aiohttp
import pytest

from grin_to_s3.sync.tasks import request_conversion
from grin_to_s3.sync.tasks.task_types import TaskAction, TaskType


@pytest.mark.asyncio
async def test_429_error_returns_failed_status(mock_pipeline):
    """429 errors should return FAILED status to trigger sequential failure counter."""
    mock_pipeline.conversion_requests_made = 10

    with patch("grin_to_s3.sync.tasks.request_conversion.request_conversion") as mock_request:
        # Create mock ClientResponseError with proper status
        error = aiohttp.ClientResponseError(request_info=None, history=(), status=429, message="Too Many Requests")
        # Mock the problematic __str__ method to avoid request_info.real_url access
        error.__str__ = lambda: "429, message='Too Many Requests'"
        mock_request.side_effect = error

        result = await request_conversion.main("TEST123", mock_pipeline)

        assert result.barcode == "TEST123"
        assert result.task_type == TaskType.REQUEST_CONVERSION
        assert result.action == TaskAction.FAILED  # FAILED triggers sequential failure counter
        assert result.reason == "fail_queue_limit_reached"
        assert result.data["conversion_status"] == "queue_limit_reached"
        assert result.data["request_count"] == 10  # Should not increment on failure


@pytest.mark.asyncio
async def test_429_error_with_different_message_format(mock_pipeline):
    """429 errors with different message formats should still be caught."""
    mock_pipeline.conversion_requests_made = 5

    with patch("grin_to_s3.sync.tasks.request_conversion.request_conversion") as mock_request:
        # Simulate different error message format
        mock_request.side_effect = aiohttp.ClientResponseError(
            request_info=None, history=(), status=429, message="Too Many Requests: Queue is full", headers={}
        )

        result = await request_conversion.main("TEST456", mock_pipeline)

        assert result.action == TaskAction.FAILED
        assert result.reason == "fail_queue_limit_reached"
        assert result.data["conversion_status"] == "queue_limit_reached"


@pytest.mark.asyncio
async def test_successful_conversion_request(mock_pipeline):
    """Successful conversion requests should return SKIPPED status."""
    mock_pipeline.conversion_requests_made = 42

    with patch("grin_to_s3.sync.tasks.request_conversion.request_conversion") as mock_request:
        mock_request.return_value = "Success"

        result = await request_conversion.main("TEST789", mock_pipeline)

        assert result.barcode == "TEST789"
        assert result.task_type == TaskType.REQUEST_CONVERSION
        assert result.action == TaskAction.SKIPPED  # Success cases are SKIPPED
        assert result.reason == "skip_conversion_requested"
        assert result.data["conversion_status"] == "requested"
        assert result.data["request_count"] == 43  # Should increment after success
        assert mock_pipeline.conversion_requests_made == 43


@pytest.mark.asyncio
async def test_already_in_process_response(mock_pipeline):
    """Books already in process should return SKIPPED status."""
    mock_pipeline.conversion_requests_made = 20

    with patch("grin_to_s3.sync.tasks.request_conversion.request_conversion") as mock_request:
        mock_request.return_value = "Book is already in process for conversion"

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

    with patch("grin_to_s3.sync.tasks.request_conversion.request_conversion") as mock_request:
        mock_request.return_value = "Book not available for conversion"

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

    with patch("grin_to_s3.sync.tasks.request_conversion.request_conversion") as mock_request:
        # Simulate a different HTTP error (not 429)
        # Use a simple ValueError to test error propagation
        mock_request.side_effect = ValueError("Simulated processing error")

        # Should raise the exception (not catch it)
        with pytest.raises(ValueError) as exc_info:
            await request_conversion.main("ERROR123", mock_pipeline)

        assert "Simulated processing error" in str(exc_info.value)
        # Conversion counter should not increment on error
        assert mock_pipeline.conversion_requests_made == 8


@pytest.mark.asyncio
async def test_generic_exception_propagates(mock_pipeline):
    """Generic exceptions should propagate normally."""
    with patch("grin_to_s3.sync.tasks.request_conversion.request_conversion") as mock_request:
        mock_request.side_effect = ValueError("Some unexpected error")

        with pytest.raises(ValueError) as exc_info:
            await request_conversion.main("ERROR456", mock_pipeline)

        assert "Some unexpected error" in str(exc_info.value)


@pytest.mark.asyncio
async def test_429_error_logging_and_data_structure(mock_pipeline):
    """Verify 429 error creates proper log messages and data structure."""
    mock_pipeline.conversion_requests_made = 100

    with patch("grin_to_s3.sync.tasks.request_conversion.request_conversion") as mock_request:
        mock_request.side_effect = aiohttp.ClientResponseError(
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


@pytest.mark.asyncio
async def test_integration_with_mock_pipeline_attributes(mock_pipeline):
    """Test that the task properly uses mock_pipeline attributes."""
    # Verify mock_pipeline has expected attributes from conftest
    assert hasattr(mock_pipeline, "library_directory")
    assert hasattr(mock_pipeline, "secrets_dir")
    assert mock_pipeline.library_directory == "TestLib"
    assert mock_pipeline.secrets_dir == "/path/to/secrets"

    # Initialize conversion counter
    mock_pipeline.conversion_requests_made = 0

    with patch("grin_to_s3.sync.tasks.request_conversion.request_conversion") as mock_request:
        mock_request.return_value = "Success"

        result = await request_conversion.main("INTEGRATION_TEST", mock_pipeline)

        # Verify the function was called with correct pipeline attributes
        mock_request.assert_called_once_with(
            "INTEGRATION_TEST", mock_pipeline.library_directory, mock_pipeline.secrets_dir
        )

        assert result.action == TaskAction.SKIPPED
        assert mock_pipeline.conversion_requests_made == 1
