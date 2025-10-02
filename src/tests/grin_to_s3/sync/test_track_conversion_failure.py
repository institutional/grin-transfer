#!/usr/bin/env python3
"""Tests for track_conversion_failure task."""

from unittest.mock import MagicMock

import pytest

from grin_to_s3.sync.db_updates import (
    track_conversion_failure_completed,
    track_conversion_failure_failed,
)
from grin_to_s3.sync.tasks import track_conversion_failure
from grin_to_s3.sync.tasks.task_types import TaskAction, TaskType
from tests.test_utils.unified_mocks import create_test_pipeline


@pytest.fixture
def mock_pipeline():
    """Mock pipeline with conversion failure metadata."""
    pipeline = create_test_pipeline()
    pipeline.conversion_failure_metadata = {
        "FAIL001": {
            "grin_convert_failed_date": "2024-01-15",
            "grin_convert_failed_info": "Error: Conversion timeout",
            "grin_detailed_convert_failed_info": "Process exceeded 3600s limit",
        }
    }
    return pipeline


class TestTrackConversionFailureTask:
    """Test track_conversion_failure task implementation."""

    @pytest.mark.asyncio
    async def test_retrieves_metadata_from_pipeline(self, mock_pipeline):
        """Task should retrieve failure metadata from pipeline."""
        result = await track_conversion_failure.main("FAIL001", mock_pipeline)

        assert result.barcode == "FAIL001"
        assert result.task_type == TaskType.TRACK_CONVERSION_FAILURE
        assert result.action == TaskAction.COMPLETED
        assert result.data is not None
        assert result.data["grin_convert_failed_date"] == "2024-01-15"
        assert result.data["grin_convert_failed_info"] == "Error: Conversion timeout"
        assert result.data["grin_detailed_convert_failed_info"] == "Process exceeded 3600s limit"

    @pytest.mark.asyncio
    async def test_fails_when_metadata_missing(self, mock_pipeline):
        """Task should fail when metadata not found in pipeline."""
        result = await track_conversion_failure.main("NONEXISTENT", mock_pipeline)

        assert result.action == TaskAction.FAILED
        assert result.error == "No failure metadata found"


class TestDatabaseHandlers:
    """Test database update handlers for track_conversion_failure."""

    @pytest.mark.asyncio
    async def test_completed_handler_writes_status(self):
        """Handler should write conversion_failed status with metadata."""
        result = MagicMock()
        result.barcode = "FAIL001"
        result.task_type = TaskType.TRACK_CONVERSION_FAILURE
        result.action = TaskAction.COMPLETED
        result.data = {
            "grin_convert_failed_date": "2024-01-15",
            "grin_convert_failed_info": "Error: Timeout",
            "grin_detailed_convert_failed_info": "Details",
        }

        handler_result = await track_conversion_failure_completed(result, {})

        assert "status" in handler_result
        status_type, status_value, metadata = handler_result["status"]
        assert status_type == "conversion"
        assert status_value == "conversion_failed"
        assert metadata["grin_convert_failed_date"] == "2024-01-15"
        assert metadata["grin_convert_failed_info"] == "Error: Timeout"
        assert metadata["grin_detailed_convert_failed_info"] == "Details"

    @pytest.mark.asyncio
    async def test_failed_handler_writes_tracking_failed_status(self):
        """Failed handler should write tracking_failed status with error metadata."""
        result = MagicMock()
        result.barcode = "FAIL001"
        result.task_type = TaskType.TRACK_CONVERSION_FAILURE
        result.action = TaskAction.FAILED
        result.error = "Failed to retrieve metadata from pipeline"

        handler_result = await track_conversion_failure_failed(result, {})

        assert "status" in handler_result
        status_type, status_value, metadata = handler_result["status"]
        assert status_type == "conversion"
        assert status_value == "tracking_failed"
        assert metadata is not None
        assert metadata["error"] == "Failed to retrieve metadata from pipeline"
