#!/usr/bin/env python3
"""
Tests for sync tasks extract_ocr module.
"""

import tempfile
from pathlib import Path
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from grin_to_s3.storage.staging import DirectoryManager
from grin_to_s3.sync.tasks import extract_ocr
from grin_to_s3.sync.tasks.task_types import TaskAction, UnpackData


@pytest.mark.asyncio
async def test_main_successful_extraction(mock_pipeline):
    """Extract OCR task should complete successfully."""
    unpack_data: UnpackData = {
        "unpacked_path": Path("/tmp/TEST123"),
    }

    with (
        patch("grin_to_s3.sync.tasks.extract_ocr.extract_ocr_pages") as mock_extract,
        patch("grin_to_s3.sync.tasks.extract_ocr.compress_file_to_temp") as mock_compress,
    ):
        mock_extract.return_value = 3
        compressed_path = Path(mock_pipeline.filesystem_manager.staging_path) / "compressed.gz"
        mock_compress.return_value.__aenter__.return_value = compressed_path
        mock_pipeline.storage.write_file = AsyncMock()

        result = await extract_ocr.main("TEST123", unpack_data, mock_pipeline)

        assert result.action == TaskAction.COMPLETED
        assert result.data
        assert result.data["page_count"] == 3
        assert "TEST123_ocr.jsonl" in str(result.data["json_file_path"])


@pytest.mark.asyncio
async def test_extract_ocr_creates_staging_file(mock_pipeline):
    """Extract OCR should create file in staging directory."""
    unpack_data: UnpackData = {
        "unpacked_path": Path("/tmp/TEST123"),
    }

    staging_path = Path(mock_pipeline.filesystem_manager.staging_path)

    with (
        patch("grin_to_s3.sync.tasks.extract_ocr.extract_ocr_pages") as mock_extract,
        patch("grin_to_s3.sync.tasks.extract_ocr.compress_file_to_temp") as mock_compress,
    ):
        mock_extract.return_value = 2
        compressed_path = staging_path / "compressed.gz"
        mock_compress.return_value.__aenter__.return_value = compressed_path
        mock_pipeline.storage.write_file = AsyncMock()

        await extract_ocr.main("TEST123", unpack_data, mock_pipeline)

        expected_jsonl = staging_path / "TEST123_ocr.jsonl"
        mock_extract.assert_called_once_with(unpack_data, expected_jsonl)


@pytest.mark.asyncio
async def test_extract_with_storage_config():
    """Extract OCR should upload to storage when bucket configured."""

    filesystem_manager = MagicMock(spec=DirectoryManager)
    pipeline = MagicMock()
    pipeline.filesystem_manager = filesystem_manager
    pipeline.storage.write_file = AsyncMock()
    pipeline.config.storage_config = {"config": {"bucket_full": "test-bucket"}}

    unpack_data: UnpackData = {
        "unpacked_path": Path("/tmp/TEST123"),
    }

    with tempfile.TemporaryDirectory() as temp_dir:
        staging_path = Path(temp_dir)
        filesystem_manager.staging_path = staging_path

        with (
            patch("grin_to_s3.sync.tasks.extract_ocr.extract_ocr_pages") as mock_extract,
            patch("grin_to_s3.sync.tasks.extract_ocr.compress_file_to_temp") as mock_compress,
        ):
            mock_extract.return_value = 1
            compressed_path = staging_path / "compressed.gz"
            mock_compress.return_value.__aenter__.return_value = compressed_path

            result = await extract_ocr.main("TEST123", unpack_data, pipeline)

            assert result.action == TaskAction.COMPLETED
            assert result.data
            assert "test-bucket/TEST123_ocr.jsonl.gz" == str(result.data["json_file_path"])
