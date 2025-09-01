#!/usr/bin/env python3
"""
Tests for sync tasks extract_ocr module.
"""

import tempfile
from pathlib import Path
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from grin_to_s3.constants import OUTPUT_DIR
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


@pytest.mark.asyncio
async def test_extract_local_storage_moves_file_to_full_directory():
    """Extract OCR should move JSONL file to 'full' subdirectory for local storage."""

    filesystem_manager = MagicMock(spec=DirectoryManager)
    pipeline = MagicMock()
    pipeline.filesystem_manager = filesystem_manager

    unpack_data: UnpackData = {
        "unpacked_path": Path("/tmp/TEST123"),
    }

    with tempfile.TemporaryDirectory() as temp_dir:
        output_dir = Path(temp_dir) / OUTPUT_DIR
        staging_path = Path(temp_dir) / "staging"
        staging_path.mkdir()
        filesystem_manager.staging_path = staging_path

        # Configure for local storage
        pipeline.config.storage_config = {"config": {"base_path": str(output_dir)}}  # No bucket_full = local storage
        pipeline.uses_block_storage = False

        # Create the JSONL file in staging
        jsonl_staging_path = staging_path / "TEST123_ocr.jsonl"
        jsonl_staging_path.write_text("test content")

        with patch("grin_to_s3.sync.tasks.extract_ocr.extract_ocr_pages") as mock_extract:
            mock_extract.return_value = 1

            result = await extract_ocr.main("TEST123", unpack_data, pipeline)

            # Verify the result
            assert result.action == TaskAction.COMPLETED
            assert result.data
            expected_final_path = output_dir / "full" / "TEST123" / "TEST123_ocr.jsonl.gz"
            assert str(result.data["json_file_path"]) == str(expected_final_path)

            # Verify staging file was moved (no longer exists)
            assert not jsonl_staging_path.exists()

            # Verify file was moved to final location
            assert expected_final_path.exists()


@pytest.mark.asyncio
async def test_extract_ocr_with_compression_enabled():
    """Extract OCR should compress JSONL when compression is enabled."""
    filesystem_manager = MagicMock(spec=DirectoryManager)
    pipeline = MagicMock()
    pipeline.filesystem_manager = filesystem_manager
    pipeline.storage.write_file = AsyncMock()
    pipeline.config.storage_config = {"config": {"bucket_full": "test-bucket"}}
    pipeline.config.sync_compression_full_enabled = True
    pipeline.uses_block_storage = True

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
            mock_extract.return_value = 2
            compressed_path = staging_path / "compressed.gz"
            mock_compress.return_value.__aenter__.return_value = compressed_path

            result = await extract_ocr.main("TEST123", unpack_data, pipeline)

            # Should use compression and create .gz file
            assert result.action == TaskAction.COMPLETED
            assert str(result.data["json_file_path"]).endswith(".gz")
            mock_compress.assert_called_once()


@pytest.mark.asyncio
async def test_extract_ocr_with_compression_disabled():
    """Extract OCR should not compress JSONL when compression is disabled."""
    filesystem_manager = MagicMock(spec=DirectoryManager)
    pipeline = MagicMock()
    pipeline.filesystem_manager = filesystem_manager
    pipeline.storage.write_file = AsyncMock()
    pipeline.config.storage_config = {"config": {"bucket_full": "test-bucket"}}
    pipeline.config.sync_compression_full_enabled = False
    pipeline.uses_block_storage = True

    unpack_data: UnpackData = {
        "unpacked_path": Path("/tmp/TEST123"),
    }

    with tempfile.TemporaryDirectory() as temp_dir:
        staging_path = Path(temp_dir)
        filesystem_manager.staging_path = staging_path

        # Create the JSONL file that extract_ocr_pages would create
        jsonl_path = staging_path / "TEST123_ocr.jsonl"
        jsonl_path.write_text("test content")

        with (
            patch("grin_to_s3.sync.tasks.extract_ocr.extract_ocr_pages") as mock_extract,
            patch("grin_to_s3.sync.tasks.extract_ocr.compress_file_to_temp") as mock_compress,
        ):
            mock_extract.return_value = 2

            result = await extract_ocr.main("TEST123", unpack_data, pipeline)

            # Should not use compression and create uncompressed file
            assert result.action == TaskAction.COMPLETED
            assert not str(result.data["json_file_path"]).endswith(".gz")
            mock_compress.assert_not_called()

            # Should upload the original file
            pipeline.storage.write_file.assert_called_once()
            upload_call = pipeline.storage.write_file.call_args
            assert upload_call[0][0] == "test-bucket/TEST123_ocr.jsonl"  # No .gz extension


@pytest.mark.asyncio
async def test_extract_ocr_local_storage_with_compression_disabled():
    """Extract OCR should handle local storage without compression."""
    filesystem_manager = MagicMock(spec=DirectoryManager)
    pipeline = MagicMock()
    pipeline.filesystem_manager = filesystem_manager
    pipeline.config.storage_config = {"config": {"base_path": "/tmp/output"}}
    pipeline.config.sync_compression_full_enabled = False
    pipeline.uses_block_storage = False

    unpack_data: UnpackData = {
        "unpacked_path": Path("/tmp/TEST123"),
    }

    with tempfile.TemporaryDirectory() as temp_dir:
        staging_path = Path(temp_dir) / "staging"
        output_dir = Path(temp_dir) / OUTPUT_DIR
        staging_path.mkdir()
        filesystem_manager.staging_path = staging_path

        # Update pipeline config to use temp output dir
        pipeline.config.storage_config = {"config": {"base_path": str(output_dir)}}

        # Create the JSONL file
        jsonl_path = staging_path / "TEST123_ocr.jsonl"
        jsonl_path.write_text("test content")

        with (
            patch("grin_to_s3.sync.tasks.extract_ocr.extract_ocr_pages") as mock_extract,
            patch("grin_to_s3.sync.tasks.extract_ocr.compress_file_to_temp") as mock_compress,
        ):
            mock_extract.return_value = 2

            result = await extract_ocr.main("TEST123", unpack_data, pipeline)

            # Should not use compression and move file without .gz extension
            assert result.action == TaskAction.COMPLETED
            expected_path = output_dir / "full" / "TEST123" / "TEST123_ocr.jsonl"
            assert str(result.data["json_file_path"]) == str(expected_path)
            mock_compress.assert_not_called()

            # File should be moved to final location
            assert expected_path.exists()
            assert not jsonl_path.exists()
