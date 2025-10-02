#!/usr/bin/env python3
"""
Tests for sync tasks extract_ocr module.
"""

import tempfile
from pathlib import Path
from unittest.mock import AsyncMock, MagicMock

import pytest

from grin_to_s3.constants import OUTPUT_DIR
from grin_to_s3.storage.staging import DirectoryManager
from grin_to_s3.sync.tasks import extract_ocr
from grin_to_s3.sync.tasks.task_types import TaskAction


@pytest.mark.asyncio
async def test_main_successful_extraction(mock_pipeline, sample_unpack_data):
    """Extract OCR task should complete successfully."""
    from tests.test_utils.unified_mocks import mock_extract_ocr_operations

    unpack_data = sample_unpack_data()

    with mock_extract_ocr_operations(page_count=3) as mocks:
        compressed_path = Path(mock_pipeline.filesystem_manager.staging_path) / "compressed.gz"
        mocks.compress_file_to_temp.return_value.__aenter__.return_value = compressed_path
        mock_pipeline.storage.write_file = AsyncMock()

        result = await extract_ocr.main("TEST123", unpack_data, mock_pipeline)

        assert result.action == TaskAction.COMPLETED
        assert result.data
        assert result.data["page_count"] == 3
        assert "TEST123_ocr.jsonl" in str(result.data["json_file_path"])


@pytest.mark.asyncio
async def test_extract_ocr_creates_staging_file(mock_pipeline, sample_unpack_data):
    """Extract OCR should create file in staging directory."""
    from tests.test_utils.unified_mocks import mock_extract_ocr_operations

    unpack_data = sample_unpack_data()

    staging_path = Path(mock_pipeline.filesystem_manager.staging_path)

    with mock_extract_ocr_operations(page_count=2) as mocks:
        compressed_path = staging_path / "compressed.gz"
        mocks.compress_file_to_temp.return_value.__aenter__.return_value = compressed_path
        mock_pipeline.storage.write_file = AsyncMock()

        await extract_ocr.main("TEST123", unpack_data, mock_pipeline)

        expected_jsonl = staging_path / "TEST123_ocr.jsonl"
        mocks.extract_ocr_pages.assert_called_once_with(unpack_data, expected_jsonl)


@pytest.mark.asyncio
async def test_extract_with_storage_config(temp_filesystem_manager, sample_unpack_data):
    """Extract OCR should upload to storage when bucket configured."""
    from tests.test_utils.unified_mocks import configure_pipeline_storage, mock_extract_ocr_operations

    pipeline = MagicMock()
    pipeline.filesystem_manager = temp_filesystem_manager
    pipeline.storage = MagicMock()
    pipeline.storage.write_file = AsyncMock()
    pipeline.config = MagicMock()
    pipeline.config.sync_compression_full_enabled = True

    configure_pipeline_storage(pipeline, bucket_full="test-bucket")

    pipeline.book_manager = MagicMock()
    pipeline.book_manager.full_text_path = MagicMock(side_effect=lambda filename: f"test-bucket/{filename}")

    unpack_data = sample_unpack_data()

    staging_path = temp_filesystem_manager.staging_path

    with mock_extract_ocr_operations(page_count=1) as mocks:
        compressed_path = staging_path / "compressed.gz"
        mocks.compress_file_to_temp.return_value.__aenter__.return_value = compressed_path

        result = await extract_ocr.main("TEST123", unpack_data, pipeline)

        assert result.action == TaskAction.COMPLETED
        assert result.data
        assert "TEST123_ocr.jsonl" in str(result.data["json_file_path"])
        # Should use compression by default
        mocks.compress_file_to_temp.assert_called_once()


@pytest.mark.asyncio
async def test_extract_local_storage_moves_file_to_full_directory(sample_unpack_data):
    """Extract OCR should upload JSONL file to 'full' subdirectory for local storage."""
    from tests.test_utils.unified_mocks import configure_pipeline_storage, mock_extract_ocr_operations

    filesystem_manager = MagicMock(spec=DirectoryManager)
    pipeline = MagicMock()
    pipeline.filesystem_manager = filesystem_manager
    pipeline.storage = MagicMock()
    pipeline.storage.write_file = AsyncMock()
    pipeline.config = MagicMock()

    unpack_data = sample_unpack_data()

    with tempfile.TemporaryDirectory() as temp_dir:
        output_dir = Path(temp_dir) / OUTPUT_DIR
        staging_path = Path(temp_dir) / "staging"
        staging_path.mkdir()
        filesystem_manager.staging_path = staging_path

        # Configure for local storage
        configure_pipeline_storage(pipeline, storage_type="local", base_path=str(output_dir))
        pipeline.config.sync_compression_full_enabled = True

        # Mock book_manager with proper path method
        pipeline.book_manager = MagicMock()

        def mock_full_text_path(filename):
            full_dir = output_dir / "full"
            return str(full_dir / filename)

        pipeline.book_manager.full_text_path.side_effect = mock_full_text_path

        with mock_extract_ocr_operations(page_count=1) as mocks:
            # Create staging file when extract_ocr_pages is called
            async def create_staging_file(unpack_data, jsonl_path):
                jsonl_path.write_text('["test page"]')
                return 1

            mocks.extract_ocr_pages.side_effect = create_staging_file

            compressed_path = staging_path / "compressed.gz"
            mocks.compress_file_to_temp.return_value.__aenter__.return_value = compressed_path

            result = await extract_ocr.main("TEST123", unpack_data, pipeline)

            # Verify the result
            assert result.action == TaskAction.COMPLETED
            assert result.data
            expected_final_path = output_dir / "full" / "TEST123_ocr.jsonl.gz"
            assert str(result.data["json_file_path"]) == str(expected_final_path)

            # Verify write_file was called with correct arguments
            pipeline.storage.write_file.assert_called_once()
            call_args = pipeline.storage.write_file.call_args
            assert call_args[0][0] == str(expected_final_path)  # destination path
            assert call_args[0][1] == str(compressed_path)  # source path
            assert "barcode" in call_args[0][2]  # metadata contains barcode


@pytest.mark.asyncio
async def test_extract_ocr_with_compression_enabled(temp_filesystem_manager, sample_unpack_data):
    """Extract OCR should compress JSONL when compression is enabled."""
    from tests.test_utils.unified_mocks import configure_pipeline_storage, mock_extract_ocr_operations

    pipeline = MagicMock()
    pipeline.filesystem_manager = temp_filesystem_manager
    pipeline.storage = MagicMock()
    pipeline.storage.write_file = AsyncMock()
    pipeline.config = MagicMock()
    pipeline.config.sync_compression_full_enabled = True

    configure_pipeline_storage(pipeline, bucket_full="test-bucket")

    pipeline.book_manager = MagicMock()
    pipeline.book_manager.full_text_path = MagicMock(side_effect=lambda filename: f"test-bucket/{filename}")

    unpack_data = sample_unpack_data()

    staging_path = temp_filesystem_manager.staging_path

    with mock_extract_ocr_operations(page_count=2) as mocks:
        compressed_path = staging_path / "compressed.gz"
        mocks.compress_file_to_temp.return_value.__aenter__.return_value = compressed_path

        result = await extract_ocr.main("TEST123", unpack_data, pipeline)

        # Should use compression
        assert result.action == TaskAction.COMPLETED
        assert "TEST123_ocr.jsonl" in str(result.data["json_file_path"])
        mocks.compress_file_to_temp.assert_called_once()


@pytest.mark.asyncio
async def test_extract_ocr_with_compression_disabled(temp_filesystem_manager, sample_unpack_data):
    """Extract OCR should not compress JSONL when compression is disabled."""
    from tests.test_utils.unified_mocks import configure_pipeline_storage, mock_extract_ocr_operations

    pipeline = MagicMock()
    pipeline.filesystem_manager = temp_filesystem_manager
    pipeline.storage = MagicMock()
    pipeline.storage.write_file = AsyncMock()
    pipeline.config = MagicMock()
    pipeline.config.sync_compression_full_enabled = False

    configure_pipeline_storage(pipeline, bucket_full="test-bucket")

    pipeline.book_manager = MagicMock()
    pipeline.book_manager.full_text_path = MagicMock(side_effect=lambda filename: f"test-bucket/{filename}")

    unpack_data = sample_unpack_data()

    staging_path = temp_filesystem_manager.staging_path

    # Create the JSONL file that extract_ocr_pages would create
    jsonl_path = staging_path / "TEST123_ocr.jsonl"
    jsonl_path.write_text("test content")

    with mock_extract_ocr_operations(page_count=2) as mocks:
        result = await extract_ocr.main("TEST123", unpack_data, pipeline)

        # Should not use compression and create uncompressed file
        assert result.action == TaskAction.COMPLETED
        assert not str(result.data["json_file_path"]).endswith(".gz")
        mocks.compress_file_to_temp.assert_not_called()

        # Should upload the original file
        pipeline.storage.write_file.assert_called_once()
        upload_call = pipeline.storage.write_file.call_args
        assert upload_call[0][0] == "test-bucket/TEST123_ocr.jsonl"  # No .gz extension


@pytest.mark.asyncio
async def test_extract_ocr_local_storage_with_compression_disabled(sample_unpack_data):
    """Extract OCR should handle local storage without compression."""
    from tests.test_utils.unified_mocks import configure_pipeline_storage, mock_extract_ocr_operations

    filesystem_manager = MagicMock(spec=DirectoryManager)
    pipeline = MagicMock()
    pipeline.filesystem_manager = filesystem_manager
    pipeline.storage = MagicMock()
    pipeline.storage.write_file = AsyncMock()
    pipeline.config = MagicMock()
    pipeline.config.sync_compression_full_enabled = False

    unpack_data = sample_unpack_data()

    with tempfile.TemporaryDirectory() as temp_dir:
        staging_path = Path(temp_dir) / "staging"
        output_dir = Path(temp_dir) / OUTPUT_DIR
        staging_path.mkdir()
        filesystem_manager.staging_path = staging_path

        # Configure for local storage
        configure_pipeline_storage(pipeline, storage_type="local", base_path=str(output_dir))

        # Mock book_manager with proper path method
        pipeline.book_manager = MagicMock()

        def mock_full_text_path(filename):
            full_dir = output_dir / "full"
            return str(full_dir / filename)

        pipeline.book_manager.full_text_path.side_effect = mock_full_text_path

        with mock_extract_ocr_operations(page_count=2) as mocks:
            # Create staging file when extract_ocr_pages is called
            async def create_staging_file(unpack_data, jsonl_path):
                jsonl_path.write_text('["test page 1", "test page 2"]')
                return 2

            mocks.extract_ocr_pages.side_effect = create_staging_file

            result = await extract_ocr.main("TEST123", unpack_data, pipeline)

            # Should not use compression and upload file without .gz extension
            assert result.action == TaskAction.COMPLETED
            expected_path = output_dir / "full" / "TEST123_ocr.jsonl"  # No .gz extension for uncompressed
            assert str(result.data["json_file_path"]) == str(expected_path)
            mocks.compress_file_to_temp.assert_not_called()

            # Verify write_file was called with correct arguments
            pipeline.storage.write_file.assert_called_once()
            call_args = pipeline.storage.write_file.call_args
            assert call_args[0][0] == str(expected_path)  # destination path
            assert str(call_args[0][1]).endswith("TEST123_ocr.jsonl")  # source path (staging file)
            assert "barcode" in call_args[0][2]  # metadata contains barcode


@pytest.mark.asyncio
async def test_extract_ocr_includes_extraction_time_ms(mock_pipeline, sample_unpack_data):
    """Extract OCR task should include extraction_time_ms in result data."""
    from tests.test_utils.unified_mocks import mock_extract_ocr_operations

    unpack_data = sample_unpack_data()

    with mock_extract_ocr_operations(page_count=3) as mocks:
        compressed_path = Path(mock_pipeline.filesystem_manager.staging_path) / "compressed.gz"
        mocks.compress_file_to_temp.return_value.__aenter__.return_value = compressed_path
        mock_pipeline.storage.write_file = AsyncMock()

        result = await extract_ocr.main("TEST123", unpack_data, mock_pipeline)

        assert result.action == TaskAction.COMPLETED
        assert result.data
        assert result.data["page_count"] == 3
        assert "extraction_time_ms" in result.data  # Just verify the field exists
        assert isinstance(result.data["extraction_time_ms"], int)  # And that it's an integer
        assert "TEST123_ocr.jsonl" in str(result.data["json_file_path"])
