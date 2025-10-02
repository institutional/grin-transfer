#!/usr/bin/env python3
"""
Tests for sync tasks upload module.
"""

import tempfile
from pathlib import Path
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from grin_to_s3.sync.tasks import upload
from grin_to_s3.sync.tasks.task_types import DecryptData, DownloadData, TaskAction


@pytest.fixture
def sample_download_data():
    """Sample download data for upload testing."""
    with tempfile.TemporaryDirectory() as temp_dir:
        original_path = Path(temp_dir) / "TEST123.tar.gz.gpg"
        yield {
            "file_path": original_path,
            "etag": "test-download-etag",
            "file_size_bytes": 2048,
            "http_status_code": 200,
        }


@pytest.fixture
def sample_decrypt_data():
    """Sample decrypt data for upload testing."""
    with tempfile.TemporaryDirectory() as temp_dir:
        decrypted_path = Path(temp_dir) / "TEST123.tar.gz"
        decrypted_path.write_bytes(b"test decrypted content")
        original_path = Path(temp_dir) / "TEST123.tar.gz.gpg"
        yield {"decrypted_path": decrypted_path, "original_path": original_path}


@pytest.fixture
def mock_book_manager():
    """Mock BookManager with common setup."""
    with patch("grin_to_s3.sync.tasks.upload.BookManager") as mock_book_manager_cls:
        mock_manager = MagicMock()
        mock_manager.raw_archive_path.return_value = "bucket/TEST123/TEST123.tar.gz"
        mock_manager.storage.write_file = AsyncMock()
        mock_book_manager_cls.return_value = mock_manager
        yield mock_book_manager_cls, mock_manager


@pytest.mark.asyncio
async def test_main_successful_upload(mock_pipeline, sample_download_data, sample_decrypt_data, mock_book_manager):
    """Upload task should complete successfully."""
    result = await upload.main("TEST123", sample_download_data, sample_decrypt_data, mock_pipeline)

    assert result.action == TaskAction.COMPLETED
    assert result.data
    assert "upload_path" in result.data
    assert str(result.data["upload_path"]) == "test-bucket/TEST123/TEST123.tar.gz"


@pytest.mark.parametrize(
    "storage_type,bucket_raw,base_path,expected_path",
    [
        ("s3", "test-bucket", None, "test-bucket/TEST123/TEST123.tar.gz"),
        ("local", None, "/tmp/local", "/tmp/local/TEST123/TEST123.tar.gz"),
    ],
)
@pytest.mark.asyncio
async def test_upload_with_storage_types(storage_type, bucket_raw, base_path, expected_path):
    """Upload should work with local and cloud storage types."""
    from tests.test_utils.unified_mocks import configure_pipeline_storage

    pipeline = MagicMock()
    pipeline.storage = MagicMock()
    pipeline.config = MagicMock()

    configure_pipeline_storage(
        pipeline, storage_type=storage_type, bucket_raw=bucket_raw or "test-raw", base_path=base_path or "/tmp/output"
    )

    with tempfile.TemporaryDirectory() as temp_dir:
        decrypted_path = Path(temp_dir) / "TEST123.tar.gz"
        decrypted_path.write_bytes(b"test content")

        decrypt_data: DecryptData = {
            "decrypted_path": decrypted_path,
            "original_path": Path(temp_dir) / "TEST123.tar.gz.gpg",
        }
        download_data: DownloadData = {
            "file_path": Path(temp_dir) / "TEST123.tar.gz.gpg",
            "etag": "test-etag",
            "file_size_bytes": 1024,
            "http_status_code": 200,
        }

        # Mock book manager directly on pipeline since we now use pipeline.book_manager
        pipeline.book_manager = MagicMock()
        pipeline.book_manager.raw_archive_path.return_value = expected_path
        pipeline.book_manager.storage.write_file = AsyncMock()
        pipeline.book_manager._manager_id = "test-mgr"

        result = await upload.main("TEST123", download_data, decrypt_data, pipeline)

        assert result.action == TaskAction.COMPLETED
        assert result.data

        # For local storage, the upload_path should be the decrypted_path
        if pipeline.uses_local_storage:
            assert str(result.data["upload_path"]) == str(decrypt_data["decrypted_path"])
        else:
            assert str(result.data["upload_path"]) == expected_path


@pytest.mark.asyncio
async def test_upload_metadata_includes_etag_and_barcode(sample_download_data, sample_decrypt_data, mock_book_manager):
    """Upload should include ETag and barcode in metadata."""
    from tests.test_utils.unified_mocks import configure_pipeline_storage

    _, mock_manager = mock_book_manager
    pipeline = MagicMock()
    pipeline.storage = MagicMock()
    pipeline.config = MagicMock()

    configure_pipeline_storage(pipeline, bucket_raw="test-bucket")

    # Set up book_manager on the pipeline
    pipeline.book_manager = mock_manager

    await upload.main("METADATA123", sample_download_data, sample_decrypt_data, pipeline)

    call_args = mock_manager.storage.write_file.call_args
    metadata = call_args[0][2]

    assert metadata["barcode"] == "METADATA123"
    assert metadata["encrypted_etag"] == "test-download-etag"
    assert "acquisition_date" in metadata
    assert "original_filename" in metadata


@pytest.mark.asyncio
async def test_upload_requires_etag_and_decrypted_path(mock_pipeline):
    """Upload should validate required data fields."""
    bad_download_data: DownloadData = {
        "file_path": Path("/test/path"),
        "etag": None,
        "file_size_bytes": 1024,
        "http_status_code": 200,
    }

    decrypt_data: DecryptData = {
        "decrypted_path": Path("/test/decrypted.tar.gz"),
        "original_path": Path("/test/original.tar.gz.gpg"),
    }

    with pytest.raises(AssertionError):
        await upload.upload_book_from_filesystem("TEST123", decrypt_data, bad_download_data, MagicMock())
