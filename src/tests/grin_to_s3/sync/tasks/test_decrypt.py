#!/usr/bin/env python3
"""
Tests for sync tasks decrypt module.
"""

import tempfile
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest

from grin_to_s3.storage.staging import DirectoryManager
from grin_to_s3.sync.tasks import decrypt
from grin_to_s3.sync.tasks.task_types import DownloadData, TaskAction


@pytest.mark.asyncio
async def test_main_successful_decrypt(mock_pipeline):
    """Decrypt task should complete successfully."""
    download_data: DownloadData = {
        "file_path": Path("/tmp/TEST123.tar.gz.gpg"),
        "etag": "abc123",
        "file_size_bytes": 1024,
        "http_status_code": 200,
    }

    with patch("grin_to_s3.sync.tasks.decrypt.decrypt_gpg_file"):
        result = await decrypt.main("TEST123", download_data, mock_pipeline)

        assert result.action == TaskAction.COMPLETED
        assert result.data
        assert result.data["decrypted_path"] == Path(mock_pipeline.filesystem_manager.staging_path) / "TEST123.tar.gz"
        assert result.data["original_path"] == download_data["file_path"]


@pytest.mark.asyncio
async def test_decrypt_creates_parent_directories(mock_pipeline):
    """Decrypt should create parent directories."""
    download_data: DownloadData = {
        "file_path": Path("/tmp/TEST123.tar.gz.gpg"),
        "etag": "abc123",
        "file_size_bytes": 1024,
        "http_status_code": 200,
    }

    # Test with the existing path function that returns nested paths
    staging_path = Path(mock_pipeline.filesystem_manager.staging_path)

    with patch("grin_to_s3.sync.tasks.decrypt.decrypt_gpg_file"):
        result = await decrypt.main("TEST123", download_data, mock_pipeline)

        # The default mock creates TEST123.tar.gz which should exist in staging directory
        expected_path = staging_path / "TEST123.tar.gz"
        assert result.data
        assert result.data["decrypted_path"] == expected_path
        assert expected_path.parent.exists()


@pytest.mark.asyncio
async def test_decrypt_with_secrets_dir():
    """Decrypt should pass secrets_dir to decrypt_gpg_file."""

    filesystem_manager = MagicMock(spec=DirectoryManager)
    secrets_dir = "/path/to/secrets"

    download_data: DownloadData = {
        "file_path": Path("/tmp/TEST123.tar.gz.gpg"),
        "etag": "abc123",
        "file_size_bytes": 1024,
        "http_status_code": 200,
    }

    with tempfile.TemporaryDirectory() as temp_dir:
        decrypted_path = Path(temp_dir) / "TEST123.tar.gz"
        filesystem_manager.get_decrypted_file_path.return_value = decrypted_path

        with patch("grin_to_s3.sync.tasks.decrypt.decrypt_gpg_file") as mock_decrypt:
            result = await decrypt.decrypt_book("TEST123", download_data, filesystem_manager, secrets_dir)

            mock_decrypt.assert_called_once_with(str(download_data["file_path"]), str(decrypted_path), secrets_dir)
            assert result["decrypted_path"] == decrypted_path
