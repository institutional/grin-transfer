#!/usr/bin/env python3
"""
Tests for sync tasks download module.

Tests the download task functionality that was previously part of operations.py.
Covers equivalent functionality to the download tests from the original operations module.
"""

import tempfile
from pathlib import Path
from unittest.mock import AsyncMock, MagicMock, patch

import aiohttp
import pytest

from grin_to_s3.storage.staging import DirectoryManager
from grin_to_s3.sync.tasks import download
from grin_to_s3.sync.tasks.task_types import TaskAction
from tests.test_utils.parametrize_helpers import meaningful_storage_parametrize
from tests.test_utils.unified_mocks import create_aiohttp_response, create_client_response_error, create_test_pipeline


@pytest.fixture
def temp_download_path():
    """Create a temporary path for download testing."""
    with tempfile.TemporaryDirectory() as temp_dir:
        temp_path = Path(temp_dir) / "TEST123.tar.gz.gpg"
        yield temp_path


def setup_download_mock(mock_grin_client, response=None, error=None):
    """Helper to setup download mocks consistently."""
    mock_grin_client.auth.make_authenticated_request = AsyncMock(side_effect=error, return_value=response)
    mock_grin_client.download_archive = AsyncMock(side_effect=error, return_value=response)


class TestDownloadMain:
    """Test the main download task entry point."""

    @pytest.mark.asyncio
    async def test_main_successful_download(self, mock_pipeline, temp_download_path):
        """Main download should return completed task result for successful downloads."""
        mock_pipeline.filesystem_manager.get_encrypted_file_path.return_value = temp_download_path

        with patch("grin_to_s3.sync.tasks.download.download_book_to_filesystem") as mock_download:
            mock_download.return_value = {
                "file_path": temp_download_path,
                "http_status_code": 200,
                "etag": "abc123",
                "file_size_bytes": 1024,
            }
            result = await download.main("TEST123", mock_pipeline)

            assert result.action == TaskAction.COMPLETED
            assert result.data is not None
            assert result.data["file_size_bytes"] == 1024

    @pytest.mark.asyncio
    async def test_main_failed_download(self, mock_pipeline, temp_download_path):
        """Main download should return failed task result when file size is zero."""
        mock_pipeline.filesystem_manager.get_encrypted_file_path.return_value = temp_download_path

        with patch("grin_to_s3.sync.tasks.download.download_book_to_filesystem") as mock_download:
            mock_download.return_value = {
                "file_path": temp_download_path,
                "http_status_code": 404,
                "etag": None,
                "file_size_bytes": 0,
            }
            result = await download.main("TEST123", mock_pipeline)

            assert result.action == TaskAction.FAILED

    @pytest.mark.asyncio
    async def test_main_creates_parent_directories(self, mock_pipeline):
        """Main download should create parent directories if they don't exist."""
        staging_path = Path(mock_pipeline.filesystem_manager.staging_path)
        nested_path = staging_path / "deep" / "nested" / "TEST123.tar.gz.gpg"
        mock_pipeline.filesystem_manager.get_encrypted_file_path.return_value = nested_path

        with patch("grin_to_s3.sync.tasks.download.download_book_to_filesystem") as mock_download:
            mock_download.return_value = {
                "file_path": nested_path,
                "http_status_code": 200,
                "etag": "abc123",
                "file_size_bytes": 1024,
            }
            await download.main("TEST123", mock_pipeline)

            assert nested_path.parent.exists()


class TestDownloadBookToFilesystem:
    """Test the core download_book_to_filesystem function."""

    @pytest.mark.asyncio
    async def test_successful_download(self, temp_download_path, mock_grin_client, temp_filesystem_manager):
        """Successful downloads should write file to disk and return metadata."""
        test_content = b"test archive content"
        response = create_aiohttp_response(content=test_content, headers={"ETag": '"abc123"'})
        setup_download_mock(mock_grin_client, response)

        result = await download.download_book_to_filesystem(
            "TEST123", temp_download_path, mock_grin_client, "TestLib", temp_filesystem_manager
        )

        assert result["http_status_code"] == 200
        assert result["etag"] == "abc123"
        assert temp_download_path.exists()
        assert temp_download_path.read_bytes() == test_content

    @pytest.mark.asyncio
    async def test_http_404_error_no_retry(self, temp_download_path, mock_grin_client, temp_filesystem_manager):
        """404 errors should not be retried."""
        error_404 = create_client_response_error(404, "Not Found")
        setup_download_mock(mock_grin_client, error=error_404)

        with pytest.raises(aiohttp.ClientResponseError):
            await download.download_book_to_filesystem(
                "TEST123", temp_download_path, mock_grin_client, "TestLib", temp_filesystem_manager
            )

        assert mock_grin_client.download_archive.call_count == 1

    @pytest.mark.asyncio
    @pytest.mark.slow
    async def test_http_500_error_with_retry(
        self, temp_download_path, mock_grin_client, temp_filesystem_manager, fast_retry_download
    ):
        """500 errors should be retried with exponential backoff."""
        error_500 = create_client_response_error(500, "Internal Server Error")
        setup_download_mock(mock_grin_client, error=error_500)

        with pytest.raises(aiohttp.ClientResponseError):
            await download.download_book_to_filesystem(
                "TEST123", temp_download_path, mock_grin_client, "TestLib", temp_filesystem_manager
            )

        assert mock_grin_client.download_archive.call_count == 3

    @pytest.mark.asyncio
    async def test_disk_space_exhaustion_recovery(self, temp_download_path, mock_grin_client):
        """Downloads should pause and retry when disk space is exhausted."""
        filesystem_manager = MagicMock(spec=DirectoryManager)
        filesystem_manager.check_disk_space.side_effect = [False, True]
        filesystem_manager.wait_for_disk_space = AsyncMock()

        chunks = [b"x" * (30 * 1024 * 1024), b"x" * (30 * 1024 * 1024)]  # 30MB each
        response = create_aiohttp_response(content=chunks, headers={"ETag": '"abc123"'})
        setup_download_mock(mock_grin_client, response)

        result = await download.download_book_to_filesystem(
            "TEST123", temp_download_path, mock_grin_client, "TestLib", filesystem_manager
        )

        assert result["http_status_code"] == 200
        filesystem_manager.wait_for_disk_space.assert_called_once_with(check_interval=60)

    @pytest.mark.asyncio
    async def test_disk_space_check_with_large_chunk(self, temp_download_path, mock_grin_client):
        """Disk space checks should trigger even with chunks larger than 50MB."""
        filesystem_manager = MagicMock(spec=DirectoryManager)
        filesystem_manager.check_disk_space.side_effect = [False, True]
        filesystem_manager.wait_for_disk_space = AsyncMock()

        large_chunk = b"x" * (51 * 1024 * 1024)  # 51MB chunk
        response = create_aiohttp_response(content=large_chunk, headers={"ETag": '"abc123"'})
        setup_download_mock(mock_grin_client, response)

        result = await download.download_book_to_filesystem(
            "TEST123", temp_download_path, mock_grin_client, "TestLib", filesystem_manager
        )

        assert result["http_status_code"] == 200
        filesystem_manager.wait_for_disk_space.assert_called_once_with(check_interval=60)

    @pytest.mark.asyncio
    @pytest.mark.slow
    async def test_file_size_verification(
        self, temp_download_path, mock_grin_client, temp_filesystem_manager, fast_retry_download
    ):
        """Download should verify file size matches bytes written and clean up on mismatch."""
        response = create_aiohttp_response(headers={"ETag": '"abc123"'})
        setup_download_mock(mock_grin_client, response)

        with (
            patch("aiofiles.open", create=True),
            patch.object(Path, "stat") as mock_stat,
            patch.object(Path, "unlink") as mock_unlink,
        ):
            mock_stat.return_value.st_size = 999  # Different from actual content length

            with pytest.raises(Exception, match="File size mismatch"):
                await download.download_book_to_filesystem(
                    "TEST123", temp_download_path, mock_grin_client, "TestLib", temp_filesystem_manager
                )

            assert mock_unlink.call_count == 3  # Called 3 times due to retries

    @pytest.mark.asyncio
    async def test_etag_handling_quoted_and_unquoted(
        self, temp_download_path, mock_grin_client, temp_filesystem_manager
    ):
        """ETags should be stripped of quotes in download results."""
        response = create_aiohttp_response(headers={"ETag": '"quoted-etag"'})
        setup_download_mock(mock_grin_client, response)

        result = await download.download_book_to_filesystem(
            "TEST123", temp_download_path, mock_grin_client, "TestLib", temp_filesystem_manager
        )
        assert result["etag"] == "quoted-etag"

        temp_download_path.unlink(missing_ok=True)
        response = create_aiohttp_response(headers={"ETag": "unquoted-etag"})
        setup_download_mock(mock_grin_client, response)

        result = await download.download_book_to_filesystem(
            "TEST123", temp_download_path, mock_grin_client, "TestLib", temp_filesystem_manager
        )
        assert result["etag"] == "unquoted-etag"

    @pytest.mark.asyncio
    @pytest.mark.slow
    async def test_missing_etag_header(
        self, temp_download_path, mock_grin_client, temp_filesystem_manager, fast_retry_download
    ):
        """Downloads should raise exception when ETag header is missing."""
        response = create_aiohttp_response(headers={})
        setup_download_mock(mock_grin_client, response)

        with pytest.raises(Exception, match="Missing ETag header in download response"):
            await download.download_book_to_filesystem(
                "TEST123", temp_download_path, mock_grin_client, "TestLib", temp_filesystem_manager
            )

    @pytest.mark.asyncio
    async def test_download_with_independent_retry_config(
        self, temp_download_path, mock_grin_client, temp_filesystem_manager
    ):
        """Downloads should use task-independent retry configuration."""
        response = create_aiohttp_response(headers={"ETag": '"abc123"'})
        setup_download_mock(mock_grin_client, response)

        result = await download.download_book_to_filesystem(
            "TEST123",
            temp_download_path,
            mock_grin_client,
            "TestLib",
            temp_filesystem_manager,
        )

        assert result["http_status_code"] == 200


class TestDownloadStorageParametrization:
    """Test download functionality across different storage types."""

    @meaningful_storage_parametrize()
    @pytest.mark.asyncio
    async def test_download_with_storage_type_consistency(
        self, storage_type, temp_download_path, mock_grin_client, temp_filesystem_manager
    ):
        """Download should work consistently across local and block storage types."""
        test_content = b"storage test content"
        response = create_aiohttp_response(content=test_content, headers={"ETag": '"storage-etag"'})
        setup_download_mock(mock_grin_client, response)

        result = await download.download_book_to_filesystem(
            "STORAGE123", temp_download_path, mock_grin_client, "TestLib", temp_filesystem_manager
        )

        # Verify consistent behavior regardless of storage type
        assert result["http_status_code"] == 200
        assert result["etag"] == "storage-etag"  # ETag stripped of quotes
        assert result["file_size_bytes"] == len(test_content)
        assert temp_download_path.exists()
        assert temp_download_path.read_bytes() == test_content

    @meaningful_storage_parametrize()
    @pytest.mark.asyncio
    async def test_main_task_result_consistency_across_storage(
        self, storage_type, mock_grin_client, temp_filesystem_manager
    ):
        """Main download task should return consistent results across storage types."""
        # Create a mock pipeline that represents the storage type
        pipeline = create_test_pipeline(filesystem_manager=temp_filesystem_manager)
        pipeline.grin_client = mock_grin_client
        pipeline.library_directory = "TestLib"

        with tempfile.TemporaryDirectory() as temp_dir:
            download_path = Path(temp_dir) / f"{storage_type}_test.tar.gz.gpg"
            pipeline.filesystem_manager.get_encrypted_file_path.return_value = download_path

            # Mock successful download
            with patch("grin_to_s3.sync.tasks.download.download_book_to_filesystem") as mock_download:
                mock_download.return_value = {
                    "file_path": download_path,
                    "http_status_code": 200,
                    "etag": f"{storage_type}-etag",
                    "file_size_bytes": 2048,
                }

                result = await download.main("MAIN123", pipeline)

                # Task result should be consistent regardless of storage type
                assert result.action == TaskAction.COMPLETED
                assert result.data is not None
                assert result.data["file_size_bytes"] == 2048
                assert result.data["etag"] == f"{storage_type}-etag"
                assert result.data["http_status_code"] == 200

                # Directory creation should work for any storage type
                mock_download.assert_called_once()
                call_args = mock_download.call_args[1] if mock_download.call_args[1] else mock_download.call_args[0]
                assert "MAIN123" in str(call_args)

    @meaningful_storage_parametrize()
    @pytest.mark.asyncio
    async def test_download_consistent_file_path_behavior(
        self, storage_type, temp_download_path, mock_grin_client, temp_filesystem_manager
    ):
        """Download should return the same file path regardless of storage type.

        The download function always writes to the local filesystem staging area,
        regardless of final storage destination. Storage-specific behavior happens
        in later pipeline stages.
        """
        response = create_aiohttp_response(headers={"ETag": '"abc123"'})
        setup_download_mock(mock_grin_client, response)

        result = await download.download_book_to_filesystem(
            "EXPECT123", temp_download_path, mock_grin_client, "TestLib", temp_filesystem_manager
        )

        # Download always succeeds to filesystem staging, regardless of storage type
        assert result["http_status_code"] == 200
        assert temp_download_path.exists()
        assert result["file_path"] == temp_download_path
