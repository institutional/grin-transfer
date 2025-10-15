#!/usr/bin/env python3
"""
Shared test fixtures for sync tasks testing.
"""

import tempfile
from pathlib import Path
from unittest.mock import AsyncMock, MagicMock

import pytest

from grin_transfer.client import GRINClient
from grin_transfer.storage.staging import DirectoryManager
from grin_transfer.sync.tasks.task_types import DecryptData, DownloadData, UnpackData
from tests.test_utils.unified_mocks import configure_pipeline_storage, create_test_pipeline


@pytest.fixture
def temp_filesystem_manager():
    """Fixture providing a filesystem manager with real temporary directory."""
    with tempfile.TemporaryDirectory() as temp_dir:
        manager = MagicMock(spec=DirectoryManager)
        manager.staging_path = Path(temp_dir)
        manager.get_decrypted_file_path = MagicMock(side_effect=lambda barcode: Path(temp_dir) / f"{barcode}.tar.gz")
        manager.get_extracted_directory_path = MagicMock(
            side_effect=lambda barcode: Path(temp_dir) / f"{barcode}_extracted"
        )
        manager.get_encrypted_file_path = MagicMock(
            side_effect=lambda barcode: Path(temp_dir) / f"{barcode}.tar.gz.gpg"
        )
        manager.check_disk_space = MagicMock(return_value=True)
        yield manager


@pytest.fixture
def mock_pipeline():
    """Mock SyncPipeline for task testing with common attributes."""
    with tempfile.TemporaryDirectory() as temp_dir:
        pipeline = create_test_pipeline()

        # Common pipeline attributes
        pipeline.library_directory = "TestLib"
        pipeline.secrets_dir = "/path/to/secrets"
        pipeline.uses_block_storage = True
        pipeline.skip_staging_cleanup = False
        pipeline.start_time = 1234567890
        pipeline.force = False  # Default to False for normal skip behavior

        # Mock grin client with auth
        pipeline.grin_client = MagicMock(spec=GRINClient)
        pipeline.grin_client.auth = MagicMock()
        pipeline.grin_client.auth.make_authenticated_request = AsyncMock()
        pipeline.grin_client.fetch_resource = AsyncMock()

        # Conversion tracking
        pipeline.conversion_requests_made = 0
        pipeline.conversion_failure_metadata = {}

        # Mock filesystem manager with temp directory paths
        pipeline.filesystem_manager = MagicMock(spec=DirectoryManager)
        pipeline.filesystem_manager.staging_path = Path(temp_dir)

        # Mock the path methods to return paths within temp directory
        def get_decrypted_path(barcode):
            return Path(temp_dir) / f"{barcode}.tar.gz"

        def get_extracted_path(barcode):
            return Path(temp_dir) / barcode

        pipeline.filesystem_manager.get_decrypted_file_path.side_effect = get_decrypted_path
        pipeline.filesystem_manager.get_extracted_directory_path.side_effect = get_extracted_path

        # Mock storage and config
        pipeline.storage = MagicMock()
        pipeline.storage.write_file = AsyncMock()
        pipeline.config = MagicMock()

        # Use helper to configure default storage
        configure_pipeline_storage(pipeline)

        # Mock book manager
        pipeline.book_manager = MagicMock()
        pipeline.book_manager.raw_archive_path = MagicMock(return_value="test-bucket/TEST123/TEST123.tar.gz")
        pipeline.book_manager.full_text_path = MagicMock(side_effect=lambda filename: f"test-bucket/{filename}")
        pipeline.book_manager.storage = pipeline.storage
        pipeline.book_manager._manager_id = "test-mgr"

        # Mock database tracker
        pipeline.db_tracker = MagicMock()
        pipeline.db_tracker.update_book_marc_metadata = AsyncMock()
        pipeline.db_tracker.close = AsyncMock()
        pipeline.db_tracker.get_all_books_csv_data = AsyncMock()

        # Mock stats and output functions (none needed currently)

        yield pipeline


@pytest.fixture
def sample_download_data():
    """Factory fixture for creating DownloadData test instances.

    Returns a function that creates DownloadData with customizable fields.
    Default values represent a successful download scenario.
    """

    def _create_download_data(
        file_path: Path | None = None,
        etag: str | None = "abc123",
        file_size_bytes: int = 1024,
        http_status_code: int = 200,
    ) -> DownloadData:
        if file_path is None:
            file_path = Path("/tmp/TEST123.tar.gz.gpg")
        return {
            "file_path": file_path,
            "etag": etag,
            "file_size_bytes": file_size_bytes,
            "http_status_code": http_status_code,
        }

    return _create_download_data


@pytest.fixture
def sample_decrypt_data():
    """Factory fixture for creating DecryptData test instances.

    Returns a function that creates DecryptData with customizable fields.
    Default values represent a successful decryption scenario.
    """

    def _create_decrypt_data(
        decrypted_path: Path | None = None,
        original_path: Path | None = None,
    ) -> DecryptData:
        if decrypted_path is None:
            decrypted_path = Path("/tmp/TEST123.tar.gz")
        if original_path is None:
            original_path = Path("/tmp/TEST123.tar.gz.gpg")
        return {
            "decrypted_path": decrypted_path,
            "original_path": original_path,
        }

    return _create_decrypt_data


@pytest.fixture
def sample_unpack_data():
    """Factory fixture for creating UnpackData test instances.

    Returns a function that creates UnpackData with customizable fields.
    Default values represent a successful unpack scenario.
    """

    def _create_unpack_data(
        unpacked_path: Path | None = None,
    ) -> UnpackData:
        if unpacked_path is None:
            unpacked_path = Path("/tmp/TEST123")
        return {
            "unpacked_path": unpacked_path,
        }

    return _create_unpack_data
