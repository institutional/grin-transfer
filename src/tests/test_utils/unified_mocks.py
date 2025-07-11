"""
Unified mock architecture for grin-to-s3 tests.

This module consolidates all mocking patterns and provides:
- Parametrized fixtures for different storage types
- Factory functions for custom mock creation
- Context managers for complex patching scenarios
- Integration with moto for realistic cloud storage testing
"""

import tempfile
from contextlib import contextmanager
from dataclasses import dataclass
from pathlib import Path
from typing import Any
from unittest.mock import AsyncMock, MagicMock, patch

import pytest
from moto import mock_aws

# =============================================================================
# Data Classes for Organized Mock Bundles
# =============================================================================

@dataclass
class StorageMockBundle:
    """Bundle of storage-related mocks."""
    storage: MagicMock
    book_storage: MagicMock
    staging_manager: MagicMock
    bucket_config: dict[str, str]


@dataclass
class SyncOperationMocks:
    """Bundle of mocks for sync operations."""
    decrypt: MagicMock
    create_storage: MagicMock
    extract_ocr: MagicMock
    extract_marc: MagicMock
    book_storage_class: MagicMock
    storage: MagicMock
    book_storage: MagicMock


@dataclass
class PipelineMocks:
    """Bundle of mocks for pipeline operations."""
    tracker: MagicMock
    reporter: MagicMock
    client: MagicMock
    staging: MagicMock


# =============================================================================
# Core Mock Factories
# =============================================================================

class MockStorageFactory:
    """Factory for creating storage mocks with various configurations."""

    @staticmethod
    def create_storage(
        storage_type: str = "local",
        s3_compatible: bool | None = None,
        should_fail: bool = False,
        custom_config: dict[str, Any] | None = None
    ) -> MagicMock:
        """
        Create a unified storage mock.

        Args:
            storage_type: Type of storage (local, s3, r2, minio)
            s3_compatible: Override s3 compatibility detection
            should_fail: Configure operations to fail
            custom_config: Custom configuration for return values

        Returns:
            Configured storage mock
        """
        mock_storage = MagicMock()
        config = custom_config or {}

        # Auto-detect s3 compatibility if not specified
        if s3_compatible is None:
            s3_compatible = storage_type in ("s3", "r2", "minio")

        mock_storage.is_s3_compatible = MagicMock(return_value=s3_compatible)

        # Configure async methods
        if should_fail:
            mock_storage.write_file = AsyncMock(side_effect=Exception("Storage write failed"))
            mock_storage.write_bytes = AsyncMock(side_effect=Exception("Storage write failed"))
            mock_storage.write_text = AsyncMock(side_effect=Exception("Storage write failed"))
            mock_storage.save_decrypted_archive_from_file = AsyncMock(
                side_effect=Exception("Storage upload failed")
            )
            mock_storage.save_ocr_text_jsonl_from_file = AsyncMock(
                side_effect=Exception("OCR upload failed")
            )
        else:
            mock_storage.write_file = AsyncMock(return_value=None)
            mock_storage.write_bytes = AsyncMock(return_value=None)
            mock_storage.write_text = AsyncMock(return_value=None)
            mock_storage.write_bytes_with_metadata = AsyncMock(
                return_value=config.get("metadata_path", "test-path")
            )
            mock_storage.save_decrypted_archive_from_file = AsyncMock(
                return_value=config.get("archive_path", "test-raw/TEST123/TEST123.tar.gz")
            )
            mock_storage.save_ocr_text_jsonl_from_file = AsyncMock(
                return_value=config.get("ocr_path", "test-full/TEST123.jsonl")
            )

        # Configure sync methods
        mock_storage.read_bytes = AsyncMock(return_value=b"test data")
        mock_storage.read_text = AsyncMock(return_value="test content")
        mock_storage.exists = AsyncMock(return_value=True)
        mock_storage.delete = AsyncMock(return_value=None)

        return mock_storage

    @staticmethod
    def create_book_manager(
        storage: MagicMock | None = None,
        bucket_config: dict[str, str] | None = None,
        base_prefix: str = "",
        should_fail: bool = False,
        custom_config: dict[str, Any] | None = None
    ) -> MagicMock:
        """
        Create a unified BookManager mock that properly wraps a Storage mock.

        Args:
            storage: Storage mock to wrap (if None, creates one automatically)
            bucket_config: Bucket configuration dictionary
            base_prefix: Base prefix for paths
            should_fail: Configure operations to fail
            custom_config: Custom configuration for return values

        Returns:
            Configured BookManager mock that wraps the storage mock
        """
        if bucket_config is None:
            bucket_config = {
                "bucket_raw": "test-raw",
                "bucket_meta": "test-meta",
                "bucket_full": "test-full"
            }

        # Create or use provided storage mock
        if storage is None:
            storage = MockStorageFactory.create_storage(should_fail=should_fail, custom_config=custom_config)

        config = custom_config or {}
        mock_book_manager = MagicMock()

        # Store the underlying storage mock (this is the key fix!)
        mock_book_manager.storage = storage

        # Set bucket attributes
        mock_book_manager.bucket_raw = bucket_config["bucket_raw"]
        mock_book_manager.bucket_meta = bucket_config["bucket_meta"]
        mock_book_manager.bucket_full = bucket_config["bucket_full"]
        mock_book_manager.base_prefix = base_prefix

        # Add path generation helper
        def meta_path(filename: str) -> str:
            if base_prefix:
                return f"{bucket_config['bucket_meta']}/{base_prefix}/{filename}"
            return f"{bucket_config['bucket_meta']}/{filename}"

        mock_book_manager._meta_path = meta_path

        # BookManager methods delegate to the underlying storage
        # This reflects the real BookManager architecture where it wraps a Storage object
        mock_book_manager.save_decrypted_archive_from_file = storage.save_decrypted_archive_from_file
        mock_book_manager.save_ocr_text_jsonl_from_file = storage.save_ocr_text_jsonl_from_file

        # BookManager-specific methods (not in Storage interface)
        if should_fail:
            mock_book_manager.upload_csv_file = AsyncMock(
                side_effect=Exception("CSV upload failed")
            )
            mock_book_manager.save_timestamp = AsyncMock(
                side_effect=Exception("Timestamp save failed")
            )
        else:
            mock_book_manager.upload_csv_file = AsyncMock(
                return_value=config.get("csv_paths", ("latest.csv", "timestamped.csv"))
            )
            mock_book_manager.save_timestamp = AsyncMock(
                return_value=config.get("timestamp_path", f"{bucket_config['bucket_raw']}/TEST123/TEST123.tar.gz.gpg.retrieval")
            )

        mock_book_manager.archive_exists = AsyncMock(return_value=config.get("archive_exists", False))
        mock_book_manager.save_text_jsonl = AsyncMock(
            return_value=config.get("text_jsonl_path", f"{bucket_config['bucket_raw']}/TEST123/TEST123.jsonl")
        )

        return mock_book_manager

    @staticmethod
    def create_staging_manager(staging_path: str = "/tmp/staging") -> MagicMock:
        """
        Create a unified staging manager mock.

        Args:
            staging_path: Base path for staging operations

        Returns:
            Configured staging manager mock
        """
        mock_staging = MagicMock()
        path_obj = Path(staging_path)

        # Set path attributes
        mock_staging.staging_path = path_obj
        mock_staging.staging_dir = path_obj

        # Configure path methods
        mock_staging.get_staging_path = MagicMock(return_value=path_obj / "test_file")
        mock_staging.get_decrypted_file_path = lambda barcode: path_obj / f"{barcode}.tar.gz"

        # Configure async methods
        mock_staging.cleanup_file = AsyncMock(return_value=1024)
        mock_staging.check_and_wait_for_space = AsyncMock()

        # Configure sync methods
        mock_staging.cleanup_files = MagicMock(return_value=1024 * 1024)  # 1MB
        mock_staging.available_space = MagicMock(return_value=10 * 1024 * 1024 * 1024)  # 10GB

        # Set space attributes
        mock_staging.staging_free_space_gb = 10.0
        mock_staging.min_free_space_gb = 1.0

        return mock_staging

    @staticmethod
    def create_progress_tracker(db_path: str = "/tmp/test.db") -> MagicMock:
        """
        Create a unified progress tracker mock.

        Args:
            db_path: Database path for the tracker

        Returns:
            Configured progress tracker mock
        """
        tracker = MagicMock()
        tracker.db_path = db_path

        # Configure async methods
        tracker.add_status_change = AsyncMock()
        tracker.update_sync_data = AsyncMock()
        tracker.get_books_for_sync = AsyncMock(return_value=[])
        tracker.get_sync_stats = AsyncMock(
            return_value={"total_converted": 0, "synced": 0, "failed": 0, "pending": 0}
        )
        tracker.update_book_marc_metadata = AsyncMock()

        return tracker


# =============================================================================
# Parametrized Fixtures for Common Scenarios
# =============================================================================

@pytest.fixture(params=["local", "s3", "r2", "minio"])
def storage_type(request):
    """Parametrized fixture for different storage types."""
    return request.param


@pytest.fixture
def bucket_config():
    """Standard bucket configuration for tests."""
    return {
        "bucket_raw": "test-raw",
        "bucket_meta": "test-meta",
        "bucket_full": "test-full"
    }


@pytest.fixture
def storage_config(storage_type):
    """Storage configuration based on storage type."""
    configs = {
        "local": {
            "base_path": "/tmp/test-storage"
        },
        "s3": {
            "access_key": "test_access_key",
            "secret_key": "test_secret_key",
            "region": "us-east-1"
        },
        "r2": {
            "access_key": "test_access_key",
            "secret_key": "test_secret_key",
            "account_id": "testaccount"
        },
        "minio": {
            "access_key": "minioadmin",
            "secret_key": "minioadmin123",
            "endpoint_url": "http://localhost:9000"
        }
    }
    return configs[storage_type]


@pytest.fixture
def mock_storage_bundle(storage_type, bucket_config):
    """Complete storage mock bundle for testing."""
    # Create storage first, then book_manager that wraps it
    storage = MockStorageFactory.create_storage(storage_type)
    book_manager = MockStorageFactory.create_book_manager(
        storage=storage,
        bucket_config=bucket_config
    )

    return StorageMockBundle(
        storage=storage,
        book_storage=book_manager,
        staging_manager=MockStorageFactory.create_staging_manager(),
        bucket_config=bucket_config
    )


# =============================================================================
# Context Managers for Complex Mocking Scenarios
# =============================================================================

@contextmanager
def mock_upload_operations(
    storage_type: str = "local",
    should_fail: bool = False,
    skip_ocr: bool = False,
    skip_marc: bool = False,
    storage_config: dict[str, Any] | None = None
):
    """
    Context manager for mocking upload_book_from_staging dependencies.

    Args:
        storage_type: Type of storage to mock
        should_fail: Configure mocks to simulate failures
        skip_ocr: Configure OCR extraction as disabled
        skip_marc: Configure MARC extraction as disabled
        storage_config: Optional configuration for mock storage objects

    Yields:
        SyncOperationMocks bundle
    """
    with (
        patch("grin_to_s3.sync.operations.decrypt_gpg_file") as mock_decrypt,
        patch("grin_to_s3.sync.operations.create_storage_from_config") as mock_create_storage,
        patch("grin_to_s3.sync.operations.extract_and_upload_ocr_text") as mock_extract_ocr,
        patch("grin_to_s3.sync.operations.extract_and_update_marc_metadata") as mock_extract_marc,
        patch("grin_to_s3.sync.operations.BookManager") as mock_book_storage_class,
    ):
        # Create storage mock first
        mock_storage = MockStorageFactory.create_storage(
            storage_type=storage_type,
            should_fail=should_fail,
            custom_config=storage_config
        )
        mock_create_storage.return_value = mock_storage

        # Create book manager mock that properly wraps the storage mock
        mock_book_manager = MockStorageFactory.create_book_manager(
            storage=mock_storage,  # Pass the storage mock to be wrapped
            should_fail=should_fail
        )
        mock_book_storage_class.return_value = mock_book_manager

        # Configure operation mocks
        if should_fail:
            mock_decrypt.side_effect = Exception("Decryption failed")
            mock_extract_ocr.side_effect = Exception("OCR extraction failed")
            mock_extract_marc.side_effect = Exception("MARC extraction failed")
        else:
            mock_decrypt.return_value = None
            mock_extract_ocr.return_value = None if not skip_ocr else None
            mock_extract_marc.return_value = None if not skip_marc else None

        yield SyncOperationMocks(
            decrypt=mock_decrypt,
            create_storage=mock_create_storage,
            extract_ocr=mock_extract_ocr,
            extract_marc=mock_extract_marc,
            book_storage_class=mock_book_storage_class,
            storage=mock_storage,
            book_storage=mock_book_manager
        )


@contextmanager
def mock_pipeline_operations():
    """Context manager for mocking sync pipeline dependencies."""
    with (
        patch("grin_to_s3.sync.pipeline.SQLiteProgressTracker") as mock_tracker_class,
        patch("grin_to_s3.sync.pipeline.ProgressReporter") as mock_reporter_class,
        patch("grin_to_s3.sync.pipeline.GRINClient") as mock_client_class,
        patch("grin_to_s3.storage.StagingDirectoryManager") as mock_staging_class,
    ):
        # Create instances
        tracker = MockStorageFactory.create_progress_tracker()
        mock_tracker_class.return_value = tracker

        reporter = MagicMock()
        mock_reporter_class.return_value = reporter

        client = MagicMock()
        mock_client_class.return_value = client

        staging = MockStorageFactory.create_staging_manager()
        mock_staging_class.return_value = staging

        yield PipelineMocks(
            tracker=tracker,
            reporter=reporter,
            client=client,
            staging=staging
        )


@contextmanager
def mock_aws_services():
    """Context manager for mocking AWS services using moto."""
    with mock_aws():
        yield


@contextmanager
def mock_minimal_upload():
    """
    Minimal context manager for simple upload tests.

    This patches only the essential operations to prevent import errors
    without full mock setup.
    """
    with (
        patch("grin_to_s3.sync.operations.extract_and_update_marc_metadata"),
        patch("grin_to_s3.sync.operations.extract_and_upload_ocr_text"),
    ):
        yield


# =============================================================================
# Pytest-Mock Integration
# =============================================================================

def setup_storage_mocks_with_mocker(mocker, storage_type: str = "local", should_fail: bool = False):
    """
    Setup storage mocks using pytest-mock mocker fixture.

    Args:
        mocker: pytest-mock mocker fixture
        storage_type: Type of storage to mock
        should_fail: Configure mocks to fail

    Returns:
        StorageMockBundle with configured mocks
    """
    bucket_config = {
        "bucket_raw": "test-raw",
        "bucket_meta": "test-meta",
        "bucket_full": "test-full"
    }

    # Create storage mock first
    mock_storage = MockStorageFactory.create_storage(storage_type, should_fail=should_fail)
    mocker.patch("grin_to_s3.storage.create_storage_from_config", return_value=mock_storage)

    # Create book manager that wraps the storage mock
    mock_book_manager = MockStorageFactory.create_book_manager(
        storage=mock_storage,
        bucket_config=bucket_config,
        should_fail=should_fail
    )
    mocker.patch("grin_to_s3.storage.BookManager", return_value=mock_book_manager)

    # Mock staging manager
    mock_staging = MockStorageFactory.create_staging_manager()
    mocker.patch("grin_to_s3.storage.StagingDirectoryManager", return_value=mock_staging)

    return StorageMockBundle(
        storage=mock_storage,
        book_storage=mock_book_manager,
        staging_manager=mock_staging,
        bucket_config=bucket_config
    )


# =============================================================================
# Realistic Test Data Factories
# =============================================================================

def create_temp_staging_dir():
    """Create a temporary staging directory for testing."""
    return tempfile.mkdtemp(prefix="grin_test_staging_")


def create_mock_book_data(barcode: str = "TEST123"):
    """Create realistic mock book data for testing."""
    return {
        "barcode": barcode,
        "title": f"Test Book {barcode}",
        "author": "Test Author",
        "pages": ["Page 1 content", "Page 2 content", "Page 3 content"]
    }


# =============================================================================
# Migration Helpers (for backward compatibility)
# =============================================================================

# Aliases for backward compatibility during migration
create_mock_storage = MockStorageFactory.create_storage
create_mock_book_storage = MockStorageFactory.create_book_manager
create_mock_staging_manager = MockStorageFactory.create_staging_manager
create_mock_progress_tracker = MockStorageFactory.create_progress_tracker

def standard_bucket_config() -> dict[str, str]:
    """Standard bucket configuration for tests."""
    return {
        "bucket_raw": "test-raw",
        "bucket_meta": "test-meta",
        "bucket_full": "test-full"
    }
