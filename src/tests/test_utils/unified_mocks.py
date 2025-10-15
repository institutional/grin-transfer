"""
Unified mock architecture for grin-to-s3 tests.

This module provides:
- Context managers for complex patching scenarios
- Integration with moto for realistic cloud storage testing
- Simple mock creation functions
"""

import tempfile
from contextlib import contextmanager
from pathlib import Path
from typing import Any
from unittest.mock import AsyncMock, MagicMock, patch

import aiohttp
import boto3
import pytest
from botocore.exceptions import ClientError
from moto import mock_aws

from grin_transfer.collect_books.models import SQLiteProgressTracker
from grin_transfer.run_config import StorageConfig
from tests.utils import create_test_archive

# =============================================================================
# Mock Creation Functions
# =============================================================================


def standard_storage_config(
    storage_type: str = "local",
    bucket_raw: str = "test-raw",
    bucket_meta: str = "test-meta",
    bucket_full: str = "test-full",
) -> StorageConfig:
    """Standard storage configuration"""
    return {
        "type": storage_type,
        "protocol": storage_type,
        "config": {"bucket_raw": bucket_raw, "bucket_meta": bucket_meta, "bucket_full": bucket_full},
    }


def create_storage_mock(
    storage_type: str = "local",
    s3_compatible: bool | None = None,
    should_fail: bool = False,
    custom_config: dict[str, Any] | None = None,
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
        mock_storage.write_text = AsyncMock(side_effect=Exception("Storage write failed"))
        mock_storage.save_decrypted_archive_from_file = AsyncMock(side_effect=Exception("Storage upload failed"))
        mock_storage.save_ocr_text_jsonl_from_file = AsyncMock(side_effect=Exception("OCR upload failed"))
    else:
        mock_storage.write_file = AsyncMock(return_value=None)
        mock_storage.write_text = AsyncMock(return_value=None)
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


def create_book_manager_mock(
    storage: MagicMock | None = None,
    storage_config: StorageConfig | None = None,
    base_prefix: str = "",
    should_fail: bool = False,
    custom_config: dict[str, Any] | None = None,
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

    # Create or use provided storage mock
    if storage is None:
        storage = create_storage_mock(should_fail=should_fail, custom_config=custom_config)

    if storage_config is None:
        storage_config = standard_storage_config()

    config = custom_config or {}
    mock_book_manager = MagicMock()

    # Store the underlying storage mock (this is the key fix!)
    mock_book_manager.storage = storage

    # Set bucket attributes
    mock_book_manager.bucket_raw = storage_config["config"].get("bucket_raw")
    mock_book_manager.bucket_meta = storage_config["config"].get("bucket_meta")
    mock_book_manager.bucket_full = storage_config["config"].get("bucket_full")
    mock_book_manager.base_prefix = base_prefix

    # Add path generation helpers
    def meta_path(filename: str) -> str:
        if base_prefix:
            return f"{mock_book_manager.bucket_meta}/{base_prefix}/{filename}"
        return f"{mock_book_manager.bucket_meta}/{filename}"

    def full_text_path(filename: str) -> str:
        if base_prefix:
            return f"{mock_book_manager.bucket_full}/{base_prefix}/{filename}"
        return f"{mock_book_manager.bucket_full}/{filename}"

    mock_book_manager.meta_path = meta_path
    mock_book_manager.full_text_path = MagicMock(side_effect=full_text_path)

    # BookManager methods delegate to the underlying storage
    # This reflects the real BookManager architecture where it wraps a Storage object
    mock_book_manager.save_decrypted_archive_from_file = storage.save_decrypted_archive_from_file
    mock_book_manager.save_ocr_text_jsonl_from_file = storage.save_ocr_text_jsonl_from_file

    # BookManager-specific methods (not in Storage interface)
    if should_fail:
        mock_book_manager.upload_csv_file = AsyncMock(side_effect=Exception("CSV upload failed"))
        mock_book_manager.save_timestamp = AsyncMock(side_effect=Exception("Timestamp save failed"))
    else:
        mock_book_manager.upload_csv_file = AsyncMock(
            return_value=config.get("csv_paths", ("latest.csv", "timestamped.csv"))
        )
        mock_book_manager.save_timestamp = AsyncMock(
            return_value=config.get(
                "timestamp_path", f"{mock_book_manager.bucket_raw}/TEST123/TEST123.tar.gz.gpg.retrieval"
            )
        )

    mock_book_manager.archive_exists = AsyncMock(return_value=config.get("archive_exists", False))
    mock_book_manager.save_text_jsonl = AsyncMock(
        return_value=config.get("text_jsonl_path", f"{mock_book_manager.bucket_raw}/TEST123/TEST123.jsonl")
    )

    return mock_book_manager


def create_staging_manager_mock(staging_path: str = "/tmp/staging") -> MagicMock:
    """
    Create a unified staging manager mock.

    Args:
        staging_path: Base path for staging operations

    Returns:
        Configured staging manager mock
    """
    mock_staging = MagicMock()

    # Create real temporary directory instead of mock Path object
    temp_dir = tempfile.mkdtemp(prefix="staging_test_")
    path_obj = Path(temp_dir)

    # Ensure the directory exists
    path_obj.mkdir(parents=True, exist_ok=True)

    # Set path attributes to real Path objects
    mock_staging.staging_path = path_obj
    mock_staging.staging_dir = path_obj

    # Configure path methods to return real Path objects
    mock_staging.get_staging_path = MagicMock(return_value=path_obj / "test_file")
    mock_staging.get_decrypted_file_path = MagicMock(side_effect=lambda barcode: path_obj / f"{barcode}.tar.gz")
    mock_staging.get_extracted_directory_path = MagicMock(side_effect=lambda barcode: path_obj / f"{barcode}_extracted")

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


def create_progress_tracker_mock(db_path: str = "/tmp/test.db") -> MagicMock:
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
    tracker.update_sync_data = AsyncMock()
    tracker.init_db = AsyncMock()
    cursor_mock = AsyncMock()
    cursor_mock.fetchone = AsyncMock(return_value=None)
    tracker._execute_query = AsyncMock(return_value=cursor_mock)
    tracker.update_book_marc_metadata = AsyncMock()

    return tracker


def create_test_pipeline(
    filesystem_manager: MagicMock | None = None,
    book_manager: MagicMock | None = None,
    storage_type: str = "s3",
    bucket_raw: str = "test-raw",
    bucket_full: str = "test-full",
    bucket_meta: str = "test-meta",
    base_path: str = "/tmp/output",
    compression_meta_enabled: bool = True,
    compression_full_enabled: bool = True,
) -> MagicMock:
    """
    Create a configured test pipeline mock.

    Args:
        filesystem_manager: Filesystem manager (created if None)
        book_manager: Book manager (created if None)
        storage_type: Storage type (s3, local, r2, minio)
        bucket_raw: Raw archive bucket name
        bucket_full: Full text bucket name
        bucket_meta: Metadata bucket name
        base_path: Base path for local storage
        compression_meta_enabled: Enable metadata compression
        compression_full_enabled: Enable full-text compression

    Returns:
        Configured pipeline mock
    """
    pipeline = MagicMock()

    # Set up filesystem manager
    pipeline.filesystem_manager = filesystem_manager or create_staging_manager_mock()

    # Set up book manager
    pipeline.book_manager = book_manager or create_book_manager_mock()

    # Set up config
    pipeline.config = MagicMock()
    pipeline.config.sync_compression_meta_enabled = compression_meta_enabled
    pipeline.config.sync_compression_full_enabled = compression_full_enabled

    # Configure storage
    configure_pipeline_storage(
        pipeline,
        storage_type=storage_type,
        bucket_raw=bucket_raw,
        bucket_full=bucket_full,
        bucket_meta=bucket_meta,
        base_path=base_path,
    )

    return pipeline


def configure_pipeline_storage(
    pipeline: MagicMock,
    storage_type: str = "s3",
    bucket_raw: str = "test-raw",
    bucket_full: str = "test-full",
    bucket_meta: str = "test-meta",
    base_path: str = "/tmp/output",
) -> None:
    """Configure pipeline storage settings.

    Args:
        pipeline: Mock pipeline to configure
        storage_type: Storage type (s3, r2, local, etc.)
        bucket_raw: Raw archive bucket name
        bucket_full: Full text bucket name
        bucket_meta: Metadata bucket name
        base_path: Base path for local storage
    """
    pipeline.config.storage_config = {
        "protocol": storage_type,
        "type": storage_type,
        "config": (
            {"base_path": base_path}
            if storage_type == "local"
            else {"bucket_raw": bucket_raw, "bucket_full": bucket_full, "bucket_meta": bucket_meta}
        ),
    }

    pipeline.uses_block_storage = storage_type != "local"

    # Update uses_local_storage property
    type(pipeline).uses_local_storage = property(lambda self: self.config.storage_config.get("protocol") == "local")


def create_fresh_tracker():
    """Create a fresh tracker with empty database for testing."""

    temp_dir = tempfile.mkdtemp()
    db_path = Path(temp_dir) / "fresh_test.db"
    return SQLiteProgressTracker(str(db_path))


# =============================================================================
# HTTP Response Mock Factories
# =============================================================================


def create_aiohttp_response(
    status: int = 200,
    headers: dict[str, str] | None = None,
    content: bytes | list[bytes] | None = None,
) -> MagicMock:
    """
    Create a mock aiohttp response with streaming support.

    Args:
        status: HTTP status code
        headers: Response headers dict
        content: Response body as bytes or list of byte chunks for streaming

    Returns:
        Mock response with status, headers, and iter_chunked support

    Example:
        # Simple response
        response = create_aiohttp_response(status=200, headers={"ETag": "abc123"})

        # Streaming response with multiple chunks
        response = create_aiohttp_response(
            content=[b"chunk1", b"chunk2"],
            headers={"ETag": '"my-etag"'}
        )
    """
    response = MagicMock()
    response.status = status
    response.headers = headers or {}

    # Handle streaming content
    chunks = content if isinstance(content, list) else [content or b""]

    async def mock_iter_chunked(_size):
        for chunk in chunks:
            yield chunk

    response.content.iter_chunked = mock_iter_chunked
    return response


def create_grin_head_response(etag: str, size: int) -> MagicMock:
    """
    Create a mock GRIN HEAD response.

    Args:
        etag: ETag value (will be quoted automatically if not already)
        size: Content-Length in bytes

    Returns:
        Mock response with status 200, ETag, and Content-Length headers
    """
    quoted_etag = etag if etag.startswith('"') else f'"{etag}"'
    return create_aiohttp_response(status=200, headers={"ETag": quoted_etag, "Content-Length": str(size)})


def create_client_response_error(
    status: int,
    message: str = "",
    request_url: str = "http://example.com/test",
) -> aiohttp.ClientResponseError:
    """
    Create a mock aiohttp ClientResponseError.

    Args:
        status: HTTP status code (404, 429, 500, etc.)
        message: Error message
        request_url: URL for the request_info

    Returns:
        ClientResponseError with properly mocked request_info

    Example:
        error_404 = create_client_response_error(404, "Not Found")
        error_500 = create_client_response_error(500, "Internal Server Error")
    """
    request_info = MagicMock()
    request_info.real_url = request_url

    error = aiohttp.ClientResponseError(
        request_info=request_info,
        history=(),
        status=status,
        message=message or f"HTTP {status}",
    )
    return error


def create_storage_client_error(
    error_code: str = "404",
    operation: str = "HeadObject",
) -> ClientError:
    """
    Create a mock boto3 ClientError.

    Args:
        error_code: AWS error code (typically "404" for not found)
        operation: AWS operation name

    Returns:
        ClientError with proper error_response structure

    Example:
        not_found = create_storage_client_error("404", "HeadObject")
        access_denied = create_storage_client_error("403", "GetObject")
    """
    return ClientError(error_response={"Error": {"Code": error_code}}, operation_name=operation)


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
    return {"bucket_raw": "test-raw", "bucket_meta": "test-meta", "bucket_full": "test-full"}


@pytest.fixture
def storage_config(storage_type):
    """Storage configuration based on storage type."""
    configs = {
        "local": {"base_path": "/tmp/test-storage"},
        "s3": {"access_key": "test_access_key", "secret_key": "test_secret_key", "region": "us-east-1"},
        "r2": {
            "access_key": "test_access_key",
            "secret_key": "test_secret_key",
            "endpoint_url": "https://testaccount.r2.cloudflarestorage.com",
        },
        "minio": {"access_key": "minioadmin", "secret_key": "minioadmin123", "endpoint_url": "http://localhost:9000"},
    }
    return configs[storage_type]


# =============================================================================
# Context Managers for Complex Mocking Scenarios
# =============================================================================


@contextmanager
def mock_upload_operations(
    storage_type: str = "local",
    should_fail: bool = False,
    skip_ocr: bool = False,
    skip_marc: bool = False,
    storage_config: dict[str, Any] | None = None,
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
        patch("grin_transfer.sync.operations.decrypt_gpg_file") as mock_decrypt,
        patch("grin_transfer.sync.operations.create_storage_from_config") as mock_create_storage,
        patch("grin_transfer.sync.operations.extract_and_upload_ocr_text") as mock_extract_ocr,
        patch("grin_transfer.sync.operations.extract_and_update_marc_metadata") as mock_extract_marc,
        patch("grin_transfer.sync.operations.extract_archive") as mock_extract_archive,
        patch("grin_transfer.sync.operations.BookManager") as mock_book_manager_class,
    ):
        # Create storage mock first
        mock_storage = create_storage_mock(
            storage_type=storage_type, should_fail=should_fail, custom_config=storage_config
        )
        mock_create_storage.return_value = mock_storage

        # Create book manager mock that properly wraps the storage mock
        mock_book_manager = create_book_manager_mock(
            storage=mock_storage,  # Pass the storage mock to be wrapped
            should_fail=should_fail,
        )
        mock_book_manager_class.return_value = mock_book_manager

        # Configure operation mocks
        if should_fail:
            mock_decrypt.side_effect = Exception("Decryption failed")
            mock_extract_archive.side_effect = Exception("Archive extraction failed")
            mock_extract_ocr.side_effect = Exception("OCR extraction failed")
            mock_extract_marc.side_effect = Exception("MARC extraction failed")
        else:
            # Create a side effect for decrypt that actually creates a test archive
            def mock_decrypt_side_effect(encrypted_path: str, decrypted_path: str, *args, **kwargs):
                # Create a simple test archive with a few pages
                pages = {
                    "00000001.txt": "Test page 1 content for mocked extraction",
                    "00000002.txt": "Test page 2 content for mocked extraction",
                    "00000003.txt": "Test page 3 content for mocked extraction",
                }
                # Ensure the target directory exists
                decrypted_path_obj = Path(decrypted_path)
                decrypted_path_obj.parent.mkdir(parents=True, exist_ok=True)

                temp_dir = decrypted_path_obj.parent
                archive_path = create_test_archive(pages, temp_dir, decrypted_path_obj.name)
                # Move the created archive to the expected location
                archive_path.rename(decrypted_path)
                return None

            mock_decrypt.side_effect = mock_decrypt_side_effect
            mock_extract_archive.return_value = 1.5  # Mock extraction duration in seconds
            mock_extract_ocr.return_value = None if not skip_ocr else None
            mock_extract_marc.return_value = [] if not skip_marc else []

        # Return a simple namespace object instead of dataclass
        class MockBundle:
            def __init__(self):
                self.decrypt = mock_decrypt
                self.create_storage = mock_create_storage
                self.extract_archive = mock_extract_archive
                self.extract_ocr = mock_extract_ocr
                self.extract_marc = mock_extract_marc
                self.book_manager_class = mock_book_manager_class
                self.storage = mock_storage
                self.book_manager = mock_book_manager

        yield MockBundle()


@contextmanager
def mock_cloud_storage_backend(storage_type: str = "s3", bucket_names: list[str] | None = None):
    """
    Context manager for testing with realistic cloud storage backend.

    Args:
        storage_type: Storage type (s3, r2, minio)
        bucket_names: List of bucket names to create (defaults to standard test buckets)

    Yields:
        dict: Configuration for cloud storage backend
    """
    if bucket_names is None:
        bucket_names = ["test-raw", "test-meta", "test-full"]

    with mock_aws():
        # Create S3 client and buckets
        if storage_type == "r2":
            # R2 needs custom endpoint setup
            endpoint_url = "https://testaccount.r2.cloudflarestorage.com"
            s3_client = boto3.client("s3", endpoint_url=endpoint_url, region_name="us-east-1")
        elif storage_type == "minio":
            # MinIO needs custom endpoint setup
            endpoint_url = "http://localhost:9000"
            s3_client = boto3.client("s3", endpoint_url=endpoint_url, region_name="us-east-1")
        else:
            # Standard S3
            s3_client = boto3.client("s3", region_name="us-east-1")

        # Create all buckets
        for bucket_name in bucket_names:
            s3_client.create_bucket(Bucket=bucket_name)

        # Build storage config for use with real Storage classes
        storage_config = {
            "type": storage_type,
            "bucket_raw": bucket_names[0] if len(bucket_names) > 0 else "test-raw",
            "bucket_meta": bucket_names[1] if len(bucket_names) > 1 else "test-meta",
            "bucket_full": bucket_names[2] if len(bucket_names) > 2 else "test-full",
            "config": {
                "bucket": bucket_names[0] if len(bucket_names) > 0 else "test-raw",  # Required for S3 storage
                "bucket_raw": bucket_names[0] if len(bucket_names) > 0 else "test-raw",
                "bucket_meta": bucket_names[1] if len(bucket_names) > 1 else "test-meta",
                "bucket_full": bucket_names[2] if len(bucket_names) > 2 else "test-full",
                "access_key": "test_access_key",
                "secret_key": "test_secret_key",
                "region": "us-east-1",
            },
        }

        if storage_type == "r2":
            storage_config["config"]["endpoint_url"] = "https://testaccount.r2.cloudflarestorage.com"
        elif storage_type == "minio":
            storage_config["config"]["endpoint_url"] = "http://localhost:9000"

        yield storage_config


@contextmanager
def mock_minimal_upload():
    """
    Minimal context manager for simple upload tests.

    This patches only the essential operations to prevent import errors
    without full mock setup.
    """
    with (
        patch("grin_transfer.sync.operations.extract_and_update_marc_metadata"),
        patch("grin_transfer.sync.operations.extract_and_upload_ocr_text"),
    ):
        yield


@contextmanager
def mock_extract_ocr_operations(page_count: int = 3, compression_enabled: bool = True):
    """
    Context manager for mocking OCR extraction task dependencies.

    Args:
        page_count: Number of pages to return from extraction
        compression_enabled: Whether to mock compression operations

    Yields:
        MockBundle with extract_ocr_pages and compress_file_to_temp mocks
    """
    with (
        patch("grin_transfer.sync.tasks.extract_ocr.extract_ocr_pages") as mock_extract,
        patch("grin_transfer.sync.tasks.extract_ocr.compress_file_to_temp") as mock_compress,
    ):
        # Configure extraction mock
        mock_extract.return_value = page_count

        # Configure compression mock
        if compression_enabled:
            # Mock compress_file_to_temp as an async context manager
            mock_compress.return_value.__aenter__ = AsyncMock()
            mock_compress.return_value.__aexit__ = AsyncMock()

        class MockBundle:
            def __init__(self):
                self.extract_ocr_pages = mock_extract
                self.compress_file_to_temp = mock_compress

        yield MockBundle()


@contextmanager
def mock_export_csv_operations(record_count: int = 0, csv_content: str = "barcode,title\n"):
    """
    Context manager for mocking CSV export task dependencies.

    Args:
        record_count: Number of records written to CSV
        csv_content: Content to write to the temporary CSV file

    Yields:
        MockBundle with upload_csv_to_storage and write_books_to_csv mocks
    """
    with (
        patch("grin_transfer.sync.tasks.export_csv.upload_csv_to_storage") as mock_upload,
        patch("grin_transfer.sync.tasks.export_csv.write_books_to_csv") as mock_write_csv,
    ):
        # Configure write_csv mock - returns (Path, record_count)
        mock_write_csv.return_value = (None, record_count)  # Path will be set by test

        # Configure upload mock
        mock_upload.return_value = None  # Path will be set by test

        class MockBundle:
            def __init__(self):
                self.upload_csv_to_storage = mock_upload
                self.write_books_to_csv = mock_write_csv

        yield MockBundle()


@contextmanager
def mock_extract_marc_operations(
    marc_metadata: dict[str, str] | None = None, normalized_metadata: dict[str, str] | None = None
):
    """
    Context manager for mocking MARC extraction task dependencies.

    Args:
        marc_metadata: Raw MARC metadata to return from extraction
        normalized_metadata: Normalized metadata for database storage

    Yields:
        MockBundle with extract_marc_metadata and convert_marc_keys_to_db_fields mocks
    """
    if marc_metadata is None:
        marc_metadata = {"title": "Test Book", "author": "Test Author"}
    if normalized_metadata is None:
        normalized_metadata = {"title_display": "Test Book", "author_display": "Test Author"}

    with (
        patch("grin_transfer.sync.tasks.extract_marc.extract_marc_metadata") as mock_extract,
        patch("grin_transfer.sync.tasks.extract_marc.convert_marc_keys_to_db_fields") as mock_convert,
    ):
        # Configure mocks
        mock_extract.return_value = marc_metadata
        mock_convert.return_value = normalized_metadata

        class MockBundle:
            def __init__(self):
                self.extract_marc_metadata = mock_extract
                self.convert_marc_keys_to_db_fields = mock_convert

        yield MockBundle()


# =============================================================================
# Test Data Utilities
# =============================================================================


def create_progress_tracker_with_db_mock(db_path: str) -> MagicMock:
    """Create a progress tracker mock with actual database backing."""
    tracker = MagicMock()
    tracker.db_path = db_path
    tracker.add_status_change = AsyncMock(return_value=True)
    tracker.update_sync_data = AsyncMock()
    tracker.init_db = AsyncMock()
    cursor_mock = AsyncMock()
    cursor_mock.fetchone = AsyncMock(return_value=None)
    tracker._execute_query = AsyncMock(return_value=cursor_mock)
    tracker.update_book_marc_metadata = AsyncMock()
    tracker.get_book_count = AsyncMock(return_value=0)
    tracker.get_enriched_book_count = AsyncMock(return_value=0)
    tracker.get_converted_books_count = AsyncMock(return_value=0)
    return tracker


def standard_bucket_config() -> dict[str, str]:
    """Standard bucket configuration for tests."""
    return {"bucket_raw": "test-raw", "bucket_meta": "test-meta", "bucket_full": "test-full"}
