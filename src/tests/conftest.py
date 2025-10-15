"""Shared test configuration utilities and fixtures."""

import sqlite3
import tempfile
from pathlib import Path
from typing import cast
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from grin_transfer.run_config import RunConfig, StorageConfig, StorageConfigDict, SyncConfig
from tests.test_utils.unified_mocks import (
    create_book_manager_mock,
    create_progress_tracker_mock,
    create_staging_manager_mock,
    create_storage_mock,
    mock_upload_operations,
)


def _no_sleep(seconds):
    """Synchronous sleep stub used to short-circuit tenacity waits in tests."""
    # Deliberately do nothing to avoid real delays during retry backoff.
    return None


@pytest.fixture(scope="session", autouse=True)
def disable_retry_delays():
    """Disable retry delays globally for all tests to speed up test suite.

    Retries will still happen (testing retry logic), but without wait times.
    Only patches tenacity's internal sleep functions, not asyncio.sleep globally.
    """
    import tenacity

    original_base_run_wait = tenacity.BaseRetrying._run_wait
    original_async_run_wait = tenacity.AsyncRetrying._run_wait

    def _zero_wait(self, retry_state):
        """Invoke original wait logic but force the computed delay to zero."""
        original_base_run_wait(self, retry_state)
        retry_state.upcoming_sleep = 0.0

    async def _zero_wait_async(self, retry_state):
        """Async equivalent that still computes retry metadata without sleeping."""
        await original_async_run_wait(self, retry_state)
        retry_state.upcoming_sleep = 0.0

    with patch("tenacity.nap.sleep", side_effect=_no_sleep):
        with patch.object(tenacity.BaseRetrying, "_run_wait", _zero_wait):
            with patch.object(tenacity.AsyncRetrying, "_run_wait", _zero_wait_async):
                yield


class ConfigBuilder:
    """Builder for creating test RunConfig instances with common configurations."""

    def __init__(self):
        # Default sync config with all required fields
        default_sync_config: SyncConfig = {
            "task_check_concurrency": 5,
            "task_download_concurrency": 3,
            "task_decrypt_concurrency": 5,
            "task_upload_concurrency": 5,
            "task_unpack_concurrency": 5,
            "task_extract_marc_concurrency": 5,
            "task_extract_ocr_concurrency": 2,
            "task_export_csv_concurrency": 1,
            "task_cleanup_concurrency": 5,
            "staging_dir": Path("/tmp/staging"),
            "disk_space_threshold": 0.8,
            "compression_meta_enabled": True,
            "compression_full_enabled": True,
        }

        # Default local storage config with protocol
        default_storage_config: StorageConfig = {
            "type": "local",
            "protocol": "file",
            "config": {"base_path": "/tmp"},
        }

        self._config = {
            "run_name": "test_run",
            "sqlite_db_path": Path("/tmp/test.db"),
            "library_directory": "test_library",
            "output_directory": Path("/tmp/output"),
            "log_file": Path("/tmp/test.log"),
            "secrets_dir": None,
            "storage_config": default_storage_config,
            "sync_config": default_sync_config,
        }

    def with_run_name(self, run_name: str):
        """Set the run name."""
        self._config["run_name"] = run_name
        return self

    def with_db_path(self, db_path: str):
        """Set the database path."""
        self._config["sqlite_db_path"] = Path(db_path)
        return self

    def with_library_directory(self, library_directory: str):
        """Set the library directory."""
        self._config["library_directory"] = library_directory
        return self

    def local_storage(self, base_path: str = "/tmp"):
        """Configure local storage."""
        storage_config: StorageConfig = {
            "type": "local",
            "protocol": "file",
            "config": {"base_path": base_path},
        }
        self._config["storage_config"] = storage_config
        return self

    def s3_storage(
        self, bucket_raw: str = "test-raw", bucket_meta: str = "test-meta", bucket_full: str = "test-full", **kwargs
    ):
        """Configure S3 storage."""
        config = {"bucket_raw": bucket_raw, "bucket_meta": bucket_meta, "bucket_full": bucket_full}
        config.update(kwargs)
        storage_config: StorageConfig = {
            "type": "s3",
            "protocol": "s3",
            "config": cast(StorageConfigDict, config),
        }
        self._config["storage_config"] = storage_config
        return self

    def r2_storage(
        self, bucket_raw: str = "test-raw", bucket_meta: str = "test-meta", bucket_full: str = "test-full", **kwargs
    ):
        """Configure R2 storage."""
        config = {"bucket_raw": bucket_raw, "bucket_meta": bucket_meta, "bucket_full": bucket_full}
        config.update(kwargs)
        storage_config: StorageConfig = {
            "type": "r2",
            "protocol": "s3",
            "config": cast(StorageConfigDict, config),
        }
        self._config["storage_config"] = storage_config
        return self

    def minio_storage(
        self, bucket_raw: str = "test-raw", bucket_meta: str = "test-meta", bucket_full: str = "test-full", **kwargs
    ):
        """Configure MinIO storage."""
        config = {"bucket_raw": bucket_raw, "bucket_meta": bucket_meta, "bucket_full": bucket_full}
        config.update(kwargs)
        storage_config: StorageConfig = {
            "type": "minio",
            "protocol": "s3",
            "config": cast(StorageConfigDict, config),
        }
        self._config["storage_config"] = storage_config
        return self

    def with_sync_config(self, **kwargs):
        """Set sync configuration options."""
        # Ensure Path objects are converted properly
        for key, value in kwargs.items():
            if key == "staging_dir" and isinstance(value, str):
                kwargs[key] = Path(value)
        self._config["sync_config"].update(kwargs)
        return self

    def with_concurrent_downloads(self, count: int):
        """Set concurrent downloads count."""
        self._config["sync_config"]["task_download_concurrency"] = count
        return self

    def with_concurrent_uploads(self, count: int):
        """Set concurrent uploads count."""
        self._config["sync_config"]["task_upload_concurrency"] = count
        return self

    def with_staging_dir(self, staging_dir: str):
        """Set staging directory."""
        self._config["sync_config"]["staging_dir"] = Path(staging_dir)
        return self

    def with_log_file(self, log_file: str):
        """Set log file path."""
        self._config["log_file"] = Path(log_file)
        return self

    def build(self) -> RunConfig:
        """Build the RunConfig instance."""
        return RunConfig(**self._config)


@pytest.fixture
def test_config_builder():
    """Fixture that provides a ConfigBuilder instance."""
    return ConfigBuilder()


@pytest.fixture
def mock_process_stage():
    """Mock process stage for testing."""
    return MagicMock()


# Enhanced fixtures for sync operation testing
@pytest.fixture
def mock_upload_deps():
    """Fixture providing mocked upload dependencies."""
    with mock_upload_operations() as mocks:
        yield mocks


@pytest.fixture
def mock_staging_manager():
    """Fixture providing a configured mock staging manager."""
    return create_staging_manager_mock()


@pytest.fixture
def mock_progress_tracker():
    """Fixture providing a configured mock progress tracker."""
    return create_progress_tracker_mock()


class DatabaseSchemaFactory:
    """Factory for creating standardized test database schemas."""

    @staticmethod
    def create_full_schema(db_path: str) -> None:
        """Create the complete database schema used by the application."""
        # Read the actual schema from docs/schema.sql
        schema_file = Path(__file__).parent.parent.parent / "docs" / "schema.sql"
        schema_sql = schema_file.read_text()

        conn = sqlite3.connect(db_path)
        conn.executescript(schema_sql)
        conn.commit()
        conn.close()


@pytest.fixture
def temp_db():
    """Create a temporary database with full schema."""
    with tempfile.NamedTemporaryFile(suffix=".db", delete=False) as f:
        db_path = f.name

    DatabaseSchemaFactory.create_full_schema(db_path)

    yield db_path

    Path(db_path).unlink(missing_ok=True)


@pytest.fixture
def mock_storage_config():
    """Fixture providing a standard mock storage configuration."""
    from tests.test_utils.unified_mocks import standard_storage_config

    return standard_storage_config()


@pytest.fixture
def mock_storage():
    """Standard storage mock used across multiple tests."""
    return create_storage_mock()


@pytest.fixture
def mock_book_manager():
    """Standard book manager mock used across multiple tests."""
    return create_book_manager_mock()


@pytest.fixture
def mock_grin_client():
    """Mock GRIN client for testing."""
    client = MagicMock()
    client.fetch_resource = AsyncMock()
    client.auth = MagicMock()

    # Create a mock response that properly handles async iteration
    mock_response = MagicMock()
    mock_response.content = MagicMock()

    # Create an async iterator for iter_chunked
    async def mock_iter_chunked(size):
        yield b"test archive content"

    mock_response.content.iter_chunked = mock_iter_chunked
    mock_response.status = 200
    mock_response.headers = {"content-length": "20", "ETag": '"test-etag"'}

    client.auth.make_authenticated_request = AsyncMock(return_value=mock_response)
    client.session = MagicMock()
    client.session.close = AsyncMock()
    # Mock new session management methods
    client.close = AsyncMock()
    client.download_archive = AsyncMock(return_value=mock_response)
    client.head_archive = AsyncMock(return_value=mock_response)

    return client


@pytest.fixture
async def db_tracker(temp_db):
    """Create and initialize a progress tracker for async database testing.

    This fixture replaces the AsyncDatabaseTestCase pattern, allowing
    pytest-asyncio tests to use the centralized database setup.
    """
    from grin_transfer.collect_books.models import SQLiteProgressTracker

    tracker = SQLiteProgressTracker(temp_db)
    await tracker.init_db()
    yield tracker
    await tracker.close()
