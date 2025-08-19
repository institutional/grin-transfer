"""Shared test configuration utilities and fixtures."""

import sqlite3
import tempfile
from pathlib import Path
from unittest.mock import AsyncMock, MagicMock

import pytest

from grin_to_s3.run_config import RunConfig
from tests.test_utils.unified_mocks import (
    create_book_manager_mock,
    create_progress_tracker_mock,
    create_staging_manager_mock,
    create_storage_mock,
    mock_upload_operations,
)


class ConfigBuilder:
    """Builder for creating test RunConfig instances with common configurations."""

    def __init__(self):
        self._config = {
            "run_name": "test_run",
            "sqlite_db_path": "/tmp/test.db",
            "library_directory": "test_library",
            "storage_config": {"type": "local", "config": {"base_path": "/tmp"}},
            "sync_config": {},
        }

    def with_run_name(self, run_name: str):
        """Set the run name."""
        self._config["run_name"] = run_name
        return self

    def with_db_path(self, db_path: str):
        """Set the database path."""
        self._config["sqlite_db_path"] = db_path
        return self

    def with_library_directory(self, library_directory: str):
        """Set the library directory."""
        self._config["library_directory"] = library_directory
        return self

    def local_storage(self, base_path: str = "/tmp"):
        """Configure local storage."""
        self._config["storage_config"] = {"type": "local", "config": {"base_path": base_path}}
        return self

    def s3_storage(
        self, bucket_raw: str = "test-raw", bucket_meta: str = "test-meta", bucket_full: str = "test-full", **kwargs
    ):
        """Configure S3 storage."""
        config = {"bucket_raw": bucket_raw, "bucket_meta": bucket_meta, "bucket_full": bucket_full}
        config.update(kwargs)
        self._config["storage_config"] = {"type": "s3", "config": config}
        return self

    def r2_storage(
        self, bucket_raw: str = "test-raw", bucket_meta: str = "test-meta", bucket_full: str = "test-full", **kwargs
    ):
        """Configure R2 storage."""
        config = {"bucket_raw": bucket_raw, "bucket_meta": bucket_meta, "bucket_full": bucket_full}
        config.update(kwargs)
        self._config["storage_config"] = {"type": "r2", "config": config}
        return self

    def minio_storage(
        self, bucket_raw: str = "test-raw", bucket_meta: str = "test-meta", bucket_full: str = "test-full", **kwargs
    ):
        """Configure MinIO storage."""
        config = {"bucket_raw": bucket_raw, "bucket_meta": bucket_meta, "bucket_full": bucket_full}
        config.update(kwargs)
        self._config["storage_config"] = {"type": "minio", "config": config}
        return self

    def with_sync_config(self, **kwargs):
        """Set sync configuration options."""
        self._config["sync_config"].update(kwargs)
        return self

    def with_concurrent_downloads(self, count: int):
        """Set concurrent downloads count."""
        self._config["sync_config"]["concurrent_downloads"] = count
        return self

    def with_concurrent_uploads(self, count: int):
        """Set concurrent uploads count."""
        self._config["sync_config"]["concurrent_uploads"] = count
        return self

    def with_batch_size(self, size: int):
        """Set batch size."""
        self._config["sync_config"]["batch_size"] = size
        return self

    def with_staging_dir(self, staging_dir: str):
        """Set staging directory."""
        self._config["sync_config"]["staging_dir"] = staging_dir
        return self

    def with_log_file(self, log_file: str):
        """Set log file path."""
        self._config["log_file"] = log_file
        return self

    def build(self) -> RunConfig:
        """Build the RunConfig instance."""
        return RunConfig(self._config)


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
    mock_response.headers = {"content-length": "20"}

    client.auth.make_authenticated_request = AsyncMock(return_value=mock_response)
    client.session = MagicMock()
    client.session.close = AsyncMock()
    return client
