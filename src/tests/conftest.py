"""Shared test configuration utilities and fixtures."""

from unittest.mock import MagicMock

import pytest

from grin_to_s3.run_config import RunConfig


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

    def s3_storage(self, bucket_raw: str = "test-raw", **kwargs):
        """Configure S3 storage."""
        config = {"bucket_raw": bucket_raw}
        config.update(kwargs)
        self._config["storage_config"] = {"type": "s3", "config": config}
        return self

    def r2_storage(self, bucket_raw: str = "test-raw", bucket_meta: str = "test-meta", **kwargs):
        """Configure R2 storage."""
        config = {"bucket_raw": bucket_raw, "bucket_meta": bucket_meta}
        config.update(kwargs)
        self._config["storage_config"] = {"type": "r2", "config": config}
        return self

    def minio_storage(self, bucket_raw: str = "test-raw", **kwargs):
        """Configure MinIO storage."""
        config = {"bucket_raw": bucket_raw}
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

    def with_enrichment_workers(self, count: int):
        """Set enrichment workers count."""
        self._config["sync_config"]["enrichment_workers"] = count
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
