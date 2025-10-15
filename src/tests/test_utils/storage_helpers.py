"""Test utilities for creating storage configurations."""

from typing_extensions import TypedDict

from grin_transfer.storage import get_storage_protocol


class TestStorageConfig(TypedDict):
    """Test storage configuration for easy test setup."""

    type: str
    protocol: str
    config: dict
    prefix: str


def create_test_storage_config(storage_type: str, prefix: str = "", **config_kwargs) -> TestStorageConfig:
    """
    Create a complete storage config for testing.

    Args:
        storage_type: Storage type (local, s3, r2, minio, gcs)
        prefix: Optional prefix for storage operations
        **config_kwargs: Configuration options for the storage type

    Returns:
        Complete storage configuration dict
    """
    return {
        "type": storage_type,
        "protocol": get_storage_protocol(storage_type),
        "config": config_kwargs,
        "prefix": prefix,
    }


def create_local_test_config(base_path: str, prefix: str = "", **bucket_config) -> TestStorageConfig:
    """Create a local storage config for testing."""
    config = {"base_path": base_path}
    config.update(bucket_config)
    return create_test_storage_config("local", prefix, **config)


def create_s3_test_config(prefix: str = "", **bucket_config) -> TestStorageConfig:
    """Create an S3 storage config for testing."""
    return create_test_storage_config("s3", prefix, **bucket_config)


def create_minio_test_config(prefix: str = "", **bucket_config) -> TestStorageConfig:
    """Create a MinIO storage config for testing."""
    config = {"endpoint_url": "http://minio:9000", "access_key": "minioadmin", "secret_key": "minioadmin123"}
    config.update(bucket_config)
    return create_test_storage_config("minio", prefix, **config)
