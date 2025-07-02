"""
Storage Factory Functions

Centralized storage creation and configuration utilities.
Provides convenient factory functions for common storage configurations.
"""

import json
import logging
from pathlib import Path
from typing import Any

from .base import Storage, StorageConfig
from .book_storage import BookStorage, BucketConfig

logger = logging.getLogger(__name__)


def get_storage_protocol(storage_type: str) -> str:
    """
    Determine storage protocol from storage type.

    Args:
        storage_type: Original storage type (minio, r2, s3, local)

    Returns:
        str: Storage protocol ("s3" or "local")
    """
    return "s3" if storage_type in ("minio", "r2", "s3") else "local"


def load_json_credentials(credentials_path: str) -> dict[str, Any]:
    """Load and parse JSON credentials file."""
    try:
        with open(credentials_path) as f:
            return json.load(f)
    except json.JSONDecodeError as e:
        raise ValueError(f"Invalid JSON in credentials file {credentials_path}: {e}") from e


def validate_required_keys(data: dict, required_keys: list, context: str = "configuration") -> None:
    """
    Validate that required keys exist in configuration dictionary.

    Args:
        data: Dictionary to validate
        required_keys: List of required key names
        context: Description for error messages

    Raises:
        ValueError: If any required keys are missing
    """
    missing_keys = [key for key in required_keys if key not in data]
    if missing_keys:
        raise ValueError(f"Missing required {context} keys: {missing_keys}")


def create_storage_from_config(storage_type: str, config: dict) -> Storage:
    """
    Create storage instance based on type and configuration.

    Centralized storage factory to eliminate duplication between modules.

    Args:
        storage_type: Storage backend type (local, minio, r2, s3)
        config: Configuration dictionary for the storage type

    Returns:
        Storage: Configured storage instance

    Raises:
        ValueError: If storage type is unknown or configuration is invalid
    """
    match storage_type:
        case "local":
            base_path = config.get("base_path")
            if not base_path:
                raise ValueError(
                    "Local storage requires explicit base_path. Provide base_path in storage configuration."
                )
            return create_local_storage(base_path)

        case "minio":
            return create_minio_storage(
                endpoint_url=config.get("endpoint_url", "http://localhost:9000"),
                access_key=config.get("access_key", "minioadmin"),
                secret_key=config.get("secret_key", "minioadmin123"),
            )

        case "r2":
            # Check for credentials file (custom path or default)
            credentials_file = config.get("credentials_file")
            if not credentials_file:
                # Use default path in config directory
                home = Path.home()
                credentials_file = home / ".config" / "grin-to-s3" / "r2_credentials.json"

            try:
                creds = load_json_credentials(str(credentials_file))
                validate_required_keys(creds, ["account_id", "access_key", "secret_key"], "R2 credentials")
                return create_r2_storage(
                    account_id=creds["account_id"], access_key=creds["access_key"], secret_key=creds["secret_key"]
                )
            except FileNotFoundError as e:
                if config.get("credentials_file"):
                    # Custom path was specified but file doesn't exist
                    raise ValueError(f"R2 credentials file not found: {credentials_file}") from e
                else:
                    # Default path doesn't exist, provide helpful error
                    raise ValueError(
                        f"R2 credentials file not found at {credentials_file}. "
                        f"Create this file with your R2 credentials or specify a custom path with --credentials-file"
                    ) from e
            except (ValueError, KeyError) as e:
                raise ValueError(f"Invalid R2 credentials file {credentials_file}: {e}") from e

        case "s3":
            bucket = config.get("bucket") or config.get("bucket_raw")
            if not bucket:
                raise ValueError("S3 storage requires bucket name")

            # AWS credentials from environment or ~/.aws/credentials
            return create_s3_storage(bucket=bucket)

        case _:
            raise ValueError(f"Unknown storage type: {storage_type}")


def create_s3_storage(bucket: str | None = None, **kwargs: Any) -> Storage:
    """Create AWS S3 storage instance."""
    config = StorageConfig.s3(bucket=bucket or "", **kwargs)
    return Storage(config)


def create_r2_storage(account_id: str, access_key: str, secret_key: str, **kwargs: Any) -> Storage:
    """Create Cloudflare R2 storage instance."""
    config = StorageConfig.r2(account_id, access_key, secret_key, **kwargs)
    return Storage(config)


def create_minio_storage(endpoint_url: str, access_key: str, secret_key: str, **kwargs: Any) -> Storage:
    """Create MinIO storage instance."""
    config = StorageConfig.minio(endpoint_url, access_key, secret_key, **kwargs)
    return Storage(config)


def create_local_storage(base_path: str, **kwargs: Any) -> Storage:
    """Create local filesystem storage instance."""
    if not base_path:
        raise ValueError("Local storage requires explicit base_path")
    config = StorageConfig.local(base_path)
    return Storage(config)


def create_gcs_storage(project: str, **kwargs: Any) -> Storage:
    """Create Google Cloud Storage instance."""
    config = StorageConfig.gcs(project=project, **kwargs)
    return Storage(config)


def create_azure_storage(account_name: str, **kwargs: Any) -> Storage:
    """Create Azure Blob Storage instance."""
    config = StorageConfig(protocol="abfs", account_name=account_name, **kwargs)
    return Storage(config)


def create_storage_for_bucket(storage_type: str, config: dict, bucket_name: str) -> Storage:
    """
    Create storage instance for a specific bucket.

    Args:
        storage_type: Storage backend type (minio, r2, s3)
        config: Configuration dictionary for the storage type
        bucket_name: Name of the bucket to create storage for

    Returns:
        Storage: Configured storage instance for the specified bucket

    Raises:
        ValueError: If storage type doesn't support buckets or configuration is invalid
    """
    match storage_type:
        case "minio":
            return create_minio_storage(
                endpoint_url=config.get("endpoint_url", "http://localhost:9000"),
                access_key=config.get("access_key", "minioadmin"),
                secret_key=config.get("secret_key", "minioadmin123"),
            )

        case "r2":
            # Get R2 credentials from file
            credentials_file = config.get("credentials_file")
            if not credentials_file:
                home = Path.home()
                credentials_file = home / ".config" / "grin-to-s3" / "r2_credentials.json"

            try:
                creds = load_json_credentials(str(credentials_file))
                validate_required_keys(creds, ["account_id", "access_key", "secret_key"], "R2 credentials")
                return create_r2_storage(
                    account_id=creds["account_id"],
                    access_key=creds["access_key"],
                    secret_key=creds["secret_key"],
                )
            except FileNotFoundError as e:
                if config.get("credentials_file"):
                    raise ValueError(f"R2 credentials file not found: {credentials_file}") from e
                else:
                    raise ValueError(
                        f"R2 credentials file not found at {credentials_file}. "
                        f"Create this file with your R2 credentials or specify a custom path with --credentials-file"
                    ) from e
            except (ValueError, KeyError) as e:
                raise ValueError(f"Invalid R2 credentials file {credentials_file}: {e}") from e

        case "s3":
            return create_s3_storage(bucket=bucket_name)

        case _:
            raise ValueError(f"Storage type {storage_type} does not support bucket-based storage")



def create_book_storage_with_full_text(storage_type: str, config: dict, base_prefix: str = "") -> BookStorage:
    """
    Create BookStorage instance with full-text bucket support.

    Args:
        storage_type: Storage backend type (minio, r2, s3)
        config: Configuration dictionary containing bucket names and credentials
        base_prefix: Optional prefix for storage paths

    Returns:
        BookStorage: Configured BookStorage instance with full-text support

    Raises:
        ValueError: If required buckets are not configured
    """
    # Create single storage instance
    storage = create_storage_from_config(storage_type, config)

    # Extract bucket configuration
    bucket_config: BucketConfig = {
        "bucket_raw": config["bucket_raw"],
        "bucket_meta": config["bucket_meta"],
        "bucket_full": config["bucket_full"]
    }

    return BookStorage(storage=storage, bucket_config=bucket_config, base_prefix=base_prefix)
