"""
Storage Factory Functions

Centralized storage creation and configuration utilities.
Provides convenient factory functions for common storage configurations.
"""

import json
import logging
from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from ..run_config import StorageConfig

from grin_to_s3.docker import process_local_storage_path

from ..auth.grin_auth import DEFAULT_CREDENTIALS_DIR, find_credential_file
from .base import BackendConfig, Storage

logger = logging.getLogger(__name__)


# Default directory names for local storage
LOCAL_STORAGE_DEFAULTS = {"bucket_raw": "raw", "bucket_meta": "meta", "bucket_full": "full"}


def get_storage_protocol(storage_type: str) -> str:
    """
    Determine storage protocol from storage type.

    Args:
        storage_type: Original storage type (minio, r2, s3, gcs, local)

    Returns:
        str: Storage protocol ("s3", "gcs", or "local")
    """
    if storage_type in ("minio", "r2", "s3"):
        return "s3"
    elif storage_type == "gcs":
        return "gcs"
    else:
        return "local"


def load_json_credentials(credentials_path: str) -> dict[str, Any]:
    """Load and parse JSON credentials file."""
    try:
        with open(credentials_path) as f:
            return json.load(f)
    except json.JSONDecodeError as e:
        raise ValueError(f"Invalid JSON in credentials file {credentials_path}: {e}") from e


def s3_credentials_available() -> bool:
    """Check if S3 credentials are available via boto3's credential resolution."""
    try:
        import boto3
    except ImportError:
        return False
    try:
        session = boto3.Session()
        credentials = session.get_credentials()
        return credentials is not None and credentials.access_key is not None
    except Exception:
        return False


def load_r2_credentials() -> tuple[str, str] | None:
    """Load R2 credentials from secrets directory.

    Returns:
        Tuple of (access_key, secret_key) if successful, None if failed
    """

    logger = logging.getLogger(__name__)
    credentials_file = find_credential_file("r2_credentials.json")

    if not credentials_file:
        logger.error(
            "Missing R2 credentials file. Please ensure credentials are properly configured in ~/.config/grin-to-s3/r2_credentials.json"
        )
        return None

    try:
        creds = load_json_credentials(str(credentials_file))
        access_key = creds.get("access_key")
        secret_key = creds.get("secret_key")

        if not access_key or not secret_key:
            logger.error(f"Invalid R2 credentials file. Missing access_key or secret_key in {credentials_file}")
            return None

        return access_key, secret_key
    except Exception as e:
        logger.error(f"Failed to load R2 credentials from {credentials_file}: {e}")
        return None


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


def create_storage_from_config(storage_config: "StorageConfig") -> Storage:
    """
    Create storage instance based on storage configuration.

    Centralized storage factory to eliminate duplication between modules.

    Args:
        storage_config: Complete storage configuration dict

    Returns:
        Storage: Configured storage instance

    Raises:
        ValueError: If storage type is unknown or configuration is invalid
    """
    storage_type = storage_config["type"]
    config = storage_config["config"]
    match storage_type:
        case "local":
            base_path = config.get("base_path")
            if not base_path:
                raise ValueError(
                    "Local storage requires explicit base_path. Provide base_path in storage configuration."
                )
            return create_local_storage(base_path)

        case "minio":
            # MinIO is only supported inside Docker container
            # Use internal Docker network address by default
            return create_minio_storage(
                endpoint_url=config.get("endpoint_url", "http://minio:9000"),
                access_key=config.get("access_key", "minioadmin"),
                secret_key=config.get("secret_key", "minioadmin123"),
            )

        case "r2":
            # Check for credentials file (custom path or default)
            credentials_file = config.get("credentials_file") or find_credential_file("r2_credentials.json")
            if not credentials_file:
                host_path = DEFAULT_CREDENTIALS_DIR / "r2_credentials.json"
                raise ValueError(
                    f"R2 credentials file not found at {host_path}. "
                    f"Copy examples/auth/r2-credentials-template.json to this location and edit with your R2 credentials."
                )

            try:
                creds = load_json_credentials(str(credentials_file))
                validate_required_keys(creds, ["endpoint_url", "access_key", "secret_key"], "R2 credentials")
                return create_r2_storage(
                    endpoint_url=creds["endpoint_url"], access_key=creds["access_key"], secret_key=creds["secret_key"]
                )
            except FileNotFoundError as e:
                error_msg = f"R2 credentials file not found: {credentials_file}"
                if not config.get("credentials_file"):
                    error_msg += ". Create this file with your R2 credentials."
                raise ValueError(error_msg) from e
            except (ValueError, KeyError) as e:
                raise ValueError(f"Invalid R2 credentials file {credentials_file}: {e}") from e

        case "s3":
            bucket = config.get("bucket") or config.get("bucket_raw")
            if not bucket or not isinstance(bucket, str):
                raise ValueError("S3 storage requires bucket name")

            # AWS credentials from environment or ~/.aws/credentials
            return create_s3_storage(bucket=bucket)

        case "gcs":
            project = config.get("project")
            if not project or not isinstance(project, str):
                raise ValueError("GCS storage requires project ID")

            # Use Application Default Credentials (ADC) - set up via: gcloud auth application-default login
            return create_gcs_storage(project=project)

        case _:
            raise ValueError(f"Unknown storage type: {storage_type}")


def create_s3_storage(bucket: str | None = None, **kwargs: Any) -> Storage:
    """Create AWS S3 storage instance."""
    config = BackendConfig.s3(bucket=bucket or "", **kwargs)
    return Storage(config)


def create_r2_storage(endpoint_url: str, access_key: str, secret_key: str, **kwargs: Any) -> Storage:
    """Create Cloudflare R2 storage instance."""
    config = BackendConfig.r2(endpoint_url, access_key, secret_key, **kwargs)
    return Storage(config)


def create_minio_storage(endpoint_url: str, access_key: str, secret_key: str, **kwargs: Any) -> Storage:
    """Create MinIO storage instance."""
    config = BackendConfig.minio(endpoint_url, access_key, secret_key, **kwargs)
    return Storage(config)


def create_local_storage(base_path: str, **kwargs: Any) -> Storage:
    """Create local filesystem storage instance."""
    if not base_path:
        raise ValueError("Local storage requires explicit base_path")
    config = BackendConfig.local(base_path)
    return Storage(config)


def create_gcs_storage(project: str, **kwargs: Any) -> Storage:
    """Create Google Cloud Storage instance."""
    config = BackendConfig.gcs(project=project, **kwargs)
    return Storage(config)


def create_azure_storage(account_name: str, **kwargs: Any) -> Storage:
    """Create Azure Blob Storage instance."""
    config = BackendConfig(protocol="abfs", account_name=account_name, **kwargs)
    return Storage(config)


async def create_local_storage_directories(storage_config: dict) -> None:
    """Create all required directories for local storage.

    Args:
        storage_config: Local storage configuration dictionary
    """
    # Create directories for local storage
    base_path_str = storage_config.get("base_path")
    if not base_path_str:
        raise ValueError("Local storage requires base_path in configuration")

    # Process path with Docker translation and validation
    base_path = process_local_storage_path(base_path_str)

    # Update config with resolved path for consistency
    storage_config["base_path"] = str(base_path)

    # Create the main directories using default names
    (base_path / LOCAL_STORAGE_DEFAULTS["bucket_raw"]).mkdir(parents=True, exist_ok=True)
    (base_path / LOCAL_STORAGE_DEFAULTS["bucket_meta"]).mkdir(parents=True, exist_ok=True)
    (base_path / LOCAL_STORAGE_DEFAULTS["bucket_full"]).mkdir(parents=True, exist_ok=True)
    print(f"Created local storage directories at {base_path}")
