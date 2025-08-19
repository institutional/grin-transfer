#!/usr/bin/env python3
"""
Run Configuration Management

Utilities for reading and using configuration written by collect_books runs.
"""

import json
import sys
from pathlib import Path
from typing import Any, NotRequired, TypedDict, cast

# Sync configuration defaults
DEFAULT_SYNC_BATCH_SIZE = 100
DEFAULT_SYNC_DISK_SPACE_THRESHOLD = 0.9

# Task concurrency defaults
DEFAULT_SYNC_TASK_CHECK_CONCURRENCY = 5
DEFAULT_SYNC_TASK_DOWNLOAD_CONCURRENCY = 2
DEFAULT_SYNC_TASK_DECRYPT_CONCURRENCY = 2
DEFAULT_SYNC_TASK_UPLOAD_CONCURRENCY = 3
DEFAULT_SYNC_TASK_UNPACK_CONCURRENCY = 2
DEFAULT_SYNC_TASK_EXTRACT_MARC_CONCURRENCY = 2
DEFAULT_SYNC_TASK_EXTRACT_OCR_CONCURRENCY = 2
DEFAULT_SYNC_TASK_EXPORT_CSV_CONCURRENCY = 2
DEFAULT_SYNC_TASK_CLEANUP_CONCURRENCY = 1


class StorageConfigDict(TypedDict, total=False):
    """Inner config dict for storage configuration."""

    bucket_raw: str
    bucket_meta: str
    bucket_full: str
    base_path: str  # For local storage
    endpoint_url: str  # For MinIO/R2
    access_key: str  # Only in memory, not saved
    secret_key: str  # Only in memory, not saved
    credentials_file: str


class StorageConfig(TypedDict):
    """Complete storage configuration."""

    type: str
    protocol: str
    config: StorageConfigDict
    prefix: NotRequired[str]  # Optional prefix for all storage operations


def to_storage_config_dict(source: dict[str, Any]) -> StorageConfigDict:
    """Convert any dict to StorageConfigDict, keeping only valid keys."""
    valid_keys = StorageConfigDict.__optional_keys__ | StorageConfigDict.__required_keys__
    filtered = {k: v for k, v in source.items() if k in valid_keys}
    return cast(StorageConfigDict, filtered)


def to_run_storage_config(
    storage_type: str, protocol: str, config: dict[str, Any] | StorageConfigDict, prefix: str = ""
) -> StorageConfig:
    """Create a properly typed RunStorageConfig."""
    result: StorageConfig = {
        "type": storage_type,
        "protocol": protocol,
        "config": to_storage_config_dict(cast(dict[str, Any], config)),
    }
    if prefix:
        result["prefix"] = prefix
    return result


class RunConfig:
    """Configuration for a specific collection run."""

    def __init__(self, config_dict: dict[str, Any]):
        self.config_dict = config_dict

    @property
    def run_name(self) -> str:
        """Get the run name."""
        return self.config_dict.get("run_name", "default")

    @property
    def run_identifier(self) -> str:
        """Get the run identifier."""
        return self.config_dict.get("run_identifier", "default")

    @property
    def output_directory(self) -> str:
        """Get the output directory path."""
        return self.config_dict.get("output_directory", f"output/{self.run_name}")

    @property
    def sqlite_db_path(self) -> str:
        """Get the SQLite database path."""
        return self.config_dict.get("sqlite_db_path", f"{self.output_directory}/books.db")

    @property
    def progress_file(self) -> str:
        """Get the progress file path."""
        return self.config_dict.get("progress_file", f"{self.output_directory}/progress.json")

    @property
    def library_directory(self) -> str:
        """Get the GRIN library directory."""
        return self.config_dict.get("library_directory", "")

    @property
    def secrets_dir(self) -> str | None:
        """Get the secrets directory."""
        return self.config_dict.get("secrets_dir")

    @property
    def storage_config(self) -> StorageConfig:
        """Get the storage configuration."""
        stored_config = self.config_dict.get("storage_config")
        if not stored_config:
            raise ValueError("Storage configuration is required but not found in run config")

        # Fill in protocol if missing (only field we auto-derive)
        if "protocol" not in stored_config:
            from .storage.factories import get_storage_protocol

            stored_config["protocol"] = get_storage_protocol(stored_config["type"])
        if "config" not in stored_config:
            stored_config["config"] = {}

        return stored_config  # Type checker will enforce RunStorageConfig structure

    @property
    def storage_type(self) -> str:
        """Get the storage type."""
        return self.storage_config["type"]

    @property
    def storage_bucket_raw(self) -> str | None:
        """Get the raw data storage bucket from storage config."""
        return self.storage_config["config"].get("bucket_raw")

    @property
    def storage_bucket_meta(self) -> str | None:
        """Get the metadata storage bucket from storage config."""
        return self.storage_config["config"].get("bucket_meta")

    @property
    def storage_bucket_full(self) -> str | None:
        """Get the full-text storage bucket from storage config."""
        return self.storage_config["config"].get("bucket_full")

    @property
    def log_file(self) -> str:
        """Get the unified log file path."""
        return self.config_dict["log_file"]

    @property
    def sync_config(self) -> dict[str, Any]:
        """Get the sync configuration section."""
        return self.config_dict.get("sync_config", {})

    @property
    def sync_task_check_concurrency(self) -> int:
        """Get the check task concurrency setting."""
        return self.sync_config.get("task_check_concurrency", DEFAULT_SYNC_TASK_CHECK_CONCURRENCY)

    @property
    def sync_task_download_concurrency(self) -> int:
        """Get the download task concurrency setting."""
        return self.sync_config.get("task_download_concurrency", DEFAULT_SYNC_TASK_DOWNLOAD_CONCURRENCY)

    @property
    def sync_task_decrypt_concurrency(self) -> int:
        """Get the decrypt task concurrency setting."""
        return self.sync_config.get("task_decrypt_concurrency", DEFAULT_SYNC_TASK_DECRYPT_CONCURRENCY)

    @property
    def sync_task_upload_concurrency(self) -> int:
        """Get the upload task concurrency setting."""
        return self.sync_config.get("task_upload_concurrency", DEFAULT_SYNC_TASK_UPLOAD_CONCURRENCY)

    @property
    def sync_task_unpack_concurrency(self) -> int:
        """Get the unpack task concurrency setting."""
        return self.sync_config.get("task_unpack_concurrency", DEFAULT_SYNC_TASK_UNPACK_CONCURRENCY)

    @property
    def sync_task_extract_marc_concurrency(self) -> int:
        """Get the MARC extraction task concurrency setting."""
        return self.sync_config.get("task_extract_marc_concurrency", DEFAULT_SYNC_TASK_EXTRACT_MARC_CONCURRENCY)

    @property
    def sync_task_extract_ocr_concurrency(self) -> int:
        """Get the OCR extraction task concurrency setting."""
        return self.sync_config.get("task_extract_ocr_concurrency", DEFAULT_SYNC_TASK_EXTRACT_OCR_CONCURRENCY)

    @property
    def sync_task_export_csv_concurrency(self) -> int:
        """Get the CSV export task concurrency setting."""
        return self.sync_config.get("task_export_csv_concurrency", DEFAULT_SYNC_TASK_EXPORT_CSV_CONCURRENCY)

    @property
    def sync_task_cleanup_concurrency(self) -> int:
        """Get the cleanup task concurrency setting."""
        return self.sync_config.get("task_cleanup_concurrency", DEFAULT_SYNC_TASK_CLEANUP_CONCURRENCY)

    def get_task_concurrency_limits(self) -> dict[str, int]:
        """Get all task concurrency limits as a dictionary."""
        return {
            "task_check_concurrency": self.sync_task_check_concurrency,
            "task_download_concurrency": self.sync_task_download_concurrency,
            "task_decrypt_concurrency": self.sync_task_decrypt_concurrency,
            "task_upload_concurrency": self.sync_task_upload_concurrency,
            "task_unpack_concurrency": self.sync_task_unpack_concurrency,
            "task_extract_marc_concurrency": self.sync_task_extract_marc_concurrency,
            "task_extract_ocr_concurrency": self.sync_task_extract_ocr_concurrency,
            "task_export_csv_concurrency": self.sync_task_export_csv_concurrency,
            "task_cleanup_concurrency": self.sync_task_cleanup_concurrency,
        }

    @property
    def sync_batch_size(self) -> int:
        """Get the batch size setting for sync operations."""
        return self.sync_config.get("batch_size", DEFAULT_SYNC_BATCH_SIZE)

    @property
    def sync_staging_dir(self) -> str | None:
        """Get the staging directory setting for sync operations."""
        return self.sync_config.get("staging_dir")

    @property
    def sync_disk_space_threshold(self) -> float:
        """Get the disk space threshold setting for sync operations."""
        return self.sync_config.get("disk_space_threshold", DEFAULT_SYNC_DISK_SPACE_THRESHOLD)

    def get_storage_args(self) -> dict[str, str]:
        """Get storage arguments suitable for command line scripts."""
        args: dict[str, str] = {}
        storage_config = self.storage_config

        args["storage"] = storage_config["type"]

        config = storage_config["config"]

        # Access fields directly to leverage TypedDict typing
        if "bucket_raw" in config:
            args["bucket-raw"] = config["bucket_raw"]
        if "bucket_meta" in config:
            args["bucket-meta"] = config["bucket_meta"]
        if "bucket_full" in config:
            args["bucket-full"] = config["bucket_full"]
        if "base_path" in config:
            args["base-path"] = config["base_path"]
        if "endpoint_url" in config:
            args["endpoint-url"] = config["endpoint_url"]
        if "credentials_file" in config:
            args["credentials-file"] = config["credentials_file"]

        # Handle prefix from the top-level storage config
        if "prefix" in storage_config:
            args["prefix"] = storage_config["prefix"]

        # Skip access_key and secret_key - these should only be read from secrets directories

        return args


def find_run_config(db_path: str) -> RunConfig | None:
    """
    Find run configuration by looking for run_config.json near the database.

    Args:
        db_path: Path to the SQLite database

    Returns:
        RunConfig if found, None otherwise
    """
    db_file = Path(db_path)

    # Look for run_config.json in the same directory as the database
    config_path = db_file.parent / "run_config.json"

    if config_path.exists():
        try:
            with open(config_path) as f:
                config_dict = json.load(f)
            return RunConfig(config_dict)
        except (json.JSONDecodeError, OSError) as e:
            print(f"Warning: Failed to read run config from {config_path}: {e}", file=sys.stderr)

    return None


def load_run_config(config_path: str) -> RunConfig:
    """
    Load run configuration from a specific path.

    Args:
        config_path: Path to the run_config.json file

    Returns:
        RunConfig instance

    Raises:
        FileNotFoundError: If config file doesn't exist
        json.JSONDecodeError: If config file is invalid JSON
    """
    with open(config_path) as f:
        config_dict = json.load(f)
    return RunConfig(config_dict)


def get_run_info_from_db_path(db_path: str) -> tuple[str, str]:
    """
    Extract run name and output directory from database path.

    Args:
        db_path: Path to the SQLite database

    Returns:
        Tuple of (run_name, output_directory)
    """
    db_file = Path(db_path)

    # Extract run name from path like "output/harvard_2024/books.db"
    if db_file.parent.name == "output":
        # Direct path like "output/books.db" - use "default"
        run_name = "default"
        output_directory = str(db_file.parent)
    else:
        # Path like "output/harvard_2024/books.db" - use parent dir name
        run_name = db_file.parent.name
        output_directory = str(db_file.parent)

    return run_name, output_directory


def apply_run_config_to_args(args: Any, db_path: str) -> None:
    """
    Apply run configuration to parsed command line arguments.

    This function modifies the args object in-place, setting default values
    from the run configuration for arguments that weren't explicitly provided.

    Args:
        args: Parsed argparse.Namespace object
        db_path: Path to the SQLite database
    """
    config = find_run_config(db_path)

    if not config:
        return

    # Apply library directory if not set
    if not getattr(args, "grin_library_directory", None):
        args.grin_library_directory = config.library_directory

    # Apply secrets_dir if not set
    if hasattr(args, "secrets_dir") and not getattr(args, "secrets_dir", None):
        args.secrets_dir = config.secrets_dir

    # Apply storage configuration if not set
    storage_args = config.get_storage_args()
    for arg_name, value in storage_args.items():
        # Convert argument name to attribute name (replace dashes with underscores)
        attr_name = arg_name.replace("-", "_")
        if hasattr(args, attr_name) and not getattr(args, attr_name, None):
            setattr(args, attr_name, value)


def print_run_config_info(db_path: str) -> None:
    """
    Print information about the run configuration for a database.

    Args:
        db_path: Path to the SQLite database
    """
    config = find_run_config(db_path)

    if config:
        print("Run Configuration Found:")
        print(f"  Run Name: {config.run_name}")
        print(f"  Output Directory: {config.output_directory}")
        print(f"  Database: {config.sqlite_db_path}")
        print(f"  GRIN Directory: {config.library_directory}")

        if config.secrets_dir:
            print(f"  Secrets Directory: {config.secrets_dir}")

        storage = config.storage_config
        print(f"  Storage Type: {storage['type']}")
        storage_config = storage["config"]
        if "bucket_raw" in storage_config:
            print(f"  Raw Data Bucket: {storage_config['bucket_raw']}")
        if "bucket_meta" in storage_config:
            print(f"  Metadata Bucket: {storage_config['bucket_meta']}")
        if "bucket_full" in storage_config:
            print(f"  Full-text Bucket: {storage_config['bucket_full']}")
        if "base_path" in storage_config:
            print(f"  Base Path: {storage_config['base_path']}")
        if storage.get("prefix"):
            print(f"  Storage Prefix: {storage.get('prefix')}")

        if config.sync_config:
            print("  Sync Configuration:")
            print("    Task Concurrency Limits:")
            print(f"      Check: {config.sync_task_check_concurrency}")
            print(f"      Download: {config.sync_task_download_concurrency}")
            print(f"      Decrypt: {config.sync_task_decrypt_concurrency}")
            print(f"      Upload: {config.sync_task_upload_concurrency}")
            print(f"      Unpack: {config.sync_task_unpack_concurrency}")
            print(f"      Extract MARC: {config.sync_task_extract_marc_concurrency}")
            print(f"      Extract OCR: {config.sync_task_extract_ocr_concurrency}")
            print(f"      Export CSV: {config.sync_task_export_csv_concurrency}")
            print(f"      Cleanup: {config.sync_task_cleanup_concurrency}")
            print(f"    Batch Size: {config.sync_batch_size}")
            print(f"    Disk Space Threshold: {config.sync_disk_space_threshold}")
            if config.sync_staging_dir:
                print(f"    Staging Directory: {config.sync_staging_dir}")
    else:
        run_name, output_dir = get_run_info_from_db_path(db_path)
        print("No run configuration found.")
        print(f"  Inferred Run Name: {run_name}")
        print(f"  Inferred Output Directory: {output_dir}")


def setup_run_database_path(args: Any, run_name: str) -> str:
    """
    Set up database path from run name and apply run configuration.

    Args:
        args: Arguments object to modify
        run_name: Name of the run

    Returns:
        Database path that was set
    """
    # Resolve path relative to grin.py location (project root)
    # Go up from src/grin_to_s3/run_config.py to project root
    project_root = Path(__file__).parent.parent.parent
    db_path = str(project_root / "output" / run_name / "books.db")
    args.db_path = db_path
    apply_run_config_to_args(args, db_path)
    return db_path


def validate_bucket_arguments(args: Any, storage_type: str | None = None) -> list[str]:
    """
    Validate that required bucket arguments are present.

    Args:
        args: Arguments object containing bucket attributes
        storage_type: Optional storage type for error messages

    Returns:
        List of missing bucket argument names (empty if all present)
    """
    # For cloud storage, bucket names are optional as they can be specified in the config file
    if storage_type in ["r2", "s3", "gcs"]:
        return []

    # For local storage, buckets are not needed
    if storage_type == "local":
        return []

    # For MinIO, bucket names are required (since it's typically used for local development)
    missing_buckets = []
    if not getattr(args, "bucket_raw", None):
        missing_buckets.append("--bucket-raw")
    if not getattr(args, "bucket_meta", None):
        missing_buckets.append("--bucket-meta")
    if not getattr(args, "bucket_full", None):
        missing_buckets.append("--bucket-full")

    return missing_buckets


def build_storage_config_dict(args: Any) -> StorageConfigDict:
    """
    Build storage configuration dictionary from arguments.

    Args:
        args: Arguments object containing bucket and storage config attributes

    Returns:
        StorageConfigDict with storage configuration (no prefix included)
    """
    from .storage.factories import find_credential_file, load_json_credentials

    storage_dict: dict[str, str] = {}

    # Add bucket names if provided
    if getattr(args, "bucket_raw", None):
        storage_dict["bucket_raw"] = args.bucket_raw
    if getattr(args, "bucket_meta", None):
        storage_dict["bucket_meta"] = args.bucket_meta
    if getattr(args, "bucket_full", None):
        storage_dict["bucket_full"] = args.bucket_full

    # Add additional storage config from --storage-config arguments
    if getattr(args, "storage_config", None):
        for item in args.storage_config:
            if "=" in item:
                key, value = item.split("=", 1)
                storage_dict[key] = value

    # For R2 storage, always load credentials from file
    storage_type = getattr(args, "storage", None)
    if storage_type == "r2":
        # Check if buckets are missing and try to load from credentials
        missing_buckets = []
        for bucket_attr in ["bucket_raw", "bucket_meta", "bucket_full"]:
            if bucket_attr not in storage_dict:
                missing_buckets.append(bucket_attr)

        # Load R2 credentials if buckets are missing OR credentials are missing
        if missing_buckets or not all(key in storage_dict for key in ["access_key", "secret_key", "endpoint_url"]):
            try:
                # Determine credentials file path using the same logic as storage factories
                credentials_file = getattr(args, "credentials_file", None)
                if not credentials_file:
                    credentials_file = find_credential_file("r2_credentials.json")

                if not credentials_file:
                    # If still no file found, skip credential loading but warn
                    print(
                        "WARNING: R2 credentials file not found, file syncing will not work properly.", file=sys.stderr
                    )
                else:
                    # Load credentials and extract bucket information
                    creds = load_json_credentials(str(credentials_file))
                    for bucket_attr in missing_buckets:
                        if bucket_attr in creds:
                            storage_dict[bucket_attr] = creds[bucket_attr]

                    # Include endpoint_url but NOT credentials - those should only be read from secrets dirs
                    if "endpoint_url" in creds:
                        storage_dict["endpoint_url"] = creds["endpoint_url"]
            except Exception:
                # If we can't load credentials, that's ok - validation will catch missing buckets later
                pass

    # Add other optional arguments (excluding secrets and prefix)
    for attr in ["endpoint_url", "credentials_file"]:
        value = getattr(args, attr, None)
        if value:
            storage_dict[attr] = value

    return to_storage_config_dict(storage_dict)


if __name__ == "__main__":
    # CLI tool for testing
    import argparse

    parser = argparse.ArgumentParser(description="Run configuration utilities")
    parser.add_argument("db_path", help="Path to SQLite database")
    parser.add_argument("--show-info", action="store_true", help="Show run configuration info")

    args = parser.parse_args()

    if args.show_info:
        print_run_config_info(args.db_path)
