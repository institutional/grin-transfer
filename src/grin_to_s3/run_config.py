#!/usr/bin/env python3
"""
Run Configuration Management

Utilities for reading and using configuration written by collect_books runs.
"""

import json
import sys
import types
from argparse import Namespace
from dataclasses import asdict, dataclass, fields
from pathlib import Path
from typing import Any, NotRequired, TypedDict, Union, cast, get_args, get_origin

from .constants import (
    DEFAULT_SYNC_DISK_SPACE_THRESHOLD,
    DEFAULT_SYNC_TASK_CHECK_CONCURRENCY,
    DEFAULT_SYNC_TASK_CLEANUP_CONCURRENCY,
    DEFAULT_SYNC_TASK_DECRYPT_CONCURRENCY,
    DEFAULT_SYNC_TASK_DOWNLOAD_CONCURRENCY,
    DEFAULT_SYNC_TASK_EXPORT_CSV_CONCURRENCY,
    DEFAULT_SYNC_TASK_EXTRACT_MARC_CONCURRENCY,
    DEFAULT_SYNC_TASK_EXTRACT_OCR_CONCURRENCY,
    DEFAULT_SYNC_TASK_UNPACK_CONCURRENCY,
    DEFAULT_SYNC_TASK_UPLOAD_CONCURRENCY,
    STORAGE_PROTOCOLS,
    STORAGE_TYPES,
)


def serialize_paths(obj: Any) -> Any:
    """Recursively convert Path objects to strings."""
    if isinstance(obj, Path):
        return str(obj)
    elif isinstance(obj, dict):
        return {k: serialize_paths(v) for k, v in obj.items()}
    elif isinstance(obj, list | tuple):
        return [serialize_paths(item) for item in obj]
    return obj


def deserialize_paths(obj: Any, type_hint: Any) -> Any:
    """Recursively convert strings back to Paths based on type hints."""
    # Handle Path type
    if type_hint == Path:
        return Path(obj) if obj is not None else None

    # Handle Optional[Path] (Path | None or Union[Path, None])
    origin = get_origin(type_hint)
    args = get_args(type_hint)

    if (origin is Union or origin is types.UnionType) and Path in args:
        if obj is not None and isinstance(obj, str):
            return Path(obj)
        return obj  # Return None or other non-string values as-is

    # Handle dataclasses (check this first since dataclasses also have __annotations__)
    if hasattr(type_hint, "__dataclass_fields__"):
        result = {}
        for field in fields(type_hint):
            if field.name in obj:
                result[field.name] = deserialize_paths(obj[field.name], field.type)
            else:
                # Include field even if not in obj (could be None or have default)
                result[field.name] = obj.get(field.name)
        return type_hint(**result)

    # Handle dicts with nested types (TypedDict)
    if isinstance(obj, dict) and hasattr(type_hint, "__annotations__"):
        result = {}
        for key, value in obj.items():
            if key in type_hint.__annotations__:
                result[key] = deserialize_paths(value, type_hint.__annotations__[key])
            else:
                result[key] = value
        return result

    return obj


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

    type: STORAGE_TYPES
    protocol: STORAGE_PROTOCOLS
    config: StorageConfigDict
    prefix: NotRequired[str]  # Optional prefix for all storage operations


class SyncConfig(TypedDict):
    task_check_concurrency: int
    task_download_concurrency: int
    task_decrypt_concurrency: int
    task_upload_concurrency: int
    task_unpack_concurrency: int
    task_extract_marc_concurrency: int
    task_extract_ocr_concurrency: int
    task_export_csv_concurrency: int
    task_cleanup_concurrency: int
    staging_dir: Path
    disk_space_threshold: float
    compression_meta_enabled: bool
    compression_full_enabled: bool


def build_sync_config_from_args(args: Namespace) -> SyncConfig:
    """Build sync configuration dictionary from CLI arguments."""
    return {
        "task_check_concurrency": args.sync_task_check_concurrency,
        "task_download_concurrency": args.sync_task_download_concurrency,
        "task_decrypt_concurrency": args.sync_task_decrypt_concurrency,
        "task_upload_concurrency": args.sync_task_upload_concurrency,
        "task_unpack_concurrency": args.sync_task_unpack_concurrency,
        "task_extract_marc_concurrency": args.sync_task_extract_marc_concurrency,
        "task_extract_ocr_concurrency": args.sync_task_extract_ocr_concurrency,
        "task_export_csv_concurrency": args.sync_task_export_csv_concurrency,
        "task_cleanup_concurrency": args.sync_task_cleanup_concurrency,
        "staging_dir": args.sync_staging_dir,
        "disk_space_threshold": args.sync_disk_space_threshold,
        "compression_full_enabled": args.compression_full_enabled if "compression_full_enabled" in args else True,
        "compression_meta_enabled": args.compression_meta_enabled if "compression_meta_enabled" in args else True,
    }


def to_storage_config_dict(source: dict[str, Any]) -> StorageConfigDict:
    """Convert any dict to StorageConfigDict, keeping only valid keys."""
    valid_keys = StorageConfigDict.__optional_keys__ | StorageConfigDict.__required_keys__
    filtered = {k: v for k, v in source.items() if k in valid_keys}
    return cast(StorageConfigDict, filtered)


@dataclass
class RunConfig:
    """Configuration for a specific collection run."""

    run_name: str
    library_directory: str
    output_directory: Path
    sqlite_db_path: Path
    storage_config: StorageConfig
    sync_config: SyncConfig
    log_file: Path
    secrets_dir: Path | None

    @property
    def storage_type(self) -> STORAGE_TYPES:
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

    @property
    def sync_staging_dir(self) -> Path:
        """Get the staging directory setting for sync operations."""
        return self.sync_config["staging_dir"]

    @property
    def sync_disk_space_threshold(self) -> float:
        """Get the disk space threshold setting for sync operations."""
        return self.sync_config.get("disk_space_threshold", DEFAULT_SYNC_DISK_SPACE_THRESHOLD)

    @property
    def sync_compression_meta_enabled(self) -> bool:
        """Get the compression enabled setting for meta bucket data."""
        return self.sync_config.get("compression_meta_enabled", True)

    @property
    def sync_compression_full_enabled(self) -> bool:
        """Get the compression enabled setting for full bucket data."""
        return self.sync_config.get("compression_full_enabled", True)

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


def load_run_config(config_path: str) -> RunConfig:
    """
    Load run configuration from a specific path.
    """
    with open(config_path) as f:
        config_dict = json.load(f)
    return deserialize_paths(config_dict, RunConfig)


def save_run_config(config: RunConfig) -> None:
    """
    Save run configuration to run_config.json in the output directory.
    """
    config_path = config.output_directory / "run_config.json"
    config_dict = serialize_paths(asdict(config))
    with open(config_path, "w") as f:
        json.dump(config_dict, f, indent=2)


def apply_run_config_to_args(args: Any, config: RunConfig) -> None:
    """
    Apply run configuration to parsed command line arguments.

    This function modifies the args object in-place, setting default values
    from the run configuration for arguments that weren't explicitly provided.
    """
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
