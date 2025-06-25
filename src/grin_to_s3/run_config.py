#!/usr/bin/env python3
"""
Run Configuration Management

Utilities for reading and using configuration written by collect_books runs.
"""

import json
import sys
from pathlib import Path
from typing import Any


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
    def storage_config(self) -> dict[str, Any] | None:
        """Get the storage configuration."""
        return self.config_dict.get("storage_config")

    @property
    def storage_type(self) -> str | None:
        """Get the storage type."""
        storage_config = self.storage_config
        return storage_config.get("type") if storage_config else None

    @property
    def storage_bucket_raw(self) -> str | None:
        """Get the raw data storage bucket from storage config."""
        storage_config = self.storage_config
        if storage_config and "config" in storage_config:
            return storage_config["config"].get("bucket_raw")
        return None

    @property
    def storage_bucket_meta(self) -> str | None:
        """Get the metadata storage bucket from storage config."""
        storage_config = self.storage_config
        if storage_config and "config" in storage_config:
            return storage_config["config"].get("bucket_meta")
        return None

    @property
    def storage_bucket_full(self) -> str | None:
        """Get the full-text storage bucket from storage config."""
        storage_config = self.storage_config
        if storage_config and "config" in storage_config:
            return storage_config["config"].get("bucket_full")
        return None

    @property
    def limit(self) -> int | None:
        """Get the limit parameter."""
        return self.config_dict.get("limit")

    def get_storage_args(self) -> dict[str, str]:
        """Get storage arguments suitable for command line scripts."""
        args: dict[str, str] = {}
        storage_config = self.storage_config

        if not storage_config:
            return args

        args["storage"] = storage_config.get("type", "")

        config = storage_config.get("config", {})
        for key, value in config.items():
            if key == "bucket_raw":
                args["bucket-raw"] = value
            elif key == "bucket_meta":
                args["bucket-meta"] = value
            elif key == "bucket_full":
                args["bucket-full"] = value
            elif key == "prefix":
                args["prefix"] = value
            elif key == "endpoint_url":
                args["endpoint-url"] = value
            elif key == "access_key":
                args["access-key"] = value
            elif key == "secret_key":
                args["secret-key"] = value
            elif key == "account_id":
                args["account-id"] = value
            elif key == "credentials_file":
                args["credentials-file"] = value

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
    if hasattr(args, 'grin_library_directory') and not getattr(args, 'grin_library_directory', None):
        args.grin_library_directory = config.library_directory

    # Apply secrets_dir if not set
    if hasattr(args, 'secrets_dir') and not getattr(args, 'secrets_dir', None):
        args.secrets_dir = config.secrets_dir

    # Apply limit if not set
    if hasattr(args, 'limit') and not getattr(args, 'limit', None):
        args.limit = config.limit

    # Apply storage configuration if not set
    storage_args = config.get_storage_args()
    for arg_name, value in storage_args.items():
        # Convert argument name to attribute name (replace dashes with underscores)
        attr_name = arg_name.replace('-', '_')
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

        if config.storage_config:
            storage = config.storage_config
            print(f"  Storage Type: {storage.get('type')}")
            storage_config = storage.get('config', {})
            if 'bucket_raw' in storage_config:
                print(f"  Raw Data Bucket: {storage_config['bucket_raw']}")
            if 'bucket_meta' in storage_config:
                print(f"  Metadata Bucket: {storage_config['bucket_meta']}")
            if 'bucket_full' in storage_config:
                print(f"  Full-text Bucket: {storage_config['bucket_full']}")
            if 'prefix' in storage_config:
                print(f"  Storage Prefix: {storage_config['prefix']}")
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
    db_path = f"output/{run_name}/books.db"
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
    # For R2 and S3, bucket names are optional as they can be specified in the config file
    if storage_type in ["r2", "s3"]:
        return []

    # For local storage, buckets are not needed
    if storage_type == "local":
        return []

    # For MinIO, bucket names are required (since it's typically used for local development)
    missing_buckets = []
    if not getattr(args, 'bucket_raw', None):
        missing_buckets.append("--bucket-raw")
    if not getattr(args, 'bucket_meta', None):
        missing_buckets.append("--bucket-meta")
    if not getattr(args, 'bucket_full', None):
        missing_buckets.append("--bucket-full")

    return missing_buckets


def build_storage_config_dict(args: Any) -> dict[str, str]:
    """
    Build storage configuration dictionary from arguments.

    Args:
        args: Arguments object containing bucket and storage config attributes

    Returns:
        Dictionary with storage configuration
    """
    storage_dict: dict[str, str] = {}

    # Add bucket names if provided
    if getattr(args, 'bucket_raw', None):
        storage_dict["bucket_raw"] = args.bucket_raw
    if getattr(args, 'bucket_meta', None):
        storage_dict["bucket_meta"] = args.bucket_meta
    if getattr(args, 'bucket_full', None):
        storage_dict["bucket_full"] = args.bucket_full

    # Add additional storage config from --storage-config arguments
    if getattr(args, 'storage_config', None):
        for item in args.storage_config:
            if "=" in item:
                key, value = item.split("=", 1)
                storage_dict[key] = value

    # For R2 storage, always load credentials from file
    storage_type = getattr(args, 'storage', None)
    if storage_type == 'r2':
        # Check if buckets are missing and try to load from credentials
        missing_buckets = []
        for bucket_attr in ['bucket_raw', 'bucket_meta', 'bucket_full']:
            if bucket_attr not in storage_dict:
                missing_buckets.append(bucket_attr)

        # Load R2 credentials if buckets are missing OR credentials are missing
        if missing_buckets or not all(key in storage_dict for key in ['access_key', 'secret_key', 'account_id']):
            try:
                from pathlib import Path

                from .common import load_json_credentials

                # Determine credentials file path
                credentials_file = getattr(args, 'credentials_file', None)
                if not credentials_file:
                    secrets_dir = getattr(args, 'secrets_dir', None)
                    if secrets_dir:
                        credentials_file = Path(secrets_dir) / "r2-credentials.json"
                    else:
                        home = Path.home()
                        credentials_file = home / ".config" / "grin-to-s3" / "r2_credentials.json"

                # Load credentials and extract bucket information
                creds = load_json_credentials(str(credentials_file))
                for bucket_attr in missing_buckets:
                    if bucket_attr in creds:
                        storage_dict[bucket_attr] = creds[bucket_attr]

                # Also include R2 credentials for storage creation
                for cred_attr in ['access_key', 'secret_key', 'account_id']:
                    if cred_attr in creds:
                        storage_dict[cred_attr] = creds[cred_attr]
            except Exception:
                # If we can't load credentials, that's ok - validation will catch missing buckets later
                pass

    # Add other optional arguments
    for attr in ['prefix', 'endpoint_url', 'access_key', 'secret_key', 'account_id', 'credentials_file']:
        value = getattr(args, attr, None)
        if value:
            storage_dict[attr] = value

    return storage_dict


if __name__ == "__main__":
    # CLI tool for testing
    import argparse

    parser = argparse.ArgumentParser(description="Run configuration utilities")
    parser.add_argument("db_path", help="Path to SQLite database")
    parser.add_argument("--show-info", action="store_true", help="Show run configuration info")

    args = parser.parse_args()

    if args.show_info:
        print_run_config_info(args.db_path)
