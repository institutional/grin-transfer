#!/usr/bin/env python3
"""
Run Configuration Management

Utilities for reading and using configuration written by collect_books runs.
"""

import json
import sys
from pathlib import Path
from typing import Any, Dict, Optional


class RunConfig:
    """Configuration for a specific collection run."""
    
    def __init__(self, config_dict: Dict[str, Any]):
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
    def directory(self) -> str:
        """Get the GRIN directory."""
        return self.config_dict.get("directory", "Harvard")
    
    @property
    def secrets_dir(self) -> Optional[str]:
        """Get the secrets directory."""
        return self.config_dict.get("secrets_dir")
    
    @property
    def storage_config(self) -> Optional[Dict[str, Any]]:
        """Get the storage configuration."""
        return self.config_dict.get("storage_config")
    
    @property
    def storage_type(self) -> Optional[str]:
        """Get the storage type."""
        storage_config = self.storage_config
        return storage_config.get("type") if storage_config else None
    
    @property
    def storage_bucket(self) -> Optional[str]:
        """Get the storage bucket from storage config."""
        storage_config = self.storage_config
        if storage_config and "config" in storage_config:
            return storage_config["config"].get("bucket")
        return None
    
    def get_storage_args(self) -> Dict[str, str]:
        """Get storage arguments suitable for command line scripts."""
        args = {}
        storage_config = self.storage_config
        
        if not storage_config:
            return args
            
        args["storage"] = storage_config.get("type", "")
        
        config = storage_config.get("config", {})
        for key, value in config.items():
            if key == "bucket":
                args["bucket"] = value
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


def find_run_config(db_path: str) -> Optional[RunConfig]:
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
    
    # Apply directory if not set
    if hasattr(args, 'directory') and not getattr(args, 'directory', None):
        args.directory = config.directory
    
    # Apply secrets_dir if not set
    if hasattr(args, 'secrets_dir') and not getattr(args, 'secrets_dir', None):
        args.secrets_dir = config.secrets_dir
    
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
        print(f"Run Configuration Found:")
        print(f"  Run Name: {config.run_name}")
        print(f"  Output Directory: {config.output_directory}")
        print(f"  Database: {config.sqlite_db_path}")
        print(f"  GRIN Directory: {config.directory}")
        
        if config.secrets_dir:
            print(f"  Secrets Directory: {config.secrets_dir}")
        
        if config.storage_config:
            storage = config.storage_config
            print(f"  Storage Type: {storage.get('type')}")
            storage_config = storage.get('config', {})
            if 'bucket' in storage_config:
                print(f"  Storage Bucket: {storage_config['bucket']}")
            if 'prefix' in storage_config:
                print(f"  Storage Prefix: {storage_config['prefix']}")
    else:
        run_name, output_dir = get_run_info_from_db_path(db_path)
        print(f"No run configuration found.")
        print(f"  Inferred Run Name: {run_name}")
        print(f"  Inferred Output Directory: {output_dir}")


if __name__ == "__main__":
    # CLI tool for testing
    import argparse
    
    parser = argparse.ArgumentParser(description="Run configuration utilities")
    parser.add_argument("db_path", help="Path to SQLite database")
    parser.add_argument("--show-info", action="store_true", help="Show run configuration info")
    
    args = parser.parse_args()
    
    if args.show_info:
        print_run_config_info(args.db_path)