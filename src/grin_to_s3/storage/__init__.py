"""
Storage Package

Unified storage abstraction providing clean APIs for all storage operations.
Eliminates circular import dependencies by consolidating storage functionality.
"""

# Core storage classes and interfaces
from .base import Storage, StorageConfig, StorageError, StorageNotFoundError

# Book-specific storage operations
from .book_manager import BookManager

# Storage creation and management
from .factories import (
    create_local_storage,
    create_minio_storage,
    create_r2_storage,
    create_s3_storage,
    create_storage_from_config,
    get_storage_protocol,
)

# Staging directory management
from .staging import StagingDirectoryManager

__all__ = [
    # Base classes
    "Storage",
    "StorageConfig",
    "StorageError",
    "StorageNotFoundError",
    # Factories
    "create_storage_from_config",
    "get_storage_protocol",
    "create_s3_storage",
    "create_r2_storage",
    "create_minio_storage",
    "create_local_storage",
    # Storage operations
    "BookManager",
    # Staging
    "StagingDirectoryManager",
]


def main():
    """Import and run storage CLI main function."""
    from .__main__ import main as _main

    return _main()
