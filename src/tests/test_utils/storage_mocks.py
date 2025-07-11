"""Storage mocking utilities for tests.

DEPRECATED: This module is being replaced by unified_mocks.py
Imports are provided for backward compatibility during migration.
"""

from .unified_mocks import MockStorageFactory


# Backward compatibility aliases
def create_mock_storage(s3_compatible: bool = False, should_fail: bool = False):
    """DEPRECATED: Use MockStorageFactory.create_storage() instead."""
    # Convert old API to new API
    storage_type = "s3" if s3_compatible else "local"
    return MockStorageFactory.create_storage(
        storage_type=storage_type,
        should_fail=should_fail
    )

def create_mock_book_storage(bucket_config=None, should_fail: bool = False, base_prefix: str = ""):
    """DEPRECATED: Use MockStorageFactory.create_book_storage() instead."""
    # Create storage first, then book_storage that wraps it
    storage = MockStorageFactory.create_storage(should_fail=should_fail)
    return MockStorageFactory.create_book_storage(
        storage=storage,
        bucket_config=bucket_config,
        base_prefix=base_prefix,
        should_fail=should_fail
    )

def create_mock_staging_manager(staging_dir: str = "/tmp/staging"):
    """DEPRECATED: Use MockStorageFactory.create_staging_manager() instead."""
    return MockStorageFactory.create_staging_manager(staging_path=staging_dir)

def standard_bucket_config():
    """DEPRECATED: Use bucket_config fixture or MockStorageFactory methods instead."""
    return {
        "bucket_raw": "test-raw",
        "bucket_meta": "test-meta",
        "bucket_full": "test-full"
    }
