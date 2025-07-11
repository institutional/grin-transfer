"""Centralized mock utilities for sync operation tests.

DEPRECATED: This module is being replaced by unified_mocks.py
Imports are provided for backward compatibility during migration.
"""

from .unified_mocks import (
    MockStorageFactory,
    PipelineMocks,
    SyncOperationMocks,
    mock_minimal_upload,
    mock_pipeline_operations,
    mock_upload_operations,
)


# Backward compatibility aliases
def create_mock_storage(config, should_fail: bool = False):
    """DEPRECATED: Use MockStorageFactory.create_storage() instead."""
    return MockStorageFactory.create_storage(
        storage_type="local",
        should_fail=should_fail,
        custom_config=config
    )

def create_mock_staging_manager(base_path: str = "/tmp/staging"):
    """DEPRECATED: Use MockStorageFactory.create_staging_manager() instead."""
    return MockStorageFactory.create_staging_manager(staging_path=base_path)

def create_mock_progress_tracker():
    """DEPRECATED: Use MockStorageFactory.create_progress_tracker() instead."""
    return MockStorageFactory.create_progress_tracker()

# Export context managers directly (no changes needed)
__all__ = [
    "mock_upload_operations",
    "mock_pipeline_operations",
    "mock_minimal_upload",
    "create_mock_storage",
    "create_mock_staging_manager",
    "create_mock_progress_tracker",
    "SyncOperationMocks",
    "PipelineMocks"
]
