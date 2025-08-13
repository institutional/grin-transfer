#!/usr/bin/env python3
"""
Shared test fixtures for sync module testing.
"""

import pytest

from grin_to_s3.sync.models import create_sync_stats
from grin_to_s3.sync.pipeline import SyncPipeline


@pytest.fixture
def sync_stats():
    """Create sync statistics for testing."""
    return create_sync_stats()


@pytest.fixture
def test_barcodes():
    """Sample barcodes for testing."""
    return ["TEST123", "TEST456", "TEST789"]


@pytest.fixture
def invalid_barcodes():
    """Invalid barcodes for testing validation."""
    return [
        "",  # Empty
        "ab",  # Too short
        "a" * 51,  # Too long
        "test@book",  # Invalid characters
        "test book",  # Space not allowed
    ]


@pytest.fixture
def mock_run_config(test_config_builder):
    """Create a mock RunConfig for sync testing."""
    return (
        test_config_builder.with_library_directory("TestLib")
        .minio_storage(bucket_raw="test-raw")
        .with_concurrent_downloads(2)
        .with_concurrent_uploads(1)
        .with_batch_size(10)
        .build()
    )


@pytest.fixture
async def sync_pipeline(mock_run_config, mock_process_stage):
    """
    Create a SyncPipeline for testing with sensible defaults.

    This fixture automatically:
    - Disables CSV export, database backup, and staging cleanup for faster tests
    - Provides proper cleanup after test completion

    Usage:
        async def test_something(sync_pipeline):
            # Use pipeline directly - cleanup is automatic
            result = await sync_pipeline.some_method()
    """
    pipeline = SyncPipeline.from_run_config(
        config=mock_run_config,
        process_summary_stage=mock_process_stage,
        skip_csv_export=True,  # Speed up tests
        skip_database_backup=True,  # Speed up tests
        skip_staging_cleanup=True,  # Prevent side effects
    )

    yield pipeline

    # Cleanup: Ensure any resources are properly closed
    try:
        await pipeline.cleanup()
    except Exception as e:
        # Don't fail tests due to cleanup issues, just warn
        print(f"Warning: Failed to cleanup pipeline: {e}")


