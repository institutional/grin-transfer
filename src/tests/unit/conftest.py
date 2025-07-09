#!/usr/bin/env python3
"""
Shared test fixtures for sync module testing.
"""

import sqlite3
import tempfile
from pathlib import Path
from typing import Any
from unittest.mock import AsyncMock, MagicMock

import pytest

from grin_to_s3.process_summary import ProcessStageMetrics
from grin_to_s3.sync.models import create_sync_stats


@pytest.fixture
def temp_db_path():
    """Create a temporary database file for testing."""
    with tempfile.NamedTemporaryFile(suffix=".db", delete=False) as f:
        db_path = f.name

    # Initialize with basic schema
    conn = sqlite3.connect(db_path)
    conn.execute("""
        CREATE TABLE IF NOT EXISTS books (
            barcode TEXT PRIMARY KEY,
            status TEXT,
            last_modified TEXT,
            sync_data TEXT,
            storage_type TEXT,
            storage_path TEXT,
            sync_timestamp TEXT,
            sync_error TEXT
        )
    """)
    conn.execute("""
        CREATE TABLE IF NOT EXISTS book_status_history (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            barcode TEXT,
            status_type TEXT,
            status_value TEXT,
            timestamp TEXT
        )
    """)
    conn.commit()
    conn.close()

    yield db_path

    # Cleanup
    Path(db_path).unlink(missing_ok=True)


@pytest.fixture
def mock_storage_config() -> dict[str, Any]:
    """Mock storage configuration for testing."""
    return {
        "bucket_raw": "test-raw",
        "bucket_meta": "test-meta",
        "bucket_full": "test-full",
        "access_key": "test-access",
        "secret_key": "test-secret",
        "endpoint_url": "http://localhost:9000",
    }


@pytest.fixture
def mock_grin_client():
    """Mock GRIN client for testing."""
    client = MagicMock()
    client.fetch_resource = AsyncMock()
    client.auth = MagicMock()
    client.auth.make_authenticated_request = AsyncMock()
    client.session = MagicMock()
    client.session.close = AsyncMock()
    return client


@pytest.fixture
def mock_progress_tracker():
    """Mock progress tracker for testing."""
    tracker = MagicMock()
    tracker.add_status_change = AsyncMock()
    tracker.update_sync_data = AsyncMock()
    tracker._db = MagicMock()
    tracker._db.close = AsyncMock()
    return tracker


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
def mock_book_storage():
    """Mock book storage for testing."""
    storage = MagicMock()
    storage.archive_exists = AsyncMock(return_value=False)
    storage.archive_matches_encrypted_etag = AsyncMock(return_value=False)
    return storage


@pytest.fixture
def mock_staging_manager():
    """Mock staging directory manager for testing."""
    manager = MagicMock()
    manager.get_staging_path = MagicMock(return_value=Path("/tmp/staging/test_file"))
    manager.cleanup_file = AsyncMock()
    manager.check_and_wait_for_space = AsyncMock()
    return manager


@pytest.fixture(autouse=True)
def mock_process_stage():
    """Create a mock process summary stage for testing. Auto-used in all tests."""
    return ProcessStageMetrics("test")
