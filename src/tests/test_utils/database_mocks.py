"""Database mocking utilities for tests.

DEPRECATED: Progress tracker mocking moved to unified_mocks.py
Database-specific utilities remain here.
"""

from unittest.mock import AsyncMock, MagicMock

from .unified_mocks import MockStorageFactory


def create_mock_progress_tracker(db_path: str = "/tmp/test.db") -> MagicMock:
    """Create a fully configured mock progress tracker with database-specific methods."""
    # Use unified factory and add database-specific functionality
    mock_tracker = MockStorageFactory.create_progress_tracker(db_path)

    # Add database-specific methods not in unified version
    mock_tracker._db = MagicMock()
    mock_tracker._db.close = AsyncMock()
    mock_tracker.close = AsyncMock()
    mock_tracker.init_db = AsyncMock()

    return mock_tracker


def create_mock_book_for_sync(barcode: str = "TEST123", **kwargs) -> dict:
    """Create a mock book dict for sync operations."""
    default_book = {
        "barcode": barcode,
        "encrypted_etag": "abc123def",
        "file_size": 1024 * 1024,
        "grin_institute": "Harvard",
        "converted_date": "2024-01-01",
        "encrypted_filepath": f"/staging/{barcode}.tar.gz.gpg",
    }
    default_book.update(kwargs)
    return default_book
