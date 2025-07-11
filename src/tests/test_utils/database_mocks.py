"""Database mocking utilities for tests.

DEPRECATED: Progress tracker mocking moved to unified_mocks.py
Database-specific utilities remain here.
"""

from unittest.mock import MagicMock

from .unified_mocks import MockStorageFactory


def create_mock_progress_tracker(db_path: str = "/tmp/test.db") -> MagicMock:
    """DEPRECATED: Use MockStorageFactory.create_progress_tracker() instead."""
    return MockStorageFactory.create_progress_tracker(db_path)


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
