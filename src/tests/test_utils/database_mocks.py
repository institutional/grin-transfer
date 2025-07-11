"""Database mocking utilities for tests."""

from unittest.mock import AsyncMock, MagicMock


def create_mock_progress_tracker(db_path: str = "/tmp/test.db") -> MagicMock:
    """Create a fully configured mock progress tracker."""
    mock_tracker = MagicMock()
    mock_tracker.db_path = db_path
    mock_tracker.add_status_change = AsyncMock()
    mock_tracker.update_sync_data = AsyncMock()
    mock_tracker.get_books_for_sync = AsyncMock(return_value=[])
    mock_tracker.get_sync_stats = AsyncMock(return_value={
        "total_converted": 0, "synced": 0, "failed": 0, "pending": 0
    })
    mock_tracker.update_book_marc_metadata = AsyncMock()
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
