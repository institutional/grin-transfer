"""Tests for database backup functionality."""

import tempfile
from pathlib import Path
from unittest.mock import AsyncMock, Mock

import pytest

from grin_to_s3.database.database_backup import create_local_database_backup, upload_database_to_storage
from grin_to_s3.sync.tasks.task_types import TaskAction


@pytest.mark.asyncio
async def test_create_local_database_backup_success():
    """Test successful local database backup creation."""
    with tempfile.TemporaryDirectory() as temp_dir:
        # Create mock database file
        db_path = Path(temp_dir) / "test.db"
        db_path.write_text("mock database content")

        # Create backup
        result = await create_local_database_backup(str(db_path))

        assert result["status"] == "completed"
        assert result["file_size"] > 0
        assert result["backup_filename"] is not None
        assert "test_backup_" in result["backup_filename"]


@pytest.mark.asyncio
async def test_create_local_database_backup_missing_file():
    """Test backup of non-existent database file."""
    result = await create_local_database_backup("/nonexistent/path.db")

    assert result["status"] == "skipped"
    assert result["file_size"] == 0
    assert result["backup_filename"] is None


@pytest.mark.asyncio
async def test_create_local_database_backup_custom_backup_dir():
    """Test backup creation with custom backup directory."""
    with tempfile.TemporaryDirectory() as temp_dir:
        # Create mock database file
        db_path = Path(temp_dir) / "test.db"
        db_path.write_text("mock database content")

        # Create custom backup directory
        custom_backup_dir = Path(temp_dir) / "custom_backups"
        custom_backup_dir.mkdir()

        # Create backup
        result = await create_local_database_backup(str(db_path), str(custom_backup_dir))

        assert result["status"] == "completed"
        assert result["file_size"] > 0
        assert result["backup_filename"] is not None

        # Verify backup file exists in custom directory
        backup_files = list(custom_backup_dir.glob("test_backup_*.db"))
        assert len(backup_files) == 1
        assert backup_files[0].name == result["backup_filename"]


@pytest.mark.asyncio
async def test_upload_database_to_storage_latest():
    """Test database upload as latest version."""
    # Mock setup
    mock_storage = Mock()
    mock_book_manager = Mock()
    mock_book_manager.meta_path.side_effect = [
        "meta/test_run/books_latest.db.gz",
        "meta/test_run/timestamped/books_latest.db.gz",
    ]
    mock_book_manager.storage = mock_storage
    mock_storage.write_file = AsyncMock()

    with tempfile.TemporaryDirectory() as temp_dir:
        # Create mock database file
        db_path = Path(temp_dir) / "test.db"
        db_path.write_text("mock database content")

        # Upload as latest
        result = await upload_database_to_storage(
            db_path,
            mock_book_manager,
        )

        assert result.action == TaskAction.COMPLETED
        assert result.data["backup_filename"] == "books_latest.db.gz"
        assert result.data["file_size"] > 0
        # Note: compressed_size is not in DatabaseBackupData, it's calculated internally
        assert mock_storage.write_file.call_count == 2
        calls = mock_storage.write_file.call_args_list
        paths_called = [call[0][0] for call in calls]
        assert "meta/test_run/books_latest.db.gz" in paths_called
        assert "meta/test_run/timestamped/books_latest.db.gz" in paths_called


@pytest.mark.asyncio
async def test_upload_database_to_storage_timestamped():
    """Test database upload uploads to both latest and timestamped paths."""
    # Mock setup
    mock_storage = Mock()
    mock_book_manager = Mock()
    mock_book_manager.meta_path.side_effect = [
        "meta/test_run/books_latest.db.gz",
        "meta/test_run/timestamped/books_latest.db.gz",
    ]
    mock_book_manager.storage = mock_storage
    mock_storage.write_file = AsyncMock()

    with tempfile.TemporaryDirectory() as temp_dir:
        # Create mock database file
        db_path = Path(temp_dir) / "test.db"
        db_path.write_text("mock database content")

        # Upload database
        result = await upload_database_to_storage(db_path, mock_book_manager)

        assert result.action == TaskAction.COMPLETED
        assert result.data["backup_filename"] == "books_latest.db.gz"
        assert result.data["file_size"] > 0
        # Note: compressed_size is not in DatabaseBackupData, it's calculated internally
        assert mock_storage.write_file.call_count == 2
        calls = mock_storage.write_file.call_args_list
        paths_called = [call[0][0] for call in calls]
        assert "meta/test_run/books_latest.db.gz" in paths_called
        assert "meta/test_run/timestamped/books_latest.db.gz" in paths_called


@pytest.mark.asyncio
async def test_upload_database_to_storage_compression_cleanup():
    """Test database upload handles temporary compression files correctly."""
    # Mock setup
    mock_storage = Mock()
    mock_book_manager = Mock()
    mock_book_manager.meta_path.side_effect = [
        "meta/test_run/books_latest.db.gz",
        "meta/test_run/timestamped/books_latest.db.gz",
    ]
    mock_book_manager.storage = mock_storage
    mock_storage.write_file = AsyncMock()

    with tempfile.TemporaryDirectory() as temp_dir:
        # Create mock database file
        db_path = Path(temp_dir) / "test.db"
        db_path.write_text("mock database content")

        # Upload database
        result = await upload_database_to_storage(db_path, mock_book_manager)

        assert result.action == TaskAction.COMPLETED
        assert result.data["backup_filename"] == "books_latest.db.gz"
        assert result.data["file_size"] > 0
        # Note: compressed_size is not in DatabaseBackupData, it's calculated internally
        assert mock_storage.write_file.call_count == 2
        calls = mock_storage.write_file.call_args_list
        paths_called = [call[0][0] for call in calls]
        assert "meta/test_run/books_latest.db.gz" in paths_called
        assert "meta/test_run/timestamped/books_latest.db.gz" in paths_called

        # Verify temporary compression files are cleaned up automatically
        # (compress_file_to_temp context manager handles cleanup)


@pytest.mark.asyncio
async def test_upload_database_to_storage_upload_error():
    """Test database upload with storage error."""
    # Mock setup with failing storage
    mock_storage = Mock()
    mock_book_manager = Mock()
    mock_book_manager.meta_path.side_effect = [
        "meta/test_run/books_latest.db.gz",
        "meta/test_run/timestamped/books_latest.db.gz",
    ]
    mock_book_manager.storage = mock_storage
    mock_storage.write_file = AsyncMock(side_effect=Exception("Storage error"))

    with tempfile.TemporaryDirectory() as temp_dir:
        # Create mock database file
        db_path = Path(temp_dir) / "test.db"
        db_path.write_text("mock database content")

        # Upload should fail
        result = await upload_database_to_storage(db_path, mock_book_manager)

        assert result.action == TaskAction.FAILED
        assert result.error is not None  # Error message should be present
        # Note: For failed results, data may have partial information depending on when failure occurred
