"""Tests for database backup functionality."""

import tempfile
from pathlib import Path
from unittest.mock import AsyncMock, Mock

import pytest

from grin_transfer.database.database_backup import create_local_database_backup, upload_database_to_storage
from grin_transfer.sync.tasks.task_types import TaskAction


@pytest.mark.asyncio
async def test_create_local_database_backup_success(temp_db):
    """Test successful local database backup creation."""
    # Create backup
    result = await create_local_database_backup(temp_db)

    assert result["status"] == "completed"
    assert result["file_size"] > 0
    assert result["backup_filename"] is not None
    assert "_backup_" in result["backup_filename"]
    assert result["backup_filename"].endswith(".db")


@pytest.mark.asyncio
async def test_create_local_database_backup_missing_file():
    """Test backup of non-existent database file."""
    result = await create_local_database_backup(Path("/nonexistent/path.db"))

    assert result["status"] == "skipped"
    assert result["file_size"] == 0
    assert result["backup_filename"] is None


@pytest.mark.asyncio
async def test_create_local_database_backup_custom_backup_dir(temp_db):
    """Test backup creation with custom backup directory."""
    with tempfile.TemporaryDirectory() as temp_dir:
        # Create custom backup directory
        custom_backup_dir = Path(temp_dir) / "custom_backups"
        custom_backup_dir.mkdir()

        # Create backup
        result = await create_local_database_backup(temp_db, str(custom_backup_dir))

        assert result["status"] == "completed"
        assert result["file_size"] > 0
        assert result["backup_filename"] is not None

        # Verify backup file exists in custom directory
        backup_files = list(custom_backup_dir.glob("*_backup_*.db"))
        assert len(backup_files) == 1
        assert backup_files[0].name == result["backup_filename"]


@pytest.mark.asyncio
async def test_upload_database_to_storage_timestamped(temp_db):
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

    # Use the temp_db fixture's database file
    db_path = Path(temp_db)

    # Upload database
    result = await upload_database_to_storage(db_path, mock_book_manager)

    assert result.action == TaskAction.COMPLETED
    assert result.data
    assert result.data["backup_filename"] == "books_latest.db.gz"
    assert result.data["file_size"] > 0
    assert mock_storage.write_file.call_count == 2
    calls = mock_storage.write_file.call_args_list
    paths_called = [call[0][0] for call in calls]
    assert "meta/test_run/books_latest.db.gz" in paths_called
    assert "meta/test_run/timestamped/books_latest.db.gz" in paths_called
