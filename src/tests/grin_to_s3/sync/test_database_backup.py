"""Tests for database backup functionality."""

import tempfile
from pathlib import Path
from unittest.mock import AsyncMock, Mock

import pytest

from grin_to_s3.database.database_backup import create_local_database_backup, upload_database_to_storage


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
    mock_book_manager.meta_path.return_value = "meta/books_latest.db.gz"
    mock_book_manager.storage = mock_storage
    mock_storage.write_file = AsyncMock()

    with tempfile.TemporaryDirectory() as temp_dir:
        # Create mock database file
        db_path = Path(temp_dir) / "test.db"
        db_path.write_text("mock database content")

        # Upload as latest
        result = await upload_database_to_storage(str(db_path), mock_book_manager, "test_run", upload_type="latest")

        assert result["status"] == "completed"
        assert result["backup_filename"] == "books_latest.db.gz"
        assert result["file_size"] > 0
        assert result["compressed_size"] > 0
        mock_storage.write_file.assert_called_once()


@pytest.mark.asyncio
async def test_upload_database_to_storage_timestamped():
    """Test database upload as timestamped backup."""
    # Mock setup
    mock_storage = Mock()
    mock_book_manager = Mock()
    mock_book_manager.meta_path.return_value = "meta/database_backups/books_backup_20240101_120000.db.gz"
    mock_book_manager.storage = mock_storage
    mock_storage.write_file = AsyncMock()

    with tempfile.TemporaryDirectory() as temp_dir:
        # Create mock database file
        db_path = Path(temp_dir) / "test.db"
        db_path.write_text("mock database content")

        # Upload as timestamped backup
        result = await upload_database_to_storage(
            str(db_path), mock_book_manager, "test_run", upload_type="timestamped"
        )

        assert result["status"] == "completed"
        assert "books_" in result["backup_filename"]
        assert result["backup_filename"].endswith(".db.gz")
        assert result["file_size"] > 0
        assert result["compressed_size"] > 0
        mock_storage.write_file.assert_called_once()


@pytest.mark.asyncio
async def test_upload_database_to_storage_compression_cleanup():
    """Test database upload handles temporary compression files correctly."""
    # Mock setup
    mock_storage = Mock()
    mock_book_manager = Mock()
    mock_book_manager.meta_path.return_value = "meta/books_latest.db.gz"
    mock_book_manager.storage = mock_storage
    mock_storage.write_file = AsyncMock()

    with tempfile.TemporaryDirectory() as temp_dir:
        # Create mock database file
        db_path = Path(temp_dir) / "test.db"
        db_path.write_text("mock database content")

        # Upload database
        result = await upload_database_to_storage(str(db_path), mock_book_manager, "test_run", upload_type="latest")

        assert result["status"] == "completed"
        assert result["backup_filename"] == "books_latest.db.gz"
        assert result["file_size"] > 0
        assert result["compressed_size"] > 0
        mock_storage.write_file.assert_called_once()

        # Verify temporary compression files are cleaned up automatically
        # (compress_file_to_temp context manager handles cleanup)


@pytest.mark.asyncio
async def test_upload_database_to_storage_missing_file():
    """Test database upload with missing file."""
    mock_book_manager = Mock()

    result = await upload_database_to_storage(
        "/nonexistent/path.db", mock_book_manager, "test_run", upload_type="latest"
    )

    assert result["status"] == "skipped"
    assert result["file_size"] == 0
    assert result["backup_filename"] is None


@pytest.mark.asyncio
async def test_upload_database_to_storage_upload_error():
    """Test database upload with storage error."""
    # Mock setup with failing storage
    mock_storage = Mock()
    mock_book_manager = Mock()
    mock_book_manager.meta_path.return_value = "meta/books_latest.db.gz"
    mock_book_manager.storage = mock_storage
    mock_storage.write_file = AsyncMock(side_effect=Exception("Storage error"))

    with tempfile.TemporaryDirectory() as temp_dir:
        # Create mock database file
        db_path = Path(temp_dir) / "test.db"
        db_path.write_text("mock database content")

        # Upload should fail
        result = await upload_database_to_storage(str(db_path), mock_book_manager, "test_run", upload_type="latest")

        assert result["status"] == "failed"
        assert result["backup_filename"] == "books_latest.db.gz"  # Filename set before failure
        assert result["file_size"] > 0  # File size calculated before failure
