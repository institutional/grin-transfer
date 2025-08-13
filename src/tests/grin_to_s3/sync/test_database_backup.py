"""Tests for database backup functionality."""

import tempfile
from pathlib import Path
from unittest.mock import AsyncMock, Mock

import pytest

from grin_to_s3.sync.database_backup import create_local_database_backup, upload_database_to_storage


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
    mock_book_manager._meta_path.return_value = "meta/books_latest.db.gz"
    mock_book_manager.storage = mock_storage
    mock_storage.write_file = AsyncMock()

    with tempfile.TemporaryDirectory() as temp_dir:
        # Create mock database file
        db_path = Path(temp_dir) / "test.db"
        db_path.write_text("mock database content")

        # Upload as latest
        result = await upload_database_to_storage(
            str(db_path), mock_book_manager, staging_manager=None, upload_type="latest"
        )

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
    mock_book_manager._meta_path.return_value = "meta/database_backups/books_backup_20240101_120000.db.gz"
    mock_book_manager.storage = mock_storage
    mock_storage.write_file = AsyncMock()

    with tempfile.TemporaryDirectory() as temp_dir:
        # Create mock database file
        db_path = Path(temp_dir) / "test.db"
        db_path.write_text("mock database content")

        # Upload as timestamped backup
        result = await upload_database_to_storage(
            str(db_path), mock_book_manager, staging_manager=None, upload_type="timestamped"
        )

        assert result["status"] == "completed"
        assert "books_backup_" in result["backup_filename"]
        assert result["backup_filename"].endswith(".db.gz")
        assert result["file_size"] > 0
        assert result["compressed_size"] > 0
        mock_storage.write_file.assert_called_once()


@pytest.mark.asyncio
async def test_upload_database_to_storage_with_staging():
    """Test database upload using staging manager."""
    # Mock setup
    mock_storage = Mock()
    mock_book_manager = Mock()
    mock_book_manager._meta_path.return_value = "meta/books_latest.db.gz"
    mock_book_manager.storage = mock_storage
    mock_storage.write_file = AsyncMock()

    mock_staging_manager = Mock()

    with tempfile.TemporaryDirectory() as temp_dir:
        # Create mock database file
        db_path = Path(temp_dir) / "test.db"
        db_path.write_text("mock database content")

        # Create staging directory
        staging_dir = Path(temp_dir) / "staging"
        staging_dir.mkdir()
        mock_staging_manager.staging_path = staging_dir

        # Upload using staging
        result = await upload_database_to_storage(
            str(db_path), mock_book_manager, staging_manager=mock_staging_manager, upload_type="latest"
        )

        assert result["status"] == "completed"
        assert result["backup_filename"] == "books_latest.db.gz"
        assert result["file_size"] > 0
        assert result["compressed_size"] > 0
        mock_storage.write_file.assert_called_once()

        # Verify no staging files remain (compressed temp files are cleaned up)
        db_files = list(staging_dir.glob("*.db"))
        gz_files = list(staging_dir.glob("*.gz"))
        assert len(db_files) == 0
        assert len(gz_files) == 0


@pytest.mark.asyncio
async def test_upload_database_to_storage_missing_file():
    """Test database upload with missing file."""
    mock_book_manager = Mock()

    result = await upload_database_to_storage(
        "/nonexistent/path.db", mock_book_manager, staging_manager=None, upload_type="latest"
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
    mock_book_manager._meta_path.return_value = "meta/books_latest.db.gz"
    mock_book_manager.storage = mock_storage
    mock_storage.write_file = AsyncMock(side_effect=Exception("Storage error"))

    with tempfile.TemporaryDirectory() as temp_dir:
        # Create mock database file
        db_path = Path(temp_dir) / "test.db"
        db_path.write_text("mock database content")

        # Upload should fail
        result = await upload_database_to_storage(
            str(db_path), mock_book_manager, staging_manager=None, upload_type="latest"
        )

        assert result["status"] == "failed"
        assert result["backup_filename"] == "books_latest.db.gz"  # Filename set before failure
        assert result["file_size"] > 0  # File size calculated before failure


@pytest.mark.asyncio
async def test_cleanup_local_backup_after_upload():
    """Test that local backup files are cleaned up after successful upload when using block storage."""
    with tempfile.TemporaryDirectory() as temp_dir:
        # Create mock database file
        db_path = Path(temp_dir) / "test.db"
        db_path.write_text("mock database content")

        # Create mock backup directory and backup file
        backup_dir = db_path.parent / "backups"
        backup_dir.mkdir()
        backup_file = backup_dir / "test_backup_20240101_120000.db"
        backup_file.write_text("backup content")

        # Import here to avoid circular imports in tests
        from grin_to_s3.sync.pipeline import SyncPipeline

        # Create minimal mock pipeline instance with block storage
        pipeline = Mock()
        pipeline.db_path = str(db_path)
        pipeline.staging_manager = Mock()  # Non-None indicates block storage

        # Call the cleanup method (need to bind it to the mock)
        cleanup_method = SyncPipeline._cleanup_local_backup.__get__(pipeline, SyncPipeline)
        await cleanup_method(backup_file.name)

        # Verify backup file was removed
        assert not backup_file.exists()


@pytest.mark.asyncio
async def test_backup_database_conditional_cleanup():
    """Test that the backup process conditionally cleans up based on storage type."""
    from unittest.mock import AsyncMock, patch

    # Test with block storage (should cleanup)
    with tempfile.TemporaryDirectory() as temp_dir:
        db_path = Path(temp_dir) / "test.db"
        db_path.write_text("mock database content")

        # Import here to avoid circular imports in tests
        from grin_to_s3.sync.pipeline import SyncPipeline

        # Create pipeline with block storage
        pipeline = Mock()
        pipeline.db_path = str(db_path)
        pipeline.staging_manager = Mock()  # Non-None indicates block storage
        pipeline.skip_database_backup = False
        pipeline.uses_block_storage = True

        # Mock the methods we'll call
        with (
            patch("grin_to_s3.sync.pipeline.create_local_database_backup") as mock_create,
            patch("grin_to_s3.sync.pipeline.upload_database_to_storage") as mock_upload,
        ):
            mock_create.return_value = {"status": "completed", "backup_filename": "test_backup.db"}
            mock_upload.return_value = {"status": "completed", "backup_filename": "test_backup.db.gz"}

            with patch.object(pipeline, "_cleanup_local_backup", new=AsyncMock()) as mock_cleanup:
                # Call the actual backup method
                backup_method = SyncPipeline._backup_database_at_start.__get__(pipeline, SyncPipeline)
                await backup_method()

                # Verify cleanup was called for block storage
                mock_cleanup.assert_called_once_with("test_backup.db")

    # Test with local storage (should NOT cleanup)
    with tempfile.TemporaryDirectory() as temp_dir:
        db_path = Path(temp_dir) / "test.db"
        db_path.write_text("mock database content")

        # Create pipeline with local storage
        pipeline = Mock()
        pipeline.db_path = str(db_path)
        pipeline.staging_manager = None  # None indicates local storage
        pipeline.skip_database_backup = False
        pipeline.uses_block_storage = False

        with (
            patch("grin_to_s3.sync.pipeline.create_local_database_backup") as mock_create,
            patch("grin_to_s3.sync.pipeline.upload_database_to_storage") as mock_upload,
        ):
            mock_create.return_value = {"status": "completed", "backup_filename": "test_backup.db"}
            mock_upload.return_value = {"status": "completed", "backup_filename": "test_backup.db.gz"}

            with patch.object(pipeline, "_cleanup_local_backup", new=AsyncMock()) as mock_cleanup:
                # Call the actual backup method
                backup_method = SyncPipeline._backup_database_at_start.__get__(pipeline, SyncPipeline)
                await backup_method()

                # Verify cleanup was NOT called for local storage
                mock_cleanup.assert_not_called()


@pytest.mark.asyncio
async def test_cleanup_handles_missing_backup_file():
    """Test that cleanup handles missing backup files gracefully."""
    with tempfile.TemporaryDirectory() as temp_dir:
        # Create mock database file
        db_path = Path(temp_dir) / "test.db"
        db_path.write_text("mock database content")

        # Create backup directory but no backup file
        backup_dir = db_path.parent / "backups"
        backup_dir.mkdir()

        # Import here to avoid circular imports in tests
        from grin_to_s3.sync.pipeline import SyncPipeline

        # Create minimal mock pipeline instance
        pipeline = Mock()
        pipeline.db_path = str(db_path)

        # Call cleanup with non-existent file
        cleanup_method = SyncPipeline._cleanup_local_backup.__get__(pipeline, SyncPipeline)
        await cleanup_method("nonexistent_backup.db")

        # Should not raise exception (graceful handling tested via no exception)
