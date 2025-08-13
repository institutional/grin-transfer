"""Integration tests for sync pipeline database backup."""

import tempfile
from pathlib import Path
from unittest.mock import Mock, patch

import pytest

from grin_to_s3.run_config import RunConfig
from grin_to_s3.sync.pipeline import SyncPipeline


@pytest.fixture
def mock_run_config():
    """Create a mock RunConfig for testing."""
    with tempfile.TemporaryDirectory() as temp_dir:
        db_path = Path(temp_dir) / "books.db"
        db_path.write_text("mock database")

        config = Mock(spec=RunConfig)
        config.sqlite_db_path = str(db_path)
        config.storage_type = "local"
        config.storage_config = {
            "type": "local",
            "protocol": "local",
            "config": {
                "base_path": temp_dir,
                "bucket_raw": "raw",
                "bucket_meta": "meta",
                "bucket_full": "full"
            }
        }
        config.library_directory = "test_lib"
        config.secrets_dir = None
        config.sync_concurrent_downloads = 1
        config.sync_concurrent_uploads = 1
        config.sync_batch_size = 10
        config.sync_disk_space_threshold = 1000
        config.sync_enrichment_workers = 1
        config.sync_staging_dir = None

        yield config


@pytest.mark.asyncio
async def test_sync_pipeline_database_backup_enabled(mock_run_config):
    """Test sync pipeline with database backup enabled."""
    # Setup
    process_summary_stage = Mock()
    pipeline = SyncPipeline.from_run_config(
        config=mock_run_config,
        process_summary_stage=process_summary_stage,
        skip_database_backup=False
    )

    with patch.object(pipeline, "_backup_database_at_start") as mock_backup_start:
        with patch.object(pipeline, "_upload_latest_database") as mock_upload_latest:
            # Test backup methods directly
            await pipeline._backup_database_at_start()
            await pipeline._upload_latest_database()

            # Verify calls were made
            mock_backup_start.assert_called_once()
            mock_upload_latest.assert_called_once()


@pytest.mark.asyncio
async def test_sync_pipeline_database_backup_disabled(mock_run_config):
    """Test sync pipeline with database backup disabled."""
    # Setup
    process_summary_stage = Mock()
    pipeline = SyncPipeline.from_run_config(
        config=mock_run_config,
        process_summary_stage=process_summary_stage,
        skip_database_backup=True
    )

    # Verify skip behavior
    await pipeline._backup_database_at_start()  # Should do nothing
    result = await pipeline._upload_latest_database()   # Should return skip result

    # Verify skipped status
    assert result["status"] == "skipped"


@pytest.mark.asyncio
async def test_sync_pipeline_backup_methods_with_mocked_dependencies(mock_run_config):
    """Test backup methods with mocked storage dependencies."""
    process_summary_stage = Mock()

    with patch("grin_to_s3.sync.pipeline.create_local_database_backup") as mock_create_backup:
        with patch("grin_to_s3.sync.pipeline.upload_database_to_storage") as mock_upload:
            with patch("grin_to_s3.sync.pipeline.create_storage_from_config") as mock_create_storage:
                with patch("grin_to_s3.sync.pipeline.BookManager") as mock_book_manager:

                    # Configure mocks before creating pipeline (storage is now initialized in constructor)
                    mock_create_backup.return_value = {
                        "status": "completed",
                        "backup_filename": "test_backup_123.db",
                        "file_size": 1024
                    }

                    mock_upload.return_value = {
                        "status": "completed",
                        "backup_filename": "books_backup_123.db",
                        "file_size": 1024
                    }

                    mock_storage = Mock()
                    mock_create_storage.return_value = mock_storage

                    # Create pipeline after mocks are set up (storage is initialized in constructor)
                    pipeline = SyncPipeline.from_run_config(
                        config=mock_run_config,
                        process_summary_stage=process_summary_stage,
                        skip_database_backup=False
                    )

                    # Test backup at start
                    await pipeline._backup_database_at_start()

                    # Verify calls
                    mock_create_backup.assert_called_once_with(mock_run_config.sqlite_db_path)
                    mock_create_storage.assert_called()
                    mock_book_manager.assert_called()
                    mock_upload.assert_called_once()


@pytest.mark.asyncio
async def test_sync_pipeline_upload_latest_database_with_mocked_dependencies(mock_run_config):
    """Test upload latest database method with mocked storage dependencies."""
    process_summary_stage = Mock()

    with patch("grin_to_s3.sync.pipeline.upload_database_to_storage") as mock_upload:
        with patch("grin_to_s3.sync.pipeline.create_storage_from_config") as mock_create_storage:
            with patch("grin_to_s3.sync.pipeline.BookManager") as mock_book_manager:

                # Configure mocks before creating pipeline (storage is now initialized in constructor)
                mock_upload.return_value = {
                    "status": "completed",
                    "backup_filename": "books_latest.db",
                    "file_size": 1024
                }

                mock_storage = Mock()
                mock_create_storage.return_value = mock_storage

                # Create pipeline after mocks are set up (storage is initialized in constructor)
                pipeline = SyncPipeline.from_run_config(
                    config=mock_run_config,
                    process_summary_stage=process_summary_stage,
                    skip_database_backup=False
                )

                # Test upload latest
                result = await pipeline._upload_latest_database()

                # Verify result
                assert result["status"] == "completed"
                assert result["backup_filename"] == "books_latest.db"
                assert result["file_size"] == 1024

                # Verify calls
                mock_create_storage.assert_called()
                mock_book_manager.assert_called()
                mock_upload.assert_called_once_with(
                    mock_run_config.sqlite_db_path,
                    mock_book_manager.return_value,
                    None,  # staging_manager
                    upload_type="latest"
                )


@pytest.mark.asyncio
async def test_sync_pipeline_backup_error_handling(mock_run_config):
    """Test error handling in database backup methods."""
    process_summary_stage = Mock()
    pipeline = SyncPipeline.from_run_config(
        config=mock_run_config,
        process_summary_stage=process_summary_stage,
        skip_database_backup=False
    )

    with patch("grin_to_s3.sync.pipeline.create_local_database_backup") as mock_create_backup:
        # Configure mock to raise exception
        mock_create_backup.side_effect = Exception("Backup failed")

        # Test error handling - should not raise exception
        try:
            await pipeline._backup_database_at_start()
        except Exception:
            pytest.fail("_backup_database_at_start should handle exceptions gracefully")

        # Verify backup was attempted
        mock_create_backup.assert_called_once()


@pytest.mark.asyncio
async def test_sync_pipeline_upload_latest_error_handling(mock_run_config):
    """Test error handling in upload latest database method."""
    process_summary_stage = Mock()

    with patch("grin_to_s3.sync.pipeline.upload_database_to_storage") as mock_upload:
        # Configure mock to raise exception during upload
        mock_upload.side_effect = Exception("Upload failed")

        # Create pipeline (storage creation should succeed)
        pipeline = SyncPipeline.from_run_config(
            config=mock_run_config,
            process_summary_stage=process_summary_stage,
            skip_database_backup=False
        )

        # Test error handling - should return failed result
        result = await pipeline._upload_latest_database()

        assert result["status"] == "failed"
        assert result["file_size"] == 0
        assert result["backup_time"] == 0.0
        assert result["backup_filename"] is None

        # Verify upload was attempted
        mock_upload.assert_called_once()


@pytest.mark.asyncio
async def test_export_csv_and_upload_database_result(mock_run_config):
    """Test the combined CSV export and database upload result method."""
    process_summary_stage = Mock()
    pipeline = SyncPipeline.from_run_config(
        config=mock_run_config,
        process_summary_stage=process_summary_stage,
        skip_database_backup=False
    )

    with patch.object(pipeline, "_export_csv_if_enabled") as mock_csv_export:
        with patch.object(pipeline, "_upload_latest_database") as mock_db_upload:
            with patch("builtins.print") as mock_print:

                # Configure mocks
                mock_csv_export.return_value = {
                    "status": "completed",
                    "num_rows": 100,
                    "file_size": 2048
                }

                mock_db_upload.return_value = {
                    "status": "completed",
                    "backup_filename": "books_latest.db",
                    "file_size": 1024
                }

                # Test combined method
                await pipeline._export_csv_and_upload_database_result()

                # Verify both operations were called
                mock_csv_export.assert_called_once()
                mock_db_upload.assert_called_once()

                # Verify console output
                assert mock_print.call_count >= 2
                print_calls = [str(call) for call in mock_print.call_args_list]

                # Should print CSV export result
                csv_printed = any("CSV exported: 100 rows (2,048 bytes)" in call for call in print_calls)
                assert csv_printed, f"CSV export not printed correctly. Calls: {print_calls}"

                # Should print database backup result
                db_printed = any("Database backed up: books_latest.db (1,024 bytes)" in call for call in print_calls)
                assert db_printed, f"Database backup not printed correctly. Calls: {print_calls}"
