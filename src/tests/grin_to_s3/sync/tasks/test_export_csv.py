#!/usr/bin/env python3
"""
Tests for sync tasks export_csv module.
"""

import tempfile
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest

from grin_to_s3.storage.staging import DirectoryManager
from grin_to_s3.sync.tasks import export_csv
from grin_to_s3.sync.tasks.task_types import TaskAction, TaskType
from tests.test_utils.unified_mocks import create_book_manager_mock, standard_storage_config


@pytest.mark.asyncio
async def test_export_csv_with_compression_enabled():
    """CSV export should compress files when compression is enabled."""
    book_manager = create_book_manager_mock(
        storage_config=standard_storage_config(bucket_meta="test-meta-bucket"),
        custom_config={"csv_paths": ("books_latest.csv.gz", "books_timestamped.csv.gz")},
    )

    filesystem_manager = MagicMock(spec=DirectoryManager)
    pipeline = MagicMock()
    pipeline.filesystem_manager = filesystem_manager
    pipeline.book_manager = book_manager
    pipeline.config.storage_config = {"config": {"bucket_meta": "test-meta-bucket"}}
    pipeline.config.sync_compression_meta_enabled = True
    pipeline.uses_block_storage = True

    with (
        tempfile.TemporaryDirectory() as temp_dir,
        patch("grin_to_s3.sync.tasks.export_csv.upload_csv_to_storage") as mock_upload,
        patch("grin_to_s3.sync.tasks.export_csv.write_books_to_csv") as mock_write_csv,
    ):
        staging_path = Path(temp_dir)
        filesystem_manager.staging_path = staging_path

        # Mock CSV generation
        temp_csv = staging_path / "temp.csv"
        temp_csv.write_text("barcode,title\nTEST123,Test Book")
        mock_write_csv.return_value = (temp_csv, 0)

        # Mock upload result
        bucket_path = Path("test-meta-bucket/books_latest.csv.gz")
        mock_upload.return_value = bucket_path

        result = await export_csv.main(pipeline)

        # Should use compression and create .gz files
        assert result.task_type == TaskType.EXPORT_CSV
        assert result.action == TaskAction.COMPLETED

        # Should call upload with compression enabled
        mock_upload.assert_called_once_with(
            csv_path=temp_csv, book_manager=pipeline.book_manager, compression_enabled=True
        )


@pytest.mark.asyncio
async def test_export_csv_with_compression_disabled():
    """CSV export should not compress files when compression is disabled."""
    book_manager = create_book_manager_mock(
        storage_config=standard_storage_config(bucket_meta="test-meta-bucket"),
        custom_config={"csv_paths": ("books_latest.csv", "books_timestamped.csv")},
    )

    filesystem_manager = MagicMock(spec=DirectoryManager)
    pipeline = MagicMock()
    pipeline.filesystem_manager = filesystem_manager
    pipeline.book_manager = book_manager
    pipeline.config.storage_config = {"config": {"bucket_meta": "test-meta-bucket"}}
    pipeline.config.sync_compression_meta_enabled = False
    pipeline.uses_block_storage = True

    with (
        tempfile.TemporaryDirectory() as temp_dir,
        patch("grin_to_s3.sync.tasks.export_csv.upload_csv_to_storage") as mock_upload,
        patch("grin_to_s3.sync.tasks.export_csv.write_books_to_csv") as mock_write_csv,
    ):
        staging_path = Path(temp_dir)
        filesystem_manager.staging_path = staging_path

        # Mock CSV generation
        temp_csv = staging_path / "temp.csv"
        temp_csv.write_text("barcode,title\nTEST123,Test Book")
        mock_write_csv.return_value = (temp_csv, 0)

        # Mock upload result
        bucket_path = Path("test-meta-bucket/books_latest.csv")
        mock_upload.return_value = bucket_path

        result = await export_csv.main(pipeline)

        # Should not use compression
        assert result.task_type == TaskType.EXPORT_CSV
        assert result.action == TaskAction.COMPLETED

        # Should call upload with compression disabled
        mock_upload.assert_called_once_with(
            csv_path=temp_csv, book_manager=pipeline.book_manager, compression_enabled=False
        )


@pytest.mark.asyncio
async def test_export_csv_local_storage():
    """CSV export should work with local storage."""
    book_manager = create_book_manager_mock(
        storage_config=standard_storage_config(storage_type="local", bucket_meta="meta"),
        custom_config={"csv_paths": ("books_latest.csv.gz", "books_timestamped.csv.gz")},
    )

    filesystem_manager = MagicMock(spec=DirectoryManager)
    pipeline = MagicMock()
    pipeline.filesystem_manager = filesystem_manager
    pipeline.book_manager = book_manager
    pipeline.config.storage_config = {"config": {"base_path": "/tmp/output"}}
    pipeline.config.sync_compression_meta_enabled = True
    pipeline.uses_block_storage = False

    with (
        tempfile.TemporaryDirectory() as temp_dir,
        patch("grin_to_s3.sync.tasks.export_csv.upload_csv_to_storage") as mock_upload,
        patch("grin_to_s3.sync.tasks.export_csv.write_books_to_csv") as mock_write_csv,
    ):
        staging_path = Path(temp_dir)
        filesystem_manager.staging_path = staging_path

        # Mock CSV generation
        temp_csv = staging_path / "temp.csv"
        temp_csv.write_text("barcode,title\nTEST123,Test Book")
        mock_write_csv.return_value = (temp_csv, 0)

        # Mock upload result
        bucket_path = Path("meta/books_latest.csv.gz")
        mock_upload.return_value = bucket_path

        result = await export_csv.main(pipeline)

        # Should complete successfully for local storage
        assert result.task_type == TaskType.EXPORT_CSV
        assert result.action == TaskAction.COMPLETED
        assert result.data["record_count"] == 0

        # Should call upload with compression enabled for local storage
        mock_upload.assert_called_once_with(
            csv_path=temp_csv, book_manager=pipeline.book_manager, compression_enabled=True
        )


@pytest.mark.asyncio
async def test_export_csv_with_sample_data():
    """CSV export should handle sample data correctly."""
    book_manager = create_book_manager_mock(
        storage_config=standard_storage_config(bucket_meta="test-meta-bucket"),
        custom_config={"csv_paths": ("books_latest.csv", "books_timestamped.csv")},
    )

    filesystem_manager = MagicMock(spec=DirectoryManager)
    pipeline = MagicMock()
    pipeline.filesystem_manager = filesystem_manager
    pipeline.book_manager = book_manager
    pipeline.config.storage_config = {"config": {"bucket_meta": "test-meta-bucket"}}
    pipeline.config.sync_compression_meta_enabled = False
    pipeline.uses_block_storage = True

    with (
        tempfile.TemporaryDirectory() as temp_dir,
        patch("grin_to_s3.sync.tasks.export_csv.upload_csv_to_storage") as mock_upload,
        patch("grin_to_s3.sync.tasks.export_csv.write_books_to_csv") as mock_write_csv,
    ):
        staging_path = Path(temp_dir)
        filesystem_manager.staging_path = staging_path

        # Mock CSV generation with sample data
        temp_csv = staging_path / "temp.csv"
        temp_csv.write_text("barcode,title\nTEST456,Another Book")
        mock_write_csv.return_value = (temp_csv, 1)

        # Mock upload result
        bucket_path = Path("test-meta-bucket/books_latest.csv")
        mock_upload.return_value = bucket_path

        result = await export_csv.main(pipeline)

        # Should process the sample data
        assert result.task_type == TaskType.EXPORT_CSV
        assert result.action == TaskAction.COMPLETED
        assert result.data["record_count"] == 1

        # Should call upload with compression disabled
        mock_upload.assert_called_once_with(
            csv_path=temp_csv, book_manager=pipeline.book_manager, compression_enabled=False
        )
