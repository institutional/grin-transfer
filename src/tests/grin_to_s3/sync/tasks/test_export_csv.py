#!/usr/bin/env python3
"""
Tests for sync tasks export_csv module.
"""

from pathlib import Path
from unittest.mock import MagicMock

import pytest

from grin_to_s3.sync.tasks import export_csv
from grin_to_s3.sync.tasks.task_types import TaskAction, TaskType
from tests.test_utils.unified_mocks import create_book_manager_mock, standard_storage_config


@pytest.mark.asyncio
async def test_export_csv_with_compression_enabled(temp_filesystem_manager):
    """CSV export should compress files when compression is enabled."""
    from tests.test_utils.unified_mocks import configure_pipeline_storage, mock_export_csv_operations

    book_manager = create_book_manager_mock(
        storage_config=standard_storage_config(bucket_meta="test-meta-bucket"),
        custom_config={"csv_paths": ("books_latest.csv.gz", "books_timestamped.csv.gz")},
    )

    pipeline = MagicMock()
    pipeline.filesystem_manager = temp_filesystem_manager
    pipeline.book_manager = book_manager
    pipeline.config = MagicMock()
    pipeline.config.sync_compression_meta_enabled = True

    configure_pipeline_storage(pipeline, bucket_meta="test-meta-bucket")

    staging_path = temp_filesystem_manager.staging_path

    with mock_export_csv_operations(record_count=0) as mocks:
        # Mock CSV generation
        temp_csv = staging_path / "temp.csv"
        temp_csv.write_text("barcode,title\nTEST123,Test Book")
        mocks.write_books_to_csv.return_value = (temp_csv, 0)

        # Mock upload result
        bucket_path = Path("test-meta-bucket/books_latest.csv.gz")
        mocks.upload_csv_to_storage.return_value = bucket_path

        result = await export_csv.main(pipeline)

        # Should use compression and create .gz files
        assert result.task_type == TaskType.EXPORT_CSV
        assert result.action == TaskAction.COMPLETED

        # Should call upload with compression enabled
        mocks.upload_csv_to_storage.assert_called_once_with(
            csv_path=temp_csv, book_manager=pipeline.book_manager, compression_enabled=True
        )


@pytest.mark.asyncio
async def test_export_csv_with_compression_disabled(temp_filesystem_manager):
    """CSV export should not compress files when compression is disabled."""
    from tests.test_utils.unified_mocks import configure_pipeline_storage, mock_export_csv_operations

    book_manager = create_book_manager_mock(
        storage_config=standard_storage_config(bucket_meta="test-meta-bucket"),
        custom_config={"csv_paths": ("books_latest.csv", "books_timestamped.csv")},
    )

    pipeline = MagicMock()
    pipeline.filesystem_manager = temp_filesystem_manager
    pipeline.book_manager = book_manager
    pipeline.config = MagicMock()
    pipeline.config.sync_compression_meta_enabled = False

    configure_pipeline_storage(pipeline, bucket_meta="test-meta-bucket")

    staging_path = temp_filesystem_manager.staging_path

    with mock_export_csv_operations(record_count=0) as mocks:
        # Mock CSV generation
        temp_csv = staging_path / "temp.csv"
        temp_csv.write_text("barcode,title\nTEST123,Test Book")
        mocks.write_books_to_csv.return_value = (temp_csv, 0)

        # Mock upload result
        bucket_path = Path("test-meta-bucket/books_latest.csv")
        mocks.upload_csv_to_storage.return_value = bucket_path

        result = await export_csv.main(pipeline)

        # Should not use compression
        assert result.task_type == TaskType.EXPORT_CSV
        assert result.action == TaskAction.COMPLETED

        # Should call upload with compression disabled
        mocks.upload_csv_to_storage.assert_called_once_with(
            csv_path=temp_csv, book_manager=pipeline.book_manager, compression_enabled=False
        )


@pytest.mark.asyncio
async def test_export_csv_local_storage(temp_filesystem_manager):
    """CSV export should work with local storage."""
    from tests.test_utils.unified_mocks import configure_pipeline_storage, mock_export_csv_operations

    book_manager = create_book_manager_mock(
        storage_config=standard_storage_config(storage_type="local", bucket_meta="meta"),
        custom_config={"csv_paths": ("books_latest.csv.gz", "books_timestamped.csv.gz")},
    )

    pipeline = MagicMock()
    pipeline.filesystem_manager = temp_filesystem_manager
    pipeline.book_manager = book_manager
    pipeline.config = MagicMock()
    pipeline.config.sync_compression_meta_enabled = True

    configure_pipeline_storage(pipeline, storage_type="local", base_path="/tmp/output")

    staging_path = temp_filesystem_manager.staging_path

    with mock_export_csv_operations(record_count=0) as mocks:
        # Mock CSV generation
        temp_csv = staging_path / "temp.csv"
        temp_csv.write_text("barcode,title\nTEST123,Test Book")
        mocks.write_books_to_csv.return_value = (temp_csv, 0)

        # Mock upload result
        bucket_path = Path("meta/books_latest.csv.gz")
        mocks.upload_csv_to_storage.return_value = bucket_path

        result = await export_csv.main(pipeline)

        # Should complete successfully for local storage
        assert result.task_type == TaskType.EXPORT_CSV
        assert result.action == TaskAction.COMPLETED
        assert result.data["record_count"] == 0

        # Should call upload with compression enabled for local storage
        mocks.upload_csv_to_storage.assert_called_once_with(
            csv_path=temp_csv, book_manager=pipeline.book_manager, compression_enabled=True
        )


@pytest.mark.asyncio
async def test_export_csv_with_sample_data(temp_filesystem_manager):
    """CSV export should handle sample data correctly."""
    from tests.test_utils.unified_mocks import configure_pipeline_storage, mock_export_csv_operations

    book_manager = create_book_manager_mock(
        storage_config=standard_storage_config(bucket_meta="test-meta-bucket"),
        custom_config={"csv_paths": ("books_latest.csv", "books_timestamped.csv")},
    )

    pipeline = MagicMock()
    pipeline.filesystem_manager = temp_filesystem_manager
    pipeline.book_manager = book_manager
    pipeline.config = MagicMock()
    pipeline.config.sync_compression_meta_enabled = False

    configure_pipeline_storage(pipeline, bucket_meta="test-meta-bucket")

    staging_path = temp_filesystem_manager.staging_path

    with mock_export_csv_operations(record_count=1) as mocks:
        # Mock CSV generation with sample data
        temp_csv = staging_path / "temp.csv"
        temp_csv.write_text("barcode,title\nTEST456,Another Book")
        mocks.write_books_to_csv.return_value = (temp_csv, 1)

        # Mock upload result
        bucket_path = Path("test-meta-bucket/books_latest.csv")
        mocks.upload_csv_to_storage.return_value = bucket_path

        result = await export_csv.main(pipeline)

        # Should process the sample data
        assert result.task_type == TaskType.EXPORT_CSV
        assert result.action == TaskAction.COMPLETED
        assert result.data["record_count"] == 1

        # Should call upload with compression disabled
        mocks.upload_csv_to_storage.assert_called_once_with(
            csv_path=temp_csv, book_manager=pipeline.book_manager, compression_enabled=False
        )
