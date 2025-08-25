#!/usr/bin/env python3
"""
Tests for sync tasks export_csv module.
"""

import tempfile
from pathlib import Path
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from grin_to_s3.storage.staging import DirectoryManager
from grin_to_s3.sync.tasks import export_csv
from grin_to_s3.sync.tasks.task_types import TaskAction, TaskType


@pytest.mark.asyncio
async def test_export_csv_with_compression_enabled():
    """CSV export should compress files when compression is enabled."""
    filesystem_manager = MagicMock(spec=DirectoryManager)
    pipeline = MagicMock()
    pipeline.filesystem_manager = filesystem_manager
    pipeline.storage.write_file = AsyncMock()
    pipeline.config.storage_config = {"config": {"bucket_meta": "test-meta-bucket"}}
    pipeline.config.sync_compression_meta_enabled = True
    pipeline.uses_block_storage = True

    # Mock database tracker
    pipeline.db_tracker.get_all_books_csv_data = AsyncMock()
    pipeline.db_tracker.get_all_books_csv_data.return_value = []

    with tempfile.TemporaryDirectory() as temp_dir:
        staging_path = Path(temp_dir)
        filesystem_manager.staging_path = staging_path

        with patch("grin_to_s3.sync.tasks.export_csv.compress_file_to_temp") as mock_compress:
            compressed_path = staging_path / "compressed.gz"
            compressed_path.write_text("compressed content")
            mock_compress.return_value.__aenter__.return_value = compressed_path

            result = await export_csv.main(pipeline)

            # Should use compression and create .gz files
            assert result.task_type == TaskType.EXPORT_CSV
            assert result.action == TaskAction.COMPLETED
            mock_compress.assert_called_once()

            # Should upload compressed files
            assert pipeline.storage.write_file.call_count == 2

            # Check that both uploads use .gz extension
            calls = pipeline.storage.write_file.call_args_list
            assert calls[0][0][0].endswith(".gz")
            assert calls[1][0][0].endswith(".gz")


@pytest.mark.asyncio
async def test_export_csv_with_compression_disabled():
    """CSV export should not compress files when compression is disabled."""
    filesystem_manager = MagicMock(spec=DirectoryManager)
    pipeline = MagicMock()
    pipeline.filesystem_manager = filesystem_manager
    pipeline.storage.write_file = AsyncMock()
    pipeline.config.storage_config = {"config": {"bucket_meta": "test-meta-bucket"}}
    pipeline.config.sync_compression_meta_enabled = False
    pipeline.uses_block_storage = True

    # Mock database tracker
    pipeline.db_tracker.get_all_books_csv_data = AsyncMock()
    pipeline.db_tracker.get_all_books_csv_data.return_value = []

    with tempfile.TemporaryDirectory() as temp_dir:
        staging_path = Path(temp_dir)
        filesystem_manager.staging_path = staging_path

        with patch("grin_to_s3.sync.tasks.export_csv.compress_file_to_temp") as mock_compress:
            result = await export_csv.main(pipeline)

            # Should not use compression
            assert result.task_type == TaskType.EXPORT_CSV
            assert result.action == TaskAction.COMPLETED
            mock_compress.assert_not_called()

            # Should upload uncompressed files
            assert pipeline.storage.write_file.call_count == 2

            # Check that both uploads don't have .gz extension
            calls = pipeline.storage.write_file.call_args_list
            assert not calls[0][0][0].endswith(".gz")
            assert not calls[1][0][0].endswith(".gz")
            assert "books_latest.csv" in calls[0][0][0]


@pytest.mark.asyncio
async def test_export_csv_local_storage():
    """CSV export should work with local storage."""
    filesystem_manager = MagicMock(spec=DirectoryManager)
    pipeline = MagicMock()
    pipeline.filesystem_manager = filesystem_manager
    pipeline.config.storage_config = {"config": {"base_path": "/tmp/output"}}
    pipeline.config.sync_compression_meta_enabled = True
    pipeline.uses_block_storage = False

    # Mock database tracker
    pipeline.db_tracker.get_all_books_csv_data = AsyncMock()
    pipeline.db_tracker.get_all_books_csv_data.return_value = []

    with tempfile.TemporaryDirectory() as temp_dir:
        staging_path = Path(temp_dir)
        filesystem_manager.staging_path = staging_path

        result = await export_csv.main(pipeline)

        # Should complete successfully for local storage
        assert result.task_type == TaskType.EXPORT_CSV
        assert result.action == TaskAction.COMPLETED
        assert result.data["record_count"] == 0

        # Should create CSV file locally
        expected_csv_path = staging_path / "meta" / "books_latest.csv"
        assert expected_csv_path.exists()


@pytest.mark.asyncio
async def test_export_csv_with_sample_data():
    """CSV export should handle sample data correctly."""
    filesystem_manager = MagicMock(spec=DirectoryManager)
    pipeline = MagicMock()
    pipeline.filesystem_manager = filesystem_manager
    pipeline.storage.write_file = AsyncMock()
    pipeline.config.storage_config = {"config": {"bucket_meta": "test-meta-bucket"}}
    pipeline.config.sync_compression_meta_enabled = False
    pipeline.uses_block_storage = True

    # Mock database tracker with sample data
    mock_book = MagicMock()
    mock_book.to_csv_row.return_value = ["TEST456", "Another Book", "2024"]
    pipeline.db_tracker.get_all_books_csv_data = AsyncMock()
    pipeline.db_tracker.get_all_books_csv_data.return_value = [mock_book]

    with tempfile.TemporaryDirectory() as temp_dir:
        staging_path = Path(temp_dir)
        filesystem_manager.staging_path = staging_path

        result = await export_csv.main(pipeline)

        # Should process the sample data
        assert result.task_type == TaskType.EXPORT_CSV
        assert result.action == TaskAction.COMPLETED
        assert result.data["record_count"] == 1

        # Should upload uncompressed files
        assert pipeline.storage.write_file.call_count == 2

        # Verify uploads use the original CSV file (no .gz)
        calls = pipeline.storage.write_file.call_args_list
        assert str(calls[0][0][1]).endswith("books_latest.csv")
        assert str(calls[1][0][1]).endswith("books_latest.csv")
