"""
Unit tests for extract CLI bucket functionality.

Tests the new bucket configuration and automatic writing features.
"""

from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from grin_to_s3.extract.__main__ import extract_single_archive, write_to_bucket


class TestWriteToBucket:
    """Test bucket writing functionality."""

    @pytest.mark.asyncio
    async def test_write_to_bucket_success(self):
        """Test successful uploading to bucket."""
        mock_storage = AsyncMock()
        mock_storage.save_ocr_text_jsonl_from_file = AsyncMock(return_value="test-prefix/12345.jsonl")

        jsonl_file_path = "/path/to/12345.jsonl"
        barcode = "12345"

        await write_to_bucket(mock_storage, barcode, jsonl_file_path, verbose=True)

        mock_storage.save_ocr_text_jsonl_from_file.assert_called_once_with(barcode, jsonl_file_path)


class TestExtractCLIBasic:
    """Minimal CLI test coverage."""

    @patch("grin_to_s3.storage.factories.create_storage_from_config")
    @patch("grin_to_s3.storage.book_manager.BookManager")
    @patch("grin_to_s3.extract.__main__.extract_ocr_pages", new_callable=AsyncMock)
    @pytest.mark.asyncio
    async def test_extract_with_bucket_storage(self, mock_extract_to_file, mock_book_manager_class, mock_create_storage):
        """Test basic extract functionality with bucket storage."""

        mock_book_manager = AsyncMock()
        mock_book_manager.save_ocr_text_jsonl_from_file = AsyncMock(return_value="book12345.jsonl")
        mock_book_manager_class.return_value = mock_book_manager

        mock_storage = MagicMock()
        mock_create_storage.return_value = mock_storage

        result = await extract_single_archive(
            archive_path="/path/to/book12345.tar.gz",
            db_path="/tmp/test.db",
            session_id="test_session",
            book_manager=mock_book_manager,
            verbose=True,
        )

        assert result["success"] is True
        assert result["archive"] == "/path/to/book12345.tar.gz"
        mock_book_manager.save_ocr_text_jsonl_from_file.assert_called_once()
