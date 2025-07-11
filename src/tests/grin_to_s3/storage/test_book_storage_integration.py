"""
Integration tests for BookStorage full-text functionality.

Tests actual file writing to storage backends to ensure files reach correct bucket paths.
"""

import asyncio
import json
import tempfile
from pathlib import Path

import pytest

from grin_to_s3.storage.base import Storage, StorageConfig
from grin_to_s3.storage.book_manager import BookManager
from grin_to_s3.storage.factories import create_book_storage_with_full_text


class TestBookStorageIntegration:
    """Integration tests with actual storage backends."""

    def test_save_ocr_text_jsonl_local_integration(self):
        """Test OCR text JSONL saving with local storage backend."""
        with tempfile.TemporaryDirectory() as temp_dir:
            # Create separate directories for raw and full buckets
            raw_dir = Path(temp_dir) / "raw"
            full_dir = Path(temp_dir) / "full"
            raw_dir.mkdir()
            full_dir.mkdir()

            # Create storage instances for raw and full buckets
            raw_storage = Storage(StorageConfig.local(str(raw_dir)))

            bucket_config = {"bucket_raw": "raw", "bucket_meta": "meta", "bucket_full": "full"}
            book_storage = BookManager(storage=raw_storage, bucket_config=bucket_config)

            text_pages = ["First page content", "Second page content", "Third page content"]
            barcode = "test12345"

            # Create temp JSONL file
            jsonl_file = Path(temp_dir) / f"{barcode}.jsonl"
            with open(jsonl_file, "w", encoding="utf-8") as f:
                for page in text_pages:
                    f.write(json.dumps(page, ensure_ascii=False) + "\n")

            # This would be async in real usage, but we'll test the sync version
            async def test_save():
                return await book_storage.save_ocr_text_jsonl_from_file(barcode, str(jsonl_file))

            # Run the async test
            result_path = asyncio.run(test_save())

            # Verify file was written to the full bucket path within raw storage
            expected_path = raw_dir / "full" / f"{barcode}.jsonl"
            assert expected_path.exists()

            # Verify file was NOT written to raw bucket path
            raw_path = raw_dir / "raw" / f"{barcode}.jsonl"
            assert not raw_path.exists()

            # Verify JSONL content is correct
            with open(expected_path, encoding="utf-8") as f:
                content = f.read()

            lines = content.strip().split("\n")
            assert len(lines) == 3
            assert json.loads(lines[0]) == "First page content"
            assert json.loads(lines[1]) == "Second page content"
            assert json.loads(lines[2]) == "Third page content"

            # Verify returned path matches expectation (now includes bucket prefix)
            assert result_path == f"full/{barcode}.jsonl"

    def test_save_ocr_text_jsonl_with_prefix_local_integration(self):
        """Test OCR text JSONL saving with base prefix using local storage."""
        with tempfile.TemporaryDirectory() as temp_dir:
            # Create separate directories for raw and full buckets
            raw_dir = Path(temp_dir) / "raw"
            full_dir = Path(temp_dir) / "full"
            raw_dir.mkdir()
            full_dir.mkdir()

            # Create storage instances
            raw_storage = Storage(StorageConfig.local(str(raw_dir)))

            base_prefix = "test-collection"
            bucket_config = {"bucket_raw": "raw", "bucket_meta": "meta", "bucket_full": "full"}
            book_storage = BookManager(storage=raw_storage, bucket_config=bucket_config, base_prefix=base_prefix)

            text_pages = ["Page with prefix content"]
            barcode = "prefix12345"

            # Create temp JSONL file
            jsonl_file = Path(temp_dir) / f"{barcode}.jsonl"
            with open(jsonl_file, "w", encoding="utf-8") as f:
                for page in text_pages:
                    f.write(json.dumps(page, ensure_ascii=False) + "\n")

            async def test_save():
                return await book_storage.save_ocr_text_jsonl_from_file(barcode, str(jsonl_file))

            result_path = asyncio.run(test_save())

            # Verify file was written to correct path with prefix
            expected_path = raw_dir / "full" / base_prefix / f"{barcode}.jsonl"
            assert expected_path.exists()

            # Verify JSONL content
            with open(expected_path, encoding="utf-8") as f:
                content = f.read()

            assert content.strip() == '"Page with prefix content"'

            # Verify returned path includes prefix
            assert result_path == f"full/{base_prefix}/{barcode}.jsonl"

    def test_save_ocr_text_jsonl_unicode_integration(self):
        """Test OCR text JSONL saving with Unicode content using local storage."""
        with tempfile.TemporaryDirectory() as temp_dir:
            full_dir = Path(temp_dir) / "full"
            full_dir.mkdir()

            raw_storage = Storage(StorageConfig.local(temp_dir))

            bucket_config = {"bucket_raw": "raw", "bucket_meta": "meta", "bucket_full": "full"}
            book_storage = BookManager(storage=raw_storage, bucket_config=bucket_config)

            text_pages = [
                "English text",
                "Français avec accents: àéîôû",
                "日本語のテキスト",
                "Emoji content: 🚀📚🔍",
                "Math symbols: ∑∫∂∇",
            ]
            barcode = "unicode12345"

            # Create temp JSONL file
            jsonl_file = Path(temp_dir) / f"{barcode}.jsonl"
            with open(jsonl_file, "w", encoding="utf-8") as f:
                for page in text_pages:
                    f.write(json.dumps(page, ensure_ascii=False) + "\n")

            async def test_save():
                return await book_storage.save_ocr_text_jsonl_from_file(barcode, str(jsonl_file))

            asyncio.run(test_save())

            # Verify file was written
            expected_path = Path(temp_dir) / "full" / f"{barcode}.jsonl"
            assert expected_path.exists()

            # Verify Unicode content is preserved
            with open(expected_path, encoding="utf-8") as f:
                content = f.read()

            lines = content.strip().split("\n")
            assert len(lines) == 5

            # Test each line can be parsed as JSON and contains original Unicode
            parsed_pages = [json.loads(line) for line in lines]
            assert parsed_pages == text_pages

    def test_save_ocr_text_empty_pages_integration(self):
        """Test OCR text JSONL saving with empty pages list using local storage."""
        with tempfile.TemporaryDirectory() as temp_dir:
            full_dir = Path(temp_dir) / "full"
            full_dir.mkdir()

            raw_storage = Storage(StorageConfig.local(temp_dir))

            bucket_config = {"bucket_raw": "raw", "bucket_meta": "meta", "bucket_full": "full"}
            book_storage = BookManager(storage=raw_storage, bucket_config=bucket_config)

            text_pages = []
            barcode = "empty12345"

            # Create temp JSONL file (empty)
            jsonl_file = Path(temp_dir) / f"{barcode}.jsonl"
            with open(jsonl_file, "w", encoding="utf-8") as f:
                for page in text_pages:
                    f.write(json.dumps(page, ensure_ascii=False) + "\n")

            async def test_save():
                return await book_storage.save_ocr_text_jsonl_from_file(barcode, str(jsonl_file))

            asyncio.run(test_save())

            # Verify file was written
            expected_path = Path(temp_dir) / "full" / f"{barcode}.jsonl"
            assert expected_path.exists()

            # Verify file is empty
            with open(expected_path, encoding="utf-8") as f:
                content = f.read()

            assert content == ""

    def test_multiple_files_different_buckets_integration(self):
        """Test that files are written to correct buckets when multiple BookStorage methods are used."""
        with tempfile.TemporaryDirectory() as temp_dir:
            # Create separate directories for raw and full buckets
            raw_dir = Path(temp_dir) / "raw"
            full_dir = Path(temp_dir) / "full"
            raw_dir.mkdir()
            full_dir.mkdir()

            # Create storage instances
            raw_storage = Storage(StorageConfig.local(str(raw_dir)))

            bucket_config = {"bucket_raw": "raw", "bucket_meta": "meta", "bucket_full": "full"}
            book_storage = BookManager(storage=raw_storage, bucket_config=bucket_config)

            barcode = "multi12345"
            text_pages = ["Page 1", "Page 2"]

            # Create temp JSONL file
            jsonl_file = Path(temp_dir) / f"{barcode}.jsonl"
            with open(jsonl_file, "w", encoding="utf-8") as f:
                for page in text_pages:
                    f.write(json.dumps(page, ensure_ascii=False) + "\n")

            async def test_multiple_saves():
                # Save OCR text to full bucket
                ocr_path = await book_storage.save_ocr_text_jsonl_from_file(barcode, str(jsonl_file))

                # Save regular text JSONL to raw bucket (existing method)
                regular_path = await book_storage.save_text_jsonl(barcode, text_pages)

                # Save timestamp to raw bucket (existing method)
                timestamp_path = await book_storage.save_timestamp(barcode)

                return ocr_path, regular_path, timestamp_path

            ocr_path, regular_path, timestamp_path = asyncio.run(test_multiple_saves())

            # Verify OCR text went to full bucket path within raw storage
            full_jsonl_path = raw_dir / "full" / f"{barcode}.jsonl"
            assert full_jsonl_path.exists()

            # Verify regular text went to raw bucket path within raw storage
            raw_jsonl_path = raw_dir / "raw" / barcode / f"{barcode}.jsonl"
            assert raw_jsonl_path.exists()

            # Verify timestamp went to raw bucket path within raw storage
            timestamp_file_path = raw_dir / "raw" / barcode / f"{barcode}.tar.gz.gpg.retrieval"
            assert timestamp_file_path.exists()

            # Verify contents are the same for both JSONL files
            with open(full_jsonl_path) as f:
                full_content = f.read()
            with open(raw_jsonl_path) as f:
                raw_content = f.read()

            assert full_content == raw_content
            expected_content = '"Page 1"\n"Page 2"\n'
            assert full_content == expected_content

    @pytest.mark.parametrize(
        "storage_type,config",
        [
            (
                "minio",
                {
                    "bucket_raw": "test-raw-bucket",
                    "bucket_meta": "test-meta-bucket",
                    "bucket_full": "test-full-bucket",
                    "endpoint_url": "http://localhost:9000",
                    "access_key": "minioadmin",
                    "secret_key": "minioadmin123",
                },
            ),
        ],
    )
    def test_create_book_manager_factory_integration(self, storage_type, config):
        """Test creating BookStorage with factory functions (mocked for CI)."""
        # This test validates the factory function works but uses mocks for CI compatibility
        from unittest.mock import MagicMock, patch

        with patch("grin_to_s3.storage.factories.create_storage_from_config") as mock_create:
            mock_storage = MagicMock()
            mock_create.return_value = mock_storage

            book_storage = create_book_storage_with_full_text(storage_type, config, "test-prefix")

            # Verify factory was called correctly (now uses single storage)
            mock_create.assert_called_once_with(storage_type, config)

            # Verify BookStorage was configured correctly
            assert isinstance(book_storage, BookManager)
            assert book_storage.storage == mock_storage
            assert book_storage.bucket_raw == "test-raw-bucket"
            assert book_storage.bucket_meta == "test-meta-bucket"
            assert book_storage.bucket_full == "test-full-bucket"
            assert book_storage.base_prefix == "test-prefix"

    def test_upload_csv_file_integration(self):
        """Test CSV file upload to metadata bucket with both latest and timestamped versions."""
        with tempfile.TemporaryDirectory() as temp_dir:
            # Create separate directories for meta bucket
            meta_dir = Path(temp_dir) / "meta"
            meta_dir.mkdir()

            # Create timestamped subdirectory
            timestamped_dir = meta_dir / "timestamped"
            timestamped_dir.mkdir()

            raw_storage = Storage(StorageConfig.local(temp_dir))
            bucket_config = {"bucket_raw": "raw", "bucket_meta": "meta", "bucket_full": "full"}
            book_storage = BookManager(storage=raw_storage, bucket_config=bucket_config)

            # Create a test CSV file
            csv_content = "barcode,title,author\nTEST001,Test Book,Test Author\nTEST002,Another Book,Another Author\n"
            csv_file = Path(temp_dir) / "test_books.csv"
            with open(csv_file, "w", encoding="utf-8") as f:
                f.write(csv_content)

            async def test_upload():
                return await book_storage.upload_csv_file(str(csv_file))

            latest_path, timestamped_path = asyncio.run(test_upload())

            # Verify both files were created
            latest_file = meta_dir / "books_latest.csv"
            assert latest_file.exists()

            # Find the timestamped file (we don't know exact timestamp)
            timestamped_files = list(timestamped_dir.glob("books_*.csv"))
            assert len(timestamped_files) == 1
            timestamped_file = timestamped_files[0]

            # Verify content in both files
            with open(latest_file, encoding="utf-8") as f:
                latest_content = f.read()
            assert latest_content == csv_content

            with open(timestamped_file, encoding="utf-8") as f:
                timestamped_content = f.read()
            assert timestamped_content == csv_content

            # Verify returned paths
            assert latest_path == "meta/books_latest.csv"
            assert timestamped_path.startswith("meta/timestamped/books_")
            assert timestamped_path.endswith(".csv")

    def test_upload_csv_file_custom_filename_integration(self):
        """Test CSV file upload with custom filename."""
        with tempfile.TemporaryDirectory() as temp_dir:
            meta_dir = Path(temp_dir) / "meta"
            meta_dir.mkdir()
            timestamped_dir = meta_dir / "timestamped"
            timestamped_dir.mkdir()

            raw_storage = Storage(StorageConfig.local(temp_dir))
            bucket_config = {"bucket_raw": "raw", "bucket_meta": "meta", "bucket_full": "full"}
            book_storage = BookManager(storage=raw_storage, bucket_config=bucket_config)

            # Create a test CSV file
            csv_content = "barcode,title\nTEST001,Custom Export\n"
            csv_file = Path(temp_dir) / "custom_export.csv"
            with open(csv_file, "w", encoding="utf-8") as f:
                f.write(csv_content)

            async def test_upload():
                return await book_storage.upload_csv_file(str(csv_file), "custom_books.csv")

            latest_path, timestamped_path = asyncio.run(test_upload())

            # Verify custom filename was used for latest
            custom_file = meta_dir / "custom_books.csv"
            assert custom_file.exists()

            with open(custom_file, encoding="utf-8") as f:
                content = f.read()
            assert content == csv_content

            # Verify returned paths use custom filename
            assert latest_path == "meta/custom_books.csv"
            assert timestamped_path.startswith("meta/timestamped/books_")  # Timestamped always uses books_ prefix

    def test_save_ocr_text_jsonl_cloud_storage_integration(self):
        """Test OCR text JSONL saving with S3-compatible cloud storage (mocked)."""
        from unittest.mock import AsyncMock, MagicMock

        with tempfile.TemporaryDirectory() as temp_dir:
            # Create mock storage for cloud backends
            mock_storage = MagicMock()
            mock_storage.is_s3_compatible.return_value = True

            # Mock the async storage methods
            mock_storage.write_file = AsyncMock(return_value=None)
            mock_storage.write_bytes_with_metadata = AsyncMock(return_value="test-full/test12345.jsonl")

            bucket_config = {"bucket_raw": "test-raw", "bucket_meta": "test-meta", "bucket_full": "test-full"}
            book_storage = BookManager(storage=mock_storage, bucket_config=bucket_config)

            text_pages = ["First page content", "Second page content", "Third page content"]
            barcode = "test12345"

            # Create temp JSONL file
            jsonl_file = Path(temp_dir) / f"{barcode}.jsonl"
            with open(jsonl_file, "w", encoding="utf-8") as f:
                for page in text_pages:
                    f.write(json.dumps(page, ensure_ascii=False) + "\n")

            async def test_save():
                return await book_storage.save_ocr_text_jsonl_from_file(barcode, str(jsonl_file))

            # Run the async test
            result_path = asyncio.run(test_save())

            # Verify the storage write method was called
            mock_storage.write_file.assert_called_once()

            # Verify the call arguments
            call_args = mock_storage.write_file.call_args
            assert call_args[0][0] == f"test-full/{barcode}.jsonl"  # path argument
            assert str(jsonl_file) in str(call_args[0][1])  # file path argument

            # Verify returned path (should include bucket prefix)
            assert result_path == f"test-full/{barcode}.jsonl"
