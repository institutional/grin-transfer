"""
Integration tests for BookStorage full-text functionality.

Tests actual file writing to storage backends to ensure files reach correct bucket paths.
"""

import json
import tempfile
from pathlib import Path

import pytest

from grin_to_s3.storage.base import Storage, StorageConfig
from grin_to_s3.storage.book_storage import BookStorage
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
            full_storage = Storage(StorageConfig.local(str(full_dir)))
            
            book_storage = BookStorage(
                storage=raw_storage,
                full_text_storage=full_storage
            )
            
            text_pages = ["First page content", "Second page content", "Third page content"]
            barcode = "test12345"
            
            # Create temp JSONL file
            jsonl_file = Path(temp_dir) / f"{barcode}.jsonl"
            with open(jsonl_file, 'w', encoding='utf-8') as f:
                for page in text_pages:
                    f.write(json.dumps(page, ensure_ascii=False) + '\n')
            
            # This would be async in real usage, but we'll test the sync version
            import asyncio
            
            async def test_save():
                return await book_storage.save_ocr_text_jsonl_from_file(barcode, str(jsonl_file))
            
            # Run the async test
            result_path = asyncio.run(test_save())
            
            # Verify file was written to full bucket (not raw bucket)
            expected_path = full_dir / f"{barcode}.jsonl"
            assert expected_path.exists()
            
            # Verify file was NOT written to raw bucket
            raw_path = raw_dir / f"{barcode}.jsonl"
            assert not raw_path.exists()
            
            # Verify JSONL content is correct
            with open(expected_path, 'r', encoding='utf-8') as f:
                content = f.read()
            
            lines = content.strip().split('\n')
            assert len(lines) == 3
            assert json.loads(lines[0]) == "First page content"
            assert json.loads(lines[1]) == "Second page content"
            assert json.loads(lines[2]) == "Third page content"
            
            # Verify returned path matches expectation
            assert result_path == f"{barcode}.jsonl"

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
            full_storage = Storage(StorageConfig.local(str(full_dir)))
            
            base_prefix = "test-collection"
            book_storage = BookStorage(
                storage=raw_storage,
                base_prefix=base_prefix,
                full_text_storage=full_storage
            )
            
            text_pages = ["Page with prefix content"]
            barcode = "prefix12345"
            
            # Create temp JSONL file
            jsonl_file = Path(temp_dir) / f"{barcode}.jsonl"
            with open(jsonl_file, 'w', encoding='utf-8') as f:
                for page in text_pages:
                    f.write(json.dumps(page, ensure_ascii=False) + '\n')
            
            import asyncio
            
            async def test_save():
                return await book_storage.save_ocr_text_jsonl_from_file(barcode, str(jsonl_file))
            
            result_path = asyncio.run(test_save())
            
            # Verify file was written to correct path with prefix
            expected_path = full_dir / base_prefix / f"{barcode}.jsonl"
            assert expected_path.exists()
            
            # Verify JSONL content
            with open(expected_path, 'r', encoding='utf-8') as f:
                content = f.read()
            
            assert content.strip() == '"Page with prefix content"'
            
            # Verify returned path includes prefix
            assert result_path == f"{base_prefix}/{barcode}.jsonl"

    def test_save_ocr_text_jsonl_unicode_integration(self):
        """Test OCR text JSONL saving with Unicode content using local storage."""
        with tempfile.TemporaryDirectory() as temp_dir:
            full_dir = Path(temp_dir) / "full"
            full_dir.mkdir()
            
            raw_storage = Storage(StorageConfig.local(temp_dir))
            full_storage = Storage(StorageConfig.local(str(full_dir)))
            
            book_storage = BookStorage(
                storage=raw_storage,
                full_text_storage=full_storage
            )
            
            text_pages = [
                "English text",
                "Français avec accents: àéîôû",
                "日本語のテキスト",
                "Emoji content: 🚀📚🔍",
                "Math symbols: ∑∫∂∇"
            ]
            barcode = "unicode12345"
            
            # Create temp JSONL file
            jsonl_file = Path(temp_dir) / f"{barcode}.jsonl"
            with open(jsonl_file, 'w', encoding='utf-8') as f:
                for page in text_pages:
                    f.write(json.dumps(page, ensure_ascii=False) + '\n')
            
            import asyncio
            
            async def test_save():
                return await book_storage.save_ocr_text_jsonl_from_file(barcode, str(jsonl_file))
            
            result_path = asyncio.run(test_save())
            
            # Verify file was written
            expected_path = full_dir / f"{barcode}.jsonl"
            assert expected_path.exists()
            
            # Verify Unicode content is preserved
            with open(expected_path, 'r', encoding='utf-8') as f:
                content = f.read()
            
            lines = content.strip().split('\n')
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
            full_storage = Storage(StorageConfig.local(str(full_dir)))
            
            book_storage = BookStorage(
                storage=raw_storage,
                full_text_storage=full_storage
            )
            
            text_pages = []
            barcode = "empty12345"
            
            # Create temp JSONL file (empty)
            jsonl_file = Path(temp_dir) / f"{barcode}.jsonl"
            with open(jsonl_file, 'w', encoding='utf-8') as f:
                for page in text_pages:
                    f.write(json.dumps(page, ensure_ascii=False) + '\n')
            
            import asyncio
            
            async def test_save():
                return await book_storage.save_ocr_text_jsonl_from_file(barcode, str(jsonl_file))
            
            result_path = asyncio.run(test_save())
            
            # Verify file was written
            expected_path = full_dir / f"{barcode}.jsonl"
            assert expected_path.exists()
            
            # Verify file is empty
            with open(expected_path, 'r', encoding='utf-8') as f:
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
            full_storage = Storage(StorageConfig.local(str(full_dir)))
            
            book_storage = BookStorage(
                storage=raw_storage,
                full_text_storage=full_storage
            )
            
            barcode = "multi12345"
            text_pages = ["Page 1", "Page 2"]
            
            # Create temp JSONL file
            jsonl_file = Path(temp_dir) / f"{barcode}.jsonl"
            with open(jsonl_file, 'w', encoding='utf-8') as f:
                for page in text_pages:
                    f.write(json.dumps(page, ensure_ascii=False) + '\n')
            
            import asyncio
            
            async def test_multiple_saves():
                # Save OCR text to full bucket
                ocr_path = await book_storage.save_ocr_text_jsonl_from_file(barcode, str(jsonl_file))
                
                # Save regular text JSONL to raw bucket (existing method)
                regular_path = await book_storage.save_text_jsonl(barcode, text_pages)
                
                # Save timestamp to raw bucket (existing method)
                timestamp_path = await book_storage.save_timestamp(barcode)
                
                return ocr_path, regular_path, timestamp_path
            
            ocr_path, regular_path, timestamp_path = asyncio.run(test_multiple_saves())
            
            # Verify OCR text went to full bucket
            full_jsonl_path = full_dir / f"{barcode}.jsonl"
            assert full_jsonl_path.exists()
            
            # Verify regular text went to raw bucket
            raw_jsonl_path = raw_dir / barcode / f"{barcode}.jsonl"
            assert raw_jsonl_path.exists()
            
            # Verify timestamp went to raw bucket
            timestamp_file_path = raw_dir / barcode / f"{barcode}.tar.gz.gpg.retrieval"
            assert timestamp_file_path.exists()
            
            # Verify contents are the same for both JSONL files
            with open(full_jsonl_path, 'r') as f:
                full_content = f.read()
            with open(raw_jsonl_path, 'r') as f:
                raw_content = f.read()
            
            assert full_content == raw_content
            expected_content = '"Page 1"\n"Page 2"\n'
            assert full_content == expected_content

    @pytest.mark.parametrize("storage_type,config", [
        ("minio", {
            "bucket_raw": "test-raw-bucket",
            "bucket_meta": "test-meta-bucket", 
            "bucket_full": "test-full-bucket",
            "endpoint_url": "http://localhost:9000",
            "access_key": "minioadmin",
            "secret_key": "minioadmin123"
        }),
    ])
    def test_create_book_storage_factory_integration(self, storage_type, config):
        """Test creating BookStorage with factory functions (mocked for CI)."""
        # This test validates the factory function works but uses mocks for CI compatibility
        from unittest.mock import patch, MagicMock
        
        with patch('grin_to_s3.storage.factories.create_storage_for_bucket') as mock_create:
            mock_raw = MagicMock()
            mock_meta = MagicMock()
            mock_full = MagicMock()
            mock_create.side_effect = [mock_raw, mock_meta, mock_full]
            
            book_storage = create_book_storage_with_full_text(storage_type, config, "test-prefix")
            
            # Verify factory was called correctly
            assert mock_create.call_count == 3
            mock_create.assert_any_call(storage_type, config, "test-raw-bucket")
            mock_create.assert_any_call(storage_type, config, "test-meta-bucket")
            mock_create.assert_any_call(storage_type, config, "test-full-bucket")
            
            # Verify BookStorage was configured correctly
            assert isinstance(book_storage, BookStorage)
            assert book_storage.storage == mock_raw
            assert book_storage.full_text_storage == mock_full
            assert book_storage.base_prefix == "test-prefix"