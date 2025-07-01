"""
Unit tests for BookStorage full-text functionality.

Tests the three-bucket storage system with full-text OCR storage capabilities.
"""

import json
import pytest
from unittest.mock import AsyncMock, MagicMock, patch

from grin_to_s3.storage.book_storage import BookStorage
from grin_to_s3.storage.factories import (
    create_book_storage_with_full_text,
    create_storage_for_bucket,
    create_three_bucket_storage,
)


class TestBookStorageFullText:
    """Test BookStorage with full-text storage functionality."""

    def test_init_with_full_text_storage(self):
        """Test BookStorage initialization with full-text storage."""
        mock_storage = MagicMock()
        mock_full_text_storage = MagicMock()
        
        book_storage = BookStorage(
            storage=mock_storage,
            base_prefix="test-prefix",
            full_text_storage=mock_full_text_storage
        )
        
        assert book_storage.storage == mock_storage
        assert book_storage.full_text_storage == mock_full_text_storage
        assert book_storage.base_prefix == "test-prefix"

    def test_init_without_full_text_storage(self):
        """Test BookStorage initialization without full-text storage."""
        mock_storage = MagicMock()
        
        book_storage = BookStorage(storage=mock_storage)
        
        assert book_storage.storage == mock_storage
        assert book_storage.full_text_storage is None
        assert book_storage.base_prefix == ""

    def test_full_text_path_with_prefix(self):
        """Test _full_text_path method with base prefix."""
        mock_storage = MagicMock()
        mock_full_text_storage = MagicMock()
        
        book_storage = BookStorage(
            storage=mock_storage,
            base_prefix="test-prefix",
            full_text_storage=mock_full_text_storage
        )
        
        path = book_storage._full_text_path("12345", "test.jsonl")
        assert path == "test-prefix/test.jsonl"

    def test_full_text_path_without_prefix(self):
        """Test _full_text_path method without base prefix."""
        mock_storage = MagicMock()
        mock_full_text_storage = MagicMock()
        
        book_storage = BookStorage(
            storage=mock_storage,
            full_text_storage=mock_full_text_storage
        )
        
        path = book_storage._full_text_path("12345", "test.jsonl")
        assert path == "test.jsonl"

    @pytest.mark.asyncio
    async def test_save_ocr_text_jsonl_from_file_success(self):
        """Test successful OCR text JSONL file upload."""
        mock_storage = MagicMock()
        mock_full_text_storage = AsyncMock()
        mock_full_text_storage.is_s3_compatible = MagicMock(return_value=False)
        
        book_storage = BookStorage(
            storage=mock_storage,
            full_text_storage=mock_full_text_storage
        )
        
        jsonl_file_path = "/path/to/12345.jsonl"
        
        result = await book_storage.save_ocr_text_jsonl_from_file("12345", jsonl_file_path)
        
        # Verify correct path generated
        assert result == "12345.jsonl"
        
        # Verify write_file called with correct file path
        mock_full_text_storage.write_file.assert_called_once_with(
            "12345.jsonl", jsonl_file_path
        )

    @pytest.mark.asyncio
    async def test_save_ocr_text_jsonl_from_file_with_metadata(self):
        """Test OCR text JSONL file upload with metadata on S3-compatible storage."""
        mock_storage = MagicMock()
        mock_full_text_storage = AsyncMock()
        mock_full_text_storage.is_s3_compatible = MagicMock(return_value=True)
        
        book_storage = BookStorage(
            storage=mock_storage,
            full_text_storage=mock_full_text_storage
        )
        
        jsonl_file_path = "/path/to/12345.jsonl"
        metadata = {"page_count": 2, "extraction_time_ms": 1500}
        
        # Mock file reading
        with patch('aiofiles.open') as mock_open:
            mock_file = AsyncMock()
            mock_file.read.return_value = b'"Page 1 content"\n"Page 2 content"\n'
            mock_open.return_value.__aenter__.return_value = mock_file
            
            result = await book_storage.save_ocr_text_jsonl_from_file("12345", jsonl_file_path, metadata)
        
        # Verify correct path generated
        assert result == "12345.jsonl"
        
        # Verify write_bytes_with_metadata called
        expected_bytes = b'"Page 1 content"\n"Page 2 content"\n'
        mock_full_text_storage.write_bytes_with_metadata.assert_called_once_with(
            "12345.jsonl", expected_bytes, metadata
        )

    @pytest.mark.asyncio
    async def test_save_ocr_text_jsonl_from_file_no_full_text_storage(self):
        """Test OCR text JSONL upload fails when full-text storage not configured."""
        mock_storage = MagicMock()
        
        book_storage = BookStorage(storage=mock_storage)  # No full_text_storage
        
        jsonl_file_path = "/path/to/12345.jsonl"
        
        with pytest.raises(ValueError, match="Full-text storage not configured"):
            await book_storage.save_ocr_text_jsonl_from_file("12345", jsonl_file_path)


class TestStorageFactories:
    """Test storage factory functions for three-bucket configuration."""

    @patch('grin_to_s3.storage.factories.create_minio_storage')
    def test_create_storage_for_bucket_minio(self, mock_create_minio):
        """Test creating storage for MinIO bucket."""
        mock_storage = MagicMock()
        mock_create_minio.return_value = mock_storage
        
        config = {
            "endpoint_url": "http://localhost:9000",
            "access_key": "testuser",
            "secret_key": "testpass"
        }
        
        result = create_storage_for_bucket("minio", config, "test-bucket")
        
        mock_create_minio.assert_called_once_with(
            endpoint_url="http://localhost:9000",
            access_key="testuser",
            secret_key="testpass",
            bucket="test-bucket"
        )
        assert result == mock_storage

    @patch('grin_to_s3.storage.factories.create_s3_storage')
    def test_create_storage_for_bucket_s3(self, mock_create_s3):
        """Test creating storage for S3 bucket."""
        mock_storage = MagicMock()
        mock_create_s3.return_value = mock_storage
        
        config = {}
        
        result = create_storage_for_bucket("s3", config, "test-bucket")
        
        mock_create_s3.assert_called_once_with(bucket="test-bucket")
        assert result == mock_storage

    @patch('grin_to_s3.storage.factories.load_json_credentials')
    @patch('grin_to_s3.storage.factories.create_r2_storage')
    def test_create_storage_for_bucket_r2(self, mock_create_r2, mock_load_creds):
        """Test creating storage for R2 bucket."""
        mock_storage = MagicMock()
        mock_create_r2.return_value = mock_storage
        mock_load_creds.return_value = {
            "account_id": "test-account",
            "access_key": "test-key",
            "secret_key": "test-secret"
        }
        
        config = {"credentials_file": "/path/to/creds.json"}
        
        result = create_storage_for_bucket("r2", config, "test-bucket")
        
        mock_load_creds.assert_called_once_with("/path/to/creds.json")
        mock_create_r2.assert_called_once_with(
            account_id="test-account",
            access_key="test-key",
            secret_key="test-secret",
            bucket="test-bucket"
        )
        assert result == mock_storage

    def test_create_storage_for_bucket_unsupported(self):
        """Test creating storage for unsupported storage type."""
        config = {}
        
        with pytest.raises(ValueError, match="does not support bucket-based storage"):
            create_storage_for_bucket("local", config, "test-bucket")

    @patch('grin_to_s3.storage.factories.create_storage_for_bucket')
    def test_create_three_bucket_storage_success(self, mock_create_storage):
        """Test creating three-bucket storage configuration."""
        mock_raw_storage = MagicMock()
        mock_meta_storage = MagicMock()
        mock_full_storage = MagicMock()
        
        mock_create_storage.side_effect = [mock_raw_storage, mock_meta_storage, mock_full_storage]
        
        config = {
            "bucket_raw": "raw-bucket",
            "bucket_meta": "meta-bucket", 
            "bucket_full": "full-bucket"
        }
        
        raw, meta, full = create_three_bucket_storage("s3", config)
        
        assert raw == mock_raw_storage
        assert meta == mock_meta_storage
        assert full == mock_full_storage
        
        # Verify all three buckets were created
        assert mock_create_storage.call_count == 3
        mock_create_storage.assert_any_call("s3", config, "raw-bucket")
        mock_create_storage.assert_any_call("s3", config, "meta-bucket")
        mock_create_storage.assert_any_call("s3", config, "full-bucket")

    def test_create_three_bucket_storage_missing_buckets(self):
        """Test three-bucket storage creation fails with missing bucket configuration."""
        config = {
            "bucket_raw": "raw-bucket",
            # Missing bucket_meta and bucket_full
        }
        
        with pytest.raises(ValueError, match="Missing required bucket configuration"):
            create_three_bucket_storage("s3", config)

    @patch('grin_to_s3.storage.factories.create_three_bucket_storage')
    def test_create_book_storage_with_full_text(self, mock_create_three_bucket):
        """Test creating BookStorage with full-text support."""
        mock_raw_storage = MagicMock()
        mock_meta_storage = MagicMock()
        mock_full_storage = MagicMock()
        
        mock_create_three_bucket.return_value = (mock_raw_storage, mock_meta_storage, mock_full_storage)
        
        config = {
            "bucket_raw": "raw-bucket",
            "bucket_meta": "meta-bucket",
            "bucket_full": "full-bucket"
        }
        
        book_storage = create_book_storage_with_full_text("s3", config, "test-prefix")
        
        mock_create_three_bucket.assert_called_once_with("s3", config, "test-prefix")
        
        assert isinstance(book_storage, BookStorage)
        assert book_storage.storage == mock_raw_storage
        assert book_storage.full_text_storage == mock_full_storage
        assert book_storage.base_prefix == "test-prefix"