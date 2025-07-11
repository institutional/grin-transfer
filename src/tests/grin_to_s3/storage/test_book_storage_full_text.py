"""
Unit tests for BookManager full-text functionality.

Tests the three-bucket storage system with full-text OCR storage capabilities.
"""

from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from grin_to_s3.storage.book_manager import BookManager
from grin_to_s3.storage.factories import (
    create_book_manager_with_full_text,
    create_storage_for_bucket,
)
from tests.test_utils.unified_mocks import MockStorageFactory, standard_bucket_config


class TestBookManagerFullText:
    """Test BookManager with full-text storage functionality."""

    def test_init_with_bucket_config(self):
        """Test BookManager initialization with bucket configuration."""
        book_manager = MockStorageFactory.create_book_manager(
            bucket_config=standard_bucket_config(),
            base_prefix="test-prefix"
        )

        assert book_manager.storage is not None
        assert book_manager.bucket_raw == "test-raw"
        assert book_manager.bucket_meta == "test-meta"
        assert book_manager.bucket_full == "test-full"
        assert book_manager.base_prefix == "test-prefix"

    def test_init_minimal_config(self):
        """Test BookManager initialization with minimal configuration."""
        book_manager = MockStorageFactory.create_book_manager(
            bucket_config=standard_bucket_config()
        )

        assert book_manager.storage is not None
        assert book_manager.bucket_raw == "test-raw"
        assert book_manager.bucket_meta == "test-meta"
        assert book_manager.bucket_full == "test-full"
        assert book_manager.base_prefix == ""

    def test_full_text_path_with_prefix(self):
        """Test _full_text_path method with base prefix."""
        mock_storage = MagicMock()
        book_manager = BookManager(
            storage=mock_storage,
            bucket_config=standard_bucket_config(),
            base_prefix="test-prefix"
        )

        path = book_manager._full_text_path("12345", "test.jsonl")
        assert path == "test-full/test-prefix/test.jsonl"

    def test_full_text_path_without_prefix(self):
        """Test _full_text_path method without base prefix."""
        mock_storage = MagicMock()
        book_manager = BookManager(
            storage=mock_storage,
            bucket_config=standard_bucket_config()
        )

        path = book_manager._full_text_path("12345", "test.jsonl")
        assert path == "test-full/test.jsonl"

    @pytest.mark.asyncio
    async def test_save_ocr_text_jsonl_from_file_success(self):
        """Test successful OCR text JSONL file upload."""
        mock_storage = MagicMock()
        mock_storage = AsyncMock()
        mock_storage.is_s3_compatible = MagicMock(return_value=False)

        book_manager = BookManager(storage=mock_storage, bucket_config=standard_bucket_config())

        jsonl_file_path = "/path/to/12345.jsonl"

        result = await book_manager.save_ocr_text_jsonl_from_file("12345", jsonl_file_path)

        # Verify correct path generated
        assert result == "test-full/12345.jsonl"

        # Verify write_file called with correct file path
        mock_storage.write_file.assert_called_once_with("test-full/12345.jsonl", jsonl_file_path)

    @pytest.mark.asyncio
    async def test_save_ocr_text_jsonl_from_file_with_metadata(self):
        """Test OCR text JSONL file upload with metadata on S3-compatible storage."""
        mock_storage = MagicMock()
        mock_storage = AsyncMock()
        mock_storage.is_s3_compatible = MagicMock(return_value=True)

        book_manager = BookManager(storage=mock_storage, bucket_config=standard_bucket_config())

        jsonl_file_path = "/path/to/12345.jsonl"
        metadata = {"page_count": 2, "extraction_time_ms": 1500}

        # Mock file reading
        with patch("aiofiles.open") as mock_open:
            mock_file = AsyncMock()
            mock_file.read.return_value = b'"Page 1 content"\n"Page 2 content"\n'
            mock_open.return_value.__aenter__.return_value = mock_file

            result = await book_manager.save_ocr_text_jsonl_from_file("12345", jsonl_file_path, metadata)

        # Verify correct path generated
        assert result == "test-full/12345.jsonl"

        # Verify write_bytes_with_metadata called
        expected_bytes = b'"Page 1 content"\n"Page 2 content"\n'
        mock_storage.write_bytes_with_metadata.assert_called_once_with(
            "test-full/12345.jsonl", expected_bytes, metadata
        )

    @pytest.mark.asyncio
    async def test_save_ocr_text_jsonl_from_file_no_full_text_storage(self):
        """Test OCR text JSONL upload fails when full-text storage not configured."""
        mock_storage = MagicMock()

        # This should fail because BookManager requires bucket_config
        with pytest.raises(TypeError):
            BookManager(storage=mock_storage)  # Missing bucket_config


class TestStorageFactories:
    """Test storage factory functions for three-bucket configuration."""

    @patch("grin_to_s3.storage.factories.create_minio_storage")
    def test_create_storage_for_bucket_minio(self, mock_create_minio):
        """Test creating storage for MinIO bucket."""
        mock_storage = MagicMock()
        mock_create_minio.return_value = mock_storage

        config = {"endpoint_url": "http://localhost:9000", "access_key": "testuser", "secret_key": "testpass"}

        result = create_storage_for_bucket("minio", config, "test-bucket")

        mock_create_minio.assert_called_once_with(
            endpoint_url="http://localhost:9000", access_key="testuser", secret_key="testpass"
        )
        assert result == mock_storage

    @patch("grin_to_s3.storage.factories.create_s3_storage")
    def test_create_storage_for_bucket_s3(self, mock_create_s3):
        """Test creating storage for S3 bucket."""
        mock_storage = MagicMock()
        mock_create_s3.return_value = mock_storage

        config = {}

        result = create_storage_for_bucket("s3", config, "test-bucket")

        mock_create_s3.assert_called_once_with(bucket="test-bucket")
        assert result == mock_storage

    @patch("grin_to_s3.storage.factories.load_json_credentials")
    @patch("grin_to_s3.storage.factories.create_r2_storage")
    def test_create_storage_for_bucket_r2(self, mock_create_r2, mock_load_creds):
        """Test creating storage for R2 bucket."""
        mock_storage = MagicMock()
        mock_create_r2.return_value = mock_storage
        mock_load_creds.return_value = {
            "account_id": "test-account",
            "access_key": "test-key",
            "secret_key": "test-secret",
        }

        config = {"credentials_file": "/path/to/creds.json"}

        result = create_storage_for_bucket("r2", config, "test-bucket")

        mock_load_creds.assert_called_once_with("/path/to/creds.json")
        mock_create_r2.assert_called_once_with(
            account_id="test-account", access_key="test-key", secret_key="test-secret"
        )
        assert result == mock_storage

    def test_create_storage_for_bucket_unsupported(self):
        """Test creating storage for unsupported storage type."""
        config = {}

        with pytest.raises(ValueError, match="does not support bucket-based storage"):
            create_storage_for_bucket("local", config, "test-bucket")

    @patch("grin_to_s3.storage.factories.create_storage_from_config")
    def test_create_book_manager_with_full_text(self, mock_create_storage):
        """Test creating BookManager with full-text support."""
        mock_storage = MagicMock()
        mock_create_storage.return_value = mock_storage

        config = {"bucket_raw": "raw-bucket", "bucket_meta": "meta-bucket", "bucket_full": "full-bucket"}

        book_manager = create_book_manager_with_full_text("s3", config, "test-prefix")

        mock_create_storage.assert_called_once_with("s3", config)

        assert isinstance(book_manager, BookManager)
        assert book_manager.storage == mock_storage
        assert book_manager.bucket_raw == "raw-bucket"
        assert book_manager.bucket_meta == "meta-bucket"
        assert book_manager.bucket_full == "full-bucket"
        assert book_manager.base_prefix == "test-prefix"
