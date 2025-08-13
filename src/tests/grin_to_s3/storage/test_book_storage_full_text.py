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
from tests.test_utils.unified_mocks import create_book_manager_mock, standard_bucket_config, standard_storage_config


class TestBookManagerFullText:
    """Test BookManager with full-text storage functionality."""

    def test_init_with_bucket_config(self):
        """Test BookManager initialization with bucket configuration."""
        book_manager = create_book_manager_mock(bucket_config=standard_bucket_config(), base_prefix="test-prefix")

        assert book_manager.storage is not None
        assert book_manager.bucket_raw == "test-raw"
        assert book_manager.bucket_meta == "test-meta"
        assert book_manager.bucket_full == "test-full"
        assert book_manager.base_prefix == "test-prefix"

    def test_init_minimal_config(self):
        """Test BookManager initialization with minimal configuration."""
        book_manager = create_book_manager_mock(bucket_config=standard_bucket_config())

        assert book_manager.storage is not None
        assert book_manager.bucket_raw == "test-raw"
        assert book_manager.bucket_meta == "test-meta"
        assert book_manager.bucket_full == "test-full"
        assert book_manager.base_prefix == ""

    def test_full_text_path_with_prefix(self):
        """Test _full_text_path method with base prefix."""
        mock_storage = MagicMock()
        book_manager = BookManager(
            storage=mock_storage, bucket_config=standard_bucket_config(), base_prefix="test-prefix"
        )

        path = book_manager._full_text_path("12345", "test.jsonl")
        assert path == "test-full/test-prefix/test.jsonl"

    def test_full_text_path_without_prefix(self):
        """Test _full_text_path method without base prefix."""
        mock_storage = MagicMock()
        book_manager = BookManager(storage=mock_storage, storage_config=standard_storage_config())

        path = book_manager._full_text_path("12345", "test.jsonl")
        assert path == "test-full/test.jsonl"

    @pytest.mark.asyncio
    async def test_save_ocr_text_jsonl_from_file_success(self):
        """Test successful OCR text JSONL file upload with compression."""
        import tempfile

        mock_storage = AsyncMock()
        mock_storage.is_s3_compatible = MagicMock(return_value=False)

        book_manager = BookManager(storage=mock_storage, storage_config=standard_storage_config())

        # Create a real temporary JSONL file for compression
        with tempfile.NamedTemporaryFile(mode="w", suffix=".jsonl", delete=False) as temp_file:
            temp_file.write('"Page 1 content"\n"Page 2 content"\n')
            temp_file.flush()
            jsonl_file_path = temp_file.name

        try:
            result = await book_manager.save_ocr_text_jsonl_from_file("12345", jsonl_file_path)

            # Verify correct compressed path generated
            assert result == "test-full/12345.jsonl.gz"

            # Verify write_file called with compressed file
            mock_storage.write_file.assert_called_once()
            args, kwargs = mock_storage.write_file.call_args
            assert args[0] == "test-full/12345.jsonl.gz"
            assert args[1].endswith(".gz")  # Should be a compressed temp file
        finally:
            # Clean up temp file
            import os

            os.unlink(jsonl_file_path)

    @pytest.mark.asyncio
    async def test_save_ocr_text_jsonl_from_file_with_metadata(self):
        """Test OCR text JSONL file upload with metadata on S3-compatible storage."""
        import tempfile

        mock_storage = AsyncMock()
        mock_storage.is_s3_compatible = MagicMock(return_value=True)

        book_manager = BookManager(storage=mock_storage, storage_config=standard_storage_config())

        # Create a real temporary JSONL file for compression
        jsonl_content = '"Page 1 content"\n"Page 2 content"\n'
        with tempfile.NamedTemporaryFile(mode="w", suffix=".jsonl", delete=False) as temp_file:
            temp_file.write(jsonl_content)
            temp_file.flush()
            jsonl_file_path = temp_file.name

        metadata = {"page_count": 2, "extraction_time_ms": 1500}

        try:
            result = await book_manager.save_ocr_text_jsonl_from_file("12345", jsonl_file_path, metadata)

            # Verify correct compressed path generated
            assert result == "test-full/12345.jsonl.gz"

            # Verify write_bytes_with_metadata called with compressed file and extended metadata
            mock_storage.write_bytes_with_metadata.assert_called_once()
            args, kwargs = mock_storage.write_bytes_with_metadata.call_args
            assert args[0] == "test-full/12345.jsonl.gz"
            # Verify original metadata plus compression info is included
            call_metadata = args[2]
            assert call_metadata["page_count"] == 2
            assert call_metadata["extraction_time_ms"] == 1500
            assert "original_size" in call_metadata
            assert "compressed_size" in call_metadata
            assert "compression_ratio" in call_metadata
        finally:
            # Clean up temp file
            import os

            os.unlink(jsonl_file_path)

    @pytest.mark.asyncio
    async def test_save_ocr_text_jsonl_from_file_no_full_text_storage(self):
        """Test OCR text JSONL upload fails when full-text storage not configured."""
        mock_storage = MagicMock()

        # This should fail because BookManager requires storage_config
        with pytest.raises(TypeError):
            BookManager(storage=mock_storage)  # Missing storage_config


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
            "endpoint_url": "https://test-account.r2.cloudflarestorage.com",
            "access_key": "test-key",
            "secret_key": "test-secret",
        }

        config = {"credentials_file": "/path/to/creds.json"}

        result = create_storage_for_bucket("r2", config, "test-bucket")

        mock_load_creds.assert_called_once_with("/path/to/creds.json")
        mock_create_r2.assert_called_once_with(
            endpoint_url="https://test-account.r2.cloudflarestorage.com",
            access_key="test-key",
            secret_key="test-secret",
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

        storage_config = {
            "type": "s3",
            "protocol": "s3",
            "config": {"bucket_raw": "raw-bucket", "bucket_meta": "meta-bucket", "bucket_full": "full-bucket"},
            "prefix": "",
        }

        book_manager = create_book_manager_with_full_text(storage_config, "test-prefix")

        # Should be called with the storage config directly
        expected_config = storage_config
        mock_create_storage.assert_called_once_with(expected_config)

        assert isinstance(book_manager, BookManager)
        assert book_manager.storage == mock_storage
        assert book_manager.bucket_raw == "raw-bucket"
        assert book_manager.bucket_meta == "meta-bucket"
        assert book_manager.bucket_full == "full-bucket"
        assert book_manager.base_prefix == "test-prefix"
