"""
Unit tests for extract CLI bucket functionality.

Tests the new bucket configuration and automatic writing features.
"""

import sys
import tempfile
from pathlib import Path
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from grin_to_s3.extract.__main__ import validate_storage_config, write_to_bucket


class TestStorageConfigValidation:
    """Test storage configuration validation."""

    def test_validate_storage_config_no_storage(self):
        """Test validation when no storage is configured."""
        args = MagicMock(storage=None)
        result = validate_storage_config(args)
        assert result is None

    def test_validate_storage_config_missing_bucket_full(self):
        """Test validation fails when storage is specified but bucket_full is missing."""
        args = MagicMock(storage="r2", bucket_full=None)
        
        with pytest.raises(ValueError, match="--bucket-full is required when using storage"):
            validate_storage_config(args)

    def test_validate_storage_config_missing_bucket_raw(self):
        """Test validation fails when bucket_raw is missing."""
        args = MagicMock(
            storage="r2",
            bucket_full="my-full-bucket",
            bucket_raw=None,
            bucket_meta="my-meta-bucket",
            credentials_file=None
        )
        
        with pytest.raises(ValueError, match="--bucket-raw is required when using storage"):
            validate_storage_config(args)

    def test_validate_storage_config_missing_bucket_meta(self):
        """Test validation fails when bucket_meta is missing."""
        args = MagicMock(
            storage="r2",
            bucket_full="my-full-bucket",
            bucket_raw="my-raw-bucket",
            bucket_meta=None,
            credentials_file=None
        )
        
        with pytest.raises(ValueError, match="--bucket-meta is required when using storage"):
            validate_storage_config(args)

    def test_validate_storage_config_three_bucket(self):
        """Test validation with three-bucket configuration."""
        args = MagicMock(
            storage="r2",
            bucket_full="full-bucket",
            bucket_raw="raw-bucket", 
            bucket_meta="meta-bucket",
            credentials_file="/path/to/creds.json"
        )
        
        result = validate_storage_config(args)
        
        expected = {
            "bucket_full": "full-bucket",
            "bucket_raw": "raw-bucket",
            "bucket_meta": "meta-bucket",
            "credentials_file": "/path/to/creds.json"
        }
        assert result == expected

    def test_validate_storage_config_all_buckets_required(self):
        """Test that all three buckets are always required."""
        # This test is now covered by the individual missing bucket tests above
        pass

    def test_validate_storage_config_minio_with_endpoint(self):
        """Test validation with MinIO configuration."""
        args = MagicMock(
            storage="minio",
            bucket_full="minio-full-bucket",
            bucket_raw="minio-raw-bucket",
            bucket_meta="minio-meta-bucket",
            endpoint_url="http://localhost:9000",
            access_key="minioadmin",
            secret_key="minioadmin123"
        )
        
        result = validate_storage_config(args)
        
        expected = {
            "bucket_full": "minio-full-bucket",
            "bucket_raw": "minio-raw-bucket",
            "bucket_meta": "minio-meta-bucket",
            "endpoint_url": "http://localhost:9000",
            "access_key": "minioadmin",
            "secret_key": "minioadmin123"
        }
        assert result == expected

    def test_validate_storage_config_s3_basic(self):
        """Test validation with basic S3 configuration."""
        args = MagicMock(
            storage="s3",
            bucket_full="s3-full-bucket",
            bucket_raw="s3-raw-bucket",
            bucket_meta="s3-meta-bucket"
        )
        
        result = validate_storage_config(args)
        
        expected = {
            "bucket_full": "s3-full-bucket",
            "bucket_raw": "s3-raw-bucket",
            "bucket_meta": "s3-meta-bucket"
        }
        assert result == expected


class TestWriteToBucket:
    """Test bucket writing functionality."""

    @pytest.mark.asyncio
    async def test_write_to_bucket_success(self):
        """Test successful uploading to bucket."""
        mock_storage = AsyncMock()
        mock_storage.save_ocr_text_jsonl_from_file.return_value = "test-prefix/12345.jsonl"
        
        jsonl_file_path = "/path/to/12345.jsonl"
        barcode = "12345"
        
        await write_to_bucket(mock_storage, barcode, jsonl_file_path, verbose=True)
        
        mock_storage.save_ocr_text_jsonl_from_file.assert_called_once_with(barcode, jsonl_file_path)

    @pytest.mark.asyncio
    async def test_write_to_bucket_failure(self):
        """Test error handling when bucket uploading fails."""
        mock_storage = AsyncMock()
        mock_storage.save_ocr_text_jsonl_from_file.side_effect = Exception("Network error")
        
        jsonl_file_path = "/path/to/12345.jsonl"
        barcode = "12345"
        
        with pytest.raises(RuntimeError, match="Failed to upload to bucket: Network error"):
            await write_to_bucket(mock_storage, barcode, jsonl_file_path)


class TestExtractCLIIntegration:
    """Test CLI integration with bucket functionality."""

    @patch('grin_to_s3.storage.factories.create_book_storage_with_full_text')
    @patch('grin_to_s3.extract.__main__.extract_text_to_jsonl_file')
    @pytest.mark.asyncio
    async def test_extract_single_archive_with_bucket(self, mock_extract_to_file, mock_create_storage):
        """Test extract_single_archive with bucket storage only."""
        from grin_to_s3.extract.__main__ import extract_single_archive
        
        mock_book_storage = AsyncMock()
        mock_book_storage.save_ocr_text_jsonl_from_file.return_value = "book12345.jsonl"
        
        # Test extraction with bucket storage only (no file output)
        result = await extract_single_archive(
            archive_path="/path/to/book12345.tar.gz",
            book_storage=mock_book_storage,
            verbose=True
        )
        
        # Verify file extraction was called to temp file
        assert mock_extract_to_file.call_count == 1
        call_args = mock_extract_to_file.call_args
        assert call_args[0][0] == "/path/to/book12345.tar.gz"
        # Second arg should be a temp file path
        assert call_args[0][1].endswith('.jsonl')
        
        # Verify bucket upload was called
        mock_book_storage.save_ocr_text_jsonl_from_file.assert_called_once()
        upload_call_args = mock_book_storage.save_ocr_text_jsonl_from_file.call_args
        assert upload_call_args[0][0] == "book12345"  # barcode
        assert upload_call_args[0][1].endswith('.jsonl')  # temp file path
        
        # Verify result
        assert result["success"] is True
        assert result["archive"] == "/path/to/book12345.tar.gz"

    @patch('grin_to_s3.extract.__main__.extract_text_to_jsonl_file')
    @pytest.mark.asyncio
    async def test_extract_single_archive_bucket_and_file(self, mock_extract_to_file):
        """Test that file output and bucket storage are mutually exclusive."""
        from grin_to_s3.extract.__main__ import main
        from unittest.mock import MagicMock
        
        # Mock arguments for both file and bucket output
        with patch('argparse.ArgumentParser.parse_args') as mock_parse_args:
            mock_args = MagicMock()
            mock_args.storage = "r2"
            mock_args.bucket_full = "test-full-bucket"
            mock_args.bucket_raw = "test-raw-bucket"
            mock_args.bucket_meta = "test-meta-bucket"
            mock_args.storage_prefix = ""
            mock_args.credentials_file = None
            mock_args.output = "/path/to/output.jsonl"  # File output
            mock_args.output_dir = None
            mock_args.archives = ["/path/to/test.tar.gz"]
            mock_args.verbose = False
            mock_args.summary = False
            
            mock_parse_args.return_value = mock_args
            
            with patch('builtins.print') as mock_print:
                result = await main()
                
                # Should exit with error code 1 due to conflicting options
                assert result == 1
                
                # Should print error about conflicting options
                mock_print.assert_called_with(
                    "Error: Cannot specify both file output and bucket storage - choose one",
                    file=sys.stderr
                )

    @patch('grin_to_s3.extract.__main__.extract_text_to_jsonl_file')
    @pytest.mark.asyncio
    async def test_extract_single_archive_bucket_only_no_stdout(self, mock_extract_to_file):
        """Test that stdout output is suppressed when using bucket storage."""
        from grin_to_s3.extract.__main__ import extract_single_archive
        
        mock_extract_to_file.return_value = 1  # Return page count
        mock_book_storage = AsyncMock()
        
        with patch('builtins.print') as mock_print:
            result = await extract_single_archive(
                archive_path="/path/to/test.tar.gz",
                book_storage=mock_book_storage
            )
            
            # Verify no stdout printing of JSONL content when using bucket storage
            # Since we removed stdout support completely, there shouldn't be any JSON content printed
            print_calls = [call for call in mock_print.call_args_list 
                          if '"Test content"' in str(call)]
            assert len(print_calls) == 0

    @patch('argparse.ArgumentParser.parse_args')
    @patch('grin_to_s3.storage.factories.create_book_storage_with_full_text')
    @patch('grin_to_s3.extract.__main__.extract_single_archive')
    @pytest.mark.asyncio
    async def test_main_with_bucket_config(self, mock_extract_single, mock_create_storage, mock_parse_args):
        """Test main function with bucket configuration."""
        from grin_to_s3.extract.__main__ import main
        
        # Mock arguments
        mock_args = MagicMock()
        mock_args.storage = "r2"
        mock_args.bucket_full = "test-full-bucket"
        mock_args.bucket_raw = "test-raw-bucket"
        mock_args.bucket_meta = "test-meta-bucket"
        mock_args.storage_prefix = ""
        mock_args.credentials_file = None
        mock_args.output = None
        mock_args.output_dir = None
        mock_args.verbose = True
        mock_args.archives = ["/path/to/test.tar.gz"]
        mock_args.summary = False
        mock_args.extraction_dir = None
        mock_args.keep_extracted = False
        mock_args.use_disk = False
        mock_args.no_streaming = False
        
        mock_parse_args.return_value = mock_args
        
        # Mock storage creation
        mock_book_storage = AsyncMock()
        mock_create_storage.return_value = mock_book_storage
        
        # Mock extraction result
        mock_extract_single.return_value = {
            "success": True,
            "archive": "/path/to/test.tar.gz",
            "pages": 1
        }
        
        result = await main()
        
        # Verify storage was created with correct config
        expected_config = {
            "bucket_full": "test-full-bucket",
            "bucket_raw": "test-raw-bucket",
            "bucket_meta": "test-meta-bucket"
        }
        mock_create_storage.assert_called_once_with("r2", expected_config, "")
        
        # Verify extraction was called with bucket storage
        mock_extract_single.assert_called_once()
        call_kwargs = mock_extract_single.call_args[1]
        assert call_kwargs["book_storage"] == mock_book_storage
        
        # Verify success exit code
        assert result == 0

    @patch('argparse.ArgumentParser.parse_args')
    @pytest.mark.asyncio
    async def test_main_missing_output_method(self, mock_parse_args):
        """Test main function fails when no output method is specified."""
        from grin_to_s3.extract.__main__ import main
        
        mock_args = MagicMock()
        mock_args.storage = None
        mock_args.output = None
        mock_args.output_dir = None
        mock_args.archives = ["/path/to/test.tar.gz"]
        
        mock_parse_args.return_value = mock_args
        
        with patch('builtins.print') as mock_print:
            result = await main()
            
            # Verify error message and exit code
            mock_print.assert_any_call(
                "Error: Must specify either file output (--output/--output-dir) or bucket storage configuration",
                file=sys.stderr
            )
            assert result == 1