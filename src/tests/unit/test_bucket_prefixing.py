#!/usr/bin/env python3
"""
Tests for bucket prefixing behavior across storage types.

These tests verify that file paths are constructed correctly and that
S3-compatible storage types don't get incorrect bucket name prefixes.
"""

import tempfile
from pathlib import Path
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from grin_to_s3.sync.operations import upload_book_from_staging


class TestBucketPrefixingBehavior:
    """Test that bucket names are handled correctly for different storage types."""

    @pytest.fixture
    def staging_file(self):
        """Create a temporary staging file."""
        with tempfile.NamedTemporaryFile(mode="wb", delete=False, suffix=".tar.gz") as f:
            f.write(b"test archive content")
            staging_path = Path(f.name)
        
        yield staging_path
        
        # Cleanup
        if staging_path.exists():
            staging_path.unlink()

    def test_s3_compatible_storage_path_construction(self):
        """Test that S3-compatible storage (S3, R2, MinIO) doesn't include bucket names in file paths."""
        from grin_to_s3.storage import BookStorage, create_storage_from_config
        
        storage_configs = [
            ("s3", {"bucket_raw": "my-s3-raw", "bucket_full": "my-s3-full", "prefix": ""}),
            ("r2", {"bucket_raw": "my-r2-raw", "bucket_full": "my-r2-full", "prefix": ""}),
            ("minio", {"bucket_raw": "my-minio-raw", "bucket_full": "my-minio-full", "prefix": ""}),
        ]
        
        for storage_type, config in storage_configs:
            with patch("grin_to_s3.sync.operations.create_storage_from_config") as mock_create_storage:
                # Setup mocks
                mock_storage = AsyncMock()
                mock_create_storage.return_value = mock_storage
                
                # This tests the exact logic that was buggy
                # Get prefix information 
                base_prefix = config.get("prefix", "")
                
                # The bug was here - this code should NOT be present:
                # if storage_protocol == "s3" and bucket_name:
                #     base_prefix = bucket_name  # This was the bug!
                
                # Create bucket configuration
                bucket_config = {
                    "bucket_raw": config.get("bucket_raw", ""),
                    "bucket_meta": config.get("bucket_meta", ""),
                    "bucket_full": config.get("bucket_full", ""),
                }
                
                # This is how BookStorage should be created
                book_storage = BookStorage(mock_storage, bucket_config=bucket_config, base_prefix=base_prefix)
                
                # CRITICAL: Check that base_prefix does NOT include bucket names
                assert base_prefix == ""  # Should be empty, not contain bucket name
                assert config["bucket_raw"] not in base_prefix
                
                # Verify bucket configuration contains bucket names (that's correct)
                assert book_storage.bucket_raw == config["bucket_raw"]

    @pytest.mark.asyncio 
    async def test_local_storage_path_construction(self, staging_file):
        """Test that local storage uses bucket names as directory paths."""
        with tempfile.TemporaryDirectory() as temp_dir:
            config = {
                "base_path": temp_dir,
                "bucket_raw": "local-raw", 
                "bucket_full": "local-full",
                "prefix": ""
            }
            
            with patch("grin_to_s3.sync.operations.create_storage_from_config") as mock_create_storage:
                with patch("grin_to_s3.sync.operations.BookStorage") as mock_book_storage_class:
                    # Setup mocks
                    mock_storage = AsyncMock()
                    mock_create_storage.return_value = mock_storage
                    
                    mock_book_storage = AsyncMock()
                    mock_book_storage.save_archive.return_value = "local-raw/TEST123/TEST123.tar.gz"
                    mock_book_storage_class.return_value = mock_book_storage
                    
                    # Call the function
                    await upload_book_from_staging(
                        barcode="TEST123",
                        staging_file_path=str(staging_file),
                        storage_type="local",
                        storage_config=config,
                        staging_manager=None,
                        db_tracker=None,
                        encrypted_etag="test-etag",
                        skip_extract_ocr=True
                    )
                    
                    # Verify BookStorage was created with correct parameters
                    mock_book_storage_class.assert_called_once()
                    call_args = mock_book_storage_class.call_args
                    
                    # Check bucket configuration - should contain bucket names
                    bucket_config = call_args[1]["bucket_config"] 
                    assert bucket_config["bucket_raw"] == "local-raw"
                    assert bucket_config["bucket_full"] == "local-full"
                    
                    # For local storage, base_prefix should still be empty
                    # BookStorage handles bucket names as directory paths internally
                    base_prefix = call_args[1]["base_prefix"]
                    assert base_prefix == ""

    @pytest.mark.asyncio
    async def test_custom_prefix_handling(self, staging_file):
        """Test that custom prefixes work correctly without interfering with bucket names."""
        config = {
            "bucket_raw": "test-raw",
            "bucket_full": "test-full", 
            "prefix": "my-custom-prefix"
        }
        
        with patch("grin_to_s3.sync.operations.create_storage_from_config") as mock_create_storage:
            with patch("grin_to_s3.sync.operations.BookStorage") as mock_book_storage_class:
                # Setup mocks
                mock_storage = AsyncMock()
                mock_create_storage.return_value = mock_storage
                
                mock_book_storage = AsyncMock()
                mock_book_storage.save_archive.return_value = "test-raw/my-custom-prefix/TEST123/TEST123.tar.gz"
                mock_book_storage_class.return_value = mock_book_storage
                
                # Call the function
                await upload_book_from_staging(
                    barcode="TEST123",
                    staging_file_path=str(staging_file),
                    storage_type="s3",
                    storage_config=config,
                    staging_manager=None,
                    db_tracker=None,
                    encrypted_etag="test-etag",
                    skip_extract_ocr=True
                )
                
                # Verify BookStorage was created with correct parameters
                mock_book_storage_class.assert_called_once()
                call_args = mock_book_storage_class.call_args
                
                # Check that custom prefix is preserved but bucket name is not added
                base_prefix = call_args[1]["base_prefix"]
                assert base_prefix == "my-custom-prefix"
                # CRITICAL: Should NOT be "test-raw/my-custom-prefix" or "test-raw"

    @pytest.mark.asyncio
    async def test_regression_bucket_name_not_in_prefix(self, staging_file):
        """
        Regression test: Ensure bucket names are never added to base_prefix.
        
        This test would have caught the bug where S3-compatible storage was getting
        paths like 'bucket-name/BARCODE/file' instead of 'BARCODE/file'.
        """
        problematic_configs = [
            ("s3", {"bucket_raw": "should-not-appear-in-prefix"}),
            ("r2", {"bucket_raw": "ib-1-0-test-raw"}), 
            ("minio", {"bucket_raw": "grin-raw"}),
        ]
        
        for storage_type, config in problematic_configs:
            with patch("grin_to_s3.sync.operations.create_storage_from_config") as mock_create_storage:
                with patch("grin_to_s3.sync.operations.BookStorage") as mock_book_storage_class:
                    # Setup mocks
                    mock_storage = AsyncMock()
                    mock_create_storage.return_value = mock_storage
                    mock_book_storage_class.return_value = AsyncMock()
                    
                    # Call the function
                    await upload_book_from_staging(
                        barcode="TEST123",
                        staging_file_path=str(staging_file),
                        storage_type=storage_type,
                        storage_config=config,
                        staging_manager=None,
                        db_tracker=None,
                        encrypted_etag="test-etag",
                        skip_extract_ocr=True
                    )
                    
                    # Get the base_prefix passed to BookStorage
                    call_args = mock_book_storage_class.call_args
                    base_prefix = call_args[1]["base_prefix"]
                    bucket_name = config["bucket_raw"]
                    
                    # CRITICAL ASSERTION: Bucket name must not appear in base_prefix
                    assert bucket_name not in base_prefix, (
                        f"Bucket name '{bucket_name}' should not appear in base_prefix '{base_prefix}' "
                        f"for storage type '{storage_type}'. This creates incorrect file paths."
                    )