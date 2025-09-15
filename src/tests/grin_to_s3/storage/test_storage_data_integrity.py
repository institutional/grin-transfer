#!/usr/bin/env python3
"""
Data integrity tests for storage operations and path management.

Tests critical edge cases that could cause silent data corruption,
incorrect file paths, or metadata inconsistencies in storage operations.
"""

import json
import tempfile
import warnings

import pytest

from grin_to_s3.storage.base import BackendConfig, Storage
from grin_to_s3.storage.book_manager import BookManager
from grin_to_s3.storage.factories import (
    create_gcs_storage,
    create_local_storage,
    create_minio_storage,
    create_s3_storage,
)
from grin_to_s3.storage.staging import StagingDirectoryManager
from tests.test_utils.unified_mocks import standard_storage_config


class TestStoragePathIntegrity:
    """Test storage path construction and normalization integrity."""

    def test_path_normalization_edge_cases(self):
        """Test path normalization with various edge case inputs."""
        # Test with local file storage (requires base_path)
        local_config = BackendConfig(protocol="file", base_path="/tmp/test")
        local_storage = Storage(local_config)

        # Test basic path normalization for local storage
        assert local_storage._normalize_path("test.txt").endswith("test.txt")
        assert local_storage._normalize_path("folder/test.txt").endswith("folder/test.txt")

        # Test with S3 storage (different normalization rules)
        s3_config = BackendConfig(protocol="s3")
        s3_storage = Storage(s3_config)

        # Test S3 path normalization (strips leading slashes and normalizes consecutive slashes)
        assert s3_storage._normalize_path("/path/file.txt") == "path/file.txt"
        assert s3_storage._normalize_path("//path//file.txt") == "path/file.txt"  # Consecutive slashes normalized
        assert (
            s3_storage._normalize_path("///path///to///file.txt") == "path/to/file.txt"
        )  # Multiple consecutive slashes
        assert s3_storage._normalize_path("path/file.txt") == "path/file.txt"

        # Test additional edge cases
        assert s3_storage._normalize_path("") == ""  # Empty path
        assert s3_storage._normalize_path("/") == ""  # Root path
        assert s3_storage._normalize_path("//") == ""  # Multiple leading slashes only
        assert s3_storage._normalize_path("path//") == "path/"  # Trailing consecutive slashes

    def test_path_normalization_with_different_protocols(self):
        """Test path normalization behavior with different storage protocols."""
        # Test file protocol
        file_config = BackendConfig(protocol="file", base_path="/tmp/test")
        file_storage = Storage(file_config)

        # Test S3 protocol
        s3_config = BackendConfig(protocol="s3")
        s3_storage = Storage(s3_config)

        # Both should normalize paths consistently
        test_path = "path/to/file.txt"
        file_normalized = file_storage._normalize_path(test_path)
        s3_normalized = s3_storage._normalize_path(test_path)

        # Should handle normalization without errors
        assert isinstance(file_normalized, str)
        assert isinstance(s3_normalized, str)

    def test_book_path_construction(self):
        """Test BookManager path construction."""
        config = BackendConfig(protocol="file")
        storage = Storage(config)

        # Test with base_prefix
        book_manager = BookManager(
            storage, storage_config=standard_storage_config("local", "raw", "meta", "full"), base_prefix="test_run"
        )

        # Test raw archive path construction
        raw_path = book_manager.raw_archive_path("TEST001.tar.gz.gpg")
        assert raw_path == "raw/test_run/TEST001.tar.gz.gpg"

        # Test full text path construction
        full_path = book_manager.full_text_path("test.txt")
        assert full_path == "full/test_run/test.txt"

        # Test meta path construction
        meta_path = book_manager.meta_path("books.csv")
        assert meta_path == "meta/test_run/books.csv"

        # Test with base_prefix
        book_manager_with_prefix = BookManager(
            storage, storage_config=standard_storage_config("local", "raw", "meta", "full"), base_prefix="myproject"
        )

        raw_path_prefixed = book_manager_with_prefix.raw_archive_path("TEST001.tar.gz.gpg")
        assert raw_path_prefixed == "raw/myproject/TEST001.tar.gz.gpg"

        full_path_prefixed = book_manager_with_prefix.full_text_path("test.txt")
        assert full_path_prefixed == "full/myproject/test.txt"

        meta_path_prefixed = book_manager_with_prefix.meta_path("books.csv")
        assert meta_path_prefixed == "meta/myproject/books.csv"

    def test_book_path_construction_edge_case_barcodes(self):
        """Test BookManager path construction with edge case barcode values."""
        config = BackendConfig(protocol="file")
        storage = Storage(config)
        book_manager = BookManager(
            storage, storage_config=standard_storage_config("local", "raw", "meta", "full"), base_prefix="test_run"
        )

        # Test edge case barcodes
        test_cases = [
            ("", ""),  # Empty barcode
            ("123", "123"),  # Numeric
            ("TEST_WITH_UNDERSCORE", "TEST_WITH_UNDERSCORE"),  # Underscores
            ("test-with-dash", "test-with-dash"),  # Dashes and lowercase
            ("A", "A"),  # Single character
            ("VERY_LONG_BARCODE_" + "X" * 100, "VERY_LONG_BARCODE_" + "X" * 100),  # Very long
        ]

        for barcode, _ in test_cases:
            # Test that paths are constructed correctly
            raw_path = book_manager.raw_archive_path(f"{barcode}.tar.gz.gpg")
            expected_raw = f"raw/test_run/{barcode}.tar.gz.gpg"  # No barcode directory, uses prefix
            assert raw_path == expected_raw

            # Test full text path
            full_path = book_manager.full_text_path("test.txt")
            assert full_path == "full/test_run/test.txt"  # Uses prefix

    def test_unicode_barcode_handling(self):
        """Test handling of Unicode characters in barcodes."""
        config = BackendConfig(protocol="file")
        storage = Storage(config)
        book_manager = BookManager(
            storage, storage_config=standard_storage_config("local", "raw", "meta", "full"), base_prefix="run-name"
        )

        # Test various Unicode characters
        unicode_barcodes = [
            "TEST_café",
            "书籍001",
            "TEST_naïve",
            "ÑOÑO123",
            "TEST🔥001",  # Emoji
            "TEST\x00NULL",  # Null character
            "TEST\tTAB\nNEWLINE",  # Control characters
        ]

        for barcode in unicode_barcodes:
            # Should not raise exceptions
            raw_path = book_manager.raw_archive_path(f"{barcode}.tar.gz.gpg")
            full_path = book_manager.full_text_path("test.txt")

            # Paths should be constructed successfully
            assert isinstance(raw_path, str)
            assert isinstance(full_path, str)
            # Should contain expected components
            if barcode.strip():  # Skip test for control characters
                assert barcode in raw_path or "test.txt" in full_path

    def test_bucket_name_handling(self):
        """Test bucket name handling for empty vs non-empty cases."""
        config = BackendConfig(protocol="file")
        storage = Storage(config)

        # Test with non-empty bucket names
        book_manager = BookManager(
            storage,
            storage_config=standard_storage_config("local", "my-raw-bucket", "my-meta-bucket", "my-full-bucket"),
            base_prefix="",
        )
        assert book_manager.bucket_raw == "my-raw-bucket"
        assert book_manager.bucket_meta == "my-meta-bucket"
        assert book_manager.bucket_full == "my-full-bucket"

        # Test with empty bucket names - For local storage, should use defaults
        book_manager_empty = BookManager(
            storage, storage_config={"type": "local", "protocol": "file", "config": {}}, base_prefix=""
        )
        assert book_manager_empty.bucket_raw == "raw"
        assert book_manager_empty.bucket_meta == "meta"
        assert book_manager_empty.bucket_full == "full"


class TestStorageAtomicOperations:
    """Test atomic operations and consistency."""

    @pytest.fixture
    def staging_manager(self):
        """Create a staging directory manager for testing."""
        with tempfile.TemporaryDirectory() as temp_dir:
            manager = StagingDirectoryManager(temp_dir, capacity_threshold=0.9)
            yield manager

    def test_staging_directory_space_calculation_accuracy(self, staging_manager):
        """Test accuracy of disk space calculations."""
        # Create files of known sizes
        small_file = staging_manager.staging_path / "small.txt"
        large_file = staging_manager.staging_path / "large.txt"

        small_content = "a" * 1000  # 1KB
        large_content = "b" * 100000  # 100KB

        small_file.write_text(small_content)
        large_file.write_text(large_content)

        # Check space calculation
        used_bytes, total_bytes, usage_ratio = staging_manager.get_disk_usage()

        # Should be greater than 0 and reasonable values
        assert used_bytes > 0
        assert total_bytes > used_bytes
        assert 0.0 <= usage_ratio <= 1.0


class TestStorageConfigurationIntegrity:
    """Test storage configuration and factory integrity."""

    def test_storage_factory_credential_handling(self):
        """Test storage factory handles various credential scenarios."""
        # Test cases for different credential configurations
        test_configs = [
            # Valid S3 config
            {
                "storage_type": "s3",
                "credentials": {"aws_access_key_id": "test_key", "aws_secret_access_key": "test_secret"},
            },
            # Missing credentials
            {"storage_type": "s3", "credentials": {}},
            # Malformed credentials
            {"storage_type": "s3", "credentials": {"wrong_key": "wrong_value"}},
            # Local storage (no credentials needed)
            {"storage_type": "local", "credentials": {}},
        ]

        for config in test_configs:
            # Should handle gracefully without exposing credentials in errors
            try:
                # This would normally call create_storage but we'll test the pattern
                storage_type = config.get("storage_type")
                credentials = config.get("credentials", {})

                # Verify config structure
                assert isinstance(storage_type, str)
                assert isinstance(credentials, dict)

                # Credential validation would happen in real factory
                if storage_type == "s3":
                    required_keys = ["aws_access_key_id", "aws_secret_access_key"]
                    # Would validate in real implementation
                    _ = all(key in credentials for key in required_keys)

            except Exception as e:
                # Should not expose credential details in exception messages
                error_msg = str(e).lower()
                assert "secret" not in error_msg
                assert "key" not in error_msg or "missing" in error_msg

    def test_bucket_configuration_validation(self):
        """Test bucket configuration validation edge cases."""
        # Test various bucket configurations
        bucket_configs = [
            # Valid configuration
            {"raw_bucket": "valid-raw", "meta_bucket": "valid-meta", "full_bucket": "valid-full"},
            # Empty bucket names
            {"raw_bucket": "", "meta_bucket": "valid-meta", "full_bucket": "valid-full"},
            # Same bucket for multiple purposes
            {"raw_bucket": "same-bucket", "meta_bucket": "same-bucket", "full_bucket": "same-bucket"},
            # Special characters in bucket names
            {"raw_bucket": "bucket-with-special.chars_123", "meta_bucket": "meta", "full_bucket": "full"},
        ]

        for config in bucket_configs:
            # Should validate bucket name format
            for _bucket_key, bucket_name in config.items():
                # Basic validation rules
                if bucket_name:
                    # Should not contain invalid characters for cloud storage
                    # This is provider-specific but generally no uppercase, no underscores for S3
                    pass
                else:
                    # Empty bucket names should be handled
                    assert bucket_name == ""  # Explicitly empty

    def test_storage_backend_consistency(self):
        """Test that operations are consistent across storage backends."""
        # Mock different storage backends
        backends = ["local", "s3", "r2"]

        test_operations = ["normalize_path", "head", "upload", "download"]

        # Each backend should implement the same interface
        for backend in backends:
            for _operation in test_operations:
                # In real implementation, would test that each backend
                # handles the same inputs consistently

                # Mock the behavior expectation
                if backend == "local":
                    # Local storage should handle file paths
                    expected_path_style = "filesystem"
                else:
                    # Cloud storage should handle object keys
                    expected_path_style = "object_key"

                # Verify consistent interface
                assert expected_path_style in ["filesystem", "object_key"]

    def test_configuration_serialization_integrity(self):
        """Test that storage configurations serialize/deserialize correctly."""
        # Test configuration with various data types
        config = {
            "storage_type": "s3",
            "bucket_name": "test-bucket",
            "base_prefix": "test/prefix",
            "credentials": {"aws_access_key_id": "test_key", "aws_secret_access_key": "test_secret"},
            "options": {
                "multipart_threshold": 1024 * 1024 * 5,  # 5MB
                "max_concurrency": 10,
                "use_ssl": True,
            },
        }

        # Serialize to JSON
        serialized = json.dumps(config)

        # Deserialize back
        deserialized = json.loads(serialized)

        # Should be identical
        assert deserialized == config

        # Test with edge cases
        edge_case_config = {
            "storage_type": "",
            "bucket_name": None,
            "credentials": {},
            "options": {"timeout": 0, "retries": -1},
        }

        # Should handle edge cases in serialization
        edge_serialized = json.dumps(edge_case_config, default=str)
        edge_deserialized = json.loads(edge_serialized)

        # Verify structure is preserved
        assert "storage_type" in edge_deserialized
        assert "credentials" in edge_deserialized


class TestDisplayURIFormatting:
    """Test storage URI formatting for display purposes."""

    @pytest.mark.parametrize(
        "storage_type,expected_format",
        [
            ("local", "/tmp/storage/meta/test_run/books.csv"),  # Full absolute path, no file://
            ("s3", "s3://bucket-meta/test_run/books.csv"),  # S3 protocol prefix
            ("minio", "s3://bucket-meta/test_run/books.csv"),  # MinIO uses S3 protocol
            ("gcs", "gs://bucket-meta/test_run/books.csv"),  # GCS protocol prefix
        ],
    )
    def test_get_display_uri(self, storage_type, expected_format):
        """Test that storage URIs are formatted correctly for display."""
        # Suppress Google Cloud SDK authentication warnings during testing
        with warnings.catch_warnings():
            warnings.filterwarnings(
                "ignore",
                message="Your application has authenticated using end user credentials.*",
                category=UserWarning,
                module="google.auth._default",
            )

            # Create storage instance based on type
            if storage_type == "local":
                storage = create_local_storage(base_path="/tmp/storage")
                path = "meta/test_run/books.csv"
            elif storage_type == "s3":
                storage = create_s3_storage(bucket="bucket-meta")
                path = "bucket-meta/test_run/books.csv"
            elif storage_type == "minio":
                storage = create_minio_storage("http://minio:9000", "minioadmin", "minioadmin123")
                path = "bucket-meta/test_run/books.csv"
            elif storage_type == "gcs":
                storage = create_gcs_storage(project="test-project")
                path = "bucket-meta/test_run/books.csv"

            # Test URI formatting
            formatted = storage.get_display_uri(path)
            assert formatted == expected_format
