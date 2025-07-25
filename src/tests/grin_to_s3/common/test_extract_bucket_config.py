"""Tests for extract_bucket_config function."""

from grin_to_s3.common import extract_bucket_config


class TestExtractBucketConfig:
    """Test bucket configuration extraction with defaults."""

    def test_extract_bucket_config_local_with_defaults(self):
        """Test local storage gets default bucket names."""
        config_dict = {}
        result = extract_bucket_config("local", config_dict)

        assert result["bucket_raw"] == "raw"
        assert result["bucket_meta"] == "meta"
        assert result["bucket_full"] == "full"

    def test_extract_bucket_config_local_with_custom_names(self):
        """Test local storage with custom bucket names."""
        config_dict = {
            "bucket_raw": "custom-raw",
            "bucket_meta": "custom-meta",
            "bucket_full": "custom-full"
        }
        result = extract_bucket_config("local", config_dict)

        assert result["bucket_raw"] == "custom-raw"
        assert result["bucket_meta"] == "custom-meta"
        assert result["bucket_full"] == "custom-full"

    def test_extract_bucket_config_local_partial_custom(self):
        """Test local storage with some custom bucket names."""
        config_dict = {
            "bucket_raw": "custom-raw",
            # bucket_meta missing - should use default
            "bucket_full": "custom-full"
        }
        result = extract_bucket_config("local", config_dict)

        assert result["bucket_raw"] == "custom-raw"
        assert result["bucket_meta"] == "meta"  # default
        assert result["bucket_full"] == "custom-full"

    def test_extract_bucket_config_cloud_storage_no_defaults(self):
        """Test cloud storage doesn't get defaults (returns empty strings)."""
        config_dict = {}

        # Test S3
        result = extract_bucket_config("s3", config_dict)
        assert result["bucket_raw"] == ""
        assert result["bucket_meta"] == ""
        assert result["bucket_full"] == ""

        # Test R2
        result = extract_bucket_config("r2", config_dict)
        assert result["bucket_raw"] == ""
        assert result["bucket_meta"] == ""
        assert result["bucket_full"] == ""

        # Test MinIO
        result = extract_bucket_config("minio", config_dict)
        assert result["bucket_raw"] == ""
        assert result["bucket_meta"] == ""
        assert result["bucket_full"] == ""

    def test_extract_bucket_config_cloud_storage_with_names(self):
        """Test cloud storage with provided bucket names."""
        config_dict = {
            "bucket_raw": "my-raw-bucket",
            "bucket_meta": "my-meta-bucket",
            "bucket_full": "my-full-bucket"
        }

        # Test S3
        result = extract_bucket_config("s3", config_dict)
        assert result["bucket_raw"] == "my-raw-bucket"
        assert result["bucket_meta"] == "my-meta-bucket"
        assert result["bucket_full"] == "my-full-bucket"
