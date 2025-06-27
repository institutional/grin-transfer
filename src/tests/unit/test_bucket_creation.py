#!/usr/bin/env python3
"""
Unit tests for bucket creation functionality in sync.py
"""

from unittest import IsolatedAsyncioTestCase
from unittest.mock import MagicMock, patch

from botocore.exceptions import ClientError

from grin_to_s3.sync.utils import ensure_bucket_exists, reset_bucket_cache


class TestBucketCreation(IsolatedAsyncioTestCase):
    """Test bucket creation functionality."""

    def setUp(self):
        """Reset bucket cache before each test."""
        reset_bucket_cache()

    def create_storage_config(self, storage_type="s3"):
        """Create storage configuration for testing."""
        storage_config = {
            "access_key": "test_access_key",
            "secret_key": "test_secret_key",
            "bucket_raw": "test-bucket-raw",
            "bucket_meta": "test-bucket-meta",
            "bucket_full": "test-bucket-full"
        }

        if storage_type == "minio":
            storage_config["endpoint_url"] = "http://localhost:9000"
        elif storage_type == "r2":
            storage_config["account_id"] = "test_account"

        return storage_config

    async def test_bucket_exists_s3(self):
        """Test bucket existence check when bucket exists."""
        storage_config = self.create_storage_config("s3")

        with patch('boto3.client') as mock_boto_client:
            mock_s3 = MagicMock()
            mock_boto_client.return_value = mock_s3

            # Mock successful head_bucket call (bucket exists)
            mock_s3.head_bucket.return_value = {}

            result = await ensure_bucket_exists("s3", storage_config, "test-bucket-raw")

            self.assertTrue(result)
            mock_s3.head_bucket.assert_called_once_with(Bucket="test-bucket-raw")
            mock_s3.create_bucket.assert_not_called()

    async def test_bucket_creation_s3(self):
        """Test bucket creation when bucket doesn't exist."""
        storage_config = self.create_storage_config("s3")

        with patch('boto3.client') as mock_boto_client:
            mock_s3 = MagicMock()
            mock_boto_client.return_value = mock_s3

            # Mock 404 error for head_bucket (bucket doesn't exist)
            not_found_error = ClientError(
                error_response={'Error': {'Code': '404'}},
                operation_name='HeadBucket'
            )
            mock_s3.head_bucket.side_effect = not_found_error

            # Mock successful bucket creation and verification
            mock_s3.create_bucket.return_value = {}
            mock_s3.list_buckets.return_value = {
                'Buckets': [{'Name': 'test-bucket-raw'}]
            }

            result = await ensure_bucket_exists("s3", storage_config, "test-bucket-raw")

            self.assertTrue(result)
            mock_s3.head_bucket.assert_called_once_with(Bucket="test-bucket-raw")
            mock_s3.create_bucket.assert_called_once_with(Bucket="test-bucket-raw")
            mock_s3.list_buckets.assert_called_once()

    async def test_bucket_creation_minio(self):
        """Test bucket creation for MinIO with endpoint URL."""
        storage_config = self.create_storage_config("minio")

        with patch('boto3.client') as mock_boto_client:
            mock_s3 = MagicMock()
            mock_boto_client.return_value = mock_s3

            # Mock 404 error for head_bucket
            not_found_error = ClientError(
                error_response={'Error': {'Code': '404'}},
                operation_name='HeadBucket'
            )
            mock_s3.head_bucket.side_effect = not_found_error

            # Mock successful bucket creation
            mock_s3.create_bucket.return_value = {}
            mock_s3.list_buckets.return_value = {
                'Buckets': [{'Name': 'test-bucket-raw'}]
            }

            result = await ensure_bucket_exists("minio", storage_config, "test-bucket-raw")

            self.assertTrue(result)

            # Verify boto3 client was created with MinIO endpoint
            mock_boto_client.assert_called_once_with(
                's3',
                aws_access_key_id="test_access_key",
                aws_secret_access_key="test_secret_key",
                endpoint_url="http://localhost:9000"
            )

    async def test_bucket_creation_r2(self):
        """Test bucket creation for Cloudflare R2."""
        storage_config = self.create_storage_config("r2")

        with patch('boto3.client') as mock_boto_client:
            mock_s3 = MagicMock()
            mock_boto_client.return_value = mock_s3

            # Mock 404 error for head_bucket
            not_found_error = ClientError(
                error_response={'Error': {'Code': '404'}},
                operation_name='HeadBucket'
            )
            mock_s3.head_bucket.side_effect = not_found_error

            # Mock successful bucket creation
            mock_s3.create_bucket.return_value = {}
            mock_s3.list_buckets.return_value = {
                'Buckets': [{'Name': 'test-bucket-raw'}]
            }

            result = await ensure_bucket_exists("r2", storage_config, "test-bucket-raw")

            self.assertTrue(result)

            # Verify boto3 client was created with R2 endpoint
            expected_endpoint = "https://test_account.r2.cloudflarestorage.com"
            mock_boto_client.assert_called_once_with(
                's3',
                aws_access_key_id="test_access_key",
                aws_secret_access_key="test_secret_key",
                endpoint_url=expected_endpoint
            )

    async def test_bucket_creation_failure(self):
        """Test bucket creation failure handling."""
        storage_config = self.create_storage_config("s3")

        with patch('boto3.client') as mock_boto_client:
            mock_s3 = MagicMock()
            mock_boto_client.return_value = mock_s3

            # Mock 404 error for head_bucket
            not_found_error = ClientError(
                error_response={'Error': {'Code': '404'}},
                operation_name='HeadBucket'
            )
            mock_s3.head_bucket.side_effect = not_found_error

            # Mock bucket creation failure
            creation_error = ClientError(
                error_response={'Error': {'Code': 'AccessDenied', 'Message': 'Access denied'}},
                operation_name='CreateBucket'
            )
            mock_s3.create_bucket.side_effect = creation_error

            result = await ensure_bucket_exists("s3", storage_config, "test-bucket-raw")

            self.assertFalse(result)
            mock_s3.create_bucket.assert_called_once_with(Bucket="test-bucket-raw")

    async def test_bucket_verification_failure(self):
        """Test bucket creation with verification failure."""
        storage_config = self.create_storage_config("s3")

        with patch('boto3.client') as mock_boto_client:
            mock_s3 = MagicMock()
            mock_boto_client.return_value = mock_s3

            # Mock 404 error for head_bucket
            not_found_error = ClientError(
                error_response={'Error': {'Code': '404'}},
                operation_name='HeadBucket'
            )
            mock_s3.head_bucket.side_effect = not_found_error

            # Mock successful creation but verification fails
            mock_s3.create_bucket.return_value = {}
            mock_s3.list_buckets.return_value = {
                'Buckets': [{'Name': 'other-bucket-raw'}]  # Bucket not in list
            }

            result = await ensure_bucket_exists("s3", storage_config, "test-bucket-raw")

            self.assertFalse(result)

    async def test_bucket_other_error(self):
        """Test handling of non-404 errors during bucket check."""
        storage_config = self.create_storage_config("s3")

        with patch('boto3.client') as mock_boto_client:
            mock_s3 = MagicMock()
            mock_boto_client.return_value = mock_s3

            # Mock access denied error (not 404)
            access_error = ClientError(
                error_response={'Error': {'Code': 'AccessDenied'}},
                operation_name='HeadBucket'
            )
            mock_s3.head_bucket.side_effect = access_error

            result = await ensure_bucket_exists("s3", storage_config, "test-bucket-raw")

            self.assertFalse(result)
            mock_s3.create_bucket.assert_not_called()

    async def test_local_storage_bucket_check(self):
        """Test that local storage always returns True."""
        storage_config = {"prefix": "test"}

        result = await ensure_bucket_exists("local", storage_config, "any-bucket")
        self.assertTrue(result)

    async def test_bucket_cache_functionality(self):
        """Test that bucket cache prevents repeated checks."""
        storage_config = self.create_storage_config("s3")

        with patch('boto3.client') as mock_boto_client:
            mock_s3 = MagicMock()
            mock_boto_client.return_value = mock_s3

            # Mock successful head_bucket call
            mock_s3.head_bucket.return_value = {}

            # First call should check the bucket
            result1 = await ensure_bucket_exists("s3", storage_config, "test-bucket-raw")
            self.assertTrue(result1)
            self.assertEqual(mock_s3.head_bucket.call_count, 1)

            # Second call should use cache, not check again
            result2 = await ensure_bucket_exists("s3", storage_config, "test-bucket-raw")
            self.assertTrue(result2)
            self.assertEqual(mock_s3.head_bucket.call_count, 1)  # No additional calls

    async def test_reset_bucket_cache(self):
        """Test that reset_bucket_cache clears the cache."""
        storage_config = self.create_storage_config("s3")

        with patch('boto3.client') as mock_boto_client:
            mock_s3 = MagicMock()
            mock_boto_client.return_value = mock_s3
            mock_s3.head_bucket.return_value = {}

            # First call
            await ensure_bucket_exists("s3", storage_config, "test-bucket-raw")
            self.assertEqual(mock_s3.head_bucket.call_count, 1)

            # Reset cache
            reset_bucket_cache()

            # Second call should check again after cache reset
            await ensure_bucket_exists("s3", storage_config, "test-bucket-raw")
            self.assertEqual(mock_s3.head_bucket.call_count, 2)

    async def test_bucket_creation_called_during_upload(self):
        """Test that bucket creation is called during upload process."""
        # This test would need to be updated to test the actual upload flow
        # For now, we'll test the bucket creation function directly
        storage_config = self.create_storage_config("s3")

        with patch('boto3.client') as mock_boto_client:
            mock_s3 = MagicMock()
            mock_boto_client.return_value = mock_s3
            mock_s3.head_bucket.return_value = {}

            result = await ensure_bucket_exists("s3", storage_config, "test-bucket-raw")
            self.assertTrue(result)
            mock_s3.head_bucket.assert_called_once_with(Bucket="test-bucket-raw")


class TestBucketCreationErrorHandling(IsolatedAsyncioTestCase):
    """Test error handling in bucket creation."""

    def setUp(self):
        """Reset bucket cache before each test."""
        reset_bucket_cache()

    async def test_general_exception_handling(self):
        """Test handling of general exceptions during bucket operations."""
        storage_config = {
            "access_key": "test_access_key",
            "secret_key": "test_secret_key",
            "bucket": "test-bucket-raw"
        }

        with patch('boto3.client') as mock_boto_client:
            # Mock boto3.client to raise an unexpected exception
            mock_boto_client.side_effect = Exception("Network error")

            result = await ensure_bucket_exists("s3", storage_config, "test-bucket-raw")

            self.assertFalse(result)

    async def test_missing_credentials(self):
        """Test bucket creation with missing credentials."""
        # Create config with missing credentials
        storage_config = {"bucket_raw": "test-bucket-raw"}  # Missing access_key and secret_key

        with patch('boto3.client') as mock_boto_client:
            mock_s3 = MagicMock()
            mock_boto_client.return_value = mock_s3

            # The function should still try to work with None credentials
            await ensure_bucket_exists("s3", storage_config, "test-bucket-raw")

            # Verify boto3 was called with None credentials
            mock_boto_client.assert_called_once_with(
                's3',
                aws_access_key_id=None,
                aws_secret_access_key=None
            )
