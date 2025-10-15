#!/usr/bin/env python3
"""
Sync Utilities

Shared utility functions for sync operations including bucket management,
storage helpers, and common operations.
"""

import logging
import os
from typing import Any

import boto3
from botocore.exceptions import ClientError

from ..storage.factories import (
    get_storage_protocol,
    load_r2_credentials,
    s3_credentials_available,
)

logger = logging.getLogger(__name__)

# Global cache for bucket existence checks
_bucket_checked_cache: set[str] = set()


def reset_bucket_cache() -> None:
    """Reset the bucket existence cache (useful for testing)."""
    global _bucket_checked_cache
    _bucket_checked_cache.clear()


async def ensure_bucket_exists(storage_type: str, storage_config: dict[str, Any], bucket_name: str) -> bool:
    """Ensure the bucket exists, create if it doesn't.

    Args:
        storage_type: Type of storage (local, minio, s3, r2, gcs)
        storage_config: Storage configuration dictionary
        bucket_name: Name of the bucket to check/create

    Returns:
        True if bucket exists or was created successfully, False otherwise
    """
    if storage_type == "local":
        return True

    bucket_key = f"{storage_type}:{bucket_name}"

    # Check cache first to avoid repeated checks
    if bucket_key in _bucket_checked_cache:
        logger.debug(f"Bucket {bucket_name} already verified (skipping check)")
        return True

    try:
        # Use boto3 directly for bucket operations (fsspec doesn't support bucket creation)
        storage_protocol = get_storage_protocol(storage_type)

        if storage_protocol == "s3":
            # Check credentials availability for each storage type
            access_key = None
            secret_key = None

            if storage_type == "minio":
                # MinIO uses hardcoded development credentials
                access_key = "minioadmin"
                secret_key = "minioadmin123"
            elif storage_type == "r2":
                # Load R2 credentials from secrets directory
                r2_creds = load_r2_credentials()
                if r2_creds is None:
                    return False
                access_key, secret_key = r2_creds
            elif storage_type == "s3":
                # For S3, check if credentials are available via boto3
                if not s3_credentials_available():
                    logger.error(
                        "Missing S3 credentials. Please ensure credentials are configured via environment variables or ~/.aws/credentials"
                    )
                    return False
                # Don't set access_key/secret_key - let boto3 handle credential loading

            # Build S3 config based on storage type
            s3_config: dict[str, str] = {}
            if storage_type == "s3":
                # For S3, let boto3 handle credential loading via standard credential chain
                pass
            elif access_key and secret_key:
                # For MinIO and R2, use explicit credentials
                # Note: Parameter names are from S3-compatible API (used by MinIO, R2, etc.)
                s3_config["aws_access_key_id"] = access_key
                s3_config["aws_secret_access_key"] = secret_key
            if storage_type in ("minio", "r2"):
                endpoint_url = storage_config.get("endpoint_url")
                if endpoint_url:
                    s3_config["endpoint_url"] = endpoint_url

            s3_client = boto3.client("s3", **s3_config)  # type: ignore[call-overload]

            try:
                s3_client.head_bucket(Bucket=bucket_name)
                logger.debug(f"Bucket '{bucket_name}' exists")
                _bucket_checked_cache.add(bucket_key)
                return True
            except ClientError as e:
                if e.response.get("Error", {}).get("Code") == "404":
                    # Bucket doesn't exist, try to create it
                    logger.info(f"Bucket '{bucket_name}' does not exist. Creating automatically...")
                    try:
                        s3_client.create_bucket(Bucket=bucket_name)

                        # Verify the bucket was actually created
                        buckets_response = s3_client.list_buckets()
                        bucket_names = [b.get("Name", "") for b in buckets_response.get("Buckets", []) if "Name" in b]

                        if bucket_name in bucket_names:
                            logger.info(f"Created and verified bucket '{bucket_name}'")
                            _bucket_checked_cache.add(bucket_key)
                            return True
                        else:
                            logger.error(f"Bucket '{bucket_name}' not found in list after creation")
                            return False

                    except ClientError as create_error:
                        logger.error(f"Failed to create bucket '{bucket_name}': {create_error}")
                        return False
                else:
                    # Some other error occurred
                    logger.error(f"Error checking bucket '{bucket_name}': {e}")
                    return False

        elif storage_protocol == "gcs":
            # Handle GCS bucket operations using gcsfs
            import gcsfs

            try:
                # Use Application Default Credentials
                project_id = storage_config.get("project")
                if not project_id:
                    logger.error("GCS storage requires project ID in configuration")
                    return False

                # Set GOOGLE_CLOUD_PROJECT for gcsfs authentication
                if not os.environ.get("GOOGLE_CLOUD_PROJECT"):
                    os.environ["GOOGLE_CLOUD_PROJECT"] = project_id

                # Create gcsfs client
                gcs_fs = gcsfs.GCSFileSystem(project=project_id)

                # Check if bucket exists
                if gcs_fs.exists(bucket_name):
                    logger.debug(f"GCS bucket '{bucket_name}' exists")
                    _bucket_checked_cache.add(bucket_key)
                    return True
                else:
                    # Bucket doesn't exist, try to create it
                    logger.info(f"GCS bucket '{bucket_name}' does not exist. Creating automatically...")
                    try:
                        gcs_fs.mkdir(bucket_name)
                        logger.info(f"Created GCS bucket '{bucket_name}'")
                        _bucket_checked_cache.add(bucket_key)
                        return True

                    except Exception as create_error:
                        logger.error(f"Failed to create GCS bucket '{bucket_name}': {create_error}")
                        return False

            except Exception as e:
                logger.error(f"Error checking GCS bucket '{bucket_name}': {e}")
                return False

        else:
            # For other storage types, assume bucket exists
            _bucket_checked_cache.add(bucket_key)
            return True

    except Exception as e:
        logger.error(f"Error checking bucket '{bucket_name}': {type(e).__name__}: {e}")
        return False
