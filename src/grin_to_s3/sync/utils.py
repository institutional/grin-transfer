#!/usr/bin/env python3
"""
Sync Utilities

Shared utility functions for sync operations including bucket management,
storage helpers, and common operations.
"""

import json
import logging
from typing import Any

import boto3
from botocore.exceptions import ClientError

from ..database import connect_async

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
        storage_type: Type of storage (local, minio, s3, r2)
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
        if storage_type in ("s3", "minio", "r2"):


            # Create boto3 client with same credentials
            s3_config = {
                "aws_access_key_id": storage_config.get("access_key"),
                "aws_secret_access_key": storage_config.get("secret_key"),
            }
            if storage_type == "minio":
                s3_config["endpoint_url"] = storage_config.get("endpoint_url")
            elif storage_type == "r2":
                account_id = storage_config.get("account_id")
                if account_id:
                    s3_config["endpoint_url"] = f"https://{account_id}.r2.cloudflarestorage.com"

            s3_client = boto3.client("s3", **s3_config)

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
        else:
            # For other storage types, assume bucket exists
            _bucket_checked_cache.add(bucket_key)
            return True

    except Exception as e:
        logger.error(f"Error checking bucket '{bucket_name}': {type(e).__name__}: {e}")
        return False


async def check_encrypted_etag(grin_client, library_directory: str, barcode: str) -> tuple[str | None, int | None]:
    """Make HEAD request to get encrypted file's ETag and file size before downloading.

    Args:
        grin_client: GRIN client instance
        library_directory: Library directory name
        barcode: Book barcode

    Returns:
        tuple: (etag, file_size) or (None, None) if check fails
    """
    try:
        from grin_to_s3.common import create_http_session

        grin_url = f"https://books.google.com/libraries/{library_directory}/{barcode}.tar.gz.gpg"
        logger.debug(f"[{barcode}] Checking encrypted ETag via HEAD request to {grin_url}")

        async with create_http_session() as session:
            # Make HEAD request to get headers without downloading content
            head_response = await grin_client.auth.make_authenticated_request(session, grin_url, method="HEAD")

            # Look for ETag and Content-Length headers
            etag = head_response.headers.get("ETag", "").strip('"')
            content_length = head_response.headers.get("Content-Length", "")

            file_size = int(content_length) if content_length else None

            if etag:
                logger.debug(f"[{barcode}] Encrypted ETag: {etag}, size: {file_size or 'unknown'}")
                return etag, file_size
            else:
                logger.debug(f"[{barcode}] No ETag found in Google response")
                return None, file_size

    except Exception as e:
        logger.warning(f"[{barcode}] Failed to check encrypted ETag: {e}")
        return None, None


async def should_skip_download(
    barcode: str, encrypted_etag: str | None, storage_type: str, storage_config: dict, db_tracker, force: bool = False
) -> tuple[bool, str | None]:
    """Check if stored ETag matches encrypted file's ETag to skip downloads.

    Uses different strategies based on storage type:
    - Block storage (S3/R2/MinIO): Check metadata on decrypted file
    - Local storage: Check database ETag history

    Args:
        barcode: Book barcode
        encrypted_etag: ETag from GRIN HEAD response
        storage_type: Type of storage
        storage_config: Storage configuration
        db_tracker: Database tracker instance
        force: Force download even if ETag matches

    Returns:
        tuple: (should_skip, reason)
    """
    if force or not encrypted_etag:
        return False, "force_flag" if force else "no_etag"

    # Determine storage protocol for logic decisions
    from grin_to_s3.storage import get_storage_protocol

    storage_protocol = get_storage_protocol(storage_type)

    # For S3-compatible storage, check metadata on decrypted file
    if storage_protocol == "s3":
        try:
            from grin_to_s3.storage import BookManager, create_storage_from_config
            from grin_to_s3.storage.book_manager import BucketConfig

            # Create storage
            storage = create_storage_from_config(storage_type, storage_config or {})

            # Get bucket and prefix information
            base_prefix = storage_config.get("prefix", "")
            bucket_name = storage_config.get("bucket_raw") if storage_config else None

            # For S3-compatible storage, bucket name must be included in path prefix
            if storage_protocol == "s3" and bucket_name:
                if base_prefix:
                    base_prefix = f"{bucket_name}/{base_prefix}"
                else:
                    base_prefix = bucket_name

            # Create bucket configuration
            bucket_config: BucketConfig = {
                "bucket_raw": storage_config.get("bucket_raw", ""),
                "bucket_meta": storage_config.get("bucket_meta", ""),
                "bucket_full": storage_config.get("bucket_full", ""),
            }

            book_storage = BookManager(storage, bucket_config=bucket_config, base_prefix=base_prefix)

            # Check if decrypted archive exists and matches encrypted ETag
            if await book_storage.decrypted_archive_exists(barcode):
                if await book_storage.archive_matches_encrypted_etag(barcode, encrypted_etag):
                    logger.info(f"[{barcode}] File unchanged (ETag match in storage metadata), skipping download")
                    return True, "storage_etag_match"
                else:
                    logger.debug(f"[{barcode}] Decrypted archive exists but ETag differs, will download")
                    return False, "storage_etag_mismatch"
            else:
                logger.debug(f"[{barcode}] Decrypted archive doesn't exist, will download")
                return False, "no_decrypted_archive"

        except Exception as e:
            logger.warning(f"[{barcode}] Error checking storage metadata for ETag: {e}")
            # Fall back to database check for block storage too
            pass

    # For local storage or fallback, use database check
    try:
        async with connect_async(db_tracker.db_path) as db:
            cursor = await db.execute(
                """
                SELECT status_value, metadata FROM book_status_history
                WHERE barcode = ? AND status_type = 'sync'
                ORDER BY timestamp DESC, id DESC LIMIT 1
            """,
                (barcode,),
            )

            if not (row := await cursor.fetchone()):
                return False, "no_metadata"

            if not (metadata := json.loads(row[1]) if row[1] else None):
                return False, "no_metadata"

            if not (stored_etag := metadata.get("encrypted_etag")):
                return False, "no_stored_etag"

            match = stored_etag.strip('"') == encrypted_etag.strip('"')
            action = "skipping" if match else "downloading"
            status = "matches" if match else "differs"
            logger.info(f"[{barcode}] ETag {status} in database, {action}")
            return match, "database_etag_match" if match else "database_etag_mismatch"

    except Exception as e:
        logger.error(f"[{barcode}] Database ETag check failed: {type(e).__name__}")
        return False, f"error_{type(e).__name__.lower()}"


async def get_converted_books(grin_client, library_directory: str) -> set[str]:
    """Get set of books that are converted and ready for download.

    Args:
        grin_client: GRIN client instance
        library_directory: Library directory name

    Returns:
        set: Set of converted book barcodes
    """
    try:
        response_text = await grin_client.fetch_resource(library_directory, "_converted?format=text")
        lines = response_text.strip().split("\n")
        converted_barcodes = set()
        for line in lines:
            if line.strip() and ".tar.gz.gpg" in line:
                barcode = line.strip().replace(".tar.gz.gpg", "")
                converted_barcodes.add(barcode)
        return converted_barcodes
    except Exception as e:
        logger.warning(f"Failed to get converted books: {e}")
        return set()
