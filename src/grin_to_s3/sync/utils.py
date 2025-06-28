#!/usr/bin/env python3
"""
Sync Utilities

Shared utility functions for sync operations including bucket management,
storage helpers, and common operations.
"""

import json
import logging
from typing import Any

import aiosqlite

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
        if storage_type in ("minio", "s3", "r2"):
            import boto3
            from botocore.exceptions import ClientError

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
                if e.response["Error"]["Code"] == "404":
                    # Bucket doesn't exist, try to create it
                    logger.info(f"Bucket '{bucket_name}' does not exist. Creating automatically...")
                    try:
                        s3_client.create_bucket(Bucket=bucket_name)

                        # Verify the bucket was actually created
                        buckets_response = s3_client.list_buckets()
                        bucket_names = [b["Name"] for b in buckets_response.get("Buckets", [])]

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


async def check_google_etag(grin_client, library_directory: str, barcode: str) -> tuple[str | None, int | None]:
    """Make HEAD request to get Google's ETag and file size before downloading.

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
        logger.debug(f"[{barcode}] Checking Google ETag via HEAD request to {grin_url}")

        async with create_http_session() as session:
            # Make HEAD request to get headers without downloading content
            head_response = await grin_client.auth.make_authenticated_request(session, grin_url, method="HEAD")

            # Look for ETag and Content-Length headers
            etag = head_response.headers.get("ETag", "").strip('"')
            content_length = head_response.headers.get("Content-Length", "")

            file_size = int(content_length) if content_length else None

            if etag:
                logger.debug(f"[{barcode}] Google ETag: {etag}, size: {file_size or 'unknown'}")
                return etag, file_size
            else:
                logger.debug(f"[{barcode}] No ETag found in Google response")
                return None, file_size

    except Exception as e:
        logger.warning(f"[{barcode}] Failed to check Google ETag: {e}")
        return None, None


async def should_skip_download(
    barcode: str, google_etag: str | None, db_tracker, force: bool = False
) -> tuple[bool, str | None]:
    """Check if stored ETag matches Google's ETag to skip downloads.

    Args:
        barcode: Book barcode
        google_etag: ETag from Google's HEAD response
        db_tracker: Database tracker instance
        force: Force download even if ETag matches

    Returns:
        tuple: (should_skip, reason)
    """
    if force or not google_etag:
        return False, "force_flag" if force else "no_etag"

    try:
        async with aiosqlite.connect(db_tracker.db_path) as db:
            async with db.execute("BEGIN IMMEDIATE"):
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

                if not (stored_etag := metadata.get("grin_etag")):
                    return False, "no_stored_etag"

                match = stored_etag.strip('"') == google_etag.strip('"')
                logger.info(
                    f'[{barcode}] ETag {"matches" if match else "differs"}, {"skipping" if match else "downloading"}'
                )
                return match, "etag_match" if match else "etag_mismatch"

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
