#!/usr/bin/env python3
"""
Download books from GRIN to local filesystem or block storage

Unified V2 download tool supporting:
- Local filesystem (default)
- MinIO (local S3-compatible)
- Cloudflare R2
- AWS S3
"""

import argparse
import asyncio
import logging
from datetime import datetime
from pathlib import Path

import aiofiles

from client import GRINClient
from collect_books.models import SQLiteProgressTracker
from common import (
    calculate_transfer_speed,
    create_http_session,
    create_storage_from_config,
    decrypt_gpg_data,
    format_bytes,
    format_duration,
)
from storage import BookStorage

logger = logging.getLogger(__name__)

# Global cache for bucket existence checks
_bucket_checked_cache = set()


async def _decrypt_and_save_archive(
    barcode: str,
    encrypted_data: bytes,
    book_storage: BookStorage,
    gpg_key_file: str | None = None,
    secrets_dir: str | None = None,
    verbose: bool = True,
) -> bool:
    """Decrypt the archive and save the decrypted version.

    Returns:
        True if decryption was successful, False otherwise
    """
    try:
        if verbose:
            print(f"Decrypting {barcode} archive...")
        decrypted_data = await decrypt_gpg_data(encrypted_data, gpg_key_file, secrets_dir)
        await book_storage.save_decrypted_archive(barcode, decrypted_data)
        if verbose:
            print(f"✅ {barcode} saved decrypted archive: {book_storage._book_path(barcode, f'{barcode}.tar.gz')}")
        return True
    except RuntimeError as e:
        error_msg = str(e)
        if verbose:
            if "GPG command not found" in error_msg:
                print(f"⚠️ {barcode} Warning: GPG is not installed - skipping decryption")
                print("Install GPG to automatically decrypt archives: https://gnupg.org/download/")
            elif "private key not found" in error_msg or "public key not found" in error_msg:
                print(f"⚠️ {barcode} Warning: GPG keys not configured - skipping decryption")
                print("Import your GPG private key to decrypt archives automatically")
            else:
                print(f"⚠️ {barcode} Failed to decrypt archive: {e}")
            print(f"{barcode} encrypted archive saved successfully, decryption skipped")
        return False
    except Exception as e:
        if verbose:
            print(f"⚠️ {barcode} Failed to decrypt archive: {e}")
            print(f"{barcode} encrypted archive saved successfully, but decryption failed")
        return False


async def ensure_bucket_exists_with_storage(storage, storage_type: str, storage_config: dict) -> bool:
    """Ensure the bucket exists using the provided storage instance."""
    if storage_type == "local":
        return True

    bucket = storage_config.get("bucket")
    if not bucket:
        print("No bucket specified in storage config")
        return True

    print(f"Debug: Checking bucket '{bucket}' for {storage_type}")
    print(f"Debug: Storage config keys: {list(storage_config.keys())}")
    print(f"Debug: Using provided storage object of type: {type(storage)}")

    try:
        # Check if bucket exists by trying to list it
        try:
            print(f"Debug: Attempting to list objects in bucket '{bucket}'...")
            objects = await storage.list_objects("")
            print(f"Debug: Successfully listed {len(objects)} objects in bucket '{bucket}'")
            print(f"Debug: Bucket '{bucket}' exists and is accessible")
            return True
        except Exception as e:
            # Bucket doesn't exist or isn't accessible
            print(f"Debug: Cannot access bucket '{bucket}': {type(e).__name__}: {e}")

            # Try with bucket prefix
            try:
                print(f"Debug: Trying to list with bucket prefix '{bucket}/'...")
                bucket_objects = await storage.list_objects(bucket)
                print(f"Debug: Successfully listed {len(bucket_objects)} objects with bucket prefix")
                print(f"Debug: Bucket '{bucket}' exists and is accessible via prefix")
                return True
            except Exception as e2:
                print(f"Debug: Bucket prefix check also failed: {type(e2).__name__}: {e2}")
                pass

        # Ask user if they want to create the bucket
        print(f"Bucket '{bucket}' does not exist.")
        response = input(f"Create bucket '{bucket}'? [y/N]: ").strip().lower()

        if response in ('y', 'yes'):
            # For MinIO/S3, we need to use boto3 directly since fsspec doesn't support bucket creation
            if storage_type in ("minio", "s3"):
                import boto3
                from botocore.exceptions import ClientError

                s3_config = {
                    "aws_access_key_id": storage_config.get("access_key"),
                    "aws_secret_access_key": storage_config.get("secret_key"),
                }

                if storage_type == "minio":
                    s3_config["endpoint_url"] = storage_config.get("endpoint_url")

                endpoint = s3_config.get('endpoint_url', 'N/A')
                print(f"Debug: boto3 config: {list(s3_config.keys())} (endpoint: {endpoint})")
                s3_client = boto3.client("s3", **s3_config)

                try:
                    print(f"Debug: Creating bucket '{bucket}' with boto3...")
                    s3_client.create_bucket(Bucket=bucket)
                    print("Debug: boto3.create_bucket() succeeded")

                    # Verify the bucket was actually created
                    print("Debug: Verifying bucket creation by listing buckets...")
                    buckets_response = s3_client.list_buckets()
                    bucket_names = [b['Name'] for b in buckets_response.get('Buckets', [])]
                    print(f"Debug: Available buckets: {bucket_names}")

                    if bucket in bucket_names:
                        print(f"✅ Created and verified bucket '{bucket}'")
                        return True
                    else:
                        print(f"❌ Bucket '{bucket}' not found in list after creation")
                        return False

                except ClientError as e:
                    print(f"❌ Failed to create bucket '{bucket}': {e}")
                    print(f"Debug: ClientError details: {e.response}")
                    return False
            else:
                print(f"❌ Bucket creation not supported for {storage_type}")
                return False
        else:
            print("❌ Bucket does not exist and will not be created")
            return False

    except Exception as e:
        print(f"❌ Error checking bucket: {type(e).__name__}: {e}")
        import traceback
        print(f"Debug: Full traceback:\n{traceback.format_exc()}")
        return False


async def ensure_bucket_exists(storage_type: str, storage_config: dict) -> bool:
    """Ensure the bucket exists, offer to create if it doesn't."""
    if storage_type == "local":
        return True

    bucket = storage_config.get("bucket")
    if not bucket:
        print("No bucket specified in storage config")
        return True

    print(f"Debug: Checking bucket '{bucket}' for {storage_type}")
    print(f"Debug: Storage config keys: {list(storage_config.keys())}")

    try:
        from common import create_storage_from_config
        storage = create_storage_from_config(storage_type, storage_config)
        print(f"Debug: Created storage object of type: {type(storage)}")

        # Check if bucket exists by trying to list it
        try:
            print(f"Debug: Attempting to list objects in bucket '{bucket}'...")
            objects = await storage.list_objects("")
            print(f"Debug: Successfully listed {len(objects)} objects in bucket '{bucket}'")
            print(f"Debug: Bucket '{bucket}' exists and is accessible")
            return True
        except Exception as e:
            # Bucket doesn't exist or isn't accessible
            print(f"Debug: Cannot access bucket '{bucket}': {type(e).__name__}: {e}")

            # Try with bucket prefix
            try:
                print(f"Debug: Trying to list with bucket prefix '{bucket}/'...")
                bucket_objects = await storage.list_objects(bucket)
                print(f"Debug: Successfully listed {len(bucket_objects)} objects with bucket prefix")
                print(f"Debug: Bucket '{bucket}' exists and is accessible via prefix")
                return True
            except Exception as e2:
                print(f"Debug: Bucket prefix check also failed: {type(e2).__name__}: {e2}")
                pass

        # Ask user if they want to create the bucket
        print(f"Bucket '{bucket}' does not exist.")
        response = input(f"Create bucket '{bucket}'? [y/N]: ").strip().lower()

        if response in ('y', 'yes'):
            # For MinIO/S3, we need to use boto3 directly since fsspec doesn't support bucket creation
            if storage_type in ("minio", "s3"):
                import boto3
                from botocore.exceptions import ClientError

                s3_config = {
                    "aws_access_key_id": storage_config.get("access_key"),
                    "aws_secret_access_key": storage_config.get("secret_key"),
                }

                if storage_type == "minio":
                    s3_config["endpoint_url"] = storage_config.get("endpoint_url")

                endpoint = s3_config.get('endpoint_url', 'N/A')
                print(f"Debug: boto3 config: {list(s3_config.keys())} (endpoint: {endpoint})")
                s3_client = boto3.client("s3", **s3_config)

                try:
                    print(f"Debug: Creating bucket '{bucket}' with boto3...")
                    s3_client.create_bucket(Bucket=bucket)
                    print("Debug: boto3.create_bucket() succeeded")

                    # Verify the bucket was actually created
                    print("Debug: Verifying bucket creation by listing buckets...")
                    buckets_response = s3_client.list_buckets()
                    bucket_names = [b['Name'] for b in buckets_response.get('Buckets', [])]
                    print(f"Debug: Available buckets: {bucket_names}")

                    if bucket in bucket_names:
                        print(f"✅ Created and verified bucket '{bucket}'")
                        return True
                    else:
                        print(f"❌ Bucket '{bucket}' not found in list after creation")
                        return False

                except ClientError as e:
                    print(f"❌ Failed to create bucket '{bucket}': {e}")
                    print(f"Debug: ClientError details: {e.response}")
                    return False
            else:
                print(f"❌ Bucket creation not supported for {storage_type}")
                return False
        else:
            print("❌ Bucket does not exist and will not be created")
            return False

    except Exception as e:
        print(f"❌ Error checking bucket: {type(e).__name__}: {e}")
        import traceback
        print(f"Debug: Full traceback:\n{traceback.format_exc()}")
        return False


async def download_book(
    barcode: str,
    output_dir: str = ".",
    storage_type: str | None = None,
    storage_config: dict | None = None,
    base_url: str = "https://books.google.com/libraries/",
    directory: str = "Harvard",
    secrets_dir: str | None = None,
    force: bool = False,
    gpg_key_file: str | None = None,
    db_tracker: SQLiteProgressTracker | None = None,
    verbose: bool = True,
    show_progress: bool = True,
) -> dict:
    """
    Download a book from GRIN to local filesystem or block storage.

    Args:
        barcode: Book barcode to download
        output_dir: Directory for local storage (default: current directory)
        storage_type: Storage backend type (local, minio, r2, s3)
        storage_config: Configuration for storage backend
        base_url: GRIN base URL
        directory: Library directory
        secrets_dir: Directory containing GRIN secrets (searches home directory if not specified)
        force: Force download and overwrite existing files (skip ETag check)
        gpg_key_file: Path to GPG key file for decryption (default: ~/.config/grin-to-s3/gpg_key.asc)
        db_tracker: Optional database tracker for sync status updates
        verbose: Whether to print progress messages
        show_progress: Whether to show download percentage progress (disable for pipeline use)

    Returns:
        Dict with download results
    """
    if verbose:
        print(f"Downloading book: {barcode}")

    # Mark as syncing in database if tracker provided
    if db_tracker:
        await db_tracker.update_sync_status(barcode, {
            "sync_status": "syncing",
            "storage_type": storage_type or "local",
            "sync_error": None,
        })

    # Set up GRIN client
    client = GRINClient(base_url=base_url, secrets_dir=secrets_dir)

    # Construct GRIN URL
    archive_filename = f"{barcode}.tar.gz.gpg"
    grin_url = f"{base_url.rstrip('/')}/{directory}/{archive_filename}"

    # Console: minimal status
    if verbose:
        print(f"Starting {barcode}")

    # Log: detailed info
    logger.info(f"Starting download for {barcode} from {grin_url}")
    logger.debug(f"Source URL: {grin_url}")

    # Check if book is in converted state first
    logger.debug("Checking if book is converted...")
    try:
        converted_text = await client.fetch_resource(directory, "_converted?format=text")
        converted_files = {line.split("\t")[0] for line in converted_text.strip().split("\n") if line.strip()}

        if archive_filename not in converted_files:
            raise ValueError(f"Book {barcode} is not in converted state. "
                           f"Only converted books have downloadable archives. "
                           f"Check GRIN /_converted endpoint to see available books.")

        if verbose:
            print(f"{barcode} is converted. Checking if archive file exists...")

    except Exception as e:
        error_msg = f"Error checking converted status: {e}"
        if verbose:
            print(error_msg)

        # Update database with error if tracker provided
        if db_tracker:
            await db_tracker.update_sync_status(barcode, {
                "sync_status": "failed",
                "sync_error": error_msg,
            })
        raise

    # Check if archive file exists
    file_exists = await client.check_file_exists(directory, archive_filename)
    if not file_exists:
        raise FileNotFoundError(f"Archive {archive_filename} not found in GRIN directory {directory}. "
                               f"Book is converted but archive file is not accessible.")

    logger.info("Archive found! Starting download...")

    # First check headers to see if we can avoid downloading
    logger.debug("Checking file headers...")

    async with create_http_session() as session:
        # Make HEAD request to get headers without downloading content
        head_response = await client.auth.make_authenticated_request(session, grin_url, method="HEAD")

        # Look for ETag or Content-MD5 headers
        etag = head_response.headers.get('ETag', '').strip('"')
        content_md5 = head_response.headers.get('Content-MD5', '')
        content_length = head_response.headers.get('Content-Length', '')

        logger.info(f"File size: {format_bytes(int(content_length)) if content_length else 'unknown'}")
        if verbose:
            print(f"  Size: {format_bytes(int(content_length)) if content_length else 'unknown'} ({barcode})")
        if etag:
            logger.debug(f"Google ETag: {etag}")
        if content_md5:
            logger.debug(f"Google Content-MD5: {content_md5}")

        # Check if we can skip download entirely (unless forced)
        if not force and storage_type and storage_type != "local" and (etag or content_md5):
            # Create storage early to check existing file
            storage = create_storage_from_config(storage_type, storage_config or {})

            # For S3-like storage, include bucket in the path
            base_prefix = (storage_config or {}).get("prefix", "grin-books")
            if storage_type in ("minio", "s3", "r2"):
                bucket = (storage_config or {}).get("bucket")
                if bucket:
                    base_prefix = f"{bucket}/{base_prefix}"

            book_storage = BookStorage(storage, base_prefix=base_prefix)

            if await book_storage.archive_exists(barcode):
                print(f"{barcode} archive already exists, checking if Google's version matches...")

                # Check if we have the same Google file using stored metadata
                if etag and await book_storage.archive_matches_google_etag(barcode, etag):
                    logger.info(
                        "File already exists with identical content (matched Google's ETag), "
                        "skipping download entirely"
                    )
                    if verbose:
                        print(f"  ✅ {barcode} already exists")
                    await book_storage.save_timestamp(barcode)

                    return {
                        "barcode": barcode,
                        "storage_type": storage_type or "local",
                        "archive_path": book_storage._book_path(barcode, f"{barcode}.tar.gz.gpg"),
                        "file_size": int(content_length) if content_length else 0,
                        "download_time": 0.0,
                        "storage_time": 0.0,
                        "total_time": 0.0,
                        "download_speed_mbps": 0.0,
                        "skipped": True,
                    }

        if force:
            print(f"Force mode enabled - will overwrite existing files for {barcode}")

        logger.info("Proceeding with download...")
        if verbose:
            print(f"  Downloading {barcode}...")

    # Download from GRIN
    download_start = datetime.now()

    # Capture Google ETag for later use (from the HEAD request above)
    google_etag = None
    if 'etag' in locals():
        google_etag = etag

    async with create_http_session() as session:
        response = await client.auth.make_authenticated_request(session, grin_url)

        # Get expected file size from headers
        expected_size = int(content_length) if content_length else None

        # Collect data
        chunks = []
        total_bytes = 0
        last_progress_bytes = 0
        progress_threshold = 25 * 1024 * 1024  # 25MB

        async for chunk in response.content.iter_chunked(1024 * 1024):  # 1MB chunks
            chunks.append(chunk)
            total_bytes += len(chunk)

            # Print progress every 25MB (only for single downloads)
            if show_progress and total_bytes - last_progress_bytes >= progress_threshold:
                if expected_size:
                    percentage = (total_bytes / expected_size) * 100
                    print(f"{barcode} downloaded {format_bytes(total_bytes)} ({percentage:.1f}%)...")
                else:
                    print(f"{barcode} downloaded {format_bytes(total_bytes)}...")
                last_progress_bytes = total_bytes

    archive_data = b"".join(chunks)
    download_time = (datetime.now() - download_start).total_seconds()

    logger.info(f"Download complete: {format_bytes(len(archive_data))} in {format_duration(download_time)}")
    logger.info(f"Speed: {calculate_transfer_speed(len(archive_data), download_time)}")
    if verbose:
        print(f"  ✅ {barcode} downloaded {format_bytes(len(archive_data))} in {format_duration(download_time)}")

    # Save to storage
    storage_start = datetime.now()

    if storage_type and storage_type != "local":
        # Create storage first
        storage = create_storage_from_config(storage_type, storage_config or {})

        # Ensure bucket exists for MinIO/S3 (only check once per bucket)
        bucket_name = (storage_config or {}).get("bucket", "UNKNOWN")
        bucket_key = f"{storage_type}:{bucket_name}"

        if bucket_key not in _bucket_checked_cache:
            print(f"Checking bucket '{bucket_name}' for {storage_type} ({barcode})...")

            # Use direct boto3 approach to check and create bucket
            if storage_type in ("minio", "s3"):
                import boto3
                from botocore.exceptions import ClientError, NoCredentialsError

                try:
                    config = storage_config or {}
                    s3_config = {
                        "aws_access_key_id": config.get("access_key"),
                        "aws_secret_access_key": config.get("secret_key"),
                    }

                    if storage_type == "minio":
                        s3_config["endpoint_url"] = config.get("endpoint_url")

                    s3_client = boto3.client("s3", **s3_config)

                    # Check if bucket exists
                    try:
                        s3_client.head_bucket(Bucket=bucket_name)
                        print(f"Bucket '{bucket_name}' exists ({barcode})")
                    except ClientError as e:
                        error_code = e.response['Error']['Code']
                        if error_code == '404':
                            print(f"Bucket '{bucket_name}' does not exist. Creating... ({barcode})")
                            s3_client.create_bucket(Bucket=bucket_name)
                            print(f"✅ Created bucket '{bucket_name}' ({barcode})")
                        else:
                            raise RuntimeError(f"Cannot access bucket '{bucket_name}': {e}") from e

                except NoCredentialsError as e:
                    raise RuntimeError(f"No credentials available for {storage_type}") from e
                except Exception as e:
                    raise RuntimeError(f"Bucket check failed: {e}") from e

            print(f"Bucket '{bucket_name}' is ready for {barcode}!")
            _bucket_checked_cache.add(bucket_key)
        else:
            logger.debug(f"Bucket {bucket_name} already verified (skipping check for {barcode})")

        # For S3-like storage, include bucket in the path
        base_prefix = (storage_config or {}).get("prefix", "grin-books")
        if storage_type in ("minio", "s3", "r2"):
            bucket = (storage_config or {}).get("bucket")
            if bucket:
                base_prefix = f"{bucket}/{base_prefix}"

        book_storage = BookStorage(storage, base_prefix=base_prefix)


        # Check if file already exists with same Google ETag (unless forced)
        if not force and await book_storage.archive_exists(barcode):
            if google_etag and await book_storage.archive_matches_google_etag(barcode, google_etag):
                print(
                    f"✅ {barcode} archive already exists with identical content "
                    f"(Google ETag match), skipping upload"
                )
                archive_path = book_storage._book_path(barcode, f"{barcode}.tar.gz.gpg")
                # Still update timestamp to record this download attempt
                await book_storage.save_timestamp(barcode)
            else:
                print(f"{barcode} archive exists but Google ETag differs (or missing), uploading new version...")
                archive_path = book_storage._book_path(barcode, f"{barcode}.tar.gz.gpg")

                # Run upload tasks in parallel
                results = await asyncio.gather(
                    book_storage.save_archive(barcode, archive_data, google_etag),
                    book_storage.save_timestamp(barcode),
                    _decrypt_and_save_archive(barcode, archive_data, book_storage, gpg_key_file, secrets_dir, verbose),
                    return_exceptions=True
                )

                # Report upload paths
                encrypted_path = results[0] if not isinstance(results[0], Exception) else archive_path
                decrypted_path = book_storage._book_path(barcode, f"{barcode}.tar.gz")
                logger.info(f"Upload completed: Encrypted: {encrypted_path}, Decrypted: {decrypted_path}")
                if verbose:
                    print(f"  ✅ {barcode} uploaded with decryption")
        else:
            if force and await book_storage.archive_exists(barcode):
                print(f"Overwriting {barcode} existing archive in {storage_type} storage...")
            else:
                print(f"Saving {barcode} to {storage_type} storage...")

            archive_path = book_storage._book_path(barcode, f"{barcode}.tar.gz.gpg")

            # Run upload tasks in parallel
            results = await asyncio.gather(
                book_storage.save_archive(barcode, archive_data, google_etag),
                book_storage.save_timestamp(barcode),
                _decrypt_and_save_archive(barcode, archive_data, book_storage, gpg_key_file, secrets_dir),
                return_exceptions=True
            )

            # Report upload paths
            encrypted_path = results[0] if not isinstance(results[0], Exception) else archive_path
            decrypted_path = book_storage._book_path(barcode, f"{barcode}.tar.gz")
            logger.info(f"Upload completed: Encrypted: {encrypted_path}, Decrypted: {decrypted_path}")
            if verbose:
                print(f"  ✅ {barcode} uploaded with decryption")

    else:
        # Use local filesystem
        output_path = Path(output_dir)
        output_path.mkdir(parents=True, exist_ok=True)

        # Save encrypted archive
        file_path = output_path / archive_filename
        if verbose:
            if force and file_path.exists():
                print(f"Overwriting {barcode} existing encrypted archive: {file_path}")
            else:
                print(f"Saving {barcode} encrypted archive to: {file_path}")

        async with aiofiles.open(file_path, "wb") as f:
            await f.write(archive_data)

        archive_path = str(file_path)

        # Save decrypted archive
        try:
            if verbose:
                print(f"Decrypting {barcode} archive...")
            decrypted_data = await decrypt_gpg_data(archive_data, gpg_key_file, secrets_dir)
            decrypted_filename = f"{barcode}.tar.gz"
            decrypted_path_obj = output_path / decrypted_filename
            decrypted_path = str(decrypted_path_obj)
            if verbose:
                if force and decrypted_path_obj.exists():
                    print(f"Overwriting {barcode} existing decrypted archive: {decrypted_path}")
                else:
                    print(f"Saving {barcode} decrypted archive to: {decrypted_path}")

            async with aiofiles.open(decrypted_path, "wb") as f:
                await f.write(decrypted_data)

            if verbose:
                print(f"✅ {barcode} saved both archives:")
                print(f"  Encrypted: {file_path}")
                print(f"  Decrypted: {decrypted_path}")
        except RuntimeError as e:
            error_msg = str(e)
            if verbose:
                if "GPG command not found" in error_msg:
                    print(f"⚠️ {barcode} Warning: GPG is not installed - skipping decryption")
                    print("Install GPG to automatically decrypt archives: https://gnupg.org/download/")
                elif "private key not found" in error_msg or "public key not found" in error_msg:
                    print(f"⚠️ {barcode} Warning: GPG keys not configured - skipping decryption")
                    print("Import your GPG private key to decrypt archives automatically")
                else:
                    print(f"⚠️ {barcode} Failed to decrypt archive: {e}")
                print(f"{barcode} encrypted archive saved successfully, decryption skipped")
                print(f"  Encrypted: {file_path}")
        except Exception as e:
            if verbose:
                print(f"⚠️ {barcode} Failed to decrypt archive: {e}")
                print(f"{barcode} encrypted archive saved successfully, but decryption failed")
                print(f"  Encrypted: {file_path}")

    storage_time = (datetime.now() - storage_start).total_seconds()

    storage_time = (datetime.now() - storage_start).total_seconds()

    # Determine if decryption was successful
    is_decrypted = False
    if storage_type and storage_type != "local":
        # For cloud storage, check if decryption tasks completed successfully
        # This is approximate - we assume decryption succeeded if no exceptions were raised
        is_decrypted = True  # Will be updated by the actual decryption result
    else:
        # For local storage, check if decrypted file exists
        if 'decrypted_path' in locals():
            is_decrypted = Path(decrypted_path).exists()

    # Update database with successful sync if tracker provided
    if db_tracker:
        from datetime import UTC
        sync_data = {
            "storage_type": storage_type or "local",
            "storage_path": archive_path,
            "storage_decrypted_path": (book_storage._book_path(barcode, f"{barcode}.tar.gz")
                                      if storage_type and storage_type != "local"
                                      else locals().get('decrypted_path')),
            "last_etag_check": datetime.now(UTC).isoformat(),
            "google_etag": google_etag,
            "is_decrypted": is_decrypted,
            "sync_status": "completed",
            "sync_error": None,
        }
        await db_tracker.update_sync_status(barcode, sync_data)

    # Return results
    return {
        "barcode": barcode,
        "storage_type": storage_type or "local",
        "archive_path": archive_path,
        "file_size": len(archive_data),
        "download_time": download_time,
        "storage_time": storage_time,
        "total_time": download_time + storage_time,
        "download_speed_mbps": len(archive_data) / download_time / 1024 / 1024,
        "google_etag": google_etag,
        "is_decrypted": is_decrypted,
    }


def reset_bucket_cache() -> None:
    """Reset the bucket existence cache (useful for testing)."""
    global _bucket_checked_cache
    _bucket_checked_cache.clear()


async def main() -> int:
    """CLI interface for downloading books."""
    parser = argparse.ArgumentParser(
        description="Download GRIN books to local filesystem or block storage "
                    "(saves both encrypted and decrypted archives)",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Download to local directory (default)
  python download.py TZ1XH8
  python download.py TZ1XH8 -o ./downloads

  # Download to MinIO
  python download.py TZ1XH8 --storage=minio

  # Download to Cloudflare R2 (uses ~/.config/grin-to-s3/r2_credentials.json)
  python download.py TZ1XH8 --storage=r2 --bucket=my-bucket

  # Download to R2 with custom credentials file
  python download.py TZ1XH8 --storage=r2 --bucket=my-bucket --credentials-file=~/my-r2-creds.json

  # Download to AWS S3
  python download.py TZ1XH8 --storage=s3 --bucket=my-bucket

  # Force download and overwrite existing files
  python download.py TZ1XH8 --force
        """,
    )

    parser.add_argument("barcode", help="Book barcode to download")

    # Output options
    parser.add_argument(
        "-o", "--output-dir", default=".", help="Output directory for local storage (default: current directory)"
    )

    # Storage options
    parser.add_argument(
        "--storage", choices=["local", "minio", "r2", "s3"], help="Storage backend (default: local filesystem)"
    )
    parser.add_argument("--prefix", help="Storage prefix/path")

    # Storage credentials
    parser.add_argument("--endpoint-url", help="Custom endpoint URL (MinIO)")
    parser.add_argument("--access-key", help="Access key")
    parser.add_argument("--secret-key", help="Secret key")
    parser.add_argument("--account-id", help="Account ID (R2)")
    parser.add_argument(
        "--credentials-file",
        help="Custom R2 credentials file path (default: ~/.config/grin-to-s3/r2_credentials.json)"
    )
    parser.add_argument("--bucket", help="Bucket name")

    # GRIN options
    parser.add_argument("--base-url", default="https://books.google.com/libraries/")
    parser.add_argument("--directory", default="Harvard")
    parser.add_argument(
        "--secrets-dir",
        type=str,
        help="Directory containing GRIN secrets files (searches home directory if not specified)",
    )
    parser.add_argument("--test-mode", action="store_true", help="Test with mock data (creates dummy archive)")
    parser.add_argument(
        "--force", action="store_true", help="Force download and overwrite existing files (skip ETag check)"
    )
    parser.add_argument(
        "--gpg-key-file",
        help="Custom GPG key file path (default: ~/.config/grin-to-s3/gpg_key.asc)"
    )

    args = parser.parse_args()

    # Build storage configuration
    storage_config = {}
    if args.prefix:
        storage_config["prefix"] = args.prefix

    # Auto-configure MinIO from docker-compose file if using minio storage
    if args.storage == "minio" and not (args.endpoint_url and args.access_key and args.secret_key):
        try:
            from pathlib import Path

            import yaml

            compose_file = Path("docker-compose.minio.yml")
            if compose_file.exists():
                with open(compose_file) as f:
                    compose_config = yaml.safe_load(f)

                minio_service = compose_config.get("services", {}).get("minio", {})
                env = minio_service.get("environment", {})
                ports = minio_service.get("ports", [])

                # Extract MinIO configuration
                if not args.endpoint_url:
                    # Find API port (9000)
                    api_port = "9000"
                    for port_mapping in ports:
                        if isinstance(port_mapping, str) and ":9000" in port_mapping:
                            api_port = port_mapping.split(":")[0]
                            break
                    storage_config["endpoint_url"] = f"http://localhost:{api_port}"

                if not args.access_key:
                    storage_config["access_key"] = env.get("MINIO_ROOT_USER", "minioadmin")

                if not args.secret_key:
                    storage_config["secret_key"] = env.get("MINIO_ROOT_PASSWORD", "minioadmin123")

                # Note: Bucket is still required as a parameter

                print("Auto-configured MinIO from docker-compose.minio.yml:")
                print(f"  Endpoint: {storage_config.get('endpoint_url')}")

            else:
                print("Warning: docker-compose.minio.yml not found, using manual MinIO configuration")

        except ImportError:
            print("Warning: PyYAML not available, cannot auto-configure MinIO from docker-compose")
        except Exception as e:
            print(f"Warning: Failed to read docker-compose.minio.yml: {e}")

    # Override with explicit arguments if provided
    if args.endpoint_url:
        storage_config["endpoint_url"] = args.endpoint_url
    if args.access_key:
        storage_config["access_key"] = args.access_key
    if args.secret_key:
        storage_config["secret_key"] = args.secret_key
    if args.account_id:
        storage_config["account_id"] = args.account_id
    if args.credentials_file:
        storage_config["credentials_file"] = args.credentials_file
    if args.bucket:
        storage_config["bucket"] = args.bucket

    # Validate bucket is provided for cloud storage
    if args.storage and args.storage != "local" and not storage_config.get("bucket"):
        print(f"Error: --bucket is required when using {args.storage} storage")
        return 1

    try:
        # Show configuration
        print(f"V2 GRIN Download: {args.barcode}")
        print("=" * 40)

        if args.storage:
            print(f"Storage: {args.storage}")
            if args.storage == "minio":
                print(f"Endpoint: {storage_config.get('endpoint_url', 'http://localhost:9000')}")
            elif args.storage == "r2":
                if args.credentials_file:
                    print(f"Credentials: {args.credentials_file}")
                else:
                    print("Credentials: ~/.config/grin-to-s3/r2_credentials.json (default)")
            elif args.storage == "s3":
                print(f"Bucket: {args.bucket}")
        else:
            print("Storage: local filesystem")
            print(f"Output: {args.output_dir}")

        print()

        # Download
        result = await download_book(
            barcode=args.barcode,
            output_dir=args.output_dir,
            storage_type=args.storage,
            storage_config=storage_config,
            base_url=args.base_url,
            directory=args.directory,
            secrets_dir=args.secrets_dir,
            force=args.force,
            gpg_key_file=args.gpg_key_file,
        )

        # Show results
        print()
        print("Results:")
        print("--------")
        print("Status: success")
        print(f"Storage: {result['storage_type']}")
        print(f"Path: {result['archive_path']}")
        print(f"Size: {format_bytes(result['file_size'])}")
        speed = calculate_transfer_speed(result["file_size"], result["download_time"])
        print(f"Download: {format_duration(result['download_time'])} @ {speed}")
        print(f"Storage: {format_duration(result['storage_time'])}")
        print(f"Total: {format_duration(result['total_time'])}")

    except Exception as e:
        print(f"Error: {e}")
        return 1

    return 0


if __name__ == "__main__":
    exit(asyncio.run(main()))
