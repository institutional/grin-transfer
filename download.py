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
from datetime import datetime
from pathlib import Path

import aiofiles

from client import GRINClient
from common import (
    calculate_transfer_speed,
    create_http_session,
    create_storage_from_config,
    decrypt_gpg_data,
    format_bytes,
    format_duration,
)
from storage import BookStorage


async def _decrypt_and_save_archive(barcode: str, encrypted_data: bytes, book_storage: BookStorage) -> None:
    """Decrypt the archive and save the decrypted version."""
    try:
        print("Decrypting archive...")
        decrypted_data = await decrypt_gpg_data(encrypted_data)
        await book_storage.save_decrypted_archive(barcode, decrypted_data)
        print(f"✅ Saved decrypted archive: {book_storage._book_path(barcode, f'{barcode}.tar.gz')}")
    except Exception as e:
        print(f"⚠️ Failed to decrypt archive: {e}")
        print("Encrypted archive saved successfully, but decryption failed")


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

    Returns:
        Dict with download results
    """
    print(f"Downloading book: {barcode}")

    # Set up GRIN client
    client = GRINClient(base_url=base_url, secrets_dir=secrets_dir)

    # Construct GRIN URL
    archive_filename = f"{barcode}.tar.gz.gpg"
    grin_url = f"{base_url.rstrip('/')}/{directory}/{archive_filename}"

    print(f"Source: {grin_url}")

    # Check if book is in converted state first
    print("Checking if book is converted...")
    try:
        converted_text = await client.fetch_resource(directory, "_converted?format=text")
        converted_files = {line.split("\t")[0] for line in converted_text.strip().split("\n") if line.strip()}

        if archive_filename not in converted_files:
            raise ValueError(f"Book {barcode} is not in converted state. "
                           f"Only converted books have downloadable archives. "
                           f"Check GRIN /_converted endpoint to see available books.")

        print("Book is converted. Checking if archive file exists...")

    except Exception as e:
        print(f"Error checking converted status: {e}")
        raise

    # Check if archive file exists
    file_exists = await client.check_file_exists(directory, archive_filename)
    if not file_exists:
        raise FileNotFoundError(f"Archive {archive_filename} not found in GRIN directory {directory}. "
                               f"Book is converted but archive file is not accessible.")

    print("Archive found! Starting download...")

    # First check headers to see if we can avoid downloading
    print("Checking file headers...")

    async with create_http_session() as session:
        # Make HEAD request to get headers without downloading content
        head_response = await client.auth.make_authenticated_request(session, grin_url, method="HEAD")

        # Look for ETag or Content-MD5 headers
        etag = head_response.headers.get('ETag', '').strip('"')
        content_md5 = head_response.headers.get('Content-MD5', '')
        content_length = head_response.headers.get('Content-Length', '')

        print(f"File size: {format_bytes(int(content_length)) if content_length else 'unknown'}")
        if etag:
            print(f"Google ETag: {etag}")
        if content_md5:
            print(f"Google Content-MD5: {content_md5}")

        # Check if we can skip download entirely
        if storage_type and storage_type != "local" and (etag or content_md5):
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
                print("Archive already exists, checking if Google's version matches...")

                # Check if we have the same Google file using stored metadata
                if etag and await book_storage.archive_matches_google_etag(barcode, etag):
                    print("✅ File already exists with identical content (matched Google's ETag), "
                          "skipping download entirely")
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

        print("Proceeding with download...")

    # Download from GRIN
    download_start = datetime.now()

    # Capture Google ETag for later use (from the HEAD request above)
    google_etag = None
    if 'etag' in locals():
        google_etag = etag

    async with create_http_session() as session:
        response = await client.auth.make_authenticated_request(session, grin_url)

        # Collect data
        chunks = []
        total_bytes = 0
        last_progress_bytes = 0
        progress_threshold = 25 * 1024 * 1024  # 25MB

        async for chunk in response.content.iter_chunked(1024 * 1024):  # 1MB chunks
            chunks.append(chunk)
            total_bytes += len(chunk)

            # Print progress every 25MB
            if total_bytes - last_progress_bytes >= progress_threshold:
                print(f"Downloaded {format_bytes(total_bytes)}...")
                last_progress_bytes = total_bytes

    archive_data = b"".join(chunks)
    download_time = (datetime.now() - download_start).total_seconds()

    print(f"Download complete: {format_bytes(len(archive_data))} in {format_duration(download_time)}")
    print(f"Speed: {calculate_transfer_speed(len(archive_data), download_time)}")

    # Save to storage
    storage_start = datetime.now()

    if storage_type and storage_type != "local":
        # Create storage first
        storage = create_storage_from_config(storage_type, storage_config or {})

        # Ensure bucket exists for MinIO/S3
        bucket_name = (storage_config or {}).get("bucket", "UNKNOWN")
        print(f"Checking bucket '{bucket_name}' for {storage_type}...")

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
                    print(f"Bucket '{bucket_name}' exists")
                except ClientError as e:
                    error_code = e.response['Error']['Code']
                    if error_code == '404':
                        print(f"Bucket '{bucket_name}' does not exist. Creating...")
                        s3_client.create_bucket(Bucket=bucket_name)
                        print(f"✅ Created bucket '{bucket_name}'")
                    else:
                        raise RuntimeError(f"Cannot access bucket '{bucket_name}': {e}") from e

            except NoCredentialsError as e:
                raise RuntimeError(f"No credentials available for {storage_type}") from e
            except Exception as e:
                raise RuntimeError(f"Bucket check failed: {e}") from e

        print(f"Bucket '{bucket_name}' is ready!")

        # For S3-like storage, include bucket in the path
        base_prefix = (storage_config or {}).get("prefix", "grin-books")
        if storage_type in ("minio", "s3", "r2"):
            bucket = (storage_config or {}).get("bucket")
            if bucket:
                base_prefix = f"{bucket}/{base_prefix}"

        book_storage = BookStorage(storage, base_prefix=base_prefix)


        # Check if file already exists with same Google ETag
        if await book_storage.archive_exists(barcode):
            if google_etag and await book_storage.archive_matches_google_etag(barcode, google_etag):
                print("✅ Archive already exists with identical content (Google ETag match), skipping upload")
                archive_path = book_storage._book_path(barcode, f"{barcode}.tar.gz.gpg")
                # Still update timestamp to record this download attempt
                await book_storage.save_timestamp(barcode)
            else:
                print("Archive exists but Google ETag differs (or missing), uploading new version...")
                archive_path = await book_storage.save_archive(barcode, archive_data, google_etag)
                await book_storage.save_timestamp(barcode)

                # Decrypt and save decrypted version
                await _decrypt_and_save_archive(barcode, archive_data, book_storage)
        else:
            print(f"Saving to {storage_type} storage...")
            archive_path = await book_storage.save_archive(barcode, archive_data, google_etag)
            await book_storage.save_timestamp(barcode)

            # Decrypt and save decrypted version
            await _decrypt_and_save_archive(barcode, archive_data, book_storage)

    else:
        # Use local filesystem
        output_path = Path(output_dir)
        output_path.mkdir(parents=True, exist_ok=True)

        # Save encrypted archive
        file_path = output_path / archive_filename
        print(f"Saving encrypted archive to: {file_path}")

        async with aiofiles.open(file_path, "wb") as f:
            await f.write(archive_data)

        archive_path = str(file_path)

        # Save decrypted archive
        try:
            print("Decrypting archive...")
            decrypted_data = await decrypt_gpg_data(archive_data)
            decrypted_filename = f"{barcode}.tar.gz"
            decrypted_path = output_path / decrypted_filename
            print(f"Saving decrypted archive to: {decrypted_path}")

            async with aiofiles.open(decrypted_path, "wb") as f:
                await f.write(decrypted_data)

            print(f"✅ Saved decrypted archive: {decrypted_path}")
        except Exception as e:
            print(f"⚠️ Failed to decrypt archive: {e}")
            print("Encrypted archive saved successfully, but decryption failed")

    storage_time = (datetime.now() - storage_start).total_seconds()

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
    }


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
