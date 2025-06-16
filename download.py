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

from auth import GRINAuth
from common import (
    calculate_transfer_speed,
    create_http_session,
    create_storage_from_config,
    format_bytes,
    format_duration,
)
from storage import BookStorage


async def download_book(
    barcode: str,
    output_dir: str = ".",
    storage_type: str | None = None,
    storage_config: dict | None = None,
    base_url: str = "https://books.google.com/libraries/",
    directory: str = "Harvard",
    credentials_file: str = ".creds.json",
    secrets_file: str = ".secrets",
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
        credentials_file: OAuth2 credentials
        secrets_file: OAuth2 secrets

    Returns:
        Dict with download results
    """
    print(f"Downloading book: {barcode}")

    # Set up GRIN authentication
    auth = GRINAuth(secrets_file=secrets_file, credentials_file=credentials_file)

    # Construct GRIN URL
    archive_filename = f"{barcode}.tar.gz.gpg"
    grin_url = f"{base_url}{directory}/{archive_filename}"

    print(f"Source: {grin_url}")

    # Download from GRIN
    download_start = datetime.now()

    async with create_http_session() as session:
        response = await auth.make_authenticated_request(session, grin_url)

        # Collect data
        chunks = []
        total_bytes = 0

        async for chunk in response.content.iter_chunked(1024 * 1024):  # 1MB chunks
            chunks.append(chunk)
            total_bytes += len(chunk)

            if len(chunks) % 10 == 0:  # Progress every 10MB
                print(f"Downloaded {format_bytes(total_bytes)}...")

    archive_data = b"".join(chunks)
    download_time = (datetime.now() - download_start).total_seconds()

    print(f"Download complete: {format_bytes(len(archive_data))} in {format_duration(download_time)}")
    print(f"Speed: {calculate_transfer_speed(len(archive_data), download_time)}")

    # Save to storage
    storage_start = datetime.now()

    if storage_type and storage_type != "local":
        # Use block storage
        storage = create_storage_from_config(storage_type, storage_config or {})
        book_storage = BookStorage(storage, base_prefix=(storage_config or {}).get("prefix", "grin-books"))

        print(f"Saving to {storage_type} storage...")
        archive_path = await book_storage.save_archive(barcode, archive_data)
        await book_storage.save_timestamp(barcode)

    else:
        # Use local filesystem
        output_path = Path(output_dir)
        output_path.mkdir(parents=True, exist_ok=True)

        file_path = output_path / archive_filename
        print(f"Saving to: {file_path}")

        async with aiofiles.open(file_path, "wb") as f:
            await f.write(archive_data)

        archive_path = str(file_path)

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
        description="Download GRIN books to local filesystem or block storage",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Download to local directory (default)
  python download.py TZ1XH8
  python download.py TZ1XH8 -o ./downloads

  # Download to MinIO
  python download.py TZ1XH8 --storage=minio

  # Download to Cloudflare R2
  python download.py TZ1XH8 --storage=r2 --credentials-file=~/r2-creds.json

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
    parser.add_argument("--credentials-file", help="Credentials JSON file (R2)")
    parser.add_argument("--bucket", help="Bucket name (S3)")

    # GRIN options
    parser.add_argument("--base-url", default="https://books.google.com/libraries/")
    parser.add_argument("--directory", default="Harvard")
    parser.add_argument("--grin-credentials", default=".creds.json", help="GRIN OAuth2 credentials")
    parser.add_argument("--grin-secrets", default=".secrets", help="GRIN OAuth2 secrets")

    args = parser.parse_args()

    # Build storage configuration
    storage_config = {}
    if args.prefix:
        storage_config["prefix"] = args.prefix
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
                    print(f"Account: {storage_config.get('account_id', 'from environment')}")
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
            credentials_file=args.grin_credentials,
            secrets_file=args.grin_secrets,
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
