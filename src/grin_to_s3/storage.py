"""
Storage Abstraction Layer using fsspec

Provides platform-independent storage operations.
Supports S3, Azure Blob, GCS, local filesystem, and more through fsspec.
"""

import asyncio
import json
from collections.abc import AsyncGenerator
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

import fsspec


class StorageError(Exception):
    """Base exception for storage operations."""

    pass


class StorageNotFoundError(StorageError):
    """Raised when storage object doesn't exist."""

    pass


class StorageConfig:
    """Configuration for different storage backends."""

    def __init__(self, protocol: str = "file", endpoint_url: str | None = None, **kwargs: Any) -> None:
        self.protocol = protocol
        self.endpoint_url = endpoint_url
        self.options = kwargs

    @classmethod
    def s3(cls, bucket: str, **kwargs: Any) -> "StorageConfig":
        """Configure for AWS S3."""
        return cls(protocol="s3", **kwargs)

    @classmethod
    def r2(cls, account_id: str, access_key: str, secret_key: str, **kwargs: Any) -> "StorageConfig":
        """Configure for Cloudflare R2 storage."""
        endpoint_url = f"https://{account_id}.r2.cloudflarestorage.com"
        return cls(protocol="s3", endpoint_url=endpoint_url, key=access_key, secret=secret_key, **kwargs)

    @classmethod
    def gcs(cls, project: str, **kwargs: Any) -> "StorageConfig":
        """Configure for Google Cloud Storage."""
        return cls(protocol="gcs", project=project, **kwargs)

    @classmethod
    def local(cls, base_path: str = ".") -> "StorageConfig":
        """Configure for local filesystem."""
        return cls(protocol="file", base_path=base_path)

    @classmethod
    def minio(cls, endpoint_url: str, access_key: str, secret_key: str) -> "StorageConfig":
        """Configure for MinIO or S3-compatible storage."""
        return cls(protocol="s3", endpoint_url=endpoint_url, key=access_key, secret=secret_key)


class Storage:
    """
    Async storage abstraction layer.

    Provides unified interface for cloud storage operations using fsspec.
    """

    def __init__(self, config: StorageConfig):
        self.config = config
        self._fs = None

    def _get_fs(self) -> Any:
        """Get filesystem instance (lazy initialization)."""
        if self._fs is None:
            options = self.config.options.copy()
            if self.config.endpoint_url:
                options["endpoint_url"] = self.config.endpoint_url
            self._fs = fsspec.filesystem(self.config.protocol, **options)
        return self._fs

    def _normalize_path(self, path: str) -> str:
        """Normalize path for the storage backend."""
        if self.config.protocol == "file":
            # For local filesystem, ensure absolute path
            if not path.startswith("/"):
                base_path = self.config.options.get("base_path", ".")
                path = str(Path(base_path) / path)
        elif self.config.protocol in ("s3", "gcs", "abfs"):
            # For cloud storage, ensure no leading slash
            path = path.lstrip("/")
        return path

    async def exists(self, path: str) -> bool:
        """Check if object exists at path."""
        loop = asyncio.get_event_loop()
        fs = self._get_fs()
        normalized_path = self._normalize_path(path)

        try:
            result = await loop.run_in_executor(None, fs.exists, normalized_path)
            return result
        except Exception:
            return False

    async def read_bytes(self, path: str) -> bytes:
        """Read file as bytes."""
        loop = asyncio.get_event_loop()
        fs = self._get_fs()
        normalized_path = self._normalize_path(path)

        try:
            result = await loop.run_in_executor(None, fs.cat, normalized_path)
            return result
        except FileNotFoundError:
            raise StorageNotFoundError(f"Object not found: {path}") from None

    async def read_text(self, path: str, encoding: str = "utf-8") -> str:
        """Read file as text."""
        data = await self.read_bytes(path)
        return data.decode(encoding)

    async def write_bytes(self, path: str, data: bytes) -> None:
        """Write bytes to file."""
        if self.config.protocol == "s3":
            # Use aioboto3 for non-blocking S3 uploads
            try:

                import aioboto3

                normalized_path = self._normalize_path(path)

                # Use the credentials from the storage config
                session_kwargs = {
                    'aws_access_key_id': self.config.options.get('key'),
                    'aws_secret_access_key': self.config.options.get('secret'),
                }

                # Add endpoint URL if present
                if self.config.endpoint_url:
                    session_kwargs['endpoint_url'] = self.config.endpoint_url

                session = aioboto3.Session()
                async with session.client('s3', **session_kwargs) as s3_client:
                    # Parse bucket and key from path
                    path_parts = normalized_path.split('/', 1)
                    if len(path_parts) == 2:
                        bucket, key = path_parts

                        # Use multipart upload for files >5MB, single-part for smaller files
                        if len(data) > 5 * 1024 * 1024:
                            await self._multipart_upload(s3_client, bucket, key, data)
                        else:
                            await s3_client.put_object(
                                Bucket=bucket,
                                Key=key,
                                Body=data
                            )
                        return
            except Exception as e:
                print(f"Failed to write with aioboto3, falling back to sync: {e}")
                # Fall through to sync method

        # Use sync method for local filesystem or as fallback
        loop = asyncio.get_event_loop()
        fs = self._get_fs()
        normalized_path = self._normalize_path(path)

        # Ensure parent directories exist for local filesystem
        if self.config.protocol == "file":
            parent = Path(normalized_path).parent
            parent.mkdir(parents=True, exist_ok=True)

        await loop.run_in_executor(None, fs.pipe, normalized_path, data)

    async def write_file(self, path: str, file_path: str) -> None:
        """Stream upload file directly without loading into memory."""
        if self.config.protocol == "s3":
            try:
                import os

                import aioboto3
                import aiofiles

                normalized_path = self._normalize_path(path)

                # Use the credentials from the storage config
                session_kwargs = {
                    'aws_access_key_id': self.config.options.get('key'),
                    'aws_secret_access_key': self.config.options.get('secret'),
                }

                # Add endpoint URL if present
                if self.config.endpoint_url:
                    session_kwargs['endpoint_url'] = self.config.endpoint_url

                session = aioboto3.Session()
                async with session.client('s3', **session_kwargs) as s3_client:
                    # Parse bucket and key from path
                    path_parts = normalized_path.split('/', 1)
                    if len(path_parts) == 2:
                        bucket, key = path_parts

                        # Get file size for multipart decision
                        file_size = os.path.getsize(file_path)

                        if file_size > 50 * 1024 * 1024:
                            # Use multipart upload for files >50MB
                            await self._multipart_upload_from_file(s3_client, bucket, key, file_path)
                        else:
                            # Single-part upload for files ≤50MB - much faster, single API call
                            async with aiofiles.open(file_path, 'rb') as f:
                                file_data = await f.read()
                                await s3_client.put_object(
                                    Bucket=bucket,
                                    Key=key,
                                    Body=file_data
                                )
                        return
            except Exception as e:
                raise RuntimeError(f"Failed to stream upload file: {e}") from e
        else:
            # For non-S3 storage, read file and use write_bytes
            import aiofiles
            async with aiofiles.open(file_path, 'rb') as f:
                data = await f.read()
            await self.write_bytes(path, data)

    async def write_bytes_with_metadata(self, path: str, data: bytes, metadata: dict[str, str]) -> None:
        """Write bytes to file with S3 metadata."""
        if self.config.protocol != "s3":
            # Fall back to regular write for non-S3 storage
            await self.write_bytes(path, data)
            return

        normalized_path = self._normalize_path(path)

        try:
            import aioboto3

            # Use the credentials from the storage config
            session_kwargs = {
                'aws_access_key_id': self.config.options.get('key'),
                'aws_secret_access_key': self.config.options.get('secret'),
            }

            # Add endpoint URL if present
            if self.config.endpoint_url:
                session_kwargs['endpoint_url'] = self.config.endpoint_url

            session = aioboto3.Session()
            async with session.client('s3', **session_kwargs) as s3_client:
                # Parse bucket and key from path
                path_parts = normalized_path.split('/', 1)
                if len(path_parts) == 2:
                    bucket, key = path_parts
                    await s3_client.put_object(
                        Bucket=bucket,
                        Key=key,
                        Body=data,
                        Metadata=metadata
                    )
        except Exception as e:
            print(f"Failed to write with metadata: {e}")
            # Fall back to regular write
            await self.write_bytes(path, data)

    async def _multipart_upload(self, s3_client, bucket: str, key: str, data: bytes) -> None:
        """Upload large files using multipart upload for better performance."""
        import io

        # Initiate multipart upload
        response = await s3_client.create_multipart_upload(
            Bucket=bucket,
            Key=key
        )

        upload_id = response['UploadId']
        parts = []
        part_number = 1
        chunk_size = 8 * 1024 * 1024  # 8MB chunks

        try:
            data_stream = io.BytesIO(data)

            while True:
                chunk = data_stream.read(chunk_size)
                if not chunk:
                    break

                # Upload part
                part_response = await s3_client.upload_part(
                    Bucket=bucket,
                    Key=key,
                    PartNumber=part_number,
                    UploadId=upload_id,
                    Body=chunk
                )

                parts.append({
                    'ETag': part_response['ETag'],
                    'PartNumber': part_number
                })

                part_number += 1

            # Complete multipart upload
            await s3_client.complete_multipart_upload(
                Bucket=bucket,
                Key=key,
                UploadId=upload_id,
                MultipartUpload={'Parts': parts}
            )

        except Exception as e:
            # Abort multipart upload on error
            try:
                await s3_client.abort_multipart_upload(
                    Bucket=bucket,
                    Key=key,
                    UploadId=upload_id
                )
            except:
                pass  # Ignore errors when aborting

            raise e

    async def _multipart_upload_from_file(self, s3_client, bucket: str, key: str, file_path: str) -> None:
        """Upload large files using multipart upload directly from file for better performance."""
        import os

        import aiofiles

        # Get file size
        file_size = os.path.getsize(file_path)

        # Initiate multipart upload
        response = await s3_client.create_multipart_upload(
            Bucket=bucket,
            Key=key
        )

        upload_id = response['UploadId']
        parts = []
        part_number = 1
        chunk_size = 50 * 1024 * 1024  # 50MB chunks for better R2 performance

        try:
            async with aiofiles.open(file_path, 'rb') as f:
                while True:
                    chunk = await f.read(chunk_size)
                    if not chunk:
                        break

                    # Upload part
                    part_response = await s3_client.upload_part(
                        Bucket=bucket,
                        Key=key,
                        PartNumber=part_number,
                        UploadId=upload_id,
                        Body=chunk
                    )

                    parts.append({
                        'ETag': part_response['ETag'],
                        'PartNumber': part_number
                    })

                    part_number += 1

                # Complete multipart upload
                await s3_client.complete_multipart_upload(
                    Bucket=bucket,
                    Key=key,
                    UploadId=upload_id,
                    MultipartUpload={'Parts': parts}
                )

        except Exception as e:
            # Abort multipart upload on error
            try:
                await s3_client.abort_multipart_upload(
                    Bucket=bucket,
                    Key=key,
                    UploadId=upload_id
                )
            except:
                pass  # Ignore errors when aborting

            raise e

    async def write_text(self, path: str, text: str, encoding: str = "utf-8") -> None:
        """Write text to file."""
        data = text.encode(encoding)
        await self.write_bytes(path, data)

    async def stream_download(self, path: str, chunk_size: int = 8192) -> AsyncGenerator[bytes, None]:
        """Stream download file in chunks."""
        loop = asyncio.get_event_loop()
        fs = self._get_fs()
        normalized_path = self._normalize_path(path)

        try:
            # Use fsspec's open method for streaming
            def _open_file():
                return fs.open(normalized_path, "rb")

            file_obj = await loop.run_in_executor(None, _open_file)
            try:
                while True:

                    def _read_chunk():
                        return file_obj.read(chunk_size)

                    chunk = await loop.run_in_executor(None, _read_chunk)
                    if not chunk:
                        break
                    yield chunk
            finally:

                def _close_file():
                    file_obj.close()

                await loop.run_in_executor(None, _close_file)
        except FileNotFoundError:
            raise StorageNotFoundError(f"Object not found: {path}") from None

    async def list_objects(self, prefix: str = "") -> list[str]:
        """List objects with given prefix."""
        loop = asyncio.get_event_loop()
        fs = self._get_fs()
        normalized_prefix = self._normalize_path(prefix)

        try:
            result = await loop.run_in_executor(None, fs.ls, normalized_prefix)
            return result
        except Exception:
            return []

    async def list_objects_detailed(self, prefix: str = "") -> list[dict]:
        """List objects with detailed metadata including sizes."""
        loop = asyncio.get_event_loop()
        fs = self._get_fs()
        normalized_prefix = self._normalize_path(prefix)

        try:
            result = await loop.run_in_executor(None, fs.ls, normalized_prefix, True)
            return result if result else []
        except Exception:
            return []

    async def delete(self, path: str) -> None:
        """Delete object at path."""
        loop = asyncio.get_event_loop()
        fs = self._get_fs()
        normalized_path = self._normalize_path(path)

        await loop.run_in_executor(None, fs.rm, normalized_path)


class BookStorage:
    """
    Storage abstraction specifically for book archive operations.

    Implements storage patterns for book data organization.
    """

    def __init__(self, storage: Storage, base_prefix: str = ""):
        self.storage = storage
        self.base_prefix = base_prefix.rstrip("/")

    def _book_path(self, barcode: str, filename: str) -> str:
        """Generate path for book file."""
        if self.base_prefix:
            return f"{self.base_prefix}/{barcode}/{filename}"
        return f"{barcode}/{filename}"

    async def save_archive(self, barcode: str, archive_data: bytes, google_etag: str | None = None) -> str:
        """Save encrypted archive (.tar.gz.gpg) with optional Google ETag metadata."""
        filename = f"{barcode}.tar.gz.gpg"
        path = self._book_path(barcode, filename)

        if self.storage.config.protocol == "s3" and google_etag:
            # Store Google's ETag as metadata for future comparison
            await self.storage.write_bytes_with_metadata(path, archive_data, {"google-etag": google_etag})
        else:
            await self.storage.write_bytes(path, archive_data)
        return path

    async def save_archive_from_file(self, barcode: str, archive_file_path: str, google_etag: str | None = None) -> str:
        """Save encrypted archive (.tar.gz.gpg) from file with optional Google ETag metadata."""
        filename = f"{barcode}.tar.gz.gpg"
        path = self._book_path(barcode, filename)

        if self.storage.config.protocol == "s3" and google_etag:
            # For S3 with metadata, we need to read the file and use write_bytes_with_metadata
            import aiofiles
            async with aiofiles.open(archive_file_path, 'rb') as f:
                archive_data = await f.read()
            await self.storage.write_bytes_with_metadata(path, archive_data, {"google-etag": google_etag})
        else:
            # Stream upload directly from file
            await self.storage.write_file(path, archive_file_path)
        return path

    async def save_decrypted_archive(self, barcode: str, archive_data: bytes) -> str:
        """Save decrypted archive (.tar.gz)."""
        filename = f"{barcode}.tar.gz"
        path = self._book_path(barcode, filename)
        await self.storage.write_bytes(path, archive_data)
        return path

    async def save_decrypted_archive_from_file(self, barcode: str, archive_file_path: str) -> str:
        """Save decrypted archive (.tar.gz) from file."""
        filename = f"{barcode}.tar.gz"
        path = self._book_path(barcode, filename)
        await self.storage.write_file(path, archive_file_path)
        return path

    async def save_text_json(self, barcode: str, pages: list[str]) -> str:
        """Save page text as JSON array."""
        filename = f"{barcode}.json"
        path = self._book_path(barcode, filename)
        json_data = json.dumps(pages, ensure_ascii=False, indent=2)
        await self.storage.write_text(path, json_data)
        return path

    async def save_timestamp(self, barcode: str, suffix: str = "retrieval") -> str:
        """Save retrieval timestamp."""
        filename = f"{barcode}.tar.gz.gpg.{suffix}"
        path = self._book_path(barcode, filename)
        timestamp = datetime.now(tz=UTC).isoformat()
        await self.storage.write_text(path, timestamp)
        return path

    async def archive_exists(self, barcode: str) -> bool:
        """Check if encrypted archive exists."""
        filename = f"{barcode}.tar.gz.gpg"
        path = self._book_path(barcode, filename)
        return await self.storage.exists(path)

    async def archive_matches_google_etag(self, barcode: str, google_etag: str) -> bool:
        """Check if existing archive was uploaded from the same Google file using stored metadata."""
        if self.storage.config.protocol != "s3":
            # Only supported for S3-compatible storage
            return False

        try:
            filename = f"{barcode}.tar.gz.gpg"
            path = self._book_path(barcode, filename)

            # Get object metadata using aioboto3 directly
            try:
                import aioboto3

                # Use the credentials from the storage config
                session_kwargs = {
                    'aws_access_key_id': self.storage.config.options.get('key'),
                    'aws_secret_access_key': self.storage.config.options.get('secret'),
                }

                # Add endpoint URL if present
                if self.storage.config.endpoint_url:
                    session_kwargs['endpoint_url'] = self.storage.config.endpoint_url

                session = aioboto3.Session()
                async with session.client('s3', **session_kwargs) as s3_client:
                    # Parse bucket and key from path
                    normalized_path = self.storage._normalize_path(path)
                    path_parts = normalized_path.split('/', 1)
                    if len(path_parts) == 2:
                        bucket, key = path_parts
                        response = await s3_client.head_object(Bucket=bucket, Key=key)
                        metadata = response.get('Metadata', {})
                    else:
                        metadata = {}
            except Exception:
                metadata = {}

            # Check if stored Google ETag matches
            stored_google_etag = metadata.get('google-etag', '')
            return stored_google_etag == google_etag

        except Exception:
            # If anything fails, assume no match
            return False


    async def get_archive(self, barcode: str) -> bytes:
        """Get encrypted archive data."""
        filename = f"{barcode}.tar.gz.gpg"
        path = self._book_path(barcode, filename)
        return await self.storage.read_bytes(path)

    async def stream_archive_download(self, barcode: str) -> AsyncGenerator[bytes, None]:
        """Stream download encrypted archive."""
        filename = f"{barcode}.tar.gz.gpg"
        path = self._book_path(barcode, filename)
        async for chunk in self.storage.stream_download(path):
            yield chunk


# Convenience functions for common configurations
def create_s3_storage(bucket: str, **kwargs: Any) -> Storage:
    """Create S3 storage instance."""
    config = StorageConfig.s3(bucket=bucket, **kwargs)
    return Storage(config)


def create_local_storage(base_path: str = ".") -> Storage:
    """Create local filesystem storage."""
    config = StorageConfig.local(base_path)
    return Storage(config)


def create_minio_storage(endpoint_url: str, access_key: str, secret_key: str, **kwargs: Any) -> Storage:
    """Create MinIO storage instance."""
    config = StorageConfig.minio(endpoint_url, access_key, secret_key)
    return Storage(config)


def create_r2_storage(account_id: str, access_key: str, secret_key: str, **kwargs: Any) -> Storage:
    """Create Cloudflare R2 storage instance."""
    config = StorageConfig.r2(account_id, access_key, secret_key)
    return Storage(config)


# Storage management functionality for CLI
try:
    import boto3
    _BOTO3_AVAILABLE = True
except ImportError:
    _BOTO3_AVAILABLE = False


def _create_s3_client_from_storage(storage: Storage) -> "boto3.client":
    """Create boto3 S3 client from existing storage instance."""
    if not _BOTO3_AVAILABLE:
        raise ImportError("boto3 is required for S3 storage management operations")

    config = storage.config

    # Extract credentials from storage config
    access_key = config.options.get("key")
    secret_key = config.options.get("secret")
    endpoint_url = config.endpoint_url
    region = config.options.get("region", "auto")

    if not all([access_key, secret_key]):
        raise ValueError("Storage instance missing required S3 credentials")

    client_kwargs = {
        'aws_access_key_id': access_key,
        'aws_secret_access_key': secret_key,
        'region_name': region,
    }

    if endpoint_url:
        client_kwargs['endpoint_url'] = endpoint_url

    return boto3.client('s3', **client_kwargs)


def list_bucket_files(storage: Storage, bucket: str, prefix: str = "") -> list[tuple[str, int]]:
    """List all files in bucket with sizes using S3 API for performance."""
    if not storage.config.protocol == "s3":
        raise ValueError("Fast bucket listing only supported for S3-compatible storage")

    s3_client = _create_s3_client_from_storage(storage)

    list_kwargs = {'Bucket': bucket}
    if prefix:
        list_kwargs['Prefix'] = prefix

    paginator = s3_client.get_paginator('list_objects_v2')

    files = []
    for page in paginator.paginate(**list_kwargs):
        contents = page.get('Contents', [])
        for obj in contents:
            files.append((obj['Key'], obj['Size']))

    return files


def get_bucket_stats(storage: Storage, bucket: str, prefix: str = "") -> tuple[int, int]:
    """Get bucket file count and total size using S3 API."""
    if not storage.config.protocol == "s3":
        raise ValueError("Fast bucket stats only supported for S3-compatible storage")

    s3_client = _create_s3_client_from_storage(storage)

    list_kwargs = {'Bucket': bucket}
    if prefix:
        list_kwargs['Prefix'] = prefix

    paginator = s3_client.get_paginator('list_objects_v2')

    total_files = 0
    total_size = 0

    for page in paginator.paginate(**list_kwargs):
        contents = page.get('Contents', [])
        for obj in contents:
            total_files += 1
            total_size += obj['Size']

    return total_files, total_size


def delete_bucket_contents(storage: Storage, bucket: str, prefix: str = "") -> tuple[int, int]:
    """Delete all objects in bucket with given prefix. Returns (deleted_count, failed_count)."""
    if not storage.config.protocol == "s3":
        raise ValueError("Fast bucket deletion only supported for S3-compatible storage")

    s3_client = _create_s3_client_from_storage(storage)

    # First, get list of all objects to delete
    list_kwargs = {'Bucket': bucket}
    if prefix:
        list_kwargs['Prefix'] = prefix

    paginator = s3_client.get_paginator('list_objects_v2')
    objects_to_delete = []

    for page in paginator.paginate(**list_kwargs):
        contents = page.get('Contents', [])
        for obj in contents:
            objects_to_delete.append(obj['Key'])

    if not objects_to_delete:
        return 0, 0

    # Delete in batches of 1000 (S3 API limit)
    deleted_count = 0
    failed_count = 0
    batch_size = 1000

    for i in range(0, len(objects_to_delete), batch_size):
        batch = objects_to_delete[i:i + batch_size]
        delete_objects = [{'Key': filename} for filename in batch]

        try:
            response = s3_client.delete_objects(
                Bucket=bucket,
                Delete={'Objects': delete_objects}
            )

            # Count successful deletions
            deleted_count += len(response.get('Deleted', []))

            # Count failures
            failed_count += len(response.get('Errors', []))

        except Exception:
            failed_count += len(batch)

    return deleted_count, failed_count


def format_size(size_bytes: int) -> str:
    """Format size in human-readable format."""
    for unit in ['B', 'KB', 'MB', 'GB', 'TB']:
        if size_bytes < 1024.0:
            return f"{size_bytes:.1f} {unit}"
        size_bytes /= 1024.0
    return f"{size_bytes:.1f} PB"


# CLI functionality for storage management
import argparse
import logging
import sys

logger = logging.getLogger(__name__)


async def cmd_ls(args) -> None:
    """List contents of all storage buckets."""
    from grin_to_s3.common import create_storage_from_config
    from grin_to_s3.run_config import (
        apply_run_config_to_args,
        build_storage_config_dict,
        setup_run_database_path,
    )

    # Set up database path and apply run configuration
    db_path = setup_run_database_path(args, args.run_name)
    print(f"Using run: {args.run_name}")
    print(f"Database: {db_path}")

    args.db_path = db_path
    apply_run_config_to_args(args, args.db_path)

    # Get run configuration
    config_path = Path(args.db_path).parent / "run_config.json"
    if not config_path.exists():
        print("❌ Error: No run configuration found")
        print("Run a collection first to create configuration")
        sys.exit(1)

    try:
        with open(config_path) as f:
            run_config = json.load(f)

        storage_config_dict = run_config.get("storage_config")
        if not storage_config_dict:
            print("❌ Error: No storage configuration found in run config")
            sys.exit(1)

        storage_type = storage_config_dict["type"]

        # Build full storage config with credentials
        from argparse import Namespace
        temp_args = Namespace(
            storage=storage_type,
            secrets_dir=run_config.get("secrets_dir"),
            bucket_raw=None,
            bucket_meta=None,
            bucket_full=None,
            storage_config=None
        )

        credentials_config = build_storage_config_dict(temp_args)
        storage_config = credentials_config.copy()
        storage_config.update(storage_config_dict.get("config", {}))
        if "prefix" in storage_config_dict:
            storage_config["prefix"] = storage_config_dict["prefix"]

        print(f"\nStorage Listing for {args.run_name}")
        print("=" * 60)
        print(f"Storage Type: {storage_type}")

        prefix = storage_config.get("prefix", "")
        if prefix:
            print(f"Using prefix: {prefix}")
        print()

        # Create storage instance
        storage = create_storage_from_config(storage_type, storage_config)

        # Summarize each bucket
        buckets = {
            "Raw Data": storage_config.get("bucket_raw"),
            "Metadata": storage_config.get("bucket_meta"),
            "Full Text": storage_config.get("bucket_full"),
        }

        total_files = 0
        total_size = 0

        for bucket_name, bucket in buckets.items():
            if not bucket:
                print(f"{bucket_name} Bucket: Not configured")
                continue

            print(f"{bucket_name} Bucket: {bucket}")
            try:
                if args.long:
                    # Long listing format with files and sizes
                    if storage_type in ("r2", "s3", "minio"):
                        files = list_bucket_files(storage, bucket, prefix)

                        if not files:
                            print("  (empty)")
                        else:
                            # Sort files by path
                            files.sort(key=lambda x: x[0])

                            for file_path, size in files:
                                if size > 0:
                                    size_str = format_size(size)
                                    print(f"  {file_path:60} {size_str:>10}")
                                else:
                                    print(f"  {file_path:60}        0 B")

                            print(f"  \nTotal: {len(files):,} files")

                        bucket_files, bucket_size = get_bucket_stats(storage, bucket, prefix)
                        total_files += bucket_files
                        total_size += bucket_size
                    else:
                        print("  ❌ Long listing not supported for this storage type")
                        # Fallback to simple count
                        objects = await storage.list_objects(f"{bucket}/{prefix}" if prefix else bucket)
                        bucket_files = len(objects)
                        total_files += bucket_files
                        print(f"  Total files: {bucket_files:,}")
                else:
                    # Regular summary format
                    if storage_type in ("r2", "s3", "minio"):
                        bucket_files, _ = get_bucket_stats(storage, bucket, prefix)
                    else:
                        objects = await storage.list_objects(f"{bucket}/{prefix}" if prefix else bucket)
                        bucket_files = len(objects)

                    total_files += bucket_files
                    print(f"  Total files: {bucket_files:,}")

            except Exception as e:
                print(f"  ❌ Error accessing bucket: {e}")

            print()

        print("Overall Summary:")
        print(f"  Total files across all buckets: {total_files:,}")
        if args.long:
            print(f"  Total storage used: {format_size(total_size)}")

    except Exception as e:
        import traceback
        print(f"❌ Error: {e}")
        traceback.print_exc()
        sys.exit(1)


async def cmd_rm(args) -> None:
    """Remove all contents from a storage bucket."""
    from grin_to_s3.common import create_storage_from_config
    from grin_to_s3.run_config import (
        apply_run_config_to_args,
        build_storage_config_dict,
        setup_run_database_path,
    )

    # Set up database path and apply run configuration
    db_path = setup_run_database_path(args, args.run_name)
    print(f"Using run: {args.run_name}")
    print(f"Database: {db_path}")

    args.db_path = db_path
    apply_run_config_to_args(args, args.db_path)

    # Get run configuration
    config_path = Path(args.db_path).parent / "run_config.json"
    if not config_path.exists():
        print("❌ Error: No run configuration found")
        print("Run a collection first to create configuration")
        sys.exit(1)

    try:
        with open(config_path) as f:
            run_config = json.load(f)

        storage_config_dict = run_config.get("storage_config")
        if not storage_config_dict:
            print("❌ Error: No storage configuration found in run config")
            sys.exit(1)

        storage_type = storage_config_dict["type"]

        # Build full storage config with credentials
        from argparse import Namespace
        temp_args = Namespace(
            storage=storage_type,
            secrets_dir=run_config.get("secrets_dir"),
            bucket_raw=None,
            bucket_meta=None,
            bucket_full=None,
            storage_config=None
        )

        credentials_config = build_storage_config_dict(temp_args)
        storage_config = credentials_config.copy()
        storage_config.update(storage_config_dict.get("config", {}))
        if "prefix" in storage_config_dict:
            storage_config["prefix"] = storage_config_dict["prefix"]

        # Find the bucket to remove
        buckets = {
            "raw": storage_config.get("bucket_raw"),
            "meta": storage_config.get("bucket_meta"),
            "full": storage_config.get("bucket_full"),
        }

        bucket_name = args.bucket_name
        if bucket_name not in buckets:
            print(f"❌ Error: Unknown bucket '{bucket_name}'")
            print(f"Available buckets: {', '.join(buckets.keys())}")
            sys.exit(1)

        bucket = buckets[bucket_name]
        if not bucket:
            print(f"❌ Error: Bucket '{bucket_name}' is not configured")
            sys.exit(1)

        prefix = storage_config.get("prefix", "")

        print(f"\nStorage Removal for {args.run_name}")
        print("=" * 60)
        print(f"Storage Type: {storage_type}")
        print(f"Target Bucket: {bucket} ({bucket_name})")
        if prefix:
            print(f"Using prefix: {prefix}")
        print()

        # Create storage instance
        storage = create_storage_from_config(storage_type, storage_config)

        # Check bucket contents
        print("Checking bucket contents...")
        try:
            if storage_type in ("r2", "s3", "minio"):
                files = list_bucket_files(storage, bucket, prefix)
                object_count = len(files)

                if object_count == 0:
                    print("Bucket is already empty")
                    return

                print(f"Found {object_count:,} objects to delete")

                if object_count <= 10:
                    print("\nObjects to be deleted:")
                    for i, (filename, _) in enumerate(files, 1):
                        print(f"  {i:2d}. {filename}")
                else:
                    print("\nFirst 5 objects to be deleted:")
                    for i, (filename, _) in enumerate(files[:5], 1):
                        print(f"  {i:2d}. {filename}")
                    print(f"  ... and {object_count - 5:,} more objects")
            else:
                print("❌ Error: Bucket removal only supported for S3-compatible storage")
                sys.exit(1)

        except Exception as e:
            print(f"❌ Error listing bucket contents: {e}")
            sys.exit(1)

        # Handle dry-run mode
        if args.dry_run:
            print(f"\n📋 DRY RUN: Would delete {object_count:,} objects from:")
            print(f"   Bucket: {bucket}")
            if prefix:
                print(f"   Prefix: {prefix}")
            print("\nTo actually delete these objects, run without --dry-run")
            return

        # Confirm deletion
        if not args.yes:
            print(f"\n⚠️  WARNING: This will permanently delete {object_count:,} objects from:")
            print(f"   Bucket: {bucket}")
            if prefix:
                print(f"   Prefix: {prefix}")
            print("\nThis action CANNOT be undone!")

            response = input(f"\nType 'DELETE' to confirm removal of {object_count:,} objects: ").strip()
            if response != "DELETE":
                print("Removal cancelled")
                return

        # Perform deletion
        print(f"\nDeleting {object_count:,} objects...")
        try:
            deleted_count, failed_count = delete_bucket_contents(storage, bucket, prefix)

            print("\nDeletion complete:")
            print(f"  Successfully deleted: {deleted_count:,}")
            if failed_count > 0:
                print(f"  Failed deletions: {failed_count:,}")
        except Exception as e:
            print(f"❌ Error during deletion: {e}")
            sys.exit(1)

    except Exception as e:
        import traceback
        print(f"❌ Error: {e}")
        traceback.print_exc()
        sys.exit(1)


async def main() -> None:
    """Main CLI entry point for storage management."""
    parser = argparse.ArgumentParser(
        description="GRIN storage management",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # List contents of all buckets (summary)
  python grin.py storage ls --run-name harvard_2024

  # Long format listing with recursive file listing
  python grin.py storage ls --run-name harvard_2024 -l

  # Remove all files from raw bucket
  python grin.py storage rm raw --run-name harvard_2024

  # Remove files with auto-confirm
  python grin.py storage rm meta --run-name harvard_2024 --yes
        """,
    )

    subparsers = parser.add_subparsers(dest="command", help="Storage commands")

    # List command
    ls_parser = subparsers.add_parser(
        "ls",
        help="List contents of all storage buckets",
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    ls_parser.add_argument("--run-name", required=True, help="Run name (e.g., harvard_2024)")
    ls_parser.add_argument("-l", "--long", action="store_true", help="Use long listing format with recursive file listing")

    # Remove command
    rm_parser = subparsers.add_parser(
        "rm",
        help="Remove all contents from a storage bucket",
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    rm_parser.add_argument("bucket_name", choices=["raw", "meta", "full"],
                          help="Bucket to clear (raw, meta, or full)")
    rm_parser.add_argument("--run-name", required=True, help="Run name (e.g., harvard_2024)")
    rm_parser.add_argument("--yes", "-y", action="store_true",
                          help="Auto-confirm without prompting (dangerous!)")
    rm_parser.add_argument("--dry-run", action="store_true",
                          help="Show what would be deleted without actually deleting")

    args = parser.parse_args()

    if not args.command:
        parser.print_help()
        sys.exit(1)

    if args.command == "ls":
        await cmd_ls(args)
    elif args.command == "rm":
        await cmd_rm(args)


if __name__ == "__main__":
    asyncio.run(main())
