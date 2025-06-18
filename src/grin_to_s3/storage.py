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

    async def save_decrypted_archive(self, barcode: str, archive_data: bytes) -> str:
        """Save decrypted archive (.tar.gz)."""
        filename = f"{barcode}.tar.gz"
        path = self._book_path(barcode, filename)
        await self.storage.write_bytes(path, archive_data)
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
