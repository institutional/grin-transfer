"""
Storage Base Classes and Configuration

Core storage abstractions providing platform-independent storage operations.
Supports S3, Azure Blob, GCS, local filesystem, and more through fsspec.
"""

import asyncio
import logging
from collections.abc import AsyncGenerator
from typing import Any

import fsspec

logger = logging.getLogger(__name__)


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
    def r2(cls, endpoint_url: str, access_key: str, secret_key: str, **kwargs: Any) -> "StorageConfig":
        """Configure for Cloudflare R2 storage."""
        return cls(protocol="s3", endpoint_url=endpoint_url, key=access_key, secret=secret_key, **kwargs)

    @classmethod
    def gcs(cls, project: str, **kwargs: Any) -> "StorageConfig":
        """Configure for Google Cloud Storage."""
        return cls(protocol="gcs", project=project, **kwargs)

    @classmethod
    def local(cls, base_path: str) -> "StorageConfig":
        """Configure for local filesystem (base_path required)."""
        if not base_path:
            raise ValueError("Local storage requires explicit base_path")
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

    def is_s3_compatible(self) -> bool:
        """Check if storage backend is S3-compatible."""
        return self.config.protocol == "s3"

    def _normalize_path(self, path: str) -> str:
        """Normalize path for the storage backend."""
        if self.config.protocol == "file":
            # For local filesystem, ensure absolute path
            if not path.startswith("/"):
                from pathlib import Path

                base_path = self.config.options.get("base_path")
                if not base_path:
                    raise ValueError("Local storage requires explicit base_path")
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
                    "aws_access_key_id": self.config.options.get("key"),
                    "aws_secret_access_key": self.config.options.get("secret"),
                }

                # Add endpoint URL if present
                if self.config.endpoint_url:
                    session_kwargs["endpoint_url"] = self.config.endpoint_url

                session = aioboto3.Session()
                async with session.client("s3", **session_kwargs) as s3_client:
                    # Parse bucket and key from path
                    path_parts = normalized_path.split("/", 1)
                    if len(path_parts) == 2:
                        bucket, key = path_parts

                        # Use multipart upload for files >5MB, single-part for smaller files
                        if len(data) > 5 * 1024 * 1024:
                            await self._multipart_upload(s3_client, bucket, key, data)
                        else:
                            await s3_client.put_object(Bucket=bucket, Key=key, Body=data)
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
            from pathlib import Path

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
                    "aws_access_key_id": self.config.options.get("key"),
                    "aws_secret_access_key": self.config.options.get("secret"),
                }

                # Add endpoint URL if present
                if self.config.endpoint_url:
                    session_kwargs["endpoint_url"] = self.config.endpoint_url

                session = aioboto3.Session()
                async with session.client("s3", **session_kwargs) as s3_client:
                    # Parse bucket and key from path
                    path_parts = normalized_path.split("/", 1)
                    if len(path_parts) != 2:
                        raise ValueError(f"Invalid S3 path format: {normalized_path}. Expected 'bucket/key' format.")

                    bucket, key = path_parts

                    # Get file size for multipart decision
                    file_size = os.path.getsize(file_path)

                    if file_size > 50 * 1024 * 1024:
                        # Use multipart upload for files >50MB
                        await self._multipart_upload_from_file(s3_client, bucket, key, file_path)
                    else:
                        # Single-part upload for files ≤50MB - much faster, single API call
                        async with aiofiles.open(file_path, "rb") as f:
                            file_data = await f.read()
                            await s3_client.put_object(Bucket=bucket, Key=key, Body=file_data)
                    return
            except Exception as e:
                raise RuntimeError(f"Failed to stream upload file: {e}") from e
        else:
            # For non-S3 storage, read file and use write_bytes
            import aiofiles

            async with aiofiles.open(file_path, "rb") as f:
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
                "aws_access_key_id": self.config.options.get("key"),
                "aws_secret_access_key": self.config.options.get("secret"),
            }

            # Add endpoint URL if present
            if self.config.endpoint_url:
                session_kwargs["endpoint_url"] = self.config.endpoint_url

            session = aioboto3.Session()
            async with session.client("s3", **session_kwargs) as s3_client:
                # Parse bucket and key from path
                path_parts = normalized_path.split("/", 1)
                if len(path_parts) == 2:
                    bucket, key = path_parts
                    await s3_client.put_object(Bucket=bucket, Key=key, Body=data, Metadata=metadata)
        except Exception as e:
            print(f"Failed to write with metadata: {e}")
            # Fall back to regular write
            await self.write_bytes(path, data)

    async def _multipart_upload(self, s3_client, bucket: str, key: str, data: bytes) -> None:
        """Upload large files using multipart upload for better performance."""
        import io

        # Initiate multipart upload
        response = await s3_client.create_multipart_upload(Bucket=bucket, Key=key)

        upload_id = response["UploadId"]
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
                    Bucket=bucket, Key=key, PartNumber=part_number, UploadId=upload_id, Body=chunk
                )

                parts.append({"ETag": part_response["ETag"], "PartNumber": part_number})

                part_number += 1

            # Complete multipart upload
            await s3_client.complete_multipart_upload(
                Bucket=bucket, Key=key, UploadId=upload_id, MultipartUpload={"Parts": parts}
            )

        except Exception as e:
            # Abort multipart upload on error
            try:
                await s3_client.abort_multipart_upload(Bucket=bucket, Key=key, UploadId=upload_id)
            except Exception:
                pass  # Ignore errors when aborting

            raise e

    async def _multipart_upload_from_file(self, s3_client, bucket: str, key: str, file_path: str) -> None:
        """Upload large files using multipart upload directly from file for better performance."""

        import aiofiles

        # Initiate multipart upload
        response = await s3_client.create_multipart_upload(Bucket=bucket, Key=key)

        upload_id = response["UploadId"]
        parts = []
        part_number = 1
        chunk_size = 50 * 1024 * 1024  # 50MB chunks for better R2 performance

        try:
            async with aiofiles.open(file_path, "rb") as f:
                while True:
                    chunk = await f.read(chunk_size)
                    if not chunk:
                        break

                    # Upload part
                    part_response = await s3_client.upload_part(
                        Bucket=bucket, Key=key, PartNumber=part_number, UploadId=upload_id, Body=chunk
                    )

                    parts.append({"ETag": part_response["ETag"], "PartNumber": part_number})

                    part_number += 1

                # Complete multipart upload
                await s3_client.complete_multipart_upload(
                    Bucket=bucket, Key=key, UploadId=upload_id, MultipartUpload={"Parts": parts}
                )

        except Exception as e:
            # Abort multipart upload on error
            try:
                await s3_client.abort_multipart_upload(Bucket=bucket, Key=key, UploadId=upload_id)
            except Exception:
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
