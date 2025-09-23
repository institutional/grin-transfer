"""
Storage Base Classes and Configuration

Core storage abstractions providing platform-independent storage operations.
Supports S3, Azure Blob, GCS, local filesystem, and more through fsspec.
"""

import asyncio
import logging
import os
import re
import shutil
import time
import uuid
from collections.abc import AsyncGenerator
from pathlib import Path
from typing import Any

import aiofiles
import fsspec

from ..constants import DEFAULT_S3_MAX_POOL_CONNECTIONS

logger = logging.getLogger(__name__)


class StorageNotFoundError(Exception):
    """Raised when storage object doesn't exist."""

    pass


class BackendConfig:
    """Configuration for different storage backends."""

    def __init__(self, protocol: str = "file", endpoint_url: str | None = None, **kwargs: Any) -> None:
        self.protocol = protocol
        self.endpoint_url = endpoint_url
        self.options = kwargs

    @classmethod
    def s3(cls, bucket: str, **kwargs: Any) -> "BackendConfig":
        """Configure for AWS S3."""
        return cls(protocol="s3", bucket=bucket, **kwargs)

    @classmethod
    def r2(cls, endpoint_url: str, access_key: str, secret_key: str, **kwargs: Any) -> "BackendConfig":
        """Configure for Cloudflare R2 storage."""
        return cls(protocol="s3", endpoint_url=endpoint_url, key=access_key, secret=secret_key, **kwargs)

    @classmethod
    def gcs(cls, project: str, **kwargs: Any) -> "BackendConfig":
        """Configure for Google Cloud Storage."""
        return cls(protocol="gcs", project=project, **kwargs)

    @classmethod
    def local(cls, base_path: str) -> "BackendConfig":
        """Configure for local filesystem (base_path required)."""
        if not base_path:
            raise ValueError("Local storage requires explicit base_path")
        return cls(protocol="file", base_path=base_path)

    @classmethod
    def minio(cls, endpoint_url: str, access_key: str, secret_key: str) -> "BackendConfig":
        """Configure for MinIO or S3-compatible storage."""
        return cls(protocol="s3", endpoint_url=endpoint_url, key=access_key, secret=secret_key)


class Storage:
    """
    Async storage abstraction layer.

    Provides unified interface for cloud storage operations using fsspec.
    """

    def __init__(self, config: BackendConfig):
        self.config = config
        self._fs = None
        self._s3_client: Any = None  # Persistent S3 client for S3-compatible storage
        self._s3_session: Any = None  # Keep session reference for cleanup
        self._exit_stack: Any = None  # AsyncExitStack for proper resource management
        self._client_lock = asyncio.Lock()  # Thread-safe client creation

        # Instance identification for logging
        self._instance_id = str(uuid.uuid4())[:8]

        logger.info(f"Storage instance created (id={self._instance_id}, protocol={config.protocol})")

    async def _get_s3_client(self):
        """Get or create persistent S3 client for S3-compatible storage."""
        # Check if we need to create a new client
        if self._s3_client is None and self.is_s3_compatible():
            async with self._client_lock:
                # Double-check pattern to prevent race condition
                if self._s3_client is None:
                    from contextlib import AsyncExitStack

                    import aioboto3
                    import aiobotocore.config

                    # Configure connection pool for high concurrency
                    # Set pool size to handle 100+ concurrent workers
                    max_pool_connections = DEFAULT_S3_MAX_POOL_CONNECTIONS
                    config = aiobotocore.config.AioConfig(
                        max_pool_connections=max_pool_connections,
                        retries={"max_attempts": 3, "mode": "adaptive"},
                        read_timeout=300,  # 5 minutes for read operations (matches DOWNLOAD_TIMEOUT)
                        connect_timeout=120,  # 2 minutes to establish connection
                    )

                    logger.info(
                        f"Creating S3 client (storage_id={self._instance_id}, "
                        f"max_pool_connections={max_pool_connections}, "
                        f"read_timeout=300s, connect_timeout=120s)"
                    )

                    # Use the credentials from the storage config
                    session_kwargs = {
                        "aws_access_key_id": self.config.options.get("key"),
                        "aws_secret_access_key": self.config.options.get("secret"),
                    }

                    # Create session once and reuse
                    self._s3_session = aioboto3.Session()

                    # Create client with endpoint URL if present
                    client_kwargs = session_kwargs.copy()
                    if self.config.endpoint_url:
                        client_kwargs["endpoint_url"] = self.config.endpoint_url
                    client_kwargs["config"] = config

                    # Create persistent client with proper async context management
                    self._exit_stack = AsyncExitStack()
                    self._s3_client = await self._exit_stack.enter_async_context(
                        self._s3_session.client("s3", **client_kwargs)
                    )

                    logger.info(f"S3 client created (storage_id={self._instance_id})")

        return self._s3_client

    def _log_operation_start(self, operation: str, path: str) -> float:
        """Log the start of an S3 operation and return start time."""
        return time.time()

    def _log_operation_end(self, operation: str, path: str, start_time: float) -> None:
        """Log the completion of an S3 operation."""
        duration = time.time() - start_time

        # Always log slow operations
        if duration > 60.0 * 3:
            logger.warning(
                f"SLOW S3 operation: {operation} for {path} took {duration:.3f}s (storage_id={self._instance_id})"
            )

    def _get_fs(self) -> Any:
        """Get filesystem instance (lazy initialization)."""
        if self._fs is None:
            options = self.config.options.copy()
            if self.config.endpoint_url:
                options["endpoint_url"] = self.config.endpoint_url

            # For GCS, ensure project ID is available and set environment variable
            if self.config.protocol == "gcs":
                project_id = options.get("project")
                if not project_id:
                    raise ValueError("GCS filesystem requires project ID in storage configuration")

                # Set GOOGLE_CLOUD_PROJECT environment variable if not already set
                # This prevents gcsfs from trying to auto-detect the project
                if not os.environ.get("GOOGLE_CLOUD_PROJECT"):
                    os.environ["GOOGLE_CLOUD_PROJECT"] = project_id

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
                base_path = self.config.options.get("base_path")
                if not base_path:
                    raise ValueError("Local storage requires explicit base_path")
                path = str(Path(base_path) / path)
        elif self.config.protocol in ("s3", "gcs", "abfs"):
            # For cloud storage, ensure no leading slash and normalize consecutive slashes
            path = path.lstrip("/")
            # Replace consecutive slashes with single slash
            path = re.sub(r"/+", "/", path)
        return path

    def get_display_uri(self, path: str) -> str:
        """Get display-friendly URI for a storage path.

        For local storage: returns absolute filesystem path
        For cloud storage: returns URI with protocol prefix (s3://, gs://, etc)
        """
        normalized_path = self._normalize_path(path)

        if self.config.protocol == "file":
            # For local storage, return absolute path without file:// prefix
            return normalized_path
        else:
            # For cloud storage, use fsspec's unstrip_protocol
            return self._get_fs().unstrip_protocol(normalized_path)

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

    async def write_file(self, path: str, file_path: str, metadata: dict[str, str] | None = None) -> None:
        """Stream file from filesystem to bucket."""
        start_time = self._log_operation_start("write_file", path)

        try:
            normalized_path = self._normalize_path(path)

            if self.config.protocol == "file":
                # For local filesystem, just copy the file

                # Ensure parent directories exist
                parent = Path(normalized_path).parent
                parent.mkdir(parents=True, exist_ok=True)

                # Use shutil.copy2 to preserve metadata
                loop = asyncio.get_event_loop()
                await loop.run_in_executor(None, shutil.copy2, file_path, normalized_path)
            else:
                # Use persistent S3 client for S3-compatible storage
                s3_client = await self._get_s3_client()

                # Parse bucket and key from path
                path_parts = normalized_path.split("/", 1)
                if len(path_parts) != 2:
                    raise ValueError(f"Invalid S3 path format: {normalized_path}. Expected 'bucket/key' format.")

                bucket, key = path_parts
                await self._multipart_upload_from_file(s3_client, bucket, key, file_path, metadata)

            self._log_operation_end("write_file", path, start_time)
        except Exception as e:
            duration = time.time() - start_time
            logger.error(
                f"Storage operation FAILED: write_file for {path} after {duration:.3f}s - {e} "
                f"(storage_id={self._instance_id})"
            )
            raise

    def _calculate_part_size(self, file_size: int) -> int:
        """Calculate optimal part size based on file size."""
        if file_size < 100 * 1024 * 1024:  # < 100MB
            return 10 * 1024 * 1024  # 10MB parts
        elif file_size < 1024 * 1024 * 1024:  # < 1GB
            return 16 * 1024 * 1024  # 16MB parts
        elif file_size < 5 * 1024 * 1024 * 1024:  # < 5GB
            return 32 * 1024 * 1024  # 32MB parts
        else:
            # Target ~100 parts for very large files, cap at 100MB per part
            target_part_size = file_size // 100
            return min(target_part_size, 100 * 1024 * 1024)

    async def _multipart_upload_from_file(
        self, s3_client, bucket: str, key: str, file_path: str, metadata: dict[str, str] | None = None
    ) -> None:
        """Upload large files using streaming parallel multipart upload with bounded memory."""

        # Get file size for adaptive part sizing
        file_size = Path(file_path).stat().st_size
        chunk_size = self._calculate_part_size(file_size)
        max_concurrent_parts = 5  # Parallel part uploads for performance
        queue_size = 10  # Maximum chunks in memory at once

        # Initiate multipart upload
        create_kwargs = {"Bucket": bucket, "Key": key}
        if metadata:
            create_kwargs["Metadata"] = metadata  # type: ignore[assignment]
        response = await s3_client.create_multipart_upload(**create_kwargs)

        upload_id = response["UploadId"]
        estimated_parts = (file_size + chunk_size - 1) // chunk_size  # Ceiling division

        logger.debug(
            f"Starting streaming multipart upload for {key} "
            f"(upload_id={upload_id}, file_size={file_size // 1024 // 1024}MB, "
            f"chunk_size={chunk_size // 1024 // 1024}MB, estimated_parts={estimated_parts}, "
            f"storage_id={self._instance_id})"
        )

        try:
            # Queue for chunks with bounded size to limit memory usage
            chunk_queue: asyncio.Queue[tuple[int, bytes] | None] = asyncio.Queue(maxsize=queue_size)
            # Shared list for collecting results (list.append is atomic in CPython)
            results: list[tuple[int, str]] = []

            async def producer():
                """Read file and add chunks to queue."""
                part_number = 1
                try:
                    async with aiofiles.open(file_path, "rb") as f:
                        while True:
                            chunk = await f.read(chunk_size)
                            if not chunk:
                                break
                            await chunk_queue.put((part_number, chunk))
                            part_number += 1
                finally:
                    # Signal end of file with one None per consumer
                    for _ in range(max_concurrent_parts):
                        await chunk_queue.put(None)

            async def consumer():
                """Take chunks from queue and upload them."""
                while True:
                    item = await chunk_queue.get()
                    if item is None:
                        # End of file signal - mark sentinel as done before breaking
                        chunk_queue.task_done()
                        break

                    part_number, chunk = item
                    try:
                        logger.debug(
                            f"Starting upload of part {part_number} ({len(chunk) // 1024 // 1024}MB) for {key}"
                        )
                        etag = await self._upload_part(s3_client, bucket, key, part_number, upload_id, chunk)
                        results.append((part_number, etag))  # Atomic append
                        logger.debug(f"Completed upload of part {part_number} for {key}")
                    except Exception as e:
                        logger.error(f"Failed to upload part {part_number} for {key}: {e}")
                        raise
                    finally:
                        chunk_queue.task_done()

            # Create tasks
            producer_task = asyncio.create_task(producer())
            consumer_tasks = [asyncio.create_task(consumer()) for _ in range(max_concurrent_parts)]

            # Wait for producer to finish
            await producer_task

            # Wait for all chunks to be processed
            await chunk_queue.join()

            # Wait for all consumers to finish
            await asyncio.gather(*consumer_tasks)

            # Sort results by part number and build parts list
            results.sort(key=lambda x: x[0])  # Sort by part number
            parts = [{"ETag": etag, "PartNumber": part_number} for part_number, etag in results]
            total_parts = len(parts)

            logger.debug(f"All {total_parts} parts uploaded successfully, completing multipart upload ({key})")

            # Complete multipart upload
            await s3_client.complete_multipart_upload(
                Bucket=bucket, Key=key, UploadId=upload_id, MultipartUpload={"Parts": parts}
            )

            logger.debug(f"Multipart upload completed successfully ({key}, upload_id={upload_id})")

        except Exception as e:
            # Abort multipart upload on error
            try:
                await s3_client.abort_multipart_upload(Bucket=bucket, Key=key, UploadId=upload_id)
                logger.error(f"Aborted multipart upload ({key}, upload_id={upload_id})")
            except Exception:
                pass  # Ignore errors when aborting

            raise e

    async def _upload_part(
        self, s3_client, bucket: str, key: str, part_number: int, upload_id: str, chunk: bytes
    ) -> str:
        """Upload a single part and return its ETag."""
        chunk_size_mb = len(chunk) / (1024 * 1024)
        logger.debug(f"Uploading part {part_number} ({chunk_size_mb:.1f}MB) for {key}")

        part_response = await s3_client.upload_part(
            Bucket=bucket, Key=key, PartNumber=part_number, UploadId=upload_id, Body=chunk
        )

        etag = part_response["ETag"]
        logger.debug(f"Part {part_number} upload completed for {key} (ETag={etag})")
        return etag

    async def write_text(self, path: str, text: str, encoding: str = "utf-8") -> None:
        """
        Utility method for writing small text files to storage.

        WARNING: Do not use for large files - use write_file() instead.
        This method loads the entire text into memory and should only be used
        for small configuration files, metadata, or similar bounded content.
        """
        data = text.encode(encoding)

        if self.config.protocol == "s3":
            # Use persistent S3 client for non-blocking S3 uploads
            normalized_path = self._normalize_path(path)
            s3_client = await self._get_s3_client()

            # Parse bucket and key from path
            path_parts = normalized_path.split("/", 1)
            if len(path_parts) == 2:
                bucket, key = path_parts

                # Use single-part upload for bytes data
                await s3_client.put_object(Bucket=bucket, Key=key, Body=data)
                return

        # Use sync method for local filesystem or as fallback
        loop = asyncio.get_event_loop()
        fs = self._get_fs()
        normalized_path = self._normalize_path(path)

        # Ensure parent directories exist for local filesystem
        if self.config.protocol == "file":
            parent = Path(normalized_path).parent
            parent.mkdir(parents=True, exist_ok=True)

        await loop.run_in_executor(None, fs.pipe, normalized_path, data)

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

    async def close(self) -> None:
        """Clean up resources, especially S3 client connections."""
        if self._exit_stack is not None:
            try:
                await self._exit_stack.aclose()
                logger.info(f"Storage resources closed (storage_id={self._instance_id})")
            except Exception as e:
                logger.error(f"Error closing storage resources: {e} (storage_id={self._instance_id})")
            finally:
                self._exit_stack = None
                self._s3_client = None
                self._s3_session = None
