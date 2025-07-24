"""
Book Storage Operations

Storage abstraction specifically for book archive operations.
Implements storage patterns for book data organization.
"""

import logging
from collections.abc import AsyncGenerator
from datetime import UTC, datetime
from typing import TypedDict

from .base import Storage

logger = logging.getLogger(__name__)


class BucketConfig(TypedDict):
    """Type-safe bucket configuration."""

    bucket_raw: str
    bucket_meta: str
    bucket_full: str


class BookManager:
    """
    Storage abstraction specifically for book archive operations.

    Implements storage patterns for book data organization.
    """

    def __init__(self, storage: Storage, *, bucket_config: BucketConfig, base_prefix: str = ""):
        """Initialize BookStorage with type-safe bucket configuration.

        Args:
            storage: Storage backend instance
            bucket_config: Bucket names configuration (keyword-only for safety)
            base_prefix: Optional prefix for all storage paths

        Raises:
            ValueError: If any bucket name is empty
        """
        # Validate bucket names are not empty
        for bucket_key, bucket_name in bucket_config.items():
            if not bucket_name or (isinstance(bucket_name, str) and not bucket_name.strip()):
                raise ValueError(f"Bucket name cannot be empty: {bucket_key}")

        self.storage = storage
        self.bucket_raw = bucket_config["bucket_raw"]
        self.bucket_meta = bucket_config["bucket_meta"]
        self.bucket_full = bucket_config["bucket_full"]
        self.base_prefix = base_prefix.rstrip("/")

    def _raw_archive_path(self, barcode: str, filename: str) -> str:
        """Generate path for raw archive file."""
        if self.base_prefix:
            return f"{self.bucket_raw}/{self.base_prefix}/{barcode}/{filename}"
        return f"{self.bucket_raw}/{barcode}/{filename}"

    def _full_text_path(self, barcode: str, filename: str) -> str:
        """Generate path for full-text bucket file."""
        if self.base_prefix:
            return f"{self.bucket_full}/{self.base_prefix}/{filename}"
        return f"{self.bucket_full}/{filename}"

    def _meta_path(self, filename: str) -> str:
        """Generate path for metadata bucket file."""
        if self.base_prefix:
            return f"{self.bucket_meta}/{self.base_prefix}/{filename}"
        return f"{self.bucket_meta}/{filename}"

    async def save_archive(self, barcode: str, archive_data: bytes, encrypted_etag: str | None = None) -> str:
        """Save encrypted archive (.tar.gz.gpg) with optional encrypted ETag metadata."""
        filename = f"{barcode}.tar.gz.gpg"
        path = self._raw_archive_path(barcode, filename)

        if self.storage.config.protocol == "s3" and encrypted_etag:
            # Store encrypted file's ETag as metadata for future comparison
            await self.storage.write_bytes_with_metadata(path, archive_data, {"encrypted-etag": encrypted_etag})
        else:
            await self.storage.write_bytes(path, archive_data)
        return path

    async def save_archive_from_file(
        self, barcode: str, archive_file_path: str, encrypted_etag: str | None = None
    ) -> str:
        """Save encrypted archive (.tar.gz.gpg) from file with optional encrypted ETag metadata."""
        filename = f"{barcode}.tar.gz.gpg"
        path = self._raw_archive_path(barcode, filename)

        if self.storage.config.protocol == "s3" and encrypted_etag:
            # For S3 with metadata, we need to read the file and use write_bytes_with_metadata
            import aiofiles

            async with aiofiles.open(archive_file_path, "rb") as f:
                archive_data = await f.read()
            await self.storage.write_bytes_with_metadata(path, archive_data, {"encrypted-etag": encrypted_etag})
        else:
            # Stream upload directly from file
            await self.storage.write_file(path, archive_file_path)
        return path

    async def save_decrypted_archive(self, barcode: str, archive_data: bytes, encrypted_etag: str | None = None) -> str:
        """Save decrypted archive (.tar.gz) with optional encrypted ETag metadata."""
        filename = f"{barcode}.tar.gz"
        path = self._raw_archive_path(barcode, filename)

        if self.storage.is_s3_compatible() and encrypted_etag:
            # Store encrypted file's ETag as metadata for future comparison
            await self.storage.write_bytes_with_metadata(path, archive_data, {"encrypted-etag": encrypted_etag})
        else:
            await self.storage.write_bytes(path, archive_data)
        return path

    async def save_decrypted_archive_from_file(
        self, barcode: str, archive_file_path: str, encrypted_etag: str | None = None
    ) -> str:
        """Save decrypted archive (.tar.gz) from file with optional encrypted ETag metadata."""
        filename = f"{barcode}.tar.gz"
        path = self._raw_archive_path(barcode, filename)

        if self.storage.is_s3_compatible() and encrypted_etag:
            # For S3-compatible storage with metadata, read file and use write_bytes_with_metadata
            import aiofiles

            async with aiofiles.open(archive_file_path, "rb") as f:
                archive_data = await f.read()
            await self.storage.write_bytes_with_metadata(path, archive_data, {"encrypted-etag": encrypted_etag})
        else:
            # Stream upload directly from file
            await self.storage.write_file(path, archive_file_path)
        return path

    async def save_text_jsonl(self, barcode: str, pages: list[str]) -> str:
        """Save page text as JSONL file (one JSON string per line)."""
        import json

        filename = f"{barcode}.jsonl"
        path = self._raw_archive_path(barcode, filename)

        # Build JSONL content inline
        if not pages:
            jsonl_data = ""
        else:
            jsonl_lines = [json.dumps(page, ensure_ascii=False) for page in pages]
            jsonl_data = "\n".join(jsonl_lines) + "\n"

        await self.storage.write_text(path, jsonl_data)
        return path

    async def save_ocr_text_jsonl_from_file(
        self, barcode: str, jsonl_file_path: str, metadata: dict | None = None
    ) -> str:
        """Upload JSONL file from local path to full-text bucket."""
        filename = f"{barcode}.jsonl"
        path = self._full_text_path(barcode, filename)

        if self.storage.is_s3_compatible() and metadata:
            # For S3-compatible storage with metadata, read file and use write_bytes_with_metadata
            import aiofiles

            async with aiofiles.open(jsonl_file_path, "rb") as f:
                file_data = await f.read()
            await self.storage.write_bytes_with_metadata(path, file_data, metadata)
        else:
            # Stream upload directly from file
            await self.storage.write_file(path, jsonl_file_path)

        return path

    async def save_timestamp(self, barcode: str, suffix: str = "retrieval") -> str:
        """Save retrieval timestamp."""
        filename = f"{barcode}.tar.gz.gpg.{suffix}"
        path = self._raw_archive_path(barcode, filename)
        timestamp = datetime.now(tz=UTC).isoformat()
        await self.storage.write_text(path, timestamp)
        return path

    async def archive_exists(self, barcode: str) -> bool:
        """Check if decrypted archive exists."""
        filename = f"{barcode}.tar.gz"
        path = self._raw_archive_path(barcode, filename)
        return await self.storage.exists(path)

    async def decrypted_archive_exists(self, barcode: str) -> bool:
        """Check if decrypted archive exists."""
        return await self.archive_exists(barcode)

    async def get_decrypted_archive_metadata(self, barcode: str) -> dict[str, str]:
        """Get metadata from decrypted archive file."""
        if not self.storage.is_s3_compatible():
            # Only supported for S3-compatible storage
            return {}

        try:
            filename = f"{barcode}.tar.gz"
            path = self._raw_archive_path(barcode, filename)

            # Get object metadata using aioboto3 directly
            import aioboto3

            # Use the credentials from the storage config
            session_kwargs = {
                "aws_access_key_id": self.storage.config.options.get("key"),
                "aws_secret_access_key": self.storage.config.options.get("secret"),
            }

            # Add endpoint URL if present
            if self.storage.config.endpoint_url:
                session_kwargs["endpoint_url"] = self.storage.config.endpoint_url

            session = aioboto3.Session()
            async with session.client("s3", **session_kwargs) as s3_client:
                # Parse bucket and key from path
                normalized_path = self.storage._normalize_path(path)
                path_parts = normalized_path.split("/", 1)
                if len(path_parts) == 2:
                    bucket, key = path_parts
                    response = await s3_client.head_object(Bucket=bucket, Key=key)
                    return response.get("Metadata", {})
                else:
                    return {}
        except Exception:
            return {}

    async def archive_matches_encrypted_etag(self, barcode: str, encrypted_etag: str) -> bool:
        """Check if existing decrypted archive was created from the same encrypted file using stored metadata."""
        if not self.storage.is_s3_compatible():
            # Only supported for S3-compatible storage
            return False

        try:
            metadata = await self.get_decrypted_archive_metadata(barcode)
            stored_encrypted_etag = metadata.get("encrypted-etag", "")

            # Compare stored encrypted ETag with current encrypted ETag
            match = stored_encrypted_etag.strip('"') == encrypted_etag.strip('"')
            return match
        except Exception:
            # If anything fails, assume no match
            return False

    async def get_archive(self, barcode: str) -> bytes:
        """Get decrypted archive data."""
        filename = f"{barcode}.tar.gz"
        path = self._raw_archive_path(barcode, filename)
        return await self.storage.read_bytes(path)

    async def stream_archive_download(self, barcode: str) -> AsyncGenerator[bytes, None]:
        """Stream download decrypted archive."""
        filename = f"{barcode}.tar.gz"
        path = self._raw_archive_path(barcode, filename)
        async for chunk in self.storage.stream_download(path):
            yield chunk

    async def upload_csv_file(self, local_csv_path: str, filename: str | None = None) -> tuple[str, str]:
        """Upload CSV file to metadata bucket with both latest and timestamped versions.

        Args:
            local_csv_path: Path to local CSV file to upload
            filename: Optional custom filename (defaults to books_latest.csv)

        Returns:
            Tuple of (latest_path, timestamped_path) that were uploaded to

        Raises:
            Exception: If upload fails for either version
        """
        if filename is None:
            filename = "books_latest.csv"

        # Generate filename-safe timestamp
        timestamp = datetime.now(UTC).strftime("%Y%m%d_%H%M%S")
        timestamped_filename = f"books_{timestamp}.csv"

        # Generate paths for both versions
        latest_path = self._meta_path(filename)
        timestamped_path = self._meta_path(f"timestamped/{timestamped_filename}")

        logger.info(f"Uploading CSV file {local_csv_path} to metadata bucket")
        logger.debug(f"Latest version: {latest_path}")
        logger.debug(f"Timestamped version: {timestamped_path}")

        try:
            # Upload to both locations
            await self.storage.write_file(latest_path, local_csv_path)
            logger.debug(f"Successfully uploaded latest CSV to {latest_path}")

            await self.storage.write_file(timestamped_path, local_csv_path)
            logger.debug(f"Successfully uploaded timestamped CSV to {timestamped_path}")

            logger.info(f"CSV upload completed: {latest_path} and {timestamped_path}")
            return latest_path, timestamped_path

        except Exception as e:
            logger.error(f"Failed to upload CSV file: {e}")
            raise
