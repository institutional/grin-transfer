"""
Book Storage Operations

Storage abstraction specifically for book archive operations.
Implements storage patterns for book data organization.
"""

import json
import logging
from collections.abc import AsyncGenerator
from datetime import UTC, datetime
from pathlib import Path
from typing import TYPE_CHECKING

import aiofiles

from grin_to_s3.run_config import StorageConfig

from ..compression import compress_file_to_temp, get_compressed_filename
from .base import Storage

if TYPE_CHECKING:
    from types_aiobotocore_s3.client import S3Client

logger = logging.getLogger(__name__)


class BookManager:
    """
    Storage abstraction specifically for book archive operations.

    Implements storage patterns for book data organization.
    """

    def __init__(self, storage: Storage, storage_config: StorageConfig, base_prefix: str = ""):
        """Initialize BookStorage with type-safe bucket configuration.

        Args:
            storage: Storage backend instance
            bucket_config: Bucket names configuration (keyword-only for safety)
            base_prefix: Optional prefix for all storage paths

        Raises:
            ValueError: If any bucket name is empty
        """

        self.storage = storage
        self.bucket_raw = storage_config["config"].get("bucket_raw")
        self.bucket_meta = storage_config["config"].get("bucket_meta")
        self.bucket_full = storage_config["config"].get("bucket_full")
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
            # For S3-compatible storage with metadata, use streaming upload
            await self.storage.write_file(path, archive_file_path, {"encrypted-etag": encrypted_etag})
        else:
            # Stream upload directly from file
            logger.info(f"[{barcode}] Starting stream upload to storage...")
            await self.storage.write_file(path, archive_file_path)
            logger.info(f"[{barcode}] Stream upload completed")
        return path

    async def save_text_jsonl(self, barcode: str, pages: list[str]) -> str:
        """Save page text as JSONL file (one JSON string per line)."""

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
        """Upload compressed JSONL file from local path to full-text bucket."""
        base_filename = f"{barcode}.jsonl"
        compressed_filename = get_compressed_filename(base_filename)
        path = self._full_text_path(barcode, compressed_filename)

        # Add compression statistics to metadata
        if metadata is None:
            metadata = {}

        original_size = Path(jsonl_file_path).stat().st_size

        async with compress_file_to_temp(jsonl_file_path) as compressed_path:
            compressed_size = compressed_path.stat().st_size
            compression_ratio = (1 - compressed_size / original_size) * 100 if original_size > 0 else 0

            # Add compression info to metadata
            metadata.update(
                {
                    "original_size": str(original_size),
                    "compressed_size": str(compressed_size),
                    "compression_ratio": f"{compression_ratio:.1f}%",
                }
            )

            logger.debug(
                f"JSONL compression for {barcode}: {original_size:,} -> {compressed_size:,} bytes "
                f"({compression_ratio:.1f}% reduction)"
            )

            if self.storage.is_s3_compatible() and metadata:
                # For S3-compatible storage with metadata, read compressed file and use write_bytes_with_metadata

                async with aiofiles.open(compressed_path, "rb") as f:
                    file_data = await f.read()
                await self.storage.write_bytes_with_metadata(path, file_data, metadata)
            else:
                # Stream upload compressed file directly
                await self.storage.write_file(path, str(compressed_path))

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
        s3_client: S3Client
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
        """Upload compressed CSV file to metadata bucket with both latest and timestamped versions.

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

        # Generate compressed filenames
        compressed_filename = get_compressed_filename(filename)
        timestamp = datetime.now(UTC).strftime("%Y%m%d_%H%M%S")
        timestamped_base = f"books_{timestamp}.csv"
        compressed_timestamped_filename = get_compressed_filename(timestamped_base)

        # Generate paths for both versions (compressed)
        latest_path = self._meta_path(compressed_filename)
        timestamped_path = self._meta_path(f"timestamped/{compressed_timestamped_filename}")

        logger.info(f"Uploading compressed CSV file {local_csv_path} to metadata bucket")
        logger.debug(f"Latest version: {latest_path}")
        logger.debug(f"Timestamped version: {timestamped_path}")

        try:
            # Use temporary compressed file for both uploads
            async with compress_file_to_temp(local_csv_path) as compressed_path:
                # Get compression statistics
                original_size = Path(local_csv_path).stat().st_size
                compressed_size = compressed_path.stat().st_size
                compression_ratio = (1 - compressed_size / original_size) * 100 if original_size > 0 else 0

                logger.debug(
                    f"CSV compression: {original_size:,} -> {compressed_size:,} bytes "
                    f"({compression_ratio:.1f}% reduction)"
                )

                # Upload compressed file to both locations
                await self.storage.write_file(latest_path, str(compressed_path))
                logger.debug(f"Successfully uploaded latest compressed CSV to {latest_path}")

                await self.storage.write_file(timestamped_path, str(compressed_path))
                logger.debug(f"Successfully uploaded timestamped compressed CSV to {timestamped_path}")

            logger.info(f"Compressed CSV upload completed: {latest_path} and {timestamped_path}")
            return latest_path, timestamped_path

        except Exception as e:
            logger.error(f"Failed to upload compressed CSV file: {e}")
            raise
