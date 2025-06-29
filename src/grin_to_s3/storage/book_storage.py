"""
Book Storage Operations

Storage abstraction specifically for book archive operations.
Implements storage patterns for book data organization.
"""

import json
import logging
from collections.abc import AsyncGenerator
from datetime import UTC, datetime

from .base import Storage

logger = logging.getLogger(__name__)


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

    async def save_archive(self, barcode: str, archive_data: bytes, encrypted_etag: str | None = None) -> str:
        """Save encrypted archive (.tar.gz.gpg) with optional encrypted ETag metadata."""
        filename = f"{barcode}.tar.gz.gpg"
        path = self._book_path(barcode, filename)

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
        path = self._book_path(barcode, filename)

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
        path = self._book_path(barcode, filename)

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
        path = self._book_path(barcode, filename)

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
        """Check if decrypted archive exists."""
        filename = f"{barcode}.tar.gz"
        path = self._book_path(barcode, filename)
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
            path = self._book_path(barcode, filename)

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
        path = self._book_path(barcode, filename)
        return await self.storage.read_bytes(path)

    async def stream_archive_download(self, barcode: str) -> AsyncGenerator[bytes, None]:
        """Stream download decrypted archive."""
        filename = f"{barcode}.tar.gz"
        path = self._book_path(barcode, filename)
        async for chunk in self.storage.stream_download(path):
            yield chunk
