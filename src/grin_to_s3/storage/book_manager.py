"""
Book Storage Operations

Storage abstraction specifically for book archive operations.
Implements storage patterns for book data organization.
"""

import logging
import uuid

from grin_to_s3.database import connect_async
from grin_to_s3.run_config import StorageConfig

from .base import Storage
from .factories import LOCAL_STORAGE_DEFAULTS

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
        self._manager_id = str(uuid.uuid4())[:8]

        logger.info(f"BookManager created (manager_id={self._manager_id})")

        self.storage = storage

        # Get bucket/directory names from config
        self.bucket_raw = storage_config["config"].get("bucket_raw")
        self.bucket_meta = storage_config["config"].get("bucket_meta")
        self.bucket_full = storage_config["config"].get("bucket_full")

        # For local storage with missing bucket names, use directory defaults
        if storage_config["type"] == "local":
            if not self.bucket_raw:
                self.bucket_raw = LOCAL_STORAGE_DEFAULTS["bucket_raw"]
            if not self.bucket_meta:
                self.bucket_meta = LOCAL_STORAGE_DEFAULTS["bucket_meta"]
            if not self.bucket_full:
                self.bucket_full = LOCAL_STORAGE_DEFAULTS["bucket_full"]

        self.base_prefix = base_prefix.rstrip("/")

    def raw_archive_path(self, barcode: str, filename: str) -> str:
        """Generate path for raw archive file."""
        if self.base_prefix:
            return f"{self.bucket_raw}/{self.base_prefix}/{barcode}/{filename}"
        return f"{self.bucket_raw}/{barcode}/{filename}"

    def full_text_path(self, barcode: str, filename: str) -> str:
        """Generate path for full-text bucket file."""
        if self.base_prefix:
            return f"{self.bucket_full}/{self.base_prefix}/{filename}"
        return f"{self.bucket_full}/{filename}"

    def meta_path(self, filename: str) -> str:
        """Generate path for metadata bucket file."""
        if self.base_prefix:
            return f"{self.bucket_meta}/{self.base_prefix}/{filename}"
        return f"{self.bucket_meta}/{filename}"

    async def get_decrypted_archive_metadata(
        self,
        barcode: str,
        db_tracker,
    ) -> dict[str, str]:
        """Get metadata from decrypted archive file."""

        filename = f"{barcode}.tar.gz"
        path = self.raw_archive_path(barcode, filename)

        # For local storage, query the database for etag
        if self.storage.config.protocol == "file":
            # Query database for stored encrypted_etag
            async with connect_async(db_tracker.db_path) as db:
                async with db.execute("SELECT encrypted_etag FROM books WHERE barcode = ?", (barcode,)) as cursor:
                    row = await cursor.fetchone()
                    result = {"encrypted_etag": row[0]} if row and row[0] else {}
        else:
            # For S3-compatible storage, use metadata from persistent S3 client
            s3_client = await self.storage._get_s3_client()

            # Parse bucket and key from path
            normalized_path = self.storage._normalize_path(path)
            path_parts = normalized_path.split("/", 1)
            if len(path_parts) == 2:
                bucket, key = path_parts
                response = await s3_client.head_object(Bucket=bucket, Key=key)
                result = response.get("Metadata", {})
            else:
                result = {}

        return result
