#!/usr/bin/env python3
"""
Compression Utilities for File Uploads

Provides gzip compression for database, CSV, and JSONL files before upload
to reduce storage costs and transfer times.
"""

import gzip
import logging
import shutil
import tempfile
from pathlib import Path

logger = logging.getLogger(__name__)

DEFAULT_COMPRESSION_LEVEL = 1  # Fastest

class CompressionError(Exception):
    """Raised when compression operations fail."""
    pass


def get_compressed_filename(original_filename: str) -> str:
    """Get compressed filename by adding .gz extension.

    Args:
        original_filename: Original filename

    Returns:
        Filename with .gz extension added

    Examples:
        get_compressed_filename("books.csv") -> "books.csv.gz"
        get_compressed_filename("books_backup_20240129.db") -> "books_backup_20240129.db.gz"
    """
    return f"{original_filename}.gz"



def compress_file_to_temp(source_path: str | Path, compression_level: int = DEFAULT_COMPRESSION_LEVEL):
    """Async context manager that compresses a file to a temporary location with automatic cleanup.

    Args:
        source_path: Path to source file to compress
        compression_level: Compression level 1-9 (9 = maximum compression)

    Returns:
        Async context manager yielding Path to the temporary compressed file

    Raises:
        CompressionError: If compression fails
        FileNotFoundError: If source file doesn't exist
    """
    source_path = Path(source_path)

    if not source_path.exists():
        raise FileNotFoundError(f"Source file not found: {source_path}")

    class AsyncCompressedTempFile:
        def __init__(self, source: Path):
            self.source_path = source
        async def __aenter__(self):
            self.temp_file = tempfile.NamedTemporaryFile(suffix=".gz", delete=True)
            temp_path = Path(self.temp_file.name)

            try:
                # Get original file size for logging
                original_size = self.source_path.stat().st_size

                # Perform compression in executor to avoid blocking
                def _compress():
                    with open(self.source_path, "rb") as f_in:
                        with gzip.open(temp_path, "wb", compresslevel=compression_level) as f_out:
                            shutil.copyfileobj(f_in, f_out)

                import asyncio
                loop = asyncio.get_event_loop()
                await loop.run_in_executor(None, _compress)

                # Get compression stats
                compressed_size = temp_path.stat().st_size
                compression_ratio = (1 - compressed_size / original_size) * 100 if original_size > 0 else 0

                logger.info(
                    f"Compression completed: {self.source_path.name} "
                    f"({original_size:,} bytes -> {compressed_size:,} bytes, "
                    f"{compression_ratio:.1f}% reduction)"
                )

                return temp_path
            except Exception as e:
                self.temp_file.close()  # Cleanup on error
                raise CompressionError(f"Failed to create temporary compressed file: {e}") from e

        async def __aexit__(self, exc_type, exc_val, exc_tb):  # noqa: ARG002
            self.temp_file.close()  # Automatic cleanup

    return AsyncCompressedTempFile(source_path)


