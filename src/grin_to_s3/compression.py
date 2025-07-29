#!/usr/bin/env python3
"""
Compression Utilities for File Uploads

Provides gzip compression for database, CSV, and JSONL files before upload
to reduce storage costs and transfer times.
"""

import asyncio
import gzip
import logging
import shutil
import tempfile
from pathlib import Path
from typing import Any

logger = logging.getLogger(__name__)


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


async def compress_file_gzip(
    source_path: str | Path,
    target_path: str | Path | None = None,
    compression_level: int = 9,
    cleanup_source: bool = False
) -> Path:
    """Compress a file using gzip compression.

    Args:
        source_path: Path to source file to compress
        target_path: Optional target path for compressed file (defaults to source + .gz)
        compression_level: Compression level 1-9 (9 = maximum compression)
        cleanup_source: Whether to delete source file after successful compression

    Returns:
        Path to the compressed file

    Raises:
        CompressionError: If compression fails
        FileNotFoundError: If source file doesn't exist
    """
    source_path = Path(source_path)

    if not source_path.exists():
        raise FileNotFoundError(f"Source file not found: {source_path}")

    if target_path is None:
        target_path = source_path.with_suffix(source_path.suffix + ".gz")
    else:
        target_path = Path(target_path)

    # Ensure target directory exists
    target_path.parent.mkdir(parents=True, exist_ok=True)

    try:
        logger.debug(f"Compressing {source_path} to {target_path} (level {compression_level})")

        # Get original file size for logging
        original_size = source_path.stat().st_size

        # Perform compression in executor to avoid blocking
        def _compress():
            with open(source_path, "rb") as f_in:
                with gzip.open(target_path, "wb", compresslevel=compression_level) as f_out:
                    shutil.copyfileobj(f_in, f_out)

        loop = asyncio.get_event_loop()
        await loop.run_in_executor(None, _compress)

        # Verify compressed file was created and get compression stats
        if not target_path.exists():
            raise CompressionError(f"Compressed file was not created: {target_path}")

        compressed_size = target_path.stat().st_size
        compression_ratio = (1 - compressed_size / original_size) * 100 if original_size > 0 else 0

        logger.info(
            f"Compression completed: {source_path.name} "
            f"({original_size:,} bytes -> {compressed_size:,} bytes, "
            f"{compression_ratio:.1f}% reduction)"
        )

        # Clean up source file if requested
        if cleanup_source:
            source_path.unlink()
            logger.debug(f"Cleaned up source file: {source_path}")

        return target_path

    except Exception as e:
        # Clean up target file if compression failed
        if target_path.exists():
            try:
                target_path.unlink()
            except Exception:
                pass  # Ignore cleanup errors

        raise CompressionError(f"Failed to compress {source_path}: {e}") from e


async def compress_file_to_temp(
    source_path: str | Path,
    compression_level: int = 9,
    temp_dir: str | Path | None = None
) -> Path:
    """Compress a file to a temporary location.

    This is useful when you need to compress a file but keep the original intact,
    and you want the compressed version in a temporary location for upload.

    Args:
        source_path: Path to source file to compress
        compression_level: Compression level 1-9 (9 = maximum compression)
        temp_dir: Optional temporary directory (uses system temp if None)

    Returns:
        Path to the temporary compressed file

    Raises:
        CompressionError: If compression fails
        FileNotFoundError: If source file doesn't exist
    """
    source_path = Path(source_path)

    if temp_dir is None:
        temp_dir = Path(tempfile.gettempdir())
    else:
        temp_dir = Path(temp_dir)

    # Use a unique temporary filename to avoid conflicts
    temp_file = temp_dir / f"grin_compress_{source_path.stem}_{id(source_path)}.gz"

    try:
        compressed_path = await compress_file_gzip(
            source_path,
            temp_file,
            compression_level=compression_level,
            cleanup_source=False  # Never clean up source when using temp
        )

        logger.debug(f"Created temporary compressed file: {compressed_path}")
        return compressed_path

    except Exception as e:
        raise CompressionError(f"Failed to create temporary compressed file: {e}") from e


class TempCompressedFile:
    """Context manager for temporary compressed files with automatic cleanup."""

    def __init__(
        self,
        source_path: str | Path,
        compression_level: int = 9,
        temp_dir: str | Path | None = None
    ):
        self.source_path = source_path
        self.compression_level = compression_level
        self.temp_dir = temp_dir
        self.compressed_path: Path | None = None

    async def __aenter__(self) -> Path:
        """Create temporary compressed file."""
        self.compressed_path = await compress_file_to_temp(
            self.source_path,
            self.compression_level,
            self.temp_dir
        )
        return self.compressed_path

    async def __aexit__(self, exc_type: Any, exc_val: Any, exc_tb: Any) -> None:
        """Clean up temporary compressed file."""
        if self.compressed_path and self.compressed_path.exists():
            try:
                self.compressed_path.unlink()
                logger.debug(f"Cleaned up temporary compressed file: {self.compressed_path}")
            except Exception as e:
                logger.warning(f"Failed to clean up temporary compressed file {self.compressed_path}: {e}")
