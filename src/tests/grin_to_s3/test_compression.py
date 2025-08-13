"""Tests for compression utilities."""

import tempfile
from pathlib import Path

import pytest

from grin_to_s3.compression import (
    compress_file_to_temp,
    get_compressed_filename,
)


def test_get_compressed_filename():
    """Test filename compression extension."""
    assert get_compressed_filename("books.csv") == "books.csv.gz"
    assert get_compressed_filename("test.db") == "test.db.gz"
    assert get_compressed_filename("data.jsonl") == "data.jsonl.gz"
    assert get_compressed_filename("file.txt.backup") == "file.txt.backup.gz"


@pytest.mark.asyncio
async def test_compress_file_to_temp():
    """Test compression to temporary location."""
    with tempfile.TemporaryDirectory() as temp_dir:
        # Create source file
        source_file = Path(temp_dir) / "test.txt"
        source_file.write_text("Test content for temporary compression")

        # Compress to temp using context manager
        async with compress_file_to_temp(source_file) as compressed_file:
            # Verify temp file exists and source is untouched
            assert compressed_file.exists()
            assert source_file.exists()
            assert compressed_file.name.endswith(".gz")

        # File should be cleaned up after context exits
        assert not compressed_file.exists()


def test_compress_file_to_temp_missing_source():
    """Test compression with missing source file."""
    with pytest.raises(FileNotFoundError):
        compress_file_to_temp("/nonexistent/file.txt")


@pytest.mark.asyncio
async def test_compress_file_to_temp_context_manager():
    """Test compress_file_to_temp context manager functionality."""
    with tempfile.TemporaryDirectory() as temp_dir:
        # Create source file
        source_file = Path(temp_dir) / "test.txt"
        test_content = "Test content for context manager"
        source_file.write_text(test_content)

        compressed_path = None

        # Use context manager
        async with compress_file_to_temp(source_file) as temp_compressed:
            compressed_path = temp_compressed

            # File should exist during context
            assert temp_compressed.exists()
            assert temp_compressed.name.endswith(".gz")

            # Verify content
            import gzip

            with gzip.open(temp_compressed, "rt") as f:
                decompressed_content = f.read()
            assert decompressed_content == test_content

        # File should be cleaned up after context
        assert not compressed_path.exists()

        # Source should still exist
        assert source_file.exists()
