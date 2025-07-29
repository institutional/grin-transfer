"""Tests for compression utilities."""

import tempfile
from pathlib import Path

import pytest

from grin_to_s3.compression import (
    TempCompressedFile,
    compress_file_gzip,
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
async def test_compress_file_gzip_basic():
    """Test basic gzip compression."""
    with tempfile.TemporaryDirectory() as temp_dir:
        # Create source file
        source_file = Path(temp_dir) / "test.txt"
        test_content = "This is test content for compression. " * 100
        source_file.write_text(test_content)

        # Compress file
        compressed_file = await compress_file_gzip(source_file)

        # Verify compressed file exists and is smaller (for most text content)
        assert compressed_file.exists()
        assert compressed_file.name == "test.txt.gz"

        # Verify we can decompress and get original content
        import gzip
        with gzip.open(compressed_file, "rt") as f:
            decompressed_content = f.read()
        assert decompressed_content == test_content


@pytest.mark.asyncio
async def test_compress_file_gzip_custom_target():
    """Test compression with custom target path."""
    with tempfile.TemporaryDirectory() as temp_dir:
        # Create source file
        source_file = Path(temp_dir) / "source.txt"
        source_file.write_text("Test content")

        # Compress to custom target
        target_file = Path(temp_dir) / "custom_name.txt.gz"
        compressed_file = await compress_file_gzip(source_file, target_file)

        assert compressed_file == target_file
        assert compressed_file.exists()


@pytest.mark.asyncio
async def test_compress_file_gzip_cleanup_source():
    """Test compression with source file cleanup."""
    with tempfile.TemporaryDirectory() as temp_dir:
        # Create source file
        source_file = Path(temp_dir) / "test.txt"
        source_file.write_text("Test content")

        # Compress with cleanup
        compressed_file = await compress_file_gzip(source_file, cleanup_source=True)

        # Source should be gone, compressed should exist
        assert not source_file.exists()
        assert compressed_file.exists()


@pytest.mark.asyncio
async def test_compress_file_gzip_missing_source():
    """Test compression with missing source file."""
    with pytest.raises(FileNotFoundError):
        await compress_file_gzip("/nonexistent/file.txt")


@pytest.mark.asyncio
async def test_compress_file_to_temp():
    """Test compression to temporary location."""
    with tempfile.TemporaryDirectory() as temp_dir:
        # Create source file
        source_file = Path(temp_dir) / "test.txt"
        source_file.write_text("Test content for temporary compression")

        # Compress to temp
        compressed_file = await compress_file_to_temp(source_file)

        # Verify temp file exists and source is untouched
        assert compressed_file.exists()
        assert source_file.exists()
        assert compressed_file.name.endswith(".gz")

        # Clean up temp file
        compressed_file.unlink()


@pytest.mark.asyncio
async def test_temp_compressed_file_context_manager():
    """Test TempCompressedFile context manager."""
    with tempfile.TemporaryDirectory() as temp_dir:
        # Create source file
        source_file = Path(temp_dir) / "test.txt"
        test_content = "Test content for context manager"
        source_file.write_text(test_content)

        compressed_path = None

        # Use context manager
        async with TempCompressedFile(source_file) as temp_compressed:
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


@pytest.mark.asyncio
async def test_temp_compressed_file_custom_temp_dir():
    """Test TempCompressedFile with custom temp directory."""
    with tempfile.TemporaryDirectory() as temp_dir:
        # Create source file
        source_file = Path(temp_dir) / "test.txt"
        source_file.write_text("Test content")

        # Create custom temp directory
        custom_temp = Path(temp_dir) / "custom_temp"
        custom_temp.mkdir()

        # Use context manager with custom temp dir
        async with TempCompressedFile(source_file, temp_dir=custom_temp) as temp_compressed:
            # Verify it's in the custom temp directory
            assert temp_compressed.parent == custom_temp
            assert temp_compressed.exists()


@pytest.mark.asyncio
async def test_compression_with_different_levels():
    """Test compression with different compression levels."""
    with tempfile.TemporaryDirectory() as temp_dir:
        # Create source file with repetitive content (compresses well)
        source_file = Path(temp_dir) / "test.txt"
        test_content = "AAAAAAAAAA" * 1000  # Very repetitive content
        source_file.write_text(test_content)

        # Test different compression levels
        compressed_1 = await compress_file_gzip(source_file,
                                              Path(temp_dir) / "test_1.gz",
                                              compression_level=1)
        compressed_9 = await compress_file_gzip(source_file,
                                              Path(temp_dir) / "test_9.gz",
                                              compression_level=9)

        # Level 9 should compress better than level 1 for repetitive content
        size_1 = compressed_1.stat().st_size
        size_9 = compressed_9.stat().st_size

        # Both should be smaller than original and level 9 should be smaller or equal to level 1
        original_size = source_file.stat().st_size
        assert size_1 < original_size
        assert size_9 < original_size
        assert size_9 <= size_1
