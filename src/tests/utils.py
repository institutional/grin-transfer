#!/usr/bin/env python3
"""
Shared test utilities for grin-to-s3 test suite.

This module provides common functionality used across multiple test files
to eliminate code duplication and ensure consistent test setup.
"""

import tarfile
from io import BytesIO
from pathlib import Path


def create_test_archive(pages: dict[str, str], temp_dir: Path, archive_name: str = "test_archive.tar.gz") -> Path:
    """
    Create a test tar.gz archive with specified pages.

    This utility function creates a tar.gz archive in memory with the specified
    page files and content. Used across multiple test modules for consistent
    test archive creation.

    Args:
        pages: Dictionary mapping filename to content (e.g., {"00000001.txt": "content"})
        temp_dir: Directory where the archive should be created
        archive_name: Name of the archive file (default: "test_archive.tar.gz")

    Returns:
        Path to the created archive file

    Example:
        with tempfile.TemporaryDirectory() as temp_dir:
            temp_path = Path(temp_dir)
            pages = {
                "00000001.txt": "First page content",
                "00000002.txt": "Second page content"
            }
            archive_path = create_test_archive(pages, temp_path)
    """
    archive_path = temp_dir / archive_name

    with tarfile.open(archive_path, "w:gz") as tar:
        for filename, content in pages.items():
            # Create file in memory
            file_data = content.encode("utf-8")
            file_info = tarfile.TarInfo(name=filename)
            file_info.size = len(file_data)

            tar.addfile(file_info, BytesIO(file_data))

    return archive_path


def create_test_archive_with_sizes(num_pages: int, content_size: int, temp_dir: Path, archive_name: str = None) -> Path:
    """
    Create a test archive with specified number of pages and content size.

    This utility function creates archives for performance testing where
    you need specific numbers of pages with controlled content sizes.

    Args:
        num_pages: Number of pages to create (1-indexed: 00000001.txt, etc.)
        content_size: Approximate size of content per page in characters
        temp_dir: Directory where the archive should be created
        archive_name: Name of the archive file (auto-generated if None)

    Returns:
        Path to the created archive file

    Example:
        # Create archive with 100 pages, ~1KB each
        archive_path = create_test_archive_with_sizes(100, 1000, temp_path)
    """
    if archive_name is None:
        archive_name = f"test_archive_{num_pages}pages.tar.gz"

    archive_path = temp_dir / archive_name

    with tarfile.open(archive_path, "w:gz") as tar:
        for i in range(1, num_pages + 1):
            filename = f"{i:08d}.txt"
            # Create content of specified size
            content = f"Page {i} content. " * (content_size // 20)
            file_data = content.encode("utf-8")

            file_info = tarfile.TarInfo(name=filename)
            file_info.size = len(file_data)

            tar.addfile(file_info, BytesIO(file_data))

    return archive_path


def create_mixed_size_archive(num_pages: int, temp_dir: Path, archive_name: str = "mixed_size_archive.tar.gz") -> Path:
    """
    Create a test archive with pages of varying sizes.

    This utility creates archives with realistic size variation where some
    pages are small, some medium, and some large - useful for testing
    performance with realistic archive characteristics.

    Args:
        num_pages: Number of pages to create
        temp_dir: Directory where the archive should be created
        archive_name: Name of the archive file

    Returns:
        Path to the created archive file

    Page size distribution:
        - Every 10th page: ~50KB (large)
        - Every 5th page: ~5KB (medium)
        - Other pages: ~500B (small)
    """
    archive_path = temp_dir / archive_name

    with tarfile.open(archive_path, "w:gz") as tar:
        for i in range(1, num_pages + 1):
            filename = f"{i:08d}.txt"

            # Vary content size - some small, some large
            if i % 10 == 0:
                content = f"Large page {i}. " * 5000  # ~50KB
            elif i % 5 == 0:
                content = f"Medium page {i}. " * 500   # ~5KB
            else:
                content = f"Small page {i}. " * 50     # ~500B

            file_data = content.encode("utf-8")
            file_info = tarfile.TarInfo(name=filename)
            file_info.size = len(file_data)

            tar.addfile(file_info, BytesIO(file_data))

    return archive_path
