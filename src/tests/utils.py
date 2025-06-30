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
