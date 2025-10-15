#!/usr/bin/env python3
"""Shared test utilities for grin-to-s3 test suite."""

import json
import tarfile
from datetime import UTC, datetime
from io import BytesIO
from pathlib import Path

from grin_transfer.database.connections import connect_async
from grin_transfer.database.database_utils import retry_database_operation


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


def create_extracted_directory(pages: dict[str, str], temp_dir: Path, dir_name: str = "extracted") -> Path:
    """
    Create a directory with extracted page files for testing.

    This utility function creates a directory structure that mimics
    what would result from extracting a tar.gz archive.

    Args:
        pages: Dictionary mapping filename to content (e.g., {"00000001.txt": "content"})
        temp_dir: Directory where the extracted directory should be created
        dir_name: Name of the extracted directory (default: "extracted")

    Returns:
        Path to the created extracted directory

    Example:
        with tempfile.TemporaryDirectory() as temp_dir:
            temp_path = Path(temp_dir)
            pages = {
                "00000001.txt": "First page content",
                "00000002.txt": "Second page content"
            }
            extracted_dir = create_extracted_directory(pages, temp_path)
    """
    extracted_dir = temp_dir / dir_name
    extracted_dir.mkdir(parents=True, exist_ok=True)

    for filename, content in pages.items():
        file_path = extracted_dir / filename
        file_path.write_text(content, encoding="utf-8")

    return extracted_dir


@retry_database_operation
async def batch_write_status_updates(db_path: str, status_updates: list) -> None:
    """Write status history rows for tests using runtime schema."""
    if not status_updates:
        return

    async with connect_async(db_path) as conn:
        for status_update in status_updates:
            await conn.execute(
                """INSERT INTO book_status_history
                   (barcode, status_type, status_value, timestamp, session_id, metadata)
                   VALUES (?, ?, ?, ?, ?, ?)""",
                (
                    status_update.barcode,
                    status_update.status_type,
                    status_update.status_value,
                    datetime.now(UTC).isoformat(),
                    status_update.session_id,
                    json.dumps(status_update.metadata) if status_update.metadata else None,
                ),
            )
        await conn.commit()
