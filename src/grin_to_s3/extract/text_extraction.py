"""
OCR Text Extraction Module

Extracts OCR text from decrypted Google Books tar.gz archives and formats
as JSON arrays for full-text search and analysis. Handles sequential page
files and provides memory-efficient processing for large archives.
"""

import json
import logging
import re
from pathlib import Path
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from grin_to_s3.sync.tasks.task_types import UnpackData

logger = logging.getLogger(__name__)


def get_barcode_from_path(archive_path: str) -> str:
    """Extract barcode from archive filename."""
    # Get the basename by taking everything before the first dot
    # This handles: .tar.gz, .tar.gz.gpg, .decrypted.tar.gz, etc.
    return Path(archive_path).name.split(".")[0]


class TextExtractionError(Exception):
    """Raised when text extraction fails due to archive issues."""

    pass


class CorruptedArchiveError(TextExtractionError):
    """Raised when archive is corrupted or cannot be opened."""

    pass


class InvalidPageFormatError(TextExtractionError):
    """Raised when page files have invalid naming format."""

    pass


def _parse_page_number(filename: str) -> int | None:
    """
    Parse page number from filename like '00000001.txt'.

    Args:
        filename: Filename to parse

    Returns:
        Page number as integer (1-indexed) or None if invalid format
    """
    # Match 8-digit page numbers: 00000001.txt, 00000023.txt, etc.
    match = re.match(r"^(\d{8})\.txt$", filename)
    if match:
        page_num = int(match.group(1))
        # Ensure reasonable page number (1-999999)
        if 1 <= page_num <= 999999:
            return page_num

    return None


def filesystem_page_generator(extracted_dir: Path):
    """
    Generator that yields (page_number, content) tuples from filesystem.

    Reads page files one at a time without loading entire files into memory.

    Raises:
        TextExtractionError: If no .txt files found
        InvalidPageFormatError: If no valid page files found
    """

    # Find all .txt files and sort by page number
    txt_files = list(extracted_dir.glob("**/*.txt"))

    if not txt_files:
        raise TextExtractionError(f"No .txt files found in {extracted_dir}")

    page_files = []

    for txt_file in txt_files:
        page_number = _parse_page_number(txt_file.name)
        if page_number is not None:
            page_files.append((page_number, txt_file))
        else:
            logger.warning(f"Skipping file with invalid page format: {txt_file.name}")

    if not page_files:
        raise InvalidPageFormatError(f"No valid page files found in {extracted_dir}")

    # Sort by page number
    page_files.sort(key=lambda x: x[0])

    expected_page = 1

    for page_num, txt_file in page_files:
        # Fill in missing pages with empty strings
        while expected_page < page_num:
            yield (expected_page, "")
            expected_page += 1

        # Read current page content
        try:
            with open(txt_file, encoding="utf-8", errors="replace") as page_file:
                content = page_file.read()
            yield (page_num, content)
        except Exception as e:
            logger.warning(f"Error reading {txt_file.name}: {e}")
            yield (page_num, "")

        expected_page = page_num + 1


async def extract_ocr_pages(unpack_data: "UnpackData", jsonl_path: Path) -> int:
    """
    Extract OCR text from unpacked archive to JSONL file.

    Args:
        unpack_data: Dictionary containing 'unpacked_path' key with Path to extracted directory
        jsonl_path: Path to output JSONL file

    Returns:
        Number of pages processed
    """
    with open(jsonl_path, "w", encoding="utf-8") as f:
        page_count = 0

        for _, content in filesystem_page_generator(unpack_data["unpacked_path"]):
            # Write the JSON-encoded content as a single line
            f.write(json.dumps(content, ensure_ascii=False) + "\n")
            page_count += 1
    return page_count
