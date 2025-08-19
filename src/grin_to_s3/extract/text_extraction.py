"""
OCR Text Extraction Module

Extracts OCR text from decrypted Google Books tar.gz archives and formats
as JSON arrays for full-text search and analysis. Handles sequential page
files and provides memory-efficient processing for large archives.
"""

import json
import logging
import re
import tarfile
from collections.abc import Iterator
from pathlib import Path

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


def _validate_and_finalize_extraction(
    page_data: dict[int, str], txt_files_found: int, archive_path: str
) -> dict[int, str]:
    """Validate extraction results and return page data."""
    if txt_files_found == 0:
        raise TextExtractionError(f"No .txt files found in archive: {archive_path}")

    if not page_data:
        raise InvalidPageFormatError(f"No valid page files found in archive: {archive_path}")

    logger.debug(f"OCR extraction completed: {len(page_data)} valid pages from {txt_files_found} .txt files")
    return page_data


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


def _build_page_array(page_data: dict[int, str]) -> list[str]:
    """
    Build final page array with empty strings for missing pages.

    Takes a dictionary of page_number -> content and creates a list
    where index corresponds to page number (0-indexed). Missing pages
    are filled with empty strings.

    Args:
        page_data: Dictionary mapping page numbers to content

    Returns:
        List of page contents, 0-indexed
    """
    if not page_data:
        return []

    # Find the range of pages
    min_page = min(page_data.keys())
    max_page = max(page_data.keys())

    # Log any gaps in page numbering
    missing_pages = []
    for page_num in range(min_page, max_page + 1):
        if page_num not in page_data:
            missing_pages.append(page_num)

    if missing_pages:
        logger.warning(f"Missing pages detected: {missing_pages}")

    # Build result array (0-indexed)
    result = []
    for page_num in range(min_page, max_page + 1):
        content = page_data.get(page_num, "")
        result.append(content)

    return result


def _page_content_generator(archive_path: str) -> Iterator[tuple[int, str]]:
    """
    Generator that yields (page_number, content) tuples from archive.

    Memory-efficient extraction that processes one page at a time.
    """
    with tarfile.open(archive_path, "r:gz") as tar:
        # First pass: get all txt files and their page numbers
        txt_members = []
        for member in tar.getmembers():
            if not member.isfile() or not Path(member.name).name.endswith(".txt"):
                continue

            page_num = _parse_page_number(Path(member.name).name)
            if page_num is not None:
                txt_members.append((member, page_num))

        if not txt_members:
            raise TextExtractionError(f"No .txt files found in archive: {archive_path}")

        # Sort by page number
        txt_members.sort(key=lambda x: x[1])

        # Yield pages with gap handling
        expected_page = 1

        for member, page_num in txt_members:
            # Yield empty strings for gaps
            while expected_page < page_num:
                yield (expected_page, "")
                expected_page += 1

            # Extract and yield current page
            file_obj = tar.extractfile(member)
            if file_obj is None:
                content = ""
            else:
                try:
                    content = file_obj.read().decode("utf-8", errors="replace")
                except Exception as e:
                    logger.warning(f"Error reading {member.name}: {e}")
                    content = ""
                finally:
                    file_obj.close()

            yield (page_num, content)
            expected_page = page_num + 1


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


def extract_ocr_to_jsonl_file(extracted_dir_path: Path, output_path: Path) -> int:
    """
    Extract OCR text to JSONL file using streaming from filesystem to minimize memory usage.

    Uses generator to process one page at a time without loading entire files into memory.

    Args:
        extracted_dir_path: Path to extracted archive directory
        output_path: Path to output JSONL file

    Returns:
        Number of pages processed

    FIXME DEPRECATED
    """
    with open(output_path, "w", encoding="utf-8") as f:
        page_count = 0

        for page_num, content in filesystem_page_generator(extracted_dir_path):
            # Write the JSON-encoded content as a single line
            f.write(json.dumps(content, ensure_ascii=False) + "\n")
            page_count += 1

            if page_count % 100 == 0:
                logger.debug(
                    f"Streaming filesystem extraction progress: {page_count} pages written, last page was {page_num}"
                )

    logger.debug(f"OCR text saved to JSONL file (streaming filesystem): {output_path} ({page_count} pages)")
    return page_count
