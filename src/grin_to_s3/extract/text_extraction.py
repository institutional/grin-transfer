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
import tempfile
import time
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


def extract_text_from_archive(
    archive_path: str,
    extraction_dir: str | None = None,
    keep_extracted: bool = False,
    extract_to_disk: bool = True,
) -> list[str]:
    """
    Extract OCR text from decrypted tar.gz archive and return as JSON array.

    Processes sequential page files (00000001.txt, 00000002.txt, etc.) from
    Google Books archives, sorts by page number, and handles missing pages
    by inserting empty strings at correct indices.

    Args:
        archive_path: Path to decrypted tar.gz archive
        extraction_dir: Directory to extract archive to. If None and extract_to_disk=True,
                       uses temporary directory. Ignored if extract_to_disk=False.
        keep_extracted: Whether to keep extracted files after processing.
                       Only applies when extract_to_disk=True. Default False (cleanup).
        extract_to_disk: If True (default), extract to disk first for better performance.
                        If False, extract files directly in memory.

    Returns:
        List of strings where index corresponds to page number (0-indexed)
        Missing pages are represented as empty strings

    Raises:
        TextExtractionError: When extraction fails
        CorruptedArchiveError: When archive cannot be opened
        InvalidPageFormatError: When page files have invalid format
    """
    archive_path_obj = Path(archive_path)

    if not archive_path_obj.exists():
        raise TextExtractionError(f"Archive file not found: {archive_path}")

    if not archive_path_obj.suffix.endswith(".gz"):
        raise TextExtractionError(f"Expected .tar.gz archive, got: {archive_path}")

    logger.debug(f"Starting OCR text extraction from {archive_path} (extract_to_disk={extract_to_disk})")

    try:
        if extract_to_disk:
            page_data = _extract_text_from_disk(archive_path, extraction_dir, keep_extracted)
        else:
            page_data = _extract_text_from_memory(archive_path)

        # Build final page array with proper indexing
        result = _build_page_array(page_data)
        return result

    except tarfile.TarError as e:
        raise CorruptedArchiveError(f"Failed to open tar.gz archive: {e}") from e
    except TextExtractionError:
        raise
    except Exception as e:
        raise TextExtractionError(f"Unexpected error during extraction: {e}") from e


def _extract_text_from_memory(archive_path: str) -> dict[int, str]:
    """Extract text using in-memory approach for low memory overhead."""
    with tarfile.open(archive_path, "r:gz") as tar:
        page_data = {}
        txt_files_found = 0

        # Extract text content from .txt files in memory-efficient way
        for member in tar.getmembers():
            if not member.isfile() or not Path(member.name).name.endswith(".txt"):
                continue

            filename = Path(member.name).name
            txt_files_found += 1
            page_number = _parse_page_number(filename)

            if page_number is None:
                logger.warning(f"Skipping file with invalid page format: {filename}")
                continue

            # Extract file content without loading entire archive into memory
            file_obj = tar.extractfile(member)
            if file_obj is None:
                logger.warning(f"Could not extract file content: {filename}")
                page_data[page_number] = ""
                continue

            try:
                # Read file content and decode as UTF-8
                content = file_obj.read().decode("utf-8", errors="replace")
                page_data[page_number] = content
                logger.debug(f"Extracted page {page_number}: {len(content):,} characters")
            except UnicodeDecodeError:
                logger.warning(f"Unicode decode error in {filename}, using empty string")
                page_data[page_number] = ""
            finally:
                file_obj.close()

        return _validate_and_finalize_extraction(page_data, txt_files_found, archive_path)


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


def _extract_text_from_disk(
    archive_path: str, extraction_dir: str | None = None, keep_extracted: bool = False
) -> dict[int, str]:
    """Extract text using disk-based approach for parallel processing."""
    # Get barcode for directory naming
    barcode = get_barcode_from_path(archive_path)

    # Determine extraction directory
    if extraction_dir is None:
        temp_dir = tempfile.mkdtemp(prefix="grin_extraction_")
        extract_to = Path(temp_dir) / barcode
        cleanup_temp = True
    else:
        extract_to = Path(extraction_dir) / barcode
        cleanup_temp = False

    extract_path = Path(extract_to)
    extract_path.mkdir(parents=True, exist_ok=True)

    logger.debug(f"Extracting archive to {extract_path}")

    try:
        # Extract the entire archive to disk
        with tarfile.open(archive_path, "r:gz") as tar:
            # Extract only .txt files for efficiency
            txt_members = [m for m in tar.getmembers() if m.name.endswith(".txt") and m.isfile()]

            if not txt_members:
                raise TextExtractionError(f"No .txt files found in archive: {archive_path}")

            for member in txt_members:
                tar.extract(member, extract_path)

        # Now read all the extracted .txt files
        page_data = {}
        txt_files_found = 0

        # Find all .txt files in the extraction directory
        for txt_file in extract_path.glob("**/*.txt"):
            txt_files_found += 1
            page_number = _parse_page_number(txt_file.name)

            if page_number is None:
                logger.warning(f"Skipping file with invalid page format: {txt_file.name}")
                continue

            try:
                with open(txt_file, encoding="utf-8", errors="replace") as f:
                    content = f.read()
                page_data[page_number] = content
                logger.debug(f"Read page {page_number}: {len(content):,} characters")
            except Exception as e:
                logger.warning(f"Error reading {txt_file.name}: {e}")
                page_data[page_number] = ""

        return _validate_and_finalize_extraction(page_data, txt_files_found, archive_path)

    finally:
        # Cleanup extracted files unless requested to keep them
        if cleanup_temp or not keep_extracted:
            import shutil
            try:
                if cleanup_temp:
                    # Remove temporary directory entirely (parent of barcode dir)
                    temp_parent = extract_path.parent
                    shutil.rmtree(temp_parent)
                    logger.debug(f"Cleaned up temporary extraction directory: {temp_parent}")
                elif not keep_extracted:
                    # Remove barcode subdirectory entirely
                    shutil.rmtree(extract_path)
                    logger.debug(f"Cleaned up barcode extraction directory: {extract_path}")
            except Exception as cleanup_error:
                logger.warning(f"Failed to cleanup extraction files: {cleanup_error}")


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


def extract_text_to_jsonl_file(archive_path: str, output_path: str) -> int:
    """
    Extract text from archive and save directly to JSONL file.

    Uses memory-efficient generator approach, reading one page at a time
    and writing each page as a JSON-encoded string per line.
    Format: "page 1 text"\n"page 2 text"\n...

    Args:
        archive_path: Path to decrypted tar.gz archive
        output_path: Path where JSONL file should be written

    Returns:
        Number of pages processed

    Raises:
        TextExtractionError: When extraction fails
    """
    output_path_obj = Path(output_path)
    output_path_obj.parent.mkdir(parents=True, exist_ok=True)

    page_count = _extract_text_to_jsonl_file_streaming(archive_path, output_path_obj)
    return page_count


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


def _extract_text_to_jsonl_file_streaming(archive_path: str, output_path: Path) -> int:
    """
    Extract text to JSONL file using streaming to minimize memory usage.

    Writes one JSON-encoded string per line, processing one page at a time.

    Returns:
        Number of pages processed
    """
    try:
        with open(output_path, "w", encoding="utf-8") as f:
            page_count = 0

            for _page_num, content in _page_content_generator(archive_path):
                # Write the JSON-encoded content as a single line
                f.write(json.dumps(content, ensure_ascii=False) + "\n")
                page_count += 1

                if page_count % 100 == 0:
                    logger.debug(f"Streaming extraction progress: {page_count} pages written")

        logger.debug(f"OCR text saved to JSONL file (streaming): {output_path} ({page_count} pages)")
        return page_count

    except Exception:
        # Clean up partial file on error
        if output_path.exists():
            output_path.unlink()
        raise


# Tracking functions for when database tracking is needed
def extract_text_with_tracking(
    archive_path: str,
    db_path: str,
    session_id: str | None = None,
    extraction_dir: str | None = None,
    keep_extracted: bool = False,
    extract_to_disk: bool = True,
) -> list[str]:
    """Extract text with database tracking."""
    from .tracking import ExtractionMethod, track_completion, track_failure, track_start

    barcode = get_barcode_from_path(archive_path)
    method = ExtractionMethod.DISK if extract_to_disk else ExtractionMethod.MEMORY

    track_start(db_path, barcode, session_id)
    start_time = time.time()

    try:
        result = extract_text_from_archive(archive_path, extraction_dir, keep_extracted, extract_to_disk)
        extraction_time_ms = int((time.time() - start_time) * 1000)
        track_completion(db_path, barcode, len(result), extraction_time_ms, method, session_id)
        return result
    except Exception as e:
        track_failure(db_path, barcode, e, method, session_id)
        raise


def extract_text_to_jsonl_with_tracking(
    archive_path: str,
    output_path: str,
    db_path: str,
    session_id: str | None = None,
) -> int:
    """Extract text to JSONL with database tracking."""
    from .tracking import ExtractionMethod, track_completion, track_failure, track_start

    barcode = get_barcode_from_path(archive_path)

    track_start(db_path, barcode, session_id)
    start_time = time.time()

    try:
        page_count = extract_text_to_jsonl_file(archive_path, output_path)
        extraction_time_ms = int((time.time() - start_time) * 1000)
        file_size = Path(output_path).stat().st_size
        track_completion(
            db_path,
            barcode,
            page_count,
            extraction_time_ms,
            ExtractionMethod.STREAMING,
            session_id,
            file_size,
            output_path,
        )
        return page_count
    except Exception as e:
        track_failure(db_path, barcode, e, ExtractionMethod.STREAMING, session_id)
        raise

