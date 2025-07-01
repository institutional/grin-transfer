#!/usr/bin/env python3
"""
Tests for OCR text extraction functionality.
"""

import json
import sqlite3
import tempfile
from pathlib import Path
from unittest.mock import patch

import pytest

from grin_to_s3.extract.text_extraction import (
    CorruptedArchiveError,
    InvalidPageFormatError,
    TextExtractionError,
    _build_page_array,
    _parse_page_number,
    extract_text_from_archive,
    extract_text_to_jsonl_file,
    get_barcode_from_path,
)
from tests.utils import create_test_archive


@pytest.fixture
def temp_db():
    """Create a temporary database for testing."""
    with tempfile.NamedTemporaryFile(suffix=".db", delete=False) as f:
        db_path = f.name

    # Initialize database with required schema
    conn = sqlite3.connect(db_path)
    conn.execute("""
        CREATE TABLE IF NOT EXISTS book_status_history (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            barcode TEXT NOT NULL,
            status_type TEXT NOT NULL,
            status_value TEXT NOT NULL,
            timestamp TEXT NOT NULL,
            session_id TEXT,
            metadata TEXT
        )
    """)
    conn.commit()
    conn.close()

    yield db_path

    # Cleanup
    Path(db_path).unlink(missing_ok=True)


class TestPageNumberParsing:
    """Test page number parsing functionality."""

    def test_parse_valid_page_numbers(self):
        """Test parsing valid page number formats."""
        assert _parse_page_number("00000001.txt") == 1
        assert _parse_page_number("00000023.txt") == 23
        assert _parse_page_number("00001234.txt") == 1234
        assert _parse_page_number("00999999.txt") == 999999

    def test_parse_invalid_page_numbers(self):
        """Test parsing invalid page number formats."""
        assert _parse_page_number("1.txt") is None
        assert _parse_page_number("000000001.txt") is None  # 9 digits
        assert _parse_page_number("0000000a.txt") is None
        assert _parse_page_number("00000001.html") is None
        assert _parse_page_number("page001.txt") is None
        assert _parse_page_number("00000000.txt") is None  # Zero page
        assert _parse_page_number("01000000.txt") is None  # Too large

    def test_parse_edge_cases(self):
        """Test edge cases for page number parsing."""
        assert _parse_page_number("") is None
        assert _parse_page_number(".txt") is None
        assert _parse_page_number("00000001") is None
        assert _parse_page_number("00000001.TXT") is None  # Case sensitive


class TestPageArrayBuilding:
    """Test page array building with gap handling."""

    def test_build_sequential_pages(self):
        """Test building array with sequential pages."""
        page_data = {1: "page 1", 2: "page 2", 3: "page 3"}
        result = _build_page_array(page_data)
        assert result == ["page 1", "page 2", "page 3"]

    def test_build_with_gaps(self):
        """Test building array with missing pages."""
        page_data = {1: "page 1", 3: "page 3", 5: "page 5"}
        result = _build_page_array(page_data)
        assert result == ["page 1", "", "page 3", "", "page 5"]

    def test_build_single_page(self):
        """Test building array with single page."""
        page_data = {1: "only page"}
        result = _build_page_array(page_data)
        assert result == ["only page"]

    def test_build_empty_dict(self):
        """Test building array with empty page data."""
        result = _build_page_array({})
        assert result == []

    def test_build_non_sequential_start(self):
        """Test building array starting with non-1 page."""
        page_data = {3: "page 3", 4: "page 4", 5: "page 5"}
        result = _build_page_array(page_data)
        assert result == ["page 3", "page 4", "page 5"]

    def test_build_with_empty_content(self):
        """Test building array with empty page content."""
        page_data = {1: "page 1", 2: "", 3: "page 3"}
        result = _build_page_array(page_data)
        assert result == ["page 1", "", "page 3"]


class TestTextExtraction:
    """Test main text extraction functionality."""

    def test_extract_simple_archive(self, temp_db):
        """Test extraction from simple archive."""
        with tempfile.TemporaryDirectory() as temp_dir:
            temp_path = Path(temp_dir)
            pages = {
                "00000001.txt": "First page content",
                "00000002.txt": "Second page content",
                "00000003.txt": "Third page content"
            }
            archive_path = create_test_archive(pages, temp_path)

            result = extract_text_from_archive(str(archive_path), temp_db, "test_session")
            assert result == ["First page content", "Second page content", "Third page content"]

    def test_extract_with_gaps(self, temp_db):
        """Test extraction with missing pages."""
        with tempfile.TemporaryDirectory() as temp_dir:
            temp_path = Path(temp_dir)
            pages = {
                "00000001.txt": "Page 1",
                "00000003.txt": "Page 3",
                "00000005.txt": "Page 5"
            }
            archive_path = create_test_archive(pages, temp_path)

            result = extract_text_from_archive(str(archive_path), temp_db, "test_session")
            assert result == ["Page 1", "", "Page 3", "", "Page 5"]

    def test_extract_with_mixed_files(self, temp_db):
        """Test extraction ignoring non-txt files."""
        with tempfile.TemporaryDirectory() as temp_dir:
            temp_path = Path(temp_dir)
            pages = {
                "00000001.txt": "Text page 1",
                "00000001.jp2": "Binary image data",
                "00000001.html": "<html>HTML content</html>",
                "00000002.txt": "Text page 2",
                "00000002.tif": "Binary TIFF data"
            }
            archive_path = create_test_archive(pages, temp_path)

            result = extract_text_from_archive(str(archive_path), temp_db, "test_session")
            assert result == ["Text page 1", "Text page 2"]

    def test_extract_empty_pages(self, temp_db):
        """Test extraction with empty page content."""
        with tempfile.TemporaryDirectory() as temp_dir:
            temp_path = Path(temp_dir)
            pages = {
                "00000001.txt": "Page 1 content",
                "00000002.txt": "",  # Empty page
                "00000003.txt": "   ",  # Whitespace only
                "00000004.txt": "Page 4 content"
            }
            archive_path = create_test_archive(pages, temp_path)

            result = extract_text_from_archive(str(archive_path), temp_db, "test_session")
            assert result == ["Page 1 content", "", "   ", "Page 4 content"]

    def test_extract_unicode_content(self, temp_db):
        """Test extraction with Unicode characters."""
        with tempfile.TemporaryDirectory() as temp_dir:
            temp_path = Path(temp_dir)
            pages = {
                "00000001.txt": "English text",
                "00000002.txt": "Français avec accents",
                "00000003.txt": "Русский текст",
                "00000004.txt": "中文内容"
            }
            archive_path = create_test_archive(pages, temp_path)

            result = extract_text_from_archive(str(archive_path), temp_db, "test_session")
            assert result == ["English text", "Français avec accents", "Русский текст", "中文内容"]

    def test_extract_with_newlines(self, temp_db):
        """Test extraction preserving newlines in content."""
        with tempfile.TemporaryDirectory() as temp_dir:
            temp_path = Path(temp_dir)
            pages = {
                "00000001.txt": "Line 1\nLine 2\nLine 3",
                "00000002.txt": "Single line",
                "00000003.txt": "\n\nMultiple newlines\n\n"
            }
            archive_path = create_test_archive(pages, temp_path)

            result = extract_text_from_archive(str(archive_path), temp_db, "test_session")
            assert result[0] == "Line 1\nLine 2\nLine 3"
            assert result[1] == "Single line"
            assert result[2] == "\n\nMultiple newlines\n\n"  # Whitespace preserved

    def test_extract_nonexistent_file(self, temp_db):
        """Test extraction with nonexistent archive file."""
        with pytest.raises(TextExtractionError, match="Archive file not found"):
            extract_text_from_archive("/nonexistent/path/archive.tar.gz", temp_db, "test_session")

    def test_extract_wrong_file_type(self, temp_db):
        """Test extraction with wrong file type."""
        with tempfile.NamedTemporaryFile(suffix=".txt") as temp_file:
            with pytest.raises(TextExtractionError, match="Expected .tar.gz archive"):
                extract_text_from_archive(temp_file.name, temp_db, "test_session")

    def test_extract_corrupted_archive(self, temp_db):
        """Test extraction with corrupted archive."""
        with tempfile.NamedTemporaryFile(suffix=".tar.gz", delete=False) as temp_file:
            # Write invalid tar.gz content
            temp_file.write(b"This is not a valid tar.gz file")
            temp_file.flush()

            try:
                with pytest.raises(CorruptedArchiveError, match="Failed to open tar.gz archive"):
                    extract_text_from_archive(temp_file.name, temp_db, "test_session")
            finally:
                Path(temp_file.name).unlink()

    def test_extract_no_txt_files(self, temp_db):
        """Test extraction with archive containing no txt files."""
        with tempfile.TemporaryDirectory() as temp_dir:
            temp_path = Path(temp_dir)
            pages = {
                "00000001.jp2": "Binary image data",
                "00000001.html": "<html>HTML content</html>",
                "data.xml": "XML content"
            }
            archive_path = create_test_archive(pages, temp_path)

            with pytest.raises(TextExtractionError, match="No .txt files found"):
                extract_text_from_archive(str(archive_path), temp_db, "test_session")

    def test_extract_no_valid_pages(self, temp_db):
        """Test extraction with txt files but no valid page format."""
        with tempfile.TemporaryDirectory() as temp_dir:
            temp_path = Path(temp_dir)
            pages = {
                "page1.txt": "Invalid filename format",
                "text.txt": "Another invalid format",
                "1.txt": "Also invalid"
            }
            archive_path = create_test_archive(pages, temp_path)

            with pytest.raises(InvalidPageFormatError, match="No valid page files found"):
                extract_text_from_archive(str(archive_path), temp_db, "test_session")

    @patch("grin_to_s3.extract.text_extraction.logger")
    def test_extract_logs_warnings(self, mock_logger, temp_db):
        """Test that extraction logs appropriate warnings."""
        with tempfile.TemporaryDirectory() as temp_dir:
            temp_path = Path(temp_dir)
            pages = {
                "00000001.txt": "Page 1",
                "00000003.txt": "Page 3",  # Gap at page 2
                "00000005.txt": "Page 5",  # Gap at page 4
                "invalid.txt": "Invalid page format"
            }
            archive_path = create_test_archive(pages, temp_path)

            extract_text_from_archive(str(archive_path), temp_db, "test_session")

            # Should log warnings for missing pages and invalid filename
            mock_logger.warning.assert_any_call("Missing pages detected: [2, 4]")
            mock_logger.warning.assert_any_call("Skipping file with invalid page format: invalid.txt")


class TestTextExtractionToFile:
    """Test text extraction to JSONL file functionality."""

    def test_extract_to_jsonl_file(self, temp_db):
        """Test extracting text and saving to JSONL file."""
        with tempfile.TemporaryDirectory() as temp_dir:
            temp_path = Path(temp_dir)
            pages = {
                "00000001.txt": "Page 1 content",
                "00000002.txt": "Page 2 content"
            }
            archive_path = create_test_archive(pages, temp_path)
            jsonl_path = temp_path / "output.jsonl"

            extract_text_to_jsonl_file(str(archive_path), str(jsonl_path), temp_db, "test_session")

            # Verify file was created and contains correct content
            assert jsonl_path.exists()

            with open(jsonl_path, encoding="utf-8") as f:
                lines = f.readlines()

            # Should be one JSON string per line
            assert len(lines) == 2
            assert json.loads(lines[0]) == "Page 1 content"
            assert json.loads(lines[1]) == "Page 2 content"

    def test_extract_to_jsonl_with_unicode(self, temp_db):
        """Test extracting Unicode text to JSONL file."""
        with tempfile.TemporaryDirectory() as temp_dir:
            temp_path = Path(temp_dir)
            pages = {
                "00000001.txt": "English",
                "00000002.txt": "Français",
                "00000003.txt": "中文"
            }
            archive_path = create_test_archive(pages, temp_path)
            jsonl_path = temp_path / "unicode.jsonl"

            extract_text_to_jsonl_file(str(archive_path), str(jsonl_path), temp_db, "test_session")

            with open(jsonl_path, encoding="utf-8") as f:
                lines = f.readlines()

            assert len(lines) == 3
            assert json.loads(lines[0]) == "English"
            assert json.loads(lines[1]) == "Français"
            assert json.loads(lines[2]) == "中文"

    def test_extract_to_jsonl_creates_directories(self, temp_db):
        """Test that extraction creates parent directories."""
        with tempfile.TemporaryDirectory() as temp_dir:
            temp_path = Path(temp_dir)
            pages = {"00000001.txt": "Test content"}
            archive_path = create_test_archive(pages, temp_path)

            # Create nested path that doesn't exist
            jsonl_path = temp_path / "nested" / "deep" / "output.jsonl"

            extract_text_to_jsonl_file(str(archive_path), str(jsonl_path), temp_db, "test_session")

            assert jsonl_path.exists()
            with open(jsonl_path, encoding="utf-8") as f:
                lines = f.readlines()
            assert len(lines) == 1
            assert json.loads(lines[0]) == "Test content"

    def test_extract_to_jsonl_streaming(self, temp_db):
        """Test streaming extraction mode."""
        with tempfile.TemporaryDirectory() as temp_dir:
            temp_path = Path(temp_dir)
            pages = {
                "00000001.txt": "Page 1",
                "00000002.txt": "Page 2",
                "00000003.txt": "Page 3"
            }
            archive_path = create_test_archive(pages, temp_path)
            jsonl_path = temp_path / "output_streaming.jsonl"

            # Use streaming mode
            extract_text_to_jsonl_file(str(archive_path), str(jsonl_path), temp_db, "test_session")

            assert jsonl_path.exists()
            with open(jsonl_path, encoding="utf-8") as f:
                lines = f.readlines()
            assert len(lines) == 3
            assert json.loads(lines[0]) == "Page 1"
            assert json.loads(lines[1]) == "Page 2"
            assert json.loads(lines[2]) == "Page 3"

    def test_extract_to_jsonl_streaming_with_gaps(self, temp_db):
        """Test streaming extraction with missing pages."""
        with tempfile.TemporaryDirectory() as temp_dir:
            temp_path = Path(temp_dir)
            pages = {
                "00000001.txt": "Page 1",
                "00000003.txt": "Page 3",
                "00000005.txt": "Page 5"
            }
            archive_path = create_test_archive(pages, temp_path)
            jsonl_path = temp_path / "output_gaps.jsonl"

            extract_text_to_jsonl_file(str(archive_path), str(jsonl_path), temp_db, "test_session")

            with open(jsonl_path, encoding="utf-8") as f:
                lines = f.readlines()
            assert len(lines) == 5
            assert json.loads(lines[0]) == "Page 1"
            assert json.loads(lines[1]) == ""
            assert json.loads(lines[2]) == "Page 3"
            assert json.loads(lines[3]) == ""
            assert json.loads(lines[4]) == "Page 5"

    def test_extract_to_jsonl_with_newlines_and_quotes(self, temp_db):
        """Test that newlines and quotes are properly JSON-escaped in JSONL."""
        with tempfile.TemporaryDirectory() as temp_dir:
            temp_path = Path(temp_dir)
            pages = {
                "00000001.txt": "This has\na newline",
                "00000002.txt": 'This has "quotes" in it',
                "00000003.txt": 'Both\nnewline and "quotes"'
            }
            archive_path = create_test_archive(pages, temp_path)
            jsonl_path = temp_path / "special_chars.jsonl"

            extract_text_to_jsonl_file(str(archive_path), str(jsonl_path), temp_db, "test_session")

            with open(jsonl_path, encoding="utf-8") as f:
                lines = f.readlines()

            assert len(lines) == 3
            # JSON escaping should handle newlines and quotes properly
            assert json.loads(lines[0]) == "This has\na newline"
            assert json.loads(lines[1]) == 'This has "quotes" in it'
            assert json.loads(lines[2]) == 'Both\nnewline and "quotes"'

    def test_jsonl_format_line_by_line_parsing(self, temp_db):
        """Test that JSONL output can be parsed line-by-line as a stream."""
        with tempfile.TemporaryDirectory() as temp_dir:
            temp_path = Path(temp_dir)
            pages = {
                "00000001.txt": "Simple text",
                "00000002.txt": "Text with\nmultiple\nnewlines",
                "00000003.txt": 'Text with "quotes" and \'single quotes\'',
                "00000004.txt": "Text with \\backslashes\\ and /forward/slashes/",
                "00000005.txt": "Text with\ttabs\tand\rcarriage\rreturns",
                "00000006.txt": "Text with unicode: \u2022 bullet • and emoji 🎉",
                "00000007.txt": "Text with\x00null\x00bytes",  # null bytes should be handled
                "00000008.txt": "",  # empty page
                "00000009.txt": "   \n\t  ",  # whitespace only
                "00000010.txt": "Very long line " + "x" * 10000,  # long content
                "00000011.txt": "Text with    multiple    spaces    between    words",
                "00000012.txt": "Text\nwith\n\nblank\n\n\nlines",
                "00000013.txt": "  Leading whitespace",
                "00000014.txt": "Trailing whitespace  ",
                "00000015.txt": "  Both leading and trailing  ",
            }
            archive_path = create_test_archive(pages, temp_path)
            jsonl_path = temp_path / "comprehensive.jsonl"

            extract_text_to_jsonl_file(str(archive_path), str(jsonl_path), temp_db, "test_session")

            # Parse line by line as a stream would
            parsed_pages = []
            with open(jsonl_path, encoding="utf-8") as f:
                for line_num, line in enumerate(f, 1):
                    try:
                        # Each line should be valid JSON
                        parsed = json.loads(line.rstrip("\n"))
                        parsed_pages.append(parsed)
                    except json.JSONDecodeError as e:
                        pytest.fail(f"Line {line_num} is not valid JSON: {e}")

            # Verify all content was preserved correctly
            assert len(parsed_pages) == 15
            assert parsed_pages[0] == "Simple text"
            assert parsed_pages[1] == "Text with\nmultiple\nnewlines"
            assert parsed_pages[2] == 'Text with "quotes" and \'single quotes\''
            assert parsed_pages[3] == "Text with \\backslashes\\ and /forward/slashes/"
            assert parsed_pages[4] == "Text with\ttabs\tand\rcarriage\rreturns"
            assert parsed_pages[5] == "Text with unicode: • bullet • and emoji 🎉"
            assert parsed_pages[6] == "Text with\x00null\x00bytes"
            assert parsed_pages[7] == ""
            assert parsed_pages[8] == "   \n\t  "  # whitespace is preserved
            assert parsed_pages[9].startswith("Very long line ")
            assert len(parsed_pages[9]) > 10000
            assert parsed_pages[10] == "Text with    multiple    spaces    between    words"
            assert parsed_pages[11] == "Text\nwith\n\nblank\n\n\nlines"
            assert parsed_pages[12] == "  Leading whitespace"
            assert parsed_pages[13] == "Trailing whitespace  "
            assert parsed_pages[14] == "  Both leading and trailing  "

    def test_jsonl_format_edge_cases_streaming(self, temp_db):
        """Test JSONL edge cases with streaming mode."""
        with tempfile.TemporaryDirectory() as temp_dir:
            temp_path = Path(temp_dir)
            # Test edge cases that might break JSON encoding
            pages = {
                "00000001.txt": '{"}\': [],',  # JSON-like content
                "00000002.txt": "Line 1\nLine 2\nLine 3\nLine 4\nLine 5",  # Multiple lines
                "00000003.txt": '"\\n\\r\\t\\b\\f\\"',  # Escaped special chars
                "00000004.txt": "\u0000\u0001\u0002\u0003\u0004",  # Control characters
                "00000005.txt": "🎨🎭🎪🎯🎲🎰🎮🎸🎺🎻",  # Only emojis
            }
            archive_path = create_test_archive(pages, temp_path)
            jsonl_path = temp_path / "edge_cases.jsonl"

            # Test with streaming mode too
            extract_text_to_jsonl_file(str(archive_path), str(jsonl_path), temp_db, "test_session")

            # Verify each line is independently parseable
            with open(jsonl_path, encoding="utf-8") as f:
                lines = f.readlines()

            assert len(lines) == 5

            # Each line should parse correctly
            assert json.loads(lines[0]) == '{"}\': [],'
            assert json.loads(lines[1]) == "Line 1\nLine 2\nLine 3\nLine 4\nLine 5"
            assert json.loads(lines[2]) == '"\\n\\r\\t\\b\\f\\"'
            assert json.loads(lines[3]) == "\u0000\u0001\u0002\u0003\u0004"
            assert json.loads(lines[4]) == "🎨🎭🎪🎯🎲🎰🎮🎸🎺🎻"

            # Also verify the file can be read as a JSONL stream
            with open(jsonl_path, encoding="utf-8") as f:
                for i, line in enumerate(f):
                    parsed = json.loads(line)
                    assert isinstance(parsed, str), f"Line {i+1} should parse to a string"


class TestErrorHandling:
    """Test error handling and edge cases."""

    def test_extraction_error_inheritance(self):
        """Test that custom exceptions inherit correctly."""
        assert issubclass(CorruptedArchiveError, TextExtractionError)
        assert issubclass(InvalidPageFormatError, TextExtractionError)

    def test_corrupted_archive_error_message(self, temp_db):
        """Test CorruptedArchiveError with specific message."""
        with tempfile.NamedTemporaryFile(suffix=".tar.gz", delete=False) as temp_file:
            temp_file.write(b"Not a tar file")
            temp_file.flush()

            try:
                with pytest.raises(CorruptedArchiveError) as exc_info:
                    extract_text_from_archive(temp_file.name, temp_db, "test_session")

                assert "Failed to open tar.gz archive" in str(exc_info.value)
            finally:
                Path(temp_file.name).unlink()

    @patch("tarfile.open")
    def test_unexpected_error_handling(self, mock_tarfile_open, temp_db):
        """Test handling of unexpected errors during extraction."""
        mock_tarfile_open.side_effect = RuntimeError("Unexpected error")

        with tempfile.NamedTemporaryFile(suffix=".tar.gz") as temp_file:
            with pytest.raises(TextExtractionError, match="Unexpected error during extraction"):
                extract_text_from_archive(temp_file.name, temp_db, "test_session")


class TestPerformanceConsiderations:
    """Test performance-related aspects of text extraction."""

    def test_memory_efficient_processing(self, temp_db):
        """Test that large archives are processed efficiently."""
        # This test verifies the extraction approach doesn't load entire archive into memory
        with tempfile.TemporaryDirectory() as temp_dir:
            temp_path = Path(temp_dir)

            # Create archive with many pages (simulating large book)
            pages = {}
            for i in range(1, 101):  # 100 pages
                pages[f"{i:08d}.txt"] = f"Content for page {i} " * 10  # Some content

            archive_path = create_test_archive(pages, temp_path)

            result = extract_text_from_archive(str(archive_path), temp_db, "test_session")

            assert len(result) == 100
            assert result[0].startswith("Content for page 1")
            assert result[99].startswith("Content for page 100")

    def test_disk_extraction_method(self, temp_db):
        """Test disk-based extraction method."""
        with tempfile.TemporaryDirectory() as temp_dir:
            temp_path = Path(temp_dir)
            pages = {
                "00000001.txt": "First page content",
                "00000002.txt": "Second page content",
                "00000003.txt": "Third page content"
            }
            archive_path = create_test_archive(pages, temp_path)

            result = extract_text_from_archive(
                str(archive_path),
                temp_db,
                "test_session",
                extract_to_disk=True,
                extraction_dir=str(temp_path / "extracted")
            )
            assert result == ["First page content", "Second page content", "Third page content"]

    def test_disk_extraction_keeps_files(self, temp_db):
        """Test disk-based extraction with keep_extracted=True."""
        with tempfile.TemporaryDirectory() as temp_dir:
            temp_path = Path(temp_dir)
            extraction_dir = temp_path / "extracted"
            pages = {
                "00000001.txt": "Page 1",
                "00000002.txt": "Page 2"
            }
            archive_path = create_test_archive(pages, temp_path)

            result = extract_text_from_archive(
                str(archive_path),
                temp_db,
                "test_session",
                extract_to_disk=True,
                extraction_dir=str(extraction_dir),
                keep_extracted=True
            )
            assert result == ["Page 1", "Page 2"]

            # Verify files were kept
            barcode = get_barcode_from_path(str(archive_path))
            barcode_dir = extraction_dir / barcode
            assert barcode_dir.exists()
            assert (barcode_dir / "00000001.txt").exists()
            assert (barcode_dir / "00000002.txt").exists()


class TestBarcodeExtraction:
    """Test barcode extraction from different file path formats."""

    def test_extract_barcode_from_tar_gz(self):
        """Test extracting barcode from .tar.gz files."""
        assert get_barcode_from_path("TZ1JJG.tar.gz") == "TZ1JJG"
        assert get_barcode_from_path("ABCD1234.tar.gz") == "ABCD1234"
        assert get_barcode_from_path("/path/to/TZ1JJG.tar.gz") == "TZ1JJG"

    def test_extract_barcode_from_encrypted_archives(self):
        """Test extracting barcode from .tar.gz.gpg files."""
        assert get_barcode_from_path("TZ1JJG.tar.gz.gpg") == "TZ1JJG"
        assert get_barcode_from_path("ABCD1234.tar.gz.gpg") == "ABCD1234"
        assert get_barcode_from_path("/path/to/TZ1JJG.tar.gz.gpg") == "TZ1JJG"

    def test_extract_barcode_from_decrypted_archives(self):
        """Test extracting barcode from .decrypted.tar.gz files."""
        assert get_barcode_from_path("TZ1JJG.decrypted.tar.gz") == "TZ1JJG"
        assert get_barcode_from_path("ABCD1234.decrypted.tar.gz") == "ABCD1234"
        assert get_barcode_from_path("/path/to/TZ1JJG.decrypted.tar.gz") == "TZ1JJG"

    def test_extract_barcode_from_tar_files(self):
        """Test extracting barcode from .tar files."""
        assert get_barcode_from_path("TZ1JJG.tar") == "TZ1JJG"
        assert get_barcode_from_path("/path/to/ABCD1234.tar") == "ABCD1234"

    def test_extract_barcode_fallback(self):
        """Test fallback for other file types."""
        assert get_barcode_from_path("TZ1JJG.txt") == "TZ1JJG"
        assert get_barcode_from_path("/path/to/ABCD1234.zip") == "ABCD1234"

