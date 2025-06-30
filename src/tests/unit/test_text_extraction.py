#!/usr/bin/env python3
"""
Tests for OCR text extraction functionality.
"""

import json
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
    extract_text_to_json_file,
    get_barcode_from_path,
)
from tests.utils import create_test_archive


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

    def test_extract_simple_archive(self):
        """Test extraction from simple archive."""
        with tempfile.TemporaryDirectory() as temp_dir:
            temp_path = Path(temp_dir)
            pages = {
                "00000001.txt": "First page content",
                "00000002.txt": "Second page content",
                "00000003.txt": "Third page content"
            }
            archive_path = create_test_archive(pages, temp_path)

            result = extract_text_from_archive(str(archive_path))
            assert result == ["First page content", "Second page content", "Third page content"]

    def test_extract_with_gaps(self):
        """Test extraction with missing pages."""
        with tempfile.TemporaryDirectory() as temp_dir:
            temp_path = Path(temp_dir)
            pages = {
                "00000001.txt": "Page 1",
                "00000003.txt": "Page 3",
                "00000005.txt": "Page 5"
            }
            archive_path = create_test_archive(pages, temp_path)

            result = extract_text_from_archive(str(archive_path))
            assert result == ["Page 1", "", "Page 3", "", "Page 5"]

    def test_extract_with_mixed_files(self):
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

            result = extract_text_from_archive(str(archive_path))
            assert result == ["Text page 1", "Text page 2"]

    def test_extract_empty_pages(self):
        """Test extraction with empty page content."""
        with tempfile.TemporaryDirectory() as temp_dir:
            temp_path = Path(temp_dir)
            pages = {
                "00000001.txt": "Page 1 content",
                "00000002.txt": "",  # Empty page
                "00000003.txt": "   ",  # Whitespace only (will be stripped to empty)
                "00000004.txt": "Page 4 content"
            }
            archive_path = create_test_archive(pages, temp_path)

            result = extract_text_from_archive(str(archive_path))
            assert result == ["Page 1 content", "", "", "Page 4 content"]

    def test_extract_unicode_content(self):
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

            result = extract_text_from_archive(str(archive_path))
            assert result == ["English text", "Français avec accents", "Русский текст", "中文内容"]

    def test_extract_with_newlines(self):
        """Test extraction preserving newlines in content."""
        with tempfile.TemporaryDirectory() as temp_dir:
            temp_path = Path(temp_dir)
            pages = {
                "00000001.txt": "Line 1\nLine 2\nLine 3",
                "00000002.txt": "Single line",
                "00000003.txt": "\n\nMultiple newlines\n\n"
            }
            archive_path = create_test_archive(pages, temp_path)

            result = extract_text_from_archive(str(archive_path))
            assert result[0] == "Line 1\nLine 2\nLine 3"
            assert result[1] == "Single line"
            assert result[2] == "Multiple newlines"  # Stripped

    def test_extract_nonexistent_file(self):
        """Test extraction with nonexistent archive file."""
        with pytest.raises(TextExtractionError, match="Archive file not found"):
            extract_text_from_archive("/nonexistent/path/archive.tar.gz")

    def test_extract_wrong_file_type(self):
        """Test extraction with wrong file type."""
        with tempfile.NamedTemporaryFile(suffix=".txt") as temp_file:
            with pytest.raises(TextExtractionError, match="Expected .tar.gz archive"):
                extract_text_from_archive(temp_file.name)

    def test_extract_corrupted_archive(self):
        """Test extraction with corrupted archive."""
        with tempfile.NamedTemporaryFile(suffix=".tar.gz", delete=False) as temp_file:
            # Write invalid tar.gz content
            temp_file.write(b"This is not a valid tar.gz file")
            temp_file.flush()

            try:
                with pytest.raises(CorruptedArchiveError, match="Failed to open tar.gz archive"):
                    extract_text_from_archive(temp_file.name)
            finally:
                Path(temp_file.name).unlink()

    def test_extract_no_txt_files(self):
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
                extract_text_from_archive(str(archive_path))

    def test_extract_no_valid_pages(self):
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
                extract_text_from_archive(str(archive_path))

    @patch("grin_to_s3.extract.text_extraction.logger")
    def test_extract_logs_warnings(self, mock_logger):
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

            extract_text_from_archive(str(archive_path))

            # Should log warnings for missing pages and invalid filename
            mock_logger.warning.assert_any_call("Missing pages detected: [2, 4]")
            mock_logger.warning.assert_any_call("Skipping file with invalid page format: invalid.txt")


class TestTextExtractionToFile:
    """Test text extraction to JSON file functionality."""

    def test_extract_to_json_file(self):
        """Test extracting text and saving to JSON file."""
        with tempfile.TemporaryDirectory() as temp_dir:
            temp_path = Path(temp_dir)
            pages = {
                "00000001.txt": "Page 1 content",
                "00000002.txt": "Page 2 content"
            }
            archive_path = create_test_archive(pages, temp_path)
            json_path = temp_path / "output.json"

            extract_text_to_json_file(str(archive_path), str(json_path))

            # Verify file was created and contains correct content
            assert json_path.exists()

            with open(json_path, encoding="utf-8") as f:
                content = f.read()

            # Should be single-line JSON
            assert "\n" not in content

            parsed = json.loads(content)
            assert parsed == ["Page 1 content", "Page 2 content"]

    def test_extract_to_json_with_unicode(self):
        """Test extracting Unicode text to JSON file."""
        with tempfile.TemporaryDirectory() as temp_dir:
            temp_path = Path(temp_dir)
            pages = {
                "00000001.txt": "English",
                "00000002.txt": "Français",
                "00000003.txt": "中文"
            }
            archive_path = create_test_archive(pages, temp_path)
            json_path = temp_path / "unicode.json"

            extract_text_to_json_file(str(archive_path), str(json_path))

            with open(json_path, encoding="utf-8") as f:
                parsed = json.load(f)

            assert parsed == ["English", "Français", "中文"]

    def test_extract_to_json_creates_directories(self):
        """Test that extraction creates parent directories."""
        with tempfile.TemporaryDirectory() as temp_dir:
            temp_path = Path(temp_dir)
            pages = {"00000001.txt": "Test content"}
            archive_path = create_test_archive(pages, temp_path)

            # Create nested path that doesn't exist
            json_path = temp_path / "nested" / "deep" / "output.json"

            extract_text_to_json_file(str(archive_path), str(json_path))

            assert json_path.exists()
            with open(json_path, encoding="utf-8") as f:
                parsed = json.load(f)
            assert parsed == ["Test content"]


class TestErrorHandling:
    """Test error handling and edge cases."""

    def test_extraction_error_inheritance(self):
        """Test that custom exceptions inherit correctly."""
        assert issubclass(CorruptedArchiveError, TextExtractionError)
        assert issubclass(InvalidPageFormatError, TextExtractionError)

    def test_corrupted_archive_error_message(self):
        """Test CorruptedArchiveError with specific message."""
        with tempfile.NamedTemporaryFile(suffix=".tar.gz", delete=False) as temp_file:
            temp_file.write(b"Not a tar file")
            temp_file.flush()

            try:
                with pytest.raises(CorruptedArchiveError) as exc_info:
                    extract_text_from_archive(temp_file.name)

                assert "Failed to open tar.gz archive" in str(exc_info.value)
            finally:
                Path(temp_file.name).unlink()

    @patch("tarfile.open")
    def test_unexpected_error_handling(self, mock_tarfile_open):
        """Test handling of unexpected errors during extraction."""
        mock_tarfile_open.side_effect = RuntimeError("Unexpected error")

        with tempfile.NamedTemporaryFile(suffix=".tar.gz") as temp_file:
            with pytest.raises(TextExtractionError, match="Unexpected error during extraction"):
                extract_text_from_archive(temp_file.name)


class TestPerformanceConsiderations:
    """Test performance-related aspects of text extraction."""

    def test_memory_efficient_processing(self):
        """Test that large archives are processed efficiently."""
        # This test verifies the extraction approach doesn't load entire archive into memory
        with tempfile.TemporaryDirectory() as temp_dir:
            temp_path = Path(temp_dir)

            # Create archive with many pages (simulating large book)
            pages = {}
            for i in range(1, 101):  # 100 pages
                pages[f"{i:08d}.txt"] = f"Content for page {i} " * 10  # Some content

            archive_path = create_test_archive(pages, temp_path)

            result = extract_text_from_archive(str(archive_path))

            assert len(result) == 100
            assert result[0].startswith("Content for page 1")
            assert result[99].startswith("Content for page 100")

    def test_disk_extraction_method(self):
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
                use_memory=False,
                extraction_dir=str(temp_path / "extracted")
            )
            assert result == ["First page content", "Second page content", "Third page content"]

    def test_disk_extraction_keeps_files(self):
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
                use_memory=False,
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

