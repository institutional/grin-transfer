"""
Unit tests for CSV export functionality.

Tests the main export.py module to ensure proper handling of MARC metadata
fields in CSV exports, including field ordering, empty field handling, and
export functionality at various pipeline stages.
"""

from unittest.mock import AsyncMock, MagicMock

import pytest

from grin_to_s3.collect_books.models import BookRecord
from grin_to_s3.export import write_books_to_csv


class TestCSVExport:
    """Test suite for CSV export functionality."""

    @pytest.fixture
    def sample_book_no_marc(self):
        """Create a sample book without MARC metadata."""
        return BookRecord(
            barcode="TEST001",
            title="Sample Book",
            scanned_date="2024-01-01",
            google_books_link="https://books.google.com/books?id=TEST001",
        )

    @pytest.fixture
    def sample_book_with_marc(self):
        """Create a sample book with MARC metadata."""
        return BookRecord(
            barcode="TEST002",
            title="Sample Book with MARC",
            scanned_date="2024-01-01",
            google_books_link="https://books.google.com/books?id=TEST002",
            marc_control_number="123456789",
            marc_date_type="s",
            marc_date_1="2020",
            marc_date_2="2021",
            marc_language="eng",
            marc_lccn="2020012345",
            marc_lc_call_number="QA76.73.P98",
            marc_isbn="9780123456789",
            marc_oclc_numbers="1234567890",
            marc_title="Programming in Python",
            marc_title_remainder="a comprehensive guide",
            marc_author_personal="Smith, John",
            marc_author_corporate="Tech Publishing Corp",
            marc_author_meeting="Python Conference",
            marc_subjects="Computer programming; Python",
            marc_genres="Technical manual",
            marc_general_note="Third edition",
            marc_extraction_timestamp="2024-01-15T10:30:00Z",
        )

    @pytest.fixture
    def mock_sqlite_tracker(self):
        """Create a mock SQLiteProgressTracker."""
        mock_tracker = MagicMock()
        mock_tracker.get_all_books_csv_data = AsyncMock()
        mock_tracker.close = AsyncMock()
        return mock_tracker

    @pytest.mark.parametrize(
        "test_scenario,book_fixture,field_expectations",
        [
            # Test MARC fields presence and order
            (
                "marc_fields_present_and_ordered",
                "sample_book_no_marc",
                {
                    "expected_marc_fields": [
                        "MARC Control Number",
                        "MARC Date Type",
                        "MARC Date 1",
                        "MARC Date 2",
                        "MARC Language",
                        "MARC LCCN",
                        "MARC LC Call Number",
                        "MARC ISBN",
                        "MARC OCLC Numbers",
                        "MARC Title",
                        "MARC Title Remainder",
                        "MARC Author Personal",
                        "MARC Author Corporate",
                        "MARC Author Meeting",
                        "MARC Subjects",
                        "MARC Genres",
                        "MARC General Note",
                        "MARC Extraction Timestamp",
                    ],
                    "check_ordering": True,
                    "check_empty_display": True,
                },
            ),
            # Test populated MARC fields display correctly
            (
                "marc_fields_populated_display",
                "sample_book_with_marc",
                {
                    "specific_field_values": {
                        "MARC Control Number": "123456789",
                        "MARC Title": "Programming in Python",
                        "MARC Author Personal": "Smith, John",
                        "MARC Extraction Timestamp": "2024-01-15T10:30:00Z",
                    },
                },
            ),
        ],
    )
    def test_marc_fields_comprehensive(self, test_scenario, book_fixture, field_expectations, request):
        """Comprehensive MARC field testing: presence, ordering, and display."""
        headers = BookRecord.csv_headers()
        book = request.getfixturevalue(book_fixture)
        csv_row = book.to_csv_row()

        # Test MARC field presence and count
        if "expected_marc_fields" in field_expectations:
            expected_fields = field_expectations["expected_marc_fields"]
            for field in expected_fields:
                assert field in headers, f"MARC field '{field}' missing from CSV headers"

            # Verify no extra unexpected MARC fields
            actual_marc_fields = [h for h in headers if h.startswith("MARC ")]
            assert len(actual_marc_fields) == len(expected_fields), (
                f"Expected {len(expected_fields)} MARC fields, found {len(actual_marc_fields)}"
            )

        # Test MARC field ordering (consecutive positions)
        if field_expectations.get("check_ordering"):
            marc_positions = [i for i, header in enumerate(headers) if header.startswith("MARC ")]
            assert len(marc_positions) > 0, "No MARC fields found in headers"
            for i in range(1, len(marc_positions)):
                assert marc_positions[i] == marc_positions[i - 1] + 1, (
                    f"MARC fields not consecutive at positions {marc_positions[i - 1]} and {marc_positions[i]}"
                )

        # Test empty MARC fields display as empty strings
        if field_expectations.get("check_empty_display"):
            marc_positions = [i for i, header in enumerate(headers) if header.startswith("MARC ")]
            for pos in marc_positions:
                assert csv_row[pos] == "", f"MARC field at position {pos} should be empty string, got '{csv_row[pos]}'"

        # Test specific field values
        if "specific_field_values" in field_expectations:
            for field_name, expected_value in field_expectations["specific_field_values"].items():
                field_pos = headers.index(field_name)
                assert csv_row[field_pos] == expected_value, (
                    f"{field_name} should be '{expected_value}', got '{csv_row[field_pos]}'"
                )

    @pytest.mark.asyncio
    async def test_write_books_to_csv_happy_path(self, mock_sqlite_tracker, sample_book_with_marc, tmp_path):
        """Test CSV writing utility function works correctly."""
        # Mock tracker to return book with MARC data
        mock_sqlite_tracker.get_all_books_csv_data.return_value = [sample_book_with_marc]

        # Test the core CSV writing functionality
        expected_csv_path = tmp_path / "test_books.csv"
        csv_path, record_count = await write_books_to_csv(mock_sqlite_tracker, expected_csv_path)

        assert record_count == 1
        assert csv_path.exists()
        assert csv_path == expected_csv_path

        # Verify file content includes MARC data
        with csv_path.open() as f:
            content = f.read()
            assert "Programming in Python" in content  # MARC title
            assert "Smith, John" in content  # MARC author
            assert "123456789" in content  # MARC control number

    def test_csv_headers_consistency_and_coverage(self):
        """Test CSV header consistency and MARC field requirements."""
        headers = BookRecord.csv_headers()

        # Create a sample book and verify row length matches headers
        sample_book = BookRecord(barcode="TEST001", title="Test")
        csv_row = sample_book.to_csv_row()

        assert len(csv_row) == len(headers), (
            f"CSV row length ({len(csv_row)}) doesn't match headers length ({len(headers)})"
        )

        # Verify all headers are strings
        for header in headers:
            assert isinstance(header, str), f"Header '{header}' is not a string"
            assert header.strip() == header, f"Header '{header}' has extra whitespace"
            assert header != "", "Empty header found"

        # Test MARC field requirements
        required_marc_fields = [
            "MARC Control Number",
            "MARC Date Type",
            "MARC Date 1",
            "MARC Date 2",
            "MARC Language",
            "MARC LCCN",
            "MARC LC Call Number",
            "MARC ISBN",
            "MARC OCLC Numbers",
            "MARC Title",
            "MARC Title Remainder",
            "MARC Author Personal",
            "MARC Author Corporate",
            "MARC Author Meeting",
            "MARC Subjects",
            "MARC Genres",
            "MARC General Note",
            "MARC Extraction Timestamp",
        ]

        # Verify all required fields are present
        for field in required_marc_fields:
            assert field in headers, f"Required MARC field '{field}' missing from CSV headers"

        # Verify no extra unexpected MARC fields
        actual_marc_fields = [h for h in headers if h.startswith("MARC ")]
        assert len(actual_marc_fields) == len(required_marc_fields), (
            f"Expected {len(required_marc_fields)} MARC fields, found {len(actual_marc_fields)}"
        )
