#!/usr/bin/env python3
"""
Tests for GRIN parsing functions.
"""

from grin_to_s3.collect_books.grin_parser import parse_grin_date, parse_grin_row


class TestGrinParser:
    """Test GRIN data parsing functions."""

    def test_parse_grin_date_valid(self):
        """parse_grin_date should convert valid GRIN dates to ISO format."""
        result = parse_grin_date("2024/01/15 14:30")
        assert result == "2024-01-15T14:30:00"

    def test_parse_grin_date_invalid(self):
        """parse_grin_date should return original string for invalid dates."""
        invalid_date = "not-a-date"
        result = parse_grin_date(invalid_date)
        assert result == invalid_date

    def test_parse_grin_date_empty(self):
        """parse_grin_date should handle empty strings."""
        result = parse_grin_date("")
        assert result == ""

    def test_parse_grin_row_basic(self):
        """parse_grin_row should map basic GRIN row data."""
        row = {"barcode": "TEST123", "title": "Test Book Title", "scanned": "2024/01/15 10:00", "state": "scanned"}

        result = parse_grin_row(row)

        assert result["barcode"] == "TEST123"
        assert result["title"] == "Test Book Title"
        assert result["scanned_date"] == "2024-01-15T10:00:00"
        assert result["grin_state"] == "scanned"

    def test_parse_grin_row_empty(self):
        """parse_grin_row should handle empty or invalid input."""
        assert parse_grin_row({}) == {}
        assert parse_grin_row({"title": "No barcode"}) == {}

    def test_parse_grin_row_google_books_link(self):
        """parse_grin_row should extract Google Books links."""
        row = {"barcode": "TEST456", "google_books": "https://books.google.com/books?id=xyz"}

        result = parse_grin_row(row)

        assert result["barcode"] == "TEST456"
        assert result["google_books_link"] == "https://books.google.com/books?id=xyz"

    def test_parse_grin_row_field_variations(self):
        """parse_grin_row should handle various field name formats."""
        row = {
            "barcode": "TEST789",
            "Converted Date": "2024/01/20 12:00",  # Capital case
            "OCR_date": "2024/01/21 13:00",  # Underscore
            "Status": "converted",  # Status instead of state
        }

        result = parse_grin_row(row)

        assert result["barcode"] == "TEST789"
        assert result["converted_date"] == "2024-01-20T12:00:00"
        assert result["ocr_date"] == "2024-01-21T13:00:00"
        assert result["grin_state"] == "converted"
