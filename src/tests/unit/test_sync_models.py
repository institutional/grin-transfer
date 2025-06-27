#!/usr/bin/env python3
"""
Tests for sync models and data validation.
"""

import pytest

from grin_to_s3.sync.models import (
    create_book_sync_result,
    create_sync_stats,
    validate_and_parse_barcodes,
)


class TestValidateAndParseBarcodes:
    """Test barcode validation and parsing."""

    def test_valid_single_barcode(self):
        """Test parsing a single valid barcode."""
        result = validate_and_parse_barcodes("TEST123")
        assert result == ["TEST123"]

    def test_valid_multiple_barcodes(self):
        """Test parsing multiple valid barcodes."""
        result = validate_and_parse_barcodes("TEST123,TEST456,TEST789")
        assert result == ["TEST123", "TEST456", "TEST789"]

    def test_barcodes_with_whitespace(self):
        """Test parsing barcodes with extra whitespace."""
        result = validate_and_parse_barcodes("  TEST123  ,  TEST456  ,  TEST789  ")
        assert result == ["TEST123", "TEST456", "TEST789"]

    def test_barcodes_with_dashes_and_underscores(self):
        """Test parsing barcodes with allowed special characters."""
        result = validate_and_parse_barcodes("TEST-123,TEST_456,TEST-789_ABC")
        assert result == ["TEST-123", "TEST_456", "TEST-789_ABC"]

    def test_empty_string(self):
        """Test that empty string raises ValueError."""
        with pytest.raises(ValueError, match="Barcodes string cannot be empty"):
            validate_and_parse_barcodes("")

    def test_whitespace_only(self):
        """Test that whitespace-only string raises ValueError."""
        with pytest.raises(ValueError, match="Barcodes string cannot be empty"):
            validate_and_parse_barcodes("   ")

    def test_no_valid_barcodes(self):
        """Test that string with no valid barcodes raises ValueError."""
        with pytest.raises(ValueError, match="No valid barcodes found"):
            validate_and_parse_barcodes(",,, ,")

    def test_barcode_too_short(self, invalid_barcodes):
        """Test that barcodes too short raise ValueError."""
        with pytest.raises(ValueError, match="invalid length"):
            validate_and_parse_barcodes("ab")

    def test_barcode_too_long(self):
        """Test that barcodes too long raise ValueError."""
        long_barcode = "a" * 51
        with pytest.raises(ValueError, match="invalid length"):
            validate_and_parse_barcodes(long_barcode)

    def test_barcode_invalid_characters(self):
        """Test that barcodes with invalid characters raise ValueError."""
        with pytest.raises(ValueError, match="contains invalid characters"):
            validate_and_parse_barcodes("test@book")

    def test_barcode_with_space(self):
        """Test that barcodes with spaces raise ValueError."""
        with pytest.raises(ValueError, match="contains invalid characters"):
            validate_and_parse_barcodes("test book")

    def test_mixed_valid_invalid(self):
        """Test that mixed valid/invalid barcodes raise ValueError."""
        with pytest.raises(ValueError, match="contains invalid characters"):
            validate_and_parse_barcodes("TEST123,invalid@barcode,TEST456")


class TestSyncStats:
    """Test sync statistics creation."""

    def test_create_sync_stats(self):
        """Test creating initialized sync statistics."""
        stats = create_sync_stats()
        assert stats["processed"] == 0
        assert stats["completed"] == 0
        assert stats["failed"] == 0
        assert stats["skipped"] == 0
        assert stats["uploaded"] == 0
        assert stats["total_bytes"] == 0

    def test_sync_stats_modification(self):
        """Test that sync statistics can be modified."""
        stats = create_sync_stats()
        stats["processed"] = 5
        stats["completed"] = 3
        stats["failed"] = 1
        stats["skipped"] = 1
        assert stats["processed"] == 5
        assert stats["completed"] == 3
        assert stats["failed"] == 1
        assert stats["skipped"] == 1


class TestBookSyncResult:
    """Test book sync result creation."""

    def test_create_book_sync_result_defaults(self):
        """Test creating book sync result with defaults."""
        result = create_book_sync_result("TEST123")
        assert result["barcode"] == "TEST123"
        assert result["status"] == "completed"
        assert result["skipped"] is False
        assert result["google_etag"] is None
        assert result["file_size"] == 0
        assert result["total_time"] == 0.0

    def test_create_book_sync_result_custom(self):
        """Test creating book sync result with custom values."""
        result = create_book_sync_result(
            barcode="TEST456",
            status="failed",
            skipped=True,
            google_etag="abc123",
            file_size=1024,
            total_time=5.5,
        )
        assert result["barcode"] == "TEST456"
        assert result["status"] == "failed"
        assert result["skipped"] is True
        assert result["google_etag"] == "abc123"
        assert result["file_size"] == 1024
        assert result["total_time"] == 5.5
