#!/usr/bin/env python3
"""
Unit tests for book collection functionality
"""

import asyncio
import csv
import os
import sys
import tempfile
from pathlib import Path

import pytest

sys.path.append(os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))

from grin_to_s3.collect_books.exporter import BookCollector, RateLimiter
from grin_to_s3.collect_books.models import BookRecord
from tests.mocks import get_test_data, setup_mock_exporter


class TestBookRecord:
    """Test BookRecord data class functionality."""

    def test_book_record_creation(self):
        """Test basic BookRecord creation."""
        record = BookRecord(barcode="TEST123", scanned_date="2024-01-01T10:00:00", title="Test Book")

        assert record.barcode == "TEST123"
        assert record.scanned_date == "2024-01-01T10:00:00"
        assert record.title == "Test Book"
        # processing state is now tracked in status history, not in the record itself

    def test_csv_headers(self):
        """Test CSV headers are consistent."""
        headers = BookRecord.csv_headers()

        # Check that essential headers are present
        assert "Barcode" in headers
        assert "Title" in headers
        assert "Processing Request Timestamp" in headers  # New status tracking field
        assert "CSV Exported" in headers
        assert "CSV Updated" in headers

        # Check that headers match the number of fields in to_csv_row
        test_record = BookRecord(barcode="TEST")
        csv_row = test_record.to_csv_row()
        assert len(headers) == len(csv_row)

    def test_to_csv_row(self):
        """Test CSV row conversion."""
        record = BookRecord(
            barcode="TEST456",
            title="Test Book Title",
            scanned_date="2024-01-01T10:00:00",
            processing_request_timestamp="2024-01-02T10:00:00",
        )

        row = record.to_csv_row()

        assert len(row) == len(BookRecord.csv_headers())
        assert row[0] == "TEST456"  # Barcode
        assert row[1] == "Test Book Title"  # Title
        assert row[2] == "2024-01-01T10:00:00"  # Scanned Date
        assert row[9] == "2024-01-02T10:00:00"  # Processing Request Timestamp


class TestRateLimiter:
    """Test rate limiting functionality."""

    @pytest.mark.asyncio
    async def test_rate_limiter_allows_first_request(self):
        """Test that rate limiter allows first request immediately."""
        limiter = RateLimiter(requests_per_second=1.0)

        start_time = asyncio.get_event_loop().time()
        await limiter.acquire()
        elapsed = asyncio.get_event_loop().time() - start_time

        assert elapsed < 0.1  # Should be nearly instantaneous

    @pytest.mark.asyncio
    async def test_rate_limiter_enforces_rate(self):
        """Test that rate limiter enforces rate between requests."""
        limiter = RateLimiter(requests_per_second=10.0)

        # First request should be immediate
        await limiter.acquire()

        # Second request should be delayed
        start_time = asyncio.get_event_loop().time()
        await limiter.acquire()
        elapsed = asyncio.get_event_loop().time() - start_time

        # Should be delayed by approximately 1/10 second
        assert 0.08 < elapsed < 0.15


class TestBookCollector:
    """Test BookCollector functionality with mocking."""

    def setup_method(self):
        """Set up test fixtures."""
        # Use temporary directory for test progress files
        import tempfile

        self.temp_dir = tempfile.mkdtemp()
        self.exporter = setup_mock_exporter(self.temp_dir)

    def teardown_method(self):
        """Clean up test fixtures."""
        import shutil

        # Remove temporary directory
        shutil.rmtree(self.temp_dir, ignore_errors=True)

    def test_parse_grin_line(self):
        """Test GRIN line parsing."""
        grin_line = "TEST123\t2024/01/01 10:00\t2024/01/02 11:00\t2024/01/03 12:00\t2024/01/04 13:00\t2024/01/05 14:00\t\t2024/01/06 15:00\thttps://books.google.com/books?id=test"

        parsed = self.exporter.parse_grin_line(grin_line)

        assert parsed["barcode"] == "TEST123"
        assert parsed["scanned_date"] == "2024-01-01T10:00:00"
        assert parsed["converted_date"] == "2024-01-02T11:00:00"
        assert parsed["google_books_link"] == "https://books.google.com/books?id=test"

    def test_parse_grin_line_with_missing_fields(self):
        """Test GRIN line parsing with missing fields."""
        grin_line = "TEST456\t\t2024/01/02 11:00\t\t\t\t\t\t"

        parsed = self.exporter.parse_grin_line(grin_line)

        assert parsed["barcode"] == "TEST456"
        assert parsed["scanned_date"] is None
        assert parsed["converted_date"] == "2024-01-02T11:00:00"
        assert parsed["downloaded_date"] is None

    def test_parse_grin_line_invalid_format(self):
        """Test GRIN line parsing with invalid format."""
        grin_line = ""  # Empty line

        parsed = self.exporter.parse_grin_line(grin_line)

        assert parsed == {}

    def test_parse_grin_line_minimal_valid(self):
        """Test GRIN line parsing with minimal valid data."""
        grin_line = "MINIMAL"  # Just barcode

        parsed = self.exporter.parse_grin_line(grin_line)

        assert parsed["barcode"] == "MINIMAL"
        assert parsed["scanned_date"] is None
        assert parsed["google_books_link"] == ""

    def test_parse_grin_line_html_format(self):
        """Test GRIN line parsing with HTML table format (production)."""
        # HTML format: BARCODE\tBARCODE\tTITLE\t\tSCANNED_DATE\tANALYZED_DATE\tCONVERTED_DATE\tDOWNLOADED_DATE
        grin_line = "RSMD7D\tRSMD7D\tThose Preston Twins\t\t2008/04/17\t2024/11/18\t2025/01/17\t2023/09/18"

        parsed = self.exporter.parse_grin_line(grin_line)

        assert parsed["barcode"] == "RSMD7D"
        assert parsed["scanned_date"] == "2008/04/17"
        assert parsed["analyzed_date"] == "2024/11/18"
        assert parsed["converted_date"] == "2025/01/17"
        assert parsed["downloaded_date"] == "2023/09/18"
        assert parsed["processed_date"] is None  # Not available in HTML format
        assert parsed["ocr_date"] is None  # Not available in HTML format
        assert parsed["google_books_link"] == ""  # Not available in HTML format

    def test_parse_grin_line_html_format_with_dates(self):
        """Test HTML format parsing with proper date conversion."""
        grin_line = "TEST789\tTEST789\tExample Book Title\t\t2024/06/15 14:30\t2024/06/16 09:15\t2024/06/17 16:45\t2024/06/18 11:20"

        parsed = self.exporter.parse_grin_line(grin_line)

        assert parsed["barcode"] == "TEST789"
        assert parsed["scanned_date"] == "2024-06-15T14:30:00"
        assert parsed["analyzed_date"] == "2024-06-16T09:15:00"
        assert parsed["converted_date"] == "2024-06-17T16:45:00"
        assert parsed["downloaded_date"] == "2024-06-18T11:20:00"

    def test_parse_grin_line_html_format_missing_fields(self):
        """Test HTML format with missing date fields."""
        grin_line = "INCOMPLETE\tINCOMPLETE\tIncomplete Book\t\t\t2024/05/01 10:00\t\t2024/05/03 15:30"

        parsed = self.exporter.parse_grin_line(grin_line)

        assert parsed["barcode"] == "INCOMPLETE"
        assert parsed["scanned_date"] is None  # Empty field[4]
        assert parsed["analyzed_date"] == "2024-05-01T10:00:00"
        assert parsed["converted_date"] is None  # Empty field[6]
        assert parsed["downloaded_date"] == "2024-05-03T15:30:00"

    def test_parse_grin_line_html_vs_text_format_detection(self):
        """Test that format detection correctly distinguishes HTML vs text format."""
        # Text format (legacy) - has date in field[2]
        text_line = "TEXT001\t2024/01/01 10:00\t2024/01/02 11:00\t2024/01/03 12:00"
        text_parsed = self.exporter.parse_grin_line(text_line)

        # HTML format - has title in field[2]
        html_line = "HTML001\tHTML001\tBook Title Here\t\t2024/01/01 10:00\t2024/01/02 11:00"
        html_parsed = self.exporter.parse_grin_line(html_line)

        # Text format should parse field[1] as scanned_date
        assert text_parsed["scanned_date"] == "2024-01-01T10:00:00"
        assert text_parsed["converted_date"] == "2024-01-02T11:00:00"

        # HTML format should parse field[4] as scanned_date
        assert html_parsed["scanned_date"] == "2024-01-01T10:00:00"
        assert html_parsed["analyzed_date"] == "2024-01-02T11:00:00"

    def test_parse_grin_line_html_format_edge_cases(self):
        """Test HTML format edge cases."""
        # Very short HTML line
        short_html = "SHORT\tSHORT\tTitle Only"
        parsed_short = self.exporter.parse_grin_line(short_html)

        assert parsed_short["barcode"] == "SHORT"
        assert parsed_short["scanned_date"] is None
        assert parsed_short["converted_date"] is None

        # HTML line with numeric title (should still be detected as HTML)
        numeric_title = "NUM123\tNUM123\t12345 Main Street Guide"
        parsed_numeric = self.exporter.parse_grin_line(numeric_title)

        assert parsed_numeric["barcode"] == "NUM123"
        # Should be detected as HTML format because title doesn't look like date

    @pytest.mark.asyncio
    async def test_progress_save_and_load(self):
        """Test progress tracking functionality."""
        with tempfile.TemporaryDirectory() as temp_dir:
            progress_file = Path(temp_dir) / "test_progress.json"
            test_db_path = Path(temp_dir) / "test_books.db"

            # Create config with test database path
            from grin_to_s3.collect_books.config import ExportConfig

            config = ExportConfig(
                library_directory="TestLibrary", resume_file=str(progress_file), sqlite_db_path=str(test_db_path)
            )

            exporter = BookCollector(directory="TestLibrary", config=config)

            # Add some processed items via SQLite tracker
            await exporter.sqlite_tracker.mark_processed("TEST001")
            await exporter.sqlite_tracker.mark_processed("TEST002")
            await exporter.sqlite_tracker.mark_failed("FAILED001", "Test error")

            # Save progress
            await exporter.save_progress()

            assert progress_file.exists()

            # Create new exporter and load progress
            config2 = ExportConfig(
                library_directory="TestLibrary", resume_file=str(progress_file), sqlite_db_path=str(test_db_path)
            )
            exporter2 = BookCollector(directory="TestLibrary", config=config2)
            await exporter2.load_progress()

            # Check that progress was loaded via SQLite
            assert await exporter2.sqlite_tracker.is_processed("TEST001")
            assert await exporter2.sqlite_tracker.is_processed("TEST002")
            assert await exporter2.sqlite_tracker.is_failed("FAILED001")

    @pytest.mark.asyncio
    async def test_enrich_book_record(self):
        """Test book record enrichment without storage."""
        record = BookRecord(barcode="TEST123")

        enriched = await self.exporter.enrich_book_record(record)

        # Should add timestamps
        assert enriched.csv_exported
        assert enriched.csv_updated
        assert enriched.csv_exported == enriched.csv_updated

        # Should preserve other fields
        assert enriched.barcode == "TEST123"

    @pytest.mark.asyncio
    async def test_get_processing_states(self):
        """Test processing state retrieval with mocked GRIN client."""
        with tempfile.TemporaryDirectory() as temp_dir:
            # Use shared mock setup
            exporter = setup_mock_exporter(temp_dir, get_test_data())

            states = await exporter.get_processing_states()

            # Check that mock data is returned
            assert "converted" in states
            assert "failed" in states
            assert "TEST001" in states["converted"]
            assert "TEST003" in states["converted"]
            assert "TEST005" in states["failed"]

    @pytest.mark.asyncio
    async def test_process_book(self):
        """Test individual book processing."""
        grin_line = "PROC001\t2024/01/01 10:00\t2024/01/02 11:00\t\t\t\t\t\thttps://books.google.com/books?id=test"
        processing_states = {"converted": {"PROC001"}, "failed": set(), "all_books": {"PROC001"}}

        record = await self.exporter.process_book(grin_line, processing_states)

        assert record is not None
        assert record.barcode == "PROC001"
        # processing state is now tracked in status history, not in the record
        assert record.scanned_date == "2024-01-01T10:00:00"
        assert record.csv_exported  # Should have timestamp

        # Ensure all background tasks complete before test ends
        if hasattr(self.exporter, "_background_tasks") and self.exporter._background_tasks:
            await asyncio.gather(*self.exporter._background_tasks, return_exceptions=True)

    @pytest.mark.asyncio
    async def test_process_book_invalid_line(self):
        """Test processing invalid GRIN line."""
        grin_line = ""  # Empty line
        processing_states = {"converted": set(), "failed": set(), "all_books": set()}

        record = await self.exporter.process_book(grin_line, processing_states)

        assert record is None

    @pytest.mark.asyncio
    async def test_process_book_already_processed(self):
        """Test skipping already processed book."""
        grin_line = "SKIP001\t2024/01/01 10:00\t\t\t\t\t\t\t"
        processing_states = {"converted": set(), "failed": set(), "all_books": set()}

        # Mark as already processed in SQLite
        await self.exporter.sqlite_tracker.mark_processed("SKIP001")

        record = await self.exporter.process_book(grin_line, processing_states)

        assert record is None

        # Ensure all background tasks complete before test ends
        if hasattr(self.exporter, "_background_tasks") and self.exporter._background_tasks:
            await asyncio.gather(*self.exporter._background_tasks, return_exceptions=True)


class TestBookCollectionIntegration:
    """Integration tests for full book collection workflow."""

    @pytest.mark.asyncio
    async def test_collect_books_with_mocked_data(self):
        """Test full book collection with mocked GRIN data."""
        with tempfile.TemporaryDirectory() as temp_dir:
            output_file = Path(temp_dir) / "test_export.csv"

            # Use shared mock setup with limited test data
            test_data = get_test_data()[:3]  # Only first 3 records
            exporter = setup_mock_exporter(temp_dir, test_data)

            # Run export with limit
            await exporter.collect_books(str(output_file), limit=3)

            # Verify CSV file
            assert output_file.exists()

            with open(output_file, newline="") as f:
                reader = csv.reader(f)
                headers = next(reader)
                rows = list(reader)

            assert headers == BookRecord.csv_headers()
            assert len(rows) == 3

            # Check that all test records are present
            barcodes = {row[0].strip('"') for row in rows}
            expected_barcodes = {"TEST001", "TEST002", "TEST003"}
            assert barcodes == expected_barcodes

            # Ensure all background tasks complete before test ends
            if hasattr(exporter, "_background_tasks") and exporter._background_tasks:
                await asyncio.gather(*exporter._background_tasks, return_exceptions=True)

    @pytest.mark.asyncio
    async def test_collect_books_resume_functionality(self):
        """Test book collection resume functionality."""
        with tempfile.TemporaryDirectory() as temp_dir:
            output_file = Path(temp_dir) / "resume_test.csv"

            # Create initial CSV with one record
            with open(output_file, "w", newline="") as f:
                writer = csv.writer(f)
                writer.writerow(BookRecord.csv_headers())
                writer.writerow(
                    [
                        "TEST001",
                        "2023-01-01T12:00:00",
                        "",
                        "",
                        "",
                        "",
                        "",
                        "https://books.google.com/books?id=test001",
                        "converted",
                        "False",
                        "",
                        "False",
                        "",
                        "",
                        "",
                        "2023-01-01T12:00:00",
                        "2023-01-01T12:00:00",
                    ]
                )

            # Use shared mock with 2 records (one existing, one new)
            test_data = get_test_data()[:2]  # TEST001 and TEST002
            exporter = setup_mock_exporter(temp_dir, test_data)

            # Run export
            await exporter.collect_books(str(output_file), limit=2)

            # Verify only new record was added
            with open(output_file, newline="") as f:
                reader = csv.reader(f)
                next(reader)  # Skip headers
                rows = list(reader)

            assert len(rows) == 2  # Original + 1 new
            barcodes = {row[0].strip('"') for row in rows}
            assert "TEST001" in barcodes
            assert "TEST002" in barcodes

            # Ensure all background tasks complete before test ends
            if hasattr(exporter, "_background_tasks") and exporter._background_tasks:
                await asyncio.gather(*exporter._background_tasks, return_exceptions=True)

    def test_book_collector_with_storage_initialization(self):
        """Test BookCollector initialization with storage configuration."""
        with tempfile.TemporaryDirectory() as temp_dir:
            storage_config = {
                "type": "local",
                "config": {"base_path": temp_dir},
                "bucket_raw": "test-raw",
                "bucket_meta": "test-meta",
                "bucket_full": "test-full",
                "prefix": "test-prefix"
            }
            
            # This should not raise an exception
            collector = BookCollector("TestDirectory", storage_config=storage_config)
            
            # Verify storage was initialized correctly
            assert collector.book_storage is not None
            assert collector.book_storage.bucket_raw == "test-raw"
            assert collector.book_storage.bucket_meta == "test-meta"
            assert collector.book_storage.bucket_full == "test-full"
            assert collector.book_storage.base_prefix == "test-prefix"


# Pytest configuration for async tests
@pytest.fixture(scope="session")
def event_loop():
    """Create an instance of the default event loop for the test session."""
    loop = asyncio.get_event_loop_policy().new_event_loop()
    yield loop
    loop.close()


if __name__ == "__main__":
    # Run tests with: python -m pytest tests/unit/test_collect_books.py -v
    pytest.main([__file__, "-v"])
