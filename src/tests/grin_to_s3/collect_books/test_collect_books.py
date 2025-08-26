#!/usr/bin/env python3
"""
Unit tests for book collection functionality
"""

import asyncio
import csv
import os
import shutil
import sys
import tempfile
from pathlib import Path

import pytest

sys.path.append(os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))

from grin_to_s3.collect_books.collector import BookCollector, RateLimiter
from grin_to_s3.collect_books.config import ExportConfig
from grin_to_s3.collect_books.models import BookRecord, SQLiteProgressTracker
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

    def test_marc_fields_in_csv_headers(self):
        """Test that MARC fields are included in CSV headers."""
        headers = BookRecord.csv_headers()

        # Check that all expected MARC headers are present
        expected_marc_headers = [
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

        for expected_header in expected_marc_headers:
            assert expected_header in headers, f"Missing MARC header: {expected_header}"

    def test_marc_fields_in_csv_row(self):
        """Test that MARC fields are properly included in CSV row output."""
        record = BookRecord(
            barcode="MARC123",
            title="Test Book",
            marc_control_number="123456789",
            marc_title="MARC Title Test",
            marc_subjects="Science, Technology",
            marc_extraction_timestamp="2025-07-10T12:00:00Z",
        )

        headers = BookRecord.csv_headers()
        row = record.to_csv_row()

        # Create header-to-value mapping
        data_dict = dict(zip(headers, row, strict=False))

        # Check specific MARC values
        assert data_dict["MARC Control Number"] == "123456789"
        assert data_dict["MARC Title"] == "MARC Title Test"
        assert data_dict["MARC Subjects"] == "Science, Technology"
        assert data_dict["MARC Extraction Timestamp"] == "2025-07-10T12:00:00Z"

        # Check that None values are converted to empty strings
        assert data_dict["MARC Date Type"] == ""
        assert data_dict["MARC Author Personal"] == ""

    def test_get_marc_fields(self):
        """Test that get_marc_fields returns all MARC field names."""
        marc_fields = BookRecord.get_marc_fields()

        expected_fields = [
            "marc_control_number",
            "marc_date_type",
            "marc_date_1",
            "marc_date_2",
            "marc_language",
            "marc_lccn",
            "marc_lc_call_number",
            "marc_isbn",
            "marc_oclc_numbers",
            "marc_title",
            "marc_title_remainder",
            "marc_author_personal",
            "marc_author_corporate",
            "marc_author_meeting",
            "marc_subjects",
            "marc_genres",
            "marc_general_note",
            "marc_extraction_timestamp",
        ]

        assert len(marc_fields) == 18, f"Expected 18 MARC fields, got {len(marc_fields)}"
        for expected_field in expected_fields:
            assert expected_field in marc_fields, f"Missing MARC field: {expected_field}"

    def test_build_update_marc_sql(self):
        """Test that MARC update SQL is properly generated."""
        sql = BookRecord.build_update_marc_sql()

        # Check that SQL contains all MARC fields
        marc_fields = BookRecord.get_marc_fields()
        for field in marc_fields:
            assert field in sql, f"MARC field {field} not found in SQL"

        # Check SQL structure
        assert "UPDATE books SET" in sql
        assert "WHERE barcode = ?" in sql
        assert "updated_at = ?" in sql

        # Count placeholders (should be 18 MARC fields + 1 updated_at + 1 barcode)
        placeholder_count = sql.count("?")
        assert placeholder_count == 20, f"Expected 20 placeholders, got {placeholder_count}"

    def test_marc_field_types(self):
        """Test that all MARC fields accept string or None values."""
        # Test with all None values
        record = BookRecord(barcode="TEST")
        assert record.marc_control_number is None
        assert record.marc_title is None
        assert record.marc_extraction_timestamp is None

        # Test with string values
        record_with_data = BookRecord(
            barcode="TEST2",
            marc_control_number="123",
            marc_title="Test Title",
            marc_extraction_timestamp="2025-07-10T12:00:00Z",
        )
        assert record_with_data.marc_control_number == "123"
        assert record_with_data.marc_title == "Test Title"
        assert record_with_data.marc_extraction_timestamp == "2025-07-10T12:00:00Z"

    def test_marc_field_defaults(self):
        """Test that MARC fields have proper default values."""
        record = BookRecord(barcode="DEFAULT_TEST")

        marc_fields = BookRecord.get_marc_fields()
        for field in marc_fields:
            value = getattr(record, field)
            assert value is None, f"MARC field {field} should default to None, got {value}"


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


class TestSQLiteProgressTracker:
    """Test SQLiteProgressTracker functionality."""

    @pytest.mark.asyncio
    async def test_load_known_barcodes_batch_empty_set(self):
        """Test load_known_barcodes_batch with empty set."""
        with tempfile.NamedTemporaryFile(suffix=".db", delete=False) as tmp_db:
            try:
                tracker = SQLiteProgressTracker(tmp_db.name)
                result = await tracker.load_known_barcodes_batch(set())
                assert result == set()
                await tracker.close()
            finally:
                os.unlink(tmp_db.name)

    @pytest.mark.asyncio
    async def test_load_known_barcodes_batch_all_unknown(self):
        """Test load_known_barcodes_batch with all unknown barcodes."""
        with tempfile.NamedTemporaryFile(suffix=".db", delete=False) as tmp_db:
            try:
                tracker = SQLiteProgressTracker(tmp_db.name)
                barcodes = {"UNKNOWN1", "UNKNOWN2", "UNKNOWN3"}
                result = await tracker.load_known_barcodes_batch(barcodes)
                assert result == set()
                await tracker.close()
            finally:
                os.unlink(tmp_db.name)

    @pytest.mark.asyncio
    async def test_load_known_barcodes_batch_mixed_known_unknown(self):
        """Test load_known_barcodes_batch with mix of known and unknown barcodes."""
        with tempfile.NamedTemporaryFile(suffix=".db", delete=False) as tmp_db:
            try:
                tracker = SQLiteProgressTracker(tmp_db.name)

                # Mark some barcodes as processed and failed
                await tracker.mark_processed("PROCESSED1")
                await tracker.mark_processed("PROCESSED2")
                await tracker.mark_failed("FAILED1", "Test error")

                # Test with mix of known and unknown barcodes
                barcodes = {"PROCESSED1", "PROCESSED2", "FAILED1", "UNKNOWN1", "UNKNOWN2"}
                result = await tracker.load_known_barcodes_batch(barcodes)

                expected = {"PROCESSED1", "PROCESSED2", "FAILED1"}
                assert result == expected
                await tracker.close()
            finally:
                os.unlink(tmp_db.name)

    @pytest.mark.asyncio
    async def test_load_known_barcodes_batch_cache_behavior(self):
        """Test that load_known_barcodes_batch uses and updates caches correctly."""
        with tempfile.NamedTemporaryFile(suffix=".db", delete=False) as tmp_db:
            try:
                tracker = SQLiteProgressTracker(tmp_db.name)

                # Mark one barcode as processed
                await tracker.mark_processed("PROCESSED1")

                # First call should query database and populate cache
                barcodes = {"PROCESSED1", "UNKNOWN1"}
                result1 = await tracker.load_known_barcodes_batch(barcodes)
                assert result1 == {"PROCESSED1"}

                # Check that caches are populated
                assert "PROCESSED1" in tracker._known_cache
                assert "UNKNOWN1" in tracker._unknown_cache

                # Second call with same barcodes should use cache (no DB query)
                result2 = await tracker.load_known_barcodes_batch(barcodes)
                assert result2 == {"PROCESSED1"}

                # Add a new processed barcode
                await tracker.mark_processed("PROCESSED2")

                # Call with mix of cached and new barcodes
                barcodes = {"PROCESSED1", "PROCESSED2", "UNKNOWN1", "UNKNOWN2"}
                result3 = await tracker.load_known_barcodes_batch(barcodes)
                assert result3 == {"PROCESSED1", "PROCESSED2"}

                await tracker.close()
            finally:
                os.unlink(tmp_db.name)

    @pytest.mark.asyncio
    async def test_load_known_barcodes_batch_large_batch(self):
        """Test load_known_barcodes_batch with large batch size."""
        with tempfile.NamedTemporaryFile(suffix=".db", delete=False) as tmp_db:
            try:
                tracker = SQLiteProgressTracker(tmp_db.name)

                # Create a large number of processed barcodes
                for i in range(100):
                    await tracker.mark_processed(f"PROCESSED{i:03d}")

                # Create test batch with some known and some unknown
                test_barcodes = set()
                for i in range(150):
                    test_barcodes.add(f"PROCESSED{i:03d}")  # First 100 exist, next 50 don't

                result = await tracker.load_known_barcodes_batch(test_barcodes)

                # Should return the first 100 that exist
                expected = {f"PROCESSED{i:03d}" for i in range(100)}
                assert result == expected
                assert len(result) == 100

                await tracker.close()
            finally:
                os.unlink(tmp_db.name)


class TestBookCollector:
    """Test BookCollector functionality with mocking."""

    def setup_method(self):
        """Set up test fixtures."""
        # Use temporary directory for test progress files

        self.temp_dir = tempfile.mkdtemp()
        self.exporter = setup_mock_exporter(self.temp_dir)

    def teardown_method(self):
        """Clean up test fixtures."""

        # Remove temporary directory
        shutil.rmtree(self.temp_dir, ignore_errors=True)

    def test_process_grin_row(self):
        """Test GRINRow dict processing."""
        grin_row = {
            "barcode": "TEST123",
            "title": "Test Title",
            "scanned_date": "2024/01/01 10:00",
            "processed_date": "2024/01/02 11:00",
            "analyzed_date": "2024/01/03 12:00",
            "google_books_link": "https://books.google.com/books?id=test",
        }

        parsed = self.exporter.process_grin_row(grin_row)

        assert parsed["barcode"] == "TEST123"
        assert parsed["title"] == "Test Title"
        assert parsed["scanned_date"] == "2024-01-01T10:00:00"
        assert parsed["processed_date"] == "2024-01-02T11:00:00"
        assert parsed["analyzed_date"] == "2024-01-03T12:00:00"
        assert parsed["google_books_link"] == "https://books.google.com/books?id=test"

    def test_process_grin_row_with_missing_fields(self):
        """Test GRINRow processing with missing fields."""
        grin_row = {"barcode": "TEST456", "processed_date": "2024/01/02 11:00"}

        parsed = self.exporter.process_grin_row(grin_row)

        assert parsed["barcode"] == "TEST456"
        assert parsed["title"] == ""
        assert parsed["scanned_date"] is None
        assert parsed["processed_date"] == "2024-01-02T11:00:00"
        assert parsed["analyzed_date"] is None

    def test_process_grin_row_invalid_format(self):
        """Test GRINRow processing with invalid format."""
        grin_row = {}  # Empty dict

        parsed = self.exporter.process_grin_row(grin_row)

        assert parsed == {}

    def test_process_grin_row_minimal_valid(self):
        """Test GRINRow processing with minimal valid data."""
        grin_row = {"barcode": "MINIMAL"}  # Just barcode

        parsed = self.exporter.process_grin_row(grin_row)

        assert parsed["barcode"] == "MINIMAL"
        assert parsed["scanned_date"] is None
        assert parsed["google_books_link"] == ""

    def test_process_grin_row_with_all_fields(self):
        """Test GRINRow processing with all fields from HTML tables."""
        grin_row = {
            "barcode": "RSMD7D",
            "title": "Those Preston Twins",
            "status": "AVAILABLE",
            "scanned_date": "2008/04/17",
            "analyzed_date": "2024/11/18",
            "converted_date": "2025/01/17",
            "downloaded_date": "2023/09/18",
            "processed_date": "2024/01/01",
            "ocr_date": "2024/02/01",
        }

        parsed = self.exporter.process_grin_row(grin_row)

        assert parsed["barcode"] == "RSMD7D"
        assert parsed["title"] == "Those Preston Twins"
        assert parsed["grin_state"] == "AVAILABLE"
        assert parsed["scanned_date"] == "2008/04/17"
        assert parsed["analyzed_date"] == "2024/11/18"
        assert parsed["converted_date"] == "2025/01/17"
        assert parsed["downloaded_date"] == "2023/09/18"
        assert parsed["processed_date"] == "2024/01/01"
        assert parsed["ocr_date"] == "2024/02/01"

    def test_process_grin_row_with_date_conversion(self):
        """Test GRINRow processing with proper date conversion."""
        grin_row = {
            "barcode": "TEST789",
            "title": "Example Book Title",
            "status": "AVAILABLE",
            "scanned_date": "2024/06/15 14:30",
            "analyzed_date": "2024/06/16 09:15",
            "converted_date": "2024/06/17 16:45",
            "downloaded_date": "2024/06/18 11:20",
        }

        parsed = self.exporter.process_grin_row(grin_row)

        assert parsed["barcode"] == "TEST789"
        assert parsed["title"] == "Example Book Title"
        assert parsed["grin_state"] == "AVAILABLE"
        assert parsed["scanned_date"] == "2024-06-15T14:30:00"
        assert parsed["analyzed_date"] == "2024-06-16T09:15:00"
        assert parsed["converted_date"] == "2024-06-17T16:45:00"
        assert parsed["downloaded_date"] == "2024-06-18T11:20:00"

    def test_process_grin_row_missing_date_fields(self):
        """Test GRINRow processing with missing date fields."""
        grin_row = {
            "barcode": "INCOMPLETE",
            "title": "Incomplete Book",
            "status": "AVAILABLE",
            "analyzed_date": "2024/05/01 10:00",
            "downloaded_date": "2024/05/03 15:30",
        }

        parsed = self.exporter.process_grin_row(grin_row)

        assert parsed["barcode"] == "INCOMPLETE"
        assert parsed["title"] == "Incomplete Book"
        assert parsed["grin_state"] == "AVAILABLE"
        assert parsed["scanned_date"] is None  # Not in dict
        assert parsed["analyzed_date"] == "2024-05-01T10:00:00"
        assert parsed["converted_date"] is None  # Not in dict
        assert parsed["downloaded_date"] == "2024-05-03T15:30:00"

    def test_process_grin_row_different_field_formats(self):
        """Test GRINRow processing with different field formats."""
        # Test title in named field vs column position
        named_field_row = {"barcode": "CONV001", "title": "Converted Book Title", "scanned_date": "2024/01/01 10:00"}
        named_parsed = self.exporter.process_grin_row(named_field_row)

        # Test title in different named field
        alt_title_row = {
            "barcode": "ALL001",
            "book_title": "All Books Title",  # Title with different name containing "title"
            "status": "AVAILABLE",
            "scanned_date": "2024/01/01 10:00",
        }
        alt_parsed = self.exporter.process_grin_row(alt_title_row)

        # Named field parsing
        assert named_parsed["barcode"] == "CONV001"
        assert named_parsed["title"] == "Converted Book Title"
        assert named_parsed["scanned_date"] == "2024-01-01T10:00:00"

        # Alternative title field parsing
        assert alt_parsed["barcode"] == "ALL001"
        assert alt_parsed["title"] == "All Books Title"
        assert alt_parsed["grin_state"] == "AVAILABLE"

    def test_process_grin_row_edge_cases(self):
        """Test GRINRow processing edge cases."""
        # Minimal data
        short_row = {"barcode": "SHORT", "title": "Title Only", "status": "AVAILABLE"}
        parsed_short = self.exporter.process_grin_row(short_row)

        assert parsed_short["barcode"] == "SHORT"
        assert parsed_short["title"] == "Title Only"
        assert parsed_short["grin_state"] == "AVAILABLE"
        assert parsed_short["scanned_date"] is None
        assert parsed_short["converted_date"] is None

        # Numeric title
        numeric_row = {"barcode": "NUM123", "title": "12345 Main Street Guide", "status": "AVAILABLE"}
        parsed_numeric = self.exporter.process_grin_row(numeric_row)

        assert parsed_numeric["barcode"] == "NUM123"
        assert parsed_numeric["title"] == "12345 Main Street Guide"
        assert parsed_numeric["grin_state"] == "AVAILABLE"

    @pytest.mark.asyncio
    async def test_progress_save_and_load(self, mock_process_stage):
        """Test progress tracking functionality."""
        with tempfile.TemporaryDirectory() as temp_dir:
            progress_file = Path(temp_dir) / "test_progress.json"
            test_db_path = Path(temp_dir) / "test_books.db"

            # Create config with test database path

            config = ExportConfig(
                library_directory="TestLibrary", resume_file=str(progress_file), sqlite_db_path=str(test_db_path)
            )

            exporter = BookCollector(directory="TestLibrary", process_summary_stage=mock_process_stage, storage_config={"type": "local", "config": {"base_path": str(temp_dir)}, "prefix": "test"}, config=config)

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
            exporter2 = BookCollector(directory="TestLibrary", process_summary_stage=mock_process_stage, storage_config={"type": "local", "config": {"base_path": str(temp_dir)}, "prefix": "test"}, config=config2)
            await exporter2.load_progress()

            # Check that progress was loaded via SQLite
            assert await exporter2.sqlite_tracker.is_processed("TEST001")
            assert await exporter2.sqlite_tracker.is_processed("TEST002")
            assert await exporter2.sqlite_tracker.is_failed("FAILED001")

    @pytest.mark.asyncio
    async def test_process_book(self):
        """Test individual book processing."""
        grin_row = {
            "barcode": "PROC001",
            "title": "Process Book Title",
            "scanned_date": "2024/01/01 10:00",
            "processed_date": "2024/01/02 11:00",
            "google_books_link": "https://books.google.com/books?id=test",
        }

        record = await self.exporter.process_book(grin_row)

        assert record is not None
        assert record.barcode == "PROC001"
        assert record.title == "Process Book Title"
        # processing state is now tracked in status history, not in the record
        assert record.scanned_date == "2024-01-01T10:00:00"

        # Ensure all background tasks complete before test ends
        if hasattr(self.exporter, "_background_tasks") and self.exporter._background_tasks:
            await asyncio.gather(*self.exporter._background_tasks, return_exceptions=True)

    @pytest.mark.asyncio
    async def test_process_book_invalid_row(self):
        """Test processing invalid GRINRow."""
        grin_row = {}  # Empty dict

        record = await self.exporter.process_book(grin_row)

        assert record is None

    @pytest.mark.asyncio
    async def test_process_book_already_processed(self):
        """Test skipping already processed book."""
        grin_row = {"barcode": "SKIP001", "scanned_date": "2024/01/01 10:00"}

        # Mark as already processed in SQLite
        await self.exporter.sqlite_tracker.mark_processed("SKIP001")

        record = await self.exporter.process_book(grin_row)

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

    def test_book_collector_with_storage_initialization(self, mock_process_stage):
        """Test BookCollector initialization with storage configuration."""
        with tempfile.TemporaryDirectory() as temp_dir:
            storage_config = {
                "type": "local",
                "config": {
                    "base_path": temp_dir,
                    "bucket_raw": "test-raw",
                    "bucket_meta": "test-meta",
                    "bucket_full": "test-full",
                },
                "prefix": "test-prefix",
            }

            # This should not raise an exception
            collector = BookCollector(
                "TestDirectory", process_summary_stage=mock_process_stage, storage_config=storage_config
            )

            # Verify storage was initialized correctly
            assert collector.book_manager is not None
            assert collector.book_manager.bucket_raw == "test-raw"
            assert collector.book_manager.bucket_meta == "test-meta"
            assert collector.book_manager.bucket_full == "test-full"
            assert collector.book_manager.base_prefix == "test-prefix"

    def test_process_grin_row_title_with_special_chars(self):
        """Test GRINRow processing with special characters in title."""
        with tempfile.TemporaryDirectory() as temp_dir:
            exporter = setup_mock_exporter(temp_dir)
            grin_row = {
                "barcode": "TITLE001",
                "title": "Converted Book Title With Special Chars & Symbols",
                "scanned_date": "2024/01/01 10:00",
                "processed_date": "2024/01/02 11:00",
            }

            parsed = exporter.process_grin_row(grin_row)

            assert parsed["barcode"] == "TITLE001"
            assert parsed["title"] == "Converted Book Title With Special Chars & Symbols"
            assert parsed["scanned_date"] == "2024-01-01T10:00:00"

    def test_process_grin_row_comprehensive_guide_title(self):
        """Test GRINRow processing with comprehensive title and status."""
        with tempfile.TemporaryDirectory() as temp_dir:
            exporter = setup_mock_exporter(temp_dir)
            grin_row = {
                "barcode": "TITLE002",
                "title": "All Books Title: A Comprehensive Guide",
                "status": "AVAILABLE",
                "scanned_date": "2024/01/01 10:00",
                "processed_date": "2024/01/02 11:00",
            }

            parsed = exporter.process_grin_row(grin_row)

            assert parsed["barcode"] == "TITLE002"
            assert parsed["title"] == "All Books Title: A Comprehensive Guide"
            assert parsed["grin_state"] == "AVAILABLE"
            assert parsed["scanned_date"] == "2024-01-01T10:00:00"

    def test_process_grin_row_empty_title_variations(self):
        """Test handling of empty titles in GRINRow data."""
        with tempfile.TemporaryDirectory() as temp_dir:
            exporter = setup_mock_exporter(temp_dir)

            # Row with no title field
            empty_row = {
                "barcode": "EMPTY001",
                "scanned_date": "2024/01/01 10:00",
                "processed_date": "2024/01/02 11:00",
            }
            parsed1 = exporter.process_grin_row(empty_row)

            assert parsed1["barcode"] == "EMPTY001"
            assert parsed1["title"] == ""

            # Row with explicit empty title
            explicit_empty_row = {
                "barcode": "EMPTY002",
                "title": "",
                "status": "AVAILABLE",
                "scanned_date": "2024/01/01 10:00",
            }
            parsed2 = exporter.process_grin_row(explicit_empty_row)

            assert parsed2["barcode"] == "EMPTY002"
            assert parsed2["title"] == ""
            assert parsed2["grin_state"] == "AVAILABLE"


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
