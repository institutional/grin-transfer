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
from grin_to_s3.collect_books.models import BookRecord, SQLiteProgressTracker
from tests.mocks import get_test_data, setup_mock_exporter
from tests.test_utils.database_helpers import get_book_for_testing


class TestBookRecord:
    """Test BookRecord data class functionality."""

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

    def test_boolean_fields_in_csv_row(self):
        """Test that boolean fields output TRUE/FALSE in CSV format."""
        # Test with False value (simulating SQLite 0)
        record_false = BookRecord(barcode="TEST_BOOL_FALSE")
        record_false.is_decrypted = 0  # Simulating SQLite integer storage
        row_false = record_false.to_csv_row()
        headers = BookRecord.csv_headers()
        is_decrypted_pos = headers.index("Is Decrypted")
        assert row_false[is_decrypted_pos] == "FALSE"

        # Test with True value (simulating SQLite 1)
        record_true = BookRecord(barcode="TEST_BOOL_TRUE")
        record_true.is_decrypted = 1  # Simulating SQLite integer storage
        row_true = record_true.to_csv_row()
        assert row_true[is_decrypted_pos] == "TRUE"

        # Test with Python boolean True
        record_bool_true = BookRecord(barcode="TEST_BOOL_PY_TRUE")
        record_bool_true.is_decrypted = True
        row_bool_true = record_bool_true.to_csv_row()
        assert row_bool_true[is_decrypted_pos] == "TRUE"

        # Test with Python boolean False
        record_bool_false = BookRecord(barcode="TEST_BOOL_PY_FALSE")
        record_bool_false.is_decrypted = False
        row_bool_false = record_bool_false.to_csv_row()
        assert row_bool_false[is_decrypted_pos] == "FALSE"

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

    @pytest.mark.asyncio
    async def test_save_book_initial_insert(self):
        """Test save_book with initial book insertion."""
        with tempfile.NamedTemporaryFile(suffix=".db", delete=False) as tmp_db:
            try:
                tracker = SQLiteProgressTracker(tmp_db.name)
                await tracker.init_db()

                # Create a test book record
                book = BookRecord(
                    barcode="TEST001", title="Test Book", scanned_date="2024-01-01T10:00:00", converted_date=None
                )

                # Save book in normal mode
                await tracker.save_book(book, refresh_mode=False)

                # Retrieve and verify the book was saved
                saved_book = await get_book_for_testing(tracker, "TEST001")
                assert saved_book is not None
                assert saved_book.barcode == "TEST001"
                assert saved_book.title == "Test Book"
                assert saved_book.scanned_date == "2024-01-01T10:00:00"
                assert saved_book.converted_date is None
                assert saved_book.created_at is not None
                assert saved_book.updated_at is not None

                await tracker.close()
            finally:
                os.unlink(tmp_db.name)

    @pytest.mark.asyncio
    async def test_save_book_normal_mode_ignores_duplicate(self):
        """Test save_book in normal mode ignores duplicate records (DO NOTHING)."""
        with tempfile.NamedTemporaryFile(suffix=".db", delete=False) as tmp_db:
            try:
                tracker = SQLiteProgressTracker(tmp_db.name)
                await tracker.init_db()

                # Create initial book record
                original_book = BookRecord(
                    barcode="TEST001", title="Original Title", scanned_date="2024-01-01T10:00:00", converted_date=None
                )
                await tracker.save_book(original_book, refresh_mode=False)

                # Try to save a duplicate with different data in normal mode
                duplicate_book = BookRecord(
                    barcode="TEST001",
                    title="Updated Title - Should Be Ignored",
                    scanned_date="2024-02-01T10:00:00",
                    converted_date="2024-02-01T11:00:00",
                )
                await tracker.save_book(duplicate_book, refresh_mode=False)

                # Verify original data is preserved
                saved_book = await get_book_for_testing(tracker, "TEST001")
                assert saved_book is not None
                assert saved_book.title == "Original Title"
                assert saved_book.scanned_date == "2024-01-01T10:00:00"
                assert saved_book.converted_date is None

                await tracker.close()
            finally:
                os.unlink(tmp_db.name)

    @pytest.mark.asyncio
    async def test_save_book_refresh_mode_updates_with_coalesce(self):
        """Test save_book in refresh mode updates with COALESCE preservation."""
        with tempfile.NamedTemporaryFile(suffix=".db", delete=False) as tmp_db:
            try:
                tracker = SQLiteProgressTracker(tmp_db.name)
                await tracker.init_db()

                # Create initial book record
                original_book = BookRecord(
                    barcode="TEST001", title="Original Title", scanned_date="2024-01-01T10:00:00", converted_date=None
                )
                await tracker.save_book(original_book, refresh_mode=False)

                # Update with new values in refresh mode (COALESCE only handles NULL, not empty strings)
                update_book = BookRecord(
                    barcode="TEST001",
                    title="Updated Title",  # This will overwrite the original title
                    scanned_date=None,  # Should be preserved via COALESCE
                    converted_date="2024-02-01T11:00:00",  # Should be updated
                    processed_date="2024-02-01T12:00:00",  # Should be added
                )
                await tracker.save_book(update_book, refresh_mode=True)

                # Verify COALESCE behavior
                saved_book = await get_book_for_testing(tracker, "TEST001")
                assert saved_book is not None
                assert saved_book.title == "Updated Title"  # Updated (COALESCE doesn't preserve non-NULL values)
                assert saved_book.scanned_date == "2024-01-01T10:00:00"  # Preserved (NULL was passed)
                assert saved_book.converted_date == "2024-02-01T11:00:00"  # Updated
                assert saved_book.processed_date == "2024-02-01T12:00:00"  # Added

                await tracker.close()
            finally:
                os.unlink(tmp_db.name)

    @pytest.mark.asyncio
    async def test_save_book_refresh_mode_inserts_new_record(self):
        """Test save_book in refresh mode can still insert new records."""
        with tempfile.NamedTemporaryFile(suffix=".db", delete=False) as tmp_db:
            try:
                tracker = SQLiteProgressTracker(tmp_db.name)
                await tracker.init_db()

                # Create new book record in refresh mode
                new_book = BookRecord(barcode="TEST001", title="New Book", scanned_date="2024-01-01T10:00:00")
                await tracker.save_book(new_book, refresh_mode=True)

                # Verify the book was inserted
                saved_book = await get_book_for_testing(tracker, "TEST001")
                assert saved_book is not None
                assert saved_book.title == "New Book"
                assert saved_book.scanned_date == "2024-01-01T10:00:00"

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

    @pytest.mark.asyncio
    async def test_process_book_invalid_row(self):
        """Test processing invalid GRINRow."""
        grin_row = {}  # Empty dict

        record = await self.exporter.process_book(grin_row)

        assert record is None

    @pytest.mark.asyncio
    async def test_process_book_already_processed(self):
        """Test processing already processed book with UPSERT (normal mode uses DO NOTHING)."""
        grin_row = {"barcode": "SKIP001", "scanned_date": "2024/01/01 10:00"}

        # Mark as already processed in SQLite
        await self.exporter.sqlite_tracker.mark_processed("SKIP001")

        record = await self.exporter.process_book(grin_row)

        # Normal mode uses ON CONFLICT DO NOTHING, so existing records are preserved
        assert record is not None
        assert record.barcode == "SKIP001"


class TestBookCollectionIntegration:
    """Integration tests for full book collection workflow."""

    @pytest.mark.asyncio
    async def test_collect_books_with_mocked_data(self):
        """Test full book collection with mocked GRIN data."""
        with tempfile.TemporaryDirectory() as temp_dir:
            test_data = get_test_data()[:3]  # Only first 3 records
            exporter = setup_mock_exporter(temp_dir, test_data)

            # Run export with limit
            await exporter.collect_books(limit=3)

            # Verify CSV file was uploaded to storage (local storage in temp_dir)
            # The MockStorage.write_file places files at: {temp_dir}/{storage_path}
            # where storage_path is from book_manager.meta_path() = "test_run/books_latest.csv"
            storage_csv_file = Path(temp_dir) / "test_run" / "books_latest.csv"
            assert storage_csv_file.exists()

            with open(storage_csv_file, newline="") as f:
                reader = csv.reader(f)
                headers = next(reader)
                rows = list(reader)

            assert headers == BookRecord.csv_headers()
            assert len(rows) == 3

            # Check that all test records are present
            barcodes = {row[0].strip('"') for row in rows}
            expected_barcodes = {"TEST001", "TEST002", "TEST003"}
            assert barcodes == expected_barcodes

    def test_book_collector_with_storage_initialization(self, mock_process_stage):
        """Test BookCollector initialization with storage configuration."""
        with tempfile.TemporaryDirectory() as temp_dir:
            storage_config = {
                "type": "local",
                "protocol": "file",
                "config": {
                    "base_path": temp_dir,
                    "bucket_raw": "test-raw",
                    "bucket_meta": "test-meta",
                    "bucket_full": "test-full",
                },
                "prefix": "test-prefix",
            }

            # Create a minimal RunConfig for testing
            from pathlib import Path

            from grin_to_s3.run_config import RunConfig, SyncConfig

            sync_config: SyncConfig = {
                "task_check_concurrency": 1,
                "task_download_concurrency": 1,
                "task_decrypt_concurrency": 1,
                "task_upload_concurrency": 1,
                "task_unpack_concurrency": 1,
                "task_extract_marc_concurrency": 1,
                "task_extract_ocr_concurrency": 1,
                "task_export_csv_concurrency": 1,
                "task_cleanup_concurrency": 1,
                "staging_dir": Path("/tmp/staging"),
                "disk_space_threshold": 0.8,
                "compression_meta_enabled": True,
                "compression_full_enabled": True,
            }

            run_config = RunConfig(
                run_name="test_run",
                library_directory="TestDirectory",
                output_directory=Path("/tmp/output"),
                sqlite_db_path=Path("/tmp/test.db"),
                storage_config=storage_config,
                sync_config=sync_config,
                log_file=Path("/tmp/log.txt"),
                secrets_dir=None,
            )

            # This should not raise an exception
            collector = BookCollector(
                process_summary_stage=mock_process_stage,
                storage_config=storage_config,
                run_config=run_config,
            )

            # Verify storage was initialized correctly
            assert collector.book_manager is not None
            assert collector.book_manager.bucket_raw == "test-raw"
            assert collector.book_manager.bucket_meta == "test-meta"
            assert collector.book_manager.bucket_full == "test-full"
            assert collector.book_manager.base_prefix == "test_run"


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
