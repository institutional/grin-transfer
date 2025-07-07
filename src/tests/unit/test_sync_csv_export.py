"""
Unit tests for sync pipeline CSV export utility function.

Tests the export_and_upload_csv function with various scenarios including
success cases, error conditions, and edge cases.
"""

import asyncio
import tempfile
from pathlib import Path
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from grin_to_s3.sync.utils import export_and_upload_csv


class TestExportAndUploadCSV:
    """Test suite for export_and_upload_csv utility function."""

    @pytest.fixture
    def mock_book_storage(self):
        """Create a mock BookStorage instance."""
        mock_storage = MagicMock()
        mock_storage.upload_csv_file = AsyncMock(
            return_value=("meta/books_latest.csv", "meta/timestamped/books_20240101_120000.csv")
        )
        return mock_storage

    @pytest.fixture
    def mock_sqlite_tracker(self):
        """Create a mock SQLiteProgressTracker."""
        mock_tracker = MagicMock()
        mock_tracker.get_all_books_csv_data = AsyncMock()
        return mock_tracker

    @pytest.fixture
    def mock_book_record(self):
        """Create a mock BookRecord class."""
        mock_record = MagicMock()
        mock_record.csv_headers.return_value = ["barcode", "title", "author"]
        mock_record.to_csv_row.return_value = ["TEST001", "Test Book", "Test Author"]
        return mock_record

    @pytest.fixture
    def sample_books(self, mock_book_record):
        """Create sample book data for testing."""
        books = []
        for i in range(3):
            book = MagicMock()
            book.to_csv_row.return_value = [f"TEST{i:03d}", f"Test Book {i}", f"Test Author {i}"]
            books.append(book)
        return books

    @pytest.mark.asyncio
    async def test_successful_export_and_upload(self, mock_book_storage, sample_books):
        """Test successful CSV export and upload workflow."""
        with tempfile.TemporaryDirectory() as temp_dir:
            db_path = str(Path(temp_dir) / "test.db")
            staging_dir = str(Path(temp_dir) / "staging")

            # Mock the dependencies
            with patch("grin_to_s3.collect_books.models.SQLiteProgressTracker") as mock_tracker_cls:
                mock_tracker = AsyncMock()
                mock_tracker.get_all_books_csv_data.return_value = sample_books
                mock_tracker_cls.return_value = mock_tracker

                with patch("grin_to_s3.collect_books.models.BookRecord") as mock_record_cls:
                    mock_record_cls.csv_headers.return_value = ["barcode", "title", "author"]

                    # Call the function
                    result = await export_and_upload_csv(
                        db_path=db_path,
                        staging_dir=staging_dir,
                        book_storage=mock_book_storage,
                        skip_export=False
                    )

                    # Verify result
                    assert result["success"] is True
                    assert result["exported"] is True
                    assert result["uploaded"] is True
                    assert result["latest_path"] == "meta/books_latest.csv"
                    assert result["timestamped_path"] == "meta/timestamped/books_20240101_120000.csv"
                    assert result["error"] is None
                    assert result["temp_file_cleaned"] is True

                    # Verify storage upload was called
                    mock_book_storage.upload_csv_file.assert_called_once()
                    args, kwargs = mock_book_storage.upload_csv_file.call_args
                    assert len(args) == 2
                    assert args[0].endswith(".csv")  # temporary file path
                    assert args[1] is None  # custom filename

                    # Verify staging directory was created
                    assert Path(staging_dir).exists()

    @pytest.mark.asyncio
    async def test_skip_export_flag(self, mock_book_storage):
        """Test that skip_export flag works correctly."""
        with tempfile.TemporaryDirectory() as temp_dir:
            db_path = str(Path(temp_dir) / "test.db")
            staging_dir = str(Path(temp_dir) / "staging")

            result = await export_and_upload_csv(
                db_path=db_path,
                staging_dir=staging_dir,
                book_storage=mock_book_storage,
                skip_export=True
            )

            # Verify result
            assert result["success"] is True
            assert result["exported"] is False
            assert result["uploaded"] is False
            assert result["latest_path"] is None
            assert result["timestamped_path"] is None
            assert result["error"] is None
            assert result["temp_file_cleaned"] is False

            # Verify storage upload was not called
            mock_book_storage.upload_csv_file.assert_not_called()

    @pytest.mark.asyncio
    async def test_custom_filename(self, mock_book_storage, sample_books):
        """Test export with custom filename."""
        with tempfile.TemporaryDirectory() as temp_dir:
            db_path = str(Path(temp_dir) / "test.db")
            staging_dir = str(Path(temp_dir) / "staging")

            with patch("grin_to_s3.collect_books.models.SQLiteProgressTracker") as mock_tracker_cls:
                mock_tracker = AsyncMock()
                mock_tracker.get_all_books_csv_data.return_value = sample_books
                mock_tracker_cls.return_value = mock_tracker

                with patch("grin_to_s3.collect_books.models.BookRecord") as mock_record_cls:
                    mock_record_cls.csv_headers.return_value = ["barcode", "title"]

                    result = await export_and_upload_csv(
                        db_path=db_path,
                        staging_dir=staging_dir,
                        book_storage=mock_book_storage,
                        custom_filename="custom_export.csv"
                    )

                    # Verify custom filename was passed
                    mock_book_storage.upload_csv_file.assert_called_once()
                    args, kwargs = mock_book_storage.upload_csv_file.call_args
                    assert args[1] == "custom_export.csv"

                    assert result["success"] is True

    @pytest.mark.asyncio
    async def test_database_error_handling(self, mock_book_storage):
        """Test error handling when database operations fail."""
        with tempfile.TemporaryDirectory() as temp_dir:
            db_path = str(Path(temp_dir) / "test.db")
            staging_dir = str(Path(temp_dir) / "staging")

            # Mock database error
            with patch("grin_to_s3.collect_books.models.SQLiteProgressTracker") as mock_tracker_cls:
                mock_tracker = AsyncMock()
                mock_tracker.get_all_books_csv_data.side_effect = Exception("Database error")
                mock_tracker_cls.return_value = mock_tracker

                result = await export_and_upload_csv(
                    db_path=db_path,
                    staging_dir=staging_dir,
                    book_storage=mock_book_storage
                )

                # Verify error handling
                assert result["success"] is False
                assert result["exported"] is False
                assert result["uploaded"] is False
                assert result["error"] is not None
                assert "Database error" in result["error"]
                assert result["temp_file_cleaned"] is True  # Temp file created and cleaned up

                # Verify storage upload was not called
                mock_book_storage.upload_csv_file.assert_not_called()

    @pytest.mark.asyncio
    async def test_upload_error_handling(self, mock_book_storage, sample_books):
        """Test error handling when upload operations fail."""
        with tempfile.TemporaryDirectory() as temp_dir:
            db_path = str(Path(temp_dir) / "test.db")
            staging_dir = str(Path(temp_dir) / "staging")

            # Mock upload error
            mock_book_storage.upload_csv_file.side_effect = Exception("Upload failed")

            with patch("grin_to_s3.collect_books.models.SQLiteProgressTracker") as mock_tracker_cls:
                mock_tracker = AsyncMock()
                mock_tracker.get_all_books_csv_data.return_value = sample_books
                mock_tracker_cls.return_value = mock_tracker

                with patch("grin_to_s3.collect_books.models.BookRecord") as mock_record_cls:
                    mock_record_cls.csv_headers.return_value = ["barcode", "title"]

                    result = await export_and_upload_csv(
                        db_path=db_path,
                        staging_dir=staging_dir,
                        book_storage=mock_book_storage
                    )

                    # Verify error handling
                    assert result["success"] is False
                    assert result["exported"] is True  # Export succeeded
                    assert result["uploaded"] is False  # Upload failed
                    assert result["error"] is not None
                    assert "Upload failed" in result["error"]
                    assert result["temp_file_cleaned"] is True  # Cleanup should still work

    @pytest.mark.asyncio
    async def test_empty_database(self, mock_book_storage):
        """Test export with empty database."""
        with tempfile.TemporaryDirectory() as temp_dir:
            db_path = str(Path(temp_dir) / "test.db")
            staging_dir = str(Path(temp_dir) / "staging")

            with patch("grin_to_s3.collect_books.models.SQLiteProgressTracker") as mock_tracker_cls:
                mock_tracker = AsyncMock()
                mock_tracker.get_all_books_csv_data.return_value = []  # Empty list
                mock_tracker_cls.return_value = mock_tracker

                with patch("grin_to_s3.collect_books.models.BookRecord") as mock_record_cls:
                    mock_record_cls.csv_headers.return_value = ["barcode", "title"]

                    result = await export_and_upload_csv(
                        db_path=db_path,
                        staging_dir=staging_dir,
                        book_storage=mock_book_storage
                    )

                    # Verify successful operation with empty data
                    assert result["success"] is True
                    assert result["exported"] is True
                    assert result["uploaded"] is True
                    assert result["temp_file_cleaned"] is True

                    # Verify upload was still called
                    mock_book_storage.upload_csv_file.assert_called_once()

    @pytest.mark.asyncio
    async def test_staging_directory_creation(self, mock_book_storage, sample_books):
        """Test that staging directory is created if it doesn't exist."""
        with tempfile.TemporaryDirectory() as temp_dir:
            db_path = str(Path(temp_dir) / "test.db")
            staging_dir = str(Path(temp_dir) / "nonexistent" / "staging")

            # Verify staging directory doesn't exist initially
            assert not Path(staging_dir).exists()

            with patch("grin_to_s3.collect_books.models.SQLiteProgressTracker") as mock_tracker_cls:
                mock_tracker = AsyncMock()
                mock_tracker.get_all_books_csv_data.return_value = sample_books
                mock_tracker_cls.return_value = mock_tracker

                with patch("grin_to_s3.collect_books.models.BookRecord") as mock_record_cls:
                    mock_record_cls.csv_headers.return_value = ["barcode", "title"]

                    result = await export_and_upload_csv(
                        db_path=db_path,
                        staging_dir=staging_dir,
                        book_storage=mock_book_storage
                    )

                    # Verify staging directory was created
                    assert Path(staging_dir).exists()
                    assert result["success"] is True

    @pytest.mark.asyncio
    async def test_cleanup_failure_handling(self, mock_book_storage, sample_books):
        """Test handling of cleanup failures after successful operation."""
        with tempfile.TemporaryDirectory() as temp_dir:
            db_path = str(Path(temp_dir) / "test.db")
            staging_dir = str(Path(temp_dir) / "staging")

            with patch("grin_to_s3.collect_books.models.SQLiteProgressTracker") as mock_tracker_cls:
                mock_tracker = AsyncMock()
                mock_tracker.get_all_books_csv_data.return_value = sample_books
                mock_tracker_cls.return_value = mock_tracker

                with patch("grin_to_s3.collect_books.models.BookRecord") as mock_record_cls:
                    mock_record_cls.csv_headers.return_value = ["barcode", "title"]

                    # Mock Path.unlink to simulate cleanup failure
                    with patch("pathlib.Path.unlink") as mock_unlink:
                        mock_unlink.side_effect = OSError("Permission denied")

                        # Should raise exception for cleanup failure after successful operation
                        with pytest.raises(Exception) as exc_info:
                            await export_and_upload_csv(
                                db_path=db_path,
                                staging_dir=staging_dir,
                                book_storage=mock_book_storage
                            )

                        assert "cleanup failed" in str(exc_info.value)

    @pytest.mark.asyncio
    async def test_temp_file_content_verification(self, mock_book_storage, sample_books):
        """Test that temporary CSV file contains correct content."""
        with tempfile.TemporaryDirectory() as temp_dir:
            db_path = str(Path(temp_dir) / "test.db")
            staging_dir = str(Path(temp_dir) / "staging")

            # Capture the temporary file path for verification
            temp_file_path = None
            original_upload = mock_book_storage.upload_csv_file

            async def capture_temp_file(path, filename=None):
                nonlocal temp_file_path
                temp_file_path = path
                # Read and verify content before upload
                with open(path, encoding="utf-8") as f:
                    content = f.read()
                    lines = content.strip().split("\n")
                    assert len(lines) == 4  # header + 3 books
                    assert lines[0] == "barcode,title,author"
                    assert lines[1] == "TEST000,Test Book 0,Test Author 0"
                    assert lines[2] == "TEST001,Test Book 1,Test Author 1"
                    assert lines[3] == "TEST002,Test Book 2,Test Author 2"
                return await original_upload(path, filename)

            mock_book_storage.upload_csv_file = capture_temp_file

            with patch("grin_to_s3.collect_books.models.SQLiteProgressTracker") as mock_tracker_cls:
                mock_tracker = AsyncMock()
                mock_tracker.get_all_books_csv_data.return_value = sample_books
                mock_tracker_cls.return_value = mock_tracker

                with patch("grin_to_s3.collect_books.models.BookRecord") as mock_record_cls:
                    mock_record_cls.csv_headers.return_value = ["barcode", "title", "author"]

                    result = await export_and_upload_csv(
                        db_path=db_path,
                        staging_dir=staging_dir,
                        book_storage=mock_book_storage
                    )

                    assert result["success"] is True
                    assert temp_file_path is not None
                    # Verify temp file was cleaned up
                    assert not Path(temp_file_path).exists()

    @pytest.mark.asyncio
    async def test_concurrent_operations(self, mock_book_storage, sample_books):
        """Test that multiple concurrent export operations work correctly."""
        with tempfile.TemporaryDirectory() as temp_dir:
            db_path = str(Path(temp_dir) / "test.db")
            staging_dir = str(Path(temp_dir) / "staging")

            with patch("grin_to_s3.collect_books.models.SQLiteProgressTracker") as mock_tracker_cls:
                mock_tracker = AsyncMock()
                mock_tracker.get_all_books_csv_data.return_value = sample_books
                mock_tracker_cls.return_value = mock_tracker

                with patch("grin_to_s3.collect_books.models.BookRecord") as mock_record_cls:
                    mock_record_cls.csv_headers.return_value = ["barcode", "title"]

                    # Run multiple concurrent operations
                    tasks = []
                    for i in range(3):
                        task = export_and_upload_csv(
                            db_path=db_path,
                            staging_dir=staging_dir,
                            book_storage=mock_book_storage,
                            custom_filename=f"concurrent_{i}.csv"
                        )
                        tasks.append(task)

                    results = await asyncio.gather(*tasks)

                    # Verify all operations succeeded
                    for result in results:
                        assert result["success"] is True
                        assert result["exported"] is True
                        assert result["uploaded"] is True
                        assert result["temp_file_cleaned"] is True

                    # Verify all uploads were called
                    assert mock_book_storage.upload_csv_file.call_count == 3
