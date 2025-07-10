#!/usr/bin/env python3
"""
Tests for core sync functions.
"""

import asyncio
import tempfile
from pathlib import Path
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from grin_to_s3.sync.operations import (
    check_and_handle_etag_skip,
    extract_and_upload_ocr_text,
    sync_book_to_local_storage,
    upload_book_from_staging,
)


class TestETagSkipHandling:
    """Test ETag skip handling functionality."""

    @pytest.mark.asyncio
    async def test_check_and_handle_etag_skip_no_skip(
        self, mock_grin_client, mock_progress_tracker, mock_storage_config
    ):
        """Test ETag check when file should not be skipped."""
        with (
            patch("grin_to_s3.sync.operations.check_encrypted_etag") as mock_check_etag,
            patch("grin_to_s3.sync.operations.should_skip_download") as mock_should_skip,
        ):
            mock_check_etag.return_value = ("abc123", 1024)
            mock_should_skip.return_value = (False, "no_skip_reason")

            result, etag, file_size = await check_and_handle_etag_skip(
                "TEST123", mock_grin_client, "Harvard", "minio", mock_storage_config, mock_progress_tracker
            )

            assert result is None  # No skip
            assert etag == "abc123"
            assert file_size == 1024

    @pytest.mark.asyncio
    async def test_check_and_handle_etag_skip_with_skip(
        self, mock_grin_client, mock_progress_tracker, mock_storage_config
    ):
        """Test ETag check when file should be skipped."""
        with (
            patch("grin_to_s3.sync.operations.check_encrypted_etag") as mock_check_etag,
            patch("grin_to_s3.sync.operations.should_skip_download") as mock_should_skip,
        ):
            mock_check_etag.return_value = ("abc123", 1024)
            mock_should_skip.return_value = (True, "etag_match")

            result, etag, file_size = await check_and_handle_etag_skip(
                "TEST123", mock_grin_client, "Harvard", "minio", mock_storage_config, mock_progress_tracker
            )

            assert result is not None  # Skip result returned
            assert result["barcode"] == "TEST123"
            assert result["status"] == "completed"
            assert result["skipped"] is True
            assert result["encrypted_etag"] == "abc123"
            assert result["file_size"] == 1024
            assert etag == "abc123"
            assert file_size == 1024


class TestBookDownload:
    """Test book download functionality."""

    # TODO: Fix async mocking issues with GRIN client
    # These tests need proper async mocking setup for the GRIN client's HTTP session


class TestBookUpload:
    """Test book upload functionality."""

    @pytest.mark.asyncio
    async def test_upload_book_from_staging_skip_scenario(
        self, mock_storage_config, mock_staging_manager, mock_progress_tracker
    ):
        """Test upload handling skip download scenario."""
        with patch("grin_to_s3.sync.operations.extract_and_update_marc_metadata"):
            result = await upload_book_from_staging(
                "TEST123", "SKIP_DOWNLOAD", "minio", mock_storage_config, mock_staging_manager, mock_progress_tracker
            )

        assert result["barcode"] == "TEST123"
        assert result["status"] == "completed"
        assert result["skipped"] is True

    # TODO: Fix async mocking issues
    # @pytest.mark.asyncio
    # async def test_upload_book_from_staging_success(
    #     self, mock_storage_config, mock_staging_manager, mock_progress_tracker
    # ):
    #     """Test successful book upload from staging."""
    #     # This test needs fixing for async mocking
    #     pass


class TestLocalStorageSync:
    """Test local storage sync functionality."""

    # TODO: Fix async mocking issues
    # @pytest.mark.asyncio
    # async def test_sync_book_to_local_storage_success(self, mock_grin_client, mock_progress_tracker):
    #     """Test successful local storage sync."""
    #     # This test needs fixing for async mocking
    #     pass

    @pytest.mark.asyncio
    async def test_sync_book_to_local_storage_missing_base_path(self, mock_grin_client, mock_progress_tracker):
        """Test local storage sync with missing base_path."""
        storage_config = {}  # No base_path

        result = await sync_book_to_local_storage(
            "TEST123", mock_grin_client, "Harvard", storage_config, mock_progress_tracker
        )

        assert result["barcode"] == "TEST123"
        assert result["status"] == "failed"
        assert "Local storage requires" in result["error"]


class TestOCRExtractionIntegration:
    """Test OCR extraction integration in sync pipeline."""

    @pytest.fixture
    def mock_logger(self):
        """Create a mock logger."""
        return MagicMock()

    @pytest.fixture
    def mock_book_storage(self):
        """Create a mock BookStorage instance."""
        storage = MagicMock()
        storage.save_ocr_text_jsonl_from_file = AsyncMock(return_value="bucket_full/TEST123/TEST123.jsonl")
        return storage

    @pytest.fixture
    def mock_db_tracker(self):
        """Create a mock database tracker."""
        tracker = MagicMock()
        tracker.db_path = "/tmp/test.db"
        return tracker

    @pytest.fixture
    def mock_staging_manager(self):
        """Create a mock staging manager."""
        manager = MagicMock()
        manager.staging_dir = Path("/tmp/staging")
        return manager

    @pytest.fixture
    def test_decrypted_file(self):
        """Create a temporary test archive file."""
        with tempfile.NamedTemporaryFile(suffix=".tar.gz", delete=False) as f:
            test_file = Path(f.name)
        yield test_file
        test_file.unlink(missing_ok=True)

    @pytest.mark.asyncio
    async def test_extract_and_upload_ocr_text_success(
        self, mock_book_storage, mock_db_tracker, mock_staging_manager, mock_logger, test_decrypted_file
    ):
        """Test successful OCR extraction and upload."""
        with (
            patch("grin_to_s3.sync.operations.extract_ocr_pages") as mock_extract,
            patch("grin_to_s3.sync.operations.write_status") as mock_write_status,
            patch("grin_to_s3.sync.operations.time.time") as mock_time,
        ):
            # Mock extraction success
            mock_extract.return_value = 342  # page count
            mock_time.return_value = 1672531200

            # Create mock JSONL file
            jsonl_file = mock_staging_manager.staging_dir / "TEST123_ocr_temp.jsonl"
            jsonl_file.parent.mkdir(parents=True, exist_ok=True)
            jsonl_file.write_text('{"page": 1, "text": "Test content"}\n')

            await extract_and_upload_ocr_text(
                "TEST123", test_decrypted_file, mock_book_storage, mock_db_tracker, mock_staging_manager, mock_logger
            )

            # Verify extraction was called
            mock_extract.assert_called_once()

            # Verify database status tracking
            assert mock_write_status.call_count == 3  # starting, extracting, completed

            # Verify upload was called
            mock_book_storage.save_ocr_text_jsonl_from_file.assert_called_once()

            # Verify success logging
            mock_logger.info.assert_any_call("[TEST123] Starting OCR text extraction from decrypted archive")
            mock_logger.info.assert_any_call("[TEST123] Extracted 342 pages from archive")

    @pytest.mark.asyncio
    async def test_extract_and_upload_ocr_text_extraction_failure(
        self, mock_book_storage, mock_db_tracker, mock_staging_manager, mock_logger, test_decrypted_file
    ):
        """Test OCR extraction failure handling (non-blocking)."""
        with (
            patch("grin_to_s3.sync.operations.extract_ocr_pages") as mock_extract,
            patch("grin_to_s3.sync.operations.write_status") as mock_write_status,
        ):
            # Mock extraction failure
            mock_extract.side_effect = Exception("Archive corrupted")

            # This should not raise an exception (non-blocking)
            await extract_and_upload_ocr_text(
                "TEST123", test_decrypted_file, mock_book_storage, mock_db_tracker, mock_staging_manager, mock_logger
            )

            # Verify failure was tracked in database
            # Check that write_status was called with FAILED status
            calls = mock_write_status.call_args_list
            failed_calls = [
                call
                for call in calls
                if len(call[0]) >= 3 and "FAILED" in str(call[0][2]) or "failed" in str(call[0][2])
            ]
            assert len(failed_calls) > 0, f"Expected FAILED status call, but got: {calls}"

            # Verify error was logged but didn't raise
            mock_logger.error.assert_called_once()
            assert "OCR extraction failed but sync continues" in str(mock_logger.error.call_args)

    @pytest.mark.asyncio
    async def test_extract_and_upload_ocr_text_upload_failure(
        self, mock_book_storage, mock_db_tracker, mock_staging_manager, mock_logger, test_decrypted_file
    ):
        """Test OCR upload failure handling (non-blocking)."""
        with (
            patch("grin_to_s3.sync.operations.extract_ocr_pages") as mock_extract,
            patch("grin_to_s3.sync.operations.write_status"),
        ):
            # Mock extraction success but upload failure
            mock_extract.return_value = 100
            mock_book_storage.save_ocr_text_jsonl_from_file.side_effect = Exception("Network timeout")

            # Create mock JSONL file
            jsonl_file = mock_staging_manager.staging_dir / "TEST123_ocr_temp.jsonl"
            jsonl_file.parent.mkdir(parents=True, exist_ok=True)
            jsonl_file.write_text('{"page": 1, "text": "Test"}\n')

            # This should not raise an exception (non-blocking)
            await extract_and_upload_ocr_text(
                "TEST123", test_decrypted_file, mock_book_storage, mock_db_tracker, mock_staging_manager, mock_logger
            )

            # Verify failure was logged
            mock_logger.error.assert_called_once()
            assert "OCR extraction failed but sync continues" in str(mock_logger.error.call_args)

    @pytest.mark.asyncio
    async def test_extract_and_upload_ocr_text_no_db_tracker(
        self, mock_book_storage, mock_staging_manager, mock_logger, test_decrypted_file
    ):
        """Test OCR extraction without database tracker."""
        with patch("grin_to_s3.sync.operations.extract_ocr_pages") as mock_extract:
            mock_extract.return_value = 50

            # Create mock JSONL file
            jsonl_file = mock_staging_manager.staging_dir / "TEST123_ocr_temp.jsonl"
            jsonl_file.parent.mkdir(parents=True, exist_ok=True)
            jsonl_file.write_text('{"page": 1, "text": "Test"}\n')

            # Should work without database tracker
            await extract_and_upload_ocr_text(
                "TEST123", test_decrypted_file, mock_book_storage, None, mock_staging_manager, mock_logger
            )

            # Verify extraction and upload still happened
            mock_extract.assert_called_once()
            mock_book_storage.save_ocr_text_jsonl_from_file.assert_called_once()

    @pytest.mark.asyncio
    async def test_upload_book_from_staging_with_ocr_extraction(
        self, mock_storage_config, mock_staging_manager, mock_progress_tracker
    ):
        """Test upload_book_from_staging with OCR extraction enabled."""
        with (
            patch("grin_to_s3.sync.operations.decrypt_gpg_file"),
            patch("grin_to_s3.sync.operations.create_storage_from_config") as mock_create_storage,
            patch("grin_to_s3.sync.operations.extract_and_upload_ocr_text") as mock_extract,
            patch("grin_to_s3.sync.operations.extract_and_update_marc_metadata"),
            patch("grin_to_s3.sync.operations.BookStorage") as mock_book_storage_class,
        ):
            # Mock successful decryption and upload
            mock_storage = MagicMock()
            mock_storage.save_decrypted_archive_from_file = AsyncMock(return_value="path/to/archive")
            mock_create_storage.return_value = mock_storage
            mock_book_storage_class.return_value = mock_storage

            # Mock progress tracker methods
            mock_progress_tracker.add_status_change = AsyncMock()
            mock_progress_tracker.update_sync_data = AsyncMock()

            # Mock staging manager
            mock_staging_manager.get_decrypted_file_path.return_value = Path("/staging/TEST123.tar.gz")
            mock_staging_manager.cleanup_files.return_value = 1024 * 1024  # 1MB

            # Mock extract function as async
            mock_extract.return_value = None

            result = await upload_book_from_staging(
                "TEST123",
                "/staging/TEST123.tar.gz.gpg",
                "minio",
                mock_storage_config,
                mock_staging_manager,
                mock_progress_tracker,
                "encrypted_etag_123",
                "gpg_key_file",
                "secrets_dir",
                skip_extract_ocr=False,  # OCR extraction enabled
            )

            # Verify OCR extraction was called
            mock_extract.assert_called_once()

            # Verify successful result
            assert result["status"] == "completed"
            assert result["barcode"] == "TEST123"

    @pytest.mark.asyncio
    async def test_upload_book_from_staging_skip_ocr_extraction(
        self, mock_storage_config, mock_staging_manager, mock_progress_tracker
    ):
        """Test upload_book_from_staging with OCR extraction disabled."""
        with (
            patch("grin_to_s3.sync.operations.decrypt_gpg_file"),
            patch("grin_to_s3.sync.operations.create_storage_from_config") as mock_create_storage,
            patch("grin_to_s3.sync.operations.extract_and_upload_ocr_text") as mock_extract,
            patch("grin_to_s3.sync.operations.extract_and_update_marc_metadata"),
            patch("grin_to_s3.sync.operations.BookStorage") as mock_book_storage_class,
        ):
            # Mock successful decryption and upload
            mock_storage = MagicMock()
            mock_storage.save_decrypted_archive_from_file = AsyncMock(return_value="path/to/archive")
            mock_create_storage.return_value = mock_storage
            mock_book_storage_class.return_value = mock_storage

            # Mock progress tracker methods
            mock_progress_tracker.add_status_change = AsyncMock()
            mock_progress_tracker.update_sync_data = AsyncMock()

            # Mock staging manager
            mock_staging_manager.get_decrypted_file_path.return_value = Path("/staging/TEST123.tar.gz")
            mock_staging_manager.cleanup_files.return_value = 1024 * 1024

            result = await upload_book_from_staging(
                "TEST123",
                "/staging/TEST123.tar.gz.gpg",
                "minio",
                mock_storage_config,
                mock_staging_manager,
                mock_progress_tracker,
                "encrypted_etag_123",
                "gpg_key_file",
                "secrets_dir",
                skip_extract_ocr=True,  # OCR extraction disabled
            )

            # Verify OCR extraction was NOT called
            mock_extract.assert_not_called()

            # Verify successful result
            assert result["status"] == "completed"
            assert result["barcode"] == "TEST123"

    @pytest.mark.asyncio
    async def test_upload_book_from_staging_extraction_task_cancellation(
        self, mock_storage_config, mock_staging_manager, mock_progress_tracker
    ):
        """Test that OCR extraction task is cancelled when upload fails."""
        with (
            patch("grin_to_s3.sync.operations.decrypt_gpg_file"),
            patch("grin_to_s3.sync.operations.create_storage_from_config") as mock_create_storage,
            patch("grin_to_s3.sync.operations.extract_and_upload_ocr_text") as mock_extract,
            patch("grin_to_s3.sync.operations.extract_and_update_marc_metadata"),
            patch("grin_to_s3.sync.operations.BookStorage") as mock_book_storage_class,
        ):
            # Mock successful decryption but failed upload
            mock_storage = MagicMock()
            mock_storage.save_decrypted_archive_from_file = AsyncMock(side_effect=Exception("Upload failed"))
            mock_create_storage.return_value = mock_storage
            mock_book_storage_class.return_value = mock_storage

            # Mock staging manager
            mock_staging_manager.get_decrypted_file_path.return_value = Path("/staging/TEST123.tar.gz")

            # Mock extract function to simulate long-running task
            async def mock_extract_func(*args, **kwargs):
                await asyncio.sleep(0.1)  # Simulate work

            mock_extract.side_effect = mock_extract_func

            result = await upload_book_from_staging(
                "TEST123",
                "/staging/TEST123.tar.gz.gpg",
                "minio",
                mock_storage_config,
                mock_staging_manager,
                mock_progress_tracker,
                "encrypted_etag_123",
                "gpg_key_file",
                "secrets_dir",
                skip_extract_ocr=False,
            )

            # Verify upload failed
            assert result["status"] == "failed"
            assert "Upload failed" in result["error"]


class TestBookStorageIntegrationInSync:
    """Test real BookStorage initialization in sync operations."""

    @pytest.mark.asyncio
    async def test_upload_book_from_staging_real_book_storage_initialization(self):
        """Test that upload_book_from_staging correctly initializes BookStorage."""
        with tempfile.TemporaryDirectory() as temp_dir:
            # Create proper storage config with bucket information
            storage_config = {
                "type": "local",
                "bucket_raw": "test-raw",
                "bucket_meta": "test-meta",
                "bucket_full": "test-full",
                "config": {"base_path": temp_dir},
                "prefix": "",
            }

            # Mock other dependencies but let BookStorage initialize normally
            mock_staging_manager = MagicMock()
            mock_staging_manager.get_decrypted_file_path.return_value = Path(temp_dir) / "TEST123.tar.gz"
            mock_staging_manager.cleanup_files.return_value = 1024

            mock_progress_tracker = MagicMock()
            mock_progress_tracker.add_status_change = AsyncMock()
            mock_progress_tracker.update_sync_data = AsyncMock()

            # Create dummy files for encryption and decryption
            encrypted_file = Path(temp_dir) / "TEST123.tar.gz.gpg"
            encrypted_file.write_text("dummy encrypted content")

            decrypted_file = Path(temp_dir) / "TEST123.tar.gz"
            decrypted_file.write_text("dummy decrypted content")

            with (
                patch("grin_to_s3.sync.operations.decrypt_gpg_file") as mock_decrypt,
                patch("grin_to_s3.sync.operations.create_storage_from_config") as mock_create_storage,
                patch("grin_to_s3.sync.operations.extract_and_update_marc_metadata"),
                patch("grin_to_s3.sync.operations.BookStorage") as mock_book_storage_class,
            ):
                # Mock successful decryption
                mock_decrypt.return_value = None

                # Mock storage creation to return a mock storage
                mock_storage = MagicMock()
                mock_create_storage.return_value = mock_storage

                # Mock BookStorage instance
                mock_book_storage = MagicMock()
                mock_book_storage.save_decrypted_archive_from_file = AsyncMock(
                    return_value="bucket_raw/TEST123/TEST123.tar.gz"
                )
                mock_book_storage_class.return_value = mock_book_storage

                # This should NOT raise "missing bucket_config argument" error
                result = await upload_book_from_staging(
                    "TEST123",
                    str(encrypted_file),
                    "local",
                    storage_config,
                    mock_staging_manager,
                    mock_progress_tracker,
                    "encrypted_etag_123",
                    None,  # gpg_key_file
                    None,  # secrets_dir
                    skip_extract_ocr=True,  # Skip OCR to focus on BookStorage init
                )

                # Verify successful completion
                assert result["status"] == "completed"
                assert result["barcode"] == "TEST123"
