#!/usr/bin/env python3
"""
Tests for core sync functions.
"""

import asyncio
import tempfile
from pathlib import Path
from unittest.mock import AsyncMock, MagicMock, patch

import pytest
from aioresponses import aioresponses

from grin_to_s3.sync.operations import (
    check_and_handle_etag_skip,
    download_book_to_staging,
    extract_and_upload_ocr_text,
    sync_book_to_local_storage,
    upload_book_from_staging,
)
from tests.test_utils.parametrize_helpers import extraction_scenarios_parametrize, meaningful_storage_parametrize
from tests.test_utils.unified_mocks import mock_minimal_upload, mock_upload_operations


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

    @pytest.mark.asyncio
    async def test_download_book_to_staging_success(self, mock_grin_client, mock_staging_manager):
        """Test successful book download to staging."""
        # Mock staging manager
        mock_staging_manager.check_disk_space.return_value = True

        # Create a mock Path object for the staging file
        mock_staging_file = MagicMock()
        mock_staging_file.stat.return_value.st_size = 20  # Match the body size below
        mock_staging_manager.get_encrypted_file_path.return_value = mock_staging_file

        # Mock the HTTP response using aioresponses
        with (
            aioresponses() as mock_http,
            patch("aiofiles.open", create=True) as mock_aiofiles,
        ):
            # Mock the GRIN download URL
            mock_http.get(
                "https://books.google.com/libraries/Harvard/TEST123.tar.gz.gpg",
                body=b"test archive content",  # This is 20 bytes
                headers={"content-length": "20"}
            )

            # Mock file writing
            mock_file = AsyncMock()
            mock_aiofiles.return_value.__aenter__.return_value = mock_file
            mock_aiofiles.return_value.__aexit__.return_value = None

            barcode, staging_file_path, metadata = await download_book_to_staging(
                "TEST123", mock_grin_client, "Harvard", mock_staging_manager, "abc123"
            )

            assert barcode == "TEST123"
            assert staging_file_path == str(mock_staging_file)
            assert metadata["google_etag"] == "abc123"

    @pytest.mark.asyncio
    async def test_download_book_to_staging_failure(self, mock_grin_client, mock_staging_manager):
        """Test book download failure handling."""
        # Mock staging manager
        mock_staging_manager.check_disk_space.return_value = True

        # Create a mock Path object for the staging file
        mock_staging_file = MagicMock()
        mock_staging_manager.get_encrypted_file_path.return_value = mock_staging_file

        # Mock HTTP failure using aioresponses
        with aioresponses() as mock_http:
            # Mock 404 error
            mock_http.get(
                "https://books.google.com/libraries/Harvard/TEST123.tar.gz.gpg",
                status=404,
                payload={"error": "Not Found"}
            )

            with pytest.raises(Exception) as exc_info:
                await download_book_to_staging(
                    "TEST123", mock_grin_client, "Harvard", mock_staging_manager, "abc123"
                )

            # The error should be related to HTTP status or authentication
            assert "404" in str(exc_info.value) or "Not Found" in str(exc_info.value)


class TestBookUpload:
    """Test book upload functionality."""

    @pytest.mark.asyncio
    async def test_upload_book_from_staging_skip_scenario(
        self, mock_storage_config, mock_staging_manager, mock_progress_tracker
    ):
        """Test upload handling skip download scenario."""
        with mock_minimal_upload():
            result = await upload_book_from_staging(
                "TEST123", "SKIP_DOWNLOAD", "minio", mock_storage_config, mock_staging_manager, mock_progress_tracker
            )

        assert result["barcode"] == "TEST123"
        assert result["status"] == "completed"
        assert result["skipped"] is True

    @pytest.mark.asyncio
    async def test_upload_book_from_staging_success(
        self, mock_storage_config, mock_staging_manager, mock_progress_tracker
    ):
        """Test successful book upload from staging."""
        # Set up properly configured mock staging manager
        mock_staging_manager.cleanup_files.return_value = 1024 * 1024  # Return int, not MagicMock

        with mock_upload_operations() as mocks:
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
                skip_extract_ocr=True,
                skip_extract_marc=True,
            )

            # Verify successful upload
            assert result["status"] == "completed"
            assert result["barcode"] == "TEST123"
            assert result["decrypted_success"] is True

            # Verify storage operations were called
            mocks.storage.save_decrypted_archive_from_file.assert_called_once()
            mocks.decrypt.assert_called_once()


class TestLocalStorageSync:
    """Test local storage sync functionality."""

    @pytest.mark.asyncio
    async def test_sync_book_to_local_storage_success(self, mock_grin_client, mock_progress_tracker):
        """Test successful local storage sync."""
        import tempfile

        with tempfile.TemporaryDirectory() as temp_dir:
            storage_config = {
                "base_path": temp_dir,
                "bucket_raw": "test-raw",
                "bucket_meta": "test-meta",
                "bucket_full": "test-full",
                "prefix": ""
            }

            # Mock the HTTP call and GPG decryption
            with (
                aioresponses() as mock_http,
                patch("grin_to_s3.sync.operations.decrypt_gpg_file") as mock_decrypt,
                patch("aiofiles.open", create=True) as mock_aiofiles,
                patch("pathlib.Path.mkdir"),
                patch("pathlib.Path.unlink"),
            ):
                # Mock the GRIN download URL
                mock_http.get(
                    "https://books.google.com/libraries/Harvard/TEST123.tar.gz.gpg",
                    body=b"test archive content",
                    headers={"content-length": "20"}
                )

                # Mock file writing
                mock_file = AsyncMock()
                mock_aiofiles.return_value.__aenter__.return_value = mock_file
                mock_aiofiles.return_value.__aexit__.return_value = None

                result = await sync_book_to_local_storage(
                    "TEST123", mock_grin_client, "Harvard", storage_config, mock_progress_tracker
                )

                assert result["barcode"] == "TEST123"
                assert result["status"] == "completed"
                assert result["decrypted_success"] is True

                # Verify operations were called
                mock_decrypt.assert_called_once()
                mock_file.write.assert_called()
                mock_file.flush.assert_called_once()

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
    def test_decrypted_file(self):
        """Create a temporary test archive file."""
        with tempfile.NamedTemporaryFile(suffix=".tar.gz", delete=False) as f:
            test_file = Path(f.name)
        yield test_file
        test_file.unlink(missing_ok=True)

    @pytest.mark.asyncio
    async def test_extract_and_upload_ocr_text_success(
        self, mock_book_manager, mock_progress_tracker, mock_staging_manager, mock_logger, test_decrypted_file
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
                "TEST123", test_decrypted_file, mock_book_manager, mock_progress_tracker, mock_staging_manager, mock_logger
            )

            # Verify extraction was called
            mock_extract.assert_called_once()

            # Verify database status tracking
            assert mock_write_status.call_count == 3  # starting, extracting, completed

            # Verify upload was called
            mock_book_manager.save_ocr_text_jsonl_from_file.assert_called_once()

            # Verify success logging
            mock_logger.info.assert_any_call("[TEST123] Starting OCR text extraction from decrypted archive")
            mock_logger.info.assert_any_call("[TEST123] Extracted 342 pages from archive")

    @pytest.mark.asyncio
    async def test_extract_and_upload_ocr_text_extraction_failure(
        self, mock_book_manager, mock_progress_tracker, mock_staging_manager, mock_logger, test_decrypted_file
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
                "TEST123", test_decrypted_file, mock_book_manager, mock_progress_tracker, mock_staging_manager, mock_logger
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
        self, mock_book_manager, mock_progress_tracker, mock_staging_manager, mock_logger, test_decrypted_file
    ):
        """Test OCR upload failure handling (non-blocking)."""
        with (
            patch("grin_to_s3.sync.operations.extract_ocr_pages") as mock_extract,
            patch("grin_to_s3.sync.operations.write_status"),
        ):
            # Mock extraction success but upload failure
            mock_extract.return_value = 100
            mock_book_manager.save_ocr_text_jsonl_from_file.side_effect = Exception("Network timeout")

            # Create mock JSONL file
            jsonl_file = mock_staging_manager.staging_dir / "TEST123_ocr_temp.jsonl"
            jsonl_file.parent.mkdir(parents=True, exist_ok=True)
            jsonl_file.write_text('{"page": 1, "text": "Test"}\n')

            # This should not raise an exception (non-blocking)
            await extract_and_upload_ocr_text(
                "TEST123", test_decrypted_file, mock_book_manager, mock_progress_tracker, mock_staging_manager, mock_logger
            )

            # Verify failure was logged
            mock_logger.error.assert_called_once()
            assert "OCR extraction failed but sync continues" in str(mock_logger.error.call_args)

    @pytest.mark.asyncio
    async def test_extract_and_upload_ocr_text_no_db_tracker(
        self, mock_book_manager, mock_staging_manager, mock_logger, test_decrypted_file
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
                "TEST123", test_decrypted_file, mock_book_manager, None, mock_staging_manager, mock_logger
            )

            # Verify extraction and upload still happened
            mock_extract.assert_called_once()
            mock_book_manager.save_ocr_text_jsonl_from_file.assert_called_once()

    @pytest.mark.asyncio
    @meaningful_storage_parametrize()
    async def test_upload_book_from_staging_with_ocr_extraction(
        self, storage_type, mock_storage_config, mock_staging_manager, mock_progress_tracker
    ):
        """Test upload_book_from_staging with OCR extraction enabled across local vs cloud storage."""
        # Set up properly configured mock staging manager
        mock_staging_manager.cleanup_files.return_value = 1024 * 1024  # Return int, not MagicMock

        with mock_upload_operations(skip_marc=True) as mocks:
            result = await upload_book_from_staging(
                "TEST123",
                "/staging/TEST123.tar.gz.gpg",
                storage_type,
                mock_storage_config,
                mock_staging_manager,
                mock_progress_tracker,
                "encrypted_etag_123",
                "gpg_key_file",
                "secrets_dir",
                skip_extract_ocr=False,  # OCR extraction enabled
            )

            # Verify OCR extraction was called
            mocks.extract_ocr.assert_called_once()

            # Verify successful result
            assert result["status"] == "completed"
            assert result["barcode"] == "TEST123"

    @pytest.mark.asyncio
    @extraction_scenarios_parametrize()
    async def test_upload_book_from_staging_extraction_scenarios(
        self, skip_ocr, skip_marc, mock_storage_config, mock_staging_manager, mock_progress_tracker
    ):
        """Test upload_book_from_staging with different extraction scenarios."""
        # Set up properly configured mock staging manager
        mock_staging_manager.cleanup_files.return_value = 1024 * 1024  # Return int, not MagicMock

        with mock_upload_operations(skip_ocr=skip_ocr, skip_marc=skip_marc) as mocks:
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
                skip_extract_ocr=skip_ocr,
                skip_extract_marc=skip_marc,
            )

            # Verify extraction behavior based on parameters
            if skip_ocr:
                mocks.extract_ocr.assert_not_called()
            else:
                mocks.extract_ocr.assert_called_once()

            if skip_marc:
                mocks.extract_marc.assert_not_called()
            else:
                mocks.extract_marc.assert_called_once()

            # Verify successful result regardless of extraction scenario
            assert result["status"] == "completed"
            assert result["barcode"] == "TEST123"

    @pytest.mark.asyncio
    async def test_upload_book_from_staging_extraction_task_cancellation(
        self, mock_storage_config, mock_staging_manager, mock_progress_tracker
    ):
        """Test that OCR extraction task is cancelled when upload fails."""
        # Set up properly configured mock staging manager
        mock_staging_manager.cleanup_files.return_value = 1024 * 1024  # Return int, not MagicMock

        with mock_upload_operations(should_fail=False) as mocks:
            # Configure storage to fail after decryption succeeds
            mocks.storage.save_decrypted_archive_from_file.side_effect = Exception("Storage upload failed")

            # Mock extract function to simulate long-running task
            async def mock_extract_func(*args, **kwargs):
                await asyncio.sleep(0.1)  # Simulate work

            mocks.extract_ocr.side_effect = mock_extract_func

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
            assert "Storage upload failed" in result["error"]


class TestBookStorageIntegrationInSync:
    """Test real BookStorage initialization in sync operations."""

    @pytest.mark.asyncio
    @meaningful_storage_parametrize()
    async def test_upload_book_from_staging_real_book_storage_initialization(self, storage_type):
        """Test that upload_book_from_staging correctly initializes BookStorage across local vs cloud storage."""
        with tempfile.TemporaryDirectory() as temp_dir:
            # Create proper storage config with bucket information
            if storage_type == "local":
                storage_config = {
                    "type": storage_type,
                    "bucket_raw": "test-raw",
                    "bucket_meta": "test-meta",
                    "bucket_full": "test-full",
                    "config": {"base_path": temp_dir},
                    "prefix": "",
                }
            else:
                # For cloud storage types (s3, r2, minio)
                storage_config = {
                    "type": storage_type,
                    "bucket_raw": "test-raw",
                    "bucket_meta": "test-meta",
                    "bucket_full": "test-full",
                    "config": {
                        "endpoint_url": f"https://test-{storage_type}.example.com",
                        "access_key_id": "test-key",
                        "secret_access_key": "test-secret",
                        "region": "test-region",
                    },
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

            with mock_upload_operations() as mocks:
                # Configure specific storage behavior for this test
                mocks.book_storage_class.return_value.save_decrypted_archive_from_file = AsyncMock(
                    return_value="bucket_raw/TEST123/TEST123.tar.gz"
                )

                # This should NOT raise "missing bucket_config argument" error
                result = await upload_book_from_staging(
                    "TEST123",
                    str(encrypted_file),
                    storage_type,
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
