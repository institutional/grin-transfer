#!/usr/bin/env python3
"""
Tests for core sync functions.
"""

import asyncio
import tempfile
from pathlib import Path
from unittest.mock import AsyncMock, MagicMock, patch

import aiohttp
import pytest
from aioresponses import aioresponses

from grin_to_s3.collect_books.models import SQLiteProgressTracker
from grin_to_s3.sync.operations import (
    check_and_handle_etag_skip,
    download_book_to_filesystem,
    download_book_to_local,
    extract_and_upload_ocr_text,
    upload_book_from_staging,
)
from tests.test_utils.parametrize_helpers import extraction_scenarios_parametrize, meaningful_storage_parametrize
from tests.test_utils.unified_mocks import mock_minimal_upload, mock_upload_operations


@pytest.fixture
def mock_semaphore():
    """Mock semaphore for GRIN API concurrency testing."""
    return asyncio.Semaphore(5)


class TestETagSkipHandling:
    """Test ETag skip handling functionality."""

    @pytest.mark.asyncio
    async def test_check_and_handle_etag_skip_no_skip(
        self, mock_grin_client, mock_progress_tracker, mock_storage_config, mock_semaphore
    ):
        """Test ETag check when file should not be skipped."""
        with (
            patch("grin_to_s3.sync.operations.check_encrypted_etag") as mock_check_etag,
            patch("grin_to_s3.sync.operations.should_skip_download") as mock_should_skip,
        ):
            mock_check_etag.return_value = ("abc123", 1024, 200)
            mock_should_skip.return_value = (False, "no_skip_reason")

            result, etag, file_size, status_updates = await check_and_handle_etag_skip(
                "TEST123", mock_grin_client, "Harvard", "minio", mock_storage_config, mock_progress_tracker, mock_semaphore
            )

            assert result is None  # No skip
            assert etag == "abc123"
            assert file_size == 1024

    @pytest.mark.asyncio
    async def test_check_and_handle_etag_skip_with_skip(
        self, mock_grin_client, mock_progress_tracker, mock_storage_config, mock_semaphore
    ):
        """Test ETag check when file should be skipped."""
        with (
            patch("grin_to_s3.sync.operations.check_encrypted_etag") as mock_check_etag,
            patch("grin_to_s3.sync.operations.should_skip_download") as mock_should_skip,
        ):
            mock_check_etag.return_value = ("abc123", 1024, 200)
            mock_should_skip.return_value = (True, "etag_match")

            result, etag, file_size, status_updates = await check_and_handle_etag_skip(
                "TEST123", mock_grin_client, "Harvard", "minio", mock_storage_config, mock_progress_tracker, mock_semaphore
            )

            assert result is not None  # Skip result returned

    @pytest.mark.asyncio
    async def test_check_and_handle_etag_skip_404_archive(
        self, mock_grin_client, mock_progress_tracker, mock_storage_config, mock_semaphore
    ):
        """Test ETag check when HEAD returns 404 (issue #180 optimization)."""
        with patch("grin_to_s3.sync.operations.check_encrypted_etag") as mock_check_etag:
            # Mock 404 response from HEAD request
            mock_check_etag.return_value = (None, None, 404)

            result, etag, file_size, status_updates = await check_and_handle_etag_skip(
                "TEST123", mock_grin_client, "Harvard", "minio", mock_storage_config, mock_progress_tracker, mock_semaphore
            )

            # Should return skip result due to 404
            assert result is not None
            assert result["status"] == "completed"
            assert result["skipped"] is True
            assert etag is None
            assert file_size == 0

            # Should have status update indicating 404 skip
            assert len(status_updates) == 1
            assert status_updates[0].status_value == "skipped"
            assert status_updates[0].metadata["skip_reason"] == "archive_not_found_404"
            assert status_updates[0].metadata["http_status"] == 404


class TestBookDownload:
    """Test book download functionality."""

    @pytest.mark.asyncio
    async def test_download_book_to_filesystem_success(self, mock_grin_client, mock_staging_manager):
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

            barcode, staging_file_path, metadata = await download_book_to_filesystem(
                "TEST123", mock_grin_client, "Harvard", "abc123", staging_manager=mock_staging_manager
            )

            assert barcode == "TEST123"
            assert staging_file_path == str(mock_staging_file)
            assert metadata["google_etag"] == "abc123"

    @pytest.mark.asyncio
    async def test_download_book_to_filesystem_failure(self, mock_grin_client, mock_staging_manager):
        """Test book download failure handling."""
        # Mock staging manager
        mock_staging_manager.check_disk_space.return_value = True

        # Create a mock Path object for the staging file
        mock_staging_file = MagicMock()
        mock_staging_manager.get_encrypted_file_path.return_value = mock_staging_file

        # Override the mock to simulate 404 HTTP error
        mock_404_error = aiohttp.ClientResponseError(
            request_info=aiohttp.RequestInfo(url="https://example.com", method="GET", headers={}),
            history=(),
            status=404,
            message="Not Found"
        )
        mock_grin_client.auth.make_authenticated_request.side_effect = mock_404_error

        # Track how many times the request was attempted
        call_count_before = len(mock_grin_client.auth.make_authenticated_request.call_args_list)

        with pytest.raises(aiohttp.ClientResponseError) as exc_info:
            await download_book_to_filesystem(
                "TEST123", mock_grin_client, "Harvard", "abc123", staging_manager=mock_staging_manager, download_retries=2
            )

        # Verify that it's a 404 error
        assert exc_info.value.status == 404

        # Verify that 404 was NOT retried (only 1 attempt should be made)
        call_count_after = len(mock_grin_client.auth.make_authenticated_request.call_args_list)
        attempts_made = call_count_after - call_count_before
        assert attempts_made == 1, f"Expected 1 attempt for 404, but {attempts_made} attempts were made"

    @pytest.mark.asyncio
    async def test_download_book_retries_non_404_errors(self, mock_staging_manager):
        """Test that non-404 HTTP errors are retried."""
        # Create a mock client
        from grin_to_s3.auth.grin_auth import GRINAuth
        mock_grin_client = MagicMock()
        mock_grin_client.auth = MagicMock(spec=GRINAuth)

        # Create a mock Path object for the staging file
        mock_staging_file = MagicMock()
        mock_staging_manager.get_encrypted_file_path.return_value = mock_staging_file

        # Override the mock to simulate 500 HTTP error (should be retried)
        import aiohttp
        mock_500_error = aiohttp.ClientResponseError(
            request_info=aiohttp.RequestInfo(url="https://example.com", method="GET", headers={}),
            history=(),
            status=500,
            message="Internal Server Error"
        )
        mock_grin_client.auth.make_authenticated_request.side_effect = mock_500_error

        # Track how many times the request was attempted
        call_count_before = len(mock_grin_client.auth.make_authenticated_request.call_args_list)

        with pytest.raises(aiohttp.ClientResponseError) as exc_info:
            await download_book_to_filesystem(
                "TEST123", mock_grin_client, "Harvard", "abc123", staging_manager=mock_staging_manager, download_retries=2
            )

        # Verify that it's a 500 error
        assert exc_info.value.status == 500

        # Verify that 500 WAS retried (3 attempts should be made: initial + 2 retries)
        call_count_after = len(mock_grin_client.auth.make_authenticated_request.call_args_list)
        attempts_made = call_count_after - call_count_before
        assert attempts_made == 3, f"Expected 3 attempts for 500 error, but {attempts_made} attempts were made"


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
        self, mock_storage_config, mock_staging_manager, temp_db
    ):
        """Test successful book upload from staging."""
        # Create real progress tracker with proper database
        progress_tracker = SQLiteProgressTracker(temp_db)

        # Set up properly configured mock staging manager
        mock_staging_manager.cleanup_files.return_value = 1024 * 1024  # Return int, not MagicMock

        with mock_upload_operations() as mocks:
            result = await upload_book_from_staging(
                "TEST123",
                "/staging/TEST123.tar.gz.gpg",
                "minio",
                mock_storage_config,
                mock_staging_manager,
                progress_tracker,
                "encrypted_etag_123",
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
    async def test_sync_book_to_local_storage_success(self, mock_grin_client, temp_db):
        """Test successful local storage sync."""
        import tempfile
        from pathlib import Path

        from grin_to_s3.collect_books.models import SQLiteProgressTracker

        # Create progress tracker with properly initialized database
        mock_progress_tracker = SQLiteProgressTracker(temp_db)

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
                patch("pathlib.Path.unlink"),
                patch("grin_to_s3.sync.operations.extract_and_upload_ocr_text") as mock_extract_ocr,
                patch("grin_to_s3.sync.operations.extract_and_update_marc_metadata") as mock_extract_marc,
                patch("tarfile.open") as mock_tarfile,
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

                # Mock extraction functions to prevent actual extraction
                mock_extract_ocr.return_value = []
                mock_extract_marc.return_value = []

                # Mock tarfile operations
                mock_tar = mock_tarfile.return_value.__enter__.return_value
                mock_tar.extractall = MagicMock()

                # Create a side effect for decrypt that creates the expected decrypted file
                def mock_decrypt_side_effect(encrypted_path, decrypted_path, *args, **kwargs):
                    # Create the decrypted file that the extraction will expect
                    decrypted_path_obj = Path(decrypted_path)
                    decrypted_path_obj.parent.mkdir(parents=True, exist_ok=True)
                    decrypted_path_obj.write_bytes(b"mock tar.gz content")
                    return None

                mock_decrypt.side_effect = mock_decrypt_side_effect

                result = await download_book_to_local(
                    "TEST123", mock_grin_client, "Harvard", storage_config, mock_progress_tracker
                )

                assert result[0] == "TEST123"

                # Verify operations were called
                mock_decrypt.assert_called_once()
                mock_file.write.assert_called()
                mock_file.flush.assert_called_once()

    @pytest.mark.asyncio
    async def test_sync_book_to_local_storage_missing_base_path(self, mock_grin_client, mock_progress_tracker):
        """Test local storage sync with missing base_path."""
        storage_config = {}  # No base_path

        with pytest.raises(ValueError) as exc_info:
            await download_book_to_local(
            "TEST123", mock_grin_client, "Harvard", storage_config, mock_progress_tracker
            )
        assert "Local storage requires" in str(exc_info.value)

    @pytest.mark.asyncio
    async def test_sync_book_to_local_storage_correct_path_construction(self, mock_grin_client, temp_db):
        """Test that local storage sync creates files under base_path, not at filesystem root."""
        import tempfile
        from pathlib import Path

        from grin_to_s3.collect_books.models import SQLiteProgressTracker

        # Create progress tracker with properly initialized database
        mock_progress_tracker = SQLiteProgressTracker(temp_db)

        with tempfile.TemporaryDirectory() as temp_dir:
            storage_config = {
                "base_path": temp_dir,
            }

            # Mock the HTTP call and GPG decryption
            with (
                aioresponses() as mock_http,
                patch("grin_to_s3.sync.operations.decrypt_gpg_file") as mock_decrypt,
                patch("pathlib.Path.unlink"),
                patch("grin_to_s3.sync.operations.extract_and_upload_ocr_text") as mock_extract_ocr,
                patch("grin_to_s3.sync.operations.extract_and_update_marc_metadata") as mock_extract_marc,
                patch("tarfile.open") as mock_tarfile,
            ):
                # Mock the GRIN download URL
                mock_http.get(
                    "https://books.google.com/libraries/Harvard/TEST123.tar.gz.gpg",
                    body=b"test archive content",
                    headers={"content-length": "20"}
                )

                # Mock extraction functions to prevent actual extraction
                mock_extract_ocr.return_value = []
                mock_extract_marc.return_value = []

                # Mock tarfile operations
                mock_tar = mock_tarfile.return_value.__enter__.return_value
                mock_tar.extractall = MagicMock()

                # Create a side effect for decrypt that creates the expected decrypted file
                def mock_decrypt_side_effect(encrypted_path, decrypted_path, *args, **kwargs):
                    # Create the decrypted file that the extraction will expect
                    decrypted_path_obj = Path(decrypted_path)
                    decrypted_path_obj.parent.mkdir(parents=True, exist_ok=True)
                    decrypted_path_obj.write_bytes(b"mock tar.gz content")
                    return None

                mock_decrypt.side_effect = mock_decrypt_side_effect

                # Track the file paths that aiofiles.open is called with
                opened_paths = []

                class MockAsyncFile:
                    def __init__(self, path):
                        opened_paths.append(str(path))
                        self.mock_file = AsyncMock()

                    async def __aenter__(self):
                        return self.mock_file

                    async def __aexit__(self, *args):
                        pass

                def mock_aiofiles_open(path, *args, **kwargs):
                    return MockAsyncFile(path)

                with patch("aiofiles.open", side_effect=mock_aiofiles_open):
                    result = await download_book_to_local(
                        "TEST123", mock_grin_client, "Harvard", storage_config, mock_progress_tracker
                    )

                    assert result[0] == "TEST123"

                    # Verify that opened paths are under base_path, not at filesystem root
                    assert len(opened_paths) >= 1, "Should have opened at least one file"
                    for path in opened_paths:
                        assert path.startswith(temp_dir), f"Path {path} should be under base_path {temp_dir}"
                        assert not path.startswith("/TEST123"), f"Path {path} should not start with /TEST123 (filesystem root)"

                    # Verify the specific path structure (fixed for Issue #139)
                    encrypted_path = opened_paths[0]  # First opened file should be encrypted
                    expected_encrypted_path = f"{temp_dir}/raw/TEST123/TEST123.tar.gz.gpg"
                    assert encrypted_path == expected_encrypted_path, f"Expected {expected_encrypted_path}, got {encrypted_path}"


class TestOCRExtractionIntegration:
    """Test OCR extraction integration in sync pipeline."""

    @pytest.fixture
    def test_decrypted_file(self):
        """Create a temporary test archive file."""
        with tempfile.NamedTemporaryFile(suffix=".tar.gz", delete=False) as f:
            test_file = Path(f.name)
        yield test_file
        test_file.unlink(missing_ok=True)

    @pytest.mark.asyncio
    async def test_extract_and_upload_ocr_text_success(
        self, mock_book_manager, mock_progress_tracker, mock_staging_manager, test_decrypted_file, caplog
    ):
        """Test successful OCR extraction and upload."""
        with (
            tempfile.TemporaryDirectory() as temp_dir,
            patch("grin_to_s3.sync.operations.extract_ocr_pages") as mock_extract,
            patch("grin_to_s3.sync.operations.time.time") as mock_time,
        ):
            # Setup staging manager with real temp directory
            temp_path = Path(temp_dir)
            mock_staging_manager.staging_dir = temp_path
            mock_staging_manager.staging_path = temp_path

            # Mock extraction success
            mock_extract.return_value = 342  # page count
            mock_time.return_value = 1672531200

            # Create mock JSONL file
            jsonl_file = temp_path / "TEST123_ocr_temp.jsonl"
            jsonl_file.write_text('{"page": 1, "text": "Test content"}\n')

            status_updates = await extract_and_upload_ocr_text(
                "TEST123", test_decrypted_file, mock_book_manager, mock_progress_tracker, mock_staging_manager
            )

            # Verify extraction was called
            mock_extract.assert_called_once()

            # Verify status updates were collected (starting, extracting, completed)
            assert len(status_updates) == 3
            assert status_updates[0].status_value == "starting"
            assert status_updates[1].status_value == "extracting"
            assert status_updates[2].status_value == "completed"

            # Verify upload was called
            mock_book_manager.save_ocr_text_jsonl_from_file.assert_called_once()

            # Verify success logging
            assert "[TEST123] Starting OCR text extraction from extracted directory" in caplog.text
            assert "[TEST123] Extracted 342 pages from archive" in caplog.text

            # Verify temporary JSONL file was cleaned up
            assert not jsonl_file.exists(), "Temporary JSONL file should be cleaned up after successful upload"


    @pytest.mark.asyncio
    async def test_extract_and_upload_ocr_text_upload_failure(
        self, mock_book_manager, mock_progress_tracker, mock_staging_manager, test_decrypted_file, caplog
    ):
        """Test OCR upload failure handling (non-blocking)."""
        with (
            tempfile.TemporaryDirectory() as temp_dir,
            patch("grin_to_s3.sync.operations.extract_ocr_pages") as mock_extract,
        ):
            # Setup staging manager with real temp directory
            temp_path = Path(temp_dir)
            mock_staging_manager.staging_dir = temp_path
            mock_staging_manager.staging_path = temp_path

            # Mock extraction success but upload failure
            mock_extract.return_value = 100
            mock_book_manager.save_ocr_text_jsonl_from_file.side_effect = Exception("Network timeout")

            # Create mock JSONL file
            jsonl_file = temp_path / "TEST123_ocr_temp.jsonl"
            jsonl_file.write_text('{"page": 1, "text": "Test"}\n')

            # This should not raise an exception (non-blocking)
            status_updates = await extract_and_upload_ocr_text(
                "TEST123", test_decrypted_file, mock_book_manager, mock_progress_tracker, mock_staging_manager
            )

            # Verify failure was tracked (starting, extracting, failed)
            assert len(status_updates) == 3
            assert status_updates[0].status_value == "starting"
            assert status_updates[1].status_value == "extracting"
            assert status_updates[2].status_value == "failed"

            # Verify failure was logged
            assert "OCR extraction failed but sync continues" in caplog.text

    @pytest.mark.asyncio
    async def test_extract_and_upload_ocr_text_no_db_tracker(
        self, mock_book_manager, mock_staging_manager, test_decrypted_file
    ):
        """Test OCR extraction without database tracker."""
        with (
            tempfile.TemporaryDirectory() as temp_dir,
            patch("grin_to_s3.sync.operations.extract_ocr_pages") as mock_extract,
        ):
            # Setup staging manager with real temp directory
            temp_path = Path(temp_dir)
            mock_staging_manager.staging_dir = temp_path
            mock_staging_manager.staging_path = temp_path

            mock_extract.return_value = 50

            # Create mock JSONL file
            jsonl_file = temp_path / "TEST123_ocr_temp.jsonl"
            jsonl_file.write_text('{"page": 1, "text": "Test"}\n')

            # Should work without database tracker
            await extract_and_upload_ocr_text(
                "TEST123", test_decrypted_file, mock_book_manager, None, mock_staging_manager
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
        self, skip_ocr, skip_marc, mock_storage_config, mock_staging_manager, temp_db
    ):
        """Test upload_book_from_staging with different extraction scenarios."""
        # Create real progress tracker with proper database
        progress_tracker = SQLiteProgressTracker(temp_db)

        # Set up properly configured mock staging manager
        mock_staging_manager.cleanup_files.return_value = 1024 * 1024  # Return int, not MagicMock

        with mock_upload_operations(skip_ocr=skip_ocr, skip_marc=skip_marc) as mocks:
            result = await upload_book_from_staging(
                "TEST123",
                "/staging/TEST123.tar.gz.gpg",
                "minio",
                mock_storage_config,
                mock_staging_manager,
                progress_tracker,
                "encrypted_etag_123",
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
    async def test_upload_book_from_staging_real_book_manager_initialization(self, storage_type):
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
            mock_staging_manager.get_extracted_directory_path.return_value = Path(temp_dir) / "TEST123_extracted"
            mock_staging_manager.staging_path = Path(temp_dir)
            mock_staging_manager.cleanup_files.return_value = 1024

            mock_progress_tracker = MagicMock()
            mock_progress_tracker.db_path = "/tmp/test.db"  # Fixed: provide proper string path
            mock_progress_tracker.update_sync_data = AsyncMock()

            # Create dummy files for encryption and decryption
            encrypted_file = Path(temp_dir) / "TEST123.tar.gz.gpg"
            encrypted_file.write_text("dummy encrypted content")

            decrypted_file = Path(temp_dir) / "TEST123.tar.gz"
            decrypted_file.write_text("dummy decrypted content")

            with mock_upload_operations() as mocks:
                # Configure specific storage behavior for this test
                mocks.book_manager_class.return_value.save_decrypted_archive_from_file = AsyncMock(
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
                    None,  # secrets_dir
                    skip_extract_ocr=True,  # Skip OCR to focus on BookStorage init
                )

                # Verify successful completion
                assert result["status"] == "completed"
                assert result["barcode"] == "TEST123"


class TestDiskSpaceHandling:
    """Test disk space management functionality."""

    @pytest.mark.asyncio
    async def test_staging_manager_wait_for_disk_space_available(self):
        """Test wait_for_disk_space when space is already available."""
        from grin_to_s3.storage.staging import StagingDirectoryManager

        with tempfile.TemporaryDirectory() as temp_dir:
            staging_manager = StagingDirectoryManager(temp_dir, capacity_threshold=0.9)

            # Mock check_disk_space to return True (space available)
            with patch.object(staging_manager, "check_disk_space", return_value=True):
                # This should complete immediately without waiting
                await staging_manager.wait_for_disk_space()
                # If we get here without timeout, the test passes

    @pytest.mark.asyncio
    async def test_staging_manager_wait_for_disk_space_becomes_available(self):
        """Test wait_for_disk_space when space becomes available after waiting."""
        from grin_to_s3.storage.staging import StagingDirectoryManager

        with tempfile.TemporaryDirectory() as temp_dir:
            staging_manager = StagingDirectoryManager(temp_dir, capacity_threshold=0.9)

            # Track number of check_disk_space calls
            call_count = 0
            def mock_check_disk_space(*args, **kwargs):
                nonlocal call_count
                call_count += 1
                # Return False first 2 times, then True (space becomes available)
                return call_count > 2

            with patch.object(staging_manager, "check_disk_space", side_effect=mock_check_disk_space):
                with patch.object(staging_manager, "get_disk_usage", return_value=(800e9, 1000e9, 0.8)):
                    # This should wait briefly then succeed
                    await staging_manager.wait_for_disk_space(check_interval=0.1)

            # Verify it checked multiple times before succeeding
            assert call_count > 2

    @pytest.mark.asyncio
    async def test_download_book_mid_download_space_exhaustion(self, mock_grin_client, mock_progress_tracker):
        """Test download behavior when disk space is exhausted mid-download."""
        from grin_to_s3.storage.staging import StagingDirectoryManager

        with tempfile.TemporaryDirectory() as temp_dir:
            staging_manager = StagingDirectoryManager(temp_dir, capacity_threshold=0.9)

            # Track disk space check calls during download
            check_calls = 0
            def mock_check_disk_space(*args, **kwargs):
                nonlocal check_calls
                check_calls += 1
                # Fail on second check (simulating space exhaustion mid-download)
                return check_calls == 1

            def mock_wait_for_disk_space(*args, **kwargs):
                # Simulate that space becomes available after cleanup
                nonlocal check_calls
                check_calls = 0  # Reset for retry
                return asyncio.sleep(0.01)

            with patch.object(staging_manager, "check_disk_space", side_effect=mock_check_disk_space):
                with patch.object(staging_manager, "wait_for_disk_space", side_effect=mock_wait_for_disk_space):
                    with aioresponses() as m:
                        # Mock large download that will trigger periodic space checks
                        large_content = b"x" * (60 * 1024 * 1024)  # 60MB to trigger check at 50MB
                        m.get("https://books.google.com/libraries/Harvard/TEST123.tar.gz.gpg", body=large_content)

                        mock_grin_client.auth.make_authenticated_request = AsyncMock()

                        # Configure the mock session response
                        mock_response = AsyncMock()

                        class AsyncIterator:
                            def __init__(self, data):
                                self.data = data
                                self.yielded = False

                            def __aiter__(self):
                                return self

                            async def __anext__(self):
                                if not self.yielded:
                                    self.yielded = True
                                    return self.data
                                raise StopAsyncIteration

                        mock_response.content.iter_chunked = lambda chunk_size: AsyncIterator(large_content)
                        mock_grin_client.auth.make_authenticated_request.return_value = mock_response

                        # This should trigger the mid-download space check and retry
                        result = await download_book_to_filesystem(
                            "TEST123",
                            mock_grin_client,
                            "Harvard",
                            "etag123",
                            staging_manager=staging_manager
                        )

                        # Should eventually succeed after retry
                        assert result[0] == "TEST123"  # barcode
                        assert Path(result[1]).exists()  # staging file created

    @pytest.mark.asyncio
    async def test_staging_manager_wait_for_disk_space_timeout(self):
        """Test wait_for_disk_space timeout when space never becomes available."""
        from grin_to_s3.storage.staging import DiskSpaceError, StagingDirectoryManager

        with tempfile.TemporaryDirectory() as temp_dir:
            staging_manager = StagingDirectoryManager(temp_dir, capacity_threshold=0.9)

            # Mock check_disk_space to always return False (no space)
            with patch.object(staging_manager, "check_disk_space", return_value=False):
                with patch.object(staging_manager, "get_disk_usage", return_value=(950e9, 1000e9, 0.95)):
                    # This should timeout after 2 seconds
                    with pytest.raises(DiskSpaceError, match="Timed out waiting for disk space after 2 seconds"):
                        await staging_manager.wait_for_disk_space(timeout=2, check_interval=0.5)
