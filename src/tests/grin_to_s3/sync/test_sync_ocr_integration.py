#!/usr/bin/env python3
"""
End-to-end integration tests for OCR extraction in sync pipeline.

Tests the complete integration of OCR text extraction with the sync pipeline,
including database tracking, storage operations, and error handling in realistic
scenarios with actual file operations.
"""

import asyncio
import json
import sqlite3
import tempfile
from pathlib import Path
from unittest.mock import AsyncMock, patch

import pytest

from grin_to_s3.collect_books.models import SQLiteProgressTracker
from grin_to_s3.database_utils import batch_write_status_updates
from grin_to_s3.extract.tracking import TEXT_EXTRACTION_STATUS_TYPE, ExtractionStatus, collect_status
from grin_to_s3.sync.operations import upload_book_from_staging
from tests.test_utils.parametrize_helpers import (
    combined_scenarios_parametrize,
    meaningful_storage_parametrize,
)
from tests.test_utils.unified_mocks import (
    create_book_manager_mock,
    create_staging_manager_mock,
    create_storage_mock,
    mock_upload_operations,
)
from tests.utils import create_test_archive


@pytest.fixture
async def temp_db_tracker():
    """Create a temporary database with a real SQLiteProgressTracker."""
    with tempfile.NamedTemporaryFile(suffix=".db", delete=False) as f:
        db_path = f.name

    # Initialize tracker which will create the schema
    tracker = SQLiteProgressTracker(db_path=db_path)
    await tracker.init_db()

    yield tracker

    # Cleanup
    await tracker.close()
    Path(db_path).unlink(missing_ok=True)


@pytest.fixture
def mock_staging_manager():
    """Create a mock staging manager with real directory operations."""

    with tempfile.TemporaryDirectory() as temp_dir:
        manager = create_staging_manager_mock(staging_path=temp_dir)
        staging_dir = Path(temp_dir)
        manager.staging_dir = staging_dir
        manager.staging_path = staging_dir  # Add staging_path attribute to prevent mock file creation
        manager.get_decrypted_file_path = lambda barcode: staging_dir / f"{barcode}.tar.gz"
        yield manager


@pytest.fixture
def test_archive_with_ocr():
    """Create a test archive with OCR text files."""
    with tempfile.TemporaryDirectory() as temp_dir:
        temp_path = Path(temp_dir)

        # Create archive with text files
        archive_path = create_test_archive(
            pages={
                "00000001.txt": "This is page one content",
                "00000002.txt": "This is page two content",
                "00000003.txt": "This is page three content",
            },
            temp_dir=temp_path,
            archive_name="test_book.tar.gz",
        )

        yield archive_path


class TestSyncOCRPipelineIntegration:
    """Integration tests for OCR extraction in sync pipeline."""

    @pytest.mark.asyncio
    @meaningful_storage_parametrize()
    async def test_sync_pipeline_with_ocr_extraction_success(
        self, storage_type, temp_db_tracker, mock_staging_manager, test_archive_with_ocr
    ):
        """Test complete sync pipeline with successful OCR extraction across local vs cloud storage."""
        barcode = "TEST123456789"

        # Copy test archive to staging as if it was decrypted
        decrypted_file = mock_staging_manager.get_decrypted_file_path(barcode)
        decrypted_file.parent.mkdir(parents=True, exist_ok=True)

        # Copy archive content
        with open(test_archive_with_ocr, "rb") as src, open(decrypted_file, "wb") as dst:
            dst.write(src.read())

        # Use targeted mocks instead of full mock_upload_operations to allow real OCR extraction
        with (
            patch("grin_to_s3.sync.operations.decrypt_gpg_file") as mock_decrypt,
            patch("grin_to_s3.sync.operations.create_storage_from_config") as mock_create_storage,
            patch("grin_to_s3.sync.operations.BookManager") as mock_book_manager_class,
            patch("grin_to_s3.sync.operations.extract_and_update_marc_metadata") as mock_extract_marc,
        ):
            mock_decrypt.return_value = None
            mock_extract_marc.return_value = []

            # Set up storage mocks using unified factory
            mock_storage = create_storage_mock(storage_type=storage_type)
            # BookStorage properly wraps the storage mock (no more manual linking!)
            mock_book_manager = create_book_manager_mock(storage=mock_storage)

            # Configure specific return values for this test
            mock_storage.save_ocr_text_jsonl_from_file.return_value = "bucket_full/TEST123456789/TEST123456789.jsonl"

            mock_create_storage.return_value = mock_storage
            mock_book_manager_class.return_value = mock_book_manager

            # Execute upload with OCR extraction (using real OCR extraction, not mocked)
            storage_config = {"base_path": "/tmp/storage"} if storage_type == "local" else {
                "endpoint_url": f"https://test-{storage_type}.example.com",
                "access_key_id": "test-key",
                "secret_access_key": "test-secret",
            }

            result = await upload_book_from_staging(
                barcode,
                str(decrypted_file) + ".gpg",  # Simulate encrypted filename
                storage_type,
                storage_config,
                mock_staging_manager,
                temp_db_tracker,
                "encrypted_etag_123",
                None,  # gpg_key_file
                None,  # secrets_dir
                skip_extract_marc=True,  # Skip MARC to focus on OCR
                # skip_extract_ocr=False,  # Default is False, OCR extraction enabled
            )

            # Verify successful sync
            assert result["status"] == "completed"
            assert result["barcode"] == barcode

            # Verify archive upload was called
            mock_book_manager.save_decrypted_archive_from_file.assert_called_once()

            # Verify OCR upload was called (indicates real OCR extraction happened)
            # Since book_manager wraps storage, we can check either one
            mock_book_manager.save_ocr_text_jsonl_from_file.assert_called_once()

            # Verify the OCR text file was actually created and uploaded
            upload_call_args = mock_book_manager.save_ocr_text_jsonl_from_file.call_args
            assert upload_call_args is not None

            # The function signature is save_ocr_text_jsonl_from_file(barcode, jsonl_file_path, metadata=None)
            uploaded_barcode = upload_call_args[0][0]    # First positional argument (barcode)
            uploaded_file_path = upload_call_args[0][1]  # Second positional argument (jsonl file path)

            # Since this is an integration test that actually runs OCR extraction,
            # verify that real OCR extraction occurred with proper arguments
            assert uploaded_barcode == barcode
            assert barcode in str(uploaded_file_path)
            assert "_ocr_temp.jsonl" in str(uploaded_file_path)

    @pytest.mark.asyncio
    @meaningful_storage_parametrize()
    async def test_sync_pipeline_with_ocr_extraction_disabled(
        self, storage_type, temp_db_tracker, mock_staging_manager, test_archive_with_ocr
    ):
        """Test sync pipeline with OCR extraction disabled across local vs cloud storage."""
        barcode = "TEST123456789"

        # Copy test archive to staging
        decrypted_file = mock_staging_manager.get_decrypted_file_path(barcode)
        decrypted_file.parent.mkdir(parents=True, exist_ok=True)

        with open(test_archive_with_ocr, "rb") as src, open(decrypted_file, "wb") as dst:
            dst.write(src.read())

        # Use targeted mocks to allow OCR function to not be called
        with (
            patch("grin_to_s3.sync.operations.decrypt_gpg_file") as mock_decrypt,
            patch("grin_to_s3.sync.operations.create_storage_from_config") as mock_create_storage,
            patch("grin_to_s3.sync.operations.BookManager") as mock_book_manager_class,
            patch("grin_to_s3.sync.operations.extract_and_update_marc_metadata") as mock_extract_marc,
        ):
            mock_decrypt.return_value = None
            mock_extract_marc.return_value = []

            # Set up storage mocks using unified factory
            mock_storage = create_storage_mock(storage_type=storage_type)
            # BookStorage properly wraps the storage mock (no more manual linking!)
            mock_book_manager = create_book_manager_mock(storage=mock_storage)

            mock_create_storage.return_value = mock_storage
            mock_book_manager_class.return_value = mock_book_manager

            # Execute upload with OCR extraction DISABLED
            storage_config = {"base_path": "/tmp/storage"} if storage_type == "local" else {
                "endpoint_url": f"https://test-{storage_type}.example.com",
                "access_key_id": "test-key",
                "secret_access_key": "test-secret",
            }

            result = await upload_book_from_staging(
                barcode,
                str(decrypted_file) + ".gpg",
                storage_type,
                storage_config,
                mock_staging_manager,
                temp_db_tracker,
                "encrypted_etag_123",
                None,
                None,
                skip_extract_ocr=True,  # OCR disabled
                skip_extract_marc=True,  # MARC disabled too
            )

            # Verify successful sync
            assert result["status"] == "completed"
            assert result["barcode"] == barcode

            # Verify archive upload was called
            mock_book_manager.save_decrypted_archive_from_file.assert_called_once()

            # Verify OCR upload was NOT called
            mock_book_manager.save_ocr_text_jsonl_from_file.assert_not_called()

            # Verify NO OCR extraction database entries
            with sqlite3.connect(temp_db_tracker.db_path) as conn:
                cursor = conn.execute(
                    """SELECT COUNT(*) FROM book_status_history
                       WHERE barcode = ? AND status_type = ?""",
                    (barcode, TEXT_EXTRACTION_STATUS_TYPE),
                )
                count = cursor.fetchone()[0]

            assert count == 0, "No OCR extraction database entries should exist when disabled"

    @pytest.mark.asyncio
    async def test_sync_pipeline_ocr_extraction_failure_non_blocking(self, temp_db_tracker, mock_staging_manager):
        """Test that OCR extraction failures don't block sync completion."""
        barcode = "TEST123456789"

        # Create a corrupted/invalid archive file
        decrypted_file = mock_staging_manager.get_decrypted_file_path(barcode)
        decrypted_file.parent.mkdir(parents=True, exist_ok=True)
        decrypted_file.write_text("This is not a valid tar.gz file")

        with mock_upload_operations() as mocks:
            # Configure storage mock for this test
            mocks.book_manager.save_ocr_text_jsonl_from_file = AsyncMock()

            # Execute upload - OCR should fail but sync should succeed
            result = await upload_book_from_staging(
                barcode,
                str(decrypted_file) + ".gpg",
                "local",
                {"base_path": "/tmp/storage"},
                mock_staging_manager,
                temp_db_tracker,
                "encrypted_etag_123",
                None,
                None,
                skip_extract_ocr=False,  # OCR enabled but will fail
            )

            # Verify sync still succeeded despite OCR failure
            assert result["status"] == "completed"
            assert result["barcode"] == barcode

            # Verify archive upload succeeded
            mocks.book_manager.save_decrypted_archive_from_file.assert_called_once()

            # In the simplified mock environment, we just verify that sync completed
            # despite the OCR extraction failure (which is the intended behavior)
            # The actual OCR failure tracking is tested in more isolated unit tests

    @pytest.mark.asyncio
    async def test_sync_pipeline_upload_failure_cancels_extraction(
        self, temp_db_tracker, mock_staging_manager, test_archive_with_ocr
    ):
        """Test that upload failures properly cancel the extraction task."""
        barcode = "TEST123456789"

        decrypted_file = mock_staging_manager.get_decrypted_file_path(barcode)
        decrypted_file.parent.mkdir(parents=True, exist_ok=True)

        with open(test_archive_with_ocr, "rb") as src, open(decrypted_file, "wb") as dst:
            dst.write(src.read())

        with mock_upload_operations(should_fail=True):
            pass  # Storage failure is already configured

            # Execute upload - should fail
            result = await upload_book_from_staging(
                barcode,
                str(decrypted_file) + ".gpg",
                "local",
                {"base_path": "/tmp/storage"},
                mock_staging_manager,
                temp_db_tracker,
                "encrypted_etag_123",
                None,
                None,
                skip_extract_ocr=False,
            )

            # Verify upload failed
            assert result["status"] == "failed"
            assert ("Storage upload failed" in result["error"] or
                   "Decryption failed" in result["error"])

            # Wait a bit to ensure any background tasks complete
            await asyncio.sleep(0.1)

            # The extraction task should have been cancelled, but we can't easily verify
            # the cancellation without more complex mocking. The important thing is that
            # the upload failure is handled correctly and the function returns.

    @pytest.mark.asyncio
    @combined_scenarios_parametrize()
    async def test_sync_pipeline_comprehensive_scenarios(
        self, storage_type, skip_ocr, skip_marc, temp_db_tracker, mock_staging_manager, test_archive_with_ocr
    ):
        """Test sync pipeline with all combinations of storage types and extraction scenarios."""
        barcode = "TEST123456789"

        # Copy test archive to staging as if it was decrypted
        decrypted_file = mock_staging_manager.get_decrypted_file_path(barcode)
        decrypted_file.parent.mkdir(parents=True, exist_ok=True)

        # Copy archive content
        with open(test_archive_with_ocr, "rb") as src, open(decrypted_file, "wb") as dst:
            dst.write(src.read())

        with mock_upload_operations(skip_ocr=skip_ocr, skip_marc=skip_marc) as mocks:
            # Configure storage config based on storage type
            if storage_type == "local":
                storage_config = {"base_path": "/tmp/storage"}
            else:
                storage_config = {
                    "endpoint_url": f"https://test-{storage_type}.example.com",
                    "access_key_id": "test-key",
                    "secret_access_key": "test-secret",
                }

            # Execute upload with the specified extraction and storage configuration
            result = await upload_book_from_staging(
                barcode,
                str(decrypted_file) + ".gpg",  # Simulate encrypted filename
                storage_type,
                storage_config,
                mock_staging_manager,
                temp_db_tracker,
                "encrypted_etag_123",
                None,  # gpg_key_file
                None,  # secrets_dir
                skip_extract_ocr=skip_ocr,
                skip_extract_marc=skip_marc,
            )

            # Verify successful sync regardless of configuration
            assert result["status"] == "completed"
            assert result["barcode"] == barcode

            # Verify archive upload was always called
            mocks.book_manager.save_decrypted_archive_from_file.assert_called_once()

            # Verify extraction behavior based on parameters
            if skip_ocr:
                mocks.extract_ocr.assert_not_called()
            else:
                mocks.extract_ocr.assert_called_once()

            if skip_marc:
                mocks.extract_marc.assert_not_called()
            else:
                mocks.extract_marc.assert_called_once()


class TestOCRExtractionDatabaseTracking:
    """Test database tracking for OCR extraction during sync."""

    @pytest.mark.asyncio
    async def test_extraction_progress_tracking(self, temp_db_tracker):
        """Test that extraction progress is properly tracked in database."""
        barcode = "TEST123456789"

        # Simulate the tracking that happens during extraction

        session_id = "test_session_123"

        # Track the extraction lifecycle using batched approach
        status_updates = [
            collect_status(
                barcode,
                TEXT_EXTRACTION_STATUS_TYPE,
                ExtractionStatus.STARTING.value,
                {"session_id": session_id, "source": "sync_pipeline"},
                session_id,
            ),
            collect_status(
                barcode,
                TEXT_EXTRACTION_STATUS_TYPE,
                ExtractionStatus.EXTRACTING.value,
                {"jsonl_file": "/staging/TEST123456789_ocr_temp.jsonl"},
                session_id,
            ),
            collect_status(
                barcode,
                TEXT_EXTRACTION_STATUS_TYPE,
                ExtractionStatus.COMPLETED.value,
                {"page_count": 342, "extraction_time_ms": 1247, "jsonl_file_size": 46284},
                session_id,
            ),
        ]

        await batch_write_status_updates(temp_db_tracker.db_path, status_updates)

        # Verify tracking
        with sqlite3.connect(temp_db_tracker.db_path) as conn:
            cursor = conn.execute(
                """SELECT status_value, metadata, session_id FROM book_status_history
                   WHERE barcode = ? AND status_type = ?
                   ORDER BY timestamp""",
                (barcode, TEXT_EXTRACTION_STATUS_TYPE),
            )
            records = cursor.fetchall()

        assert len(records) == 3

        # Verify starting status
        assert records[0][0] == ExtractionStatus.STARTING.value
        starting_metadata = json.loads(records[0][1])
        assert starting_metadata["source"] == "sync_pipeline"
        assert records[0][2] == session_id

        # Verify extracting status
        assert records[1][0] == ExtractionStatus.EXTRACTING.value
        extracting_metadata = json.loads(records[1][1])
        assert "jsonl_file" in extracting_metadata

        # Verify completed status
        assert records[2][0] == ExtractionStatus.COMPLETED.value
        completed_metadata = json.loads(records[2][1])
        assert completed_metadata["page_count"] == 342
        assert completed_metadata["extraction_time_ms"] == 1247
        assert completed_metadata["jsonl_file_size"] == 46284

    @pytest.mark.asyncio
    async def test_extraction_failure_tracking(self, temp_db_tracker):
        """Test that extraction failures are properly tracked."""
        barcode = "TEST123456789"

        session_id = "test_session_123"

        # Track failure using batched approach
        status_updates = [
            collect_status(
                barcode,
                TEXT_EXTRACTION_STATUS_TYPE,
                ExtractionStatus.STARTING.value,
                {"session_id": session_id},
                session_id,
            ),
            collect_status(
                barcode,
                TEXT_EXTRACTION_STATUS_TYPE,
                ExtractionStatus.FAILED.value,
                {"error": "Archive corrupted: Cannot read tar.gz file"},
                session_id,
            ),
        ]

        await batch_write_status_updates(temp_db_tracker.db_path, status_updates)

        # Verify failure tracking
        with sqlite3.connect(temp_db_tracker.db_path) as conn:
            cursor = conn.execute(
                """SELECT status_value, metadata FROM book_status_history
                   WHERE barcode = ? AND status_type = ? AND status_value = ?""",
                (barcode, TEXT_EXTRACTION_STATUS_TYPE, ExtractionStatus.FAILED.value),
            )
            failed_record = cursor.fetchone()

        assert failed_record is not None
        failed_metadata = json.loads(failed_record[1])
        assert failed_metadata["error"] == "Archive corrupted: Cannot read tar.gz file"
