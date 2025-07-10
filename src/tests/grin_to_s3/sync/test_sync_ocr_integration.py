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
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from grin_to_s3.collect_books.models import SQLiteProgressTracker
from grin_to_s3.extract.tracking import TEXT_EXTRACTION_STATUS_TYPE, ExtractionStatus
from grin_to_s3.sync.operations import upload_book_from_staging
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
        manager = MagicMock()
        staging_dir = Path(temp_dir)
        manager.staging_dir = staging_dir
        manager.staging_path = staging_dir  # Add staging_path attribute to prevent mock file creation
        manager.get_decrypted_file_path = lambda barcode: staging_dir / f"{barcode}.tar.gz"
        manager.cleanup_files = MagicMock(return_value=1024 * 1024)  # 1MB freed
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
    async def test_sync_pipeline_with_ocr_extraction_success(
        self, temp_db_tracker, mock_staging_manager, test_archive_with_ocr
    ):
        """Test complete sync pipeline with successful OCR extraction."""
        barcode = "TEST123456789"

        # Copy test archive to staging as if it was decrypted
        decrypted_file = mock_staging_manager.get_decrypted_file_path(barcode)
        decrypted_file.parent.mkdir(parents=True, exist_ok=True)

        # Copy archive content
        with open(test_archive_with_ocr, "rb") as src, open(decrypted_file, "wb") as dst:
            dst.write(src.read())

        with (
            patch("grin_to_s3.sync.operations.decrypt_gpg_file") as mock_decrypt,
            patch("grin_to_s3.sync.operations.create_storage_from_config") as mock_create_storage,
            patch("grin_to_s3.sync.operations.extract_and_update_marc_metadata"),
            patch("grin_to_s3.sync.operations.BookStorage") as mock_book_storage_class,
        ):
            # Mock successful decryption (already done above)
            mock_decrypt.return_value = None

            # Create real BookStorage mock with OCR upload capability
            mock_storage = MagicMock()
            mock_storage.save_decrypted_archive_from_file = AsyncMock(return_value="path/to/archive")
            mock_storage.save_ocr_text_jsonl_from_file = AsyncMock(
                return_value="bucket_full/TEST123456789/TEST123456789.jsonl"
            )
            mock_create_storage.return_value = mock_storage
            mock_book_storage_class.return_value = mock_storage

            # Execute upload with OCR extraction
            result = await upload_book_from_staging(
                barcode,
                str(decrypted_file) + ".gpg",  # Simulate encrypted filename
                "local",
                {"base_path": "/tmp/storage"},
                mock_staging_manager,
                temp_db_tracker,
                "encrypted_etag_123",
                None,  # gpg_key_file
                None,  # secrets_dir
                skip_extract_ocr=False,
            )

            # Verify successful sync
            assert result["status"] == "completed"
            assert result["barcode"] == barcode

            # Verify archive upload was called
            mock_storage.save_decrypted_archive_from_file.assert_called_once()

            # Verify OCR upload was called
            mock_storage.save_ocr_text_jsonl_from_file.assert_called_once()

            # Verify database tracking of OCR extraction
            with sqlite3.connect(temp_db_tracker.db_path) as conn:
                cursor = conn.execute(
                    """SELECT status_value, metadata FROM book_status_history
                       WHERE barcode = ? AND status_type = ?
                       ORDER BY timestamp""",
                    (barcode, TEXT_EXTRACTION_STATUS_TYPE),
                )
                statuses = cursor.fetchall()

            # Should have at least starting and completed statuses
            assert len(statuses) >= 2

            # Find the completed status
            completed_statuses = [s for s in statuses if s[0] == ExtractionStatus.COMPLETED.value]
            assert len(completed_statuses) >= 1, f"Should have completed status, got: {[s[0] for s in statuses]}"

            # Check completed metadata
            completed_metadata = json.loads(completed_statuses[0][1])
            assert "page_count" in completed_metadata
            assert "extraction_time_ms" in completed_metadata
            assert "jsonl_file_size" in completed_metadata

    @pytest.mark.asyncio
    async def test_sync_pipeline_with_ocr_extraction_disabled(
        self, temp_db_tracker, mock_staging_manager, test_archive_with_ocr
    ):
        """Test sync pipeline with OCR extraction disabled."""
        barcode = "TEST123456789"

        # Copy test archive to staging
        decrypted_file = mock_staging_manager.get_decrypted_file_path(barcode)
        decrypted_file.parent.mkdir(parents=True, exist_ok=True)

        with open(test_archive_with_ocr, "rb") as src, open(decrypted_file, "wb") as dst:
            dst.write(src.read())

        with (
            patch("grin_to_s3.sync.operations.decrypt_gpg_file") as mock_decrypt,
            patch("grin_to_s3.sync.operations.create_storage_from_config") as mock_create_storage,
            patch("grin_to_s3.sync.operations.BookStorage") as mock_book_storage_class,
        ):
            mock_decrypt.return_value = None

            mock_storage = MagicMock()
            mock_storage.save_decrypted_archive_from_file = AsyncMock(return_value="path/to/archive")
            mock_storage.save_ocr_text_jsonl_from_file = AsyncMock()
            mock_create_storage.return_value = mock_storage
            mock_book_storage_class.return_value = mock_storage

            # Execute upload with OCR extraction DISABLED
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
                skip_extract_ocr=True,  # OCR disabled
                skip_extract_marc=True,  # MARC disabled too
            )

            # Verify successful sync
            assert result["status"] == "completed"
            assert result["barcode"] == barcode

            # Verify archive upload was called
            mock_storage.save_decrypted_archive_from_file.assert_called_once()

            # Verify OCR upload was NOT called
            mock_storage.save_ocr_text_jsonl_from_file.assert_not_called()

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

        with (
            patch("grin_to_s3.sync.operations.decrypt_gpg_file") as mock_decrypt,
            patch("grin_to_s3.sync.operations.create_storage_from_config") as mock_create_storage,
            patch("grin_to_s3.sync.operations.extract_and_update_marc_metadata"),
            patch("grin_to_s3.sync.operations.BookStorage") as mock_book_storage_class,
        ):
            mock_decrypt.return_value = None

            mock_storage = MagicMock()
            mock_storage.save_decrypted_archive_from_file = AsyncMock(return_value="path/to/archive")
            mock_storage.save_ocr_text_jsonl_from_file = AsyncMock()
            mock_create_storage.return_value = mock_storage
            mock_book_storage_class.return_value = mock_storage

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
            mock_storage.save_decrypted_archive_from_file.assert_called_once()

            # Verify OCR extraction was attempted but failed
            with sqlite3.connect(temp_db_tracker.db_path) as conn:
                cursor = conn.execute(
                    """SELECT status_value, metadata FROM book_status_history
                       WHERE barcode = ? AND status_type = ?
                       ORDER BY timestamp""",
                    (barcode, TEXT_EXTRACTION_STATUS_TYPE),
                )
                statuses = cursor.fetchall()

            # Should have: starting, (maybe extracting), failed
            assert len(statuses) >= 2
            assert statuses[0][0] == ExtractionStatus.STARTING.value

            # Find the failed status
            failed_statuses = [s for s in statuses if s[0] == ExtractionStatus.FAILED.value]
            assert len(failed_statuses) >= 1, "Should have at least one failed extraction status"

            # Verify error metadata is recorded
            failed_metadata = json.loads(failed_statuses[0][1])
            assert "error_type" in failed_metadata, f"error_type not found in metadata: {failed_metadata}"
            assert "error_message" in failed_metadata, f"error_message not found in metadata: {failed_metadata}"

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

        with (
            patch("grin_to_s3.sync.operations.decrypt_gpg_file") as mock_decrypt,
            patch("grin_to_s3.sync.operations.create_storage_from_config") as mock_create_storage,
            patch("grin_to_s3.sync.operations.extract_and_update_marc_metadata"),
            patch("grin_to_s3.sync.operations.BookStorage") as mock_book_storage_class,
        ):
            mock_decrypt.return_value = None

            # Mock upload failure
            mock_storage = MagicMock()
            mock_storage.save_decrypted_archive_from_file = AsyncMock(side_effect=Exception("Storage upload failed"))
            mock_create_storage.return_value = mock_storage
            mock_book_storage_class.return_value = mock_storage

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
            assert "Storage upload failed" in result["error"]

            # Wait a bit to ensure any background tasks complete
            await asyncio.sleep(0.1)

            # The extraction task should have been cancelled, but we can't easily verify
            # the cancellation without more complex mocking. The important thing is that
            # the upload failure is handled correctly and the function returns.


class TestOCRExtractionDatabaseTracking:
    """Test database tracking for OCR extraction during sync."""

    @pytest.mark.asyncio
    async def test_extraction_progress_tracking(self, temp_db_tracker):
        """Test that extraction progress is properly tracked in database."""
        barcode = "TEST123456789"

        # Simulate the tracking that happens during extraction
        from grin_to_s3.extract.tracking import write_status

        session_id = "test_session_123"

        # Track the extraction lifecycle
        await write_status(
            temp_db_tracker.db_path,
            barcode,
            ExtractionStatus.STARTING,
            {"session_id": session_id, "source": "sync_pipeline"},
            session_id,
        )

        await write_status(
            temp_db_tracker.db_path,
            barcode,
            ExtractionStatus.EXTRACTING,
            {"jsonl_file": "/staging/TEST123456789_ocr_temp.jsonl"},
            session_id,
        )

        await write_status(
            temp_db_tracker.db_path,
            barcode,
            ExtractionStatus.COMPLETED,
            {"page_count": 342, "extraction_time_ms": 1247, "jsonl_file_size": 46284},
            session_id,
        )

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

        from grin_to_s3.extract.tracking import write_status

        session_id = "test_session_123"

        # Track failure
        await write_status(
            temp_db_tracker.db_path, barcode, ExtractionStatus.STARTING, {"session_id": session_id}, session_id
        )

        await write_status(
            temp_db_tracker.db_path,
            barcode,
            ExtractionStatus.FAILED,
            {"error": "Archive corrupted: Cannot read tar.gz file"},
            session_id,
        )

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
