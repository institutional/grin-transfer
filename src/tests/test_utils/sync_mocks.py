"""Centralized mock utilities for sync operation tests."""

from contextlib import contextmanager
from dataclasses import dataclass
from pathlib import Path
from typing import Any
from unittest.mock import AsyncMock, MagicMock, patch


@dataclass
class SyncOperationMocks:
    """Bundle of mocks for sync operations."""

    decrypt: MagicMock
    create_storage: MagicMock
    extract_ocr: MagicMock
    extract_marc: MagicMock
    book_storage_class: MagicMock


@contextmanager
def mock_upload_operations(
    skip_ocr: bool = False,
    skip_marc: bool = False,
    storage_config: dict[str, Any] | None = None,
    should_fail: bool = False,
):
    """
    Context manager for mocking upload_book_from_staging dependencies.

    Args:
        skip_ocr: If True, OCR extraction will be configured as disabled
        skip_marc: If True, MARC extraction will be configured as disabled
        storage_config: Optional configuration for mock storage objects
        should_fail: If True, configures mocks to simulate failures
    """
    with (
        patch("grin_to_s3.sync.operations.decrypt_gpg_file") as mock_decrypt,
        patch("grin_to_s3.sync.operations.create_storage_from_config") as mock_create_storage,
        patch("grin_to_s3.sync.operations.extract_and_upload_ocr_text") as mock_extract_ocr,
        patch("grin_to_s3.sync.operations.extract_and_update_marc_metadata") as mock_extract_marc,
        patch("grin_to_s3.sync.operations.BookStorage") as mock_book_storage_class,
    ):
        # Pre-configure common mock behaviors
        if should_fail:
            mock_decrypt.side_effect = Exception("Decryption failed")
        else:
            mock_decrypt.return_value = None

        # Set up storage mocks
        mock_storage = create_mock_storage(storage_config or {}, should_fail=should_fail)
        mock_create_storage.return_value = mock_storage

        # Create a separate book storage mock that delegates to the main storage
        mock_book_storage = MagicMock()
        mock_book_storage.save_decrypted_archive_from_file = mock_storage.save_decrypted_archive_from_file
        mock_book_storage.save_ocr_text_jsonl_from_file = mock_storage.save_ocr_text_jsonl_from_file
        mock_book_storage_class.return_value = mock_book_storage

        # Configure extraction mocks
        if should_fail:
            mock_extract_ocr.side_effect = Exception("OCR extraction failed")
            mock_extract_marc.side_effect = Exception("MARC extraction failed")
        else:
            mock_extract_ocr.return_value = None
            mock_extract_marc.return_value = None

        # Store both storage mocks in the result for easy access
        mocks = SyncOperationMocks(
            decrypt=mock_decrypt,
            create_storage=mock_create_storage,
            extract_ocr=mock_extract_ocr,
            extract_marc=mock_extract_marc,
            book_storage_class=mock_book_storage_class,
        )
        # Add storage references for convenience
        mocks.storage = mock_storage
        mocks.book_storage = mock_book_storage

        yield mocks


# Import from specialized modules to avoid duplication


def create_mock_storage(config: dict[str, Any], should_fail: bool = False) -> MagicMock:
    """
    Create a properly configured mock storage object.

    Args:
        config: Configuration dictionary for customizing mock behavior
        should_fail: If True, configures storage operations to fail

    Returns:
        Configured MagicMock storage object
    """
    mock_storage = MagicMock()

    if should_fail:
        mock_storage.save_decrypted_archive_from_file = AsyncMock(
            side_effect=Exception("Storage upload failed")
        )
        mock_storage.save_ocr_text_jsonl_from_file = AsyncMock(
            side_effect=Exception("OCR upload failed")
        )
    else:
        mock_storage.save_decrypted_archive_from_file = AsyncMock(
            return_value=config.get("archive_path", "path/to/archive")
        )
        mock_storage.save_ocr_text_jsonl_from_file = AsyncMock(
            return_value=config.get("ocr_path", "bucket_full/TEST123/TEST123.jsonl")
        )

    return mock_storage


@contextmanager
def mock_pipeline_operations():
    """Context manager for mocking sync pipeline dependencies."""
    with (
        patch("grin_to_s3.sync.pipeline.SQLiteProgressTracker") as mock_tracker,
        patch("grin_to_s3.sync.pipeline.ProgressReporter") as mock_reporter,
        patch("grin_to_s3.sync.pipeline.GRINClient") as mock_client,
        patch("grin_to_s3.storage.StagingDirectoryManager") as mock_staging,
    ):
        # Pre-configure common behaviors
        tracker_instance = MagicMock()
        tracker_instance.get_books_for_sync = AsyncMock(return_value=[])
        tracker_instance.get_sync_stats = AsyncMock(
            return_value={"total_converted": 0, "synced": 0, "failed": 0, "pending": 0}
        )
        tracker_instance.add_status_change = AsyncMock()
        mock_tracker.return_value = tracker_instance

        reporter_instance = MagicMock()
        mock_reporter.return_value = reporter_instance

        client_instance = MagicMock()
        mock_client.return_value = client_instance

        staging_instance = MagicMock()
        staging_instance.staging_path = Path("/tmp/staging")
        staging_instance.get_staging_path = MagicMock(return_value=Path("/tmp/staging/test_file"))
        staging_instance.cleanup_file = AsyncMock()
        staging_instance.check_and_wait_for_space = AsyncMock()
        mock_staging.return_value = staging_instance

        yield {
            "tracker": tracker_instance,
            "reporter": reporter_instance,
            "client": client_instance,
            "staging": staging_instance,
        }


def create_mock_staging_manager(base_path: str = "/tmp/staging") -> MagicMock:
    """
    Create a properly configured mock staging manager.

    Args:
        base_path: Base path for staging operations

    Returns:
        Configured MagicMock staging manager
    """
    staging_manager = MagicMock()
    staging_manager.get_decrypted_file_path = MagicMock(
        return_value=Path(f"{base_path}/TEST123.tar.gz")
    )
    staging_manager.cleanup_files = MagicMock(return_value=1024 * 1024)  # 1MB
    staging_manager.get_staging_path = MagicMock(
        return_value=Path(f"{base_path}/test_file")
    )
    staging_manager.staging_path = Path(base_path)
    staging_manager.cleanup_file = AsyncMock()
    staging_manager.check_and_wait_for_space = AsyncMock()

    # Add any additional attributes that might be accessed
    staging_manager.staging_free_space_gb = 10.0  # 10GB free space
    staging_manager.min_free_space_gb = 1.0       # 1GB minimum

    return staging_manager


def create_mock_progress_tracker() -> MagicMock:
    """
    Create a properly configured mock progress tracker.

    Returns:
        Configured MagicMock progress tracker
    """
    tracker = MagicMock()
    tracker.add_status_change = AsyncMock()
    tracker.update_sync_data = AsyncMock()
    tracker.get_books_for_sync = AsyncMock(return_value=[])
    tracker.get_sync_stats = AsyncMock(
        return_value={"total_converted": 0, "synced": 0, "failed": 0, "pending": 0}
    )
    tracker.update_book_marc_metadata = AsyncMock()

    # Add database path for any operations that need it
    tracker.db_path = "/tmp/test.db"

    return tracker


@contextmanager
def mock_minimal_upload():
    """
    Minimal context manager for simple upload tests that don't need full mocking.

    This is useful for tests that just need to ensure upload_book_from_staging
    doesn't crash due to missing patches, but don't need to verify specific behaviors.
    """
    with (
        patch("grin_to_s3.sync.operations.extract_and_update_marc_metadata"),
        patch("grin_to_s3.sync.operations.extract_and_upload_ocr_text"),
    ):
        yield
