#!/usr/bin/env python3
"""
Integration tests for request processing with specific barcodes.

Tests that when running sync pipeline with specific barcodes that aren't available
in GRIN, the request processing loop is activated to trigger conversion requests.
"""

from unittest.mock import AsyncMock, MagicMock, patch

import aiohttp
import pytest

from grin_transfer.sync.tasks import check, request_conversion
from grin_transfer.sync.tasks.task_types import TaskAction, TaskType


@pytest.mark.asyncio
async def test_specific_barcode_triggers_conversion_request(mock_pipeline):
    """Specific barcode returning 404 should trigger REQUEST_CONVERSION task."""

    barcode = "TEST_BARCODE_404"

    # Mock GRIN client to return 404 for HEAD request
    mock_grin_client = AsyncMock()
    mock_grin_client.head_archive.side_effect = aiohttp.ClientResponseError(
        request_info=None, history=(), status=404, message="Not Found"
    )

    # Mock pipeline components
    mock_pipeline.grin_client = mock_grin_client
    mock_pipeline.library_directory = "test_library"
    mock_pipeline.secrets_dir = None
    mock_pipeline.conversion_requests_made = 0

    # Mock the book manager async methods
    mock_pipeline.book_manager.get_decrypted_archive_metadata = AsyncMock(return_value=None)
    mock_pipeline.book_manager.get_archive_etag_from_s3 = AsyncMock(return_value=None)

    # Test the CHECK task first
    check_result = await check.main(barcode, mock_pipeline)

    # Verify CHECK task failed with archive missing
    assert check_result.task_type == TaskType.CHECK
    assert check_result.action == TaskAction.FAILED
    assert check_result.reason == "fail_archive_missing"
    assert check_result.error == "Archive not available in GRIN"
    assert check_result.data["http_status_code"] == 404

    # Verify that CHECK task indicates no next tasks (failure means stop)
    next_tasks = check_result.next_tasks()
    assert next_tasks == [TaskType.REQUEST_CONVERSION]


@pytest.mark.asyncio
async def test_mixed_barcode_list_handles_available_and_missing(mock_pipeline):
    """Mixed list of barcodes should handle available and missing ones correctly."""

    # Mock pipeline components
    mock_grin_client = AsyncMock()
    mock_pipeline.grin_client = mock_grin_client
    mock_pipeline.library_directory = "test_library"

    # Mock book manager async methods
    mock_pipeline.book_manager.get_decrypted_archive_metadata = AsyncMock(return_value=None)
    mock_pipeline.book_manager.get_archive_etag_from_s3 = AsyncMock(return_value=None)

    # Test barcode that exists in GRIN (200 response)
    available_barcode = "AVAILABLE_BOOK"
    mock_grin_client.head_archive.return_value = MagicMock(
        status=200, headers={"ETag": '"abc123"', "Content-Length": "12345"}
    )

    available_result = await check.main(available_barcode, mock_pipeline)
    assert available_result.action == TaskAction.COMPLETED
    assert available_result.data["http_status_code"] == 200
    assert TaskType.REQUEST_CONVERSION not in available_result.next_tasks()

    # Test barcode that doesn't exist in GRIN (404 response)
    missing_barcode = "MISSING_BOOK"
    mock_grin_client.head_archive.side_effect = aiohttp.ClientResponseError(
        request_info=None, history=(), status=404, message="Not Found"
    )

    missing_result = await check.main(missing_barcode, mock_pipeline)
    assert missing_result.action == TaskAction.FAILED
    assert missing_result.reason == "fail_archive_missing"
    assert missing_result.error == "Archive not available in GRIN"
    assert missing_result.data["http_status_code"] == 404
    assert missing_result.next_tasks() == [TaskType.REQUEST_CONVERSION]


@pytest.mark.asyncio
async def test_barcode_already_in_storage_but_not_in_grin(mock_pipeline):
    """Barcode in storage but not in GRIN should be skipped (not request conversion)."""

    barcode = "IN_STORAGE_NOT_GRIN"

    # Mock GRIN client to return 404
    mock_grin_client = AsyncMock()
    mock_grin_client.head_archive.side_effect = aiohttp.ClientResponseError(
        request_info=None, history=(), status=404, message="Not Found"
    )
    mock_pipeline.grin_client = mock_grin_client

    # Mock book manager async methods - return existing etag (book exists in storage)
    mock_pipeline.book_manager.get_decrypted_archive_metadata = AsyncMock(
        return_value={"encrypted_etag": '"stored_etag"'}
    )
    mock_pipeline.book_manager.get_archive_etag_from_s3 = AsyncMock(return_value='"stored_etag"')

    result = await check.main(barcode, mock_pipeline)

    # Should be skipped, not failed (so no conversion request)
    assert result.action == TaskAction.SKIPPED
    assert result.reason == "skip_found_in_storage_not_grin"
    assert result.data["http_status_code"] == 404
    assert TaskType.REQUEST_CONVERSION not in result.next_tasks()


@pytest.mark.asyncio
async def test_request_conversion_updates_counter(mock_pipeline):
    """REQUEST_CONVERSION task should increment the conversion requests counter."""

    barcode = "TEST_COUNTER"
    mock_pipeline.conversion_requests_made = 5
    mock_pipeline.library_directory = "test_library"
    mock_pipeline.secrets_dir = None

    # Mock the grin_client.fetch_resource to simulate successful request
    mock_pipeline.grin_client.fetch_resource.return_value = "Barcode\tStatus\n" + barcode + "\tSuccess"

    result = await request_conversion.main(barcode, mock_pipeline)

    # Verify counter was incremented
    assert mock_pipeline.conversion_requests_made == 6
    assert result.data["request_count"] == 6


class TestSyncPipelineBarcodeValidation:
    """Test SyncPipeline barcode validation and dry-run behavior."""

    @pytest.mark.asyncio
    async def test_specific_barcodes_validates_against_database(self, sync_pipeline):
        """Specific barcodes should validate against database and create entries for missing ones."""
        # Mock the barcode validation methods on the real pipeline
        sync_pipeline.db_tracker.check_barcodes_exist = AsyncMock(return_value=({"EXIST001"}, {"MISSING001"}))
        sync_pipeline.db_tracker.create_empty_book_entries = AsyncMock()

        # Mock get_all_conversion_failed_books to avoid network call
        with patch("grin_transfer.sync.pipeline.get_all_conversion_failed_books") as mock_failed:
            mock_failed.return_value = {}

            # Mock filter_and_print_barcodes to return barcodes (avoid early return)
            with patch("grin_transfer.sync.pipeline.filter_and_print_barcodes") as mock_filter:
                mock_filter.return_value = ["EXIST001", "MISSING001"]

                # Mock process_books_with_queue to avoid actual processing
                with patch("grin_transfer.sync.pipeline.process_books_with_queue") as mock_process:
                    mock_process.return_value = {}

                    # Call setup_sync_loop with specific barcodes
                    await sync_pipeline.setup_sync_loop(
                        queues=[], specific_barcodes=["EXIST001", "MISSING001"], limit=None
                    )

        # Verify check_barcodes_exist was called with the right barcodes
        sync_pipeline.db_tracker.check_barcodes_exist.assert_called_once_with(["EXIST001", "MISSING001"])

        # Verify create_empty_book_entries was called with missing barcodes
        sync_pipeline.db_tracker.create_empty_book_entries.assert_called_once_with(["MISSING001"])

    @pytest.mark.asyncio
    async def test_dry_run_skips_database_entry_creation(self, sync_pipeline):
        """Dry-run mode should skip creating database entries for missing barcodes."""
        # Set dry_run mode
        sync_pipeline.dry_run = True

        # Mock the barcode validation methods
        sync_pipeline.db_tracker.check_barcodes_exist = AsyncMock(
            return_value=({"EXIST001"}, {"MISSING001", "MISSING002"})
        )
        sync_pipeline.db_tracker.create_empty_book_entries = AsyncMock()

        # Mock get_all_conversion_failed_books to avoid network call
        with patch("grin_transfer.sync.pipeline.get_all_conversion_failed_books") as mock_failed:
            mock_failed.return_value = {}

            # Mock filter_and_print_barcodes to return the barcodes
            with patch("grin_transfer.sync.pipeline.filter_and_print_barcodes") as mock_filter:
                mock_filter.return_value = ["EXIST001", "MISSING001", "MISSING002"]

                # Call setup_sync_loop with specific barcodes in dry-run mode
                await sync_pipeline.setup_sync_loop(
                    queues=[], specific_barcodes=["EXIST001", "MISSING001", "MISSING002"], limit=None
                )

        # Verify check_barcodes_exist was still called
        sync_pipeline.db_tracker.check_barcodes_exist.assert_called_once_with(["EXIST001", "MISSING001", "MISSING002"])

        # Verify create_empty_book_entries was NOT called in dry-run mode
        sync_pipeline.db_tracker.create_empty_book_entries.assert_not_called()

    @pytest.mark.asyncio
    async def test_queue_mode_skips_barcode_validation(self, sync_pipeline):
        """Test that queue mode (not specific barcodes) skips barcode validation."""
        # Mock the barcode validation methods
        sync_pipeline.db_tracker.check_barcodes_exist = AsyncMock()
        sync_pipeline.db_tracker.create_empty_book_entries = AsyncMock()

        # Mock queue processing functions
        with patch("grin_transfer.sync.pipeline.get_books_from_queue") as mock_get_books:
            mock_get_books.return_value = {"QUEUE001", "QUEUE002"}

            with patch("grin_transfer.sync.pipeline.filter_and_print_barcodes") as mock_filter:
                mock_filter.return_value = ["QUEUE001", "QUEUE002"]

                with patch("grin_transfer.sync.pipeline.process_books_with_queue") as mock_process:
                    mock_process.return_value = {}

                    # Call setup_sync_loop with queues (not specific barcodes)
                    await sync_pipeline.setup_sync_loop(
                        queues=["converted"],
                        specific_barcodes=[],  # Empty list means queue mode
                        limit=None,
                    )

        # Verify barcode validation methods were NOT called in queue mode
        sync_pipeline.db_tracker.check_barcodes_exist.assert_not_called()
        sync_pipeline.db_tracker.create_empty_book_entries.assert_not_called()
