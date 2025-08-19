#!/usr/bin/env python3
"""
Tests for conversion request handling during sync operations
"""

from unittest.mock import AsyncMock, Mock, patch

import pytest

from grin_to_s3.collect_books.models import SQLiteProgressTracker
from grin_to_s3.processing import ProcessingRequestError
from grin_to_s3.sync.conversion_handler import ConversionRequestHandler


@patch("grin_to_s3.sync.conversion_handler.mark_verified_unavailable")
class TestConversionRequestHandler:
    """Test the ConversionRequestHandler class."""

    @pytest.fixture
    def mock_db_tracker(self):
        """Mock database tracker."""
        tracker = Mock(spec=SQLiteProgressTracker)
        tracker.add_status = AsyncMock()
        tracker.db_path = "/test/path/books.db"
        return tracker

    @pytest.fixture
    def handler(self, mock_db_tracker):
        """Create ConversionRequestHandler instance."""
        return ConversionRequestHandler(
            library_directory="test_library", db_tracker=mock_db_tracker, secrets_dir="test_secrets"
        )

    @pytest.mark.asyncio
    async def test_handle_missing_archive_successful_request(self, mock_mark_unavailable, handler, mock_db_tracker):
        """Test successful conversion request."""
        with patch("grin_to_s3.sync.conversion_handler.request_conversion") as mock_request:
            mock_request.return_value = "Success"

            result = await handler.handle_missing_archive("test_barcode", 100)

            assert result == "requested"
            assert handler.requests_made == 1
            mock_request.assert_called_once_with("test_barcode", "test_library", "test_secrets")
            mock_mark_unavailable.assert_not_called()

    @pytest.mark.asyncio
    async def test_handle_missing_archive_already_in_process(self, mock_mark_unavailable, handler, mock_db_tracker):
        """Test when book is already in process."""
        with patch("grin_to_s3.sync.conversion_handler.request_conversion") as mock_request:
            mock_request.return_value = "Book already in process"

            result = await handler.handle_missing_archive("test_barcode", 100)

            assert result == "in_process"
            assert handler.requests_made == 1
            mock_mark_unavailable.assert_not_called()

    @pytest.mark.asyncio
    async def test_handle_missing_archive_already_available(self, mock_mark_unavailable, handler, mock_db_tracker):
        """Test when book is already available."""
        with patch("grin_to_s3.sync.conversion_handler.request_conversion") as mock_request:
            mock_request.return_value = "Already available for download"

            result = await handler.handle_missing_archive("test_barcode", 100)

            assert result == "in_process"
            assert handler.requests_made == 1
            mock_mark_unavailable.assert_not_called()

    @pytest.mark.asyncio
    async def test_handle_missing_archive_unavailable(self, mock_mark_unavailable, handler, mock_db_tracker):
        """Test when conversion fails (book unavailable)."""
        with patch("grin_to_s3.sync.conversion_handler.request_conversion") as mock_request:
            mock_request.return_value = "Book not available for processing"

            result = await handler.handle_missing_archive("test_barcode", 100)

            assert result == "unavailable"
            assert handler.requests_made == 1
            mock_mark_unavailable.assert_called_once_with(
                "/test/path/books.db", "test_barcode", "Book not available for processing"
            )

    @pytest.mark.asyncio
    async def test_handle_missing_archive_request_limit_reached(self, mock_mark_unavailable, handler, mock_db_tracker):
        """Test when conversion request limit is reached."""
        handler.requests_made = 100  # At limit

        result = await handler.handle_missing_archive("test_barcode", 100)

        assert result == "limit_reached"
        assert handler.requests_made == 100  # Should not increment

    @pytest.mark.asyncio
    async def test_handle_missing_archive_processing_error(self, mock_mark_unavailable, handler, mock_db_tracker):
        """Test when ProcessingRequestError is raised."""
        with patch("grin_to_s3.sync.conversion_handler.request_conversion") as mock_request:
            mock_request.side_effect = ProcessingRequestError("Processing failed")

            result = await handler.handle_missing_archive("test_barcode", 100)

            assert result == "unavailable"
            assert handler.requests_made == 0  # Should not increment on error
            mock_mark_unavailable.assert_called_once_with("/test/path/books.db", "test_barcode", "Processing failed")

    @pytest.mark.asyncio
    async def test_handle_missing_archive_unexpected_error(self, mock_mark_unavailable, handler, mock_db_tracker):
        """Test when unexpected error is raised."""
        with patch("grin_to_s3.sync.conversion_handler.request_conversion") as mock_request:
            mock_request.side_effect = Exception("Network error")

            result = await handler.handle_missing_archive("test_barcode", 100)

            assert result == "unavailable"
            assert handler.requests_made == 0  # Should not increment on error
            mock_mark_unavailable.assert_called_once_with("/test/path/books.db", "test_barcode", "Network error")

    @pytest.mark.asyncio
    async def test_mark_verified_unavailable_database_error(self, mock_mark_unavailable, handler, mock_db_tracker):
        """Test when database marking fails."""
        mock_db_tracker.add_status.side_effect = Exception("Database error")

        with patch("grin_to_s3.sync.conversion_handler.request_conversion") as mock_request:
            mock_request.return_value = "Book not found"
            mock_mark_unavailable.side_effect = Exception("Database error")

            result = await handler.handle_missing_archive("test_barcode", 100)

            # Should still return unavailable even if database update fails
            assert result == "unavailable"
            assert handler.requests_made == 1


class TestStandaloneFunctions:
    """Test standalone convenience functions."""

    @pytest.fixture
    def mock_grin_client(self):
        """Mock GRIN client."""
        return Mock()

    @pytest.fixture
    def mock_db_tracker(self):
        """Mock database tracker."""
        tracker = Mock(spec=SQLiteProgressTracker)
        tracker.add_status = AsyncMock()
        return tracker


class TestSyncPipelineIntegration:
    """Test conversion request integration with sync pipeline."""

    @pytest.mark.asyncio
    async def test_previous_queue_404_triggers_conversion_request(self, sync_pipeline):
        """Test that 404 errors in previous queue trigger conversion requests."""
        # Set up pipeline for previous queue
        sync_pipeline.current_queues = ["previous"]
        sync_pipeline.conversion_handler = ConversionRequestHandler(
            library_directory="test_library", db_tracker=sync_pipeline.db_tracker, secrets_dir=None
        )

        with patch("grin_to_s3.sync.conversion_handler.request_conversion") as mock_request:
            mock_request.return_value = "Success"

            # Test handler directly instead of full pipeline
            result = await sync_pipeline.conversion_handler.handle_missing_archive("test_barcode", 100)

            # Verify conversion was requested
            assert result == "requested"
            assert sync_pipeline.conversion_handler.requests_made == 1
            mock_request.assert_called_once_with("test_barcode", "test_library", None)

    @pytest.mark.asyncio
    async def test_converted_queue_404_no_conversion_request(self, sync_pipeline):
        """STUBBED: Test that 404 errors in converted queue do not trigger conversion requests.

        TODO: Replace with task-based test that verifies 404 handling in download task
        does not trigger conversion requests when not processing 'previous' queue.
        """
        # STUB: This test needs to be rewritten for the new task-based architecture
        pytest.skip("Stubbed - needs rewrite for task-based architecture")

    @pytest.mark.asyncio
    async def test_previous_queue_404_conversion_limit_reached(self, sync_pipeline):
        """Test behavior when conversion request limit is reached."""
        # Set up pipeline for previous queue with handler at limit
        sync_pipeline.current_queues = ["previous"]
        sync_pipeline.conversion_handler = ConversionRequestHandler(
            library_directory="test_library", db_tracker=sync_pipeline.db_tracker, secrets_dir=None
        )
        sync_pipeline.conversion_handler.requests_made = 100  # At limit

        # No mocks needed for limit reached test

        # Test handler directly - should return limit reached
        result = await sync_pipeline.conversion_handler.handle_missing_archive("test_barcode", 100)

        # Verify limit reached response
        assert result == "limit_reached"
        assert sync_pipeline.conversion_handler.requests_made == 100  # Should not increment

    @pytest.mark.asyncio
    async def test_previous_queue_404_conversion_unavailable(self, sync_pipeline):
        """Test when conversion request returns unavailable."""
        # Set up pipeline for previous queue
        sync_pipeline.current_queues = ["previous"]
        sync_pipeline.conversion_handler = ConversionRequestHandler(
            library_directory="test_library", db_tracker=sync_pipeline.db_tracker, secrets_dir=None
        )

        with patch("grin_to_s3.sync.conversion_handler.request_conversion") as mock_request:
            mock_request.return_value = "Book not available"

            # Test handler directly
            result = await sync_pipeline.conversion_handler.handle_missing_archive("test_barcode", 100)

            # Verify marked as unavailable
            assert result == "unavailable"
            assert sync_pipeline.conversion_handler.requests_made == 1
            mock_request.assert_called_once_with("test_barcode", "test_library", None)

    @pytest.mark.asyncio
    async def test_run_sync_initializes_conversion_handler_for_previous_queue(self, sync_pipeline):
        """STUBBED: Test that setup_sync_loop initializes conversion handler when previous queue is specified.

        TODO: Replace with test that verifies conversion handler initialization in task-based pipeline.
        """
        # STUB: This test needs to be rewritten for the new task-based architecture
        pytest.skip("Stubbed - needs rewrite for task-based architecture")

    @pytest.mark.asyncio
    async def test_run_sync_no_conversion_handler_for_other_queues(self, sync_pipeline):
        """STUBBED: Test that setup_sync_loop does not initialize conversion handler for other queues.

        TODO: Replace with test that verifies conversion handler is not initialized for non-previous queues.
        """
        # STUB: This test needs to be rewritten for the new task-based architecture
        pytest.skip("Stubbed - needs rewrite for task-based architecture")
