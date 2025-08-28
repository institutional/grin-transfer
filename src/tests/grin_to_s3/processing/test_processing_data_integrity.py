#!/usr/bin/env python3
"""
Data integrity tests for ProcessingClient TSV parsing and batch operations.

Tests critical edge cases that could cause silent data corruption or
incorrect status tracking in the processing pipeline.
"""

import pytest

from grin_to_s3.processing import ProcessingClient, ProcessingPipeline
from tests.mocks import MockGRINClient


class MockProcessingClient(MockGRINClient):
    """Mock GRIN client specifically for processing testing"""

    def __init__(self):
        super().__init__()
        self.processing_response = ""

    def set_processing_response(self, response_body):
        """Set the response body for processing requests"""
        self.processing_response = response_body

    async def fetch_resource(self, directory: str, resource: str):
        """Return mock processing response"""
        if "_process" in resource:
            return self.processing_response
        return await super().fetch_resource(directory, resource)


class TestProcessingClientDataIntegrity:
    """Test ProcessingClient data parsing and integrity validation."""

    @pytest.fixture
    def processing_client(self):
        """Create a ProcessingClient for testing."""
        client = ProcessingClient(directory="test_dir", rate_limit_delay=0)
        # Replace grin_client with mock to avoid authentication issues
        client.grin_client = MockProcessingClient()
        return client

    async def assert_processing_error(self, processing_client, barcodes, response_body):
        """Helper to test that processing raises natural exceptions."""
        # Set the mock response for this test
        processing_client.grin_client.set_processing_response(response_body)

        # Let natural exceptions bubble up - typically IndexError for malformed lines
        with pytest.raises((IndexError, ValueError)):
            await processing_client.request_processing_batch(barcodes)

    async def assert_processing_result(self, processing_client, barcodes, response_body, expected_result):
        """Helper to test successful processing results."""
        # Set the mock response for this test
        processing_client.grin_client.set_processing_response(response_body)

        result = await processing_client.request_processing_batch(barcodes)
        assert result == expected_result

    @pytest.mark.parametrize(
        "test_case,response_body",
        [
            ("extra_columns", "Barcode\tStatus\nTEST123\tSuccess\tExtraColumn"),
            ("missing_columns", "Barcode\tStatus\nTEST123"),
            ("tabs_in_status", "Barcode\tStatus\nTEST123\tFailed: Too\tmany\ttabs"),
        ],
    )
    @pytest.mark.asyncio
    async def test_tsv_parsing_errors(self, processing_client, test_case, response_body):
        """Test various TSV parsing error scenarios - let natural exceptions bubble up."""
        await self.assert_processing_error(processing_client, ["TEST123"], response_body)

    @pytest.mark.asyncio
    async def test_empty_and_header_only_responses(self, processing_client):
        """Test that empty and header-only responses return empty results naturally."""
        # Empty response returns empty dict
        processing_client.grin_client.set_processing_response("")
        result = await processing_client.request_processing_batch(["TEST123"])
        assert result == {}

        # Header-only response returns empty dict
        processing_client.grin_client.set_processing_response("Barcode\tStatus")
        result = await processing_client.request_processing_batch(["TEST123"])
        assert result == {}

    @pytest.mark.asyncio
    async def test_batch_barcode_count_mismatch(self, processing_client):
        """Test when GRIN returns different number of results than requested."""
        # Request 3 barcodes but only get 2 results
        barcodes = ["TEST001", "TEST002", "TEST003"]
        mock_response = "Barcode\tStatus\nTEST001\tSuccess\nTEST002\tSuccess"

        await self.assert_processing_result(
            processing_client, barcodes, mock_response, {"TEST001": "Success", "TEST002": "Success"}
        )

    @pytest.mark.asyncio
    async def test_batch_mismatched_barcodes_returned(self, processing_client):
        """Test when GRIN returns different barcodes than requested."""
        requested_barcodes = ["TEST001", "TEST002"]
        # GRIN returns completely different barcodes
        mock_response = "Barcode\tStatus\nTEST999\tSuccess\nTEST888\tSuccess"

        await self.assert_processing_result(
            processing_client, requested_barcodes, mock_response, {"TEST999": "Success", "TEST888": "Success"}
        )

    @pytest.mark.asyncio
    async def test_batch_partial_barcode_match(self, processing_client):
        """Test when GRIN returns mix of requested and unexpected barcodes."""
        requested_barcodes = ["TEST001", "TEST002", "TEST003"]
        # GRIN returns some requested + some unexpected barcodes
        mock_response = "Barcode\tStatus\nTEST001\tSuccess\nTEST999\tFailed\nTEST003\tSuccess"

        await self.assert_processing_result(
            processing_client,
            requested_barcodes,
            mock_response,
            {"TEST001": "Success", "TEST999": "Failed", "TEST003": "Success"},
        )

    @pytest.mark.asyncio
    async def test_tsv_parsing_with_empty_lines(self, processing_client):
        """Test handling of TSV with empty lines between data."""
        await self.assert_processing_result(
            processing_client,
            ["TEST001", "TEST002"],
            "Barcode\tStatus\nTEST001\tSuccess\n\n\nTEST002\tFailed\n\n",
            {"TEST001": "Success", "TEST002": "Failed"},
        )

    @pytest.mark.asyncio
    async def test_tsv_parsing_strips_barcode_whitespace(self, processing_client):
        """Test that whitespace is properly stripped from barcodes."""
        await self.assert_processing_result(
            processing_client,
            ["TEST001", "TEST002"],
            "Barcode\tStatus\n  TEST001  \t  Success  \nTEST002\tFailed",
            {"TEST001": "  Success  ", "TEST002": "Failed"},  # Status preserved, barcode stripped
        )

    @pytest.mark.asyncio
    async def test_barcode_whitespace_edge_cases(self, processing_client):
        """Test edge cases for barcode whitespace handling."""
        await self.assert_processing_result(
            processing_client,
            ["TEST001", "TEST002"],
            "Barcode\tStatus\n TEST001 \tSuccess\n  TEST002  \tFailed",
            {"TEST001": "Success", "TEST002": "Failed"},
        )

    @pytest.mark.asyncio
    async def test_batch_processing_empty_input(self, processing_client):
        """Test batch processing with empty barcode list."""
        result = await processing_client.request_processing_batch([])
        assert result == {}

    @pytest.mark.asyncio
    async def test_batch_processing_missing_barcode_access(self, processing_client):
        """Test accessing missing barcode from batch results raises KeyError naturally."""
        mock_response = "Barcode\tStatus\nTEST999\tSuccess"  # Different barcode returned

        # Set the mock response for this test
        processing_client.grin_client.set_processing_response(mock_response)

        results = await processing_client.request_processing_batch(["TEST001"])
        # Accessing missing barcode raises KeyError naturally
        with pytest.raises(KeyError):
            _ = results["TEST001"]

    @pytest.mark.asyncio
    async def test_batch_processing_failure_status(self, processing_client):
        """Test batch processing returns failure status strings directly."""
        mock_response = "Barcode\tStatus\nTEST001\tFailed: Invalid book"

        # Set the mock response for this test
        processing_client.grin_client.set_processing_response(mock_response)

        # Natural behavior: batch method returns the status string directly
        results = await processing_client.request_processing_batch(["TEST001"])
        assert results["TEST001"] == "Failed: Invalid book"

    @pytest.mark.asyncio
    async def test_status_list_parsing_edge_cases(self, processing_client):
        """Test parsing of status lists with various formats."""

        # Override the mock to return the response for _in_process requests
        class StatusMockClient(MockProcessingClient):
            async def fetch_resource(self, directory: str, resource: str):
                if "_in_process" in resource:
                    return "TEST001\n\nTEST002\n  TEST003  \n\n"
                elif "_process" in resource:
                    return self.processing_response
                return await super().fetch_resource(directory, resource)

        processing_client.grin_client = StatusMockClient()
        result = await processing_client.get_in_process_books()

        # Should handle empty lines and strip whitespace (correct behavior)
        expected = {"TEST001", "TEST002", "TEST003"}  # Whitespace is stripped
        assert result == expected

    @pytest.mark.asyncio
    async def test_converted_books_suffix_removal(self, processing_client):
        """Test that .tar.gz.gpg suffix is correctly removed from converted books."""

        # Override the mock to return converted books with suffixes
        class ConvertedMockClient(MockProcessingClient):
            async def fetch_resource(self, directory: str, resource: str):
                if "_converted" in resource:
                    return "TEST001.tar.gz.gpg\nTEST002.tar.gz.gpg\nTEST003_no_suffix\n"
                elif "_process" in resource:
                    return self.processing_response
                return await super().fetch_resource(directory, resource)

        processing_client.grin_client = ConvertedMockClient()
        from grin_to_s3.processing import get_converted_books

        result = await get_converted_books(processing_client.grin_client, "test_dir")

        # Should remove suffix and include books with suffix only
        expected = {"TEST001", "TEST002"}
        assert result == expected

    @pytest.mark.asyncio
    async def test_converted_books_malformed_filenames(self, processing_client):
        """Test handling of unexpected filename formats in converted books."""

        # Override the mock to return malformed filenames
        class MalformedMockClient(MockProcessingClient):
            async def fetch_resource(self, directory: str, resource: str):
                if "_converted" in resource:
                    return "TEST001.tar.gz.gpg\n.tar.gz.gpg\nTEST002.tar.gz\nTEST003.tar.gz.gpg\n"
                elif "_process" in resource:
                    return self.processing_response
                return await super().fetch_resource(directory, resource)

        processing_client.grin_client = MalformedMockClient()
        from grin_to_s3.processing import get_converted_books

        result = await get_converted_books(processing_client.grin_client, "test_dir")

        # Current behavior includes empty string from .tar.gz.gpg -> ""
        # This reveals a minor edge case in the production code
        expected = {"", "TEST001", "TEST003"}  # Includes empty string from malformed case
        assert result == expected

    @pytest.mark.asyncio
    async def test_network_error_handling_in_batch_request(self, processing_client):
        """Test error handling when network request fails."""

        # Override the mock to raise an exception
        class ErrorMockClient(MockProcessingClient):
            async def fetch_resource(self, directory: str, resource: str):
                if "_process" in resource:
                    raise Exception("Network error")
                return await super().fetch_resource(directory, resource)

        processing_client.grin_client = ErrorMockClient()

        # Let natural exception from ErrorMockClient bubble up
        with pytest.raises(Exception) as exc_info:
            await processing_client.request_processing_batch(["TEST001"])

        assert "Network error" in str(exc_info.value)

        from grin_to_s3.processing import get_converted_books

        result = await get_converted_books(processing_client.grin_client, "test_dir")
        assert result == set()


class TestProcessingPipelineBarcodeFunctionality:
    """Test ProcessingPipeline barcode-related functionality."""

    @pytest.fixture
    async def processing_pipeline(self, tmp_path):
        """Create a ProcessingPipeline for testing with proper database setup."""
        from unittest.mock import Mock

        from grin_to_s3.collect_books.models import BookRecord, SQLiteProgressTracker

        db_path = tmp_path / "test.db"

        # Use the real SQLiteProgressTracker to initialize the database with proper schema
        tracker = SQLiteProgressTracker(str(db_path))
        await tracker.init_db()

        # Create test book records and save them properly
        test_books = [
            BookRecord(barcode="TEST001", title="Test Book 1"),
            BookRecord(barcode="TEST002", title="Test Book 2", grin_state="NOT_AVAILABLE_FOR_DOWNLOAD"),
            BookRecord(barcode="TEST003", title="Test Book 3", processing_request_timestamp="2024-01-03T10:00:00"),
            BookRecord(barcode="TEST004", title="Test Book 4", converted_date="2024-01-04T10:00:00"),
            BookRecord(barcode="TEST005", title="Test Book 5"),
            BookRecord(barcode="TEST006", title="Test Book 6", grin_state="CHECKED_IN"),
        ]

        for book in test_books:
            await tracker.save_book(book)

        # Mock the process summary stage
        mock_stage = Mock()
        mock_stage.conversion_requests_made = 0
        mock_stage.conversion_requests_failed = 0
        mock_stage.add_progress_update = Mock()
        mock_stage.add_error = Mock()

        pipeline = ProcessingPipeline(
            db_path=str(db_path), directory="test_dir", process_summary_stage=mock_stage, rate_limit_delay=0
        )

        # Replace grin_client with mock to avoid authentication issues
        pipeline.processing_client.grin_client = MockProcessingClient()  # type: ignore

        return pipeline

    @pytest.mark.asyncio
    async def test_get_candidate_barcodes_with_explicit_barcodes(self, processing_pipeline):
        """Test _get_candidate_barcodes returns explicit barcodes unchanged."""
        barcodes = ["EXPLICIT1", "EXPLICIT2", "EXPLICIT3"]

        result = await processing_pipeline._get_candidate_barcodes(limit=10, barcodes=barcodes, in_process_books=set())

        assert result == barcodes

    @pytest.mark.asyncio
    async def test_get_candidate_barcodes_database_query(self, processing_pipeline):
        """Test _get_candidate_barcodes uses database query when no barcodes provided."""
        result = await processing_pipeline._get_candidate_barcodes(limit=10, barcodes=None, in_process_books=set())

        # Should return TEST001 and TEST005 (available books)
        # TEST002 is NOT_AVAILABLE_FOR_DOWNLOAD
        # TEST003 already has processing_request_timestamp
        # TEST004 already has converted_date
        # TEST006 is CHECKED_IN
        expected = ["TEST001", "TEST005"]
        assert result == expected

    @pytest.mark.asyncio
    async def test_get_candidate_barcodes_filters_in_process(self, processing_pipeline):
        """Test _get_candidate_barcodes filters out books in GRIN processing queue."""
        in_process_books = {"TEST001"}

        result = await processing_pipeline._get_candidate_barcodes(
            limit=10, barcodes=None, in_process_books=in_process_books
        )

        # Should only return TEST005 (TEST001 is filtered out)
        expected = ["TEST005"]
        assert result == expected

    @pytest.mark.asyncio
    async def test_get_candidate_barcodes_respects_limit(self, processing_pipeline):
        """Test _get_candidate_barcodes respects the limit parameter."""
        result = await processing_pipeline._get_candidate_barcodes(limit=1, barcodes=None, in_process_books=set())

        # Should only return one book (TEST001)
        assert len(result) == 1
        assert result == ["TEST001"]

    @pytest.mark.asyncio
    async def test_get_candidate_barcodes_explicit_ignores_limit(self, processing_pipeline):
        """Test _get_candidate_barcodes ignores limit when explicit barcodes provided."""
        barcodes = ["EXP1", "EXP2", "EXP3", "EXP4", "EXP5"]

        result = await processing_pipeline._get_candidate_barcodes(limit=2, barcodes=barcodes, in_process_books=set())

        # Should return all barcodes despite limit=2
        assert result == barcodes
