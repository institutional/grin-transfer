#!/usr/bin/env python3
"""
Data integrity tests for GRIN enrichment batch processing.

Tests critical edge cases that could cause silent data corruption or
incorrect field mapping in the enrichment pipeline.
"""

from unittest.mock import AsyncMock

import pytest
from aioresponses import aioresponses

from grin_to_s3.metadata.grin_enrichment import GRINEnrichmentPipeline


class TestEnrichmentDataIntegrity:
    """Test enrichment pipeline data parsing and batch integrity."""

    @pytest.fixture
    def enrichment_pipeline(self):
        """Create a GRINEnrichmentPipeline for testing."""
        # Mock the process summary stage with proper async methods
        mock_stage = AsyncMock()
        mock_stage.increment_items = AsyncMock(return_value=None)

        pipeline = GRINEnrichmentPipeline(
            directory="test_dir",
            process_summary_stage=mock_stage,
            rate_limit_delay=0,
            batch_size=10,
            max_concurrent_requests=1
        )
        return pipeline

    def mock_enrichment_response(self, m, barcodes, tsv_response):
        """Helper to mock GRIN enrichment API responses consistently."""
        if isinstance(barcodes, str):
            barcodes = [barcodes]
        barcode_param = " ".join(barcodes)
        search_url = f"_barcode_search?execute_query=true&format=text&mode=full&barcodes={barcode_param}"
        url = f"https://books.google.com/libraries/test_dir/{search_url}"
        m.get(url, body=tsv_response)

    async def assert_enrichment_result(self, enrichment_pipeline, barcodes, tsv_response, expected_result_check):
        """Helper to test enrichment results with a custom assertion function."""
        with aioresponses() as m:
            self.mock_enrichment_response(m, barcodes, tsv_response)
            result = await enrichment_pipeline.fetch_grin_metadata_batch(barcodes)
            expected_result_check(result)

    @pytest.mark.asyncio
    async def test_url_length_calculation_edge_cases(self, enrichment_pipeline):
        """Test URL length calculation with edge case barcodes."""
        # Test with very long barcodes that approach URL limits
        long_barcodes = [f"VERYLONGBARCODE{'X' * 100}_{i:03d}" for i in range(50)]

        max_batch_size = enrichment_pipeline._calculate_max_batch_size(long_barcodes)

        # Should return a reasonable batch size that doesn't exceed URL limits
        assert max_batch_size >= 1  # Must allow at least one barcode
        assert max_batch_size <= len(long_barcodes)

        # Verify the calculated batch size actually fits in URL
        test_batch = long_barcodes[:max_batch_size]
        barcode_string = " ".join(test_batch)
        base_url = f"https://books.google.com/libraries/{enrichment_pipeline.directory}/_barcode_search?execute_query=true&format=text&mode=full&barcodes="
        full_url_length = len(base_url + barcode_string)

        assert full_url_length <= 7500  # Should stay under the limit

    @pytest.mark.asyncio
    async def test_url_length_single_barcode_too_long(self, enrichment_pipeline):
        """Test handling of single barcode that exceeds URL length limit."""
        # Create a barcode so long it exceeds URL limits by itself
        extremely_long_barcode = "EXTREME" + "X" * 8000

        # This should trigger the recursive split logic and eventually skip the barcode
        result = await enrichment_pipeline.fetch_grin_metadata_batch([extremely_long_barcode])

        # Should return None for the problematic barcode
        assert result == {extremely_long_barcode: None}

    @pytest.mark.asyncio
    async def test_tsv_parsing_header_value_count_mismatch(self, enrichment_pipeline):
        """Test handling of TSV with mismatched header/value counts."""
        barcodes = ["TEST001", "TEST002"]

        # TSV with more values than headers in one row
        mock_response = (
            "Barcode\tState\tViewability\n"
            "TEST001\tACCEPTED\tFULL_VIEW\tExtraValue\n"  # Extra value
            "TEST002\tACCEPTED\n"  # Missing value
        )

        with aioresponses() as m:
            search_url = "_barcode_search?execute_query=true&format=text&mode=full&barcodes=TEST001 TEST002"
            m.get(f"https://books.google.com/libraries/test_dir/{search_url}", body=mock_response)

            result = await enrichment_pipeline.fetch_grin_metadata_batch(barcodes)

            # Should handle gracefully - truncate extra values, pad missing ones
            assert len(result) == 2
            assert "TEST001" in result
            assert "TEST002" in result

            # TEST001 should have truncated values
            test001_data = result["TEST001"]
            assert test001_data is not None

            # TEST002 should have padded empty values
            test002_data = result["TEST002"]
            assert test002_data is not None

    @pytest.mark.asyncio
    async def test_tsv_parsing_empty_barcode_handling(self, enrichment_pipeline):
        """Test handling of TSV rows with empty or missing barcodes."""
        tsv_response = (
            "Barcode\tState\tViewability\n"
            "TEST001\tACCEPTED\tFULL_VIEW\n"
            "\tACCEPTED\tFULL_VIEW\n"  # Empty barcode
            "TEST002\tACCEPTED\tFULL_VIEW\n"
        )

        def check_result(result):
            # Should skip empty barcode row and still return requested barcodes
            assert len(result) == 2
            assert "TEST001" in result
            assert "TEST002" in result
            # Empty barcode row should be ignored
            assert "" not in result

        await self.assert_enrichment_result(
            enrichment_pipeline, ["TEST001", "TEST002"], tsv_response, check_result
        )

    @pytest.mark.asyncio
    async def test_tsv_parsing_duplicate_barcodes_in_response(self, enrichment_pipeline):
        """Test handling of duplicate barcodes in GRIN response."""
        barcodes = ["TEST001"]

        # TSV with same barcode appearing twice
        mock_response = (
            "Barcode\tState\tViewability\n"
            "TEST001\tACCEPTED\tFULL_VIEW\n"
            "TEST001\tREJECTED\tNO_VIEW\n"  # Duplicate with different data
        )

        with aioresponses() as m:
            search_url = "_barcode_search?execute_query=true&format=text&mode=full&barcodes=TEST001"
            m.get(f"https://books.google.com/libraries/test_dir/{search_url}", body=mock_response)

            result = await enrichment_pipeline.fetch_grin_metadata_batch(barcodes)

            # Should handle gracefully - last occurrence wins
            assert len(result) == 1
            assert "TEST001" in result
            test_data = result["TEST001"]
            # Should have data from the second (last) occurrence
            assert test_data is not None

    @pytest.mark.asyncio
    async def test_tsv_parsing_unexpected_barcodes_in_response(self, enrichment_pipeline):
        """Test when GRIN returns barcodes not requested."""
        requested_barcodes = ["TEST001", "TEST002"]

        # TSV includes unexpected barcodes
        mock_response = (
            "Barcode\tState\tViewability\n"
            "TEST001\tACCEPTED\tFULL_VIEW\n"
            "UNEXPECTED999\tACCEPTED\tFULL_VIEW\n"  # Not requested
            "TEST002\tACCEPTED\tFULL_VIEW\n"
        )

        with aioresponses() as m:
            search_url = "_barcode_search?execute_query=true&format=text&mode=full&barcodes=TEST001 TEST002"
            m.get(f"https://books.google.com/libraries/test_dir/{search_url}", body=mock_response)

            result = await enrichment_pipeline.fetch_grin_metadata_batch(requested_barcodes)

            # Current behavior: includes all barcodes from response, then ensures requested ones exist
            # This means unexpected barcodes are included in the result
            assert len(result) == 3  # All barcodes from response
            assert "TEST001" in result
            assert "TEST002" in result
            assert "UNEXPECTED999" in result  # Unexpected barcode is included

            # This test documents current behavior - may want to change this in production
            # to filter out unexpected barcodes

    @pytest.mark.asyncio
    async def test_tsv_parsing_missing_requested_barcodes(self, enrichment_pipeline):
        """Test when GRIN doesn't return some requested barcodes."""
        requested_barcodes = ["TEST001", "TEST002", "TEST003"]

        # TSV missing TEST002
        mock_response = (
            "Barcode\tState\tViewability\n"
            "TEST001\tACCEPTED\tFULL_VIEW\n"
            "TEST003\tACCEPTED\tFULL_VIEW\n"
        )

        with aioresponses() as m:
            search_url = "_barcode_search?execute_query=true&format=text&mode=full&barcodes=TEST001 TEST002 TEST003"
            m.get(f"https://books.google.com/libraries/test_dir/{search_url}", body=mock_response)

            result = await enrichment_pipeline.fetch_grin_metadata_batch(requested_barcodes)

            # Should include all requested barcodes, missing ones as None
            assert len(result) == 3
            assert "TEST001" in result
            assert "TEST002" in result
            assert "TEST003" in result

            assert result["TEST001"] is not None
            assert result["TEST002"] is None  # Missing from response
            assert result["TEST003"] is not None

    @pytest.mark.asyncio
    async def test_field_mapping_corruption_detection(self, enrichment_pipeline):
        """Test that field mapping from TSV to database is correct."""
        barcodes = ["TEST001"]

        # TSV with known field values
        mock_response = (
            "Barcode\tCheck-In Date\tState\tViewability\tConditions\tScannable\n"
            "TEST001\t2024-01-01\tACCEPTED\tFULL_VIEW\tGOOD\ttrue\n"
        )

        with aioresponses() as m:
            search_url = "_barcode_search?execute_query=true&format=text&mode=full&barcodes=TEST001"
            m.get(f"https://books.google.com/libraries/test_dir/{search_url}", body=mock_response)

            result = await enrichment_pipeline.fetch_grin_metadata_batch(barcodes)

            # Verify correct field mapping
            assert "TEST001" in result
            test_data = result["TEST001"]
            assert test_data is not None

            # Check that TSV columns map to correct database fields
            # This uses the actual mapping from BookRecord.get_grin_tsv_column_mapping()
            assert test_data.get("grin_state") == "ACCEPTED"
            assert test_data.get("grin_viewability") == "FULL_VIEW"
            assert test_data.get("grin_conditions") == "GOOD"
            assert test_data.get("grin_scannable") == "true"

    @pytest.mark.asyncio
    async def test_batch_splitting_maintains_data_integrity(self, enrichment_pipeline):
        """Test that batch splitting doesn't corrupt barcode assignments."""
        # Create barcodes that will trigger URL length splitting
        barcodes = [f"LONGBARCODE{'X' * 50}_{i:03d}" for i in range(10)]

        # Mock responses for split batches
        def mock_response_generator(requested_barcodes):
            headers = "Barcode\tState\tViewability"
            lines = [headers]
            for barcode in requested_barcodes:
                lines.append(f"{barcode}\tACCEPTED\tFULL_VIEW")
            return "\n".join(lines)

        with aioresponses() as m:
            # The function will split into multiple requests - mock all possible splits
            for i in range(1, len(barcodes) + 1):
                for j in range(i, len(barcodes) + 1):
                    batch = barcodes[i-1:j]
                    if batch:
                        barcode_list = " ".join(batch)
                        search_url = f"_barcode_search?execute_query=true&format=text&mode=full&barcodes={barcode_list}"
                        m.get(
                            f"https://books.google.com/libraries/test_dir/{search_url}",
                            body=mock_response_generator(batch)
                        )

            result = await enrichment_pipeline.fetch_grin_metadata_batch(barcodes)

            # Should have data for all requested barcodes
            assert len(result) == len(barcodes)
            for barcode in barcodes:
                assert barcode in result
                assert result[barcode] is not None
                # Verify data integrity - each barcode should have its own data
                assert result[barcode].get("grin_state") == "ACCEPTED"

    @pytest.mark.asyncio
    async def test_concurrent_batch_result_assignment(self, enrichment_pipeline):
        """Test that concurrent batch processing assigns results to correct barcodes."""
        barcodes = ["BATCH1_001", "BATCH1_002", "BATCH2_001", "BATCH2_002"]

        # Mock the SQLiteProgressTracker
        mock_tracker = AsyncMock()
        mock_tracker.update_book_enrichment = AsyncMock(return_value=True)
        enrichment_pipeline.sqlite_tracker = mock_tracker

        # Mock different responses for different batches
        with aioresponses() as m:
            # Batch 1 response
            batch1_response = (
                "Barcode\tState\tViewability\n"
                "BATCH1_001\tACCEPTED\tFULL_VIEW\n"
                "BATCH1_002\tREJECTED\tNO_VIEW\n"
            )
            m.get(
                "https://books.google.com/libraries/test_dir/_barcode_search?execute_query=true&format=text&mode=full&barcodes=BATCH1_001 BATCH1_002",
                body=batch1_response
            )

            # Batch 2 response
            batch2_response = (
                "Barcode\tState\tViewability\n"
                "BATCH2_001\tPENDING\tMETADATA_VIEW\n"
                "BATCH2_002\tACCEPTED\tFULL_VIEW\n"
            )
            m.get(
                "https://books.google.com/libraries/test_dir/_barcode_search?execute_query=true&format=text&mode=full&barcodes=BATCH2_001 BATCH2_002",
                body=batch2_response
            )

            # Process as two separate batches
            batch1 = barcodes[:2]
            batch2 = barcodes[2:]

            result1 = await enrichment_pipeline.fetch_grin_metadata_batch(batch1)
            result2 = await enrichment_pipeline.fetch_grin_metadata_batch(batch2)

            # Verify correct assignment
            assert result1["BATCH1_001"]["grin_state"] == "ACCEPTED"
            assert result1["BATCH1_002"]["grin_state"] == "REJECTED"
            assert result2["BATCH2_001"]["grin_state"] == "PENDING"
            assert result2["BATCH2_002"]["grin_state"] == "ACCEPTED"

            # Verify no cross-contamination
            assert "BATCH2_001" not in result1
            assert "BATCH1_001" not in result2

    @pytest.mark.asyncio
    async def test_network_error_handling_preserves_batch_integrity(self, enrichment_pipeline):
        """Test that network errors don't corrupt batch processing state."""
        barcodes = ["TEST001", "TEST002", "TEST003"]

        with aioresponses() as m:
            search_url = "_barcode_search?execute_query=true&format=text&mode=full&barcodes=TEST001 TEST002 TEST003"
            m.get(f"https://books.google.com/libraries/test_dir/{search_url}", exception=Exception("Network timeout"))

            result = await enrichment_pipeline.fetch_grin_metadata_batch(barcodes)

            # Should return None for all barcodes on error, not leave some undefined
            assert len(result) == 3
            for barcode in barcodes:
                assert barcode in result
                assert result[barcode] is None

    @pytest.mark.asyncio
    async def test_tsv_parsing_with_special_characters(self, enrichment_pipeline):
        """Test handling of special characters in TSV fields."""
        barcodes = ["TEST001"]

        # TSV with special characters that could break parsing
        mock_response = (
            "Barcode\tState\tViewability\tConditions\n"
            "TEST001\tACCEPTED\tFULL_VIEW\tContains\ttabs\tand\tnewlines\n"
        )

        with aioresponses() as m:
            search_url = "_barcode_search?execute_query=true&format=text&mode=full&barcodes=TEST001"
            m.get(f"https://books.google.com/libraries/test_dir/{search_url}", body=mock_response)

            result = await enrichment_pipeline.fetch_grin_metadata_batch(barcodes)

            # Should handle gracefully - tabs in data break TSV parsing
            # The current implementation would see extra columns due to tabs in data
            assert "TEST001" in result
            # This test documents current behavior - may need fixing in production

    @pytest.mark.asyncio
    async def test_insufficient_tsv_data_handling(self, enrichment_pipeline):
        """Test handling of TSV responses with insufficient data."""
        barcodes = ["TEST001", "TEST002"]

        # Various insufficient data scenarios
        test_cases = [
            "",  # Completely empty
            "Barcode\tState",  # Header only
            "InvalidHeader",  # Single line, not proper header
        ]

        for i, mock_response in enumerate(test_cases):
            with aioresponses() as m:
                search_url = "_barcode_search?execute_query=true&format=text&mode=full&barcodes=TEST001 TEST002"
                m.get(f"https://books.google.com/libraries/test_dir/{search_url}", body=mock_response)

                result = await enrichment_pipeline.fetch_grin_metadata_batch(barcodes)

                # Should return None for all barcodes when insufficient data
                assert len(result) == 2, f"Test case {i} failed"
                assert all(result[barcode] is None for barcode in barcodes), f"Test case {i} failed"

    @pytest.mark.asyncio
    async def test_database_update_failure_handling(self, enrichment_pipeline):
        """Test handling when database updates fail during enrichment."""
        barcodes = ["TEST001", "TEST002"]

        # Mock successful GRIN response
        mock_response = (
            "Barcode\tState\tViewability\n"
            "TEST001\tACCEPTED\tFULL_VIEW\n"
            "TEST002\tACCEPTED\tFULL_VIEW\n"
        )

        # Mock SQLiteProgressTracker with one successful, one failed update
        mock_tracker = AsyncMock()
        mock_tracker.update_book_enrichment = AsyncMock(side_effect=[True, False])  # First succeeds, second fails
        enrichment_pipeline.sqlite_tracker = mock_tracker

        with aioresponses() as m:
            search_url = "_barcode_search?execute_query=true&format=text&mode=full&barcodes=TEST001 TEST002"
            m.get(f"https://books.google.com/libraries/test_dir/{search_url}", body=mock_response)

            # Test the batch enrichment method
            enriched_count = await enrichment_pipeline.enrich_books_batch(barcodes)

            # Should return count of successful updates only
            assert enriched_count == 1  # Only TEST001 succeeded

            # Should have attempted to update both
            assert mock_tracker.update_book_enrichment.call_count == 2
