#!/usr/bin/env python3
"""
GRIN enrichment tests
"""

import os
import sys

import pytest
import pytest_asyncio

sys.path.append(os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))

from grin_transfer.collect_books.models import BookRecord, SQLiteProgressTracker
from grin_transfer.metadata.grin_enrichment import GRINEnrichmentPipeline
from grin_transfer.metadata.tsv_parser import parse_grin_tsv
from tests.mocks import MockGRINClient
from tests.test_utils.database_helpers import StatusUpdate, get_book_for_testing
from tests.utils import batch_write_status_updates


class MockGRINEnrichmentClient(MockGRINClient):
    """Mock GRIN client for enrichment testing"""

    def __init__(self, test_data: dict = None):
        super().__init__()
        self.test_data = test_data or {}
        self.fetch_count = 0

    async def fetch_resource(self, directory: str, resource: str):
        """Return mock TSV data based on requested barcodes"""
        self.fetch_count += 1

        if "_barcode_search" in resource and "barcodes=" in resource:
            # Extract barcodes from URL
            barcodes_param = resource.split("barcodes=")[1].split("&")[0]
            requested_barcodes = barcodes_param.replace("%20", " ").split(" ")

            # Create TSV response
            headers = [
                "Barcode",
                "Check-In Date",
                "State",
                "Viewability",
                "Conditions",
                "Scannable",
                "Opted-Out (post-scan)",
                "Tagging",
                "Audit",
                "Material Error%",
                "Overall Error%",
                "Scanned Date",
                "Processed Date",
                "Analyzed Date",
                "Converted Date",
                "Allow Download Updated Date",
                "Viewability Updated Date",
                "Source Library Bibkey",
                "Rubbish",
                "Downloaded Date",
                "Claimed",
                "OCR GTD Score",
                "OCR Analysis Score",
                "Digitization Method",
                "OCR'd Date",
            ]

            lines = ["\t".join(headers)]

            for barcode in requested_barcodes:
                if barcode in self.test_data:
                    data = self.test_data[barcode]
                    row = [
                        barcode,
                        "",  # Check-In Date
                        data.get("grin_state", ""),
                        data.get("grin_viewability", "VIEW_METADATA"),
                        data.get("grin_conditions", ""),
                        data.get("grin_scannable", "false"),
                        data.get("grin_opted_out", "false"),
                        data.get("grin_tagging", "true"),
                        data.get("grin_audit", ""),
                        data.get("grin_material_error_percent", "0%"),
                        data.get("grin_overall_error_percent", "0%"),
                        "",  # Scanned Date
                        "",  # Processed Date
                        "",  # Analyzed Date
                        "",  # Converted Date
                        "",  # Allow Download Updated Date
                        "",  # Viewability Updated Date
                        "",  # Source Library Bibkey
                        "",  # Rubbish
                        "",  # Downloaded Date
                        data.get("grin_claimed", "false"),
                        data.get("grin_ocr_gtd_score", "75"),
                        data.get("grin_ocr_analysis_score", "80"),
                        data.get("grin_digitization_method", "NON_DESTRUCTIVE"),
                        "",  # OCR'd Date
                    ]
                    lines.append("\t".join(row))

            return "\n".join(lines)

        return super().fetch_resource(directory, resource)


class TestTSVParser:
    """Test the extracted TSV parser pure function"""

    def test_parse_basic_tsv(self):
        """TSV parser should handle basic valid data"""
        result = parse_grin_tsv(
            "Barcode\tState\tViewability\tScannable\n"
            "TEST001\tACTIVE\tVIEW_FULL\ttrue\n"
            "TEST002\tINACTIVE\tVIEW_METADATA\tfalse"
        )

        assert len(result) == 2
        assert result["TEST001"]["grin_state"] == "ACTIVE"
        assert result["TEST001"]["grin_viewability"] == "VIEW_FULL"
        assert result["TEST002"]["grin_state"] == "INACTIVE"

    def test_parse_empty_tsv(self):
        """TSV parser should handle empty input gracefully"""
        assert parse_grin_tsv("") == {}
        assert parse_grin_tsv("Headers only\n") == {}

    def test_parse_missing_values(self):
        """TSV parser should handle missing values by using empty strings"""
        tsv_data = (
            "Barcode\tState\tViewability\tScannable\tExtra\n"
            "TEST001\tACTIVE\tVIEW_FULL\n"  # Missing last two values
        )

        result = parse_grin_tsv(tsv_data)

        assert result["TEST001"]["grin_state"] == "ACTIVE"
        assert result["TEST001"]["grin_viewability"] == "VIEW_FULL"
        assert result["TEST001"]["grin_scannable"] == ""  # Missing value becomes empty


class TestGRINEnrichmentPipeline:
    """Test simplified GRIN enrichment pipeline"""

    @pytest_asyncio.fixture
    async def temp_db_with_books(self, temp_db):
        """Create a temporary database with test books"""
        tracker = SQLiteProgressTracker(temp_db)
        await tracker.init_db()

        test_books = [
            BookRecord(barcode="TEST001", title="Test Book 1"),
            BookRecord(barcode="TEST002", title="Test Book 2"),
            BookRecord(barcode="TEST003", title="Test Book 3"),
        ]

        for book in test_books:
            await tracker.save_book(book)

        # Add processing status
        status_updates = [StatusUpdate(book.barcode, "processing_request", "converted") for book in test_books]
        await batch_write_status_updates(str(temp_db), status_updates)

        yield temp_db

    @pytest.fixture
    def mock_enrichment_data(self):
        """Test enrichment data"""
        return {
            "TEST001": {
                "grin_viewability": "VIEW_FULL",
                "grin_scannable": "true",
                "grin_ocr_gtd_score": "95",
            },
            "TEST002": {
                "grin_viewability": "VIEW_METADATA",
                "grin_scannable": "false",
                "grin_ocr_gtd_score": "60",
            },
            "TEST003": {
                "grin_viewability": "VIEW_SNIPPET",
                "grin_scannable": "true",
                "grin_ocr_gtd_score": "85",
            },
        }

    @pytest.mark.asyncio
    async def test_enrich_single_batch(self, temp_db_with_books, mock_enrichment_data, mock_process_stage):
        """Test enriching a single batch of books"""
        mock_client = MockGRINEnrichmentClient(mock_enrichment_data)

        pipeline = GRINEnrichmentPipeline(
            directory="TestLibrary", db_path=temp_db_with_books, process_summary_stage=mock_process_stage
        )
        pipeline.grin_client = mock_client

        # Test the new _fetch_and_update method
        enriched_count = await pipeline._fetch_and_update(["TEST001", "TEST002"])

        assert enriched_count == 2
        assert mock_client.fetch_count == 1

        # Verify database was updated
        tracker = SQLiteProgressTracker(temp_db_with_books)
        book1 = await get_book_for_testing(tracker, "TEST001")
        assert book1.grin_viewability == "VIEW_FULL"
        assert book1.enrichment_timestamp is not None

    @pytest.mark.asyncio
    async def test_dynamic_url_batching(self, mock_process_stage):
        """Test that URL batches respect header size limits"""
        pipeline = GRINEnrichmentPipeline(
            directory="TestLibrary", db_path=":memory:", process_summary_stage=mock_process_stage
        )

        # Create barcodes of varying lengths
        short_barcodes = ["A" * 10] * 100  # 100 short barcodes
        long_barcodes = ["B" * 50] * 100  # 100 long barcodes

        short_batches = pipeline._create_url_batches(short_barcodes)
        long_batches = pipeline._create_url_batches(long_barcodes)

        # Short barcodes should fit more per batch than long ones
        assert len(short_batches) <= len(long_batches)

        # All barcodes should be included
        total_short = sum(len(batch) for batch in short_batches)
        total_long = sum(len(batch) for batch in long_batches)

        assert total_short == len(short_barcodes)
        assert total_long == len(long_barcodes)

    @pytest.mark.asyncio
    async def test_retry_on_failure(self, mock_process_stage):
        """Test retry logic for GRIN API failures"""
        mock_client = MockGRINEnrichmentClient()
        fail_count = 0

        async def failing_then_succeeding_fetch(directory, resource):
            nonlocal fail_count
            fail_count += 1
            if fail_count <= 2:  # Fail first 2 times
                raise Exception("Temporary network error")
            return "Barcode\nTEST001"  # Succeed on 3rd try

        mock_client.fetch_resource = failing_then_succeeding_fetch

        pipeline = GRINEnrichmentPipeline(
            directory="TestLibrary", db_path=":memory:", process_summary_stage=mock_process_stage
        )
        pipeline.grin_client = mock_client

        # Should succeed after retries
        result = await pipeline._fetch_grin_batch(["TEST001"])
        assert result == "Barcode\nTEST001"
        assert fail_count == 3  # Failed twice, succeeded on third

    @pytest.mark.asyncio
    async def test_partial_batch_failure(self, temp_db_with_books, mock_process_stage):
        """Test handling when some books in batch fail"""
        # Mock client that only has data for some books
        mock_client = MockGRINEnrichmentClient(
            {
                "TEST001": {"grin_viewability": "VIEW_FULL"}
                # TEST002 not in mock data
            }
        )

        pipeline = GRINEnrichmentPipeline(
            directory="TestLibrary", db_path=temp_db_with_books, process_summary_stage=mock_process_stage
        )
        pipeline.grin_client = mock_client

        # Should handle partial success gracefully
        enriched_count = await pipeline._fetch_and_update(["TEST001", "TEST002"])

        assert enriched_count == 1  # Only TEST001 had data

        # Both books should be marked as processed
        tracker = SQLiteProgressTracker(temp_db_with_books)
        book1 = await get_book_for_testing(tracker, "TEST001")
        book2 = await get_book_for_testing(tracker, "TEST002")

        assert book1.grin_viewability == "VIEW_FULL"
        assert book1.enrichment_timestamp is not None

        assert book2.grin_viewability is None  # No data available
        assert book2.enrichment_timestamp is not None  # But marked as processed

    @pytest.mark.asyncio
    async def test_url_batches_not_truncated(self, mock_process_stage):
        """Test that all URL batches are processed, not just max_concurrent_requests"""
        pipeline = GRINEnrichmentPipeline(
            directory="TestLibrary",
            db_path=":memory:",
            process_summary_stage=mock_process_stage,
            max_concurrent_requests=2,  # Small number to trigger the bug
        )

        # Create many long barcodes that will force multiple URL batches
        # Each barcode is ~200 characters to trigger URL length limits
        long_prefix = "x" * 180
        barcodes = [f"{long_prefix}_barcode_{i:04d}" for i in range(100)]

        # Get URL batches
        url_batches = pipeline._create_url_batches(barcodes)

        # Count total barcodes in all batches
        total_barcodes_in_batches = sum(len(batch) for batch in url_batches)

        # Before the fix: total_barcodes_in_batches would be less than len(barcodes)
        # After the fix: they should be equal
        assert total_barcodes_in_batches == len(barcodes), (
            f"Lost barcodes! Expected {len(barcodes)}, got {total_barcodes_in_batches}"
        )

        # Should create multiple batches due to URL length limits
        assert len(url_batches) > 1, "Should create multiple URL batches for long barcodes"

        # Should create more batches than max_concurrent_requests to test the fix
        assert len(url_batches) > pipeline.max_concurrent_requests, (
            f"Need more URL batches ({len(url_batches)}) than max_concurrent ({pipeline.max_concurrent_requests}) to test fix"
        )

    @pytest.mark.asyncio
    async def test_reset_enrichment(self, temp_db_with_books, mock_enrichment_data, mock_process_stage):
        """Test resetting enrichment data before re-enriching"""
        mock_client = MockGRINEnrichmentClient(mock_enrichment_data)

        pipeline = GRINEnrichmentPipeline(
            directory="TestLibrary", db_path=temp_db_with_books, process_summary_stage=mock_process_stage
        )
        pipeline.grin_client = mock_client

        # First enrich some books
        await pipeline._fetch_and_update(["TEST001", "TEST002"])

        # Verify enrichment data exists
        tracker = SQLiteProgressTracker(temp_db_with_books)
        book1 = await get_book_for_testing(tracker, "TEST001")
        assert book1.enrichment_timestamp is not None
        assert book1.grin_viewability == "VIEW_FULL"

        # Reset enrichment data
        reset_count = await pipeline.reset_enrichment_data()
        assert reset_count == 2

        # Verify enrichment data was cleared
        book1_after = await get_book_for_testing(tracker, "TEST001")
        assert book1_after.enrichment_timestamp is None
        assert book1_after.grin_viewability is None


if __name__ == "__main__":
    pytest.main([__file__])
