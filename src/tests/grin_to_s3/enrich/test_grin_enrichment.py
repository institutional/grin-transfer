#!/usr/bin/env python3
"""
Unit tests for GRIN enrichment functionality
"""

import os
import sys
import tempfile

import pytest
import pytest_asyncio

sys.path.append(os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))

from grin_to_s3.collect_books.models import BookRecord, SQLiteProgressTracker
from grin_to_s3.metadata.grin_enrichment import GRINEnrichmentPipeline
from tests.mocks import MockGRINClient


class MockGRINEnrichmentClient(MockGRINClient):
    """Mock GRIN client specifically for enrichment testing"""

    def __init__(self, test_enrichment_data: dict = None):
        super().__init__()
        self.test_enrichment_data = test_enrichment_data or {}

    async def fetch_resource(self, directory: str, resource: str):
        """Return mock enrichment data based on requested barcodes"""
        if "_barcode_search" in resource:
            # Extract barcodes from the resource URL
            if "barcodes=" in resource:
                barcodes_param = resource.split("barcodes=")[1].split("&")[0]
                requested_barcodes = barcodes_param.split("%20")  # URL encoded spaces
                if len(requested_barcodes) == 1:
                    requested_barcodes = barcodes_param.split(" ")

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
                    if barcode in self.test_enrichment_data:
                        data = self.test_enrichment_data[barcode]
                        # Create full TSV row with defaults
                        row = [
                            barcode,  # Barcode
                            "",  # Check-In Date
                            data.get("grin_state", ""),  # State
                            data.get("grin_viewability", "VIEW_METADATA"),  # Viewability
                            data.get("grin_conditions", ""),  # Conditions
                            data.get("grin_scannable", "false"),  # Scannable
                            data.get("grin_opted_out", "false"),  # Opted-Out
                            data.get("grin_tagging", "true"),  # Tagging
                            data.get("grin_audit", ""),  # Audit
                            data.get("grin_material_error_percent", "0%"),  # Material Error%
                            data.get("grin_overall_error_percent", "0%"),  # Overall Error%
                            "",  # Scanned Date
                            "",  # Processed Date
                            "",  # Analyzed Date
                            "",  # Converted Date
                            "",  # Allow Download Updated Date
                            "",  # Viewability Updated Date
                            "",  # Source Library Bibkey
                            "",  # Rubbish
                            "",  # Downloaded Date
                            data.get("grin_claimed", "false"),  # Claimed
                            data.get("grin_ocr_gtd_score", "75"),  # OCR GTD Score
                            data.get("grin_ocr_analysis_score", "80"),  # OCR Analysis Score
                            data.get("grin_digitization_method", "NON_DESTRUCTIVE"),  # Digitization Method
                            "",  # OCR'd Date
                        ]
                        lines.append("\t".join(row))

                return "\n".join(lines)

        return super().fetch_resource(directory, resource)


class TestGRINEnrichmentPipeline:
    """Test GRIN enrichment pipeline functionality."""

    @pytest_asyncio.fixture
    async def temp_db(self):
        """Create a temporary database with test books"""
        temp_file = tempfile.NamedTemporaryFile(suffix=".db", delete=False)
        temp_file.close()

        db_path = temp_file.name
        tracker = SQLiteProgressTracker(db_path)

        # Initialize database
        await tracker.init_db()

        # Add some test books
        test_books = [
            BookRecord(barcode="TEST001", title="Test Book 1"),
            BookRecord(barcode="TEST002", title="Test Book 2"),
            BookRecord(barcode="TEST003", title="Test Book 3"),
        ]

        for book in test_books:
            await tracker.save_book(book)
            # Add processing status using status history
            await tracker.add_status_change(book.barcode, "processing_request", "converted")

        yield db_path

        # Cleanup
        try:
            os.unlink(db_path)
        except Exception:
            pass

    @pytest.fixture
    def mock_enrichment_data(self):
        """Test enrichment data"""
        return {
            "TEST001": {
                "grin_viewability": "VIEW_FULL",
                "grin_scannable": "true",
                "grin_ocr_gtd_score": "95",
                "grin_ocr_analysis_score": "98",
            },
            "TEST002": {
                "grin_viewability": "VIEW_METADATA",
                "grin_scannable": "false",
                "grin_ocr_gtd_score": "60",
                "grin_ocr_analysis_score": "65",
            },
            "TEST003": {
                "grin_viewability": "VIEW_SNIPPET",
                "grin_scannable": "true",
                "grin_ocr_gtd_score": "85",
                "grin_ocr_analysis_score": "88",
            },
        }

    @pytest.mark.asyncio
    async def test_fetch_grin_metadata_batch_single(self, mock_enrichment_data, mock_process_stage):
        """Test fetching enrichment data for a single barcode"""
        mock_client = MockGRINEnrichmentClient(mock_enrichment_data)

        pipeline = GRINEnrichmentPipeline(
            directory="TestLibrary", db_path=":memory:", process_summary_stage=mock_process_stage
        )
        pipeline.grin_client = mock_client

        # Test single barcode batch
        result = await pipeline.fetch_grin_metadata_batch(["TEST001"])

        assert "TEST001" in result
        assert result["TEST001"]["grin_viewability"] == "VIEW_FULL"
        assert result["TEST001"]["grin_scannable"] == "true"
        assert result["TEST001"]["grin_ocr_gtd_score"] == "95"
        assert result["TEST001"]["grin_ocr_analysis_score"] == "98"

    @pytest.mark.asyncio
    async def test_fetch_grin_metadata_batch_multiple(self, mock_enrichment_data, mock_process_stage):
        """Test fetching enrichment data for multiple barcodes"""
        mock_client = MockGRINEnrichmentClient(mock_enrichment_data)

        pipeline = GRINEnrichmentPipeline(
            directory="TestLibrary", db_path=":memory:", process_summary_stage=mock_process_stage
        )
        pipeline.grin_client = mock_client

        # Test multiple barcode batch
        result = await pipeline.fetch_grin_metadata_batch(["TEST001", "TEST002", "TEST003"])

        assert len(result) == 3
        assert result["TEST001"]["grin_viewability"] == "VIEW_FULL"
        assert result["TEST002"]["grin_viewability"] == "VIEW_METADATA"
        assert result["TEST003"]["grin_viewability"] == "VIEW_SNIPPET"

    @pytest.mark.asyncio
    async def test_fetch_grin_metadata_batch_missing_barcode(self, mock_enrichment_data, mock_process_stage):
        """Test handling of missing barcodes in batch response"""
        mock_client = MockGRINEnrichmentClient(mock_enrichment_data)

        pipeline = GRINEnrichmentPipeline(
            directory="TestLibrary", db_path=":memory:", process_summary_stage=mock_process_stage
        )
        pipeline.grin_client = mock_client

        # Test with a barcode not in mock data
        result = await pipeline.fetch_grin_metadata_batch(["TEST001", "MISSING"])

        assert len(result) == 2
        assert result["TEST001"]["grin_viewability"] == "VIEW_FULL"
        assert result["MISSING"] is None

    @pytest.mark.asyncio
    async def test_fetch_grin_metadata_batch_header_value_mismatch(self, mock_process_stage):
        """Test handling of header/value mismatch (padding)"""
        # Create mock client that returns fewer values than headers
        mock_client = MockGRINEnrichmentClient()

        async def mock_fetch_resource(directory, resource):
            if "_barcode_search" in resource:
                # Return TSV with 25 headers but only 21 values (missing last 4)
                headers = "\t".join(
                    [
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
                )

                # Only 21 values (missing last 4)
                values = "\t".join(
                    [
                        "TEST001",
                        "",
                        "",
                        "VIEW_METADATA",
                        "",
                        "false",
                        "false",
                        "true",
                        "",
                        "0%",
                        "5%",
                        "",
                        "",
                        "",
                        "",
                        "",
                        "",
                        "",
                        "",
                        "",
                        "false",
                    ]
                )

                return f"{headers}\n{values}"
            return ""

        mock_client.fetch_resource = mock_fetch_resource

        pipeline = GRINEnrichmentPipeline(
            directory="TestLibrary", db_path=":memory:", process_summary_stage=mock_process_stage
        )
        pipeline.grin_client = mock_client

        # This should handle the mismatch gracefully by padding
        result = await pipeline.fetch_grin_metadata_batch(["TEST001"])

        assert "TEST001" in result
        assert result["TEST001"]["grin_viewability"] == "VIEW_METADATA"
        assert result["TEST001"]["grin_overall_error_percent"] == "5%"
        # Missing fields should be empty
        assert result["TEST001"]["grin_ocr_gtd_score"] == ""
        assert result["TEST001"]["grin_ocr_analysis_score"] == ""

    @pytest.mark.asyncio
    async def test_enrich_books_batch(self, temp_db, mock_enrichment_data, mock_process_stage):
        """Test enriching a batch of books"""
        mock_client = MockGRINEnrichmentClient(mock_enrichment_data)

        pipeline = GRINEnrichmentPipeline(
            directory="TestLibrary", db_path=temp_db, process_summary_stage=mock_process_stage
        )
        pipeline.grin_client = mock_client

        # Enrich all books
        barcodes = ["TEST001", "TEST002", "TEST003"]
        enriched_count = await pipeline.enrich_books_batch(barcodes)

        assert enriched_count == 3

        # Verify books were updated in database
        tracker = SQLiteProgressTracker(temp_db)

        book1 = await tracker.get_book("TEST001")
        assert book1.grin_viewability == "VIEW_FULL"
        assert book1.enrichment_timestamp is not None

        book2 = await tracker.get_book("TEST002")
        assert book2.grin_viewability == "VIEW_METADATA"
        assert book2.enrichment_timestamp is not None

        book3 = await tracker.get_book("TEST003")
        assert book3.grin_viewability == "VIEW_SNIPPET"
        assert book3.enrichment_timestamp is not None

    @pytest.mark.asyncio
    async def test_reset_enrichment_data(self, temp_db, mock_enrichment_data, mock_process_stage):
        """Test resetting enrichment data"""
        mock_client = MockGRINEnrichmentClient(mock_enrichment_data)

        pipeline = GRINEnrichmentPipeline(
            directory="TestLibrary", db_path=temp_db, process_summary_stage=mock_process_stage
        )
        pipeline.grin_client = mock_client

        # First enrich some books
        barcodes = ["TEST001", "TEST002"]
        await pipeline.enrich_books_batch(barcodes)

        # Verify enrichment data exists
        tracker = SQLiteProgressTracker(temp_db)
        book1 = await tracker.get_book("TEST001")
        assert book1.enrichment_timestamp is not None
        assert book1.grin_viewability == "VIEW_FULL"

        # Reset enrichment data
        reset_count = await pipeline.reset_enrichment_data()
        assert reset_count == 2

        # Verify enrichment data was cleared
        book1_after = await tracker.get_book("TEST001")
        assert book1_after.enrichment_timestamp is None
        assert book1_after.grin_viewability is None

    @pytest.mark.asyncio
    async def test_dynamic_batch_size_splitting(self, temp_db, mock_enrichment_data, mock_process_stage):
        """Test that batches use dynamic sizing based on URL length"""
        mock_client = MockGRINEnrichmentClient(mock_enrichment_data)

        # Track calls to fetch_resource
        call_count = 0
        original_fetch = mock_client.fetch_resource

        async def counting_fetch_resource(directory, resource):
            nonlocal call_count
            call_count += 1
            return await original_fetch(directory, resource)

        mock_client.fetch_resource = counting_fetch_resource

        pipeline = GRINEnrichmentPipeline(
            directory="TestLibrary", db_path=temp_db, process_summary_stage=mock_process_stage
        )
        pipeline.grin_client = mock_client

        # Process 3 books - with dynamic sizing, should fit in one request for small batches
        barcodes = ["TEST001", "TEST002", "TEST003"]
        enriched_count = await pipeline.enrich_books_batch(barcodes)

        assert enriched_count == 3
        assert call_count == 1  # Should fit in one GRIN request for small batches

    @pytest.mark.asyncio
    async def test_pipeline_initialization(self, mock_process_stage):
        """Test pipeline initialization with various parameters"""
        pipeline = GRINEnrichmentPipeline(
            directory="TestDir",
            process_summary_stage=mock_process_stage,
            db_path="/test/path.db",
            rate_limit_delay=0.5,
            batch_size=500,
            timeout=30,
        )

        assert pipeline.directory == "TestDir"
        assert pipeline.db_path == "/test/path.db"
        assert pipeline.rate_limiter.requests_per_second == 2.0  # 1/0.5 = 2.0
        assert pipeline.batch_size == 500
        assert pipeline.timeout == 30

    @pytest.mark.asyncio
    async def test_error_handling_in_batch_fetch(self, temp_db, mock_process_stage):
        """Test error handling when GRIN request fails"""
        mock_client = MockGRINEnrichmentClient()

        # Make fetch_resource raise an exception
        async def failing_fetch_resource(directory, resource):
            raise Exception("Network error")

        mock_client.fetch_resource = failing_fetch_resource

        pipeline = GRINEnrichmentPipeline(
            directory="TestLibrary", db_path=temp_db, process_summary_stage=mock_process_stage
        )
        pipeline.grin_client = mock_client

        # This should handle the error gracefully
        result = await pipeline.fetch_grin_metadata_batch(["TEST001"])

        assert result["TEST001"] is None

    @pytest.mark.asyncio
    async def test_error_handling_in_enrich_batch(self, temp_db, mock_process_stage):
        """Test error handling when enrichment processing fails"""
        mock_client = MockGRINEnrichmentClient({"TEST001": {"grin_viewability": "VIEW_FULL"}})

        # Make fetch_resource fail
        async def failing_fetch_resource(directory, resource):
            raise Exception("Network error")

        mock_client.fetch_resource = failing_fetch_resource

        pipeline = GRINEnrichmentPipeline(
            directory="TestLibrary", db_path=temp_db, process_summary_stage=mock_process_stage
        )
        pipeline.grin_client = mock_client

        # Should handle errors gracefully and still mark books as processed
        enriched_count = await pipeline.enrich_books_batch(["TEST001"])

        assert enriched_count == 0  # No successful enrichments

        # But book should still be marked as processed (with empty enrichment)
        tracker = SQLiteProgressTracker(temp_db)
        book = await tracker.get_book("TEST001")
        assert book.enrichment_timestamp is not None  # Marked as processed


class TestEnrichmentDataExtraction:
    """Test enrichment data extraction and mapping"""

    def test_enrichment_field_mapping(self, mock_process_stage):
        """Test that TSV data is correctly mapped to enrichment fields"""
        GRINEnrichmentPipeline(directory="TestLibrary", db_path=":memory:", process_summary_stage=mock_process_stage)

        # Mock TSV data
        headers = [
            "Barcode",
            "State",
            "Viewability",
            "Opted-Out (post-scan)",
            "Conditions",
            "Scannable",
            "Tagging",
            "Audit",
            "Material Error%",
            "Overall Error%",
            "Claimed",
            "OCR Analysis Score",
            "OCR GTD Score",
            "Digitization Method",
        ]

        values = [
            "TEST001",
            "ACTIVE",
            "VIEW_FULL",
            "false",
            "GOOD",
            "true",
            "true",
            "PASSED",
            "2%",
            "1%",
            "false",
            "95",
            "98",
            "NON_DESTRUCTIVE",
        ]

        data_map = dict(zip(headers, values, strict=False))

        # Test the field extraction logic using BookRecord's GRIN TSV mapping
        from grin_to_s3.collect_books.models import BookRecord

        grin_tsv_mapping = BookRecord.get_grin_tsv_column_mapping()

        enrichment_data = {}
        for grin_tsv_column, field_name in grin_tsv_mapping.items():
            enrichment_data[field_name] = data_map.get(grin_tsv_column, "")

        # Test a few key fields to verify the mapping works correctly
        assert enrichment_data["grin_state"] == "ACTIVE"
        assert enrichment_data["grin_viewability"] == "VIEW_FULL"
        assert enrichment_data["grin_material_error_percent"] == "2%"
        assert enrichment_data["grin_ocr_analysis_score"] == "95"
        assert enrichment_data["grin_digitization_method"] == "NON_DESTRUCTIVE"

        # Verify all 18 enrichment fields are present in the mapping
        assert len(enrichment_data) == 18


if __name__ == "__main__":
    pytest.main([__file__])
