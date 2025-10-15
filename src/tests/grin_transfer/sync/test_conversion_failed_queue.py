#!/usr/bin/env python3
"""Tests for fetching conversion failures from GRIN's _failed endpoint."""

from unittest.mock import AsyncMock, Mock

import pytest

from grin_transfer.queue_utils import get_all_conversion_failed_books


class TestGetAllConversionFailedBooks:
    """Test fetching all conversion failures from GRIN."""

    @pytest.mark.asyncio
    async def test_returns_all_failures(self):
        """get_all_conversion_failed_books should return all failures from GRIN."""
        mock_client = AsyncMock()
        mock_client.fetch_resource = AsyncMock(return_value="<html></html>")
        mock_client._parse_books_from_html = Mock(
            return_value=[
                {
                    "barcode": "FAIL001",
                    "convert_failed_date": "2024-01-15",
                    "convert_failed_info": "Error: Timeout",
                    "detailed_convert_failed_info": "Process exceeded 3600s",
                },
                {
                    "barcode": "FAIL002",
                    "convert_failed_date": "2024-01-16",
                    "convert_failed_info": "Error: Out of memory",
                    "detailed_convert_failed_info": "OOM killed",
                },
            ]
        )

        result = await get_all_conversion_failed_books(mock_client, "test_lib")

        assert len(result) == 2
        assert "FAIL001" in result
        assert "FAIL002" in result
        assert result["FAIL001"]["grin_convert_failed_date"] == "2024-01-15"
        assert result["FAIL001"]["grin_convert_failed_info"] == "Error: Timeout"
        assert result["FAIL002"]["grin_convert_failed_date"] == "2024-01-16"

    @pytest.mark.asyncio
    async def test_empty_failed_endpoint(self):
        """get_all_conversion_failed_books should handle empty _failed endpoint."""
        mock_client = AsyncMock()
        mock_client.fetch_resource = AsyncMock(return_value="<html></html>")
        mock_client._parse_books_from_html = Mock(return_value=[])

        result = await get_all_conversion_failed_books(mock_client, "test_lib")

        assert len(result) == 0
        assert result == {}

    @pytest.mark.asyncio
    async def test_skips_entries_without_barcodes(self):
        """get_all_conversion_failed_books should skip entries without barcodes."""
        mock_client = AsyncMock()
        mock_client.fetch_resource = AsyncMock(return_value="<html></html>")
        mock_client._parse_books_from_html = Mock(
            return_value=[
                {
                    "convert_failed_date": "2024-01-01",
                    "convert_failed_info": "Error: Missing barcode",
                    # No barcode field
                },
                {
                    "barcode": "FAIL001",
                    "convert_failed_date": "2024-01-02",
                    "convert_failed_info": "Error: Valid entry",
                },
            ]
        )

        result = await get_all_conversion_failed_books(mock_client, "test_lib")

        assert len(result) == 1
        assert "FAIL001" in result

    @pytest.mark.asyncio
    async def test_handles_missing_metadata_fields(self):
        """get_all_conversion_failed_books should handle missing metadata fields with empty strings."""
        mock_client = AsyncMock()
        mock_client.fetch_resource = AsyncMock(return_value="<html></html>")
        mock_client._parse_books_from_html = Mock(
            return_value=[
                {
                    "barcode": "FAIL001",
                    # Missing date/info fields
                }
            ]
        )

        result = await get_all_conversion_failed_books(mock_client, "test_lib")

        assert len(result) == 1
        assert result["FAIL001"]["grin_convert_failed_date"] == ""
        assert result["FAIL001"]["grin_convert_failed_info"] == ""
        assert result["FAIL001"]["grin_detailed_convert_failed_info"] == ""

    @pytest.mark.asyncio
    async def test_correct_metadata_structure(self):
        """get_all_conversion_failed_books should return correct metadata structure."""
        mock_client = AsyncMock()
        mock_client.fetch_resource = AsyncMock(return_value="<html></html>")
        mock_client._parse_books_from_html = Mock(
            return_value=[
                {
                    "barcode": "FAIL001",
                    "convert_failed_date": "2024-01-15",
                    "convert_failed_info": "Error: Timeout",
                    "detailed_convert_failed_info": "Details here",
                }
            ]
        )

        result = await get_all_conversion_failed_books(mock_client, "test_lib")

        assert "FAIL001" in result
        metadata = result["FAIL001"]
        assert isinstance(metadata, dict)
        assert set(metadata.keys()) == {
            "grin_convert_failed_date",
            "grin_convert_failed_info",
            "grin_detailed_convert_failed_info",
        }

    @pytest.mark.asyncio
    async def test_calls_fetch_with_correct_params(self):
        """get_all_conversion_failed_books should fetch from _failed endpoint with result_count=-1."""
        mock_client = AsyncMock()
        mock_client.fetch_resource = AsyncMock(return_value="<html></html>")
        mock_client._parse_books_from_html = Mock(return_value=[])

        await get_all_conversion_failed_books(mock_client, "test_lib")

        mock_client.fetch_resource.assert_called_once_with("test_lib", "_failed?result_count=-1")
