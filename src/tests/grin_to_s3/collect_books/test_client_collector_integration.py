"""
Test integration between client and collector to catch type mismatches.
"""

import tempfile
from unittest.mock import AsyncMock, patch

import pytest

from grin_to_s3.client import GRINClient
from grin_to_s3.collect_books.collector import BookCollector
from grin_to_s3.collect_books.models import SQLiteProgressTracker


@pytest.mark.asyncio
async def test_client_collector_integration():
    """Test that collector can handle GRINRow dicts from client."""

    # Mock HTML response that would produce a GRINRow
    mock_html = """
    <table>
        <thead>
            <tr>
                <th>Title</th>
                <th>Date</th>
                <th>Status</th>
            </tr>
        </thead>
        <tbody>
            <tr>
                <td>test_barcode_123</td>
                <td>Test Book Title</td>
                <td>2024-01-01</td>
                <td>converted</td>
            </tr>
        </tbody>
    </table>
    """

    # Create a real client instance
    client = GRINClient()

    # Mock the HTML parsing to return our test data
    with patch.object(client, "_parse_books_from_html") as mock_parse:
        # This should return GRINRow dicts, not strings
        mock_parse.return_value = [{"barcode": "test_barcode_123", "title": "Test Book Title", "date": "2024-01-01"}]

        # Mock the prefetch and network calls
        with (
            patch.object(client, "_prefetch_page"),
            patch.object(client.auth, "make_authenticated_request") as mock_request,
        ):
            # Mock response
            mock_response = AsyncMock()
            mock_response.text.return_value = mock_html
            mock_request.return_value = mock_response

            # Create a temporary database for real SQLiteProgressTracker
            with tempfile.NamedTemporaryFile(suffix=".db", delete=False) as tmp_db:
                # Create collector and replace client
                collector = BookCollector(
                    directory="Harvard",
                    process_summary_stage=AsyncMock(),
                    storage_config={"type": "local", "config": {"base_path": "/tmp/test"}, "prefix": "test"},
                )
                collector.grin_client = client
                collector.sqlite_tracker = SQLiteProgressTracker(tmp_db.name)

                try:
                    # Test that collector can handle GRINRow from get_converted_books_html
                    books = []
                    async for book_data, _known_barcodes in collector.get_converted_books_html():
                        books.append(book_data)
                        if len(books) >= 1:  # Just test one book
                            break

                    # This should not fail with 'dict' object has no attribute 'strip'
                    assert len(books) == 1
                    book_data = books[0]

                    # The collector should now handle GRINRow dicts properly
                    # Either by converting them to strings or handling them directly
                    assert isinstance(book_data, str | dict)

                    if isinstance(book_data, dict):
                        assert "barcode" in book_data
                        assert book_data["barcode"] == "test_barcode_123"
                    else:
                        # If it's still a string, it should contain the barcode
                        assert "test_barcode_123" in book_data
                finally:
                    # Clean up database connection
                    await collector.sqlite_tracker.close()
                    import os

                    os.unlink(tmp_db.name)


@pytest.mark.asyncio
async def test_collector_stream_all_books_integration():
    """Test the full stream_all_books method with GRINRow integration."""

    client = GRINClient()

    with patch.object(client, "stream_book_list_html_prefetch") as mock_stream:
        # Mock returning GRINRow dicts
        async def mock_generator():
            yield {"barcode": "test_123", "title": "Test Book"}, set()

        mock_stream.return_value = mock_generator()

        # Create a temporary database for real SQLiteProgressTracker
        with tempfile.NamedTemporaryFile(suffix=".db", delete=False) as tmp_db:
            collector = BookCollector(
                directory="Harvard",
                process_summary_stage=AsyncMock(),
                storage_config={"type": "local", "config": {"base_path": "/tmp/test"}, "prefix": "test"},
            )
            collector.client = client
            collector.sqlite_tracker = SQLiteProgressTracker(tmp_db.name)

            try:
                # This should not crash with attribute errors (including missing load_known_barcodes_batch)
                books = []
                async for book_data, _known_barcodes in collector.get_all_books():
                    books.append(book_data)
                    if len(books) >= 1:
                        break

                assert len(books) == 1
                book_data = books[0]

                # Should handle the GRINRow properly
                assert isinstance(book_data, dict)
                assert "barcode" in book_data
            finally:
                # Clean up database connection
                await collector.sqlite_tracker.close()
                import os

                os.unlink(tmp_db.name)
