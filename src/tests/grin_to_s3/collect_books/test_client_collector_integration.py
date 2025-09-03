"""
Test integration between client and collector to catch type mismatches.
"""

import tempfile
from pathlib import Path
from unittest.mock import AsyncMock, patch

import pytest

from grin_to_s3.client import GRINClient
from grin_to_s3.collect_books.collector import BookCollector
from grin_to_s3.collect_books.models import SQLiteProgressTracker
from grin_to_s3.run_config import RunConfig, StorageConfig, SyncConfig


def create_test_run_config(db_path: str) -> RunConfig:
    """Helper function to create a test RunConfig."""
    storage_config: StorageConfig = {
        "type": "local",
        "protocol": "file",
        "config": {"base_path": "/tmp/test"},
        "prefix": "test",
    }

    sync_config: SyncConfig = {
        "task_check_concurrency": 1,
        "task_download_concurrency": 1,
        "task_decrypt_concurrency": 1,
        "task_upload_concurrency": 1,
        "task_unpack_concurrency": 1,
        "task_extract_marc_concurrency": 1,
        "task_extract_ocr_concurrency": 1,
        "task_export_csv_concurrency": 1,
        "task_cleanup_concurrency": 1,
        "staging_dir": Path("/tmp/staging"),
        "disk_space_threshold": 0.8,
        "compression_meta_enabled": True,
        "compression_full_enabled": True,
    }

    return RunConfig(
        run_name="test_run",
        library_directory="Harvard",
        output_directory=Path("/tmp/output"),
        sqlite_db_path=Path(db_path),
        storage_config=storage_config,
        sync_config=sync_config,
        log_file=Path("/tmp/log.txt"),
        secrets_dir=None,
    )


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
                run_config = create_test_run_config(tmp_db.name)

                # Create collector and replace client
                collector = BookCollector(
                    directory="Harvard",
                    process_summary_stage=AsyncMock(),
                    storage_config=run_config.storage_config,
                    run_config=run_config,
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
            run_config = create_test_run_config(tmp_db.name)

            collector = BookCollector(
                directory="Harvard",
                process_summary_stage=AsyncMock(),
                storage_config=run_config.storage_config,
                run_config=run_config,
            )
            collector.grin_client = client
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


@pytest.mark.asyncio
async def test_get_all_books_limit_functionality():
    """Test that get_all_books respects limit parameter across both phases."""

    client = GRINClient()

    # Mock phase 1 (converted books) to return 3 books
    async def mock_converted_generator():
        yield {"barcode": "conv_001", "title": "Converted Book 1"}, set()
        yield {"barcode": "conv_002", "title": "Converted Book 2"}, set()
        yield {"barcode": "conv_003", "title": "Converted Book 3"}, set()

    # Mock phase 2 (non-converted books) to return 5 books
    async def mock_all_books_generator():
        yield {"barcode": "book_001", "title": "Non-Converted Book 1"}, set()
        yield {"barcode": "book_002", "title": "Non-Converted Book 2"}, set()
        yield {"barcode": "book_003", "title": "Non-Converted Book 3"}, set()
        yield {"barcode": "book_004", "title": "Non-Converted Book 4"}, set()
        yield {"barcode": "book_005", "title": "Non-Converted Book 5"}, set()

    with tempfile.NamedTemporaryFile(suffix=".db", delete=False) as tmp_db:
        run_config = create_test_run_config(tmp_db.name)
        run_config = RunConfig(
            run_name="test_run",
            library_directory="TestLibrary",
            output_directory=Path("/tmp/output"),
            sqlite_db_path=Path(tmp_db.name),
            storage_config=run_config.storage_config,
            sync_config=run_config.sync_config,
            log_file=Path("/tmp/log.txt"),
            secrets_dir=None,
        )

        collector = BookCollector(
            directory="TestLibrary",
            process_summary_stage=AsyncMock(),
            storage_config=run_config.storage_config,
            run_config=run_config,
        )
        collector.grin_client = client
        collector.sqlite_tracker = SQLiteProgressTracker(tmp_db.name)

        try:
            # Test case 1: Limit = 2 (should get 2 from phase 1, 0 from phase 2)
            with patch.object(collector, "get_converted_books_html", return_value=mock_converted_generator()):
                with patch.object(collector, "get_all_books_html", return_value=mock_all_books_generator()):
                    books = []
                    async for book_data, _ in collector.get_all_books(limit=2):
                        books.append(book_data)

                    assert len(books) == 2
                    assert books[0]["barcode"] == "conv_001"
                    assert books[1]["barcode"] == "conv_002"

            # Test case 2: Limit = 5 (should get 3 from phase 1, 2 from phase 2)
            with patch.object(collector, "get_converted_books_html", return_value=mock_converted_generator()):
                with patch.object(collector, "get_all_books_html", return_value=mock_all_books_generator()):
                    books = []
                    async for book_data, _ in collector.get_all_books(limit=5):
                        books.append(book_data)

                    assert len(books) == 5
                    # First 3 should be from phase 1 (converted)
                    assert books[0]["barcode"] == "conv_001"
                    assert books[1]["barcode"] == "conv_002"
                    assert books[2]["barcode"] == "conv_003"
                    # Next 2 should be from phase 2 (non-converted)
                    assert books[3]["barcode"] == "book_001"
                    assert books[4]["barcode"] == "book_002"

            # Test case 3: Limit = 10 (should get all 8 books: 3 + 5)
            with patch.object(collector, "get_converted_books_html", return_value=mock_converted_generator()):
                with patch.object(collector, "get_all_books_html", return_value=mock_all_books_generator()):
                    books = []
                    async for book_data, _ in collector.get_all_books(limit=10):
                        books.append(book_data)

                    assert len(books) == 8  # Only 8 books total available
                    # All 3 converted books
                    assert books[0]["barcode"] == "conv_001"
                    assert books[1]["barcode"] == "conv_002"
                    assert books[2]["barcode"] == "conv_003"
                    # All 5 non-converted books
                    assert books[3]["barcode"] == "book_001"
                    assert books[4]["barcode"] == "book_002"
                    assert books[5]["barcode"] == "book_003"
                    assert books[6]["barcode"] == "book_004"
                    assert books[7]["barcode"] == "book_005"

            # Test case 4: No limit (should get all books)
            with patch.object(collector, "get_converted_books_html", return_value=mock_converted_generator()):
                with patch.object(collector, "get_all_books_html", return_value=mock_all_books_generator()):
                    books = []
                    async for book_data, _ in collector.get_all_books():
                        books.append(book_data)

                    assert len(books) == 8  # All available books

        finally:
            await collector.sqlite_tracker.close()
            import os

            os.unlink(tmp_db.name)
