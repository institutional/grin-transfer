#!/usr/bin/env python3
"""
Shared mock classes for testing CSV export functionality without network calls
"""

import random
from pathlib import Path

from grin_to_s3.collect_books.collector import BookCollector
from grin_to_s3.collect_books.config import ExportConfig
from grin_to_s3.process_summary import ProcessStageMetrics


def _generate_title(seed: int) -> str:
    """Generate a consistent random title using the given seed."""
    random.seed(seed)
    adjectives = ["Advanced", "Complete", "Modern", "Essential", "Practical", "Comprehensive", "Professional", "Strategic"]
    nouns = ["Methods", "Principles", "Systems", "Analysis", "Framework", "Theory", "Research", "Studies"]
    return f"{random.choice(adjectives)} {random.choice(nouns)}"


class MockAuth:
    """Mock authentication for testing"""

    async def validate_credentials(self, directory=None):
        """Always return successful validation"""
        return True


class MockGRINClient:
    """Mock GRIN client for controlled testing without network calls"""

    def __init__(self, test_data: list[dict[str, str]] = None):
        self.test_data = test_data or []
        self.auth = MockAuth()

    async def stream_book_list(self, directory: str, list_type: str, mode_all: bool = False):
        """Yield test data as GRINRow dicts"""
        for grin_row in self.test_data:
            yield grin_row

    async def stream_book_list_html(
        self,
        directory: str,
        list_type: str,
        page_size: int = 1000,
        max_pages: int = 1000,
        start_page: int = 1,
        start_url: str = None,
        pagination_callback=None,
    ):
        """Yield test data as GRINRow dicts using HTML pagination"""
        for grin_row in self.test_data:
            yield grin_row

    async def stream_book_list_html_prefetch(
        self,
        directory: str,
        list_type: str,
        page_size: int = 1000,
        max_pages: int = 1000,
        start_page: int = 1,
        start_url: str = None,
        pagination_callback=None,
        sqlite_tracker=None,
    ):
        """Yield test data as GRINRow dicts using HTML pagination with prefetch (mock version)"""
        # Mock the prefetch version - yields (grin_row_dict, known_barcodes_set) tuples
        for grin_row in self.test_data:
            yield grin_row, set()  # Empty set for known barcodes since this is a mock

    async def fetch_resource(self, directory: str, resource: str):
        """Return mock processing state data"""
        if "_converted" in resource:
            # Return some test barcodes as converted
            return "\n".join(
                ["TEST001\t2023-01-01 12:00\t2023-01-02 12:00", "TEST003\t2023-01-01 12:00\t2023-01-02 12:00"]
            )
        elif "_failed" in resource:
            # Return some test barcodes as failed
            return "TEST005\t2023-01-01 12:00\tFailure reason"
        return ""


class MockBookStorage:
    """Mock storage for testing"""

    def __init__(self):
        self.archived_barcodes = {"TEST001", "TEST003", "TEST007"}
        self.json_barcodes = {"TEST001", "TEST002"}

    async def archive_exists(self, barcode: str) -> bool:
        return barcode in self.archived_barcodes

    def _book_path(self, barcode: str, filename: str) -> str:
        return f"mock/{barcode}/{filename}"


class MockStorage:
    """Mock Storage for testing"""

    def __init__(self):
        self.files = {
            "mock/TEST001/TEST001.tar.gz.gpg.retrieval": "2023-01-01T12:00:00Z",
            "mock/TEST003/TEST003.tar.gz.gpg.retrieval": "2023-01-02T12:00:00Z",
        }

    async def read_text(self, path: str) -> str:
        if path in self.files:
            return self.files[path]
        raise FileNotFoundError(f"Mock file not found: {path}")

    async def exists(self, path: str) -> bool:
        return path in self.files


def get_test_data():
    """Standard test data for consistent testing - kept small for speed"""
    return [
        {"barcode": "TEST001", "title": _generate_title(1001), "scanned_date": "2023-01-01 12:00", "converted_date": "2023-01-02 12:00", "processed_date": "2023-01-03 12:00", "ocr_date": "2023-01-04 12:00", "google_books_link": "https://books.google.com/books?id=test001"},
        {"barcode": "TEST002", "title": _generate_title(1002), "scanned_date": "2023-01-01 12:00", "converted_date": "2023-01-02 12:00", "processed_date": "2023-01-03 12:00", "ocr_date": "2023-01-04 12:00", "google_books_link": "https://books.google.com/books?id=test002"},
        {"barcode": "TEST003", "title": _generate_title(1003), "scanned_date": "2023-01-01 12:00", "converted_date": "2023-01-02 12:00", "processed_date": "2023-01-03 12:00", "ocr_date": "2023-01-04 12:00", "google_books_link": "https://books.google.com/books?id=test003"},
        {"barcode": "TEST004", "title": _generate_title(1004), "scanned_date": "2023-01-01 12:00", "converted_date": "2023-01-02 12:00", "processed_date": "2023-01-03 12:00", "ocr_date": "2023-01-04 12:00", "google_books_link": "https://books.google.com/books?id=test004"},
        {"barcode": "TEST005", "title": _generate_title(1005), "scanned_date": "2023-01-01 12:00", "converted_date": "2023-01-02 12:00", "processed_date": "2023-01-03 12:00", "ocr_date": "2023-01-04 12:00", "google_books_link": "https://books.google.com/books?id=test005"},
    ]


def get_large_test_data():
    """Larger test data for specific tests that need more records"""
    return [
        {"barcode": "TEST001", "title": _generate_title(2001), "scanned_date": "2023-01-01 12:00", "converted_date": "2023-01-02 12:00", "processed_date": "2023-01-03 12:00", "ocr_date": "2023-01-04 12:00", "google_books_link": "https://books.google.com/books?id=test001"},
        {"barcode": "TEST002", "title": _generate_title(2002), "scanned_date": "2023-01-01 12:00", "converted_date": "2023-01-02 12:00", "processed_date": "2023-01-03 12:00", "ocr_date": "2023-01-04 12:00", "google_books_link": "https://books.google.com/books?id=test002"},
        {"barcode": "TEST003", "title": _generate_title(2003), "scanned_date": "2023-01-01 12:00", "converted_date": "2023-01-02 12:00", "processed_date": "2023-01-03 12:00", "ocr_date": "2023-01-04 12:00", "google_books_link": "https://books.google.com/books?id=test003"},
        {"barcode": "TEST004", "title": _generate_title(2004), "scanned_date": "2023-01-01 12:00", "converted_date": "2023-01-02 12:00", "processed_date": "2023-01-03 12:00", "ocr_date": "2023-01-04 12:00", "google_books_link": "https://books.google.com/books?id=test004"},
        {"barcode": "TEST005", "title": _generate_title(2005), "scanned_date": "2023-01-01 12:00", "converted_date": "2023-01-02 12:00", "processed_date": "2023-01-03 12:00", "ocr_date": "2023-01-04 12:00", "google_books_link": "https://books.google.com/books?id=test005"},
        {"barcode": "TEST006", "title": _generate_title(2006), "scanned_date": "2023-01-01 12:00", "converted_date": "2023-01-02 12:00", "processed_date": "2023-01-03 12:00", "ocr_date": "2023-01-04 12:00", "google_books_link": "https://books.google.com/books?id=test006"},
        {"barcode": "TEST007", "title": _generate_title(2007), "scanned_date": "2023-01-01 12:00", "converted_date": "2023-01-02 12:00", "processed_date": "2023-01-03 12:00", "ocr_date": "2023-01-04 12:00", "google_books_link": "https://books.google.com/books?id=test007"},
        {"barcode": "TEST008", "title": _generate_title(2008), "scanned_date": "2023-01-01 12:00", "converted_date": "2023-01-02 12:00", "processed_date": "2023-01-03 12:00", "ocr_date": "2023-01-04 12:00", "google_books_link": "https://books.google.com/books?id=test008"},
        {"barcode": "TEST009", "title": _generate_title(2009), "scanned_date": "2023-01-01 12:00", "converted_date": "2023-01-02 12:00", "processed_date": "2023-01-03 12:00", "ocr_date": "2023-01-04 12:00", "google_books_link": "https://books.google.com/books?id=test009"},
        {"barcode": "TEST010", "title": _generate_title(2010), "scanned_date": "2023-01-01 12:00", "converted_date": "2023-01-02 12:00", "processed_date": "2023-01-03 12:00", "ocr_date": "2023-01-04 12:00", "google_books_link": "https://books.google.com/books?id=test010"},
    ]


def get_html_test_data():
    """HTML format test data matching GRIN HTML table output"""
    return [
        {"barcode": "HTML001", "title": _generate_title(3001), "scanned_date": "2023-01-01 12:00", "converted_date": "2023-01-05 14:30", "processed_date": "2023-01-02 12:00", "analyzed_date": "2023-01-03 12:00"},
        {"barcode": "HTML002", "title": _generate_title(3002), "scanned_date": "2023-01-01 12:00", "converted_date": "2023-01-06 09:15", "processed_date": "2023-01-02 12:00", "analyzed_date": "2023-01-03 12:00"},
        {"barcode": "HTML003", "title": _generate_title(3003), "scanned_date": "2023-01-01 12:00", "converted_date": "2023-01-07 16:45", "processed_date": "2023-01-02 12:00", "analyzed_date": "2023-01-03 12:00"},
        {"barcode": "HTML004", "title": _generate_title(3004), "scanned_date": "2023-01-01 12:00", "converted_date": "2023-01-08 11:20", "processed_date": "2023-01-02 12:00", "analyzed_date": "2023-01-03 12:00"},
        {"barcode": "HTML005", "title": _generate_title(3005), "scanned_date": "2023-01-01 12:00", "converted_date": "2023-01-09 13:45", "processed_date": "2023-01-02 12:00", "analyzed_date": "2023-01-03 12:00"},
    ]


def get_large_html_test_data():
    """Larger HTML format test data for tests that need more records"""
    return [
        {"barcode": "HTML001", "title": _generate_title(4001), "scanned_date": "2023-01-01 12:00", "converted_date": "2023-01-05 14:30", "processed_date": "2023-01-02 12:00", "analyzed_date": "2023-01-03 12:00"},
        {"barcode": "HTML002", "title": _generate_title(4002), "scanned_date": "2023-01-01 12:00", "converted_date": "2023-01-06 09:15", "processed_date": "2023-01-02 12:00", "analyzed_date": "2023-01-03 12:00"},
        {"barcode": "HTML003", "title": _generate_title(4003), "scanned_date": "2023-01-01 12:00", "converted_date": "2023-01-07 16:45", "processed_date": "2023-01-02 12:00", "analyzed_date": "2023-01-03 12:00"},
        {"barcode": "HTML004", "title": _generate_title(4004), "scanned_date": "2023-01-01 12:00", "converted_date": "2023-01-08 11:20", "processed_date": "2023-01-02 12:00", "analyzed_date": "2023-01-03 12:00"},
        {"barcode": "HTML005", "title": _generate_title(4005), "scanned_date": "2023-01-01 12:00", "converted_date": "2023-01-09 13:45", "processed_date": "2023-01-02 12:00", "analyzed_date": "2023-01-03 12:00"},
        {"barcode": "HTML006", "title": _generate_title(4006), "scanned_date": "2023-01-01 12:00", "converted_date": "2023-01-10 10:30", "processed_date": "2023-01-02 12:00", "analyzed_date": "2023-01-03 12:00"},
        {"barcode": "HTML007", "title": _generate_title(4007), "scanned_date": "2023-01-01 12:00", "converted_date": "2023-01-11 15:20", "processed_date": "2023-01-02 12:00", "analyzed_date": "2023-01-03 12:00"},
        {"barcode": "HTML008", "title": _generate_title(4008), "scanned_date": "2023-01-01 12:00", "converted_date": "2023-01-12 08:45", "processed_date": "2023-01-02 12:00", "analyzed_date": "2023-01-03 12:00"},
        {"barcode": "HTML009", "title": _generate_title(4009), "scanned_date": "2023-01-01 12:00", "converted_date": "2023-01-13 12:15", "processed_date": "2023-01-02 12:00", "analyzed_date": "2023-01-03 12:00"},
        {"barcode": "HTML010", "title": _generate_title(4010), "scanned_date": "2023-01-01 12:00", "converted_date": "2023-01-14 17:00", "processed_date": "2023-01-02 12:00", "analyzed_date": "2023-01-03 12:00"},
    ]


def setup_mock_exporter(temp_dir, test_data=None, storage_config=None):
    """Create a properly mocked BookCollector for testing"""

    if test_data is None:
        test_data = get_test_data()

    resume_file = Path(temp_dir) / "test_progress.json"
    sqlite_db_path = Path(temp_dir) / "test_progress.db"  # Unique database per test

    # Create config with unique database path
    config = ExportConfig(
        library_directory="TestDirectory",
        rate_limit=100.0,  # Very fast for testing
        resume_file=str(resume_file),
        sqlite_db_path=str(sqlite_db_path),
    )

    # Create a mock process summary stage if not provided

    mock_stage = ProcessStageMetrics("test")

    exporter = BookCollector(
        directory="TestDirectory", process_summary_stage=mock_stage, storage_config=storage_config, config=config
    )

    # Replace client with mock
    exporter.client = MockGRINClient(test_data)

    # Mock book storage if configured
    if storage_config:
        mock_book_manager = MockBookStorage()
        mock_book_manager.storage = MockStorage()
        exporter.book_manager = mock_book_manager

    return exporter
