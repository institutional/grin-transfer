#!/usr/bin/env python3
"""
Shared mock classes for testing CSV export functionality without network calls
"""

from pathlib import Path

from grin_to_s3.collect_books.collector import BookCollector
from grin_to_s3.collect_books.config import ExportConfig
from grin_to_s3.process_summary import ProcessStageMetrics


class MockAuth:
    """Mock authentication for testing"""

    async def validate_credentials(self, directory=None):
        """Always return successful validation"""
        return True


class MockGRINClient:
    """Mock GRIN client for controlled testing without network calls"""

    def __init__(self, test_data: list[str] = None):
        self.test_data = test_data or []
        self.auth = MockAuth()

    async def stream_book_list(self, directory: str, list_type: str, mode_all: bool = False):
        """Yield test data lines"""
        for line in self.test_data:
            yield line

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
        """Yield test data lines using HTML pagination"""
        for line in self.test_data:
            yield line

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
        """Yield test data lines using HTML pagination with prefetch (mock version)"""
        # Mock the prefetch version - yields (line, known_barcodes_set) tuples
        for line in self.test_data:
            yield line, set()  # Empty set for known barcodes since this is a mock

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
        "TEST001\t2023-01-01 12:00\t2023-01-02 12:00\t2023-01-03 12:00\t\t\t\t2023-01-04 12:00\thttps://books.google.com/books?id=test001",
        "TEST002\t2023-01-01 12:00\t2023-01-02 12:00\t2023-01-03 12:00\t\t\t\t2023-01-04 12:00\thttps://books.google.com/books?id=test002",
        "TEST003\t2023-01-01 12:00\t2023-01-02 12:00\t2023-01-03 12:00\t\t\t\t2023-01-04 12:00\thttps://books.google.com/books?id=test003",
        "TEST004\t2023-01-01 12:00\t2023-01-02 12:00\t2023-01-03 12:00\t\t\t\t2023-01-04 12:00\thttps://books.google.com/books?id=test004",
        "TEST005\t2023-01-01 12:00\t2023-01-02 12:00\t2023-01-03 12:00\t\t\t\t2023-01-04 12:00\thttps://books.google.com/books?id=test005",
    ]


def get_large_test_data():
    """Larger test data for specific tests that need more records"""
    return [
        "TEST001\t2023-01-01 12:00\t2023-01-02 12:00\t2023-01-03 12:00\t\t\t\t2023-01-04 12:00\thttps://books.google.com/books?id=test001",
        "TEST002\t2023-01-01 12:00\t2023-01-02 12:00\t2023-01-03 12:00\t\t\t\t2023-01-04 12:00\thttps://books.google.com/books?id=test002",
        "TEST003\t2023-01-01 12:00\t2023-01-02 12:00\t2023-01-03 12:00\t\t\t\t2023-01-04 12:00\thttps://books.google.com/books?id=test003",
        "TEST004\t2023-01-01 12:00\t2023-01-02 12:00\t2023-01-03 12:00\t\t\t\t2023-01-04 12:00\thttps://books.google.com/books?id=test004",
        "TEST005\t2023-01-01 12:00\t2023-01-02 12:00\t2023-01-03 12:00\t\t\t\t2023-01-04 12:00\thttps://books.google.com/books?id=test005",
        "TEST006\t2023-01-01 12:00\t2023-01-02 12:00\t2023-01-03 12:00\t\t\t\t2023-01-04 12:00\thttps://books.google.com/books?id=test006",
        "TEST007\t2023-01-01 12:00\t2023-01-02 12:00\t2023-01-03 12:00\t\t\t\t2023-01-04 12:00\thttps://books.google.com/books?id=test007",
        "TEST008\t2023-01-01 12:00\t2023-01-02 12:00\t2023-01-03 12:00\t\t\t\t2023-01-04 12:00\thttps://books.google.com/books?id=test008",
        "TEST009\t2023-01-01 12:00\t2023-01-02 12:00\t2023-01-03 12:00\t\t\t\t2023-01-04 12:00\thttps://books.google.com/books?id=test009",
        "TEST010\t2023-01-01 12:00\t2023-01-02 12:00\t2023-01-03 12:00\t\t\t\t2023-01-04 12:00\thttps://books.google.com/books?id=test010",
    ]


def get_html_test_data():
    """HTML format test data matching GRIN HTML table output"""
    return [
        "HTML001\tHTML001\tThe Great Gatsby\t\t2023-01-01 12:00\t2023-01-05 14:30\t2023-01-02 12:00\t2023-01-03 12:00",
        "HTML002\tHTML002\tTo Kill a Mockingbird\t\t2023-01-01 12:00\t2023-01-06 09:15\t2023-01-02 12:00\t2023-01-03 12:00",
        "HTML003\tHTML003\t1984 by George Orwell\t\t2023-01-01 12:00\t2023-01-07 16:45\t2023-01-02 12:00\t2023-01-03 12:00",
        "HTML004\tHTML004\tPride and Prejudice\t\t2023-01-01 12:00\t2023-01-08 11:20\t2023-01-02 12:00\t2023-01-03 12:00",
        "HTML005\tHTML005\tThe Catcher in the Rye\t\t2023-01-01 12:00\t2023-01-09 13:45\t2023-01-02 12:00\t2023-01-03 12:00",
    ]


def get_large_html_test_data():
    """Larger HTML format test data for tests that need more records"""
    return [
        "HTML001\tHTML001\tThe Great Gatsby\t\t2023-01-01 12:00\t2023-01-05 14:30\t2023-01-02 12:00\t2023-01-03 12:00",
        "HTML002\tHTML002\tTo Kill a Mockingbird\t\t2023-01-01 12:00\t2023-01-06 09:15\t2023-01-02 12:00\t2023-01-03 12:00",
        "HTML003\tHTML003\t1984 by George Orwell\t\t2023-01-01 12:00\t2023-01-07 16:45\t2023-01-02 12:00\t2023-01-03 12:00",
        "HTML004\tHTML004\tPride and Prejudice\t\t2023-01-01 12:00\t2023-01-08 11:20\t2023-01-02 12:00\t2023-01-03 12:00",
        "HTML005\tHTML005\tThe Catcher in the Rye\t\t2023-01-01 12:00\t2023-01-09 13:45\t2023-01-02 12:00\t2023-01-03 12:00",
        "HTML006\tHTML006\tLord of the Flies\t\t2023-01-01 12:00\t2023-01-10 10:30\t2023-01-02 12:00\t2023-01-03 12:00",
        "HTML007\tHTML007\tThe Hobbit\t\t2023-01-01 12:00\t2023-01-11 15:20\t2023-01-02 12:00\t2023-01-03 12:00",
        "HTML008\tHTML008\tHarry Potter and the Sorcerer's Stone\t\t2023-01-01 12:00\t2023-01-12 08:45\t2023-01-02 12:00\t2023-01-03 12:00",
        "HTML009\tHTML009\tThe Chronicles of Narnia\t\t2023-01-01 12:00\t2023-01-13 12:15\t2023-01-02 12:00\t2023-01-03 12:00",
        "HTML010\tHTML010\tJane Eyre\t\t2023-01-01 12:00\t2023-01-14 17:00\t2023-01-02 12:00\t2023-01-03 12:00",
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
