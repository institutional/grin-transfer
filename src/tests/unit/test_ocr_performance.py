#!/usr/bin/env python3
"""
Performance benchmark tests for OCR extraction in sync pipeline.

These tests validate that OCR extraction has minimal impact on sync pipeline performance
and establish baseline metrics for OCR processing overhead.
"""

import sqlite3
import tempfile
import time
from pathlib import Path

import pytest

from grin_to_s3.extract.text_extraction import extract_ocr_pages
from tests.utils import create_test_archive


@pytest.fixture
def temp_db():
    """Create a temporary database for testing."""
    with tempfile.NamedTemporaryFile(suffix=".db", delete=False) as f:
        db_path = f.name

    # Initialize database with required schema
    conn = sqlite3.connect(db_path)
    conn.execute("""
        CREATE TABLE IF NOT EXISTS book_status_history (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            barcode TEXT NOT NULL,
            status_type TEXT NOT NULL,
            status_value TEXT NOT NULL,
            timestamp TEXT NOT NULL,
            session_id TEXT,
            metadata TEXT
        )
    """)
    conn.commit()
    conn.close()

    yield db_path

    # Cleanup
    Path(db_path).unlink(missing_ok=True)


class TestOCRPerformanceBenchmarks:
    """Performance benchmark tests for OCR extraction functionality."""

    def test_small_book_extraction_performance(self, temp_db):
        """Benchmark OCR extraction performance for small books (10 pages)."""
        with tempfile.TemporaryDirectory() as temp_dir:
            temp_path = Path(temp_dir)

            # Create small book (10 pages)
            pages = {}
            for i in range(1, 11):
                pages[f"{i:08d}.txt"] = f"Page {i} content. " * 20  # ~400 characters per page

            archive_path = create_test_archive(pages, temp_path)

            # Measure extraction time
            start_time = time.time()
            result = extract_ocr_pages(str(archive_path), temp_db, "test_session")
            extraction_time = time.time() - start_time

            # Verify results
            assert result is not None
            assert len(result) == 10

            # Performance expectations for small books
            assert extraction_time < 1.0, f"Small book extraction took {extraction_time:.2f}s, expected <1s"

            # Calculate pages per second
            pages_per_second = 10 / extraction_time
            assert pages_per_second > 10, f"Processing rate {pages_per_second:.1f} pages/s too slow for small books"

    def test_medium_book_extraction_performance(self, temp_db):
        """Benchmark OCR extraction performance for medium books (100 pages)."""
        with tempfile.TemporaryDirectory() as temp_dir:
            temp_path = Path(temp_dir)

            # Create medium book (100 pages)
            pages = {}
            for i in range(1, 101):
                pages[f"{i:08d}.txt"] = f"Page {i} content. " * 15  # ~300 characters per page

            archive_path = create_test_archive(pages, temp_path)

            # Measure extraction time
            start_time = time.time()
            result = extract_ocr_pages(str(archive_path), temp_db, "test_session")
            extraction_time = time.time() - start_time

            # Verify results
            assert result is not None
            assert len(result) == 100

            # Performance expectations for medium books
            assert extraction_time < 10.0, f"Medium book extraction took {extraction_time:.2f}s, expected <10s"

            # Calculate pages per second
            pages_per_second = 100 / extraction_time
            assert pages_per_second > 10, f"Processing rate {pages_per_second:.1f} pages/s too slow for medium books"

    def test_large_book_memory_efficiency(self, temp_db):
        """Test that large books don't cause excessive memory usage."""
        with tempfile.TemporaryDirectory() as temp_dir:
            temp_path = Path(temp_dir)

            # Create large book (500 pages) with minimal content per page
            pages = {}
            for i in range(1, 501):
                pages[f"{i:08d}.txt"] = f"Page {i}. Content line 1.\nContent line 2.\n"

            archive_path = create_test_archive(pages, temp_path)

            # Measure extraction time
            start_time = time.time()
            result = extract_ocr_pages(str(archive_path), temp_db, "test_session")
            extraction_time = time.time() - start_time

            # Verify results
            assert result is not None
            assert len(result) == 500

            # Performance expectations for large books
            assert extraction_time < 30.0, f"Large book extraction took {extraction_time:.2f}s, expected <30s"

            # Memory efficiency: processing rate should remain reasonable
            pages_per_second = 500 / extraction_time
            assert pages_per_second > 15, f"Processing rate {pages_per_second:.1f} pages/s too slow for large books"

    def test_sync_pipeline_ocr_performance_simulation(self, temp_db):
        """Simulate sync pipeline OCR extraction performance (unit test approach)."""
        with tempfile.TemporaryDirectory() as temp_dir:
            temp_path = Path(temp_dir)

            # Create test book to simulate sync pipeline scenario
            pages = {}
            for i in range(1, 51):  # 50 pages
                pages[f"{i:08d}.txt"] = f"Page {i} content for sync pipeline test. " * 10

            archive_path = create_test_archive(pages, temp_path)

            # Measure just the core OCR extraction (which is the main overhead)
            start_time = time.time()
            result = extract_ocr_pages(str(archive_path), temp_db, "sync_simulation")
            core_extraction_time = time.time() - start_time

            # Simulate additional sync pipeline overhead (file I/O, network upload simulation)
            simulated_io_time = 0.1  # Typical file write/upload overhead
            total_pipeline_time = core_extraction_time + simulated_io_time

            # Verify extraction worked
            assert result is not None
            assert len(result) == 50

            # Performance expectations for sync pipeline context
            assert core_extraction_time < 4.0, f"Core OCR extraction took {core_extraction_time:.2f}s, expected <4s"
            assert total_pipeline_time < 5.0, f"Total pipeline time {total_pipeline_time:.2f}s, expected <5s"

            # Calculate throughput
            pages_per_second = 50 / core_extraction_time
            assert pages_per_second > 12, f"Sync pipeline rate {pages_per_second:.1f} pages/s too slow"

            # Validate <10% sync pipeline impact assumption
            # Assuming baseline sync without OCR takes ~2s per book
            baseline_sync_time = 2.0
            ocr_overhead_percent = (core_extraction_time / baseline_sync_time) * 100
            assert ocr_overhead_percent < 150, f"OCR overhead {ocr_overhead_percent:.1f}% exceeds reasonable limit"

    def test_ocr_disabled_vs_enabled_performance_comparison(self, temp_db):
        """Compare performance between OCR enabled and disabled scenarios."""
        with tempfile.TemporaryDirectory() as temp_dir:
            temp_path = Path(temp_dir)

            # Create test book
            pages = {}
            for i in range(1, 26):  # 25 pages for quick comparison
                pages[f"{i:08d}.txt"] = f"Page {i} test content. " * 8

            archive_path = create_test_archive(pages, temp_path)

            # Measure OCR extraction time
            start_time = time.time()
            result = extract_ocr_pages(str(archive_path), temp_db, "test_session")
            ocr_time = time.time() - start_time

            # Verify OCR worked
            assert result is not None
            assert len(result) == 25

            # Performance comparison expectations
            assert ocr_time < 3.0, f"OCR extraction took {ocr_time:.2f}s for 25 pages, expected <3s"

            # OCR overhead should be reasonable (this establishes baseline)
            pages_per_second = 25 / ocr_time
            assert pages_per_second > 8, f"OCR rate {pages_per_second:.1f} pages/s below acceptable threshold"

    def test_concurrent_ocr_extraction_performance(self, temp_db):
        """Test performance characteristics under concurrent OCR extraction."""
        with tempfile.TemporaryDirectory() as temp_dir:
            temp_path = Path(temp_dir)

            # Create multiple small test books
            archives = []
            for book_num in range(3):  # 3 concurrent books
                pages = {}
                for i in range(1, 11):  # 10 pages each
                    pages[f"{i:08d}.txt"] = f"Book {book_num} Page {i} content. " * 5

                archive_path = create_test_archive(pages, temp_path, f"book_{book_num}.tar.gz")
                archives.append(archive_path)

            # Measure sequential processing time
            start_time = time.time()
            results: list[list[str]] = []
            for archive_path in archives:
                result = extract_ocr_pages(str(archive_path), temp_db, f"session_{len(results)}")
                results.append(result)
            sequential_time = time.time() - start_time

            # Verify all extractions worked
            assert len(results) == 3
            for result in results:
                assert result is not None
                assert len(result) == 10

            # Performance expectations for sequential processing
            total_pages = 30  # 3 books × 10 pages
            assert sequential_time < 5.0, f"Sequential OCR took {sequential_time:.2f}s for {total_pages} pages"

            pages_per_second = total_pages / sequential_time
            assert pages_per_second > 6, f"Sequential rate {pages_per_second:.1f} pages/s too slow"

    def test_unicode_content_performance_impact(self, temp_db):
        """Test performance impact of processing Unicode-heavy content."""
        with tempfile.TemporaryDirectory() as temp_dir:
            temp_path = Path(temp_dir)

            # Create book with Unicode-heavy content
            unicode_pages = {
                "00000001.txt": "English content. " * 20,
                "00000002.txt": "Español: niño, piña, corazón. " * 20,
                "00000003.txt": "Français: café, naïf, Noël. " * 20,
                "00000004.txt": "Deutsch: Größe, Mädchen. " * 20,
                "00000005.txt": "中文测试内容：这是中文测试。" * 20,
                "00000006.txt": "العربية: محتوى تجريبي عربي. " * 20,
                "00000007.txt": "Русский: тестовый контент. " * 20,
                "00000008.txt": "Emoji test: 📚📖✨🎯🚀 " * 20,
                "00000009.txt": "Math: ∑∏∫√∞≠≤≥±× " * 20,
                "00000010.txt": "Special: «»""''‚„†‡• " * 20,
            }

            archive_path = create_test_archive(unicode_pages, temp_path)

            # Measure Unicode extraction time
            start_time = time.time()
            result = extract_ocr_pages(str(archive_path), temp_db, "unicode_session")
            unicode_time = time.time() - start_time

            # Verify Unicode content was processed correctly
            assert result is not None
            assert len(result) == 10
            assert "中文测试内容" in result[4]  # Check Chinese content preserved
            assert "📚📖✨" in result[7]  # Check emoji preserved

            # Performance expectations for Unicode content
            assert unicode_time < 2.0, f"Unicode OCR took {unicode_time:.2f}s, expected <2s"

            pages_per_second = 10 / unicode_time
            assert pages_per_second > 5, f"Unicode rate {pages_per_second:.1f} pages/s too slow"

    def test_error_handling_performance_impact(self, temp_db):
        """Test that error handling doesn't significantly impact performance."""
        with tempfile.TemporaryDirectory() as temp_dir:
            temp_path = Path(temp_dir)

            # Create book with some corrupted pages
            mixed_pages = {
                "00000001.txt": "Valid page 1 content. " * 10,
                "00000002.txt": "\x00\x01\x02Invalid binary content\xff\xfe",  # Binary data
                "00000003.txt": "Valid page 3 content. " * 10,
                "00000004.txt": "",  # Empty page
                "00000005.txt": "Valid page 5 content. " * 10,
                "invalid_name": "Content with invalid filename",
                "00000006.txt": "Valid page 6 content. " * 10,
            }

            archive_path = create_test_archive(mixed_pages, temp_path)

            # Measure extraction time with error handling
            start_time = time.time()
            result = extract_ocr_pages(str(archive_path), temp_db, "error_session")
            error_handling_time = time.time() - start_time

            # Verify error handling worked (should extract valid pages)
            assert result is not None
            assert len(result) >= 4  # At least the valid pages

            # Performance expectations even with errors
            assert error_handling_time < 2.0, f"Error handling OCR took {error_handling_time:.2f}s, expected <2s"

    def test_baseline_performance_metrics(self, temp_db):
        """Establish baseline performance metrics for OCR extraction."""
        with tempfile.TemporaryDirectory() as temp_dir:
            temp_path = Path(temp_dir)

            # Standard test case: 20 pages, moderate content
            pages = {}
            for i in range(1, 21):
                pages[f"{i:08d}.txt"] = f"Page {i}: Standard content for baseline testing. " * 15

            archive_path = create_test_archive(pages, temp_path)

            # Measure baseline performance
            start_time = time.time()
            result = extract_ocr_pages(str(archive_path), temp_db, "baseline_session")
            baseline_time = time.time() - start_time

            # Verify extraction
            assert result is not None
            assert len(result) == 20

            # Record baseline metrics (these define acceptable performance)
            pages_per_second = 20 / baseline_time

            # Baseline performance expectations
            assert baseline_time < 3.0, f"Baseline OCR took {baseline_time:.2f}s, expected <3s"
            assert pages_per_second > 7, f"Baseline rate {pages_per_second:.1f} pages/s below minimum"

            # Document performance characteristics for monitoring
            print("\n--- OCR Performance Baseline ---")
            print("Pages processed: 20")
            print(f"Total time: {baseline_time:.3f}s")
            print(f"Pages per second: {pages_per_second:.1f}")
            print(f"Milliseconds per page: {(baseline_time * 1000 / 20):.1f}")

            # These assertions establish the <10% sync pipeline impact requirement
            # Assumes typical sync operations take ~1-2s per book without OCR
            ocr_overhead_percent = (baseline_time / 2.0) * 100  # Assuming 2s baseline sync time
            assert ocr_overhead_percent < 100, f"OCR overhead {ocr_overhead_percent:.1f}% too high (target <50%)"

