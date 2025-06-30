#!/usr/bin/env python3
"""
Performance tests for OCR text extraction functionality.
"""

import tempfile
import time
import tracemalloc
from pathlib import Path

import pytest

from grin_to_s3.extract.text_extraction import extract_text_from_archive
from tests.utils import create_mixed_size_archive, create_test_archive_with_sizes


class TestTextExtractionPerformance:
    """Test performance characteristics of text extraction."""

    def test_small_archive_performance(self):
        """Test performance with small archive (10 pages)."""
        with tempfile.TemporaryDirectory() as temp_dir:
            temp_path = Path(temp_dir)
            archive_path = create_test_archive_with_sizes(10, 500, temp_path)  # 10 pages, ~500 chars each

            start_time = time.time()
            result = extract_text_from_archive(str(archive_path))
            end_time = time.time()

            extraction_time = end_time - start_time

            assert len(result) == 10
            assert extraction_time < 1.0  # Should complete in under 1 second

    def test_medium_archive_performance(self):
        """Test performance with medium archive (100 pages)."""
        with tempfile.TemporaryDirectory() as temp_dir:
            temp_path = Path(temp_dir)
            archive_path = create_test_archive_with_sizes(100, 1000, temp_path)  # 100 pages, ~1KB each

            start_time = time.time()
            result = extract_text_from_archive(str(archive_path))
            end_time = time.time()

            extraction_time = end_time - start_time

            assert len(result) == 100
            assert extraction_time < 3.0  # Should complete in under 3 seconds

    def test_large_archive_performance(self):
        """Test performance with large archive (300 pages) - target requirement."""
        with tempfile.TemporaryDirectory() as temp_dir:
            temp_path = Path(temp_dir)
            archive_path = create_test_archive_with_sizes(300, 2000, temp_path)  # 300 pages, ~2KB each

            start_time = time.time()
            result = extract_text_from_archive(str(archive_path))
            end_time = time.time()

            extraction_time = end_time - start_time

            assert len(result) == 300
            # Target: process typical archive (300 pages) in <5 seconds
            assert extraction_time < 5.0

    def test_very_large_archive_performance(self):
        """Test performance with very large archive (700 pages) - stress test."""
        with tempfile.TemporaryDirectory() as temp_dir:
            temp_path = Path(temp_dir)
            archive_path = create_test_archive_with_sizes(700, 1500, temp_path)  # 700 pages, ~1.5KB each

            start_time = time.time()
            result = extract_text_from_archive(str(archive_path))
            end_time = time.time()

            extraction_time = end_time - start_time

            assert len(result) == 700
            # Should still complete in reasonable time
            assert extraction_time < 15.0

    def test_memory_usage_efficiency(self):
        """Test that memory usage remains reasonable for large archives."""
        with tempfile.TemporaryDirectory() as temp_dir:
            temp_path = Path(temp_dir)

            # Start memory tracking
            tracemalloc.start()

            # Create and process medium-sized archive
            archive_path = create_test_archive_with_sizes(200, 5000, temp_path)  # 200 pages, ~5KB each

            # Get memory snapshot before extraction
            snapshot_before = tracemalloc.take_snapshot()

            result = extract_text_from_archive(str(archive_path))

            # Get memory snapshot after extraction
            snapshot_after = tracemalloc.take_snapshot()

            tracemalloc.stop()

            # Calculate memory usage
            top_stats = snapshot_after.compare_to(snapshot_before, "lineno")
            total_memory_used = sum(stat.size_diff for stat in top_stats)

            assert len(result) == 200

            # Memory usage should be reasonable (less than 50MB for this test)
            # This is quite generous but ensures we're not loading entire archive
            assert total_memory_used < 50 * 1024 * 1024

    def test_archive_with_large_individual_pages(self):
        """Test performance with archive containing very large individual pages."""
        with tempfile.TemporaryDirectory() as temp_dir:
            temp_path = Path(temp_dir)

            # Create archive with 10 pages but each page is very large (~100KB)
            archive_path = create_test_archive_with_sizes(10, 100000, temp_path)

            start_time = time.time()
            result = extract_text_from_archive(str(archive_path))
            end_time = time.time()

            extraction_time = end_time - start_time

            assert len(result) == 10
            # Should handle large individual pages efficiently
            assert extraction_time < 2.0

            # Verify content is correct
            assert "Page 1 content" in result[0]
            assert "Page 10 content" in result[9]

    def test_archive_with_mixed_page_sizes(self):
        """Test performance with archive containing pages of varying sizes."""
        with tempfile.TemporaryDirectory() as temp_dir:
            temp_path = Path(temp_dir)
            archive_path = create_mixed_size_archive(50, temp_path)

            start_time = time.time()
            result = extract_text_from_archive(str(archive_path))
            end_time = time.time()

            extraction_time = end_time - start_time

            assert len(result) == 50
            assert extraction_time < 3.0  # Should handle mixed sizes efficiently

            # Verify different sized content is extracted correctly
            assert "Small page 1" in result[0]
            assert "Medium page 5" in result[4]
            assert "Large page 10" in result[9]

    def test_extraction_scales_linearly(self):
        """Test that extraction time scales approximately linearly with archive size."""
        with tempfile.TemporaryDirectory() as temp_dir:
            temp_path = Path(temp_dir)

            # Test with different archive sizes
            sizes_and_times = []

            for num_pages in [50, 100, 200]:
                archive_path = create_test_archive_with_sizes(num_pages, 1000, temp_path)

                start_time = time.time()
                result = extract_text_from_archive(str(archive_path))
                end_time = time.time()

                extraction_time = end_time - start_time
                sizes_and_times.append((num_pages, extraction_time))

                assert len(result) == num_pages

            # Verify that doubling the pages doesn't more than triple the time
            # (allows for some overhead but ensures roughly linear scaling)
            small_time = sizes_and_times[0][1]
            medium_time = sizes_and_times[1][1]
            large_time = sizes_and_times[2][1]

            # 100 pages should not take more than 3x the time of 50 pages
            assert medium_time < small_time * 3

            # 200 pages should not take more than 2.5x the time of 100 pages
            assert large_time < medium_time * 2.5


class TestMemoryBenchmarks:
    """Memory usage benchmarks for different archive characteristics."""

    @pytest.mark.slow
    def test_memory_usage_typical_book(self):
        """Benchmark memory usage for typical book size (300 pages)."""
        with tempfile.TemporaryDirectory() as temp_dir:
            temp_path = Path(temp_dir)

            tracemalloc.start()

            # Typical book: 300 pages, ~2KB per page
            archive_path = create_test_archive_with_sizes(300, 2000, temp_path)

            snapshot_before = tracemalloc.take_snapshot()
            result = extract_text_from_archive(str(archive_path))
            snapshot_after = tracemalloc.take_snapshot()

            tracemalloc.stop()

            # Calculate peak memory usage
            top_stats = snapshot_after.compare_to(snapshot_before, "lineno")
            peak_memory = max(stat.size_diff for stat in top_stats if stat.size_diff > 0)

            assert len(result) == 300

            # Memory usage should be reasonable for typical book
            # Peak usage should be less than 20MB for this size
            assert peak_memory < 20 * 1024 * 1024

    @pytest.mark.slow
    def test_memory_usage_large_book(self):
        """Benchmark memory usage for large book (700+ pages)."""
        with tempfile.TemporaryDirectory() as temp_dir:
            temp_path = Path(temp_dir)

            tracemalloc.start()

            # Large book: 700 pages, ~1.5KB per page
            archive_path = create_test_archive_with_sizes(700, 1500, temp_path)

            snapshot_before = tracemalloc.take_snapshot()
            result = extract_text_from_archive(str(archive_path))
            snapshot_after = tracemalloc.take_snapshot()

            tracemalloc.stop()

            top_stats = snapshot_after.compare_to(snapshot_before, "lineno")
            peak_memory = max(stat.size_diff for stat in top_stats if stat.size_diff > 0)

            assert len(result) == 700

            # Even for large books, memory usage should be controlled
            # Peak usage should be less than 50MB
            assert peak_memory < 50 * 1024 * 1024
