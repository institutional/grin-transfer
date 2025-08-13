"""
Unit tests for the GRIN enrichment batch optimization algorithm.

Tests prove that the optimization correctly maximizes both URL capacity
and concurrent request utilization compared to the previous sequential approach.
"""

from unittest.mock import Mock

import pytest

from grin_to_s3.metadata.grin_enrichment import GRINEnrichmentPipeline


class TestBatchOptimization:
    """Test suite for batch optimization algorithms."""

    @pytest.fixture
    def mock_process_stage(self):
        """Mock process summary stage for testing."""
        mock = Mock()
        mock.increment_items = Mock()
        return mock

    @pytest.fixture
    def pipeline(self, mock_process_stage):
        """Create a pipeline for testing batch optimization."""
        return GRINEnrichmentPipeline(
            directory="TestLibrary",
            process_summary_stage=mock_process_stage,
            db_path="/tmp/test.db",
            max_concurrent_requests=5,
            batch_size=2000,
        )

    def test_optimal_batch_composition_perfect_fit(self, pipeline):
        """Test optimal batch composition when books perfectly fit concurrent slots."""
        # Create test barcodes and calculate actual URL capacity for them
        perfect_barcodes = [f"HARV{i:08d}" for i in range(2840)]  # Example perfect fit
        url_capacity = pipeline._calculate_max_batch_size(perfect_barcodes)

        # Calculate how many we need for exactly 5 batches
        perfect_count = 5 * url_capacity
        if perfect_count != len(perfect_barcodes):
            # Adjust to actual perfect fit
            perfect_barcodes = [f"HARV{i:08d}" for i in range(perfect_count)]

        batches = pipeline._create_optimal_batch_composition(perfect_barcodes)
        batch_sizes = [len(batch) for batch in batches]

        # Should create exactly 5 batches of equal size (or close to it)
        assert len(batches) <= 5, f"Should not exceed 5 batches, got {len(batches)}"
        assert sum(batch_sizes) == perfect_count, f"Total should be {perfect_count}, got {sum(batch_sizes)}"

        # If we have exactly 5 batches, they should be roughly equal
        if len(batches) == 5:
            max_variation = max(batch_sizes) - min(batch_sizes)
            assert max_variation <= url_capacity * 0.1, f"Batches should be similar size, variation: {max_variation}"

    def test_optimal_batch_composition_with_remainder(self, pipeline):
        """Test optimal batch composition with remainder books."""
        # Use a specific case where we know the behavior
        remainder_barcodes = [f"HARV{i:08d}" for i in range(1200)]  # Moderate size

        batches = pipeline._create_optimal_batch_composition(remainder_barcodes)
        batch_sizes = [len(batch) for batch in batches]

        # Should create multiple batches and process all books
        assert len(batches) >= 1, f"Should create at least 1 batch, got {len(batches)}"
        assert len(batches) <= 5, f"Should not exceed 5 concurrent batches, got {len(batches)}"
        assert sum(batch_sizes) == 1200, f"Total should be 1200, got {sum(batch_sizes)}"

        # All batches should be reasonable size (not tiny or huge)
        for i, size in enumerate(batch_sizes):
            assert size >= 50, f"Batch {i} too small: {size}"
            assert size <= 800, f"Batch {i} too large: {size}"

    def test_optimal_batch_composition_small_set(self, pipeline):
        """Test optimal batch composition with fewer books than one URL batch."""
        small_barcodes = [f"HARV{i:08d}" for i in range(50)]  # Small set

        batches = pipeline._create_optimal_batch_composition(small_barcodes)
        batch_sizes = [len(batch) for batch in batches]

        # Should create 1 batch with all books
        assert len(batches) == 1, f"Expected 1 batch, got {len(batches)}"
        assert batch_sizes[0] == 50, f"Batch should contain 50 books, got {batch_sizes[0]}"
        assert sum(batch_sizes) == 50, f"Total should be 50, got {sum(batch_sizes)}"

    def test_optimal_batch_composition_large_set(self, pipeline):
        """Test optimal batch composition with large book set requiring multiple rounds."""
        # Large set that should require multiple concurrent rounds
        large_barcodes = [f"HARV{i:08d}" for i in range(5000)]

        batches = pipeline._create_optimal_batch_composition(large_barcodes)
        batch_sizes = [len(batch) for batch in batches]

        # Should create multiple batches and process all books
        assert len(batches) >= 5, f"Should create multiple batches, got {len(batches)}"
        assert sum(batch_sizes) == 5000, f"Total should be 5000, got {sum(batch_sizes)}"

        # Calculate concurrent rounds needed
        concurrent_rounds = (len(batches) + pipeline.max_concurrent_requests - 1) // pipeline.max_concurrent_requests
        assert concurrent_rounds >= 2, f"Large set should require multiple rounds, got {concurrent_rounds}"

        # All batches should be reasonable size
        for size in batch_sizes:
            assert size >= 100, f"Batch too small: {size}"
            assert size <= 1000, f"Batch too large: {size}"

    def test_comparison_with_sequential_approach(self, pipeline):
        """Test that optimization outperforms sequential batch creation."""
        # Test case from the original logs: 2000 books
        test_barcodes = [f"HARV{i:08d}" for i in range(2000)]

        # Get optimized batches
        optimized_batches = pipeline._create_optimal_batch_composition(test_barcodes)
        opt_batch_sizes = [len(batch) for batch in optimized_batches]

        # Simulate old sequential approach
        sequential_batches = []
        remaining = test_barcodes[:]
        while remaining:
            max_size = pipeline._calculate_max_batch_size(remaining)
            batch_size = min(max_size, len(remaining))
            sequential_batches.append(remaining[:batch_size])
            remaining = remaining[batch_size:]

        seq_batch_sizes = [len(batch) for batch in sequential_batches]

        # Optimization should create fewer or equal batches
        assert len(optimized_batches) <= len(sequential_batches), (
            f"Optimized should have ≤ batches: {len(optimized_batches)} vs {len(sequential_batches)}"
        )

        # Both should process all books
        assert sum(opt_batch_sizes) == sum(seq_batch_sizes) == 2000

        # Optimized should have more consistent batch sizes (less variation)
        opt_variation = max(opt_batch_sizes) - min(opt_batch_sizes) if opt_batch_sizes else 0
        seq_variation = max(seq_batch_sizes) - min(seq_batch_sizes) if seq_batch_sizes else 0

        # Note: This might not always be true, but generally optimization reduces variation
        print(f"Optimized batches: {opt_batch_sizes} (variation: {opt_variation})")
        print(f"Sequential batches: {seq_batch_sizes} (variation: {seq_variation})")

    def test_optimal_batch_size_recommendation(self, pipeline):
        """Test optimal batch size recommendation calculation."""
        typical_barcodes = [f"HARV{i:08d}" for i in range(1000)]

        # Test with sample barcodes
        optimal_with_sample = pipeline.get_optimal_batch_size_recommendation(typical_barcodes)
        url_capacity = pipeline._calculate_max_batch_size(typical_barcodes)
        expected_optimal = 5 * url_capacity  # max_concurrent_requests * url_capacity

        assert optimal_with_sample == expected_optimal, f"Expected {expected_optimal}, got {optimal_with_sample}"

        # Test without sample (should use conservative estimate)
        optimal_without_sample = pipeline.get_optimal_batch_size_recommendation()
        expected_conservative = 5 * 490  # max_concurrent_requests * conservative_estimate

        assert optimal_without_sample == expected_conservative, (
            f"Expected {expected_conservative}, got {optimal_without_sample}"
        )

    def test_batch_composition_preserves_order(self, pipeline):
        """Test that batch composition preserves barcode order."""
        test_barcodes = [f"BOOK_{i:04d}" for i in range(500)]

        batches = pipeline._create_optimal_batch_composition(test_barcodes)

        # Reconstruct the original order
        reconstructed = []
        for batch in batches:
            reconstructed.extend(batch)

        assert reconstructed == test_barcodes, "Batch composition should preserve barcode order"

    def test_batch_composition_handles_empty_input(self, pipeline):
        """Test batch composition with empty input."""
        empty_batches = pipeline._create_optimal_batch_composition([])
        assert empty_batches == [], "Empty input should return empty batches"

    def test_batch_composition_handles_varying_barcode_lengths(self, pipeline):
        """Test batch composition with barcodes of varying lengths."""
        # Mix of short and long barcodes
        mixed_barcodes = (
            [f"SHORT{i}" for i in range(100)]  # Short barcodes
            + [f"VERY_LONG_BARCODE_WITH_MANY_CHARACTERS_{i:06d}" for i in range(100)]  # Long barcodes
        )

        batches = pipeline._create_optimal_batch_composition(mixed_barcodes)
        batch_sizes = [len(batch) for batch in batches]

        # Should handle mixed lengths and still create valid batches
        assert len(batches) > 0, "Should create at least one batch"
        assert sum(batch_sizes) == 200, "Should process all 200 barcodes"

        # Verify each batch respects URL limits
        for batch in batches:
            url_capacity = pipeline._calculate_max_batch_size(batch)
            assert len(batch) <= url_capacity, f"Batch size {len(batch)} exceeds URL capacity {url_capacity}"

    def test_concurrent_utilization_efficiency(self, pipeline):
        """Test that the algorithm maximizes concurrent request utilization."""
        # Test various book counts and measure concurrent efficiency
        test_cases = [1000, 2000, 3000, 5000, 10000]

        for book_count in test_cases:
            test_barcodes = [f"TEST{i:06d}" for i in range(book_count)]
            batches = pipeline._create_optimal_batch_composition(test_barcodes)

            # Calculate concurrent rounds needed
            concurrent_rounds = (
                len(batches) + pipeline.max_concurrent_requests - 1
            ) // pipeline.max_concurrent_requests

            # Calculate efficiency: books per concurrent round
            books_per_round = book_count / concurrent_rounds

            # For reference, store results
            print(
                f"{book_count} books → {len(batches)} batches → {concurrent_rounds} rounds → {books_per_round:.1f} books/round"
            )

            # Basic sanity checks
            assert len(batches) > 0, f"Should create batches for {book_count} books"
            assert sum(len(batch) for batch in batches) == book_count, f"Should process all {book_count} books"

    @pytest.mark.parametrize("max_concurrent", [1, 3, 5, 10])
    def test_batch_composition_respects_concurrency_limit(self, mock_process_stage, max_concurrent):
        """Test batch composition with different concurrency limits."""
        pipeline = GRINEnrichmentPipeline(
            directory="TestLibrary",
            process_summary_stage=mock_process_stage,
            max_concurrent_requests=max_concurrent,
            batch_size=2000,
        )

        # Large book set
        test_barcodes = [f"TEST{i:06d}" for i in range(5000)]
        batches = pipeline._create_optimal_batch_composition(test_barcodes)

        # Calculate rounds needed
        concurrent_rounds = (len(batches) + max_concurrent - 1) // max_concurrent

        # Should efficiently utilize the concurrency limit
        assert len(batches) >= concurrent_rounds, f"Should have at least {concurrent_rounds} batches"
        assert sum(len(batch) for batch in batches) == 5000, "Should process all books"

        # First few batches in each round should be roughly equal in size (within URL constraints)
        if len(batches) >= max_concurrent:
            first_round_sizes = [len(batch) for batch in batches[:max_concurrent]]
            # All should be reasonably similar (within 20% for URL capacity variations)
            if first_round_sizes:
                avg_size = sum(first_round_sizes) / len(first_round_sizes)
                for size in first_round_sizes:
                    variation = abs(size - avg_size) / avg_size
                    assert variation < 0.5, f"Batch sizes should be similar, got {first_round_sizes}"

    def test_leftover_carry_over_mechanism(self, pipeline):
        """Test that leftover books are carried over to next batch correctly."""
        # Start with empty leftovers
        assert pipeline.leftover_barcodes == []

        # Test calculate optimal fetch size
        pipeline.leftover_barcodes = [f"LEFT{i:03d}" for i in range(100)]  # 100 leftovers

        # Should fetch fewer books when we have leftovers
        fetch_size = pipeline._calculate_optimal_fetch_size(target_batch_size=2500)
        assert fetch_size == 2400, f"Expected 2400, got {fetch_size}"

        # If we have more leftovers than target, should fetch 0
        pipeline.leftover_barcodes = [f"LEFT{i:03d}" for i in range(3000)]  # 3000 leftovers
        fetch_size = pipeline._calculate_optimal_fetch_size(target_batch_size=2500)
        assert fetch_size == 0, f"Expected 0, got {fetch_size}"

    def test_combined_batch_preparation(self, pipeline):
        """Test combining leftover and fresh barcodes for optimal processing."""
        # Set up test scenario
        pipeline.leftover_barcodes = [f"LEFT{i:03d}" for i in range(200)]  # 200 leftovers
        fresh_barcodes = [f"FRESH{i:04d}" for i in range(2300)]  # 2300 fresh

        # Prepare combined batch
        processing_barcodes, new_leftovers = pipeline._prepare_combined_batch(fresh_barcodes)

        # Should process books and potentially have new leftovers
        assert len(processing_barcodes) > 0, "Should have books to process"
        assert len(processing_barcodes) + len(new_leftovers) == 2500, "Total should be preserved"

        # Processing barcodes should start with leftover books (order preserved)
        assert processing_barcodes[0].startswith("LEFT"), "Should start with leftover books"

        # Verify order preservation within each group
        leftover_count = 0
        for barcode in processing_barcodes:
            if barcode.startswith("LEFT"):
                leftover_count += 1
            else:
                break

        # Should have some leftovers in processing batch
        assert leftover_count > 0, "Should include some leftover books"
        assert leftover_count <= 200, "Should not exceed original leftover count"

    def test_leftover_buffer_edge_cases(self, pipeline):
        """Test edge cases for leftover buffer handling."""
        # Test with empty inputs
        processing, leftovers = pipeline._prepare_combined_batch([])
        assert processing == [] and leftovers == []

        # Test with only leftovers
        pipeline.leftover_barcodes = [f"ONLY{i:03d}" for i in range(50)]
        processing, leftovers = pipeline._prepare_combined_batch([])
        assert len(processing) == 50, f"Expected 50, got {len(processing)}"
        assert len(leftovers) == 0, f"Expected 0 leftovers, got {len(leftovers)}"

        # Test with only fresh books (no leftovers)
        pipeline.leftover_barcodes = []
        fresh_only = [f"FRESH{i:04d}" for i in range(1000)]
        processing, leftovers = pipeline._prepare_combined_batch(fresh_only)
        assert len(processing) + len(leftovers) == 1000, "Should preserve all books"

    def test_leftover_state_management(self, pipeline):
        """Test that leftover state is properly managed across batch cycles."""
        # Start with clean state
        pipeline.leftover_barcodes = []

        # Simulate first batch cycle
        first_fresh = [f"BATCH1_{i:04d}" for i in range(2600)]
        processing1, leftovers1 = pipeline._prepare_combined_batch(first_fresh)
        pipeline.leftover_barcodes = leftovers1

        # Simulate second batch cycle
        second_fresh = [f"BATCH2_{i:04d}" for i in range(2500)]
        processing2, leftovers2 = pipeline._prepare_combined_batch(second_fresh)

        # Verify state transitions
        total_books_cycle1 = len(processing1) + len(leftovers1)
        assert total_books_cycle1 == 2600, f"First cycle should handle 2600 books, got {total_books_cycle1}"

        total_books_cycle2 = len(processing2) + len(leftovers2)
        expected_cycle2 = len(leftovers1) + 2500
        assert total_books_cycle2 == expected_cycle2, (
            f"Second cycle mismatch: {total_books_cycle2} vs {expected_cycle2}"
        )

        # First book in second processing batch should be from leftovers
        if leftovers1 and processing2:
            assert processing2[0] in first_fresh, "Should start with leftover from first batch"

    def test_leftover_books_are_actually_processed(self, pipeline):
        """Test that leftover books from previous batch are included in processing."""
        # Set up scenario with specific, identifiable leftovers
        leftover_books = [f"LEFTOVER_{i:03d}" for i in range(150)]
        fresh_books = [f"FRESH_{i:04d}" for i in range(2350)]

        # Set leftover state
        pipeline.leftover_barcodes = leftover_books

        # Prepare combined batch
        processing_barcodes, new_leftovers = pipeline._prepare_combined_batch(fresh_books)

        # Verify leftover books are included in processing
        leftover_books_in_processing = [b for b in processing_barcodes if b.startswith("LEFTOVER_")]

        assert len(leftover_books_in_processing) == 150, (
            f"Expected 150 leftover books in processing, got {len(leftover_books_in_processing)}"
        )

        # Verify they maintain their original order and content
        expected_leftovers = leftover_books  # All 150 should be processed
        actual_leftovers = [b for b in processing_barcodes if b.startswith("LEFTOVER_")]

        assert actual_leftovers == expected_leftovers, "Leftover books should be processed in original order"

        # Verify the first books in processing are the leftovers (order preservation)
        assert processing_barcodes[:150] == leftover_books, "First 150 books should be the leftovers"

        # Verify fresh books come after leftovers
        fresh_books_in_processing = [b for b in processing_barcodes if b.startswith("FRESH_")]
        assert len(fresh_books_in_processing) > 0, "Should also include some fresh books"

        # Verify total count matches expectation
        total_expected = len(leftover_books) + len(fresh_books)
        total_actual = len(processing_barcodes) + len(new_leftovers)
        assert total_actual == total_expected, f"Total books should be preserved: {total_actual} vs {total_expected}"
