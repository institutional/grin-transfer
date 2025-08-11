#!/usr/bin/env python3
"""
Tests for the in_process caching functionality in processing.py.

Tests the caching behavior, TTL, and error handling of the get_in_process_set function.
"""

import asyncio
import time
from unittest import IsolatedAsyncioTestCase
from unittest.mock import AsyncMock, patch

from grin_to_s3.processing import _in_process_cache, get_in_process_set


class TestProcessingCaching(IsolatedAsyncioTestCase):
    """Test in_process queue caching functionality."""

    async def asyncSetUp(self):
        """Clear cache before each test."""
        _in_process_cache.clear()

    async def asyncTearDown(self):
        """Clean up cache after each test."""
        _in_process_cache.clear()

    async def test_cache_miss_and_populate(self):
        """Test that cache is populated on first call."""
        # Mock GRIN client
        mock_client = AsyncMock()
        mock_client.fetch_resource.return_value = "BOOK001\nBOOK002\nBOOK003"

        library_dir = "test_library"

        # First call should hit the API
        result = await get_in_process_set(mock_client, library_dir)

        # Verify API was called
        mock_client.fetch_resource.assert_called_once_with(library_dir, "_in_process?format=text")

        # Verify correct result
        expected = {"BOOK001", "BOOK002", "BOOK003"}
        self.assertEqual(result, expected)

        # Verify cache was populated
        self.assertIn(library_dir, _in_process_cache)
        cached_books, _ = _in_process_cache[library_dir]
        self.assertEqual(cached_books, expected)

    async def test_cache_hit_within_ttl(self):
        """Test that cache is used when TTL is not expired."""
        # Mock GRIN client
        mock_client = AsyncMock()
        mock_client.fetch_resource.return_value = "BOOK001\nBOOK002"

        library_dir = "test_library"

        # First call populates cache
        result1 = await get_in_process_set(mock_client, library_dir)
        self.assertEqual(result1, {"BOOK001", "BOOK002"})

        # Reset mock to track second call
        mock_client.reset_mock()
        mock_client.fetch_resource.return_value = "DIFFERENT_BOOK"

        # Second call should use cache (within TTL)
        result2 = await get_in_process_set(mock_client, library_dir)

        # Should not call API again
        mock_client.fetch_resource.assert_not_called()

        # Should return cached result, not the new mock response
        self.assertEqual(result2, {"BOOK001", "BOOK002"})

    async def test_cache_expiry_after_ttl(self):
        """Test that cache expires after TTL and API is called again."""
        # Mock GRIN client
        mock_client = AsyncMock()
        mock_client.fetch_resource.return_value = "BOOK001\nBOOK002"

        library_dir = "test_library"

        # First call populates cache
        result1 = await get_in_process_set(mock_client, library_dir)
        self.assertEqual(result1, {"BOOK001", "BOOK002"})

        # Manually expire the cache by setting old timestamp
        if library_dir in _in_process_cache:
            books, _ = _in_process_cache[library_dir]
            old_timestamp = time.time() - 3700  # 1 hour + 100 seconds ago
            _in_process_cache[library_dir] = (books, old_timestamp)

        # Reset mock with different response
        mock_client.reset_mock()
        mock_client.fetch_resource.return_value = "BOOK003\nBOOK004"

        # Second call should fetch fresh data
        result2 = await get_in_process_set(mock_client, library_dir)

        # API should be called again
        mock_client.fetch_resource.assert_called_once_with(library_dir, "_in_process?format=text")

        # Should return new result
        self.assertEqual(result2, {"BOOK003", "BOOK004"})

    async def test_multiple_library_directories_separate_cache(self):
        """Test that different library directories have separate cache entries."""
        # Mock GRIN client
        mock_client = AsyncMock()

        # Set up different responses for different calls
        def side_effect(library_dir, endpoint):
            if library_dir == "lib1":
                return "LIB1_BOOK1\nLIB1_BOOK2"
            elif library_dir == "lib2":
                return "LIB2_BOOK1\nLIB2_BOOK2\nLIB2_BOOK3"
            else:
                return ""

        mock_client.fetch_resource.side_effect = side_effect

        # Call for first library
        result1 = await get_in_process_set(mock_client, "lib1")
        self.assertEqual(result1, {"LIB1_BOOK1", "LIB1_BOOK2"})

        # Call for second library
        result2 = await get_in_process_set(mock_client, "lib2")
        self.assertEqual(result2, {"LIB2_BOOK1", "LIB2_BOOK2", "LIB2_BOOK3"})

        # Verify both are cached separately
        self.assertIn("lib1", _in_process_cache)
        self.assertIn("lib2", _in_process_cache)

        cached_lib1, _ = _in_process_cache["lib1"]
        cached_lib2, _ = _in_process_cache["lib2"]

        self.assertEqual(cached_lib1, {"LIB1_BOOK1", "LIB1_BOOK2"})
        self.assertEqual(cached_lib2, {"LIB2_BOOK1", "LIB2_BOOK2", "LIB2_BOOK3"})

    async def test_empty_response_handling(self):
        """Test handling of empty API responses."""
        # Mock GRIN client with empty response
        mock_client = AsyncMock()
        mock_client.fetch_resource.return_value = ""

        library_dir = "empty_library"

        # Should return empty set
        result = await get_in_process_set(mock_client, library_dir)
        self.assertEqual(result, set())

        # Should still cache the empty result
        self.assertIn(library_dir, _in_process_cache)
        cached_books, _ = _in_process_cache[library_dir]
        self.assertEqual(cached_books, set())

    async def test_whitespace_filtering(self):
        """Test that whitespace and empty lines are properly filtered."""
        # Mock GRIN client with messy response
        mock_client = AsyncMock()
        mock_client.fetch_resource.return_value = "\nBOOK001\n  BOOK002  \n\n  \nBOOK003\n\n"

        library_dir = "messy_library"

        result = await get_in_process_set(mock_client, library_dir)

        # Should filter out empty/whitespace lines and trim
        self.assertEqual(result, {"BOOK001", "BOOK002", "BOOK003"})

    async def test_error_propagation(self):
        """Test that API errors are properly propagated, not masked."""
        # Mock GRIN client to raise an exception
        mock_client = AsyncMock()
        mock_client.fetch_resource.side_effect = Exception("API Error")

        library_dir = "error_library"

        # Should propagate the exception, not mask it
        with self.assertRaises(Exception) as context:
            await get_in_process_set(mock_client, library_dir)

        self.assertEqual(str(context.exception), "API Error")

        # Cache should remain empty
        self.assertNotIn(library_dir, _in_process_cache)

    async def test_concurrent_requests_same_library(self):
        """Test behavior with concurrent requests for the same library."""
        # Mock GRIN client with a delay to simulate network latency
        mock_client = AsyncMock()

        async def delayed_response(*args, **kwargs):
            await asyncio.sleep(0.1)  # 100ms delay
            return "CONCURRENT_BOOK1\nCONCURRENT_BOOK2"

        mock_client.fetch_resource.side_effect = delayed_response

        library_dir = "concurrent_library"

        # Start multiple concurrent requests
        tasks = [
            get_in_process_set(mock_client, library_dir),
            get_in_process_set(mock_client, library_dir),
            get_in_process_set(mock_client, library_dir)
        ]

        results = await asyncio.gather(*tasks)

        # All should return the same result
        expected = {"CONCURRENT_BOOK1", "CONCURRENT_BOOK2"}
        for result in results:
            self.assertEqual(result, expected)

        # Note: Due to race conditions, the API might be called multiple times
        # This is acceptable behavior - we're not implementing request deduplication
        self.assertGreaterEqual(mock_client.fetch_resource.call_count, 1)

    @patch("grin_to_s3.processing.logger")
    async def test_cache_hit_logging(self, mock_logger):
        """Test that cache hits are properly logged at debug level."""
        # Mock GRIN client
        mock_client = AsyncMock()
        mock_client.fetch_resource.return_value = "LOGGED_BOOK1"

        library_dir = "logged_library"

        # First call populates cache
        await get_in_process_set(mock_client, library_dir)

        # Reset logger mock
        mock_logger.reset_mock()

        # Second call should hit cache and log
        await get_in_process_set(mock_client, library_dir)

        # Verify debug log was called
        mock_logger.debug.assert_called_once_with(f"Using cached in_process data for {library_dir}")

    async def test_cache_ttl_boundary_condition(self):
        """Test cache behavior exactly at TTL boundary."""
        # Mock GRIN client
        mock_client = AsyncMock()
        mock_client.fetch_resource.return_value = "BOUNDARY_BOOK1"

        library_dir = "boundary_library"

        # First call populates cache
        await get_in_process_set(mock_client, library_dir)

        # Set cache timestamp to exactly TTL seconds ago (3600)
        if library_dir in _in_process_cache:
            books, _ = _in_process_cache[library_dir]
            boundary_timestamp = time.time() - 3600  # Exactly at TTL
            _in_process_cache[library_dir] = (books, boundary_timestamp)

        # Reset mock with different response
        mock_client.reset_mock()
        mock_client.fetch_resource.return_value = "BOUNDARY_BOOK2"

        # Call should fetch fresh data (cache expired)
        result = await get_in_process_set(mock_client, library_dir)

        # Should call API and return new result
        mock_client.fetch_resource.assert_called_once()
        self.assertEqual(result, {"BOUNDARY_BOOK2"})
