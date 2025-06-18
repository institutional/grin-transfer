#!/usr/bin/env python3
"""
Test async GRIN client
"""

import asyncio
import os
import sys

sys.path.append(os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))

from grin_to_s3.client import GRINClient


async def test_fetch_resource():
    """Test basic resource fetching."""
    print("Testing Resource Fetching")

    client = GRINClient()

    try:
        # Test basic resource fetch
        result = await client.fetch_resource(directory="Harvard", resource="_all_books?format=text&result_count=3")

        lines = result.strip().split("\n")
        print(f"✓ Fetched {len(lines)} books")

        if lines:
            print(f"   Sample: {lines[0][:60]}...")

        return True

    except Exception as e:
        print(f"✗ Resource fetch failed: {e}")
        return False


async def test_book_list():
    """Test book list functionality."""
    print("Testing Book List")

    client = GRINClient()

    try:
        # Test book list with limit
        books = await client.get_book_list(directory="Harvard", result_count=5, mode_all=True)

        print(f"✓ Retrieved {len(books)} books")

        if books:
            print(f"   Sample: {books[0]}")

        return True

    except Exception as e:
        print(f"✗ Book list failed: {e}")
        return False


async def test_streaming():
    """Test streaming functionality."""
    print("Testing Streaming")

    client = GRINClient()

    try:
        count = 0
        sample_books = []

        # Stream first few books using HTML streaming method
        async for barcode in client.stream_book_list_html(
            directory="Harvard", list_type="_all_books", page_size=10, max_pages=2
        ):
            count += 1
            if len(sample_books) < 3:
                sample_books.append(barcode)

            # Stop after reasonable amount for testing
            if count >= 10:
                break

        print(f"✓ Streamed {count} books")

        if sample_books:
            print(f"   Samples: {sample_books}")

        return True

    except Exception as e:
        print(f"✗ Streaming failed: {e}")
        return False


async def test_bearer_token():
    """Test bearer token functionality."""
    print("Testing Bearer Token")

    client = GRINClient()

    try:
        token = await client.get_bearer_token()
        print(f"✓ Bearer token: {token[:30]}...")
        return True

    except Exception as e:
        print(f"✗ Bearer token failed: {e}")
        return False


async def main():
    """Run all client tests."""
    print("Testing GRIN Client\n")

    tests = [
        test_bearer_token(),
        test_fetch_resource(),
        test_book_list(),
        test_streaming(),
    ]

    results = await asyncio.gather(*tests, return_exceptions=True)

    print()

    success_count = sum(1 for r in results if r is True)
    total_tests = len(tests)

    if success_count == total_tests:
        print("✓ All client tests passed! HTTP client working perfectly.")
    else:
        print(f"! {success_count}/{total_tests} tests passed.")


if __name__ == "__main__":
    asyncio.run(main())
