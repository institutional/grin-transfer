#!/usr/bin/env python3
"""
Test OAuth2 implementation
"""

import asyncio
import os
import sys

sys.path.append(os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))

import aiohttp

from grin_to_s3.auth import GRINAuth


async def test_credentials():
    """Test credential loading."""
    print("Testing Credential Loading")

    auth = GRINAuth()

    try:
        await auth.get_credentials()
        print("✅ Credentials loaded successfully")
        return True
    except Exception as e:
        print(f"❌ Credential loading failed: {e}")
        return False


async def test_bearer_token():
    """Test bearer token generation."""
    print("Testing Bearer Token")

    auth = GRINAuth()

    try:
        token = await auth.get_bearer_token()
        print(f"✅ Bearer token: {token[:30]}...")
        return True
    except Exception as e:
        print(f"❌ Bearer token failed: {e}")
        return False


async def test_grin_api():
    """Test GRIN API access."""
    print("Testing GRIN API")

    auth = GRINAuth()

    try:
        async with aiohttp.ClientSession() as session:
            url = "https://books.google.com/libraries/Harvard/_all_books?format=text&mode=all&result_count=3"

            response = await auth.make_authenticated_request(session, url)
            text = await response.text()

            lines = text.strip().split("\n")
            print(f"✅ GRIN API working: {len(lines)} books retrieved")

            if lines:
                print(f"   Sample: {lines[0][:60]}...")

            return True

    except Exception as e:
        print(f"❌ GRIN API failed: {e}")
        return False


async def main():
    """Run all tests."""
    print("Testing GRIN Authentication\n")

    tests = [
        test_credentials(),
        test_bearer_token(),
        test_grin_api(),
    ]

    results = await asyncio.gather(*tests, return_exceptions=True)

    print()

    success_count = sum(1 for r in results if r is True)
    total_tests = len(tests)

    if success_count == total_tests:
        print("✅ All tests passed! Auth implementation working perfectly.")
    else:
        print(f"⚠️  {success_count}/{total_tests} tests passed.")


if __name__ == "__main__":
    asyncio.run(main())
