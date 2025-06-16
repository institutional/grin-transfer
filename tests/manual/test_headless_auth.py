#!/usr/bin/env python3
"""
Test headless authentication workflow.
Simulates server environment where credentials exist but no interactive flow is available.
"""

import asyncio
import shutil
import sys
import tempfile
from pathlib import Path

# Add parent directory to path for imports
sys.path.insert(0, str(Path(__file__).parent.parent.parent))

from auth import CredentialsMissingError, GRINAuth


async def test_headless_auth():
    """Test authentication without interactive flow."""
    print("Testing headless authentication workflow...")
    print("=" * 50)

    # Test 1: Normal operation with existing credentials
    print("\nTest 1: Using existing credentials")
    try:
        auth = GRINAuth(".secrets", ".creds.json")
        credentials = await auth.get_credentials()
        token = await auth.get_bearer_token()

        print("✅ Loaded credentials successfully")
        print(f"✅ Generated bearer token: {token[:30]}...")
        print(f"✅ Refresh token available: {'Yes' if credentials.refresh_token else 'No'}")

    except Exception as e:
        print(f"❌ Failed: {e}")
        return False

    # Test 2: Simulate headless server environment
    print("\nTest 2: Simulating headless server deployment")

    # Create temporary directory to simulate server
    with tempfile.TemporaryDirectory() as temp_dir:
        temp_path = Path(temp_dir)
        server_secrets = temp_path / ".secrets"
        server_creds = temp_path / ".creds.json"

        # Copy auth files to "server"
        shutil.copy2(".secrets", server_secrets)
        shutil.copy2(".creds.json", server_creds)

        print(f"✅ Copied auth files to simulated server: {temp_dir}")

        # Test authentication in "server" environment
        try:
            server_auth = GRINAuth(str(server_secrets), str(server_creds))
            await server_auth.get_credentials()
            server_token = await server_auth.get_bearer_token()

            print("✅ Server authentication successful")
            print(f"✅ Server bearer token: {server_token[:30]}...")

        except Exception as e:
            print(f"❌ Server authentication failed: {e}")
            return False

    # Test 3: Missing credentials scenario
    print("\nTest 3: Missing credentials handling")
    try:
        missing_auth = GRINAuth("nonexistent.secrets", "nonexistent.creds")
        await missing_auth.get_credentials()
        print("❌ Should have failed with missing credentials")
        return False

    except CredentialsMissingError as e:
        print(f"✅ Correctly handled missing credentials: {e}")

    except Exception as e:
        print(f"❌ Unexpected error: {e}")
        return False

    # Test 4: Test with actual GRIN API call using client
    print("\nTest 4: Testing with GRIN API call")
    try:
        from client import GRINClient

        client = GRINClient()
        result = await client.fetch_resource(directory="Harvard", resource="_all_books?format=text&result_count=3")

        lines = result.strip().split("\n")

        print("✅ GRIN API call successful")
        print(f"✅ Retrieved {len(lines)} lines of data")
        if lines:
            print(f"✅ Sample line: {lines[0][:50]}...")

    except Exception as e:
        print(f"❌ GRIN API test failed: {e}")
        return False

    print("\n" + "=" * 50)
    print("✅ All headless authentication tests passed!")
    print("\nHeadless deployment workflow:")
    print("1. Run OAuth setup on development machine")
    print("2. Securely transfer .secrets and .creds.json to server")
    print("3. Server applications use existing credentials automatically")
    print("4. Refresh tokens handle long-term authentication")

    return True


async def main():
    """Main test function."""
    success = await test_headless_auth()
    return 0 if success else 1


if __name__ == "__main__":
    exit_code = asyncio.run(main())
    exit(exit_code)
