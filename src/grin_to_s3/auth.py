"""
OAuth2 implementation for GRIN access
"""

import asyncio
import json
import logging
import sys
import time
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

import aiohttp
from google.auth.transport.requests import Request
from google.oauth2.credentials import Credentials
from google_auth_oauthlib.flow import InstalledAppFlow

logger = logging.getLogger(__name__)


# OAuth2 scopes for GRIN access
SCOPES = [
    "openid",
    "https://www.googleapis.com/auth/userinfo.email",
    "https://www.googleapis.com/auth/userinfo.profile",
]


class AuthError(Exception):
    """Base exception for authentication errors."""

    pass


class CredentialsMissingError(AuthError):
    """Raised when credentials are missing or invalid."""

    pass


class GRINPermissionError(AuthError):
    """Raised when GRIN denies access (403)."""

    pass


class GRINAuth:
    """Async OAuth2 implementation for GRIN access."""

    def __init__(
        self, secrets_file: str | None = None, credentials_file: str | None = None, secrets_dir: str | None = None
    ):
        """
        Initialize GRIN authentication.

        Args:
            secrets_file: Path to OAuth2 secrets file. If None, will search default locations.
            credentials_file: Path to credentials file. If None, will search default locations.
            secrets_dir: Directory containing secrets files. If None, will search default locations.
        """
        self.secrets_file = self._find_secrets_file(secrets_file, secrets_dir)
        self.credentials_file = self._find_credentials_file(credentials_file, secrets_dir)
        self.credentials: Credentials | None = None

    def _find_secrets_file(self, secrets_file: str | None, secrets_dir: str | None) -> Path:
        """Find the OAuth2 secrets file, checking multiple locations."""
        if secrets_file:
            return Path(secrets_file)

        # Search locations in order of preference
        search_paths = []

        # If secrets_dir is provided, check there first
        if secrets_dir:
            search_paths.append(Path(secrets_dir) / "client_secret.json")
            search_paths.append(Path(secrets_dir) / "secrets.json")

        # Check user's home directory (following XDG Base Directory specification)
        home = Path.home()
        search_paths.extend(
            [
                # XDG config directory (Linux/Unix standard)
                home / ".config" / "grin-to-s3" / "client_secret.json",
                home / ".config" / "grin-to-s3" / "secrets.json",
            ]
        )

        for path in search_paths:
            if path.exists():
                logger.info(f"Found secrets file: {path}")
                return path

        # Default to XDG config directory
        default_path = home / ".config" / "grin-to-s3" / "client_secret.json"
        logger.warning(f"No secrets file found, using default: {default_path}")
        return default_path

    def _find_credentials_file(self, credentials_file: str | None, secrets_dir: str | None) -> Path:
        """Find the credentials file, checking multiple locations."""
        if credentials_file:
            return Path(credentials_file)

        # Search locations in order of preference
        search_paths = []

        # If secrets_dir is provided, check there first
        if secrets_dir:
            search_paths.append(Path(secrets_dir) / "credentials.json")
            search_paths.append(Path(secrets_dir) / "token.json")

        # Check user's home directory (following XDG Base Directory specification)
        home = Path.home()
        search_paths.extend(
            [
                # XDG config directory (Linux/Unix standard)
                home / ".config" / "grin-to-s3" / "credentials.json",
                home / ".config" / "grin-to-s3" / "token.json",
            ]
        )

        for path in search_paths:
            if path.exists():
                logger.info(f"Found credentials file: {path}")
                return path

        # Default to XDG config directory
        default_path = home / ".config" / "grin-to-s3" / "credentials.json"
        return default_path

    def _load_credentials(self) -> Credentials | None:
        """Load existing credentials from JSON file."""
        if not self.credentials_file.exists():
            return None

        try:
            with open(self.credentials_file) as f:
                creds_data = json.load(f)

            credentials = Credentials(
                token=creds_data["token"],
                refresh_token=creds_data["refresh_token"],
                token_uri=creds_data["token_uri"],
                client_id=creds_data["client_id"],
                client_secret=creds_data["client_secret"],
                scopes=creds_data["scopes"],
            )

            return credentials

        except (json.JSONDecodeError, KeyError, FileNotFoundError):
            return None

    def _save_credentials(self, credentials: Credentials) -> None:
        """Save credentials to JSON file."""
        creds_data = {
            "token": credentials.token,
            "refresh_token": credentials.refresh_token,
            "token_uri": credentials.token_uri,
            "client_id": credentials.client_id,
            "client_secret": credentials.client_secret,
            "scopes": credentials.scopes,
        }

        with open(self.credentials_file, "w") as f:
            json.dump(creds_data, f, indent=2)

        # Secure the credentials file
        self.credentials_file.chmod(0o600)

    def _refresh_credentials(self, credentials: Credentials, force: bool = False) -> Credentials:
        """Refresh expired credentials."""
        if (force or credentials.expired) and credentials.refresh_token:
            refresh_start = time.time()
            try:
                logger.debug(f"Starting credential refresh at {datetime.now().strftime('%H:%M:%S.%f')[:-3]}")
                logger.debug(f"{credentials.expired=}, {force=}")
                request = Request()
                credentials.refresh(request)  # This is where potential hangs occur
                refresh_elapsed = time.time() - refresh_start
                refresh_time = datetime.now().strftime("%H:%M:%S.%f")[:-3]
                logger.debug(f"Credential refresh completed in {refresh_elapsed:.1f}s at {refresh_time}")
                self._save_credentials(credentials)
            except Exception as e:
                failed_elapsed = time.time() - refresh_start
                logger.error(f"Credential refresh failed after {failed_elapsed:.1f}s: {e}")
                raise AuthError(f"Token refresh failed: {e}") from e
        return credentials

    async def get_credentials(self) -> Credentials:
        """
        Get valid credentials, handling refresh as needed.

        Returns:
            Credentials: Valid credentials for GRIN access

        Raises:
            CredentialsMissingError: If credentials can't be obtained
        """
        # Load credentials if not already cached
        if not self.credentials:
            self.credentials = self._load_credentials()

        if self.credentials:
            # Refresh if needed
            self.credentials = self._refresh_credentials(self.credentials)
            return self.credentials

        # No valid credentials found
        raise CredentialsMissingError(f"No credentials found at {self.credentials_file}. Run OAuth setup first.")

    async def get_bearer_token(self) -> str:
        """Get current bearer token for API requests."""
        credentials = await self.get_credentials()
        if not credentials.token:
            raise AuthError("No valid access token available")
        return credentials.token

    async def validate_credentials(self, directory: str = "Harvard") -> bool:
        """
        Validate credentials by making a lightweight test request to GRIN.

        Args:
            directory: GRIN directory to test against (default: Harvard)

        Returns:
            bool: True if credentials are valid and working

        Raises:
            AuthError: If credentials are invalid and cannot be refreshed
        """
        try:
            # Make a lightweight test request to GRIN (following Google's documentation pattern)
            import aiohttp

            credentials = await self.get_credentials()

            # Use proper timeout and connector cleanup
            timeout = aiohttp.ClientTimeout(total=30, connect=10)
            connector = aiohttp.TCPConnector(limit=10, limit_per_host=5)

            # Test with a minimal GRIN request (just check if we can access the directory)
            test_url = f"https://books.google.com/libraries/{directory}/?format=text&result_count=1"

            async with aiohttp.ClientSession(timeout=timeout, connector=connector) as session:
                headers = {"Authorization": f"Bearer {credentials.token}"}

                async with session.get(test_url, headers=headers, allow_redirects=False) as response:
                    if response.status == 200:
                        print("✅ Credentials validated successfully")
                        return True
                    elif response.status == 401:
                        # Try to refresh credentials
                        if credentials.refresh_token:
                            print("Credentials expired - attempting refresh...")
                            try:
                                # Force refresh by setting expiry to past
                                from datetime import datetime

                                credentials.expiry = datetime.now(UTC)
                                self.credentials = self._refresh_credentials(credentials)

                                # Test the refreshed credentials
                                headers["Authorization"] = f"Bearer {self.credentials.token}"
                                async with session.get(
                                    test_url, headers=headers, allow_redirects=False
                                ) as test_response:
                                    if test_response.status == 200:
                                        print("✅ Credential refresh and validation successful")
                                        return True
                                    elif test_response.status == 302:
                                        # 302 redirect often means credentials work but need additional auth
                                        print("✅ Credentials valid")
                                        return True
                                    else:
                                        raise AuthError(
                                            f"Refreshed credentials still invalid (status {test_response.status}). "
                                            "Run 'python auth.py setup' to recreate credentials."
                                        )
                            except Exception as e:
                                print(f"❌ Credential refresh failed: {e}")
                                raise AuthError(
                                    f"Credentials invalid and refresh failed: {e}. "
                                    "Run 'python auth.py setup' to recreate credentials."
                                ) from e
                        else:
                            raise AuthError(
                                "Credentials invalid and no refresh token available. "
                                "Run 'python auth.py setup' to recreate credentials."
                            )
                    elif response.status == 302:
                        # 302 redirect can be normal for GRIN auth flow
                        print("✅ Credentials valid")
                        return True
                    elif response.status == 403:
                        raise AuthError(f"GRIN access denied (403). Check directory permissions for '{directory}'.")
                    else:
                        raise AuthError(f"GRIN validation failed with status {response.status}")

        except aiohttp.ClientError as e:
            raise AuthError(f"Network error during credential validation: {e}") from e
        except Exception as e:
            if isinstance(e, AuthError):
                raise
            raise AuthError(f"Unexpected error during credential validation: {e}") from e

    async def make_authenticated_request(
        self, session: aiohttp.ClientSession, url: str, method: str = "GET", retry_refresh: bool = True, **kwargs: Any
    ) -> aiohttp.ClientResponse:
        """
        Make authenticated request to GRIN API with automatic credential revalidation.

        Args:
            session: aiohttp session
            url: URL to request
            method: HTTP method
            retry_refresh: Whether to retry after token refresh on auth failure
            **kwargs: Additional arguments for aiohttp

        Returns:
            aiohttp.ClientResponse: Response object

        Raises:
            GRINPermissionError: If GRIN denies access (403)
            AuthError: For other authentication issues
        """
        credentials = await self.get_credentials()

        headers = kwargs.get("headers", {})
        headers["Authorization"] = f"Bearer {credentials.token}"
        kwargs["headers"] = headers

        # Disable automatic redirects for GRIN (302 redirects are normal)
        kwargs.setdefault("allow_redirects", False)
        response = await session.request(method, url, **kwargs)

        # Check for redirect to login (invalid credentials)
        location = response.headers.get("Location", "")
        if response.status == 302 and "accounts.google.com/ServiceLogin" in location:
            if retry_refresh and credentials.refresh_token:
                logger.debug("Detected credential expiration - attempting token refresh...")
                try:
                    # Try refreshing the token
                    self.credentials = self._refresh_credentials(credentials, force=True)
                    logger.debug("Token refresh successful")

                    # Retry the request with new token
                    headers["Authorization"] = f"Bearer {self.credentials.token}"
                    kwargs["headers"] = headers
                    return await self.make_authenticated_request(session, url, method, retry_refresh=False, **kwargs)
                except Exception as e:
                    logger.error(f"Token refresh failed: {e}")
                    raise AuthError(
                        f"Credentials expired and refresh failed: {e}. "
                        "Run 'python auth.py setup' to recreate credentials."
                    ) from e
            else:
                raise AuthError(
                    "Redirected to Google login. Credentials may be invalid. "
                    "Run 'python auth.py setup' to recreate credentials."
                )

        # Handle other authentication errors (401, etc.)
        if response.status == 401:
            if retry_refresh and credentials.refresh_token:
                logger.debug("Received 401 Unauthorized - attempting token refresh...")
                try:
                    self.credentials = self._refresh_credentials(credentials, force=True)
                    logger.debug("Token refresh successful")

                    # Retry the request with new token
                    headers["Authorization"] = f"Bearer {self.credentials.token}"
                    kwargs["headers"] = headers
                    return await self.make_authenticated_request(session, url, method, retry_refresh=False, **kwargs)
                except Exception as e:
                    logger.error(f"Token refresh failed: {e}")
                    raise AuthError(
                        f"Authentication failed and refresh failed: {e}. "
                        "Run 'python auth.py setup' to recreate credentials."
                    ) from e
            else:
                raise AuthError("Authentication failed (401). Run 'python auth.py setup' to recreate credentials.")

        # Handle GRIN-specific responses
        if response.status == 403:
            raise GRINPermissionError(
                "GRIN denied this request (403). You may not have permission to "
                "access this directory, or ACL changes can take up to 12 hours."
            )
        elif response.status == 302:
            # 302 redirects are normal for GRIN - follow the redirect manually
            redirect_location: str | None = response.headers.get("Location")
            if redirect_location:
                # Follow redirect with credentials
                return await self.make_authenticated_request(
                    session, redirect_location, method, retry_refresh=retry_refresh, **kwargs
                )
            else:
                # No location header, return the redirect response
                response.raise_for_status()
                return response

        response.raise_for_status()
        return response


def setup_credentials(secrets_file: str | None = None, credentials_file: str | None = None) -> bool:
    """
    Interactive setup to walk user through credential configuration.
    """
    print("GRIN OAuth2 Credential Setup")
    print("=" * 32)

    # Use the same file search logic as GRINAuth
    auth = GRINAuth(secrets_file, credentials_file)
    secrets_path = auth.secrets_file
    creds_path = auth.credentials_file

    # Step 1: Check for secrets file
    print("\nStep 1: OAuth2 Client Configuration")
    if not secrets_path.exists():
        print(f"❌ Missing client secrets file: {secrets_path}")
        print("\nYou need to create a Google Cloud OAuth2 application:")
        print("   1. Go to: https://console.cloud.google.com/apis/credentials")
        print("   2. Create a new project or select existing")
        print("   3. Click 'Create Credentials' → 'OAuth 2.0 Client IDs'")
        print("   4. Application type: 'Desktop application'")
        print("   5. Download the JSON file")
        print(f"   6. Save it as '{secrets_path}'")

        print("\nThe file should look like:")
        print("   {")
        print('     "installed": {')
        print('       "client_id": "your-client-id.apps.googleusercontent.com",')
        print('       "client_secret": "your-client-secret",')
        print('       "project_id": "your-project-id",')
        print('       "auth_uri": "https://accounts.google.com/o/oauth2/auth",')
        print('       "token_uri": "https://oauth2.googleapis.com/token",')
        print('       "redirect_uris": ["http://localhost:8080"]')
        print("     }")
        print("   }")

        input(f"\nPress Enter once you've saved the secrets file as '{secrets_path}'...")

        if not secrets_path.exists():
            print(f"❌ Still can't find {secrets_path}. Please create it first.")
            return False

    print(f"✅ Found client secrets: {secrets_path}")

    # Step 2: Validate secrets file
    try:
        with open(secrets_path) as f:
            secrets_data = json.load(f)

        if "installed" not in secrets_data:
            print("❌ Invalid secrets file format. Missing 'installed' key.")
            return False

        required_keys = ["client_id", "client_secret", "auth_uri", "token_uri"]
        missing_keys = [key for key in required_keys if key not in secrets_data["installed"]]

        if missing_keys:
            print(f"❌ Missing required keys in secrets file: {missing_keys}")
            return False

        print("✅ Client secrets file is valid")

    except json.JSONDecodeError:
        print("❌ Secrets file is not valid JSON")
        return False
    except Exception as e:
        print(f"❌ Error reading secrets file: {e}")
        return False

    # Step 3: OAuth2 Flow
    print("\nStep 2: OAuth2 Authorization")

    # Check if existing credentials have compatible scopes
    if creds_path.exists():
        try:
            with open(creds_path) as f:
                existing_creds = json.load(f)

            existing_scopes = set(existing_creds.get("scopes", []))
            current_scopes = set(SCOPES)

            if existing_scopes == current_scopes:
                print(f"Compatible credentials found: {creds_path}")
                overwrite = input("Do you want to create new credentials? (y/N): ").strip().lower()
                if overwrite not in ["y", "yes"]:
                    print("✅ Keeping existing credentials")
                    return True
            else:
                print("Existing credentials have different scopes")
                print(f"Existing: {sorted(existing_scopes)}")
                print(f"Required: {sorted(current_scopes)}")
                print("New credentials required")

        except (json.JSONDecodeError, KeyError, FileNotFoundError):
            print(f"Invalid credentials file found: {creds_path}")
            print("New credentials required")

    print("Starting OAuth2 flow...")
    print("This will open your browser for Google authentication")

    try:
        # Use InstalledAppFlow which handles the local server properly
        flow = InstalledAppFlow.from_client_secrets_file(str(secrets_path), scopes=SCOPES)

        print("\nStarting local server for OAuth2 callback...")
        print("This will open your browser automatically for Google authentication")
        print("If the browser doesn't open automatically, copy the URL that appears")

        # Run the local server flow with auto-shutdown
        credentials = flow.run_local_server(
            host="localhost",
            port=8080,
            open_browser=True,
            prompt="consent",
            authorization_prompt_message="",
            success_message="Authentication successful! You can close this browser tab.",
            stop_server=True,
        )

        # Save credentials
        creds_data = {
            "token": credentials.token,
            "refresh_token": credentials.refresh_token,
            "token_uri": credentials.token_uri,
            "client_id": credentials.client_id,
            "client_secret": credentials.client_secret,
            "scopes": credentials.scopes,
        }

        with open(creds_path, "w") as f:
            json.dump(creds_data, f, indent=2)

        # Secure the credentials file
        creds_path.chmod(0o600)

        print(f"✅ Credentials saved to: {creds_path}")
        print("File permissions set to 600 (owner read/write only)")

    except Exception as e:
        print(f"❌ OAuth2 flow failed: {e}")
        return False

    # Step 4: Test credentials
    print("\nStep 3: Testing Credentials")

    try:
        # Reuse the same auth instance to ensure consistent file paths
        test_auth = GRINAuth(str(secrets_path), str(creds_path))

        # Test credential loading
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)

        loop.run_until_complete(test_auth.get_credentials())
        print("✅ Credentials loaded successfully")

        # Test bearer token
        token = loop.run_until_complete(test_auth.get_bearer_token())
        print(f"✅ Bearer token generated: {token[:30]}...")

        loop.close()

    except Exception as e:
        print(f"❌ Credential test failed: {e}")
        return False

    # Success!
    print("\n✅ Setup Complete!")
    print("=" * 16)
    print(f"Client secrets: {secrets_path}")
    print(f"User credentials: {creds_path}")
    print("OAuth2 authentication working")

    return True


def main() -> None:
    """
    Command-line interface for credential setup.
    """
    if len(sys.argv) > 1 and sys.argv[1] == "setup":
        success = setup_credentials()
        sys.exit(0 if success else 1)
    else:
        print("GRIN OAuth2 Authentication")
        print("Usage:")
        print("  python auth.py setup    # Interactive credential setup")
        print("\nFor programmatic use, import GRINAuth class")


if __name__ == "__main__":
    main()
