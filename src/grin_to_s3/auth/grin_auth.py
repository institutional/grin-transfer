"""
OAuth2 implementation for GRIN access
"""

import json
import logging
import os
import time
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

import aiohttp
from google.auth.transport.requests import Request
from google.oauth2.credentials import Credentials

from .exceptions import AuthError, CredentialsMissingError, GRINPermissionError

logger = logging.getLogger(__name__)


# OAuth2 scopes for GRIN access
SCOPES = [
    "openid",
    "https://www.googleapis.com/auth/userinfo.email",
    "https://www.googleapis.com/auth/userinfo.profile",
]


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

        # Check environment variable first (for Docker)
        env_path = os.environ.get("GRIN_CLIENT_SECRET_FILE")
        if env_path and Path(env_path).exists():
            return Path(env_path)

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
                return path

        # Default to XDG config directory
        default_path = home / ".config" / "grin-to-s3" / "client_secret.json"
        return default_path

    def _find_credentials_file(self, credentials_file: str | None, secrets_dir: str | None) -> Path:
        """Find the credentials file, checking multiple locations."""
        if credentials_file:
            return Path(credentials_file)

        # Check environment variable first (for Docker)
        env_dir = os.environ.get("GRIN_CREDENTIALS_DIR")
        if env_dir:
            return Path(env_dir) / "credentials.json"

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

    async def validate_credentials(self, directory: str) -> bool:
        """
        Validate credentials by making a lightweight test request to GRIN.

        Args:
            directory: GRIN directory to test against

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
