"""
OAuth2 implementation for GRIN access
"""

import json
import logging
import os
import sys
import time
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

import aiohttp
from google.auth.transport.requests import Request
from google.oauth2.credentials import Credentials
from google_auth_oauthlib.flow import InstalledAppFlow

from ..docker import is_docker_environment
from .exceptions import AuthError, CredentialsMissingError, GRINPermissionError

logger = logging.getLogger(__name__)

# Default credentials directory
DEFAULT_CREDENTIALS_DIR = Path.home() / ".config" / "grin-to-s3"


def detect_remote_shell() -> bool:
    """
    Detect if running in a remote shell environment (SSH, cloud instance, etc.).

    Returns:
        bool: True if remote shell environment is detected
    """
    # Check for SSH connection indicators
    ssh_indicators = ["SSH_CLIENT", "SSH_TTY", "SSH_CONNECTION"]

    for indicator in ssh_indicators:
        if os.environ.get(indicator):
            return True

    # Check TERM patterns common in SSH sessions
    term = os.environ.get("TERM", "")
    if term in ["screen", "tmux", "screen-256color", "tmux-256color"]:
        return True

    # Check if DISPLAY is not set (headless environment)
    # But exclude Docker containers which also don't have DISPLAY but should use Docker flow
    if not os.environ.get("DISPLAY") and not sys.platform.startswith("win") and not is_docker_environment():
        # Additional check: if we're on a known cloud platform
        cloud_indicators = ["AWS_EXECUTION_ENV", "GOOGLE_CLOUD_PROJECT", "AZURE_FUNCTIONS_ENVIRONMENT"]
        for indicator in cloud_indicators:
            if os.environ.get(indicator):
                return True

    return False


def manual_authorization_flow(flow: InstalledAppFlow) -> Credentials:
    """
    Perform manual authorization code flow for remote shell environments.

    Args:
        flow: Configured OAuth2 flow

    Returns:
        Credentials: Obtained credentials

    Raises:
        Exception: If authorization fails
    """
    # Configure flow for out-of-band (manual) mode
    flow.redirect_uri = "urn:ietf:wg:oauth:2.0:oob"

    # Generate authorization URL
    auth_url, _ = flow.authorization_url(prompt="consent", access_type="offline")

    # Display instructions to user
    print("\n" + "=" * 60)
    print("REMOTE SHELL OAUTH2 SETUP")
    print("=" * 60)
    print("Remote shell detected - manual authorization required")
    print()
    print("1. Open this URL in a browser on your LOCAL machine:")
    print(f"   {auth_url}")
    print()
    print("2. Complete Google authentication with your GRIN account")
    print("3. Google will display an authorization code")
    print("4. Copy the code and paste it below")
    print("=" * 60)
    print()

    # Get authorization code from user
    while True:
        try:
            auth_code = input("Authorization code: ").strip()
            if auth_code:
                break
            print("Please enter the authorization code.")
        except KeyboardInterrupt:
            raise Exception("Authorization cancelled by user") from None

    # Exchange code for tokens
    try:
        flow.fetch_token(code=auth_code)
        return flow.credentials  # type: ignore
    except Exception as e:
        raise Exception(f"Failed to exchange authorization code: {e}") from e


# OAuth2 scopes for GRIN access
SCOPES = [
    "openid",
    "https://www.googleapis.com/auth/userinfo.email",
    "https://www.googleapis.com/auth/userinfo.profile",
]


class GRINAuth:
    """Async OAuth2 implementation for GRIN access."""

    def __init__(
        self,
        secrets_file: Path | str | None = None,
        credentials_file: Path | str | None = None,
        secrets_dir: Path | str | None = None,
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

    def _find_secrets_file(self, secrets_file: Path | str | None, secrets_dir: Path | str | None) -> Path:
        """Find the OAuth2 secrets file, checking multiple locations."""
        if secrets_file:
            return Path(secrets_file)

        # Check credentials directory first (configurable, used by Docker)
        creds_dir = os.environ.get("GRIN_CREDENTIALS_DIR", str(DEFAULT_CREDENTIALS_DIR))
        creds_path = Path(creds_dir) / "client_secret.json"
        if creds_path.exists():
            return creds_path

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

    def _find_credentials_file(self, credentials_file: Path | str | None, secrets_dir: Path | str | None) -> Path:
        """Find the credentials file, checking multiple locations."""
        if credentials_file:
            return Path(credentials_file)

        # Check writable credentials directory first (for Docker and OAuth tokens)
        writable_dir = os.environ.get("GRIN_WRITABLE_CREDENTIALS_DIR")
        if writable_dir:
            return Path(writable_dir) / "credentials.json"

        # Fall back to main credentials directory
        creds_dir = os.environ.get("GRIN_CREDENTIALS_DIR", str(DEFAULT_CREDENTIALS_DIR))
        return Path(creds_dir) / "credentials.json"

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


def _do_credential_setup(secrets_path: Path, creds_path: Path, remote_auth: bool = False) -> tuple[bool, str]:
    """
    Internal credential setup logic that returns results instead of calling exit().

    Returns:
        tuple[bool, str]: (success, error_message)
    """
    # Step 1: Check for secrets file
    if not secrets_path.exists():
        return False, f"missing_secrets_file:{secrets_path}"

    # Step 2: Validate secrets file
    try:
        with open(secrets_path) as f:
            secrets_data = json.load(f)

        if "installed" not in secrets_data:
            return False, "Invalid secrets file format. Missing 'installed' key."

        required_keys = ["client_id", "client_secret", "auth_uri", "token_uri"]
        missing_keys = [key for key in required_keys if key not in secrets_data["installed"]]

        if missing_keys:
            return False, f"Missing required keys in secrets file: {missing_keys}"

    except FileNotFoundError:
        return False, f"Client secrets file not found: {secrets_path}"
    except json.JSONDecodeError:
        return False, "Secrets file is not valid JSON"
    except Exception as e:
        return False, f"Error reading secrets file: {e}"

    # Show Step 1 information early
    print("\nStep 1: OAuth2 Client Configuration")
    print(f"Looking for client secrets at: {secrets_path}")
    print("✅ Client secrets file is valid")

    # Step 3: Check if existing credentials have compatible scopes
    if creds_path.exists():
        try:
            with open(creds_path) as f:
                existing_creds = json.load(f)

            existing_scopes = set(existing_creds.get("scopes", []))
            current_scopes = set(SCOPES)

            if existing_scopes == current_scopes:
                return True, f"Compatible credentials found: {creds_path}"
            # If scopes don't match, continue with new credential creation

        except (json.JSONDecodeError, KeyError, FileNotFoundError):
            pass  # Continue with new credential creation

    # Step 4: Execute OAuth2 Flow
    try:
        # Use InstalledAppFlow which handles the local server properly
        flow = InstalledAppFlow.from_client_secrets_file(str(secrets_path), scopes=SCOPES)

        # Determine which authentication flow to use
        is_remote = remote_auth or detect_remote_shell()
        is_docker = is_docker_environment()

        if is_remote:
            # Remote shell environment - use manual authorization code flow
            credentials = manual_authorization_flow(flow)
        elif is_docker:
            # Show Docker-specific setup instructions immediately
            print("\nStep 2: OAuth2 Authorization")
            print("Running in Docker container - using port forwarding flow")
            print("\n" + "=" * 60)
            print("DOCKER OAUTH2 SETUP")
            print("=" * 60)

            # Get OAuth2 port from environment
            oauth_port = int(os.environ.get("GRIN_OAUTH_PORT", "58432"))

            print(f"1. The OAuth2 server will start on port {oauth_port}")
            print("2. A URL will be displayed for you to visit")
            print("3. Complete the Google authorization")
            print(f"4. The browser will redirect to localhost:{oauth_port}")
            print("   This will complete authentication automatically")
            print("=" * 60)

            # Generate and display the authorization URL
            flow.redirect_uri = f"http://localhost:{oauth_port}"
            auth_url, _ = flow.authorization_url(prompt="consent", access_type="offline")

            # Start local server with Docker-compatible settings
            import urllib.parse
            from http.server import BaseHTTPRequestHandler, HTTPServer

            auth_code = None
            server_error = None

            class CallbackHandler(BaseHTTPRequestHandler):
                def do_GET(self):
                    nonlocal auth_code, server_error

                    # Parse the callback URL
                    parsed_url = urllib.parse.urlparse(self.path)
                    query_params = urllib.parse.parse_qs(parsed_url.query)

                    if "code" in query_params:
                        auth_code = query_params["code"][0]
                        self.send_response(200)
                        self.send_header("Content-type", "text/html")
                        self.end_headers()
                        self.wfile.write(
                            b"<html><body><h1>Authentication successful!</h1><p>You can close this browser tab.</p></body></html>"
                        )
                    elif "error" in query_params:
                        server_error = query_params["error"][0]
                        self.send_response(400)
                        self.send_header("Content-type", "text/html")
                        self.end_headers()
                        self.wfile.write(
                            b"<html><body><h1>Authentication failed!</h1><p>Error: "
                            + server_error.encode()
                            + b"</p></body></html>"
                        )
                    else:
                        self.send_response(400)
                        self.send_header("Content-type", "text/html")
                        self.end_headers()
                        self.wfile.write(b"<html><body><h1>Invalid request</h1></body></html>")

                def log_message(self, _format, *_args):
                    # Suppress log messages
                    pass

            # Start the HTTP server
            server = HTTPServer(("0.0.0.0", oauth_port), CallbackHandler)
            server.timeout = 300  # 5 minute timeout

            print(f"\nOAuth2 server started on port {oauth_port}")
            print("Please visit this URL to complete authentication:")
            print(f"\n{auth_url}\n")
            print("Waiting for authorization callback...")

            # Handle the request
            server.handle_request()
            server.server_close()

            if server_error:
                raise Exception(f"OAuth2 authorization failed: {server_error}")

            if not auth_code:
                raise Exception("No authorization code received")

            # Exchange the authorization code for credentials
            token = flow.fetch_token(code=auth_code)

            # Convert OAuth2Token to Credentials object
            credentials = Credentials(
                token=token.get("access_token"),
                refresh_token=token.get("refresh_token"),
                token_uri="https://oauth2.googleapis.com/token",
                client_id=flow.client_config["client_id"],
                client_secret=flow.client_config["client_secret"],
                scopes=SCOPES,
            )
        else:
            # Show local setup instructions immediately
            print("\nStep 2: OAuth2 Authorization")
            print("This will open your browser for Google authentication.")
            print("When prompted, log in with your GRIN Google account.")
            print("(If the browser doesn't open automatically, copy the URL that appears.)")

            # Use same port logic for consistency
            oauth_port = int(os.environ.get("GRIN_OAUTH_PORT", "58432"))

            # Run the local server flow with auto-shutdown
            credentials = flow.run_local_server(
                host="localhost",
                port=oauth_port,
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
            "token_uri": getattr(credentials, "token_uri", "https://oauth2.googleapis.com/token"),
            "client_id": credentials.client_id,
            "client_secret": credentials.client_secret,
            "scopes": credentials.scopes,
        }

        with open(creds_path, "w") as f:
            json.dump(creds_data, f, indent=2)

        # Secure the credentials file
        creds_path.chmod(0o600)

        print(f"\n✅ Credentials saved to: {creds_path}")
        print("File permissions set to 600 (owner read/write only)")

    except Exception as e:
        return False, f"OAuth2 flow failed: {e}"

    # Step 5: Test credentials
    print("\nStep 3: Testing Credentials")
    try:
        # Test credential loading synchronously
        test_auth = GRINAuth(str(secrets_path), str(creds_path))

        # Test loading credentials from file
        loaded_creds = test_auth._load_credentials()
        if not loaded_creds:
            return False, "Could not load credentials from file"

        print("✅ Credentials loaded successfully")
        if loaded_creds.token:
            print("✅ Bearer token available: [token secured]")
        else:
            print("⚠️  No bearer token found, but credentials are valid")

    except Exception as e:
        return False, f"Credential test failed: {e}"

    # Success!
    print("\n✅ Setup Complete!")
    print("=" * 16)
    print(f"Client secrets: {secrets_path}")
    print(f"User credentials: {creds_path}")
    print("OAuth2 authentication working")

    return True, "Setup completed successfully"


def setup_credentials(
    secrets_file: str | None = None, credentials_file: str | None = None, remote_auth: bool = False
) -> bool:
    """
    Interactive setup to walk user through credential configuration.

    This is the CLI interface that handles all user interaction and error display.
    The core logic is in _do_credential_setup().
    """
    print("GRIN OAuth2 Credential Setup")
    print("=" * 32)

    # Use the same file search logic as GRINAuth
    auth = GRINAuth(secrets_file, credentials_file)
    secrets_path = auth.secrets_file
    creds_path = auth.credentials_file

    # Execute the core logic
    success, message = _do_credential_setup(secrets_path, creds_path, remote_auth)

    if not success:
        # Handle different error types with appropriate CLI output
        if message.startswith("missing_secrets_file:"):
            # Extract path from error message
            missing_path = message.split(":", 1)[1]
            _display_missing_secrets_error(missing_path)
            exit(1)
        else:
            print(f"❌ {message}")
            return False

    # Handle success case for existing credentials
    if "Compatible credentials found" in message:
        print("\nStep 1: OAuth2 Client Configuration")
        print(f"Looking for client secrets at: {secrets_path}")
        print("✅ Client secrets file is valid")
        print("\nStep 2: OAuth2 Authorization")
        print(f"✅ {message}")

    return True


def _display_missing_secrets_error(secrets_path: str) -> None:
    """Display detailed instructions for missing secrets file."""
    print("\nStep 1: OAuth2 Client Configuration")
    print(
        f"""
❌ Missing OAuth 2.0 client secrets file: {secrets_path}

If you already have a client_secret.json file, """,
        end="",
    )

    print("save it in your home directory at ~/.config/grin-to-s3/client_secret.json")

    print("""
If you don't have a client_secret.json file, follow these steps to create one:

   1. Go to: https://console.cloud.google.com/apis/credentials
   2. Create a new project or select existing
   3. Click 'Create Credentials' → 'OAuth 2.0 Client IDs'
   4. Application type: 'Desktop application'
   5. Download the JSON file""")

    # Detect Docker environment and provide appropriate instructions
    if is_docker_environment():
        print("   6. Save it in your home directory as ~/.config/grin-to-s3/client_secret.json")
        print(f"   7. The file will be available inside the container at: {secrets_path}")
        print("   8. Tokens will be saved to a writable project directory for secure refresh")
    else:
        print(f"   6. Save it as '{secrets_path}'")

    print("\nThe file should look like:")
    print("   {")
    print('     "installed": {')
    print('       "client_id": "your-client-id.apps.googleusercontent.com",')
    print('       "client_secret": "your-client-secret",')
    print('       "project_id": "your-project-id",')
    print('       "auth_uri": "https://accounts.google.com/o/oauth2/auth",')
    print('       "token_uri": "https://oauth2.googleapis.com/token",')
    print('       "redirect_uris": ["http://localhost:58432"]')
    print("     }")
    print("   }")
    print("\nNOTE: The redirect URI must match the OAuth2 port (default: 58432)")
    print("If you use a custom port, update the redirect_uris accordingly.")


def main() -> None:
    """
    Command-line interface for credential setup.
    """
    remote_auth = False
    if len(sys.argv) > 1 and sys.argv[1] == "setup":
        # Check for --remote-auth flag
        if "--remote-auth" in sys.argv:
            remote_auth = True
        success = setup_credentials(remote_auth=remote_auth)
        sys.exit(0 if success else 1)
    else:
        print("GRIN OAuth2 Authentication")
        print("Usage:")
        print("  python auth.py setup                # Interactive credential setup")
        print("  python auth.py setup --remote-auth  # Force manual authorization for remote environments")
        print("\nFor programmatic use, import GRINAuth class")


def find_credential_file(filename: str) -> Path | None:
    """
    Find a credential file, checking credentials directory first, then standard locations.

    Args:
        filename: Name of the credential file to find

    Returns:
        Path to the credential file if found, None otherwise
    """
    # Check credentials directory (defaults to standard location if not set)
    creds_dir = os.environ.get("GRIN_CREDENTIALS_DIR", str(DEFAULT_CREDENTIALS_DIR))
    creds_path = Path(creds_dir) / filename
    if creds_path.exists():
        return creds_path

    return None


if __name__ == "__main__":
    main()
