"""
Interactive credential setup for GRIN OAuth2 authentication
"""

import json
import os
import sys

from google.oauth2.credentials import Credentials
from google_auth_oauthlib.flow import InstalledAppFlow

from ..docker import is_docker_environment
from .grin_auth import SCOPES, GRINAuth


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
        print(
            f"""
❌ Missing OAuth 2.0 client secrets file: {secrets_path}

If you already have a client_secret.json file, """, end="")

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

        exit(1)

    print(f"Looking for client secrets at: {secrets_path}")

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

    except FileNotFoundError:
        print(f"❌ Client secrets file not found: {secrets_path}")
        print("\nTo fix this:")
        print("1. Create the directory: mkdir -p ~/.config/grin-to-s3/")
        print("2. Copy the template: cp examples/auth/client_secret.json ~/.config/grin-to-s3/client_secret.json")
        print("3. Edit the file with your OAuth2 credentials from Google Cloud Console")
        return False
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
                print(f"✅ Compatible credentials found: {creds_path}")
                return True
            else:
                print("⚠️  Existing credentials have different scopes")
                print(f"   Existing: {sorted(existing_scopes)}")
                print(f"   Required: {sorted(current_scopes)}")
                print("   Creating new credentials with correct scopes...")

        except (json.JSONDecodeError, KeyError, FileNotFoundError):
            print(f"⚠️  Invalid credentials file found: {creds_path}")
            print("   Creating new credentials...")

    print("Starting OAuth2 flow...")

    try:
        # Use InstalledAppFlow which handles the local server properly
        flow = InstalledAppFlow.from_client_secrets_file(str(secrets_path), scopes=SCOPES)

        if is_docker_environment():
            print("Running in Docker container - using port forwarding flow")
            print("\n" + "="*60)
            print("Docker setup")
            print("="*60)

            # Get OAuth2 port from environment
            oauth_port = int(os.environ.get("GRIN_OAUTH_PORT", "58432"))

            print("1. Cut and paste the URL presented below into a browser window")
            print("2. When prompted, log in with your GRIN Google account")
            print("3. You should get a success message in the browser after login")
            print("="*60)

            # Generate and display the authorization URL
            flow.redirect_uri = f"http://localhost:{oauth_port}"
            auth_url, _ = flow.authorization_url(
                prompt="consent",
                access_type="offline"
            )

            print("\nPlease visit this URL to authorize the GRIN pipeline:\n")
            print(f"{auth_url}\n")
            print("Waiting for authorization to complete...")

            # Start local server with Docker-compatible settings
            # Use fetch_token to handle the callback manually
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
                        self.wfile.write(b"<html><body><h1>GRIN pipeline authorization was successful!</h1><p>You can close this browser tab and continue working with the tool from the command line.</p></body></html>")
                    elif "error" in query_params:
                        server_error = query_params["error"][0]
                        self.send_response(400)
                        self.send_header("Content-type", "text/html")
                        self.end_headers()
                        self.wfile.write(b"<html><body><h1>GRIN pipeline authorization failed!</h1><p>Error: " + server_error.encode() + b"</p></body></html>")
                    else:
                        self.send_response(400)
                        self.send_header("Content-type", "text/html")
                        self.end_headers()
                        self.wfile.write(b"<html><body><h1>Invalid request</h1></body></html>")

                def log_message(self, format, *args):
                    # Suppress log messages
                    pass

            # Start the HTTP server
            server = HTTPServer(("0.0.0.0", oauth_port), CallbackHandler)
            server.timeout = 600  # 10 minute timeout

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
                success_message="GRIN pipeline authorization was successful! You can close this browser tab and continue working with the tool from the command line.",
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

        print(f"✅ Credentials saved to: {creds_path}")
        print("File permissions set to 600 (owner read/write only)")

    except Exception as e:
        print(f"❌ OAuth2 flow failed: {e}")
        return False

    # Step 4: Test credentials
    print("\nStep 3: Testing Credentials")

    try:
        # Test credential loading synchronously
        test_auth = GRINAuth(str(secrets_path), str(creds_path))

        # Test loading credentials from file
        loaded_creds = test_auth._load_credentials()
        if loaded_creds:
            print("✅ Credentials loaded successfully")

            # Test bearer token access
            if loaded_creds.token:
                print(f"✅ Bearer token available: {loaded_creds.token[:30]}...")
            else:
                print("⚠️  No bearer token found, but credentials are valid")
        else:
            print("❌ Could not load credentials from file")
            return False

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
