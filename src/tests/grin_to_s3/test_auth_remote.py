"""
Tests for remote shell OAuth2 authentication functionality.
"""

import os
from unittest.mock import MagicMock, Mock, patch

import pytest
from google.oauth2.credentials import Credentials
from google_auth_oauthlib.flow import InstalledAppFlow

from grin_to_s3.auth import detect_remote_shell, manual_authorization_flow, setup_credentials


class TestRemoteShellDetection:
    """Test remote shell environment detection."""

    def test_detect_ssh_client(self):
        """Test detection via SSH_CLIENT environment variable."""
        with patch.dict(os.environ, {"SSH_CLIENT": "192.168.1.100 54321 22"}, clear=False):
            assert detect_remote_shell() is True

    def test_detect_ssh_tty(self):
        """Test detection via SSH_TTY environment variable."""
        with patch.dict(os.environ, {"SSH_TTY": "/dev/pts/0"}, clear=False):
            assert detect_remote_shell() is True

    def test_detect_ssh_connection(self):
        """Test detection via SSH_CONNECTION environment variable."""
        with patch.dict(os.environ, {"SSH_CONNECTION": "192.168.1.100 54321 192.168.1.1 22"}, clear=False):
            assert detect_remote_shell() is True

    def test_detect_tmux_term(self):
        """Test detection via TERM environment variable for tmux."""
        with patch.dict(os.environ, {"TERM": "tmux-256color"}, clear=False):
            assert detect_remote_shell() is True

    def test_detect_screen_term(self):
        """Test detection via TERM environment variable for screen."""
        with patch.dict(os.environ, {"TERM": "screen"}, clear=False):
            assert detect_remote_shell() is True

    def test_detect_cloud_without_display(self):
        """Test detection of cloud environment without DISPLAY."""
        env_vars = {
            "AWS_EXECUTION_ENV": "AWS_ECS_FARGATE",
            "DISPLAY": ""  # Explicitly empty
        }
        with patch.dict(os.environ, env_vars, clear=False):
            with patch("sys.platform", "linux"):
                assert detect_remote_shell() is True

    def test_detect_gcp_without_display(self):
        """Test detection of Google Cloud Platform environment."""
        env_vars = {
            "GOOGLE_CLOUD_PROJECT": "my-project",
            "DISPLAY": ""
        }
        with patch.dict(os.environ, env_vars, clear=False):
            with patch("sys.platform", "linux"):
                assert detect_remote_shell() is True

    def test_detect_azure_without_display(self):
        """Test detection of Azure environment."""
        env_vars = {
            "AZURE_FUNCTIONS_ENVIRONMENT": "Production",
            "DISPLAY": ""
        }
        with patch.dict(os.environ, env_vars, clear=False):
            with patch("sys.platform", "linux"):
                assert detect_remote_shell() is True

    def test_no_remote_shell_detected(self):
        """Test when no remote shell indicators are present."""
        # Clear potentially interfering environment variables
        env_to_clear = {
            "SSH_CLIENT": None,
            "SSH_TTY": None,
            "SSH_CONNECTION": None,
            "TERM": "xterm-256color",  # Normal local terminal
            "DISPLAY": ":0",  # Local display
            "AWS_EXECUTION_ENV": None,
            "GOOGLE_CLOUD_PROJECT": None,
            "AZURE_FUNCTIONS_ENVIRONMENT": None
        }

        # Use dict comprehension to only include non-None values
        env_dict = {k: v for k, v in env_to_clear.items() if v is not None}

        with patch.dict(os.environ, env_dict, clear=True):
            assert detect_remote_shell() is False

    def test_windows_ignores_display_check(self):
        """Test that Windows systems ignore DISPLAY environment variable."""
        env_vars = {
            "DISPLAY": "",  # No display
            "AWS_EXECUTION_ENV": "AWS_ECS_FARGATE"  # Cloud indicator
        }
        with patch.dict(os.environ, env_vars, clear=False):
            with patch("sys.platform", "win32"):
                # Should not detect remote shell on Windows even without DISPLAY
                assert detect_remote_shell() is False


class TestManualAuthorizationFlow:
    """Test manual authorization code flow."""

    def test_manual_authorization_flow_success(self):
        """Test successful manual authorization flow."""
        # Mock the flow object
        mock_flow = Mock(spec=InstalledAppFlow)
        mock_flow.authorization_url.return_value = ("https://accounts.google.com/oauth2/auth?client_id=test", "state")

        # Mock credentials
        mock_credentials = Mock(spec=Credentials)
        mock_flow.credentials = mock_credentials

        # Mock user input
        with patch("builtins.input", return_value="test_auth_code"):
            with patch("builtins.print"):  # Suppress print statements
                result = manual_authorization_flow(mock_flow)

        # Verify flow configuration
        assert mock_flow.redirect_uri == "urn:ietf:wg:oauth:2.0:oob"
        mock_flow.authorization_url.assert_called_once_with(
            prompt="consent",
            access_type="offline"
        )
        mock_flow.fetch_token.assert_called_once_with(code="test_auth_code")
        assert result == mock_credentials

    def test_manual_authorization_flow_empty_input(self):
        """Test handling of empty authorization code input."""
        mock_flow = Mock(spec=InstalledAppFlow)
        mock_flow.authorization_url.return_value = ("https://accounts.google.com/oauth2/auth", "state")

        # Mock user input: first empty, then valid code
        inputs = ["", "   ", "valid_code"]
        with patch("builtins.input", side_effect=inputs) as mock_input:
            with patch("builtins.print"):  # Suppress print statements
                manual_authorization_flow(mock_flow)

        # Should have been called 3 times (2 empty, 1 valid)
        assert mock_input.call_count == 3
        mock_flow.fetch_token.assert_called_once_with(code="valid_code")

    def test_manual_authorization_flow_keyboard_interrupt(self):
        """Test handling of user cancellation."""
        mock_flow = Mock(spec=InstalledAppFlow)
        mock_flow.authorization_url.return_value = ("https://accounts.google.com/oauth2/auth", "state")

        with patch("builtins.input", side_effect=KeyboardInterrupt):
            with patch("builtins.print"):
                with pytest.raises(Exception, match="Authorization cancelled by user"):
                    manual_authorization_flow(mock_flow)

    def test_manual_authorization_flow_fetch_token_error(self):
        """Test handling of token exchange failure."""
        mock_flow = Mock(spec=InstalledAppFlow)
        mock_flow.authorization_url.return_value = ("https://accounts.google.com/oauth2/auth", "state")
        mock_flow.fetch_token.side_effect = Exception("Invalid authorization code")

        with patch("builtins.input", return_value="invalid_code"):
            with patch("builtins.print"):
                with pytest.raises(Exception, match="Failed to exchange authorization code"):
                    manual_authorization_flow(mock_flow)


class TestSetupCredentialsRemoteAuth:
    """Test setup_credentials with remote authentication."""

    @patch("grin_to_s3.auth.InstalledAppFlow.from_client_secrets_file")
    @patch("grin_to_s3.auth.detect_remote_shell")
    @patch("grin_to_s3.auth.is_docker_environment")
    @patch("grin_to_s3.auth.manual_authorization_flow")
    @patch("builtins.print")
    def test_setup_credentials_remote_detected(
        self,
        mock_print,
        mock_manual_flow,
        mock_is_docker,
        mock_detect_remote,
        mock_flow_from_file
    ):
        """Test setup_credentials with remote shell detected."""
        # Setup mocks
        mock_detect_remote.return_value = True
        mock_is_docker.return_value = False

        mock_flow = Mock()
        mock_flow_from_file.return_value = mock_flow

        mock_credentials = Mock()
        mock_credentials.token = "test_token"
        mock_credentials.refresh_token = "refresh_token"
        mock_credentials.client_id = "client_id"
        mock_credentials.client_secret = "client_secret"
        mock_credentials.scopes = ["scope1", "scope2"]
        mock_manual_flow.return_value = mock_credentials

        # Mock file operations and GRINAuth
        with patch("grin_to_s3.auth.GRINAuth") as mock_auth_class:
            # Mock the initial GRINAuth instance for file paths
            mock_auth = Mock()
            mock_auth.secrets_file.exists.return_value = True
            mock_auth.credentials_file.exists.return_value = False
            mock_auth.credentials_file.chmod = Mock()

            # Mock the test GRINAuth instance for credential validation
            mock_test_auth = Mock()
            mock_loaded_creds = Mock()
            mock_loaded_creds.token = "test_token"
            mock_test_auth._load_credentials.return_value = mock_loaded_creds

            # Return different instances for different calls
            mock_auth_class.side_effect = [mock_auth, mock_test_auth]

            with patch("builtins.open", MagicMock()):
                with patch("json.load", return_value={"installed": {"client_id": "test", "client_secret": "test", "auth_uri": "test", "token_uri": "test"}}):
                    with patch("json.dump"):
                        result = setup_credentials(remote_auth=False)

        # Verify remote flow was used
        assert result is True
        mock_manual_flow.assert_called_once_with(mock_flow)

    @patch("grin_to_s3.auth.InstalledAppFlow.from_client_secrets_file")
    @patch("grin_to_s3.auth.detect_remote_shell")
    @patch("grin_to_s3.auth.is_docker_environment")
    @patch("grin_to_s3.auth.manual_authorization_flow")
    @patch("builtins.print")
    def test_setup_credentials_remote_flag_override(
        self,
        mock_print,
        mock_manual_flow,
        mock_is_docker,
        mock_detect_remote,
        mock_flow_from_file
    ):
        """Test setup_credentials with --remote-auth flag override."""
        # Setup mocks - remote not detected, but flag forces remote auth
        mock_detect_remote.return_value = False
        mock_is_docker.return_value = False

        mock_flow = Mock()
        mock_flow_from_file.return_value = mock_flow

        mock_credentials = Mock()
        mock_credentials.token = "test_token"
        mock_credentials.refresh_token = "refresh_token"
        mock_credentials.client_id = "client_id"
        mock_credentials.client_secret = "client_secret"
        mock_credentials.scopes = ["scope1", "scope2"]
        mock_manual_flow.return_value = mock_credentials

        # Mock file operations and GRINAuth
        with patch("grin_to_s3.auth.GRINAuth") as mock_auth_class:
            # Mock the initial GRINAuth instance for file paths
            mock_auth = Mock()
            mock_auth.secrets_file.exists.return_value = True
            mock_auth.credentials_file.exists.return_value = False
            mock_auth.credentials_file.chmod = Mock()

            # Mock the test GRINAuth instance for credential validation
            mock_test_auth = Mock()
            mock_loaded_creds = Mock()
            mock_loaded_creds.token = "test_token"
            mock_test_auth._load_credentials.return_value = mock_loaded_creds

            # Return different instances for different calls
            mock_auth_class.side_effect = [mock_auth, mock_test_auth]

            with patch("builtins.open", MagicMock()):
                with patch("json.load", return_value={"installed": {"client_id": "test", "client_secret": "test", "auth_uri": "test", "token_uri": "test"}}):
                    with patch("json.dump"):
                        result = setup_credentials(remote_auth=True)

        # Verify remote flow was used despite not being detected
        assert result is True
        mock_manual_flow.assert_called_once_with(mock_flow)

    @patch("grin_to_s3.auth.InstalledAppFlow.from_client_secrets_file")
    @patch("grin_to_s3.auth.detect_remote_shell")
    @patch("grin_to_s3.auth.is_docker_environment")
    @patch("builtins.print")
    def test_setup_credentials_local_flow_when_not_remote(
        self,
        mock_print,
        mock_is_docker,
        mock_detect_remote,
        mock_flow_from_file
    ):
        """Test setup_credentials uses local flow when not remote/docker."""
        # Setup mocks
        mock_detect_remote.return_value = False
        mock_is_docker.return_value = False

        mock_flow = Mock()
        mock_credentials = Mock()
        mock_credentials.token = "test_token"
        mock_credentials.refresh_token = "refresh_token"
        mock_credentials.client_id = "client_id"
        mock_credentials.client_secret = "client_secret"
        mock_credentials.scopes = ["scope1", "scope2"]

        mock_flow.run_local_server.return_value = mock_credentials
        mock_flow_from_file.return_value = mock_flow

        # Mock file operations and GRINAuth
        with patch("grin_to_s3.auth.GRINAuth") as mock_auth_class:
            # Mock the initial GRINAuth instance for file paths
            mock_auth = Mock()
            mock_auth.secrets_file.exists.return_value = True
            mock_auth.credentials_file.exists.return_value = False
            mock_auth.credentials_file.chmod = Mock()

            # Mock the test GRINAuth instance for credential validation
            mock_test_auth = Mock()
            mock_loaded_creds = Mock()
            mock_loaded_creds.token = "test_token"
            mock_test_auth._load_credentials.return_value = mock_loaded_creds

            # Return different instances for different calls
            mock_auth_class.side_effect = [mock_auth, mock_test_auth]

            with patch("builtins.open", MagicMock()):
                with patch("json.load", return_value={"installed": {"client_id": "test", "client_secret": "test", "auth_uri": "test", "token_uri": "test"}}):
                    with patch("json.dump"):
                        with patch("os.environ.get", return_value="58432"):
                            result = setup_credentials(remote_auth=False)

        # Verify local server flow was used
        assert result is True
        mock_flow.run_local_server.assert_called_once()
