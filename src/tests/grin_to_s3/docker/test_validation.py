"""Tests for Docker validation module."""

import os
import tempfile
from pathlib import Path
from unittest.mock import patch

import pytest

from grin_to_s3.docker.validation import (
    is_docker_environment,
    process_local_storage_path,
    translate_docker_data_path_for_local_storage,
    validate_docker_local_storage_path,
)


class TestIsDockerEnvironment:
    """Test Docker environment detection."""

    def test_docker_environment_with_file(self):
        """Test detection when /.dockerenv exists."""
        with patch("os.path.exists", return_value=True):
            assert is_docker_environment() is True

    def test_non_docker_environment(self):
        """Test detection when /.dockerenv doesn't exist."""
        with patch("os.path.exists", return_value=False):
            with patch.dict(os.environ, {}, clear=True):
                assert is_docker_environment() is False


class TestTranslateDockerDataPath:
    """Test Docker data path translation."""

    def test_translate_docker_data_path(self):
        """Test translation of docker-data paths."""
        assert translate_docker_data_path_for_local_storage("docker-data/test") == "/app/docker-data/test"
        assert translate_docker_data_path_for_local_storage("docker-data/foo/bar") == "/app/docker-data/foo/bar"

    def test_translate_docker_data_subdirectories(self):
        """Test translation of specific docker-data subdirectories."""
        assert translate_docker_data_path_for_local_storage("docker-data/data/test") == "/app/data/test"
        assert translate_docker_data_path_for_local_storage("docker-data/output/test") == "/app/output/test"
        assert translate_docker_data_path_for_local_storage("docker-data/logs/test") == "/app/logs/test"
        assert translate_docker_data_path_for_local_storage("docker-data/staging/test") == "/app/staging/test"

    def test_non_docker_data_paths(self):
        """Test that non-docker-data paths are not translated."""
        assert translate_docker_data_path_for_local_storage("/tmp/test") == "/tmp/test"
        assert translate_docker_data_path_for_local_storage("~/test") == "~/test"
        assert translate_docker_data_path_for_local_storage("relative/path") == "relative/path"
        assert translate_docker_data_path_for_local_storage("/docker-data/test") == "/docker-data/test"
        assert translate_docker_data_path_for_local_storage("~/docker-data/test") == "~/docker-data/test"


class TestValidateDockerLocalStoragePath:
    """Test Docker local storage path validation."""

    @patch("pathlib.Path.mkdir")
    @patch("pathlib.Path.unlink")
    def test_valid_docker_data_path(self, mock_unlink, mock_mkdir):
        """Test validation of valid docker-data path."""
        # Mock the write_text method
        with patch("pathlib.Path.write_text"):
            # Should not raise
            validate_docker_local_storage_path(Path("/app/docker-data/test"))
            mock_mkdir.assert_called_once_with(parents=True, exist_ok=True)

    @patch("pathlib.Path.mkdir")
    @patch("pathlib.Path.unlink")
    def test_valid_app_paths(self, mock_unlink, mock_mkdir):
        """Test validation of various valid /app paths."""
        with patch("pathlib.Path.write_text"):
            # Test all mounted prefixes
            for path in [
                "/app/data/test",
                "/app/output/test",
                "/app/logs/test",
                "/app/staging/test",
                "/app/docker-data/test",
                "/app/custom/test",
            ]:
                mock_mkdir.reset_mock()
                validate_docker_local_storage_path(Path(path))
                mock_mkdir.assert_called_with(parents=True, exist_ok=True)

    def test_invalid_path_raises(self):
        """Test that invalid paths raise ValueError."""
        with pytest.raises(ValueError) as exc_info:
            validate_docker_local_storage_path(Path("/home/user/data"))

        assert "Storage path not mounted" in str(exc_info.value)
        assert "/home/user/data" in str(exc_info.value)

    def test_invalid_root_path_raises(self):
        """Test that root paths raise ValueError."""
        with pytest.raises(ValueError) as exc_info:
            validate_docker_local_storage_path(Path("/etc/config"))

        assert "Storage path not mounted" in str(exc_info.value)
        assert "/etc/config" in str(exc_info.value)

    @patch("pathlib.Path.mkdir", side_effect=OSError("Permission denied"))
    def test_mkdir_failure_raises(self, mock_mkdir):
        """Test that mkdir failures are caught and re-raised with helpful message."""
        with pytest.raises(ValueError) as exc_info:
            validate_docker_local_storage_path(Path("/app/docker-data/test"))

        assert "Storage path not writable" in str(exc_info.value)
        assert "/app/docker-data/test" in str(exc_info.value)


class TestProcessLocalStoragePath:
    """Test the complete path processing function."""

    @patch("grin_to_s3.docker.validation.is_docker_environment", return_value=False)
    def test_process_path_non_docker(self, mock_is_docker):
        """Test path processing outside Docker."""
        with tempfile.TemporaryDirectory() as temp_dir:
            test_path = Path(temp_dir) / "test"
            test_path.mkdir()

            result = process_local_storage_path(str(test_path))

            assert result == test_path.resolve()
            mock_is_docker.assert_called_once()

    @patch("grin_to_s3.docker.validation.is_docker_environment", return_value=True)
    @patch("pathlib.Path.mkdir")
    @patch("pathlib.Path.unlink")
    @patch("pathlib.Path.write_text")
    def test_process_docker_data_path_in_docker(self, mock_write, mock_unlink, mock_mkdir, mock_is_docker):
        """Test processing docker-data path inside Docker."""
        # Test a simple docker-data path
        result = process_local_storage_path("docker-data/test")

        # The path should be translated and resolved
        assert str(result).startswith("/app/docker-data/test")
        mock_is_docker.assert_called_once()

    @patch("grin_to_s3.docker.validation.is_docker_environment", return_value=True)
    def test_process_invalid_path_in_docker(self, mock_is_docker):
        """Test processing invalid path inside Docker."""
        with pytest.raises(ValueError) as exc_info:
            process_local_storage_path("/invalid/path")

        assert "Storage path not mounted" in str(exc_info.value)
        assert "/invalid/path" in str(exc_info.value)
        mock_is_docker.assert_called_once()

    @patch("grin_to_s3.docker.validation.is_docker_environment", return_value=True)
    @patch("pathlib.Path.mkdir")
    @patch("pathlib.Path.unlink")
    @patch("pathlib.Path.write_text")
    def test_process_docker_data_subdirectory(self, mock_write, mock_unlink, mock_mkdir, mock_is_docker):
        """Test processing docker-data subdirectory paths."""
        # Test path that should be mapped to /app/data/
        result = process_local_storage_path("docker-data/data/mydata")

        assert str(result).startswith("/app/data/mydata")
        mock_is_docker.assert_called_once()

    @patch("grin_to_s3.docker.validation.is_docker_environment", return_value=False)
    def test_process_relative_path_non_docker(self, mock_is_docker):
        """Test relative path processing outside Docker."""
        with tempfile.TemporaryDirectory() as temp_dir:
            # Change to temp directory
            import os

            original_cwd = os.getcwd()
            try:
                os.chdir(temp_dir)

                # Create the relative path
                Path("relative/path").mkdir(parents=True)

                result = process_local_storage_path("relative/path")

                # Compare normalized paths to handle symlinks
                expected = Path(temp_dir).resolve() / "relative" / "path"
                assert result.resolve() == expected.resolve()
                mock_is_docker.assert_called_once()
            finally:
                os.chdir(original_cwd)

    @patch("grin_to_s3.docker.validation.is_docker_environment", return_value=False)
    def test_process_tilde_expansion_non_docker(self, mock_is_docker):
        """Test tilde expansion outside Docker."""
        # Use actual home directory expansion
        result = process_local_storage_path("~/test")

        # Result should start with home directory
        assert str(result).startswith(str(Path.home()))
        assert str(result).endswith("test")
        mock_is_docker.assert_called_once()
