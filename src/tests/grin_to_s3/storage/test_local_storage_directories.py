"""Tests for create_local_storage_directories function."""

import tempfile
from pathlib import Path
from unittest.mock import patch

import pytest

from grin_to_s3.storage.factories import LOCAL_STORAGE_DEFAULTS, create_local_storage_directories


class TestCreateLocalStorageDirectories:
    """Test local storage directory creation."""

    @pytest.mark.asyncio
    async def test_create_directories_non_docker(self):
        """Test directory creation outside Docker environment."""
        with tempfile.TemporaryDirectory() as temp_dir:
            storage_config = {"base_path": temp_dir}

            with patch("grin_to_s3.docker.validation.is_docker_environment", return_value=False):
                await create_local_storage_directories(storage_config)

            # Check directories were created
            base_path = Path(temp_dir)
            assert (base_path / "raw").exists()
            assert (base_path / "meta").exists()
            assert (base_path / "full").exists()

            # Check config was updated with resolved path
            # Use resolve() to handle symlinks on macOS
            assert Path(storage_config["base_path"]).resolve() == Path(temp_dir).resolve()

    @pytest.mark.asyncio
    async def test_create_directories_docker_valid_path(self):
        """Test directory creation in Docker with valid path."""
        with patch("grin_to_s3.docker.validation.is_docker_environment", return_value=True):
            # Mock all the path operations
            with patch("pathlib.Path.mkdir"):
                with patch("pathlib.Path.write_text"):
                    with patch("pathlib.Path.unlink"):
                        # Mock the path processing to return a valid docker path
                        mock_path = Path("/app/docker-data/test")
                        with patch("grin_to_s3.docker.validation.translate_docker_data_path_for_local_storage",
                                 return_value="/app/docker-data/test"):
                            with patch("pathlib.Path.resolve", return_value=mock_path):
                                with patch("pathlib.Path.expanduser", return_value=mock_path):
                                    storage_config = {"base_path": "docker-data/test"}

                                    await create_local_storage_directories(storage_config)

                                    # Check config was updated with processed path
                                    assert storage_config["base_path"] == str(mock_path)

    @pytest.mark.asyncio
    async def test_create_directories_missing_base_path(self):
        """Test error when base_path is missing."""
        storage_config = {}

        with pytest.raises(ValueError) as exc_info:
            await create_local_storage_directories(storage_config)

        assert "Local storage requires base_path" in str(exc_info.value)

    @pytest.mark.asyncio
    async def test_create_directories_docker_invalid_path(self):
        """Test error when Docker path is invalid."""
        with patch("grin_to_s3.docker.validation.is_docker_environment", return_value=True):
            with patch("grin_to_s3.docker.validation.process_local_storage_path", side_effect=ValueError("Invalid Docker path")):
                storage_config = {"base_path": "/invalid/path"}

                with pytest.raises(ValueError) as exc_info:
                    await create_local_storage_directories(storage_config)

                assert "Storage path not mounted" in str(exc_info.value)

    @pytest.mark.asyncio
    async def test_create_directories_with_tilde(self):
        """Test directory creation with tilde expansion."""
        with tempfile.TemporaryDirectory() as temp_dir:
            # Mock home directory
            with patch("pathlib.Path.home", return_value=Path(temp_dir)):
                with patch("pathlib.Path.expanduser") as mock_expanduser:
                    # Make expanduser return a path in our temp directory
                    expanded_path = Path(temp_dir) / "test"
                    mock_expanduser.return_value = expanded_path

                    storage_config = {"base_path": "~/test"}

                    with patch("grin_to_s3.docker.validation.is_docker_environment", return_value=False):
                        await create_local_storage_directories(storage_config)

                    # Check directories were created with expanded path
                    assert (expanded_path / "raw").exists()
                    assert (expanded_path / "meta").exists()
                    assert (expanded_path / "full").exists()

                    # Check config was updated with resolved path
                    assert storage_config["base_path"] == str(expanded_path.resolve())

    @pytest.mark.asyncio
    async def test_create_directories_uses_default_names(self):
        """Test that default bucket names are used from LOCAL_STORAGE_DEFAULTS."""
        with tempfile.TemporaryDirectory() as temp_dir:
            storage_config = {"base_path": temp_dir}

            with patch("grin_to_s3.docker.validation.is_docker_environment", return_value=False):
                await create_local_storage_directories(storage_config)

            # Check directories match the defaults
            base_path = Path(temp_dir)
            assert (base_path / LOCAL_STORAGE_DEFAULTS["bucket_raw"]).exists()
            assert (base_path / LOCAL_STORAGE_DEFAULTS["bucket_meta"]).exists()
            assert (base_path / LOCAL_STORAGE_DEFAULTS["bucket_full"]).exists()
