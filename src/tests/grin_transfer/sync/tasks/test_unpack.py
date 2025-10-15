#!/usr/bin/env python3
"""
Tests for sync tasks unpack module.
"""

import tempfile
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest

from grin_transfer.sync.tasks import unpack
from grin_transfer.sync.tasks.task_types import TaskAction


@pytest.mark.asyncio
async def test_main_successful_unpack(mock_pipeline, sample_decrypt_data):
    """Unpack task should complete successfully."""
    decrypt_data = sample_decrypt_data()

    with (
        patch("grin_transfer.sync.tasks.unpack.tarfile.open") as mock_tarfile,
        tempfile.TemporaryDirectory() as temp_dir,
    ):
        mock_tar = MagicMock()
        mock_tarfile.return_value.__enter__.return_value = mock_tar

        # Set expected path based on temp directory structure
        extracted_path = Path(temp_dir) / "TEST123"

        # Clear side_effect first, then set return_value (side_effect takes precedence)
        mock_pipeline.filesystem_manager.get_extracted_directory_path.side_effect = None
        mock_pipeline.filesystem_manager.get_extracted_directory_path.return_value = extracted_path

        result = await unpack.main("TEST123", decrypt_data, mock_pipeline)

        assert result.action == TaskAction.COMPLETED
        assert result.data

        # The returned path should match what the filesystem manager returned
        returned_path = result.data["unpacked_path"]
        assert returned_path == extracted_path

        # Verify directory was created and filesystem manager was used
        assert extracted_path.exists()
        assert extracted_path.is_dir()
        mock_pipeline.filesystem_manager.get_extracted_directory_path.assert_called_once_with("TEST123")
        mock_tar.extractall.assert_called_once_with(path=extracted_path)
