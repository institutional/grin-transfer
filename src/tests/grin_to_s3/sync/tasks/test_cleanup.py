#!/usr/bin/env python3
"""
Tests for sync tasks cleanup module.
"""

import tempfile
from pathlib import Path

import pytest

from grin_to_s3.storage.staging import LocalDirectoryManager, StagingDirectoryManager
from grin_to_s3.sync.tasks import cleanup
from grin_to_s3.sync.tasks.task_types import TaskAction
from tests.test_utils.unified_mocks import create_test_pipeline


def test_local_directory_manager_get_paths_for_cleanup():
    """LocalDirectoryManager should return encrypted file and extracted directory paths for cleanup."""

    with tempfile.TemporaryDirectory() as temp_dir:
        base_path = Path(temp_dir)
        filesystem_manager = LocalDirectoryManager(staging_path=base_path)

        barcode = "TEST123"
        paths = filesystem_manager.get_paths_for_cleanup(barcode)

        # Should return exactly 2 paths: encrypted file and extracted directory
        assert len(paths) == 2

        expected_encrypted = filesystem_manager.get_encrypted_file_path(barcode)
        expected_extracted = filesystem_manager.get_extracted_directory_path(barcode)

        assert expected_encrypted in paths
        assert expected_extracted in paths


def test_staging_directory_manager_get_paths_for_cleanup():
    """StagingDirectoryManager should return encrypted, decrypted, and extracted paths for cleanup."""

    with tempfile.TemporaryDirectory() as temp_dir:
        base_path = Path(temp_dir)
        filesystem_manager = StagingDirectoryManager(staging_path=base_path)

        barcode = "TEST123"
        paths = filesystem_manager.get_paths_for_cleanup(barcode)

        # Should return exactly 3 paths: encrypted, decrypted, and extracted
        assert len(paths) == 3

        expected_encrypted = filesystem_manager.get_encrypted_file_path(barcode)
        expected_decrypted = filesystem_manager.get_decrypted_file_path(barcode)
        expected_extracted = filesystem_manager.get_extracted_directory_path(barcode)

        assert expected_encrypted in paths
        assert expected_decrypted in paths
        assert expected_extracted in paths


@pytest.mark.asyncio
async def test_cleanup_removes_staging_files_for_local_storage():
    """Cleanup should remove files returned by get_paths_for_cleanup for local storage."""

    with tempfile.TemporaryDirectory() as temp_dir:
        base_path = Path(temp_dir)
        staging_path = base_path / "staging"
        staging_path.mkdir()

        # Create LocalDirectoryManager
        filesystem_manager = LocalDirectoryManager(staging_path=base_path)

        # Create a mock pipeline for local storage
        pipeline = create_test_pipeline(filesystem_manager=filesystem_manager)
        pipeline.skip_staging_cleanup = False

        # Create an encrypted file in staging
        barcode = "TEST123"
        encrypted_file = filesystem_manager.get_encrypted_file_path(barcode)
        encrypted_file.parent.mkdir(parents=True, exist_ok=True)
        encrypted_file.write_bytes(b"encrypted test content")

        # Create an extracted directory
        extracted_dir = filesystem_manager.get_extracted_directory_path(barcode)
        extracted_dir.mkdir(parents=True, exist_ok=True)
        (extracted_dir / "test.txt").write_text("extracted content")

        # Verify files exist before cleanup
        assert encrypted_file.exists()
        assert extracted_dir.exists()

        # Run cleanup
        result = await cleanup.main(barcode, pipeline, {})

        # Verify cleanup completed successfully
        assert result.action == TaskAction.COMPLETED

        # Verify staging file was removed
        assert not encrypted_file.exists()

        # Verify extracted directory was removed
        assert not extracted_dir.exists()


@pytest.mark.asyncio
async def test_cleanup_removes_staging_files_for_staging_storage():
    """Cleanup should remove files returned by get_paths_for_cleanup for staging storage."""

    with tempfile.TemporaryDirectory() as temp_dir:
        base_path = Path(temp_dir)

        # Create StagingDirectoryManager
        filesystem_manager = StagingDirectoryManager(staging_path=base_path)

        # Create a mock pipeline
        pipeline = create_test_pipeline(filesystem_manager=filesystem_manager)
        pipeline.skip_staging_cleanup = False

        barcode = "TEST123"

        # Create encrypted, decrypted, and extracted files/directories
        encrypted_file = filesystem_manager.get_encrypted_file_path(barcode)
        encrypted_file.parent.mkdir(parents=True, exist_ok=True)
        encrypted_file.write_bytes(b"encrypted content")

        decrypted_file = filesystem_manager.get_decrypted_file_path(barcode)
        decrypted_file.write_bytes(b"decrypted content")

        extracted_dir = filesystem_manager.get_extracted_directory_path(barcode)
        extracted_dir.mkdir(parents=True, exist_ok=True)
        (extracted_dir / "test.txt").write_text("extracted content")

        # Verify files exist before cleanup
        assert encrypted_file.exists()
        assert decrypted_file.exists()
        assert extracted_dir.exists()

        # Run cleanup
        result = await cleanup.main(barcode, pipeline, {})

        # Verify cleanup completed successfully
        assert result.action == TaskAction.COMPLETED

        # Verify all files were removed
        assert not encrypted_file.exists()
        assert not decrypted_file.exists()
        assert not extracted_dir.exists()


@pytest.mark.asyncio
async def test_cleanup_respects_skip_staging_cleanup_flag():
    """Cleanup should respect skip_staging_cleanup flag."""

    with tempfile.TemporaryDirectory() as temp_dir:
        base_path = Path(temp_dir)

        # Create StagingDirectoryManager (for block storage)
        filesystem_manager = StagingDirectoryManager(staging_path=base_path)

        # Create a mock pipeline with cleanup disabled
        pipeline = create_test_pipeline(filesystem_manager=filesystem_manager)
        pipeline.skip_staging_cleanup = True

        barcode = "TEST123"

        # Create files that would normally be cleaned up
        encrypted_file = filesystem_manager.get_encrypted_file_path(barcode)
        encrypted_file.parent.mkdir(parents=True, exist_ok=True)
        encrypted_file.write_bytes(b"test content")

        # Verify file exists before cleanup
        assert encrypted_file.exists()

        # Run cleanup
        result = await cleanup.main(barcode, pipeline, {})

        # Verify cleanup completed but files remain (due to skip flag)
        assert result.action == TaskAction.COMPLETED
        assert encrypted_file.exists()  # Should still exist due to skip flag


@pytest.mark.asyncio
async def test_cleanup_handles_missing_files_gracefully():
    """Cleanup should handle missing files gracefully."""

    with tempfile.TemporaryDirectory() as temp_dir:
        base_path = Path(temp_dir)
        filesystem_manager = LocalDirectoryManager(staging_path=base_path)

        # Create a mock pipeline
        pipeline = create_test_pipeline(filesystem_manager=filesystem_manager)
        pipeline.skip_staging_cleanup = False

        barcode = "NONEXISTENT123"

        # Don't create any files - they should not exist
        paths = filesystem_manager.get_paths_for_cleanup(barcode)
        for path in paths:
            assert not path.exists()

        # Run cleanup - should complete without error even if files don't exist
        result = await cleanup.main(barcode, pipeline, {})

        # Verify cleanup completed successfully
        assert result.action == TaskAction.COMPLETED
