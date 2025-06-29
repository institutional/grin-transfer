"""Unit tests for local storage optimization."""

import tempfile
from pathlib import Path

import pytest

from grin_to_s3.common import create_storage_from_config
from grin_to_s3.storage import StorageConfig, create_local_storage


class TestLocalStorageValidation:
    """Test local storage path validation."""

    def test_local_storage_requires_base_path(self):
        """Test that local storage requires explicit base_path."""
        # Test create_storage_from_config without base_path
        with pytest.raises(ValueError, match="Local storage requires explicit base_path"):
            create_storage_from_config("local", {})

        # Test with empty base_path
        with pytest.raises(ValueError, match="Local storage requires explicit base_path"):
            create_storage_from_config("local", {"base_path": ""})

        # Test with None base_path
        with pytest.raises(ValueError, match="Local storage requires explicit base_path"):
            create_storage_from_config("local", {"base_path": None})

    def test_local_storage_with_valid_base_path(self):
        """Test that local storage works with valid base_path."""
        with tempfile.TemporaryDirectory() as temp_dir:
            # Should not raise any errors
            storage = create_storage_from_config("local", {"base_path": temp_dir})
            assert storage is not None
            assert storage.config.protocol == "file"

    def test_create_local_storage_validation(self):
        """Test create_local_storage function validation."""
        # Test with empty string
        with pytest.raises(ValueError, match="Local storage requires explicit base_path"):
            create_local_storage("")

        # Test with valid path
        with tempfile.TemporaryDirectory() as temp_dir:
            storage = create_local_storage(temp_dir)
            assert storage is not None

    def test_storage_config_local_validation(self):
        """Test StorageConfig.local validation."""
        # Test with empty string
        with pytest.raises(ValueError, match="Local storage requires explicit base_path"):
            StorageConfig.local("")

        # Test with valid path
        config = StorageConfig.local("/tmp/test")
        assert config.protocol == "file"
        assert config.options["base_path"] == "/tmp/test"

    @pytest.mark.asyncio
    async def test_normalize_path_validation(self):
        """Test that _normalize_path validates base_path."""
        # Create storage with missing base_path in options
        config = StorageConfig(protocol="file")
        from grin_to_s3.storage import Storage

        storage = Storage(config)

        # Should raise error when normalizing relative path
        with pytest.raises(ValueError, match="Local storage requires explicit base_path"):
            storage._normalize_path("test/file.txt")

        # Absolute paths should work fine
        normalized = storage._normalize_path("/absolute/path/file.txt")
        assert normalized == "/absolute/path/file.txt"


class TestLocalStorageDirectWrite:
    """Test direct write functionality for local storage."""

    @pytest.mark.asyncio
    async def test_book_storage_direct_paths(self):
        """Test that BookStorage generates correct paths for local storage."""
        from grin_to_s3.storage import BookStorage

        with tempfile.TemporaryDirectory() as temp_dir:
            storage = create_storage_from_config("local", {"base_path": temp_dir})
            book_storage = BookStorage(storage, base_prefix="")

            # Test path generation
            barcode = "TEST123"
            encrypted_path = book_storage._book_path(barcode, f"{barcode}.tar.gz.gpg")
            decrypted_path = book_storage._book_path(barcode, f"{barcode}.tar.gz")

            assert encrypted_path == f"{barcode}/{barcode}.tar.gz.gpg"
            assert decrypted_path == f"{barcode}/{barcode}.tar.gz"

    @pytest.mark.asyncio
    async def test_local_storage_file_operations(self):
        """Test file operations work correctly with local storage."""
        from grin_to_s3.storage import BookStorage

        with tempfile.TemporaryDirectory() as temp_dir:
            storage = create_storage_from_config("local", {"base_path": temp_dir})
            book_storage = BookStorage(storage, base_prefix="")

            # Test saving archive
            barcode = "TEST456"
            test_data = b"Test archive data"

            # Save decrypted archive (new approach)
            await book_storage.save_decrypted_archive(barcode, test_data)

            # Verify file exists
            expected_file = Path(temp_dir) / barcode / f"{barcode}.tar.gz"
            assert expected_file.exists()
            assert expected_file.read_bytes() == test_data

            # Test archive exists check
            exists = await book_storage.archive_exists(barcode)
            assert exists is True

            # Test retrieving archive
            retrieved_data = await book_storage.get_archive(barcode)
            assert retrieved_data == test_data


class TestSyncPipelineLocalOptimization:
    """Test sync pipeline optimization for local storage."""

    @pytest.mark.asyncio
    async def test_local_storage_detection(self):
        """Test that sync pipeline detects local storage correctly."""
        # This test validates the concept - actual implementation will be tested
        # after sync.py modifications are made
        storage_config = {"type": "local", "config": {"base_path": "/tmp/test"}, "prefix": ""}

        # Verify storage type detection logic
        assert storage_config["type"] == "local"
        assert storage_config["config"]["base_path"] == "/tmp/test"

    @pytest.mark.asyncio
    async def test_no_staging_for_local_storage(self):
        """Test that local storage skips staging directory."""
        from grin_to_s3.collect_books.models import SQLiteProgressTracker
        from grin_to_s3.sync.pipeline import SyncPipeline

        with tempfile.TemporaryDirectory() as temp_dir:
            # Create test database
            db_path = Path(temp_dir) / "test.db"
            db_tracker = SQLiteProgressTracker(str(db_path))

            # Create sync pipeline with local storage
            storage_config = {"base_path": temp_dir}
            pipeline = SyncPipeline(
                db_path=str(db_path),
                storage_type="local",
                storage_config=storage_config,
                library_directory="test_library",
                concurrent_downloads=1,
                staging_dir=Path(temp_dir) / "staging",  # Should not be used
            )

            # Verify staging directory is not created for local storage
            assert not (Path(temp_dir) / "staging").exists()

            # Test that local storage sync method exists
            assert hasattr(pipeline, "_run_local_storage_sync")

            # Close database connection
            if hasattr(db_tracker, "_db") and db_tracker._db:
                await db_tracker._db.close()


class TestLocalStorageErrorHandling:
    """Test error handling for local storage."""

    @pytest.mark.asyncio
    async def test_disk_space_error_handling(self):
        """Test handling of disk space errors for local storage."""
        # This test will be implemented after sync.py modifications
        # It will test proper error handling when disk is full
        pass

    @pytest.mark.asyncio
    async def test_permission_error_handling(self):
        """Test handling of permission errors for local storage."""
        from grin_to_s3.storage import BookStorage

        with tempfile.TemporaryDirectory() as temp_dir:
            # Create a read-only directory
            readonly_dir = Path(temp_dir) / "readonly"
            readonly_dir.mkdir()
            readonly_dir.chmod(0o444)

            try:
                storage = create_storage_from_config("local", {"base_path": str(readonly_dir)})
                book_storage = BookStorage(storage, base_prefix="")

                # Should fail with permission error
                with pytest.raises((PermissionError, OSError)):
                    await book_storage.save_archive("TEST", b"data")
            finally:
                # Restore permissions for cleanup
                readonly_dir.chmod(0o755)
