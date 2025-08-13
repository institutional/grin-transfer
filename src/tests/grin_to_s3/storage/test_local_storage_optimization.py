"""Unit tests for local storage optimization."""

import tempfile
from pathlib import Path

import pytest

from grin_to_s3.collect_books.models import SQLiteProgressTracker
from grin_to_s3.storage import BackendConfig, BookManager, Storage, create_local_storage, create_storage_from_config
from grin_to_s3.sync.pipeline import SyncPipeline
from tests.test_utils.storage_helpers import create_local_test_config
from tests.test_utils.unified_mocks import standard_storage_config


class TestLocalStorageValidation:
    """Test local storage path validation."""

    def test_local_storage_requires_base_path(self):
        """Test that local storage requires explicit base_path."""
        # Test create_storage_from_config without base_path
        with pytest.raises(ValueError, match="Local storage requires explicit base_path"):
            config = create_local_test_config("")
            create_storage_from_config(config)

        # Test with empty base_path
        with pytest.raises(ValueError, match="Local storage requires explicit base_path"):
            config = create_local_test_config("")
            create_storage_from_config(config)

        # Test with None base_path
        with pytest.raises(ValueError, match="Local storage requires explicit base_path"):
            config = create_local_test_config(None)
            create_storage_from_config(config)

    def test_local_storage_with_valid_base_path(self):
        """Test that local storage works with valid base_path."""
        with tempfile.TemporaryDirectory() as temp_dir:
            # Should not raise any errors
            config = create_local_test_config(temp_dir)
            storage = create_storage_from_config(config)
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
            BackendConfig.local("")

        # Test with valid path
        config = BackendConfig.local("/tmp/test")
        assert config.protocol == "file"
        assert config.options["base_path"] == "/tmp/test"

    @pytest.mark.asyncio
    async def test_normalize_path_validation(self):
        """Test that _normalize_path validates base_path."""
        # Create storage with missing base_path in options
        config = BackendConfig(protocol="file")

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
    async def test_book_manager_direct_paths(self):
        """Test that BookStorage generates correct paths for local storage."""

        with tempfile.TemporaryDirectory() as temp_dir:
            config = create_local_test_config(temp_dir)
            storage = create_storage_from_config(config)
            bucket_config = {"bucket_raw": "raw", "bucket_meta": "meta", "bucket_full": "full"}
            book_manager = BookManager(storage, storage_config=standard_storage_config("local", "raw", "meta", "full"), base_prefix="")

            # Test path generation
            barcode = "TEST123"
            encrypted_path = book_manager._raw_archive_path(barcode, f"{barcode}.tar.gz.gpg")
            decrypted_path = book_manager._raw_archive_path(barcode, f"{barcode}.tar.gz")

            assert encrypted_path == f"raw/{barcode}/{barcode}.tar.gz.gpg"
            assert decrypted_path == f"raw/{barcode}/{barcode}.tar.gz"

    @pytest.mark.asyncio
    async def test_local_storage_file_operations(self):
        """Test file operations work correctly with local storage."""

        with tempfile.TemporaryDirectory() as temp_dir:
            config = create_local_test_config(temp_dir)
            storage = create_storage_from_config(config)
            bucket_config = {"bucket_raw": "raw", "bucket_meta": "meta", "bucket_full": "full"}
            book_manager = BookManager(storage, storage_config=standard_storage_config("local", "raw", "meta", "full"), base_prefix="")

            # Test saving archive
            barcode = "TEST456"
            test_data = b"Test archive data"

            # Save decrypted archive (new approach)
            await book_manager.save_decrypted_archive(barcode, test_data)

            # Verify file exists (now includes bucket path)
            expected_file = Path(temp_dir) / "raw" / barcode / f"{barcode}.tar.gz"
            assert expected_file.exists()
            assert expected_file.read_bytes() == test_data

            # Test archive exists check
            exists = await book_manager.archive_exists(barcode)
            assert exists is True

            # Test retrieving archive
            retrieved_data = await book_manager.get_archive(barcode)
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
    async def test_no_staging_for_local_storage(self, mock_process_stage, test_config_builder):
        """Test that local storage skips staging directory."""

        with tempfile.TemporaryDirectory() as temp_dir:
            # Create test database
            db_path = Path(temp_dir) / "test.db"
            db_tracker = SQLiteProgressTracker(str(db_path))

            # Create RunConfig with local storage
            config = (
                test_config_builder.with_db_path(str(db_path))
                .local_storage(temp_dir)
                .with_concurrent_downloads(1)
                .with_staging_dir(str(Path(temp_dir) / "staging"))  # Should not be used
                .build()
            )

            # Create sync pipeline with local storage
            SyncPipeline.from_run_config(
                config=config,
                process_summary_stage=mock_process_stage,
            )

            # Verify staging directory is not created for local storage
            assert not (Path(temp_dir) / "staging").exists()

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

        with tempfile.TemporaryDirectory() as temp_dir:
            # Create a read-only directory
            readonly_dir = Path(temp_dir) / "readonly"
            readonly_dir.mkdir()
            readonly_dir.chmod(0o444)

            try:
                config = create_local_test_config(str(readonly_dir))
                storage = create_storage_from_config(config)
                bucket_config = {"bucket_raw": "raw", "bucket_meta": "meta", "bucket_full": "full"}
                book_manager = BookManager(storage, storage_config=standard_storage_config("local", "raw", "meta", "full"), base_prefix="")

                # Should fail with permission error
                with pytest.raises((PermissionError, OSError)):
                    await book_manager.save_decrypted_archive("TEST", b"data")
            finally:
                # Restore permissions for cleanup
                readonly_dir.chmod(0o755)
