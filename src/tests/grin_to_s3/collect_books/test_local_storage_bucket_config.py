"""
Test local storage bucket configuration extraction (Issue #139).

The bug: collector.py extracts bucket names from storage_config directly,
but they are stored under storage_config["config"] in the nested structure.
"""

from unittest.mock import Mock, patch

from grin_to_s3.collect_books.collector import BookCollector


class TestLocalStorageBucketConfig:
    """Test that local storage bucket configuration is extracted correctly."""

    def test_bucket_config_extraction_from_nested_structure(self):
        """Test that bucket names are extracted from storage_config['config'], not storage_config directly."""

        # This is the storage config structure created by __main__.py
        storage_config = {
            "type": "local",
            "config": {
                "base_path": "/tmp/test_storage",
                "bucket_raw": "raw",
                "bucket_meta": "meta",
                "bucket_full": "full",
            },
            "prefix": "",
        }

        with patch("grin_to_s3.collect_books.collector.create_storage_from_config") as mock_create_storage:
            mock_storage = Mock()
            mock_create_storage.return_value = mock_storage

            # This should not raise "Bucket name cannot be empty" error
            collector = BookCollector(
                directory="Harvard", process_summary_stage=Mock(), storage_config=storage_config, test_mode=True
            )

            # Verify that BookManager was created with correct bucket config
            assert collector.book_manager is not None

            # Verify the correct bucket names were extracted
            assert collector.book_manager.bucket_raw == "raw"
            assert collector.book_manager.bucket_meta == "meta"
            assert collector.book_manager.bucket_full == "full"

    def test_bucket_config_extraction_fails_with_direct_lookup(self):
        """Test that the current buggy implementation fails with nested storage config."""

        # This is what the collector currently tries to do (incorrectly)
        storage_config = {
            "type": "local",
            "config": {  # Bucket names are nested here
                "base_path": "/tmp/test_storage",
                "bucket_raw": "raw",
                "bucket_meta": "meta",
                "bucket_full": "full",
            },
            "prefix": "",
        }

        # Simulate the current buggy extraction logic
        buggy_bucket_config = {
            "bucket_raw": storage_config.get("bucket_raw", ""),  # Gets "" (empty)
            "bucket_meta": storage_config.get("bucket_meta", ""),  # Gets "" (empty)
            "bucket_full": storage_config.get("bucket_full", ""),  # Gets "" (empty)
        }

        # Verify that the buggy extraction gets empty strings
        assert buggy_bucket_config["bucket_raw"] == ""
        assert buggy_bucket_config["bucket_meta"] == ""
        assert buggy_bucket_config["bucket_full"] == ""

        # This is what causes the "Bucket name cannot be empty" error

    def test_correct_bucket_config_extraction(self):
        """Test the correct way to extract bucket config from nested structure."""

        storage_config = {
            "type": "local",
            "config": {
                "base_path": "/tmp/test_storage",
                "bucket_raw": "raw",
                "bucket_meta": "meta",
                "bucket_full": "full",
            },
            "prefix": "",
        }

        # This is the correct extraction logic (the fix)
        config_dict = storage_config.get("config", {})
        correct_bucket_config = {
            "bucket_raw": config_dict.get("bucket_raw", ""),
            "bucket_meta": config_dict.get("bucket_meta", ""),
            "bucket_full": config_dict.get("bucket_full", ""),
        }

        # Verify that the correct extraction gets the right values
        assert correct_bucket_config["bucket_raw"] == "raw"
        assert correct_bucket_config["bucket_meta"] == "meta"
        assert correct_bucket_config["bucket_full"] == "full"

    def test_local_storage_collector_initialization_with_real_config(self):
        """Integration test with realistic local storage configuration."""

        # Simulate the config structure from a real CLI run
        storage_config = {
            "type": "local",
            "config": {
                "base_path": "/tmp/grin_test",
                "bucket_raw": "raw_archives",
                "bucket_meta": "metadata_files",
                "bucket_full": "fulltext_data",
            },
            "prefix": "harvard_2024",
        }

        with patch("grin_to_s3.collect_books.collector.create_storage_from_config") as mock_create_storage:
            mock_storage = Mock()
            mock_create_storage.return_value = mock_storage

            # Should not raise any exceptions
            collector = BookCollector(
                directory="Harvard", process_summary_stage=Mock(), storage_config=storage_config, test_mode=True
            )

            # Verify bucket configuration was set correctly
            assert collector.book_manager.bucket_raw == "raw_archives"
            assert collector.book_manager.bucket_meta == "metadata_files"
            assert collector.book_manager.bucket_full == "fulltext_data"

            # Verify prefix was set correctly
            assert collector.book_manager.base_prefix == "harvard_2024"
