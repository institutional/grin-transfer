#!/usr/bin/env python3
"""
Shared test fixtures for sync tasks testing.
"""

import tempfile
from pathlib import Path
from unittest.mock import AsyncMock, MagicMock

import pytest

from grin_to_s3.client import GRINClient
from grin_to_s3.storage.staging import DirectoryManager


@pytest.fixture
def mock_pipeline():
    """Mock SyncPipeline for task testing with common attributes."""
    with tempfile.TemporaryDirectory() as temp_dir:
        pipeline = MagicMock()

        # Common pipeline attributes
        pipeline.library_directory = "TestLib"
        pipeline.secrets_dir = "/path/to/secrets"
        pipeline.uses_block_storage = True
        pipeline.skip_staging_cleanup = False
        pipeline.start_time = 1234567890
        pipeline.force = False  # Default to False for normal skip behavior

        # Download/upload settings
        pipeline.download_timeout = 300
        pipeline.download_retries = 3

        # Mock grin client with auth
        pipeline.grin_client = MagicMock(spec=GRINClient)
        pipeline.grin_client.auth = MagicMock()
        pipeline.grin_client.auth.make_authenticated_request = AsyncMock()

        # Mock filesystem manager with temp directory paths
        pipeline.filesystem_manager = MagicMock(spec=DirectoryManager)
        pipeline.filesystem_manager.staging_path = Path(temp_dir)

        # Mock the path methods to return paths within temp directory
        def get_decrypted_path(barcode):
            return Path(temp_dir) / f"{barcode}.tar.gz"
        def get_extracted_path(barcode):
            return Path(temp_dir) / barcode
        pipeline.filesystem_manager.get_decrypted_file_path.side_effect = get_decrypted_path
        pipeline.filesystem_manager.get_extracted_directory_path.side_effect = get_extracted_path

        # Mock storage and config
        pipeline.storage = MagicMock()
        pipeline.storage.write_file = AsyncMock()
        pipeline.config = MagicMock()
        pipeline.config.storage_config = {
            "config": {
                "bucket_raw": "test-raw",
                "bucket_full": "test-full",
                "bucket_meta": "test-meta"
            }
        }

        # Mock database tracker
        pipeline.db_tracker = MagicMock()
        pipeline.db_tracker.update_book_marc_metadata = AsyncMock()
        pipeline.db_tracker.close = AsyncMock()
        pipeline.db_tracker.get_all_books_csv_data = AsyncMock()

        # Mock stats and output functions
        pipeline._print_final_stats_and_outputs = AsyncMock(return_value={})

        yield pipeline
