#!/usr/bin/env python3
"""
GRIN Sync Module

Modular sync operations for downloading and uploading books from GRIN to storage.
"""

from .__main__ import main
from .models import (
    BookSyncResult,
    SyncStats,
    create_book_sync_result,
    create_sync_stats,
)
from .operations import (
    check_and_handle_etag_skip,
    download_book_to_filesystem,
    download_book_to_local,
    upload_book_from_staging,
)
from .pipeline import SyncPipeline
from .status import (
    export_sync_status_csv,
    get_sync_statistics,
    show_sync_status,
    validate_database_file,
)
from .utils import (
    check_encrypted_etag,
    ensure_bucket_exists,
    reset_bucket_cache,
    should_skip_download,
)

__all__ = [
    # Main entry point
    "main",
    # Core sync functions
    "check_and_handle_etag_skip",
    "download_book_to_filesystem",
    "download_book_to_local",
    "upload_book_from_staging",
    # Models and data structures
    "BookSyncResult",
    "SyncStats",
    "create_book_sync_result",
    "create_sync_stats",
    # Pipeline orchestration
    "SyncPipeline",
    # Status operations
    "export_sync_status_csv",
    "get_sync_statistics",
    "show_sync_status",
    "validate_database_file",
    # Utility functions
    "check_encrypted_etag",
    "ensure_bucket_exists",
    "reset_bucket_cache",
    "should_skip_download",
]
