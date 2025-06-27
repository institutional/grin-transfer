#!/usr/bin/env python3
"""
GRIN Sync Module

Modular sync operations for downloading and uploading books from GRIN to storage.
"""

from .catchup import (
    confirm_catchup_sync,
    find_catchup_books,
    get_books_for_catchup_sync,
    mark_books_for_catchup_processing,
    run_catchup_validation,
    show_catchup_dry_run,
)
from .core import (
    check_and_handle_etag_skip,
    download_book_to_staging,
    sync_book_to_local_storage,
    upload_book_from_staging,
)
from .models import (
    BookSyncResult,
    SyncStats,
    create_book_sync_result,
    create_sync_stats,
    validate_and_parse_barcodes,
)
from .pipeline import SyncPipeline
from .status import (
    export_sync_status_csv,
    get_sync_statistics,
    show_sync_status,
    validate_database_file,
)
from .utils import (
    check_google_etag,
    ensure_bucket_exists,
    get_converted_books,
    reset_bucket_cache,
    should_skip_download,
)
from .__main__ import main

__all__ = [
    # Catchup operations
    "confirm_catchup_sync",
    "find_catchup_books",
    "get_books_for_catchup_sync",
    "mark_books_for_catchup_processing",
    "run_catchup_validation",
    "show_catchup_dry_run",
    # Core sync functions
    "check_and_handle_etag_skip",
    "download_book_to_staging",
    "sync_book_to_local_storage", 
    "upload_book_from_staging",
    # Models and data structures
    "BookSyncResult",
    "SyncStats", 
    "create_book_sync_result",
    "create_sync_stats",
    "validate_and_parse_barcodes",
    # Pipeline orchestration
    "SyncPipeline",
    # Status operations
    "export_sync_status_csv",
    "get_sync_statistics",
    "show_sync_status",
    "validate_database_file",
    # Utility functions
    "check_google_etag",
    "ensure_bucket_exists", 
    "get_converted_books",
    "reset_bucket_cache",
    "should_skip_download",
    # Main entry point
    "main",
]