#!/usr/bin/env python3
"""
Sync Models and Data Validation

Data models, validation functions, and type definitions for sync operations.
"""

from pathlib import Path
from typing import Literal, TypedDict

from grin_to_s3.common import Barcode


class FileTransferResult(TypedDict):
    barcode: Barcode
    completed: bool
    success: bool
    http_status_code: int


class HeadRequestResult(FileTransferResult):
    etag: str | None
    file_size_bytes: int | None


class DownloadRequestResult(FileTransferResult):
    pass


class ETagMatchResult(TypedDict):
    """Whether an Etag matched stored etag metadata, and why the determination was made"""

    matched: bool
    reason: Literal["etag_match", "etag_mismatch", "no_archive", "no_etag"]


class DownloadResult(FileTransferResult):
    download_success: bool
    conversion_requested: bool
    already_in_process: bool
    marked_unavailable: bool
    conversion_limit_reached: bool
    encrypted_etag: str
    etag_matched: bool
    skipped: bool


class CompletedDownload(FileTransferResult):
    downloaded_file_path: Path
    encrypted_etag: str
    file_size_bytes: int


class UploadResult(FileTransferResult):
    pass


class FileResult(TypedDict):
    """Base result structure for file operations."""

    status: str
    file_size: int


class SyncStats(TypedDict):
    """Statistics for sync operations."""

    processed: int
    synced: int  # Actually downloaded and uploaded to storage
    failed: int
    skipped: int
    skipped_etag_match: int
    skipped_conversion_limit: int
    conversion_requested: int
    marked_unavailable: int
    uploaded: int
    total_bytes: int


class BookSyncResult(FileResult):
    """Result of syncing a single book."""

    barcode: str
    skipped: bool
    encrypted_etag: str | None
    total_time: float


def create_sync_stats() -> SyncStats:
    """Create initialized sync statistics."""
    return SyncStats(
        processed=0,
        synced=0,
        failed=0,
        skipped=0,
        skipped_etag_match=0,
        skipped_conversion_limit=0,
        conversion_requested=0,
        marked_unavailable=0,
        uploaded=0,
        total_bytes=0,
    )


def create_book_sync_result(
    barcode: str,
    status: str = "completed",
    skipped: bool = False,
    encrypted_etag: str | None = None,
    file_size: int = 0,
    total_time: float = 0.0,
) -> BookSyncResult:
    """Create a book sync result."""
    return BookSyncResult(
        barcode=barcode,
        status=status,
        skipped=skipped,
        encrypted_etag=encrypted_etag,
        file_size=file_size,
        total_time=total_time,
    )
