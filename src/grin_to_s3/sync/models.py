#!/usr/bin/env python3
"""
Sync Models and Data Validation

Data models, validation functions, and type definitions for sync operations.
"""

from typing import TypedDict


class FileResult(TypedDict):
    """Base result structure for file operations."""

    status: str
    file_size: int


class SyncStats(TypedDict):
    """Statistics for sync operations."""

    processed: int
    completed: int
    failed: int
    skipped: int
    uploaded: int
    total_bytes: int
    enrichment_queue_size: int


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
        completed=0,
        failed=0,
        skipped=0,
        uploaded=0,
        total_bytes=0,
        enrichment_queue_size=0,
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
