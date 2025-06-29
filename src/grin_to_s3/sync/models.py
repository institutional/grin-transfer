#!/usr/bin/env python3
"""
Sync Models and Data Validation

Data models, validation functions, and type definitions for sync operations.
"""

from typing import TypedDict


class SyncStats(TypedDict):
    """Statistics for sync operations."""

    processed: int
    completed: int
    failed: int
    skipped: int
    uploaded: int
    total_bytes: int


class BookSyncResult(TypedDict):
    """Result of syncing a single book."""

    barcode: str
    status: str
    skipped: bool
    encrypted_etag: str | None
    file_size: int
    total_time: float


def validate_and_parse_barcodes(barcodes_str: str) -> list[str]:
    """Validate and parse comma-separated barcode string.

    Args:
        barcodes_str: Comma-separated string of barcodes

    Returns:
        List of validated barcodes

    Raises:
        ValueError: If any barcode is invalid
    """
    if not barcodes_str.strip():
        raise ValueError("Barcodes string cannot be empty")

    # Split by comma and clean up whitespace
    barcodes = [barcode.strip() for barcode in barcodes_str.split(",")]

    # Remove empty entries
    barcodes = [barcode for barcode in barcodes if barcode]

    if not barcodes:
        raise ValueError("No valid barcodes found")

    # Basic barcode validation - check for reasonable format
    for barcode in barcodes:
        if not barcode:
            raise ValueError("Empty barcode found")
        if len(barcode) < 3 or len(barcode) > 50:
            raise ValueError(f"Barcode '{barcode}' has invalid length (must be 3-50 characters)")
        # Check for reasonable characters (alphanumeric, dash, underscore)
        if not all(c.isalnum() or c in "-_" for c in barcode):
            raise ValueError(
                f"Barcode '{barcode}' contains invalid characters (only alphanumeric, dash, underscore allowed)"
            )

    return barcodes


def create_sync_stats() -> SyncStats:
    """Create initialized sync statistics."""
    return SyncStats(
        processed=0,
        completed=0,
        failed=0,
        skipped=0,
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
