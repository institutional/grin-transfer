#!/usr/bin/env python3
"""
Sync Catchup Operations

Functions for catching up on already-converted books from GRIN.
"""

import logging
import sys
from typing import Any

import aiosqlite

from grin_to_s3.client import GRINClient
from grin_to_s3.collect_books.models import SQLiteProgressTracker
from grin_to_s3.sync.utils import get_converted_books

logger = logging.getLogger(__name__)


async def find_catchup_books(
    db_path: str, library_directory: str, secrets_dir: str | None = None
) -> tuple[set[str], set[str], set[str]]:
    """Find books available for catchup sync.

    Args:
        db_path: Path to the SQLite database
        library_directory: GRIN library directory
        secrets_dir: Secrets directory path

    Returns:
        tuple: (converted_barcodes, all_books, catchup_candidates)
    """
    # Create a simple GRIN client to get converted books
    grin_client = GRINClient(secrets_dir=secrets_dir)

    try:
        converted_barcodes = await get_converted_books(grin_client, library_directory)
        print(f"GRIN reports {len(converted_barcodes):,} total converted books available")

    except Exception as e:
        print(f"❌ Error fetching converted books from GRIN: {e}")
        sys.exit(1)
    finally:
        if hasattr(grin_client, "session") and grin_client.session:
            await grin_client.session.close()

    # Find books in our database that are converted but not yet synced
    db_tracker = SQLiteProgressTracker(db_path)
    await db_tracker.init_db()

    # Get all books in our database
    async with aiosqlite.connect(db_path) as db:
        cursor = await db.execute("SELECT barcode FROM books")
        all_books = {row[0] for row in await cursor.fetchall()}

    print(f"Database contains {len(all_books):,} books")

    # Find books that are both in our database and converted in GRIN
    catchup_candidates = all_books.intersection(converted_barcodes)
    print(f"Found {len(catchup_candidates):,} books that are converted and in our database")

    return converted_barcodes, all_books, catchup_candidates


async def get_books_for_catchup_sync(
    db_path: str, storage_type: str, catchup_candidates: set[str], limit: int | None = None
) -> list[str]:
    """Get books that need catchup sync.

    Args:
        db_path: Path to the SQLite database
        storage_type: Storage type for filtering
        catchup_candidates: Set of candidate barcodes
        limit: Optional limit on number of books

    Returns:
        list: Books ready for catchup sync
    """
    db_tracker = SQLiteProgressTracker(db_path)

    # Filter out books already successfully synced using efficient batch query
    books_to_sync = await db_tracker.get_books_for_sync(
        storage_type=storage_type,
        limit=999999,  # Get all candidates
        status_filter=None,  # We want books that haven't been synced yet
        converted_barcodes=set(catchup_candidates),
        specific_barcodes=None,  # Not filtering to specific barcodes in catchup
    )

    print(f"Found {len(books_to_sync):,} books ready for catchup sync")

    if not books_to_sync:
        print("All converted books are already synced")
        return []

    # Apply limit if specified
    if limit:
        books_to_sync = books_to_sync[:limit]
        print(f"Limited to {len(books_to_sync):,} books for this catchup run")

    return books_to_sync


def show_catchup_dry_run(books_to_sync: list[str]) -> None:
    """Show what books would be synced in dry run mode.

    Args:
        books_to_sync: List of books that would be synced
    """
    print(f"\n📋 DRY RUN: Would sync {len(books_to_sync):,} books")
    print("=" * 50)

    if len(books_to_sync) <= 20:
        print("Books that would be synced:")
        for i, barcode in enumerate(books_to_sync, 1):
            print(f"  {i:3d}. {barcode}")
    else:
        print("First 10 books that would be synced:")
        for i, barcode in enumerate(books_to_sync[:10], 1):
            print(f"  {i:3d}. {barcode}")
        print(f"  ... and {len(books_to_sync) - 10:,} more books")

        print("\nLast 10 books that would be synced:")
        for i, barcode in enumerate(books_to_sync[-10:], len(books_to_sync) - 9):
            print(f"  {i:3d}. {barcode}")

    print("\nTo actually sync these books, run without --dry-run")


def confirm_catchup_sync(books_to_sync: list[str], auto_confirm: bool = False) -> bool:
    """Confirm with user before starting catchup sync.

    Args:
        books_to_sync: List of books to sync
        auto_confirm: Whether to skip confirmation prompt

    Returns:
        bool: True if user confirmed, False otherwise
    """
    if auto_confirm:
        return True

    response = input(f"\nDownload {len(books_to_sync):,} books? [y/N]: ").strip().lower()
    if response not in ("y", "yes"):
        print("Catchup cancelled")
        return False

    return True


async def mark_books_for_catchup_processing(db_path: str, books_to_sync: list[str], timestamp: str) -> None:
    """Mark books as being processed for catchup.

    Args:
        db_path: Path to the SQLite database
        books_to_sync: List of books to mark
        timestamp: Timestamp for session tracking
    """
    db_tracker = SQLiteProgressTracker(db_path)

    # Record that these books are being processed as part of catchup
    session_id = f"catchup_{timestamp}"
    for barcode in books_to_sync:
        # Mark processing as requested (catchup) and converted
        await db_tracker.add_status_change(
            barcode,
            "processing_request",
            "converted",
            session_id=session_id,
            metadata={"source": "catchup", "reason": "already_converted_in_grin"},
        )


async def run_catchup_validation(
    run_config: dict[str, Any], storage_type: str, storage_config: dict[str, Any]
) -> tuple[str, dict[str, Any]]:
    """Validate catchup configuration and return validated config.

    Args:
        run_config: Run configuration dictionary
        storage_type: Storage type
        storage_config: Storage configuration

    Returns:
        tuple: (storage_type, storage_config)
    """
    # Validate required configuration
    if not run_config.get("library_directory"):
        print("❌ Error: No library directory found in run config")
        print("Run collect_books.py first to create a run configuration")
        sys.exit(1)

    if not storage_type or not storage_config:
        print("❌ Error: No storage configuration found in run config")
        print("Run collect_books.py with storage configuration first")
        sys.exit(1)

    print(f"Using storage: {storage_type}")
    if storage_type == "local":
        base_path = storage_config.get("base_path")
        print(f"Local storage path: {base_path}")
    else:
        bucket_raw = storage_config.get("bucket_raw")
        if bucket_raw:
            print(f"Target bucket: {bucket_raw}")

    return storage_type, storage_config
