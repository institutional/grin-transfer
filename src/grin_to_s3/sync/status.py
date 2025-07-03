#!/usr/bin/env python3
"""
Sync Status Operations

Functions for checking and reporting sync status and statistics.
"""

import logging
import sqlite3
import sys
from pathlib import Path
from typing import Any

from grin_to_s3.collect_books.models import SQLiteProgressTracker

from ..database import connect_async, connect_sync

logger = logging.getLogger(__name__)


async def show_sync_status(db_path: str, storage_type: str | None = None) -> None:
    """Show sync status for books in the database.

    Args:
        db_path: Path to the SQLite database
        storage_type: Optional storage type filter
    """
    # Validate database file
    db_file = Path(db_path)
    if not db_file.exists():
        print(f"❌ Error: Database file does not exist: {db_path}")
        return

    print("Sync Status Report")
    print(f"Database: {db_path}")
    if storage_type:
        print(f"Storage Type: {storage_type}")
    print("=" * 50)

    tracker = SQLiteProgressTracker(db_path)

    try:
        # Get overall book counts
        total_books = await tracker.get_book_count()
        enriched_books = await tracker.get_enriched_book_count()
        converted_books = await tracker.get_converted_books_count()

        print("Overall Book Counts:")
        print(f"  Total books in database: {total_books:,}")
        print(f"  Books with enrichment data: {enriched_books:,}")
        print(f"  Books in converted state: {converted_books:,}")
        print()

        # Get sync statistics
        sync_stats = await tracker.get_sync_stats(storage_type)

        print("Sync Status:")
        print(f"  Total converted books: {sync_stats['total_converted']:,}")
        print(f"  Successfully synced: {sync_stats['synced']:,}")
        print(f"  Failed syncs: {sync_stats['failed']:,}")
        print(f"  Pending syncs: {sync_stats['pending']:,}")
        print(f"  Currently syncing: {sync_stats['syncing']:,}")
        print(f"  Books with decrypted archives: {sync_stats['decrypted']:,}")

        if sync_stats["total_converted"] > 0:
            sync_percentage = (sync_stats["synced"] / sync_stats["total_converted"]) * 100
            print(f"  Sync completion: {sync_percentage:.1f}%")

        print()

        # Get enrichment statistics
        enrichment_stats = await tracker.get_enrichment_stats()

        print("Enrichment Status:")
        print(f"  Total books in database: {enrichment_stats['total_books']:,}")
        print(f"  Successfully enriched: {enrichment_stats['enriched']:,}")
        print(f"  Failed enrichment: {enrichment_stats['failed']:,}")
        print(f"  Pending enrichment: {enrichment_stats['pending']:,}")
        print(f"  Currently enriching: {enrichment_stats['in_progress']:,}")
        print(f"  No enrichment attempted: {enrichment_stats['no_enrichment_history']:,}")

        # Calculate enrichment completion percentage
        books_needing_enrichment = (
            enrichment_stats["enriched"] +
            enrichment_stats["failed"] +
            enrichment_stats["pending"] +
            enrichment_stats["in_progress"]
        )
        if books_needing_enrichment > 0:
            enrichment_percentage = (enrichment_stats["enriched"] / books_needing_enrichment) * 100
            print(f"  Enrichment completion: {enrichment_percentage:.1f}%")

        # Show enrichment rate and ETA if there are pending books
        remaining_books = enrichment_stats["pending"] + enrichment_stats["no_enrichment_history"]
        if remaining_books > 0:
            rate_stats = await tracker.get_enrichment_rate_stats(time_window_hours=24)
            if rate_stats["rate_per_hour"] > 0:
                eta_hours = remaining_books / rate_stats["rate_per_hour"]
                if eta_hours < 24:
                    print(f"  Estimated completion: {eta_hours:.1f} hours")
                else:
                    eta_days = eta_hours / 24
                    print(f"  Estimated completion: {eta_days:.1f} days")
                print(f"  Current enrichment rate: {rate_stats["rate_per_hour"]:.1f} books/hour")

        print()

        # Show breakdown by storage type if not filtered
        if not storage_type:
            print("Storage Type Breakdown:")

            # Get books by storage type and extract bucket from storage_path
            async with connect_async(db_path) as db:
                cursor = await db.execute("""
                    SELECT storage_type, storage_path, COUNT(*) as count
                    FROM books
                    WHERE storage_type IS NOT NULL AND storage_path IS NOT NULL
                    GROUP BY storage_type, storage_path
                    ORDER BY storage_type, count DESC
                """)
                storage_breakdown = await cursor.fetchall()

                if storage_breakdown:
                    # Group by storage type and extract bucket from path
                    storage_buckets: dict[str, int] = {}
                    for storage, path, count in storage_breakdown:
                        # Extract bucket from path (first part after removing prefix)
                        bucket = "unknown"
                        if path:
                            # For paths like "bucket/BARCODE/..." extract the bucket
                            parts = path.split("/")
                            if parts:
                                bucket = parts[0]

                        key = f"{storage}/{bucket}"
                        storage_buckets[key] = storage_buckets.get(key, 0) + count

                    for storage_bucket, count in sorted(storage_buckets.items()):
                        print(f"  {storage_bucket}: {count:,} books")
                else:
                    print("  No books have been synced to any storage yet")

                print()

        # Show recent enrichment activity
        print("Recent Enrichment Activity (last 10):")
        async with connect_async(db_path) as db:
            cursor = await db.execute("""
                SELECT b.barcode, h.status_value, h.timestamp, h.metadata
                FROM books b
                JOIN book_status_history h ON b.barcode = h.barcode
                WHERE h.status_type = 'enrichment'
                  AND h.id = (
                      SELECT MAX(h2.id)
                      FROM book_status_history h2
                      WHERE h2.barcode = b.barcode AND h2.status_type = 'enrichment'
                  )
                ORDER BY h.timestamp DESC
                LIMIT 10
            """)
            recent_enrichments = await cursor.fetchall()

            if recent_enrichments:
                for barcode, status, timestamp, metadata in recent_enrichments:
                    status_icon = {
                        "completed": "✅",
                        "failed": "❌",
                        "in_progress": "🔄",
                        "pending": "⏳"
                    }.get(status, "❓")
                    print(f"  {status_icon} {barcode} - {status} at {timestamp}")

                    # Show error details for failed enrichments
                    if status == "failed" and metadata:
                        try:
                            import json
                            meta = json.loads(metadata)
                            if "error_message" in meta:
                                error_msg = meta["error_message"][:100]  # Truncate long errors
                                print(f"      Error: {error_msg}")
                        except (json.JSONDecodeError, KeyError):
                            pass
            else:
                print("  No enrichment activity found")

        print()

        # Show recent sync activity
        print("Recent Sync Activity (last 10):")
        async with connect_async(db_path) as db:
            query = """
                SELECT b.barcode, h.status_value, b.sync_timestamp, b.sync_error, b.storage_type
                FROM books b
                JOIN book_status_history h ON b.barcode = h.barcode
                WHERE h.status_type = 'sync'
                  AND h.id = (
                      SELECT MAX(h2.id)
                      FROM book_status_history h2
                      WHERE h2.barcode = b.barcode AND h2.status_type = 'sync'
                  )
                  AND b.sync_timestamp IS NOT NULL
            """
            params = []

            if storage_type:
                query += " AND b.storage_type = ?"
                params.append(storage_type)

            query += " ORDER BY b.sync_timestamp DESC LIMIT 10"

            cursor = await db.execute(query, params)
            recent_syncs = await cursor.fetchall()

            if recent_syncs:
                for barcode, status, timestamp, error, st_type in recent_syncs:
                    status_icon = "✅" if status == "completed" else "❌" if status == "failed" else "🔄"
                    print(f"  {status_icon} {barcode} ({st_type}) - {status} at {timestamp}")
                    if error:
                        print(f"      Error: {error}")
            else:
                print("  No recent sync activity found")

    except Exception as e:
        print(f"❌ Error reading database: {e}")
        return

    finally:
        # Clean up database connections
        try:
            if hasattr(tracker, "_db") and tracker._db:
                await tracker._db.close()
        except Exception:
            pass


def validate_database_file(db_path: str) -> None:
    """Validate that the database file exists and contains the required tables.

    Args:
        db_path: Path to the SQLite database

    Raises:
        SystemExit: If database is invalid
    """
    db_file = Path(db_path)

    if not db_file.exists():
        print(f"❌ Error: Database file does not exist: {db_path}")
        print("\nRun a book collection first:")
        print("python grin.py collect --run-name <your_run_name>")
        sys.exit(1)

    try:
        with connect_sync(db_path) as conn:
            cursor = conn.cursor()
            cursor.execute("SELECT name FROM sqlite_master WHERE type='table';")
            tables = cursor.fetchall()

            table_names = [table[0] for table in tables]
            required_tables = ["books", "book_status_history"]

            for table in required_tables:
                if table not in table_names:
                    print(f"❌ Error: Database missing required table: {table}")
                    print(f"Available tables: {', '.join(table_names)}")
                    print("\nRun a fresh book collection to create the database:")
                    print("python grin.py collect --run-name <your_run_name>")
                    sys.exit(1)

    except sqlite3.Error as e:
        print(f"❌ Error reading database: {e}")
        print("\nThe database file may be corrupted. Try running a fresh collection:")
        print("python grin.py collect --run-name <your_run_name>")
        sys.exit(1)


async def get_sync_statistics(db_path: str, storage_type: str | None = None) -> dict[str, Any]:
    """Get sync statistics for the database.

    Args:
        db_path: Path to the SQLite database
        storage_type: Optional storage type filter

    Returns:
        dict: Sync statistics
    """
    tracker = SQLiteProgressTracker(db_path)

    try:
        # Get overall counts
        total_books = await tracker.get_book_count()
        enriched_books = await tracker.get_enriched_book_count()
        converted_books = await tracker.get_converted_books_count()

        # Get sync statistics
        sync_stats = await tracker.get_sync_stats(storage_type)

        return {
            "total_books": total_books,
            "enriched_books": enriched_books,
            "converted_books": converted_books,
            **sync_stats,
        }

    finally:
        # Clean up database connections
        try:
            if hasattr(tracker, "_db") and tracker._db:
                await tracker._db.close()
        except Exception:
            pass


async def export_sync_status_csv(db_path: str, output_path: str, storage_type: str | None = None) -> None:
    """Export sync status to CSV file.

    Args:
        db_path: Path to the SQLite database
        output_path: Path for output CSV file
        storage_type: Optional storage type filter
    """
    import csv

    async with connect_async(db_path) as db:
        query = """
            SELECT
                b.barcode,
                b.storage_type,
                b.storage_path,
                b.storage_decrypted_path,
                b.is_decrypted,
                b.sync_timestamp,
                b.sync_error,
                h.status_value as sync_status
            FROM books b
            LEFT JOIN book_status_history h ON b.barcode = h.barcode
            WHERE h.status_type = 'sync'
              AND h.id = (
                  SELECT MAX(h2.id)
                  FROM book_status_history h2
                  WHERE h2.barcode = b.barcode AND h2.status_type = 'sync'
              )
        """
        params = []

        if storage_type:
            query += " AND b.storage_type = ?"
            params.append(storage_type)

        query += " ORDER BY b.sync_timestamp DESC"

        cursor = await db.execute(query, params)
        rows = await cursor.fetchall()

        # Write CSV
        with open(output_path, "w", newline="") as csvfile:
            writer = csv.writer(csvfile)

            # Header
            writer.writerow(
                [
                    "barcode",
                    "storage_type",
                    "storage_path",
                    "storage_decrypted_path",
                    "is_decrypted",
                    "sync_timestamp",
                    "sync_error",
                    "sync_status",
                ]
            )

            # Data rows
            for row in rows:
                writer.writerow(row)

    print(f"Sync status exported to: {output_path}")
