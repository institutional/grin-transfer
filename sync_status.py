#!/usr/bin/env python3
"""
Sync Status Checker

Quick utility to check the sync status of books in the database.
"""

import argparse
import asyncio
import sys
from pathlib import Path

import aiosqlite
from collect_books.models import SQLiteProgressTracker


async def show_sync_status(db_path: str, storage_type: str | None = None) -> None:
    """Show sync status for books in the database."""
    
    # Validate database file
    db_file = Path(db_path)
    if not db_file.exists():
        print(f"❌ Error: Database file does not exist: {db_path}")
        return

    print(f"Sync Status Report")
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
        
        print(f"Overall Book Counts:")
        print(f"  Total books in database: {total_books:,}")
        print(f"  Books with enrichment data: {enriched_books:,}")
        print(f"  Books in converted state: {converted_books:,}")
        print()
        
        # Get sync statistics
        sync_stats = await tracker.get_sync_stats(storage_type)
        
        print(f"Sync Status:")
        print(f"  Total converted books: {sync_stats['total_converted']:,}")
        print(f"  Successfully synced: {sync_stats['synced']:,}")
        print(f"  Failed syncs: {sync_stats['failed']:,}")
        print(f"  Pending syncs: {sync_stats['pending']:,}")
        print(f"  Currently syncing: {sync_stats['syncing']:,}")
        print(f"  Books with decrypted archives: {sync_stats['decrypted']:,}")
        
        if sync_stats['total_converted'] > 0:
            sync_percentage = (sync_stats['synced'] / sync_stats['total_converted']) * 100
            print(f"  Sync completion: {sync_percentage:.1f}%")
        
        print()
        
        # Show breakdown by storage type if not filtered
        if not storage_type:
            print("Storage Type Breakdown:")
            
            # Get books by storage type
            async with aiosqlite.connect(db_path) as db:
                cursor = await db.execute("""
                    SELECT storage_type, COUNT(*) as count
                    FROM books 
                    WHERE storage_type IS NOT NULL
                    GROUP BY storage_type
                    ORDER BY count DESC
                """)
                storage_breakdown = await cursor.fetchall()
                
                if storage_breakdown:
                    for storage, count in storage_breakdown:
                        print(f"  {storage}: {count:,} books")
                else:
                    print("  No books have been synced to any storage yet")
                    
                print()
        
        # Show recent sync activity
        print("Recent Sync Activity (last 10):")
        async with aiosqlite.connect(db_path) as db:
            query = """
                SELECT barcode, sync_status, sync_timestamp, sync_error, storage_type
                FROM books 
                WHERE sync_timestamp IS NOT NULL
            """
            params = []
            
            if storage_type:
                query += " AND storage_type = ?"
                params.append(storage_type)
                
            query += " ORDER BY sync_timestamp DESC LIMIT 10"
            
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


def main() -> None:
    """Main CLI entry point."""
    parser = argparse.ArgumentParser(
        description="Check sync status of books in the database",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Check overall sync status
  python sync_status.py output/harvard_2024/books.db

  # Check sync status for specific storage type
  python sync_status.py output/harvard_2024/books.db --storage-type=r2
        """,
    )

    parser.add_argument("db_path", help="SQLite database path (e.g., output/harvard_2024/books.db)")
    parser.add_argument("--storage-type", choices=["local", "minio", "r2", "s3"], 
                       help="Filter by storage type")

    args = parser.parse_args()

    try:
        asyncio.run(show_sync_status(args.db_path, args.storage_type))
    except KeyboardInterrupt:
        print("\nOperation cancelled by user")
        sys.exit(1)
    except Exception as e:
        print(f"Error: {e}")
        sys.exit(1)


if __name__ == "__main__":
    main()