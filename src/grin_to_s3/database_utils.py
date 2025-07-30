#!/usr/bin/env python3
"""
Database utility functions for grin-to-s3.

Contains shared database validation and utility functions to eliminate
duplication and local imports across the codebase.
"""

import json
import logging
import sqlite3
import sys
from datetime import UTC, datetime
from functools import wraps
from pathlib import Path

import aiosqlite
from tenacity import retry, retry_if_exception_type, stop_after_attempt, wait_exponential

from .database import connect_async, connect_sync

logger = logging.getLogger(__name__)


def retry_database_operation(func):
    """
    Decorator to retry database operations that fail due to database locks.

    Retries both sqlite3.OperationalError and aiosqlite.OperationalError with
    "database is locked" message up to 3 times with exponential backoff.
    """
    @retry(
        stop=stop_after_attempt(5),
        retry=retry_if_exception_type((sqlite3.OperationalError, aiosqlite.OperationalError)),
        wait=wait_exponential(multiplier=0.2, min=0.2, max=2.0),
        reraise=True
    )
    @wraps(func)
    async def wrapper(*args, **kwargs):
        try:
            return await func(*args, **kwargs)
        except (sqlite3.OperationalError, aiosqlite.OperationalError) as e:
            if "database is locked" not in str(e).lower():
                raise
            logger.warning(f"Database lock detected in {func.__name__}, retrying... ({str(e)})")
            raise

    return wrapper


@retry_database_operation
async def batch_write_status_updates(db_path: str, status_updates: list) -> None:
    """Write multiple status updates in a single transaction."""
    if not status_updates:
        return

    async with connect_async(db_path) as conn:
        for status_update in status_updates:
            await conn.execute(
                """INSERT INTO book_status_history
                   (barcode, status_type, status_value, timestamp, session_id, metadata)
                   VALUES (?, ?, ?, ?, ?, ?)""",
                (
                    status_update.barcode,
                    status_update.status_type,
                    status_update.status_value,
                    datetime.now(UTC).isoformat(),
                    status_update.session_id,
                    json.dumps(status_update.metadata) if status_update.metadata else None,
                ),
            )
        await conn.commit()


def validate_database_file(db_path: str, check_tables: bool = False, check_books_count: bool = False) -> None:
    """
    Validate that the database file exists and is a valid SQLite database.

    Args:
        db_path: Path to the SQLite database file
        check_tables: Whether to validate that required tables exist
        check_books_count: Whether to validate that books table has records

    Raises:
        SystemExit: If the database file is invalid or inaccessible
    """
    db_file = Path(db_path)

    # Check if file exists
    if not db_file.exists():
        print(f"❌ Error: Database file does not exist: {db_path}")
        print("\nMake sure you've run a book collection first:")
        print("python grin.py collect --run-name <your_run_name>")
        print("\nOr check available databases:")

        # Try to show available databases
        output_dir = Path("output")
        if output_dir.exists():
            print("\nAvailable run directories:")
            for run_dir in output_dir.iterdir():
                if run_dir.is_dir():
                    db_file_path = run_dir / "books.db"
                    if db_file_path.exists():
                        print(f"  {db_file_path}")

        sys.exit(1)

    # Check if it's a valid SQLite database
    try:
        with connect_sync(db_path) as conn:
            cursor = conn.cursor()

            # Always do a basic check to ensure it's a valid SQLite database
            cursor.execute("SELECT name FROM sqlite_master WHERE type='table';")
            tables = cursor.fetchall()

            if check_tables:
                # Check for expected tables
                table_names = [table[0] for table in tables]
                required_tables = ["books", "processed", "failed"]
                missing_tables = [table for table in required_tables if table not in table_names]

                if missing_tables:
                    print(f"❌ Error: Database is missing required tables: {missing_tables}")
                    print(f"Database file: {db_path}")
                    print("This doesn't appear to be a valid book collection database.")
                    sys.exit(1)

            if check_books_count:
                # Check that books table has records
                cursor.execute("SELECT COUNT(*) FROM books")
                count = cursor.fetchone()[0]

                if count == 0:
                    print(f"❌ Error: Database contains no books: {db_path}")
                    sys.exit(1)

                logger.debug(f"Using database: {db_path} ({count:,} books)")
            else:
                logger.debug(f"Using database: {db_path}")

    except sqlite3.Error as e:
        print(f"❌ Error: Cannot read SQLite database: {e}")
        print(f"Database file: {db_path}")
        print("The file may be corrupted or not a valid SQLite database.")
        sys.exit(1)
