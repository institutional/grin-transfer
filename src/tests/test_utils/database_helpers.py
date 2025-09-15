#!/usr/bin/env python3
"""
Database test helper functions
"""

from grin_to_s3.collect_books.models import BookRecord, SQLiteProgressTracker


async def get_book_for_testing(tracker: SQLiteProgressTracker, barcode: str) -> BookRecord | None:
    """Test helper to retrieve a book record from the database."""
    await tracker.init_db()

    cursor = await tracker._execute_query(
        f"{BookRecord.build_select_sql()} WHERE barcode = ?",
        (barcode,),
    )
    row = await cursor.fetchone()
    if row:
        return BookRecord(*row)
    return None


async def get_all_barcodes_for_testing(tracker: SQLiteProgressTracker, limit: int = None) -> list[str]:
    """Test helper to get all book barcodes, optionally limited."""
    await tracker.init_db()

    query = "SELECT barcode FROM books ORDER BY created_at DESC"
    params = ()

    if limit is not None:
        query += " LIMIT ?"
        params = (limit,)

    cursor = await tracker._execute_query(query, params)
    rows = await cursor.fetchall()
    return [row[0] for row in rows]


async def get_barcodes_from_set_for_testing(
    tracker: SQLiteProgressTracker, barcode_set: set[str], limit: int = None
) -> list[str]:
    """Test helper to get barcodes that exist in database and are in the given set."""
    await tracker.init_db()

    if not barcode_set:
        return []

    placeholders = ",".join("?" * len(barcode_set))
    query = f"SELECT barcode FROM books WHERE barcode IN ({placeholders}) ORDER BY created_at DESC"
    params = list(barcode_set)

    if limit is not None:
        query += " LIMIT ?"
        params.append(limit)

    cursor = await tracker._execute_query(query, tuple(params))
    rows = await cursor.fetchall()
    return [row[0] for row in rows]


async def get_barcodes_with_sync_status_for_testing(
    tracker: SQLiteProgressTracker, status_filter: str, limit: int = None
) -> list[str]:
    """Test helper to get barcodes with specific sync status."""
    await tracker.init_db()

    query = """
        SELECT DISTINCT h1.barcode
        FROM book_status_history h1
        INNER JOIN (
            SELECT barcode, MAX(timestamp) as max_timestamp, MAX(id) as max_id
            FROM book_status_history
            WHERE status_type = 'sync'
            GROUP BY barcode
        ) h2 ON h1.barcode = h2.barcode
            AND h1.timestamp = h2.max_timestamp
            AND h1.id = h2.max_id
        WHERE h1.status_type = 'sync'
        AND h1.status_value = ?
        ORDER BY h1.barcode
    """
    params = [status_filter]

    if limit is not None:
        query += " LIMIT ?"
        params.append(limit)

    cursor = await tracker._execute_query(query, tuple(params))
    rows = await cursor.fetchall()
    return [row[0] for row in rows]


async def get_barcodes_needing_sync_for_testing(tracker: SQLiteProgressTracker, limit: int = None) -> list[str]:
    """Test helper to get barcodes that need syncing (no sync status OR failed/syncing status)."""
    await tracker.init_db()

    query = """
        SELECT barcode FROM books
        WHERE (
            barcode NOT IN (
                SELECT DISTINCT barcode
                FROM book_status_history
                WHERE status_type = 'sync'
            )
            OR barcode IN (
                SELECT DISTINCT h1.barcode
                FROM book_status_history h1
                INNER JOIN (
                    SELECT barcode, MAX(timestamp) as max_timestamp, MAX(id) as max_id
                    FROM book_status_history
                    WHERE status_type = 'sync'
                    GROUP BY barcode
                ) h2 ON h1.barcode = h2.barcode
                    AND h1.timestamp = h2.max_timestamp
                    AND h1.id = h2.max_id
                WHERE h1.status_type = 'sync'
                AND h1.status_value IN ('failed', 'syncing')
            )
        )
        ORDER BY created_at DESC
    """
    params = []

    if limit is not None:
        query += " LIMIT ?"
        params.append(limit)

    cursor = await tracker._execute_query(query, tuple(params))
    rows = await cursor.fetchall()
    return [row[0] for row in rows]


async def get_latest_status_for_testing(tracker: SQLiteProgressTracker, barcode: str, status_type: str) -> str | None:
    """Get the latest status value for a book and status type.

    Args:
        tracker: The SQLiteProgressTracker instance
        barcode: Book barcode
        status_type: Type of status to retrieve

    Returns:
        Latest status value or None if no status found
    """
    await tracker.init_db()

    return await tracker._execute_single_value_query(
        """
        SELECT status_value FROM book_status_history
        WHERE barcode = ? AND status_type = ?
        ORDER BY timestamp DESC, id DESC
        LIMIT 1
        """,
        (barcode, status_type),
    )


async def get_books_with_latest_status_for_testing(
    tracker: SQLiteProgressTracker, status_type: str, status_values: list[str] | None = None, limit: int | None = None
) -> list[tuple[str, str]]:
    """Get books with their latest status for a given status type.

    Args:
        tracker: The SQLiteProgressTracker instance
        status_type: Type of status to filter by
        status_values: Optional list of status values to filter by
        limit: Optional limit on number of results

    Returns:
        List of (barcode, latest_status_value) tuples
    """
    await tracker.init_db()

    # Build query to get latest status for each book
    base_query = """
        SELECT DISTINCT h1.barcode, h1.status_value
        FROM book_status_history h1
        INNER JOIN (
            SELECT barcode, MAX(timestamp) as max_timestamp, MAX(id) as max_id
            FROM book_status_history
            WHERE status_type = ?
            GROUP BY barcode
        ) h2 ON h1.barcode = h2.barcode
            AND h1.timestamp = h2.max_timestamp
            AND h1.id = h2.max_id
        WHERE h1.status_type = ?
    """

    params = [status_type, status_type]

    if status_values:
        placeholders = ",".join("?" * len(status_values))
        base_query += f" AND h1.status_value IN ({placeholders})"
        params.extend(status_values)

    base_query += " ORDER BY h1.timestamp DESC"

    if limit:
        base_query += " LIMIT ?"
        params.append(str(limit))

    cursor = await tracker._execute_query(base_query, tuple(params))
    rows = await cursor.fetchall()
    return [(row[0], row[1]) for row in rows]
