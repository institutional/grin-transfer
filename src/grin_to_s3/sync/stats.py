#!/usr/bin/env python3
"""Sync statistics helpers."""

from __future__ import annotations

from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from grin_to_s3.collect_books.models import SQLiteProgressTracker


async def get_sync_stats(db_tracker: SQLiteProgressTracker) -> dict[str, int]:
    """Aggregate sync statistics for all tracked books."""

    await db_tracker.init_db()

    query = """
    WITH latest_sync AS (
        SELECT h.barcode, h.status_value
        FROM book_status_history h
        WHERE h.status_type = 'sync'
          AND h.timestamp = (
            SELECT MAX(h2.timestamp)
            FROM book_status_history h2
            WHERE h2.barcode = h.barcode AND h2.status_type = 'sync'
        )
    )
    SELECT
        COUNT(*) AS total_books,
        COALESCE(SUM(CASE WHEN b.is_decrypted = 1 THEN 1 ELSE 0 END), 0) AS decrypted,
        COALESCE(SUM(CASE WHEN ls.status_value = 'completed' THEN 1 ELSE 0 END), 0) AS synced,
        COALESCE(SUM(CASE WHEN ls.status_value = 'failed' THEN 1 ELSE 0 END), 0) AS failed,
        COALESCE(SUM(CASE WHEN ls.status_value = 'syncing' THEN 1 ELSE 0 END), 0) AS syncing,
        COALESCE(SUM(CASE WHEN ls.status_value = 'pending' THEN 1 ELSE 0 END), 0) AS pending_with_history,
        COALESCE(SUM(CASE WHEN ls.barcode IS NULL THEN 1 ELSE 0 END), 0) AS no_history
    FROM books b
    LEFT JOIN latest_sync ls ON b.barcode = ls.barcode
    """

    cursor = await db_tracker._execute_query(query, ())
    row = await cursor.fetchone()

    if not row:
        return {
            "total_converted": 0,
            "synced": 0,
            "failed": 0,
            "pending": 0,
            "syncing": 0,
            "decrypted": 0,
        }

    total_books = int(row[0]) if row[0] is not None else 0
    decrypted_count = int(row[1]) if row[1] is not None else 0
    synced_count = int(row[2]) if row[2] is not None else 0
    failed_count = int(row[3]) if row[3] is not None else 0
    syncing_count = int(row[4]) if row[4] is not None else 0
    pending_with_history = int(row[5]) if row[5] is not None else 0
    no_history = int(row[6]) if row[6] is not None else 0

    pending_count = pending_with_history + no_history

    return {
        "total_converted": total_books,
        "synced": synced_count,
        "failed": failed_count,
        "pending": pending_count,
        "syncing": syncing_count,
        "decrypted": decrypted_count,
    }
