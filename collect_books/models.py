#!/usr/bin/env python3
"""
Data models for CSV export functionality.

Contains BookRecord and other data classes used in the CSV export system.
"""

import asyncio
import time
from collections import OrderedDict
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path

import aiosqlite


@dataclass
class BookRecord:
    """Book record for V2 CSV export and SQLite storage."""

    # Core identification
    barcode: str
    title: str = ""

    # GRIN timestamps (from _all_books endpoint)
    scanned_date: str | None = None
    converted_date: str | None = None
    downloaded_date: str | None = None
    processed_date: str | None = None
    analyzed_date: str | None = None
    ocr_date: str | None = None
    google_books_link: str = ""

    # Processing state (inferred from GRIN endpoint presence)
    processing_state: str = "unknown"  # all_books, converted, failed, pending

    # GRIN enrichment fields (populated by separate enrichment pipeline)
    grin_state: str | None = None
    viewability: str | None = None
    opted_out: str | None = None
    conditions: str | None = None
    scannable: str | None = None
    tagging: str | None = None
    audit: str | None = None
    material_error_percent: str | None = None
    overall_error_percent: str | None = None
    claimed: str | None = None
    ocr_analysis_score: str | None = None
    ocr_gtd_score: str | None = None
    digitization_method: str | None = None
    enrichment_timestamp: str | None = None

    # Export tracking
    csv_exported: str | None = None
    csv_updated: str | None = None

    @classmethod
    def csv_headers(cls) -> list:
        """Get CSV column headers."""
        return [
            "Barcode",
            "Title",
            "Scanned Date",
            "Converted Date",
            "Downloaded Date",
            "Processed Date",
            "Analyzed Date",
            "OCR Date",
            "Google Books Link",
            "Processing State",
            "GRIN State",
            "Viewability",
            "Opted Out",
            "Conditions",
            "Scannable",
            "Tagging",
            "Audit",
            "Material Error %",
            "Overall Error %",
            "Claimed",
            "OCR Analysis Score",
            "OCR GTD Score",
            "Digitization Method",
            "Enrichment Timestamp",
            "CSV Exported",
            "CSV Updated",
        ]

    def to_csv_row(self) -> list[str]:
        """Convert to CSV row values."""
        return [
            self.barcode,
            self.title,
            self.scanned_date or "",
            self.converted_date or "",
            self.downloaded_date or "",
            self.processed_date or "",
            self.analyzed_date or "",
            self.ocr_date or "",
            self.google_books_link,
            self.processing_state,
            self.grin_state or "",
            self.viewability or "",
            self.opted_out or "",
            self.conditions or "",
            self.scannable or "",
            self.tagging or "",
            self.audit or "",
            self.material_error_percent or "",
            self.overall_error_percent or "",
            self.claimed or "",
            self.ocr_analysis_score or "",
            self.ocr_gtd_score or "",
            self.digitization_method or "",
            self.enrichment_timestamp or "",
            self.csv_exported or "",
            self.csv_updated or "",
        ]


class RateLimiter:
    """Configurable rate limiter for API requests."""

    def __init__(self, requests_per_second: float = 1.0, burst_limit: int = 5):
        self.requests_per_second = requests_per_second
        self.burst_limit = burst_limit
        self.tokens = float(burst_limit)
        self.last_update = time.time()

    async def acquire(self):
        """Wait until a request token is available."""
        now = time.time()
        elapsed = now - self.last_update

        # Add tokens based on elapsed time
        self.tokens = min(self.burst_limit, self.tokens + elapsed * self.requests_per_second)
        self.last_update = now

        if self.tokens < 1:
            # Wait until we have a token
            wait_time = (1 - self.tokens) / self.requests_per_second
            await asyncio.sleep(wait_time)
            self.tokens = 1

        self.tokens -= 1


class BoundedSet:
    """
    A memory-bounded set that automatically evicts old items when capacity is exceeded.

    Uses LRU (Least Recently Used) eviction policy to maintain a fixed maximum size.
    Provides set-like interface for membership testing and insertion.
    """

    def __init__(self, max_size: int = 50000):
        self.max_size = max_size
        self._data: OrderedDict[str, bool] = OrderedDict()  # Maintains insertion order for LRU

    def add(self, item: str) -> None:
        """Add an item to the set, evicting oldest if necessary."""
        if item in self._data:
            # Move to end (most recently used)
            self._data.move_to_end(item)
        else:
            # Add new item
            self._data[item] = True

            # Evict oldest items if over capacity
            while len(self._data) > self.max_size:
                self._data.popitem(last=False)  # Remove oldest (FIFO/LRU)

    def __contains__(self, item: str) -> bool:
        """Check if item is in the set."""
        if item in self._data:
            # Move to end (mark as recently used)
            self._data.move_to_end(item)
            return True
        return False

    def __len__(self) -> int:
        """Return current number of items in the set."""
        return len(self._data)

    def clear(self) -> None:
        """Remove all items from the set."""
        self._data.clear()

    def to_set(self) -> set[str]:
        """Convert to a regular Python set (for serialization)."""
        return set(self._data.keys())

    def update(self, items: set[str]) -> None:
        """Add multiple items from a regular set."""
        for item in items:
            self.add(item)


class SQLiteProgressTracker:
    """
    SQLite-based progress tracker that avoids memory leaks from large barcode sets.

    Stores processed and failed barcodes in a SQLite database with O(log n) lookups.
    Maintains minimal memory footprint regardless of dataset size.
    """

    def __init__(self, db_path: str = "output/default/books.db", cache_size: int = 10000):
        self.db_path = Path(db_path)
        self.session_id = int(datetime.now(UTC).timestamp())
        self._initialized = False
        # Small LRU cache to avoid repeated SQLite queries for same barcodes
        self._known_cache = BoundedSet(max_size=cache_size)
        self._unknown_cache = BoundedSet(max_size=cache_size)
        self._db_connections: set[aiosqlite.Connection] = set()  # Track connections for cleanup

    async def init_db(self) -> None:
        """Initialize database schema if not exists."""
        if self._initialized:
            return

        # Ensure directory exists
        self.db_path.parent.mkdir(parents=True, exist_ok=True)

        async with aiosqlite.connect(self.db_path) as db:
            # Create tables
            await db.execute("""
                CREATE TABLE IF NOT EXISTS processed (
                    barcode TEXT PRIMARY KEY,
                    timestamp TEXT NOT NULL,
                    session_id INTEGER NOT NULL
                )
            """)

            await db.execute("""
                CREATE TABLE IF NOT EXISTS failed (
                    barcode TEXT PRIMARY KEY,
                    timestamp TEXT NOT NULL,
                    session_id INTEGER NOT NULL,
                    error_message TEXT
                )
            """)

            # Create books table for storing all book data
            await db.execute("""
                CREATE TABLE IF NOT EXISTS books (
                    barcode TEXT PRIMARY KEY,
                    title TEXT,
                    scanned_date TEXT,
                    converted_date TEXT,
                    downloaded_date TEXT,
                    processed_date TEXT,
                    analyzed_date TEXT,
                    ocr_date TEXT,
                    google_books_link TEXT,
                    processing_state TEXT,
                    grin_state TEXT,
                    viewability TEXT,
                    opted_out TEXT,
                    conditions TEXT,
                    scannable TEXT,
                    tagging TEXT,
                    audit TEXT,
                    material_error_percent TEXT,
                    overall_error_percent TEXT,
                    claimed TEXT,
                    ocr_analysis_score TEXT,
                    ocr_gtd_score TEXT,
                    digitization_method TEXT,
                    enrichment_timestamp TEXT,
                    csv_exported TEXT,
                    csv_updated TEXT,
                    created_at TEXT NOT NULL,
                    updated_at TEXT NOT NULL
                )
            """)

            # Create indexes for fast lookups
            await db.execute("CREATE INDEX IF NOT EXISTS idx_processed_barcode ON processed(barcode)")
            await db.execute("CREATE INDEX IF NOT EXISTS idx_failed_barcode ON failed(barcode)")
            await db.execute("CREATE INDEX IF NOT EXISTS idx_processed_session ON processed(session_id)")
            await db.execute("CREATE INDEX IF NOT EXISTS idx_failed_session ON failed(session_id)")
            await db.execute("CREATE INDEX IF NOT EXISTS idx_books_barcode ON books(barcode)")
            await db.execute("CREATE INDEX IF NOT EXISTS idx_books_processing_state ON books(processing_state)")
            await db.execute("CREATE INDEX IF NOT EXISTS idx_books_enrichment ON books(enrichment_timestamp)")

            await db.commit()

        self._initialized = True

    async def mark_processed(self, barcode: str) -> None:
        """Mark barcode as successfully processed."""
        await self.init_db()

        async with aiosqlite.connect(self.db_path) as db:
            await db.execute(
                "INSERT OR REPLACE INTO processed (barcode, timestamp, session_id) VALUES (?, ?, ?)",
                (barcode, datetime.now(UTC).isoformat(), self.session_id),
            )
            await db.commit()

    async def mark_failed(self, barcode: str, error_message: str = "") -> None:
        """Mark barcode as failed with optional error message."""
        await self.init_db()

        async with aiosqlite.connect(self.db_path) as db:
            await db.execute(
                "INSERT OR REPLACE INTO failed (barcode, timestamp, session_id, error_message) VALUES (?, ?, ?, ?)",
                (barcode, datetime.now(UTC).isoformat(), self.session_id, error_message),
            )
            await db.commit()

    async def is_processed(self, barcode: str) -> bool:
        """Check if barcode was successfully processed."""
        await self.init_db()

        async with aiosqlite.connect(self.db_path) as db:
            cursor = await db.execute("SELECT 1 FROM processed WHERE barcode = ?", (barcode,))
            return await cursor.fetchone() is not None

    async def is_failed(self, barcode: str) -> bool:
        """Check if barcode previously failed."""
        await self.init_db()

        async with aiosqlite.connect(self.db_path) as db:
            cursor = await db.execute("SELECT 1 FROM failed WHERE barcode = ?", (barcode,))
            return await cursor.fetchone() is not None

    async def is_known(self, barcode: str) -> bool:
        """Check if barcode was processed or failed (either state) with caching."""
        # Check cache first
        if barcode in self._known_cache:
            return True
        if barcode in self._unknown_cache:
            return False

        # Not in cache - query database
        is_known = await self.is_processed(barcode) or await self.is_failed(barcode)

        # Cache the result
        if is_known:
            self._known_cache.add(barcode)
        else:
            self._unknown_cache.add(barcode)

        return is_known

    async def load_known_barcodes_batch(self, barcodes: set[str]) -> set[str]:
        """
        Batch load known barcodes (processed or failed) for performance.

        Args:
            barcodes: Set of barcodes to check

        Returns:
            Set of barcodes that are known (processed or failed)
        """
        if not barcodes:
            return set()

        await self.init_db()
        known_barcodes: set[str] = set()

        # Convert to list for SQL query
        barcode_list = list(barcodes)

        async with aiosqlite.connect(self.db_path) as db:
            # Check processed barcodes in batch
            placeholders = ",".join("?" * len(barcode_list))

            cursor = await db.execute(f"SELECT barcode FROM processed WHERE barcode IN ({placeholders})", barcode_list)
            processed_rows = await cursor.fetchall()
            known_barcodes.update(row[0] for row in processed_rows)

            # Check failed barcodes in batch
            cursor = await db.execute(f"SELECT barcode FROM failed WHERE barcode IN ({placeholders})", barcode_list)
            failed_rows = await cursor.fetchall()
            known_barcodes.update(row[0] for row in failed_rows)

        return known_barcodes

    async def get_all_known_barcodes(self) -> set[str]:
        """
        Load all known barcodes (processed + failed) into memory.
        Use carefully - only for small to medium datasets.
        """
        await self.init_db()
        known_barcodes: set[str] = set()

        async with aiosqlite.connect(self.db_path) as db:
            # Get all processed barcodes
            cursor = await db.execute("SELECT barcode FROM processed")
            processed_rows = await cursor.fetchall()
            known_barcodes.update(row[0] for row in processed_rows)

            # Get all failed barcodes
            cursor = await db.execute("SELECT barcode FROM failed")
            failed_rows = await cursor.fetchall()
            known_barcodes.update(row[0] for row in failed_rows)

        return known_barcodes

    async def get_processed_count(self) -> int:
        """Get total number of successfully processed barcodes."""
        await self.init_db()

        async with aiosqlite.connect(self.db_path) as db:
            cursor = await db.execute("SELECT COUNT(*) FROM processed")
            row = await cursor.fetchone()
            return row[0] if row else 0

    async def get_failed_count(self) -> int:
        """Get total number of failed barcodes."""
        await self.init_db()

        async with aiosqlite.connect(self.db_path) as db:
            cursor = await db.execute("SELECT COUNT(*) FROM failed")
            row = await cursor.fetchone()
            return row[0] if row else 0

    async def get_session_stats(self) -> dict:
        """Get statistics for current session."""
        await self.init_db()

        async with aiosqlite.connect(self.db_path) as db:
            # Current session counts
            cursor = await db.execute("SELECT COUNT(*) FROM processed WHERE session_id = ?", (self.session_id,))
            session_processed_row = await cursor.fetchone()
            session_processed = session_processed_row[0] if session_processed_row else 0

            cursor = await db.execute("SELECT COUNT(*) FROM failed WHERE session_id = ?", (self.session_id,))
            session_failed_row = await cursor.fetchone()
            session_failed = session_failed_row[0] if session_failed_row else 0

            # Total counts
            total_processed = await self.get_processed_count()
            total_failed = await self.get_failed_count()

            return {
                "session_processed": session_processed,
                "session_failed": session_failed,
                "total_processed": total_processed,
                "total_failed": total_failed,
                "session_id": self.session_id,
            }

    async def cleanup_old_sessions(self, keep_sessions: int = 5) -> None:
        """Remove data from old sessions, keeping only the most recent N sessions."""
        await self.init_db()

        async with aiosqlite.connect(self.db_path) as db:
            # Get session IDs to keep (most recent N)
            cursor = await db.execute(
                """
                SELECT DISTINCT session_id FROM (
                    SELECT session_id FROM processed
                    UNION
                    SELECT session_id FROM failed
                ) ORDER BY session_id DESC LIMIT ?
            """,
                (keep_sessions,),
            )

            keep_session_ids = [row[0] for row in await cursor.fetchall()]

            if keep_session_ids:
                placeholders = ",".join("?" * len(keep_session_ids))

                # Delete old processed records
                await db.execute(f"DELETE FROM processed WHERE session_id NOT IN ({placeholders})", keep_session_ids)

                # Delete old failed records
                await db.execute(f"DELETE FROM failed WHERE session_id NOT IN ({placeholders})", keep_session_ids)

                await db.commit()

    async def close(self) -> None:
        """Close any open database connections and clean up resources."""
        # Clear caches
        self._known_cache.clear()
        self._unknown_cache.clear()

        # Close any tracked connections
        for conn in list(self._db_connections):
            try:
                await conn.close()
            except Exception:
                pass
        self._db_connections.clear()

    async def save_book(self, book: BookRecord) -> None:
        """Save or update a book record in the database."""
        await self.init_db()

        now = datetime.now(UTC).isoformat()

        async with aiosqlite.connect(self.db_path) as db:
            await db.execute(
                """
                INSERT OR REPLACE INTO books (
                    barcode, title, scanned_date, converted_date, downloaded_date,
                    processed_date, analyzed_date, ocr_date, google_books_link,
                    processing_state, grin_state, viewability, opted_out,
                    conditions, scannable, tagging, audit, material_error_percent,
                    overall_error_percent, claimed, ocr_analysis_score,
                    ocr_gtd_score, digitization_method, enrichment_timestamp,
                    csv_exported, csv_updated, created_at, updated_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
                (
                    book.barcode,
                    book.title,
                    book.scanned_date,
                    book.converted_date,
                    book.downloaded_date,
                    book.processed_date,
                    book.analyzed_date,
                    book.ocr_date,
                    book.google_books_link,
                    book.processing_state,
                    book.grin_state,
                    book.viewability,
                    book.opted_out,
                    book.conditions,
                    book.scannable,
                    book.tagging,
                    book.audit,
                    book.material_error_percent,
                    book.overall_error_percent,
                    book.claimed,
                    book.ocr_analysis_score,
                    book.ocr_gtd_score,
                    book.digitization_method,
                    book.enrichment_timestamp,
                    book.csv_exported,
                    book.csv_updated,
                    now,
                    now,
                ),
            )
            await db.commit()

    async def get_book(self, barcode: str) -> BookRecord | None:
        """Retrieve a book record from the database."""
        await self.init_db()

        async with aiosqlite.connect(self.db_path) as db:
            cursor = await db.execute(
                """
                SELECT barcode, title, scanned_date, converted_date, downloaded_date,
                       processed_date, analyzed_date, ocr_date, google_books_link,
                       processing_state, grin_state, viewability, opted_out,
                       conditions, scannable, tagging, audit, material_error_percent,
                       overall_error_percent, claimed, ocr_analysis_score,
                       ocr_gtd_score, digitization_method, enrichment_timestamp,
                       csv_exported, csv_updated
                FROM books WHERE barcode = ?
            """,
                (barcode,),
            )

            row = await cursor.fetchone()
            if row:
                return BookRecord(*row)
            return None

    async def update_book_enrichment(self, barcode: str, enrichment_data: dict) -> bool:
        """Update enrichment fields for an existing book record."""
        await self.init_db()

        now = datetime.now(UTC).isoformat()

        async with aiosqlite.connect(self.db_path) as db:
            await db.execute(
                """
                UPDATE books SET
                    grin_state = ?, viewability = ?, opted_out = ?, conditions = ?,
                    scannable = ?, tagging = ?, audit = ?, material_error_percent = ?,
                    overall_error_percent = ?, claimed = ?, ocr_analysis_score = ?,
                    ocr_gtd_score = ?, digitization_method = ?, enrichment_timestamp = ?,
                    updated_at = ?
                WHERE barcode = ?
            """,
                (
                    enrichment_data.get("grin_state"),
                    enrichment_data.get("viewability"),
                    enrichment_data.get("opted_out"),
                    enrichment_data.get("conditions"),
                    enrichment_data.get("scannable"),
                    enrichment_data.get("tagging"),
                    enrichment_data.get("audit"),
                    enrichment_data.get("material_error_percent"),
                    enrichment_data.get("overall_error_percent"),
                    enrichment_data.get("claimed"),
                    enrichment_data.get("ocr_analysis_score"),
                    enrichment_data.get("ocr_gtd_score"),
                    enrichment_data.get("digitization_method"),
                    now,
                    now,
                    barcode,
                ),
            )

            rows_affected = db.total_changes
            await db.commit()
            return rows_affected > 0

    async def get_books_for_enrichment(self, limit: int = 1000) -> list[str]:
        """Get barcodes for books that need enrichment (no enrichment_timestamp)."""
        await self.init_db()

        async with aiosqlite.connect(self.db_path) as db:
            cursor = await db.execute(
                """
                SELECT barcode FROM books
                WHERE enrichment_timestamp IS NULL
                ORDER BY created_at
                LIMIT ?
            """,
                (limit,),
            )

            rows = await cursor.fetchall()
            return [row[0] for row in rows]

    async def get_all_books_csv_data(self) -> list[BookRecord]:
        """Get all book records for CSV export."""
        await self.init_db()

        async with aiosqlite.connect(self.db_path) as db:
            cursor = await db.execute("""
                SELECT barcode, title, scanned_date, converted_date, downloaded_date,
                       processed_date, analyzed_date, ocr_date, google_books_link,
                       processing_state, grin_state, viewability, opted_out,
                       conditions, scannable, tagging, audit, material_error_percent,
                       overall_error_percent, claimed, ocr_analysis_score,
                       ocr_gtd_score, digitization_method, enrichment_timestamp
                FROM books ORDER BY barcode
            """)

            rows = await cursor.fetchall()
            return [BookRecord(*row) for row in rows]

    async def get_book_count(self) -> int:
        """Get total number of books in database."""
        await self.init_db()

        async with aiosqlite.connect(self.db_path) as db:
            cursor = await db.execute("SELECT COUNT(*) FROM books")
            row = await cursor.fetchone()
            return row[0] if row else 0

    async def get_enriched_book_count(self) -> int:
        """Get count of books with enrichment data."""
        await self.init_db()

        async with aiosqlite.connect(self.db_path) as db:
            cursor = await db.execute("""
                SELECT COUNT(*) FROM books
                WHERE enrichment_timestamp IS NOT NULL
            """)
            row = await cursor.fetchone()
            return row[0] if row else 0
