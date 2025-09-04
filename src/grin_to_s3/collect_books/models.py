#!/usr/bin/env python3
"""
Data models for CSV export functionality.

Contains BookRecord and other data classes used in the CSV export system.
"""

import json
import logging
from collections import OrderedDict
from dataclasses import dataclass, field, fields
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any

import aiosqlite

# Database connection utilities no longer needed - using persistent connections
from ..database.database_utils import retry_database_operation

logger = logging.getLogger(__name__)


@dataclass
class BookRecord:
    """Book record for CSV export and SQLite storage."""

    # Core identification
    barcode: str = field(metadata={"csv": "Barcode"})
    title: str = field(default="", metadata={"csv": "Title"})

    # GRIN timestamps (from _all_books endpoint and enrichment TSV)
    scanned_date: str | None = field(default=None, metadata={"csv": "Scanned Date", "grin_tsv": "Scanned Date"})
    converted_date: str | None = field(default=None, metadata={"csv": "Converted Date", "grin_tsv": "Converted Date"})
    downloaded_date: str | None = field(
        default=None, metadata={"csv": "Downloaded Date", "grin_tsv": "Downloaded Date"}
    )
    processed_date: str | None = field(default=None, metadata={"csv": "Processed Date", "grin_tsv": "Processed Date"})
    analyzed_date: str | None = field(default=None, metadata={"csv": "Analyzed Date", "grin_tsv": "Analyzed Date"})
    ocr_date: str | None = field(default=None, metadata={"csv": "OCR Date", "grin_tsv": "OCR'd Date"})
    google_books_link: str = field(default="", metadata={"csv": "Google Books Link"})

    # Processing request tracking (status tracked in history table)
    processing_request_timestamp: str | None = field(default=None, metadata={"csv": "Processing Request Timestamp"})

    # GRIN enrichment fields (populated by separate enrichment pipeline)
    grin_state: str | None = field(default=None, metadata={"csv": "GRIN State", "grin_tsv": "State"})
    grin_viewability: str | None = field(default=None, metadata={"csv": "GRIN Viewability", "grin_tsv": "Viewability"})
    grin_opted_out: str | None = field(
        default=None, metadata={"csv": "GRIN Opted Out", "grin_tsv": "Opted-Out (post-scan)"}
    )
    grin_conditions: str | None = field(default=None, metadata={"csv": "GRIN Conditions", "grin_tsv": "Conditions"})
    grin_scannable: str | None = field(default=None, metadata={"csv": "GRIN Scannable", "grin_tsv": "Scannable"})
    grin_tagging: str | None = field(default=None, metadata={"csv": "GRIN Tagging", "grin_tsv": "Tagging"})
    grin_audit: str | None = field(default=None, metadata={"csv": "GRIN Audit", "grin_tsv": "Audit"})
    grin_material_error_percent: str | None = field(
        default=None, metadata={"csv": "GRIN Material Error %", "grin_tsv": "Material Error%"}
    )
    grin_overall_error_percent: str | None = field(
        default=None, metadata={"csv": "GRIN Overall Error %", "grin_tsv": "Overall Error%"}
    )
    grin_claimed: str | None = field(default=None, metadata={"csv": "GRIN Claimed", "grin_tsv": "Claimed"})
    grin_ocr_analysis_score: str | None = field(
        default=None, metadata={"csv": "GRIN OCR Analysis Score", "grin_tsv": "OCR Analysis Score"}
    )
    grin_ocr_gtd_score: str | None = field(
        default=None, metadata={"csv": "GRIN OCR GTD Score", "grin_tsv": "OCR GTD Score"}
    )
    grin_digitization_method: str | None = field(
        default=None, metadata={"csv": "GRIN Digitization Method", "grin_tsv": "Digitization Method"}
    )
    grin_check_in_date: str | None = field(
        default=None, metadata={"csv": "GRIN Check-In Date", "grin_tsv": "Check-In Date"}
    )
    grin_source_library_bibkey: str | None = field(
        default=None, metadata={"csv": "GRIN Source Library Bibkey", "grin_tsv": "Source Library Bibkey"}
    )
    grin_rubbish: str | None = field(default=None, metadata={"csv": "GRIN Rubbish", "grin_tsv": "Rubbish"})
    grin_allow_download_updated_date: str | None = field(
        default=None, metadata={"csv": "GRIN Allow Download Updated Date", "grin_tsv": "Allow Download Updated Date"}
    )
    grin_viewability_updated_date: str | None = field(
        default=None, metadata={"csv": "GRIN Viewability Updated Date", "grin_tsv": "Viewability Updated Date"}
    )
    enrichment_timestamp: str | None = field(default=None, metadata={"csv": "Enrichment Timestamp"})

    # MARC metadata fields (from METS XML parsing)
    marc_control_number: str | None = field(default=None, metadata={"csv": "MARC Control Number"})
    marc_date_type: str | None = field(default=None, metadata={"csv": "MARC Date Type"})
    marc_date_1: str | None = field(default=None, metadata={"csv": "MARC Date 1"})
    marc_date_2: str | None = field(default=None, metadata={"csv": "MARC Date 2"})
    marc_language: str | None = field(default=None, metadata={"csv": "MARC Language"})
    marc_lccn: str | None = field(default=None, metadata={"csv": "MARC LCCN"})
    marc_lc_call_number: str | None = field(default=None, metadata={"csv": "MARC LC Call Number"})
    marc_isbn: str | None = field(default=None, metadata={"csv": "MARC ISBN"})
    marc_oclc_numbers: str | None = field(default=None, metadata={"csv": "MARC OCLC Numbers"})
    marc_title: str | None = field(default=None, metadata={"csv": "MARC Title"})
    marc_title_remainder: str | None = field(default=None, metadata={"csv": "MARC Title Remainder"})
    marc_author_personal: str | None = field(default=None, metadata={"csv": "MARC Author Personal"})
    marc_author_corporate: str | None = field(default=None, metadata={"csv": "MARC Author Corporate"})
    marc_author_meeting: str | None = field(default=None, metadata={"csv": "MARC Author Meeting"})
    marc_subjects: str | None = field(default=None, metadata={"csv": "MARC Subjects"})
    marc_genres: str | None = field(default=None, metadata={"csv": "MARC Genres"})
    marc_general_note: str | None = field(default=None, metadata={"csv": "MARC General Note"})
    marc_extraction_timestamp: str | None = field(default=None, metadata={"csv": "MARC Extraction Timestamp"})

    # Sync tracking for storage pipeline (status tracked in history table)
    storage_path: str | None = field(default=None, metadata={"csv": "Storage Path"})
    last_etag_check: str | None = field(default=None, metadata={"csv": "Last ETag Check"})
    encrypted_etag: str | None = field(default=None, metadata={"csv": "Encrypted ETag"})
    is_decrypted: bool = field(default=False, metadata={"csv": "Is Decrypted"})
    sync_timestamp: str | None = field(default=None, metadata={"csv": "Sync Timestamp"})

    # Record keeping
    created_at: str | None = field(default=None, metadata={"csv": "Created At"})
    updated_at: str | None = field(default=None, metadata={"csv": "Updated At"})

    @classmethod
    def csv_headers(cls) -> list[str]:
        """Get CSV column headers from field metadata."""
        headers = []
        for f in fields(cls):
            if "csv" in f.metadata:
                headers.append(f.metadata["csv"])
        return headers

    def to_csv_row(self) -> list[str]:
        """Convert to CSV row values from field metadata."""
        values = []
        for f in fields(self):
            if "csv" in f.metadata:
                value = getattr(self, f.name)
                if value is None:
                    values.append("")
                elif f.type is bool:
                    # Handle boolean fields (SQLite returns 0/1 for BOOLEAN)
                    values.append("TRUE" if value else "FALSE")
                else:
                    values.append(str(value))
        return values

    @classmethod
    def build_insert_sql(cls) -> str:
        """Generate INSERT SQL from dataclass fields."""
        field_names = [f.name for f in fields(cls)]
        columns = ", ".join(field_names)
        placeholders = ", ".join("?" for _ in field_names)
        return f"INSERT OR REPLACE INTO books ({columns}) VALUES ({placeholders})"

    @classmethod
    def build_select_sql(cls) -> str:
        """Generate SELECT SQL from dataclass fields."""
        field_names = [f.name for f in fields(cls)]
        columns = ", ".join(field_names)
        return f"SELECT {columns} FROM books"

    @classmethod
    def build_update_enrichment_sql(cls) -> str:
        """Generate UPDATE SQL for enrichment fields."""
        enrichment_fields = [f.name for f in fields(cls) if "grin_tsv" in f.metadata]
        # Build COALESCE clauses for enrichment fields (preserve existing values if None)
        set_clauses = [f"{field} = COALESCE(?, {field})" for field in enrichment_fields]
        # Always update timestamps
        set_clauses.extend(["enrichment_timestamp = ?", "updated_at = ?"])
        set_clause = ", ".join(set_clauses)
        return f"UPDATE books SET {set_clause} WHERE barcode = ?"

    @classmethod
    def build_reset_enrichment_sql(cls) -> str:
        """Generate UPDATE SQL to reset all enrichment fields to NULL."""
        enrichment_fields = [f.name for f in fields(cls) if "grin_tsv" in f.metadata]
        enrichment_fields.append("enrichment_timestamp")
        set_clause = ", ".join(f"{field} = NULL" for field in enrichment_fields)
        set_clause += ", updated_at = ?"
        return f"UPDATE books SET {set_clause} WHERE enrichment_timestamp IS NOT NULL"

    @classmethod
    def build_update_marc_sql(cls) -> str:
        """Generate UPDATE SQL for MARC fields."""
        marc_fields = [f.name for f in fields(cls) if f.name.startswith("marc_")]
        # Build COALESCE clauses for MARC fields (preserve existing values if None)
        set_clauses = [f"{field} = COALESCE(?, {field})" for field in marc_fields]
        # Always update timestamp
        set_clauses.append("updated_at = ?")
        set_clause = ", ".join(set_clauses)
        return f"UPDATE books SET {set_clause} WHERE barcode = ?"

    @classmethod
    def get_field_names(cls) -> list[str]:
        """Get all field names."""
        return [f.name for f in fields(cls)]

    @classmethod
    def get_enrichment_fields(cls) -> list[str]:
        """Get field names that have GRIN TSV mappings."""
        return [f.name for f in fields(cls) if "grin_tsv" in f.metadata]

    @classmethod
    def get_marc_fields(cls) -> list[str]:
        """Get field names for MARC metadata fields."""
        return [f.name for f in fields(cls) if f.name.startswith("marc_")]

    @classmethod
    def get_grin_tsv_column_mapping(cls) -> dict[str, str]:
        """Get mapping from GRIN TSV column names to field names."""
        mapping = {}
        for f in fields(cls):
            if "grin_tsv" in f.metadata:
                mapping[f.metadata["grin_tsv"]] = f.name
        return mapping

    def to_tuple(self) -> tuple:
        """Convert to tuple for SQL operations."""
        return tuple(getattr(self, f.name) for f in fields(self))


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
    SQLite-based progress tracker.
    """

    def __init__(self, db_path: Path | str = "output/default/books.db", cache_size: int = 10000):
        self.db_path = Path(db_path)
        self.session_id = int(datetime.now(UTC).timestamp())
        self._initialized = False
        # Small LRU cache to avoid repeated SQLite queries for same barcodes
        self._known_cache = BoundedSet(max_size=cache_size)
        self._unknown_cache = BoundedSet(max_size=cache_size)
        self._persistent_conn: aiosqlite.Connection | None = None

    async def initialize(self):
        """Initialize persistent connection for optimized database operations."""
        if self._persistent_conn is None:
            self._persistent_conn = await aiosqlite.connect(str(self.db_path))
            await self._configure_connection_for_large_db()

    async def __aenter__(self):
        """Context manager support - ensure connection is initialized."""
        await self.initialize()
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        """Clean up persistent connection."""
        await self.close()

    async def _configure_connection_for_large_db(self):
        """Configure connection optimized for large databases."""
        if not self._persistent_conn:
            return

        await self._persistent_conn.execute("PRAGMA journal_mode=WAL")
        await self._persistent_conn.execute("PRAGMA synchronous=NORMAL")
        await self._persistent_conn.execute("PRAGMA cache_size=50000")  # ~200MB cache
        await self._persistent_conn.execute("PRAGMA temp_store=memory")
        await self._persistent_conn.execute("PRAGMA busy_timeout=5000")
        await self._persistent_conn.execute("PRAGMA mmap_size=268435456")  # 256MB mmap
        await self._persistent_conn.execute("PRAGMA page_size=8192")  # Requires VACUUM to apply

    async def _ensure_connection(self):
        """Ensure persistent connection is initialized and ready."""
        await self.initialize()
        if not self._persistent_conn:
            raise RuntimeError("Failed to initialize database connection")

    async def _execute_query(self, query: str, params: tuple = ()):
        """Execute query using persistent connection."""
        await self._ensure_connection()
        assert self._persistent_conn is not None  # _ensure_connection guarantees this
        return await self._persistent_conn.execute(query, params)

    async def _execute_query_with_commit(self, query: str, params: tuple = ()):
        """Execute query with commit using persistent connection."""
        await self._ensure_connection()
        assert self._persistent_conn is not None  # _ensure_connection guarantees this
        cursor = await self._persistent_conn.execute(query, params)
        await self._persistent_conn.commit()
        return cursor

    async def get_connection(self) -> aiosqlite.Connection:
        """Get the persistent connection for reuse by utility functions.

        Returns:
            The persistent database connection, initialized if needed.
        """
        await self._ensure_connection()
        assert self._persistent_conn is not None
        return self._persistent_conn

    async def init_db(self) -> None:
        """Initialize database schema if not exists."""
        if self._initialized:
            return

        # Ensure directory exists
        self.db_path.parent.mkdir(parents=True, exist_ok=True)

        # Load schema from SQL file
        schema_file = Path(__file__).parent.parent.parent.parent / "docs" / "schema.sql"
        if not schema_file.exists():
            raise FileNotFoundError(f"Database schema file not found: {schema_file}")

        schema_sql = schema_file.read_text(encoding="utf-8")

        await self.initialize()

        if not self._persistent_conn:
            raise RuntimeError("Failed to initialize database connection")

        # Execute the complete schema
        # Split by semicolon and execute each statement separately
        statements = [stmt.strip() for stmt in schema_sql.split(";") if stmt.strip()]

        for statement in statements:
            if statement.strip():
                await self._persistent_conn.execute(statement)

        await self._persistent_conn.commit()

        self._initialized = True

    async def mark_processed(self, barcode: str) -> None:
        """Mark barcode as successfully processed."""
        await self.init_db()

        await self._execute_query_with_commit(
            "INSERT OR REPLACE INTO processed (barcode, timestamp, session_id) VALUES (?, ?, ?)",
            (barcode, datetime.now(UTC).isoformat(), self.session_id),
        )

    async def mark_failed(self, barcode: str, error_message: str = "") -> None:
        """Mark barcode as failed with optional error message."""
        await self.init_db()

        await self._execute_query_with_commit(
            "INSERT OR REPLACE INTO failed (barcode, timestamp, session_id, error_message) VALUES (?, ?, ?, ?)",
            (barcode, datetime.now(UTC).isoformat(), self.session_id, error_message),
        )

    async def is_processed(self, barcode: str) -> bool:
        """Check if barcode was successfully processed."""
        return await self._execute_exists_query("SELECT 1 FROM processed WHERE barcode = ?", (barcode,))

    async def is_failed(self, barcode: str) -> bool:
        """Check if barcode previously failed."""
        return await self._execute_exists_query("SELECT 1 FROM failed WHERE barcode = ?", (barcode,))

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
        """Load known barcodes (processed or failed) from a batch set with caching.

        Args:
            barcodes: Set of barcodes to check

        Returns:
            Set of barcodes that are known (processed or failed)
        """
        if not barcodes:
            return set()

        await self.init_db()

        known_barcodes: set[str] = set()
        uncached_barcodes: set[str] = set()

        # Check cache first
        for barcode in barcodes:
            if barcode in self._known_cache:
                known_barcodes.add(barcode)
            elif barcode not in self._unknown_cache:
                uncached_barcodes.add(barcode)

        # Query database for uncached barcodes if any
        if uncached_barcodes:
            # Create placeholders for IN clause
            placeholders = ",".join("?" * len(uncached_barcodes))
            uncached_list = list(uncached_barcodes)

            # Query both processed and failed tables
            query = f"""
                SELECT DISTINCT barcode FROM (
                    SELECT barcode FROM processed WHERE barcode IN ({placeholders})
                    UNION
                    SELECT barcode FROM failed WHERE barcode IN ({placeholders})
                )
            """

            cursor = await self._execute_query(query, tuple(uncached_list + uncached_list))
            db_known = {row[0] for row in await cursor.fetchall()}

            # Update caches and results
            for barcode in uncached_barcodes:
                if barcode in db_known:
                    self._known_cache.add(barcode)
                    known_barcodes.add(barcode)
                else:
                    self._unknown_cache.add(barcode)

        return known_barcodes

    async def get_processed_count(self) -> int:
        """Get total number of successfully processed barcodes."""
        return await self._execute_count_query("SELECT COUNT(*) FROM processed", ())

    async def get_failed_count(self) -> int:
        """Get total number of failed barcodes."""
        return await self._execute_count_query("SELECT COUNT(*) FROM failed", ())

    async def get_session_stats(self) -> dict:
        """Get statistics for current session."""
        # Current session counts
        session_processed = await self._execute_count_query(
            "SELECT COUNT(*) FROM processed WHERE session_id = ?", (self.session_id,)
        )
        session_failed = await self._execute_count_query(
            "SELECT COUNT(*) FROM failed WHERE session_id = ?", (self.session_id,)
        )

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

        # Get session IDs to keep (most recent N)
        cursor = await self._execute_query(
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
            await self._execute_query(
                f"DELETE FROM processed WHERE session_id NOT IN ({placeholders})", tuple(keep_session_ids)
            )

            # Delete old failed records
            await self._execute_query(
                f"DELETE FROM failed WHERE session_id NOT IN ({placeholders})", tuple(keep_session_ids)
            )

            # Commit the changes
            await self.initialize()
            if self._persistent_conn:
                await self._persistent_conn.commit()

    async def close(self) -> None:
        """Close any open database connections and clean up resources."""
        # Clear caches
        self._known_cache.clear()
        self._unknown_cache.clear()

        # Close persistent connection
        if self._persistent_conn:
            try:
                await self._persistent_conn.close()
            except Exception:
                pass
            self._persistent_conn = None

    async def save_book(self, book: BookRecord, refresh_mode: bool = False) -> None:
        """Save or update a book record in the database using UPSERT."""
        await self.init_db()

        now = datetime.now(UTC).isoformat()

        # Update timestamps if not already set
        if not book.created_at:
            book.created_at = now
        book.updated_at = now

        # Always use UPSERT and change conflict behavior based on mode
        if refresh_mode:
            # Update with COALESCE to preserve existing non-null values
            conflict_action = """DO UPDATE SET
                title = COALESCE(excluded.title, books.title),
                scanned_date = COALESCE(excluded.scanned_date, books.scanned_date),
                converted_date = COALESCE(excluded.converted_date, books.converted_date),
                downloaded_date = COALESCE(excluded.downloaded_date, books.downloaded_date),
                processed_date = COALESCE(excluded.processed_date, books.processed_date),
                analyzed_date = COALESCE(excluded.analyzed_date, books.analyzed_date),
                ocr_date = COALESCE(excluded.ocr_date, books.ocr_date),
                google_books_link = COALESCE(excluded.google_books_link, books.google_books_link),
                processing_request_timestamp = COALESCE(excluded.processing_request_timestamp, books.processing_request_timestamp),
                grin_state = COALESCE(excluded.grin_state, books.grin_state),
                updated_at = excluded.updated_at"""
        else:
            # Normal mode: skip existing records
            conflict_action = "DO NOTHING"

        sql_query = f"""INSERT INTO books (barcode, title, scanned_date, converted_date,
                                      downloaded_date, processed_date, analyzed_date,
                                      ocr_date, google_books_link, processing_request_timestamp,
                                      grin_state, created_at, updated_at)
               VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
               ON CONFLICT(barcode) {conflict_action}"""

        logger.debug(f"save_book SQL: {sql_query}")
        logger.debug(f"save_book values: barcode={book.barcode}, refresh_mode={refresh_mode}")

        await self._execute_query_with_commit(
            sql_query,
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
                book.processing_request_timestamp,
                book.grin_state,
                book.created_at,
                book.updated_at,
            ),
        )

    async def get_book(self, barcode: str) -> BookRecord | None:
        """Retrieve a book record from the database."""
        await self.init_db()

        cursor = await self._execute_query(
            f"{BookRecord.build_select_sql()} WHERE barcode = ?",
            (barcode,),
        )
        row = await cursor.fetchone()
        if row:
            return BookRecord(*row)
        return None

    @retry_database_operation
    async def update_book_enrichment(self, barcode: str, enrichment_data: dict) -> bool:
        """Update enrichment fields for an existing book record."""
        await self.init_db()

        now = datetime.now(UTC).isoformat()

        # Build values tuple for enrichment fields
        enrichment_fields = BookRecord.get_enrichment_fields()
        values = [enrichment_data.get(field) for field in enrichment_fields]
        # Add enrichment_timestamp and updated_at
        values.extend([now, now])
        # Add barcode for WHERE clause
        values.append(barcode)

        await self._execute_query_with_commit(BookRecord.build_update_enrichment_sql(), tuple(values))
        assert self._persistent_conn is not None  # Guaranteed by _execute_query_with_commit
        return self._persistent_conn.total_changes > 0

    @retry_database_operation
    async def update_book_marc_metadata(self, barcode: str, marc_data: dict) -> bool:
        """Update MARC metadata fields for an existing book record."""
        await self.init_db()

        now = datetime.now(UTC).isoformat()

        # Build values tuple for MARC fields
        marc_fields = BookRecord.get_marc_fields()
        values = [marc_data.get(field) for field in marc_fields]
        # Add updated_at timestamp
        values.append(now)
        # Add barcode for WHERE clause
        values.append(barcode)

        await self._execute_query_with_commit(BookRecord.build_update_marc_sql(), tuple(values))
        assert self._persistent_conn is not None  # Guaranteed by _execute_query_with_commit
        return self._persistent_conn.total_changes > 0

    async def get_books_for_enrichment(self, limit: int = 1000) -> list[str]:
        """Get barcodes for books that need enrichment (have no enrichment_timestamp)."""
        await self.init_db()

        cursor = await self._execute_query(
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

        cursor = await self._execute_query(f"{BookRecord.build_select_sql()} ORDER BY barcode")
        rows = await cursor.fetchall()
        return [BookRecord(*row) for row in rows]

    async def get_book_count(self) -> int:
        """Get total number of books in database."""
        return await self._execute_count_query("SELECT COUNT(*) FROM books", ())

    async def get_enriched_book_count(self) -> int:
        """Get count of books with enrichment data."""
        return await self._execute_count_query("SELECT COUNT(*) FROM books WHERE enrichment_timestamp IS NOT NULL", ())

    @retry_database_operation
    async def update_sync_data(self, barcode: str, sync_data: dict) -> bool:
        """Update sync tracking fields for a book record."""
        await self.init_db()

        now = datetime.now(UTC).isoformat()

        query = """
            UPDATE books SET
                storage_path = COALESCE(?, storage_path),
                last_etag_check = COALESCE(?, last_etag_check),
                encrypted_etag = COALESCE(?, encrypted_etag),
                is_decrypted = COALESCE(?, is_decrypted),
                sync_timestamp = ?,
                updated_at = ?
            WHERE barcode = ?
        """
        params = (
            sync_data.get("storage_path"),
            sync_data.get("last_etag_check"),
            sync_data.get("encrypted_etag"),
            sync_data.get("is_decrypted", False),
            sync_data.get("sync_timestamp", now),
            now,
            barcode,
        )

        await self._execute_query_with_commit(query, params)
        assert self._persistent_conn is not None  # Guaranteed by _execute_query_with_commit
        return self._persistent_conn.total_changes > 0

    async def get_books_for_sync(
        self,
        limit: int | None = None,
        status_filter: str | None = None,
        converted_barcodes: set[str] | None = None,
        specific_barcodes: list[str] | None = None,
    ) -> list[str]:
        """Get barcodes for books that need syncing to storage.

        Args:
            limit: Maximum number of books to return (no limit if None)
            status_filter: Optional sync status filter ("pending", "failed", etc.)
            converted_barcodes: Optional set of barcodes known to be converted/ready for download
            specific_barcodes: Optional list of specific barcodes to sync

        Returns:
            List of barcodes that need syncing
        """
        await self.init_db()

        if specific_barcodes:
            # Filter to only the specific barcodes requested
            placeholders = ",".join("?" * len(specific_barcodes))
            base_query = f"""
                SELECT barcode FROM books
                WHERE barcode IN ({placeholders})
            """
            params: list[Any] = list(specific_barcodes)
        elif converted_barcodes:
            # Filter to only books that are known to be converted AND exist in our database
            placeholders = ",".join("?" * len(converted_barcodes))
            base_query = f"""
                SELECT barcode FROM books
                WHERE barcode IN ({placeholders})
            """
            params = list(converted_barcodes)
        else:
            # Original behavior - check all books in database
            base_query = """
                SELECT barcode FROM books
                WHERE 1=1
            """
            params = []

        # Filter by sync status using status history
        if status_filter:
            base_query += """
                AND barcode IN (
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
                )
            """
            params.append(status_filter)
        else:
            # Default: get books that have no sync status OR failed/syncing sync status
            base_query += """
                AND (
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
            """

        base_query += " ORDER BY created_at DESC"
        if limit is not None:
            base_query += " LIMIT ?"
            params.append(limit)

        cursor = await self._execute_query(base_query, tuple(params))
        rows = await cursor.fetchall()
        return [row[0] for row in rows]

    async def get_sync_stats(self) -> dict:
        """Get sync statistics for books using atomic status history.

        Note: This method returns stats for all books in the database since the actual
        download availability is determined dynamically by checking GRIN's _converted endpoint.

        Returns:
            Dictionary with sync statistics
        """
        # Base filters - check all books since availability is determined at download time
        where_clause = "WHERE 1=1"
        params: list[str] = []

        # Total books (potential candidates for download)
        total_converted = await self._execute_count_query(f"SELECT COUNT(*) FROM books b {where_clause}", tuple(params))

        # Get latest sync status for each book using status history
        # Synced books (downloaded status)
        synced_count = await self._execute_count_query(
            f"""
            SELECT COUNT(DISTINCT b.barcode)
            FROM books b
            JOIN book_status_history h ON b.barcode = h.barcode
            {where_clause} AND h.status_type = 'sync' AND h.status_value = 'completed'
            AND h.timestamp = (
                SELECT MAX(timestamp)
                FROM book_status_history h2
                WHERE h2.barcode = h.barcode AND h2.status_type = 'sync'
            )
            """,
            tuple(params),
        )

        # Failed sync books
        failed_count = await self._execute_count_query(
            f"""
            SELECT COUNT(DISTINCT b.barcode)
            FROM books b
            JOIN book_status_history h ON b.barcode = h.barcode
            {where_clause} AND h.status_type = 'sync' AND h.status_value = 'failed'
            AND h.timestamp = (
                SELECT MAX(timestamp)
                FROM book_status_history h2
                WHERE h2.barcode = h.barcode AND h2.status_type = 'sync'
            )
            """,
            tuple(params),
        )

        # Currently syncing
        syncing_count = await self._execute_count_query(
            f"""
            SELECT COUNT(DISTINCT b.barcode)
            FROM books b
            JOIN book_status_history h ON b.barcode = h.barcode
            {where_clause} AND h.status_type = 'sync' AND h.status_value = 'syncing'
            AND h.timestamp = (
                SELECT MAX(timestamp)
                FROM book_status_history h2
                WHERE h2.barcode = h.barcode AND h2.status_type = 'sync'
            )
            """,
            tuple(params),
        )

        # Pending/not started (books with no sync status history or latest status is pending)
        no_sync_history = await self._execute_count_query(
            f"""
            SELECT COUNT(*) FROM books b {where_clause}
            AND b.barcode NOT IN (
                SELECT DISTINCT barcode
                FROM book_status_history
                WHERE status_type = 'sync'
            )
            """,
            tuple(params),
        )

        pending_with_history = await self._execute_count_query(
            f"""
            SELECT COUNT(DISTINCT b.barcode)
            FROM books b
            JOIN book_status_history h ON b.barcode = h.barcode
            {where_clause} AND h.status_type = 'sync' AND h.status_value = 'pending'
            AND h.timestamp = (
                SELECT MAX(timestamp)
                FROM book_status_history h2
                WHERE h2.barcode = h.barcode AND h2.status_type = 'sync'
            )
            """,
            tuple(params),
        )

        pending_count = no_sync_history + pending_with_history

        # Decrypted count
        decrypted_count = await self._execute_count_query(
            f"SELECT COUNT(*) FROM books b {where_clause} AND b.is_decrypted = 1", tuple(params)
        )

        return {
            "total_converted": total_converted,
            "synced": synced_count,
            "failed": failed_count,
            "pending": pending_count,
            "syncing": syncing_count,
            "decrypted": decrypted_count,
        }

    async def get_enrichment_stats(self) -> dict:
        """Get enrichment statistics for books using atomic status history.

        Returns:
            Dictionary with enrichment statistics including:
            - total_books: Total books in database
            - enriched: Books successfully enriched
            - failed: Books with enrichment failures
            - pending: Books pending enrichment
            - in_progress: Books currently being enriched
            - no_enrichment_history: Books without any enrichment attempts
        """
        await self.init_db()

        # Total books in database
        total_books = await self._execute_count_query("SELECT COUNT(*) FROM books", ())

        # Successfully enriched books (latest status is completed)
        enriched_count = await self._execute_count_query(
            """
            SELECT COUNT(DISTINCT b.barcode)
            FROM books b
            JOIN book_status_history h ON b.barcode = h.barcode
            WHERE h.status_type = 'enrichment' AND h.status_value = 'completed'
            AND h.timestamp = (
                SELECT MAX(timestamp)
                FROM book_status_history h2
                WHERE h2.barcode = h.barcode AND h2.status_type = 'enrichment'
            )
            """,
            (),
        )

        # Failed enrichment books (latest status is failed)
        failed_count = await self._execute_count_query(
            """
            SELECT COUNT(DISTINCT b.barcode)
            FROM books b
            JOIN book_status_history h ON b.barcode = h.barcode
            WHERE h.status_type = 'enrichment' AND h.status_value = 'failed'
            AND h.timestamp = (
                SELECT MAX(timestamp)
                FROM book_status_history h2
                WHERE h2.barcode = h.barcode AND h2.status_type = 'enrichment'
            )
            """,
            (),
        )

        # Currently being enriched (latest status is in_progress)
        in_progress_count = await self._execute_count_query(
            """
            SELECT COUNT(DISTINCT b.barcode)
            FROM books b
            JOIN book_status_history h ON b.barcode = h.barcode
            WHERE h.status_type = 'enrichment' AND h.status_value = 'in_progress'
            AND h.timestamp = (
                SELECT MAX(timestamp)
                FROM book_status_history h2
                WHERE h2.barcode = h.barcode AND h2.status_type = 'enrichment'
            )
            """,
            (),
        )

        # Pending enrichment (latest status is pending)
        pending_count = await self._execute_count_query(
            """
            SELECT COUNT(DISTINCT b.barcode)
            FROM books b
            JOIN book_status_history h ON b.barcode = h.barcode
            WHERE h.status_type = 'enrichment' AND h.status_value = 'pending'
            AND h.timestamp = (
                SELECT MAX(timestamp)
                FROM book_status_history h2
                WHERE h2.barcode = h.barcode AND h2.status_type = 'enrichment'
            )
            """,
            (),
        )

        # Books with no enrichment history
        no_enrichment_history = await self._execute_count_query(
            """
            SELECT COUNT(*) FROM books b
            WHERE b.barcode NOT IN (
                SELECT DISTINCT barcode
                FROM book_status_history
                WHERE status_type = 'enrichment'
            )
            """,
            (),
        )

        return {
            "total_books": total_books,
            "enriched": enriched_count,
            "failed": failed_count,
            "pending": pending_count,
            "in_progress": in_progress_count,
            "no_enrichment_history": no_enrichment_history,
        }

    async def get_enrichment_rate_stats(self, time_window_hours: int = 24) -> dict:
        """Get enrichment rate statistics for estimating completion time.

        Args:
            time_window_hours: Time window in hours to calculate rate

        Returns:
            Dictionary with rate statistics
        """
        await self.init_db()

        # Get enrichments completed in the time window
        cutoff_time = datetime.now(UTC) - timedelta(hours=time_window_hours)
        cutoff_iso = cutoff_time.isoformat()

        completed_in_window = await self._execute_count_query(
            """
            SELECT COUNT(*)
            FROM book_status_history
            WHERE status_type = 'enrichment'
            AND status_value = 'completed'
            AND timestamp >= ?
            """,
            (cutoff_iso,),
        )

        # Calculate rate (enrichments per hour)
        rate_per_hour = completed_in_window / time_window_hours if time_window_hours > 0 else 0

        return {
            "completed_in_window": completed_in_window,
            "time_window_hours": time_window_hours,
            "rate_per_hour": rate_per_hour,
            "rate_per_day": rate_per_hour * 24,
        }

    async def get_converted_books_count(self) -> int:
        """Get count of books in converted state (ready for sync)."""
        return await self._execute_count_query("SELECT COUNT(*) FROM books WHERE grin_state = 'converted'", ())

    async def get_latest_status(self, barcode: str, status_type: str) -> str | None:
        """Get the latest status value for a book and status type.

        Args:
            barcode: Book barcode
            status_type: Type of status to retrieve

        Returns:
            Latest status value or None if no status found
        """
        return await self._execute_single_value_query(
            """
            SELECT status_value FROM book_status_history
            WHERE barcode = ? AND status_type = ?
            ORDER BY timestamp DESC, id DESC
            LIMIT 1
            """,
            (barcode, status_type),
        )

    async def get_latest_status_with_metadata(self, barcode: str, status_type: str) -> tuple[str | None, dict | None]:
        """Get the latest status value and metadata for a book and status type.

        Args:
            barcode: Book barcode
            status_type: Type of status to retrieve

        Returns:
            tuple: (status_value, metadata_dict) or (None, None) if no status found
        """
        await self.init_db()
        cursor = await self._execute_query(
            "SELECT status_value, metadata FROM book_status_history "
            "WHERE barcode = ? AND status_type = ? ORDER BY timestamp DESC, id DESC LIMIT 1",
            (barcode, status_type),
        )
        if row := await cursor.fetchone():
            return row[0], json.loads(row[1]) if row[1] else None
        return None, None

    async def get_books_with_latest_status(
        self, status_type: str, status_values: list[str] | None = None, limit: int | None = None
    ) -> list[tuple[str, str]]:
        """Get books with their latest status for a given status type.

        Args:
            status_type: Type of status to filter by
            status_values: Optional list of status values to filter by
            limit: Optional limit on number of results

        Returns:
            List of (barcode, latest_status_value) tuples
        """
        await self.init_db()

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

        cursor = await self._execute_query(base_query, tuple(params))
        rows = await cursor.fetchall()
        return [(row[0], row[1]) for row in rows]

    async def _execute_barcode_query(self, query: str, params: tuple) -> set[str]:
        """Execute a SQL query and return a set of barcodes.

        Args:
            query: SQL query that selects barcodes
            params: Query parameters

        Returns:
            Set of barcodes from query results
        """
        await self.init_db()
        cursor = await self._execute_query(query, params)
        rows = await cursor.fetchall()
        return {row[0] for row in rows}

    async def _execute_single_value_query(self, query: str, params: tuple, default_value=None):
        """Execute a SQL query and return the first column of the first row.

        Args:
            query: SQL query that selects a single value
            params: Query parameters
            default_value: Value to return if no row found (default: None)

        Returns:
            First column value of first row, or default_value if no row found
        """
        await self.init_db()
        cursor = await self._execute_query(query, params)
        row = await cursor.fetchone()
        return row[0] if row else default_value

    async def _execute_count_query(self, query: str, params: tuple) -> int:
        """Execute a COUNT query and return the result as integer.

        Args:
            query: SQL COUNT query
            params: Query parameters

        Returns:
            Count result as integer (0 if no row found)
        """
        result = await self._execute_single_value_query(query, params, 0)
        return result if result is not None else 0

    async def _execute_exists_query(self, query: str, params: tuple) -> bool:
        """Execute an existence query and return True if any row exists.

        Args:
            query: SQL query to check for existence (typically SELECT 1 FROM ...)
            params: Query parameters

        Returns:
            True if at least one row exists, False otherwise
        """
        await self.init_db()
        cursor = await self._execute_query(query, params)
        return await cursor.fetchone() is not None

    async def get_books_by_grin_state(self, grin_state: str) -> set[str]:
        """Get barcodes for books with specific GRIN state.

        Args:
            grin_state: GRIN state to filter by (e.g., 'PREVIOUSLY_DOWNLOADED')

        Returns:
            Set of barcodes with the specified GRIN state
        """
        return await self._execute_barcode_query("SELECT barcode FROM books WHERE grin_state = ?", (grin_state,))

    async def get_books_with_status(self, status_value: str, status_type: str = "sync") -> set[str]:
        """Get barcodes for books with specific status value.

        Args:
            status_value: Status value to filter by (e.g., 'verified_unavailable')
            status_type: Status type to filter by (default: 'sync')

        Returns:
            Set of barcodes with the specified status
        """
        return await self._execute_barcode_query(
            "SELECT DISTINCT barcode FROM book_status_history WHERE status_type = ? AND status_value = ?",
            (status_type, status_value),
        )

    async def get_synced_books(self) -> set[str]:
        """Get set of barcodes that are already synced (completed status in book_status_history)."""
        return await self._execute_barcode_query(
            """
            SELECT DISTINCT barcode
            FROM book_status_history
            WHERE status_type = 'sync' AND status_value = 'completed'
            """,
            (),
        )

    async def get_collection_stats(self) -> dict:
        """Get collection phase statistics."""
        await self.init_db()

        # Total collected books
        total_collected = await self.get_processed_count()

        # Total failed collections
        total_failed = await self.get_failed_count()

        # Get last collection timestamp
        last_collection_time = await self._execute_single_value_query("SELECT MAX(timestamp) FROM processed", ())

        # Calculate collection rate (books per hour in last 24 hours)
        cutoff_time = (datetime.now(UTC) - timedelta(hours=24)).isoformat()
        recent_collected = await self._execute_count_query(
            "SELECT COUNT(*) FROM processed WHERE timestamp >= ?",
            (cutoff_time,),
        )
        collection_rate_per_hour = recent_collected  # Already per 24 hours, so divide by 24 later

        return {
            "total_collected": total_collected,
            "total_failed": total_failed,
            "collection_rate_per_hour": collection_rate_per_hour / 24.0,
            "last_collection_time": last_collection_time,
        }

    async def get_conversion_stats(self) -> dict:
        """Get conversion request statistics from book_status_history."""
        await self.init_db()

        # Get latest status for each book in processing_request type
        status_counts = {}
        cursor = await self._execute_query(
            """
            SELECT h1.status_value, COUNT(*) as count, MAX(h1.timestamp) as last_update
            FROM book_status_history h1
            INNER JOIN (
                SELECT barcode, MAX(timestamp) as max_timestamp, MAX(id) as max_id
                FROM book_status_history
                WHERE status_type = 'processing_request'
                GROUP BY barcode
            ) h2 ON h1.barcode = h2.barcode
                AND h1.timestamp = h2.max_timestamp
                AND h1.id = h2.max_id
            WHERE h1.status_type = 'processing_request'
            GROUP BY h1.status_value
            """,
            (),
        )
        rows = await cursor.fetchall()

        for row in rows:
            status_counts[row[0]] = {"count": row[1], "last_update": row[2]}

        # Calculate totals and rates
        total_requested = sum(data["count"] for data in status_counts.values())
        total_in_queue = status_counts.get("queued", {}).get("count", 0)
        total_completed = status_counts.get("completed", {}).get("count", 0)
        total_failed = status_counts.get("failed", {}).get("count", 0)

        # Calculate request and completion rates (last 24 hours)
        cutoff_time = (datetime.now(UTC) - timedelta(hours=24)).isoformat()

        requests_in_24h = await self._execute_count_query(
            """
            SELECT COUNT(*) FROM book_status_history
            WHERE status_type = 'processing_request' AND status_value = 'requested'
            AND timestamp >= ?
            """,
            (cutoff_time,),
        )

        completions_in_24h = await self._execute_count_query(
            """
            SELECT COUNT(*) FROM book_status_history
            WHERE status_type = 'processing_request' AND status_value = 'completed'
            AND timestamp >= ?
            """,
            (cutoff_time,),
        )

        # Get last request timestamp
        last_request_time = await self._execute_single_value_query(
            """
            SELECT MAX(timestamp) FROM book_status_history
            WHERE status_type = 'processing_request' AND status_value = 'requested'
            """,
            (),
        )

        # Queue capacity estimation (assuming 50K default limit)
        max_queue_capacity = 50000
        estimated_completion_hours = total_in_queue / max(1, completions_in_24h / 24.0) if total_in_queue > 0 else 0

        return {
            "total_requested": total_requested,
            "total_in_queue": total_in_queue,
            "total_completed": total_completed,
            "total_failed": total_failed,
            "conversion_request_rate_per_hour": requests_in_24h / 24.0,
            "conversion_completion_rate_per_hour": completions_in_24h / 24.0,
            "last_request_time": last_request_time,
            "queue_status": {
                "current_queue_size": total_in_queue,
                "max_queue_capacity": max_queue_capacity,
                "estimated_queue_completion_hours": round(estimated_completion_hours, 1),
            },
        }

    async def get_enhanced_sync_stats(self) -> dict:
        """Get enhanced sync/upload statistics from book_status_history."""
        await self.init_db()

        # Get latest sync status for each book
        status_counts = {}
        cursor = await self._execute_query(
            """
            SELECT h1.status_value, COUNT(*) as count, MAX(h1.timestamp) as last_update
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
            GROUP BY h1.status_value
            """,
            (),
        )
        rows = await cursor.fetchall()

        for row in rows:
            status_counts[row[0]] = {"count": row[1], "last_update": row[2]}

        # Calculate totals
        total_attempted = sum(data["count"] for data in status_counts.values())
        total_uploaded = status_counts.get("completed", {}).get("count", 0)
        total_failed = status_counts.get("failed", {}).get("count", 0)
        total_skipped = status_counts.get("skipped", {}).get("count", 0)

        # Calculate upload rate (last 24 hours)
        cutoff_time = (datetime.now(UTC) - timedelta(hours=24)).isoformat()
        uploads_in_24h = await self._execute_count_query(
            """
            SELECT COUNT(*) FROM book_status_history
            WHERE status_type = 'sync' AND status_value = 'completed'
            AND timestamp >= ?
            """,
            (cutoff_time,),
        )

        # Get last sync timestamp
        last_sync_time = await self._execute_single_value_query(
            """
            SELECT MAX(timestamp) FROM book_status_history
            WHERE status_type = 'sync' AND status_value = 'completed'
            """,
            (),
        )

        return {
            "total_attempted": total_attempted,
            "total_uploaded": total_uploaded,
            "total_failed": total_failed,
            "total_skipped": total_skipped,
            "sync_upload_rate_per_hour": uploads_in_24h / 24.0,
            "last_sync_time": last_sync_time,
        }

    async def get_pipeline_summary(self) -> dict:
        """Get overall pipeline health and completion statistics."""
        await self.init_db()

        # Get total books in collection
        total_books = await self.get_book_count()

        # Get books that completed all phases (collected -> converted -> synced)
        fully_completed = await self._execute_count_query(
            """
            SELECT COUNT(DISTINCT b.barcode)
            FROM books b
            JOIN book_status_history sync_hist ON b.barcode = sync_hist.barcode
            WHERE sync_hist.status_type = 'sync'
            AND sync_hist.status_value = 'completed'
            AND sync_hist.timestamp = (
                SELECT MAX(timestamp)
                FROM book_status_history sync_h2
                WHERE sync_h2.barcode = sync_hist.barcode AND sync_h2.status_type = 'sync'
            )
            """,
            (),
        )

        # Calculate completion percentage
        completion_percentage = (fully_completed / max(1, total_books)) * 100

        # Determine bottleneck stage by comparing queue sizes
        conversion_stats = await self.get_conversion_stats()
        sync_stats = await self.get_enhanced_sync_stats()

        # Simple bottleneck detection
        conversion_queue_size = conversion_stats.get("total_in_queue", 0)
        pending_sync = total_books - sync_stats.get("total_attempted", 0)

        if conversion_queue_size > pending_sync:
            bottleneck_stage = "conversion_queue"
        elif pending_sync > conversion_queue_size:
            bottleneck_stage = "sync_pipeline"
        else:
            bottleneck_stage = "balanced"

        # Estimate completion time based on bottleneck
        conversion_rate = conversion_stats.get("conversion_completion_rate_per_hour", 0)
        sync_rate = sync_stats.get("sync_upload_rate_per_hour", 0)

        remaining_work = total_books - fully_completed
        if bottleneck_stage == "conversion_queue" and conversion_rate > 0:
            estimated_completion_hours = remaining_work / conversion_rate
        elif bottleneck_stage == "sync_pipeline" and sync_rate > 0:
            estimated_completion_hours = remaining_work / sync_rate
        else:
            # Use the slower of the two rates
            min_rate = (
                min(conversion_rate, sync_rate)
                if conversion_rate > 0 and sync_rate > 0
                else max(conversion_rate, sync_rate)
            )
            estimated_completion_hours = remaining_work / max(1, min_rate)

        estimated_completion_days = estimated_completion_hours / 24.0

        return {
            "total_books": total_books,
            "fully_completed": fully_completed,
            "completion_percentage": round(completion_percentage, 1),
            "bottleneck_stage": bottleneck_stage,
            "estimated_total_completion_days": round(estimated_completion_days, 1),
        }
