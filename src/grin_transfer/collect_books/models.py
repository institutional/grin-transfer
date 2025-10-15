#!/usr/bin/env python3
"""
Data models for CSV export functionality.

Contains BookRecord and other data classes used in the CSV export system.
"""

import logging
from collections import OrderedDict
from dataclasses import dataclass, field, fields
from datetime import UTC, datetime
from pathlib import Path

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

    async def get_book_count(self) -> int:
        """Get total number of books in database."""
        return await self._execute_count_query("SELECT COUNT(*) FROM books", ())

    async def get_enriched_book_count(self) -> int:
        """Get count of books with enrichment data."""
        return await self._execute_count_query("SELECT COUNT(*) FROM books WHERE enrichment_timestamp IS NOT NULL", ())

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

    async def check_barcodes_exist(self, barcodes: list[str]) -> tuple[set[str], set[str]]:
        """Check which barcodes exist in the database using a temporary table.

        Args:
            barcodes: List of barcodes to check

        Returns:
            Tuple of (existing_barcodes, missing_barcodes) as sets
        """
        if not barcodes:
            return set(), set()

        await self.init_db()

        # Use persistent connection for efficiency
        await self._ensure_connection()
        assert self._persistent_conn is not None

        # Create temp table with requested barcodes
        await self._persistent_conn.execute("CREATE TEMP TABLE requested_barcodes (barcode TEXT PRIMARY KEY)")

        try:
            # Insert barcodes in batches for efficiency (use INSERT OR IGNORE to handle duplicates)
            await self._persistent_conn.executemany(
                "INSERT OR IGNORE INTO requested_barcodes VALUES (?)", [(b,) for b in barcodes]
            )

            # Find existing barcodes via JOIN
            query = """
                SELECT b.barcode
                FROM books b
                INNER JOIN requested_barcodes r ON b.barcode = r.barcode
            """
            existing = set()
            async with self._persistent_conn.execute(query) as cursor:
                async for row in cursor:
                    existing.add(row[0])
        finally:
            # Clean up temp table
            await self._persistent_conn.execute("DROP TABLE requested_barcodes")

        requested = set(barcodes)
        missing = requested - existing
        return existing, missing

    async def create_empty_book_entries(self, barcodes: list[str]) -> None:
        """Create empty database entries for barcodes that don't exist yet.

        Args:
            barcodes: List of barcodes to create empty entries for
        """
        if not barcodes:
            return

        # Create minimal BookRecord entries and save them using existing method
        for barcode in barcodes:
            empty_book = BookRecord(barcode=barcode)
            await self.save_book(empty_book)

    async def get_all_books_csv_data(self) -> list[BookRecord]:
        """Get all books from database as BookRecord objects for CSV export.

        Returns:
            List of BookRecord objects representing all books in the database
        """
        await self.init_db()

        cursor = await self._execute_query(BookRecord.build_select_sql(), ())
        rows = await cursor.fetchall()

        # Convert each row to a BookRecord object
        return [BookRecord(*row) for row in rows]
