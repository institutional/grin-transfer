#!/usr/bin/env python3
"""
Data models for CSV export functionality.

Contains BookRecord and other data classes used in the CSV export system.
"""

import json
import logging
from collections import OrderedDict
from dataclasses import dataclass, field, fields
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

import aiosqlite

from ..database import connect_async

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
    downloaded_date: str | None = field(default=None, metadata={"csv": "Downloaded Date", "grin_tsv": "Downloaded Date"})
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

    # Export tracking
    csv_exported: str | None = field(default=None, metadata={"csv": "CSV Exported"})
    csv_updated: str | None = field(default=None, metadata={"csv": "CSV Updated"})

    # Sync tracking for storage pipeline (status tracked in history table)
    storage_type: str | None = field(default=None, metadata={"csv": "Storage Type"})
    storage_path: str | None = field(default=None, metadata={"csv": "Storage Path"})
    storage_decrypted_path: str | None = field(default=None, metadata={"csv": "Storage Decrypted Path"})
    last_etag_check: str | None = field(default=None, metadata={"csv": "Last ETag Check"})
    encrypted_etag: str | None = field(default=None, metadata={"csv": "Encrypted ETag"})
    is_decrypted: bool = field(default=False, metadata={"csv": "Is Decrypted"})
    sync_timestamp: str | None = field(default=None, metadata={"csv": "Sync Timestamp"})
    sync_error: str | None = field(default=None, metadata={"csv": "Sync Error"})

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

                # Use marc_title as fallback when title is empty
                if f.name == "title" and (not value or value == ""):
                    marc_title = getattr(self, "marc_title", None)
                    if marc_title:
                        # Clean up marc_title (remove trailing slash and whitespace)
                        value = marc_title.rstrip("/ ")

                if value is None:
                    values.append("")
                elif isinstance(value, bool):
                    values.append(str(value) if value else "")
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
        enrichment_fields.extend(["enrichment_timestamp", "updated_at"])
        set_clause = ", ".join(f"{field} = ?" for field in enrichment_fields)
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
        marc_fields.append("updated_at")
        set_clause = ", ".join(f"{field} = ?" for field in marc_fields)
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

        # Load schema from SQL file
        schema_file = Path(__file__).parent.parent.parent.parent / "docs" / "schema.sql"
        if not schema_file.exists():
            raise FileNotFoundError(f"Database schema file not found: {schema_file}")

        schema_sql = schema_file.read_text(encoding="utf-8")

        async with connect_async(self.db_path) as db:
            # Execute the complete schema
            # Split by semicolon and execute each statement separately
            statements = [stmt.strip() for stmt in schema_sql.split(";") if stmt.strip()]

            for statement in statements:
                if statement.strip():
                    await db.execute(statement)

            await db.commit()

        self._initialized = True

    async def mark_processed(self, barcode: str) -> None:
        """Mark barcode as successfully processed."""
        await self.init_db()

        async with connect_async(self.db_path) as db:
            await db.execute(
                "INSERT OR REPLACE INTO processed (barcode, timestamp, session_id) VALUES (?, ?, ?)",
                (barcode, datetime.now(UTC).isoformat(), self.session_id),
            )
            await db.commit()

    async def mark_failed(self, barcode: str, error_message: str = "") -> None:
        """Mark barcode as failed with optional error message."""
        await self.init_db()

        async with connect_async(self.db_path) as db:
            await db.execute(
                "INSERT OR REPLACE INTO failed (barcode, timestamp, session_id, error_message) VALUES (?, ?, ?, ?)",
                (barcode, datetime.now(UTC).isoformat(), self.session_id, error_message),
            )
            await db.commit()

    async def is_processed(self, barcode: str) -> bool:
        """Check if barcode was successfully processed."""
        await self.init_db()

        async with connect_async(self.db_path) as db:
            cursor = await db.execute("SELECT 1 FROM processed WHERE barcode = ?", (barcode,))
            return await cursor.fetchone() is not None

    async def is_failed(self, barcode: str) -> bool:
        """Check if barcode previously failed."""
        await self.init_db()

        async with connect_async(self.db_path) as db:
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

        async with connect_async(self.db_path) as db:
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

        async with connect_async(self.db_path) as db:
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

        async with connect_async(self.db_path) as db:
            cursor = await db.execute("SELECT COUNT(*) FROM processed")
            row = await cursor.fetchone()
            return row[0] if row else 0

    async def get_failed_count(self) -> int:
        """Get total number of failed barcodes."""
        await self.init_db()

        async with connect_async(self.db_path) as db:
            cursor = await db.execute("SELECT COUNT(*) FROM failed")
            row = await cursor.fetchone()
            return row[0] if row else 0

    async def get_session_stats(self) -> dict:
        """Get statistics for current session."""
        await self.init_db()

        async with connect_async(self.db_path) as db:
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

        async with connect_async(self.db_path) as db:
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

        # Update timestamps if not already set
        if not book.created_at:
            book.created_at = now
        book.updated_at = now

        async with connect_async(self.db_path) as db:
            await db.execute(BookRecord.build_insert_sql(), book.to_tuple())
            await db.commit()

    async def get_book(self, barcode: str) -> BookRecord | None:
        """Retrieve a book record from the database."""
        await self.init_db()

        async with connect_async(self.db_path) as db:
            cursor = await db.execute(
                f"{BookRecord.build_select_sql()} WHERE barcode = ?",
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

        # Build values tuple for enrichment fields
        enrichment_fields = BookRecord.get_enrichment_fields()
        values = [enrichment_data.get(field) for field in enrichment_fields]
        # Add enrichment_timestamp and updated_at
        values.extend([now, now])
        # Add barcode for WHERE clause
        values.append(barcode)

        async with connect_async(self.db_path) as db:
            await db.execute(BookRecord.build_update_enrichment_sql(), values)
            rows_affected = db.total_changes
            await db.commit()
            return rows_affected > 0

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

        async with connect_async(self.db_path) as db:
            await db.execute(BookRecord.build_update_marc_sql(), values)
            rows_affected = db.total_changes
            await db.commit()
            return rows_affected > 0

    async def get_books_for_enrichment(self, limit: int = 1000) -> list[str]:
        """Get barcodes for books that need enrichment (no enrichment_timestamp)."""
        await self.init_db()

        async with connect_async(self.db_path) as db:
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

        async with connect_async(self.db_path) as db:
            cursor = await db.execute(f"{BookRecord.build_select_sql()} ORDER BY barcode")
            rows = await cursor.fetchall()
            return [BookRecord(*row) for row in rows]

    async def get_book_count(self) -> int:
        """Get total number of books in database."""
        await self.init_db()

        async with connect_async(self.db_path) as db:
            cursor = await db.execute("SELECT COUNT(*) FROM books")
            row = await cursor.fetchone()
            return row[0] if row else 0

    async def get_enriched_book_count(self) -> int:
        """Get count of books with enrichment data."""
        await self.init_db()

        async with connect_async(self.db_path) as db:
            cursor = await db.execute("""
                SELECT COUNT(*) FROM books
                WHERE enrichment_timestamp IS NOT NULL
            """)
            row = await cursor.fetchone()
            return row[0] if row else 0

    async def update_sync_data(self, barcode: str, sync_data: dict) -> bool:
        """Update sync tracking fields for a book record."""
        await self.init_db()

        now = datetime.now(UTC).isoformat()

        async with connect_async(self.db_path) as db:
            await db.execute(
                """
                UPDATE books SET
                    storage_type = ?, storage_path = ?, storage_decrypted_path = ?,
                    last_etag_check = ?, encrypted_etag = ?, is_decrypted = ?,
                    sync_timestamp = ?, sync_error = ?,
                    updated_at = ?
                WHERE barcode = ?
            """,
                (
                    sync_data.get("storage_type"),
                    sync_data.get("storage_path"),
                    sync_data.get("storage_decrypted_path"),
                    sync_data.get("last_etag_check"),
                    sync_data.get("encrypted_etag"),
                    sync_data.get("is_decrypted", False),
                    sync_data.get("sync_timestamp", now),
                    sync_data.get("sync_error"),
                    now,
                    barcode,
                ),
            )

            rows_affected = db.total_changes
            await db.commit()
            return rows_affected > 0

    async def get_books_for_sync(
        self,
        storage_type: str,
        limit: int = 100,
        status_filter: str | None = None,
        converted_barcodes: set[str] | None = None,
        specific_barcodes: list[str] | None = None,
    ) -> list[str]:
        """Get barcodes for books that need syncing to storage.

        Args:
            storage_type: Target storage type ("r2", "minio", "s3", "local")
            limit: Maximum number of books to return
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

        # Optionally filter by storage type (for re-syncing)
        if storage_type:
            base_query += " AND (storage_type IS NULL OR storage_type = ?)"
            params.append(storage_type)

        base_query += " ORDER BY created_at DESC LIMIT ?"
        params.append(limit)

        async with connect_async(self.db_path) as db:
            cursor = await db.execute(base_query, params)
            rows = await cursor.fetchall()
            return [row[0] for row in rows]

    async def get_sync_stats(self, storage_type: str | None = None) -> dict:
        """Get sync statistics for books using atomic status history.

        Note: This method returns stats for all books in the database since the actual
        download availability is determined dynamically by checking GRIN's _converted endpoint.

        Args:
            storage_type: Optional storage type filter

        Returns:
            Dictionary with sync statistics
        """
        await self.init_db()

        async with connect_async(self.db_path) as db:
            # Base filters - check all books since availability is determined at download time
            where_clause = "WHERE 1=1"
            params = []

            if storage_type:
                where_clause += " AND (b.storage_type IS NULL OR b.storage_type = ?)"
                params.append(storage_type)

            # Total books (potential candidates for download)
            cursor = await db.execute(f"SELECT COUNT(*) FROM books b {where_clause}", params)
            row = await cursor.fetchone()
            total_converted = row[0] if row else 0

            # Get latest sync status for each book using status history
            # Synced books (downloaded status)
            cursor = await db.execute(
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
                params,
            )
            row = await cursor.fetchone()
            synced_count = row[0] if row else 0

            # Failed sync books
            cursor = await db.execute(
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
                params,
            )
            row = await cursor.fetchone()
            failed_count = row[0] if row else 0

            # Currently syncing
            cursor = await db.execute(
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
                params,
            )
            row = await cursor.fetchone()
            syncing_count = row[0] if row else 0

            # Pending/not started (books with no sync status history or latest status is pending)
            cursor = await db.execute(
                f"""
                SELECT COUNT(*) FROM books b {where_clause}
                AND b.barcode NOT IN (
                    SELECT DISTINCT barcode
                    FROM book_status_history
                    WHERE status_type = 'sync'
                )
            """,
                params,
            )
            row = await cursor.fetchone()
            no_sync_history = row[0] if row else 0

            cursor = await db.execute(
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
                params,
            )
            row = await cursor.fetchone()
            pending_with_history = row[0] if row else 0

            pending_count = no_sync_history + pending_with_history

            # Decrypted count
            cursor = await db.execute(f"SELECT COUNT(*) FROM books b {where_clause} AND b.is_decrypted = 1", params)
            row = await cursor.fetchone()
            decrypted_count = row[0] if row else 0

            return {
                "total_converted": total_converted,
                "synced": synced_count,
                "failed": failed_count,
                "pending": pending_count,
                "syncing": syncing_count,
                "decrypted": decrypted_count,
                "storage_type": storage_type,
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

        async with connect_async(self.db_path) as db:
            # Total books in database
            cursor = await db.execute("SELECT COUNT(*) FROM books")
            row = await cursor.fetchone()
            total_books = row[0] if row else 0

            # Successfully enriched books (latest status is completed)
            cursor = await db.execute(
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
                """
            )
            row = await cursor.fetchone()
            enriched_count = row[0] if row else 0

            # Failed enrichment books (latest status is failed)
            cursor = await db.execute(
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
                """
            )
            row = await cursor.fetchone()
            failed_count = row[0] if row else 0

            # Currently being enriched (latest status is in_progress)
            cursor = await db.execute(
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
                """
            )
            row = await cursor.fetchone()
            in_progress_count = row[0] if row else 0

            # Pending enrichment (latest status is pending)
            cursor = await db.execute(
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
                """
            )
            row = await cursor.fetchone()
            pending_count = row[0] if row else 0

            # Books with no enrichment history
            cursor = await db.execute(
                """
                SELECT COUNT(*) FROM books b
                WHERE b.barcode NOT IN (
                    SELECT DISTINCT barcode
                    FROM book_status_history
                    WHERE status_type = 'enrichment'
                )
                """
            )
            row = await cursor.fetchone()
            no_enrichment_history = row[0] if row else 0

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

        async with connect_async(self.db_path) as db:
            # Get enrichments completed in the time window
            from datetime import datetime, timedelta

            cutoff_time = datetime.now(UTC) - timedelta(hours=time_window_hours)
            cutoff_iso = cutoff_time.isoformat()

            cursor = await db.execute(
                """
                SELECT COUNT(*)
                FROM book_status_history
                WHERE status_type = 'enrichment'
                AND status_value = 'completed'
                AND timestamp >= ?
                """,
                (cutoff_iso,),
            )
            row = await cursor.fetchone()
            completed_in_window = row[0] if row else 0

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
        await self.init_db()

        async with connect_async(self.db_path) as db:
            cursor = await db.execute("""
                SELECT COUNT(*) FROM books
                WHERE grin_state = 'converted'
            """)
            row = await cursor.fetchone()
            return row[0] if row else 0

    async def add_status_change(
        self,
        barcode: str,
        status_type: str,
        status_value: str,
        session_id: str | None = None,
        metadata: dict | None = None,
    ) -> bool:
        """Atomically record a status change for a book.

        Args:
            barcode: Book barcode
            status_type: Type of status ("processing_request", "sync", "enrichment", etc.)
            status_value: New status value
            session_id: Optional session identifier for batch tracking
            metadata: Optional metadata as dict (will be JSON encoded)

        Returns:
            True if status was recorded successfully
        """
        await self.init_db()

        now = datetime.now(UTC).isoformat()
        metadata_json = json.dumps(metadata) if metadata else None

        async with connect_async(self.db_path) as db:
            await db.execute(
                """
                INSERT INTO book_status_history
                (barcode, status_type, status_value, timestamp, session_id, metadata)
                VALUES (?, ?, ?, ?, ?, ?)
                """,
                (barcode, status_type, status_value, now, session_id, metadata_json),
            )

            # Update the updated_at timestamp in books table
            await db.execute("UPDATE books SET updated_at = ? WHERE barcode = ?", (now, barcode))

            await db.commit()
            return True

    async def get_latest_status(self, barcode: str, status_type: str) -> str | None:
        """Get the latest status value for a book and status type.

        Args:
            barcode: Book barcode
            status_type: Type of status to retrieve

        Returns:
            Latest status value or None if no status found
        """
        await self.init_db()

        async with connect_async(self.db_path) as db:
            cursor = await db.execute(
                """
                SELECT status_value FROM book_status_history
                WHERE barcode = ? AND status_type = ?
                ORDER BY timestamp DESC, id DESC
                LIMIT 1
                """,
                (barcode, status_type),
            )
            row = await cursor.fetchone()
            return row[0] if row else None

    async def get_latest_status_with_metadata(self, barcode: str, status_type: str) -> tuple[str | None, dict | None]:
        """Get the latest status value and metadata for a book and status type.

        Args:
            barcode: Book barcode
            status_type: Type of status to retrieve

        Returns:
            tuple: (status_value, metadata_dict) or (None, None) if no status found
        """
        await self.init_db()
        async with connect_async(self.db_path) as db:
            cursor = await db.execute(
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

        async with connect_async(self.db_path) as db:
            cursor = await db.execute(base_query, params)
            rows = await cursor.fetchall()
            return [(row[0], row[1]) for row in rows]
