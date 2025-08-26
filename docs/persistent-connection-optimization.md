# Persistent Database Connection Optimization for Sync Pipeline

## Problem Statement

PR #239 introduced persistent database connection infrastructure to optimize performance for large SQLite databases (500MB+). However, the implementation is incomplete:

1. **The sync pipeline never initializes the persistent connection** - SQLiteProgressTracker has the infrastructure but it's not activated
2. **Critical functions bypass the persistent connection entirely** - `commit_book_record_updates()` creates its own connection for every call
3. **Result**: For a 50,000 book sync, we're creating ~100,000+ unnecessary database connections

## Current State Analysis

### What PR #239 Accomplished
- Added `_persistent_conn` attribute to SQLiteProgressTracker
- Created `initialize()`, `__aenter__()`, `__aexit__()` methods for connection lifecycle
- Converted all internal SQLiteProgressTracker methods to use `_execute_query()` helpers
- Added optimized connection settings (WAL mode, 200MB cache, etc.)

### What's Still Missing
- Sync pipeline creates SQLiteProgressTracker but never calls `initialize()`
- `commit_book_record_updates()` creates a fresh connection every time (2x per book)
- No connection reuse between different database operations

## Implementation Plan

* No backwards compatibility
* Use minimal code changes 

### Step 1: Initialize the Persistent Connection

**File: `src/grin_to_s3/sync/pipeline.py`**

In the `SyncPipeline.__init__()` method (around line 218), after creating db_tracker:
```python
# Initialize components
self.db_tracker = SQLiteProgressTracker(self.db_path)
# ADD: Initialize the persistent connection
await self.db_tracker.initialize()
```

Since `__init__` is not async, create an async initialization method:
```python
async def initialize_resources(self):
    """Initialize async resources that require await."""
    await self.db_tracker.initialize()
    # Any other async initialization
```

Call this from the pipeline runner before processing begins.

### Step 2: Expose the Persistent Connection

**File: `src/grin_to_s3/collect_books/models.py`**

Add a method to SQLiteProgressTracker to safely expose the connection:
```python
async def get_connection(self) -> aiosqlite.Connection:
    """Get the persistent connection for reuse by utility functions.
    
    Returns:
        The persistent database connection, initialized if needed.
    """
    await self._ensure_connection()
    assert self._persistent_conn is not None
    return self._persistent_conn
```

### Step 3: Modify commit_book_record_updates to Accept Connection

**File: `src/grin_to_s3/sync/db_updates.py`**

Modify the function signature and implementation:
```python
@retry_database_operation
async def commit_book_record_updates(
    pipeline: "SyncPipeline", 
    barcode: str,
    conn: aiosqlite.Connection 
):
    """Commit all accumulated database record updates for a book.
    
    Args:
        pipeline: The sync pipeline instance
        barcode: Book barcode to commit updates for
        connection: Optional persistent connection to reuse
    """
    record_updates = pipeline.book_record_updates.get(barcode)
    if not record_updates:
        return

    now = datetime.now(UTC).isoformat()
    
    try:
        await _execute_updates(conn, record_updates, barcode, now)
        await conn.commit()
    finally:
        # Clean up after commit
        del pipeline.book_record_updates[barcode]

async def _execute_updates(conn, record_updates, barcode, now):
    """Helper to execute the actual database updates."""
    # Write all status history records
    if record_updates["status_history"]:
        for status_update in record_updates["status_history"]:
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

    # Update books table fields
    if record_updates["books_fields"]:
        sync_data = record_updates["books_fields"]
        await conn.execute(
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
```

### Step 4: Update Task Manager to Pass Connection

**File: `src/grin_to_s3/sync/task_manager.py`**

Update both calls to `commit_book_record_updates` (lines ~218 and ~368):
```python
# Get persistent connection from db_tracker
conn = await pipeline.db_tracker.get_connection()
# Pass connection to avoid creating a new one
await commit_book_record_updates(pipeline, barcode, conn)
```


## Testing Strategy

1. **Performance Testing**
   - Measure connection count before/after changes
   - Compare sync times for large datasets
   - Monitor SQLite lock contention

2. **Unit Tests**
   - Verify persistent connection is reused
   - Test fallback to new connection when none provided
   - Ensure proper cleanup on errors

3. **Integration Tests**
   - Run full sync pipeline with persistent connections
   - Verify data integrity is maintained
   - Test concurrent operations

## Expected Benefits

For a typical 50,000 book sync operation:
- **Before**: ~100,000 database connections (2 per book)
- **After**: 1 persistent connection reused throughout
- **Performance gain**: Significant reduction in I/O overhead, especially for large databases
- **Resource usage**: Lower file descriptor usage, reduced lock contention


