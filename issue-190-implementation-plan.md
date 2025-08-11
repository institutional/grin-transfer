# Implementation Plan for Issue #190: Implement `--queue previous`

## Overview
This feature adds support for processing books with `PREVIOUSLY_DOWNLOADED` status that were removed from GRIN but may now be available again. The implementation will be broken into 5 atomic PRs to ensure clean, testable changes.

## Background
Following issue #188 which made `--queue` a required parameter for `sync pipeline`, this implements the `previous` queue option to handle books that were previously downloaded but may have been removed and re-added to GRIN.

## Atomic PR Breakdown

### PR 1: Add database schema for `verified_unavailable` status
**Scope:** Database infrastructure for tracking unavailable books

**Files to modify:**
- `docs/schema.sql` - Add comment documenting the new status value
- `src/grin_to_s3/database_utils.py` - Add migration/validation for new status
- `src/tests/grin_to_s3/sync/test_status_history.py` - Tests for new status

**Changes:**
- Document `verified_unavailable` as a valid status_value in book_status_history table
- No schema changes needed (status_value is already TEXT)
- Add constants for the new status value
- Update status validation functions

**Testing:**
- Unit tests verifying the new status can be written and queried
- Integration test confirming status transitions

---

### PR 2: Add in_process queue fetching and filtering ✅ **COMPLETED**
**Scope:** Infrastructure for fetching and caching GRIN's in_process queue

**Files modified:**
- `src/grin_to_s3/processing.py` - Added reusable `get_in_process_set()` function with 1-hour caching
- `src/grin_to_s3/processing.py` - Refactored existing `ProcessingClient.get_in_process_books()` and `Monitor.get_in_process_books()` to use new function
- `src/grin_to_s3/common.py` - Added `BarcodeSet` type alias for consistent barcode handling
- `src/tests/grin_to_s3/processing/test_processing_caching.py` - Added comprehensive test file for caching behavior
- `src/tests/grin_to_s3/processing/test_processing_data_integrity.py` - Removed redundant error handling test

**PR:** https://github.com/instdin/grin-to-s3-wip/pull/194

**Key implementation changes:**
- **Centralized caching**: Simple global dict with 1-hour TTL instead of duplicate implementations
- **Type safety**: Added `BarcodeSet = set[str]` and `CacheEntry = tuple[BarcodeSet, float]` type aliases
- **Error propagation**: Removed exception swallowing - errors now propagate to callers for proper handling
- **Code consolidation**: Unified `ProcessingClient` and `ProcessingMonitor` in_process methods to use shared function
- **Clean testing**: Comprehensive test suite with cache behavior, TTL, concurrent access, and error scenarios

**Actual implementation:**
```python
# Type aliases in common.py
type BarcodeSet = set[str]

# Cache implementation in processing.py  
type CacheEntry = tuple[BarcodeSet, float]
_in_process_cache: dict[str, CacheEntry] = {}

async def get_in_process_set(grin_client, library_directory: str) -> BarcodeSet:
    current_time = time.time()
    cache_key = library_directory
    
    # Check cache (1 hour TTL)
    if cache_key in _in_process_cache:
        books, cached_time = _in_process_cache[cache_key]
        if current_time - cached_time < 3600:
            logger.debug(f"Using cached in_process data for {library_directory}")
            return books

    # Fetch and cache
    response_text = await grin_client.fetch_resource(library_directory, "_in_process?format=text")
    lines = response_text.strip().split("\n")
    books = {line.strip() for line in lines if line.strip()}
    
    _in_process_cache[cache_key] = (books, current_time)
    return books
```

**Testing completed:**
- Cache miss/hit/expiry scenarios with TTL boundary testing
- Multiple library directory isolation  
- Error propagation verification (no more silent failures)
- Concurrent request handling and whitespace filtering
- Debug logging for cache hits

---

### PR 3: Implement previous queue logic
**Scope:** Core logic for identifying and filtering previous queue books

**Files to modify:**
- `src/grin_to_s3/sync/utils.py` - Implement `get_books_from_queue()` for "previous" queue
- `src/grin_to_s3/collect_books/models.py` - Add query methods for PREVIOUSLY_DOWNLOADED books
- `src/tests/grin_to_s3/sync/test_previous_queue.py` - New test file
- `src/tests/grin_to_s3/processing/test_processing_monitor_status.py` - Commit uncommitted file from PR 2

**Changes:**
```python
async def get_books_from_queue(grin_client, library_directory: str, queue_name: str, db_tracker=None) -> set[str]:
    # ... existing code ...
    elif queue_name == "previous":
        # Get books with PREVIOUSLY_DOWNLOADED status
        previously_downloaded = await db_tracker.get_books_by_grin_state("PREVIOUSLY_DOWNLOADED")
        
        # Get in_process queue for filtering (cache irrelevant for single call per sync)
        in_process = await get_in_process_set(grin_client, library_directory)
        
        # Get verified_unavailable books for filtering
        verified_unavailable = await db_tracker.get_books_with_status("verified_unavailable")
        
        # Return filtered set
        return previously_downloaded - in_process - verified_unavailable
```

**Database queries needed:**
```sql
-- Get books with PREVIOUSLY_DOWNLOADED state
SELECT barcode FROM books WHERE grin_state = 'PREVIOUSLY_DOWNLOADED';

-- Get books marked as verified_unavailable
SELECT DISTINCT barcode FROM book_status_history 
WHERE status_type = 'sync' AND status_value = 'verified_unavailable';
```

**Cache behavior and timing considerations:**
- `get_books_from_queue()` returns a **static snapshot** at sync start time
- Books currently `in_process` are filtered out and will be available in subsequent sync runs
- The 1-hour cache in `get_in_process_set()` has minimal impact since this is typically a single call per sync execution
- Cache purpose: Optimize **within-execution** repeated calls during monitoring operations, not cross-execution persistence

**Static snapshot implications:**
- A book that is `in_process` at sync start will be excluded from this run
- If that book completes processing during the sync (becoming `converted`), it remains excluded until the next sync run
- This matches the behavior of other queue types and keeps implementation simple

**Testing:**
- Test filtering logic with various combinations
- Database query performance tests
- Edge cases (empty sets, all filtered out, etc.)
- Test behavior when books transition states during sync (documentation only, no code changes needed)

---

### PR 4: Add HEAD request and ETag checking for previous queue
**Scope:** Archive availability checking and ETag comparison

**Files to modify:**
- `src/grin_to_s3/sync/operations.py` - Add archive availability checking
- `src/grin_to_s3/sync/pipeline.py` - Integrate HEAD checking into sync flow
- `src/tests/grin_to_s3/sync/test_archive_checking.py` - New test file

**Changes:**
```python
async def check_archive_availability(barcode: str, grin_client, library_directory: str) -> dict:
    """Check if archive exists and get its ETag.
    
    Returns:
        dict with keys:
        - available: bool (True if 200, False if 404)
        - etag: str | None (ETag if available)
        - needs_download: bool (True if ETag differs from stored)
    """
    # HEAD request implementation
    pass
```

**Integration in pipeline:**
- For previous queue books, check availability before download
- Compare ETags to determine if re-download needed
- Skip if ETag matches (already have latest version)

**Testing:**
- Mock HEAD responses (200 with ETag, 404, errors)
- ETag comparison logic
- Integration with existing sync flow

---

### PR 5: Add conversion request handling for missing archives
**Scope:** Request conversion for 404 archives and handle responses

**Files to modify:**
- `src/grin_to_s3/sync/pipeline.py` - Add conversion request logic
- `src/grin_to_s3/processing.py` - Export request functions for reuse
- `src/grin_to_s3/sync/conversion_handler.py` - New file for conversion logic
- `src/tests/grin_to_s3/sync/test_conversion_requests.py` - New test file

**Changes:**
```python
async def handle_missing_archive(barcode: str, grin_client, db_tracker, request_limit: int) -> str:
    """Handle archive that returned 404.
    
    Returns status:
    - "requested": Conversion requested successfully
    - "in_process": Already being processed
    - "unavailable": Cannot be converted
    - "limit_reached": At request limit
    """
    # Check if under request limit
    if requests_made >= request_limit:
        return "limit_reached"
    
    # Request conversion
    try:
        result = await request_conversion(barcode)
        if result == "Success":
            return "requested"
        elif "already in process" in result.lower():
            logger.warning(f"Book {barcode} already in process (shouldn't happen with filtering)")
            return "in_process"
        else:
            # Mark as verified_unavailable
            await db_tracker.mark_verified_unavailable(barcode)
            return "unavailable"
    except Exception as e:
        logger.error(f"Conversion request failed for {barcode}: {e}")
        return "unavailable"
```

**Key behaviors:**
- Respect conversion request limits (track count)
- Update database status for unavailable books
- Comprehensive error handling and logging
- Rate limiting for conversion requests

**Testing:**
- Mock conversion responses (success, already in process, failures)
- Request limit enforcement
- Database status updates
- Error scenarios

---

## Implementation Order and Dependencies

1. **PR 1** - Database infrastructure (no dependencies)
2. **PR 2** - In-process queue fetching (depends on PR 1)
3. **PR 3** - Previous queue logic (depends on PR 2)
4. **PR 4** - HEAD/ETag checking (depends on PR 3)
5. **PR 5** - Conversion requests (depends on PR 4)

## Key Design Decisions

### Why filter in_process books?
Books already in GRIN's processing queue don't need to be checked or re-requested, avoiding unnecessary API calls and potential errors.

### Why track verified_unavailable?
Some books genuinely cannot be converted (copyright, technical issues, etc.). Tracking these prevents repeated failed attempts.

### Why use HEAD requests?
HEAD requests are lightweight and provide ETag without downloading the entire archive, saving bandwidth and time.

### Why atomic PRs?
- Easier code review
- Simpler testing
- Can be rolled back individually if issues arise
- Clear separation of concerns

## Success Metrics

- Previous queue returns correct set of barcodes
- No duplicate conversion requests for in_process books
- Verified_unavailable books are not repeatedly attempted
- ETag matching prevents unnecessary re-downloads
- Conversion requests respect rate limits

## Testing Strategy

### Unit Tests
- Each new function thoroughly tested
- Mock external dependencies (GRIN API)
- Edge cases and error conditions

### Integration Tests
- Full previous queue flow
- Database state transitions
- Interaction with existing sync pipeline

### Manual Testing Checklist
- [ ] Previous queue with no books
- [ ] Previous queue with all books filtered out
- [ ] Mix of available and unavailable archives
- [ ] ETag matching scenarios
- [ ] Conversion request limit enforcement
- [ ] Error recovery and logging

## Rollback Plan

Each PR can be reverted independently without affecting the others, except in dependency order (must revert PR 5 before PR 4, etc.).

## Future Enhancements

- Add metrics/monitoring for previous queue processing
- Consider batch HEAD requests if GRIN supports it
- Add retry logic for transient failures
- Consider persistent cache for in_process queue