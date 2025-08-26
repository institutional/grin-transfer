# Enrichment Pipeline Refactoring Plan

## Goal
Reduce enrichment pipeline code by 25-35% while maintaining functionality and improving clarity.

## Current State
- **Main pipeline**: 863 lines (`grin_enrichment.py`)
- **Tests**: Multiple test files with extensive edge case coverage
- **Issues**:
  - 16 try/except blocks with nested error handling
  - Complex batch optimization with leftover state management
  - Mixed responsibilities in single class
  - Excessive defensive programming
  - Overly comprehensive test coverage for rare edge cases

## Target Architecture

### Core Principles
1. **Keep what works**: Dynamic URL batch sizing (important for performance)
2. **Remove complexity**: Leftover barcode management, nested error handling
3. **Extract pure functions**: TSV parsing, batch calculations
4. **Simplify tests**: Focus on happy-path and common failures only

## Refactoring Tasks

### 1. Simplify Main Processing Loop (-120 lines)

**Current**: Complex leftover management across batches
```python
# Lines 77, 209-254, 542-647
self.leftover_barcodes = []
fetch_size = self._calculate_optimal_fetch_size(target_batch_size)
processing_barcodes, new_leftovers = self._prepare_combined_batch(fresh_barcodes)
```

**New**: Simple batch processing
```python
async def run_enrichment(self, limit=None, reset=False):
    """Run enrichment pipeline with simple batch processing."""
    if reset:
        await self.reset_enrichment_data()
    
    processed = 0
    while limit is None or processed < limit:
        # Get batch from database
        batch_size = min(self.batch_size, limit - processed) if limit else self.batch_size
        barcodes = await self.db_tracker.get_books_for_enrichment(batch_size)
        
        if not barcodes:
            break
        
        # Create URL-sized batches dynamically for this set
        url_batches = self._create_url_batches(barcodes)
        
        # Process concurrently with semaphore
        tasks = [self._fetch_and_update(batch) for batch in url_batches]
        results = await asyncio.gather(*tasks, return_exceptions=True)
        
        # Update progress
        enriched = sum(r for r in results if isinstance(r, int))
        processed += len(barcodes)
        
        logger.info(f"Processed {len(barcodes)} books, {enriched} enriched")
```

### 2. Extract TSV Parsing (-50 lines)

**Current**: Inline parsing with defensive handling (lines 289-347)

**New**: Pure function in separate module
```python
# grin_to_s3/metadata/tsv_parser.py
def parse_grin_tsv(tsv_text: str) -> dict[str, dict]:
    """Parse GRIN TSV response into barcode -> metadata dict.
    
    Args:
        tsv_text: TSV response from GRIN API
        
    Returns:
        Dict mapping barcode to metadata fields
    """
    lines = tsv_text.strip().split("\n")
    if len(lines) < 2:
        return {}
    
    headers = lines[0].split("\t")
    mapping = BookRecord.get_grin_tsv_column_mapping()
    results = {}
    
    for line in lines[1:]:
        values = line.split("\t")
        if not values or not values[0]:
            continue
            
        barcode = values[0]
        # Build metadata dict using column mapping
        metadata = {}
        for tsv_col, field_name in mapping.items():
            if tsv_col in headers:
                idx = headers.index(tsv_col)
                metadata[field_name] = values[idx] if idx < len(values) else ""
            else:
                metadata[field_name] = ""
        
        results[barcode] = metadata
    
    return results
```

### 3. Streamline Error Handling (-80 lines)

**Current**: Nested try/except blocks (lines 400-459)

**New**: Single-level with retry decorator
```python
from tenacity import retry, stop_after_attempt, wait_exponential

@retry(
    stop=stop_after_attempt(3),
    wait=wait_exponential(min=2, max=8),
    reraise=True
)
async def _fetch_grin_batch(self, barcodes: list[str]) -> str:
    """Fetch TSV data from GRIN with automatic retry."""
    barcode_param = " ".join(barcodes)
    url = f"_barcode_search?execute_query=true&format=text&mode=full&barcodes={barcode_param}"
    return await self.grin_client.fetch_resource(self.directory, url)

async def _fetch_and_update(self, barcodes: list[str]) -> int:
    """Fetch metadata and update database for a batch.
    
    Returns:
        Number of successfully enriched books
    """
    try:
        # Fetch with automatic retry
        tsv_data = await self._fetch_grin_batch(barcodes)
        
        # Parse response
        metadata = parse_grin_tsv(tsv_data)
        
        # Update database
        enriched_count = 0
        for barcode, data in metadata.items():
            if data and await self.db_tracker.update_book_enrichment(barcode, data):
                enriched_count += 1
                self.process_summary_stage.increment_items(processed=1, successful=1)
            else:
                self.process_summary_stage.increment_items(processed=1)
        
        # Mark missing barcodes as attempted
        missing = set(barcodes) - set(metadata.keys())
        for barcode in missing:
            await self.db_tracker.update_book_enrichment(barcode, {})
            self.process_summary_stage.increment_items(processed=1)
        
        return enriched_count
        
    except Exception as e:
        logger.error(f"Batch of {len(barcodes)} failed: {e}")
        # Mark all as attempted to prevent infinite retries
        for barcode in barcodes:
            await self.db_tracker.update_book_enrichment(barcode, {})
            self.process_summary_stage.increment_items(processed=1, failed=1)
        return 0
```

### 4. Simplify URL Batch Creation (-30 lines)

**Current**: Complex optimal batch composition (lines 134-180)

**New**: Simple dynamic batching
```python
def _create_url_batches(self, barcodes: list[str]) -> list[list[str]]:
    """Split barcodes into URL-sized batches.
    
    Dynamically sizes batches based on actual barcode lengths
    to maximize throughput while staying under HTTP header limits.
    """
    base_url = f"https://books.google.com/libraries/{self.directory}/_barcode_search?execute_query=true&format=text&mode=full&barcodes="
    max_url_length = 7500  # Conservative HTTP header limit
    available = max_url_length - len(base_url)
    
    batches = []
    current_batch = []
    current_length = 0
    
    for barcode in barcodes:
        # +1 for space separator (except first item)
        additional = len(barcode) + (1 if current_batch else 0)
        
        if current_length + additional > available:
            # Current batch is full, start new one
            if current_batch:
                batches.append(current_batch)
            current_batch = [barcode]
            current_length = len(barcode)
        else:
            # Add to current batch
            current_batch.append(barcode)
            current_length += additional
    
    # Add final batch
    if current_batch:
        batches.append(current_batch)
    
    # Limit concurrent batches to max_concurrent_requests
    # Process in rounds if needed
    return batches[:self.max_concurrent_requests]
```

### 5. Remove Unnecessary Methods (-90 lines)

**Delete entirely**:
- `_calculate_max_batch_size()` (lines 105-132)
- `_create_optimal_batch_composition()` (lines 134-179) 
- `get_optimal_batch_size_recommendation()` (lines 181-198)
- `_calculate_optimal_fetch_size()` (lines 200-216)
- `_prepare_combined_batch()` (lines 218-254)
- Recursive URL splitting in `fetch_grin_metadata_batch()` (lines 268-284)

### 6. Simplify Test Suite (-200+ lines)

**Current**: Extensive edge case testing

**New**: Focus on essential scenarios only

```python
# test_grin_enrichment.py - Keep only these tests:

async def test_enrich_single_batch():
    """Test enriching a single batch of books."""
    # Happy path - books get enriched successfully
    
async def test_enrich_multiple_batches():
    """Test enriching multiple batches with progress tracking."""
    # Verify batch processing and progress updates
    
async def test_dynamic_url_batching():
    """Test that URL batches respect header size limits."""
    # Verify long and short barcodes are batched correctly
    
async def test_retry_on_failure():
    """Test retry logic for GRIN API failures."""
    # Verify tenacity retry decorator works
    
async def test_partial_batch_failure():
    """Test handling when some books in batch fail."""
    # Common failure mode - some books don't exist
    
async def test_reset_enrichment():
    """Test resetting enrichment data before re-enriching."""
    # Verify reset functionality works

# Delete tests for:
# - Edge cases with malformed TSV
# - Complex leftover barcode scenarios  
# - Recursive URL splitting
# - Various padding/truncation scenarios
# - Optimization calculations
```

## Expected Results

### Code Reduction
| Component | Current | Target | Reduction |
|-----------|---------|--------|-----------|
| Main pipeline | 863 lines | 550 lines | -36% |
| Tests | ~400 lines | 200 lines | -50% |
| **Total** | ~1263 lines | 750 lines | **-41%** |

### Improvements
1. **Clearer logic flow**: Simple while loop, no state management
2. **Better testability**: Pure functions, focused tests
3. **Easier maintenance**: Less defensive code, clearer structure
4. **Same performance**: Dynamic batching preserved

## Implementation Order

1. **Phase 1**: Extract TSV parsing to pure function
2. **Phase 2**: Remove leftover barcode management 
3. **Phase 3**: Simplify batch creation logic
4. **Phase 4**: Add retry decorators and simplify error handling
5. **Phase 5**: Clean up tests to focus on essential scenarios
6. **Phase 6**: Remove unused methods and consolidate

## Success Criteria

- [ ] All existing functionality preserved
- [ ] Code reduced by at least 25%
- [ ] Tests pass with simplified suite
- [ ] Performance unchanged or improved
- [ ] No nested try/except blocks
- [ ] Clear separation of concerns