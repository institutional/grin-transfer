# Plan to Remove progress.json and Improve process_summary.json

## Overview

This document outlines the plan to remove the obsolete `progress.json` file and improve `process_summary.json` with more specific status tracking. No backward compatibility will be maintained.

## Current State Analysis

### progress.json (Obsolete)
- **Location**: `output/{run_name}/progress.json`
- **Usage**: Only written by the collect command
- **Purpose**: Originally for resume functionality (now deleted)
- **Current state**: Contains enhanced phase tracking but duplicates process_summary.json
- **Problems**: 
  - Resume functionality has been removed
  - Creates redundancy with process_summary.json
  - Only used by one command

### process_summary.json (Active)
- **Location**: `output/{run_name}/process_summary.json`
- **Usage**: All pipeline commands (collect, process, sync, enrich)
- **Purpose**: Track pipeline-wide metrics
- **Problems**:
  - Uses vague "items_processed" terminology
  - Unclear what "processed" means in each context
  - Same counter names for different operations

## Part 1: Remove progress.json Infrastructure

### Files to Modify

#### 1. `src/grin_to_s3/collect_books/collector.py`
Remove:
- `self.resume_file` attribute
- `save_progress()` method (lines ~220-297)
- `archive_progress()` method (if exists)
- Progress file initialization in `__init__`
- All references to writing progress.json

#### 2. `src/grin_to_s3/collect_books/config.py`
Remove:
- `resume_file: str = "output/default/progress.json"` field from `CollectConfig` dataclass

#### 3. `src/grin_to_s3/collect_books/__main__.py`
Remove:
- Any progress file existence checks
- Progress file path configuration

#### 4. `src/tests/grin_to_s3/collect_books/test_collect_books.py`
Remove:
- Test `test_progress_tracking()` or update to test process_summary instead
- Any assertions checking progress.json creation

#### 5. `src/tests/mocks.py`
Remove:
- Any mock data or utilities for progress.json

### Files to Delete
- `docs/progress-json-improvements.md` - Obsolete documentation
- Any example progress.json files in the repository

### Other Updates
- `README.md` - Remove any mentions of progress.json
- `src/grin_to_s3/run_config.py` - Remove any progress.json references

## Part 2: Redesign process_summary.json Structure

### Current Structure Problems
```python
# Current vague tracking:
items_processed: int  # What does "processed" mean?
items_successful: int  # Success at what?
items_failed: int  # Failed doing what?
```

### New Stage-Specific Structure
```python
@dataclass
class ProcessStageMetrics:
    stage_name: str
    start_time: float | None = None
    end_time: float | None = None
    duration_seconds: float | None = None
    
    # Remove generic counters, add stage-specific ones:
    
    # Collection stage metrics
    books_collected: int = 0
    collection_failed: int = 0
    collection_rate_per_hour: float = 0.0
    
    # Processing (conversion request) stage metrics
    conversion_requests_made: int = 0
    conversion_requests_failed: int = 0
    conversion_request_rate_per_hour: float = 0.0
    
    # Sync stage metrics
    books_synced: int = 0
    sync_skipped: int = 0  # Already synced/etag match
    sync_failed: int = 0
    conversions_requested_during_sync: int = 0  # Sent for conversion
    sync_rate_per_hour: float = 0.0
    
    # Enrichment stage metrics
    books_enriched: int = 0
    enrichment_skipped: int = 0
    enrichment_failed: int = 0
    enrichment_rate_per_hour: float = 0.0
    
    # Retain these for all stages
    bytes_processed: int = 0
    error_count: int = 0
    error_types: dict[str, int] = field(default_factory=dict)
    last_error_message: str | None = None
    last_operation_time: str | None = None
    
    # Stage-specific detailed metrics (varies by stage)
    stage_details: dict[str, Any] = field(default_factory=dict)
```

## Part 3: Update Each Command

### Collect Command (`src/grin_to_s3/collect_books/collector.py`)

**Remove**:
- All progress.json writing code
- `save_progress()` calls
- Resume file configuration

**Update**:
```python
# Old:
self.process_summary_stage.increment_items(processed=1, successful=1)

# New:
self.process_summary_stage.books_collected += 1

# For failures:
self.process_summary_stage.collection_failed += 1
```

### Process Command (`src/grin_to_s3/processing.py`)

**Update** (~lines 492-509):
```python
# Old:
self.process_summary_stage.increment_items(processed=1, successful=1)

# New:
self.process_summary_stage.conversion_requests_made += 1

# For failures:
self.process_summary_stage.conversion_requests_failed += 1
```

### Sync Command (`src/grin_to_s3/sync/pipeline.py`)

**Update** (~lines 614-646):
```python
# Determine outcome and increment appropriate counter:
if book_outcome == "synced":
    self.process_summary_stage.books_synced += 1
elif book_outcome == "skipped":
    self.process_summary_stage.sync_skipped += 1
elif book_outcome == "conversion_requested":
    self.process_summary_stage.conversions_requested_during_sync += 1
elif book_outcome == "failed":
    self.process_summary_stage.sync_failed += 1
```

### Enrich Command (`src/grin_to_s3/metadata/grin_enrichment.py`)

**Update** (~lines 161-179):
```python
# Old:
self.process_summary_stage.increment_items(processed=1, successful=1)

# New:
self.process_summary_stage.books_enriched += 1

# For skipped:
self.process_summary_stage.enrichment_skipped += 1

# For failures:
self.process_summary_stage.enrichment_failed += 1
```

## Part 4: Update ProcessStageMetrics Methods

### In `src/grin_to_s3/process_summary.py`:

1. **Remove** `increment_items()` method entirely
2. **Remove** generic counters from `get_summary_dict()`
3. **Add** stage-specific counter handling:

```python
def get_stage_totals(self) -> tuple[int, int, int]:
    """Get total processed, successful, failed for this stage."""
    if self.stage_name == "collect":
        total = self.books_collected + self.collection_failed
        successful = self.books_collected
        failed = self.collection_failed
    elif self.stage_name == "process":
        total = self.conversion_requests_made + self.conversion_requests_failed
        successful = self.conversion_requests_made
        failed = self.conversion_requests_failed
    elif self.stage_name == "sync":
        total = self.books_synced + self.sync_skipped + self.sync_failed + self.conversions_requested_during_sync
        successful = self.books_synced + self.sync_skipped + self.conversions_requested_during_sync
        failed = self.sync_failed
    elif self.stage_name == "enrich":
        total = self.books_enriched + self.enrichment_skipped + self.enrichment_failed
        successful = self.books_enriched + self.enrichment_skipped
        failed = self.enrichment_failed
    else:
        total = successful = failed = 0
    
    return total, successful, failed
```

## Part 5: Update Display Functions

### Modify `display_step_summary()` (~line 515):

```python
def display_step_summary(summary: RunSummary, step_name: str) -> None:
    """Display stage-specific summary with appropriate terminology."""
    
    if step_name == "collect":
        print(f"✓ Collected {step.books_collected:,} books in {duration_str}")
        if step.collection_failed > 0:
            print(f"  Failed: {step.collection_failed:,}")
            
    elif step_name == "process":
        print(f"✓ Requested conversion for {step.conversion_requests_made:,} books in {duration_str}")
        if step.conversion_requests_failed > 0:
            print(f"  Failed: {step.conversion_requests_failed:,}")
            
    elif step_name == "sync":
        print(f"✓ Synced {step.books_synced:,} books in {duration_str}")
        if step.sync_skipped > 0:
            print(f"  Skipped (already synced): {step.sync_skipped:,}")
        if step.conversions_requested_during_sync > 0:
            print(f"  Sent for conversion: {step.conversions_requested_during_sync:,}")
        if step.sync_failed > 0:
            print(f"  Failed: {step.sync_failed:,}")
            
    elif step_name == "enrich":
        print(f"✓ Enriched {step.books_enriched:,} books in {duration_str}")
        if step.enrichment_skipped > 0:
            print(f"  Skipped: {step.enrichment_skipped:,}")
        if step.enrichment_failed > 0:
            print(f"  Failed: {step.enrichment_failed:,}")
```

## Part 6: Update JSON Serialization

### In `get_summary_dict()` method:

```python
stages_dict[stage_name] = {
    "start_time": stage.start_time,
    "end_time": stage.end_time,
    "duration_seconds": stage.duration_seconds,
    
    # Stage-specific counters based on stage type
    **({"books_collected": stage.books_collected,
        "collection_failed": stage.collection_failed}
       if stage_name == "collect" else {}),
    
    **({"conversion_requests_made": stage.conversion_requests_made,
        "conversion_requests_failed": stage.conversion_requests_failed}
       if stage_name == "process" else {}),
    
    **({"books_synced": stage.books_synced,
        "sync_skipped": stage.sync_skipped,
        "sync_failed": stage.sync_failed,
        "conversions_requested": stage.conversions_requested_during_sync}
       if stage_name == "sync" else {}),
    
    **({"books_enriched": stage.books_enriched,
        "enrichment_skipped": stage.enrichment_skipped,
        "enrichment_failed": stage.enrichment_failed}
       if stage_name == "enrich" else {}),
    
    # Common fields
    "bytes_processed": stage.bytes_processed,
    "error_count": stage.error_count,
    "error_types": stage.error_types,
    "stage_details": stage.stage_details,
}
```

## Implementation Order

1. **Phase 1: Update process_summary.py**
   - Add new stage-specific fields to ProcessStageMetrics
   - Update get_summary_dict() for new structure
   - Update display_step_summary() for new fields

2. **Phase 2: Update each command**
   - Process command: Use conversion-specific counters
   - Sync command: Use sync-specific counters  
   - Enrich command: Use enrichment-specific counters
   - Collect command: Keep using process_summary (prepare for progress.json removal)

3. **Phase 3: Remove progress.json from collect**
   - Remove all progress.json writing code
   - Update collector to only use process_summary
   - Remove configuration options

4. **Phase 4: Clean up**
   - Delete obsolete documentation
   - Update tests
   - Remove any remaining references

## Testing Checklist

- [ ] Collect command works without progress.json
- [ ] Process summary shows stage-specific metrics
- [ ] All commands update correct counters
- [ ] Summary display shows appropriate terminology
- [ ] Cloud upload of process_summary still works
- [ ] No references to progress.json remain
- [ ] Tests pass with new structure

## Benefits

1. **Single source of truth**: One file for all progress tracking
2. **Clear semantics**: No ambiguous "processed" terminology
3. **Stage-appropriate metrics**: Each stage tracks what matters
4. **Simpler codebase**: No duplicate tracking systems
5. **Better monitoring**: Can alert on specific stage metrics

## Risks and Mitigations

| Risk | Mitigation |
|------|------------|
| External tools expect progress.json | Document change in release notes |
| Tests depend on old structure | Update tests before deployment |
| Monitoring dashboards break | Update queries to use new field names |
| Historical data comparison | One-time migration script if needed |

## Success Criteria

- All pipeline commands run successfully
- process_summary.json contains all necessary information
- No progress.json files are created
- Display shows stage-specific terminology
- Tests pass with new structure