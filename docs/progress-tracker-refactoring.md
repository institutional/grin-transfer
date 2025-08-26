# Progress Tracker Refactoring Analysis

## Overview

This document analyzes opportunities to refactor the collect step to use the sync pipeline's progress tracking system instead of maintaining separate implementations.

## Current State

### Collect Step Progress Tracking
- **`ProgressReporter`** (`common.py:308`): Simple progress reporter with basic rate calculation
- **`ProgressTracker`** (`collect_books/progress.py:32`): Handles persistence, pagination state, and job metadata
- **Features**: Basic progress display, file archiving, resume capability, SQLite integration

### Sync Pipeline Progress Tracking
- **`SlidingWindowRateCalculator`** (`sync/progress_reporter.py:13`): Advanced rate calculation using sliding window
- **`show_queue_progress`** function: Detailed progress display with ETA, queue depths, and task breakdowns
- **Features**: Better ETA accuracy, detailed task monitoring, rate smoothing

## Key Differences

The sync pipeline's progress tracker is superior in several ways:

1. **Rate Calculation**: Uses a sliding window to avoid startup overhead skewing rates, providing more stable ETA calculations
2. **Progress Display**: Shows detailed information including ETA, processing rates, and queue depths
3. **Accuracy**: Configurable window size for different use cases

The collect step's current `ProgressReporter` provides basic functionality but lacks the sophisticated rate calculation and ETA features.

## Refactoring Options

### Option 1: Hybrid Enhancement (Recommended)
Enhance the collect step's `ProgressReporter` to use `SlidingWindowRateCalculator` internally while maintaining the existing API.

**Pros:**
- Maintains API compatibility
- Minimal changes to existing code
- Gets benefits of better rate calculation

**Cons:**
- Still maintains separate progress display logic
- Doesn't fully unify the progress experience

### Option 2: Direct Integration
Modify the collect step to directly use the sync pipeline's progress components.

**Required Changes:**
- **Sync Pipeline**: ~20 lines of code to extract a generic progress function
- **Collect Step**: ~10 lines to switch progress systems

**Implementation Approach:**

#### Step 1: Extract Generic Progress Display Function
Create a generic version of `show_queue_progress` in `sync/progress_reporter.py`:

```python
def show_progress(
    start_time: float,
    total_items: int | None,
    rate_calculator: SlidingWindowRateCalculator,
    completed_count: int,
    operation_name: str = "items",
    extra_info: dict[str, str] | None = None,
) -> None:
    """Show generic progress with ETA calculation."""
    current_time = time.time()
    percentage = (completed_count / total_items) * 100 if total_items and total_items > 0 else None
    elapsed = current_time - start_time
    
    rate_calculator.add_batch(current_time, completed_count)
    rate = rate_calculator.get_rate(start_time, completed_count)
    
    # ETA calculation
    eta_text = ""
    if rate > 0 and total_items and total_items > completed_count:
        remaining = total_items - completed_count
        eta_seconds = remaining / rate
        eta_text = f" (ETA: {format_duration(eta_seconds)})"
    
    # Build progress display
    if percentage is not None:
        progress_part = f"{completed_count:,}/{total_items:,} ({percentage:.1f}%)"
    else:
        progress_part = f"{completed_count:,} {operation_name}"
    
    base_info = f"{progress_part} - {rate:.1f} {operation_name}/sec - elapsed: {format_duration(elapsed)}{eta_text}"
    
    # Add extra info (queue depths for sync, current record for collect)
    if extra_info:
        extra_parts = [f"{k}: {v}" for k, v in extra_info.items()]
        print(f"{base_info} [{', '.join(extra_parts)}]")
    else:
        print(base_info)

# Keep existing function as wrapper for backwards compatibility
def show_queue_progress(...):
    # Implementation calls show_progress with queue-specific extra_info
```

#### Step 2: Modify Collect Step
In `collect_books/collector.py`:

```python
from grin_to_s3.sync.progress_reporter import SlidingWindowRateCalculator, show_progress

class BookCollector:
    def __init__(self, ...):
        # Replace ProgressReporter with rate calculator
        self.rate_calculator = SlidingWindowRateCalculator(window_size=5)
        self.start_time = None
        # Keep existing ProgressTracker for persistence

    async def collect_books(self, ...):
        self.start_time = time.time()
        
        # Replace progress.increment() calls with:
        if processed_count % 100 == 0:
            extra_info = {"current": record.barcode} if record else None
            show_progress(
                start_time=self.start_time,
                total_items=self.total_books_estimate,
                rate_calculator=self.rate_calculator,
                completed_count=processed_count,
                operation_name="books",
                extra_info=extra_info,
            )
```

**Pros:**
- Unified progress experience across collect and sync
- Better ETAs for collect operations
- Code consolidation
- Future improvements benefit both systems

**Cons:**
- More invasive changes to collect step
- Creates dependency from collect to sync module
- Requires more thorough testing

### Option 3: Common Progress Library
Create a shared progress tracking library that both collect and sync can use.

**Pros:**
- Clean separation of concerns
- No circular dependencies
- Reusable across the entire codebase

**Cons:**
- Most work required
- Larger refactoring scope

## Recommendation

**Option 2 (Direct Integration)** is recommended because:

1. **Minimal Sync Changes**: The sync pipeline's progress components are already well-designed for reuse
2. **Significant Collect Benefits**: Collect step immediately gets better ETAs and progress display
3. **Code Consolidation**: Reduces duplicate progress tracking logic
4. **Unified UX**: Consistent progress display across all operations

## Implementation Benefits

1. **Better ETAs**: More accurate time estimates during long collect operations
2. **Consistent UX**: Similar progress display across collect and sync operations  
3. **Code Reuse**: Eliminate duplicate progress tracking logic
4. **Enhanced Monitoring**: Better visibility into collection performance

## Compatibility Considerations

- The existing `ProgressTracker` for persistence should remain unchanged (it works well)
- SQLite integration and pagination state handling should continue as-is
- Progress file archiving and job metadata tracking remain with `ProgressTracker`

## Next Steps

1. Extract generic `show_progress` function from `show_queue_progress`
2. Modify collect step to use `SlidingWindowRateCalculator` and `show_progress`
3. Test collect operations to ensure progress persistence still works
4. Consider deprecating old `ProgressReporter` once migration is complete