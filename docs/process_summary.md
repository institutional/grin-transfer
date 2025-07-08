# Process Summary Infrastructure

The process summary infrastructure provides comprehensive tracking and logging capabilities for long-running pipeline operations. This system captures timing metrics, error counts, interruption detection, and custom metrics for operations like collect, process, sync, and enrich.

## Overview

The process summary system consists of:

- **ProcessSummary**: Core class that tracks metrics during pipeline execution
- **ProcessSummaryManager**: Handles persistence and recovery of process summaries
- **Module Functions**: Convenience functions for creating and saving summaries

## Key Features

### Timing Metrics
- High-precision timing using `time.perf_counter()`
- Start/end times with automatic duration calculation
- Processing rate calculation (items per second)

### Operation Tracking
- Items processed, successful, failed, and retried
- Bytes processed for data transfer operations
- Success rate percentage calculation

### Error Management
- Error count and categorization by type
- Last error message and timestamp tracking
- Comprehensive error type statistics

### Interruption Detection
- Automatic detection of process interruptions
- Checkpoint-based monitoring (5-minute threshold)
- Interruption count tracking

### Progress Monitoring
- Timestamped progress updates with context
- Recent progress summary (last 5 updates)
- Custom progress metadata support

### Session Persistence
- Survives process interruptions and restarts
- JSON-based state persistence
- Automatic recovery with interruption detection

## Usage Example

```python
from grin_to_s3.process_summary import create_process_summary, save_process_summary

# Create or load process summary
summary = await create_process_summary("harvard_2024", "collect")

# Track operations
summary.increment_items(processed=10, successful=9, failed=1)
summary.add_progress_update("Processing batch 1", batch_size=10)
summary.checkpoint()

# Handle errors
try:
    # Some operation
    pass
except Exception as e:
    summary.add_error("NetworkError", str(e))

# Set custom metrics
summary.set_custom_metric("storage_type", "minio")
summary.set_custom_metric("bucket_name", "grin-raw")

# Save progress (call periodically)
await save_process_summary(summary)

# End process
summary.end_process()
await save_process_summary(summary)

# Get final summary
final_summary = summary.get_summary_dict()
```

## ProcessSummary Class

### Core Properties

```python
@dataclass
class ProcessSummary:
    # Process identification
    process_name: str           # e.g., "collect", "sync", "enrich"
    run_name: str              # e.g., "harvard_2024"
    session_id: int            # Unix timestamp
    
    # Timing metrics
    start_time: float          # perf_counter() timestamp
    end_time: Optional[float]  # perf_counter() timestamp
    duration_seconds: Optional[float]
    
    # Operation counters
    items_processed: int
    items_successful: int
    items_failed: int
    items_retried: int
    bytes_processed: int
    
    # Error tracking
    error_count: int
    error_types: Dict[str, int]
    last_error_message: Optional[str]
    last_error_time: Optional[float]
    
    # Interruption detection
    interruption_count: int
    last_checkpoint_time: Optional[float]
    
    # Progress tracking
    progress_updates: List[Dict[str, Any]]
    
    # Custom metrics
    custom_metrics: Dict[str, Any]
```

### Key Methods

#### Process Control
- `start_process()`: Initialize process tracking
- `end_process()`: Complete process and calculate final metrics
- `checkpoint()`: Update checkpoint time for interruption detection

#### Metrics Tracking
- `increment_items(processed, successful, failed, retried, bytes_count)`: Update counters
- `add_error(error_type, error_message)`: Record error occurrence
- `add_progress_update(message, **kwargs)`: Add timestamped progress update
- `set_custom_metric(key, value)`: Set command-specific metrics

#### Data Access
- `get_summary_dict()`: Complete summary as dictionary
- `get_progress_summary()`: Recent progress updates summary
- `detect_interruption()`: Check for process interruption

## ProcessSummaryManager Class

Handles persistence and recovery of process summaries.

### Key Methods

- `load_or_create_summary()`: Load existing summary or create new one
- `save_summary(summary)`: Save summary to JSON file
- `cleanup_summary()`: Remove summary file after completion

### File Structure

Summary files are saved as:
```
runs/{run_name}/process_summary_{process_name}.json
```

For example:
```
runs/harvard_2024/process_summary_collect.json
runs/harvard_2024/process_summary_sync.json
runs/harvard_2024/process_summary_enrich.json
```

## Summary Data Structure

The complete summary dictionary includes:

```json
{
  "process_name": "collect",
  "run_name": "harvard_2024",
  "session_id": 1751920822,
  "start_time": 1449311.978860791,
  "end_time": 1449361.234567890,
  "duration_seconds": 49.255707099,
  "is_completed": true,
  "items_processed": 1000,
  "items_successful": 995,
  "items_failed": 5,
  "items_retried": 3,
  "bytes_processed": 1048576,
  "success_rate_percent": 99.5,
  "processing_rate_per_second": 20.31,
  "error_count": 8,
  "error_types": {
    "NetworkError": 5,
    "ValidationError": 3
  },
  "last_error_message": "Connection timeout",
  "last_error_time": 1449360.123456789,
  "interruption_count": 1,
  "progress_updates_count": 15,
  "progress_updates": [
    {
      "timestamp": 1449315.123456789,
      "elapsed_seconds": 3.14,
      "message": "Processing batch 1",
      "items_processed": 50,
      "items_successful": 50,
      "items_failed": 0,
      "batch_size": 50
    }
  ],
  "custom_metrics": {
    "storage_type": "minio",
    "bucket_name": "grin-raw",
    "books_collected": 1000
  },
  "generated_at": "2024-01-15T10:30:45.123456Z"
}
```

## Error Handling

The system handles various error scenarios:

- **Corrupted summary files**: Creates new summary and logs warning
- **File system errors**: Logs errors but continues operation
- **JSON parsing errors**: Falls back to new summary creation
- **Missing directories**: Creates directories as needed

## Integration with Pipeline Commands

The process summary infrastructure is designed to integrate with existing pipeline commands:

```python
async def collect_command(run_name: str, ...):
    # Create process summary
    summary = await create_process_summary(run_name, "collect")
    
    try:
        # Your existing collect logic here
        for barcode in barcodes:
            try:
                # Process barcode
                result = await process_barcode(barcode)
                summary.increment_items(processed=1, successful=1)
            except Exception as e:
                summary.add_error(type(e).__name__, str(e))
                summary.increment_items(processed=1, failed=1)
            
            # Periodic checkpoint and save
            if barcode_count % 100 == 0:
                summary.checkpoint()
                await save_process_summary(summary)
                summary.add_progress_update(f"Processed {barcode_count} books")
    
    finally:
        # Always end process and save final summary
        summary.end_process()
        await save_process_summary(summary)
```

## Design Patterns

The process summary infrastructure follows established patterns in the codebase:

- **Session IDs**: Uses Unix timestamps like other components
- **Precise Timing**: Uses `time.perf_counter()` for high-precision measurements
- **Error Handling**: Graceful degradation with logging
- **Async File Operations**: Uses `aiofiles` for non-blocking I/O
- **Progress Reporting**: Consistent with `ProgressReporter` patterns
- **State Persistence**: JSON-based like other configuration files

## Testing

The module includes comprehensive unit tests covering:

- All core functionality and edge cases
- Error handling and recovery scenarios
- File persistence and loading
- Integration workflows
- Mock-based testing for external dependencies

Run tests with:
```bash
PYTHONPATH=src python -m pytest tests/test_process_summary.py -v
```

## Future Enhancements

Potential future improvements:

1. **Metrics Aggregation**: Aggregate metrics across multiple runs
2. **Real-time Monitoring**: WebSocket-based real-time updates
3. **Alert Integration**: Send alerts on error thresholds
4. **Performance Analytics**: Historical performance trend analysis
5. **Dashboard Integration**: Web-based monitoring dashboard