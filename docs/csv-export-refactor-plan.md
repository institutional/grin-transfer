# CSV Export Refactoring Plan

## Problem
The collect command currently exports CSV files locally but doesn't upload them to cloud storage, while the sync command has this functionality. We need to share this CSV upload capability between both commands.

## Current State
- `sync/tasks/export_csv.py` has a `main()` function tightly coupled to the SyncPipeline object
- `collect` command exports CSV locally in `collector.export_csv_from_database()`
- Both commands need the same CSV upload functionality

## Proposed Solution

### 1. Create new focused functions in `sync/csv_export.py`

#### Function 1: Export CSV to file
```python
async def export_csv_to_file(
    db_tracker,  # Has get_all_books_csv_data() method
    output_path: Path,
) -> int:
    """Export books from database to CSV file. Returns record count."""
    output_path.parent.mkdir(parents=True, exist_ok=True)
    books = await db_tracker.get_all_books_csv_data()
    
    record_count = 0
    with open(output_path, "w", encoding="utf-8", newline="") as f:
        writer = csv.writer(f)
        writer.writerow(BookRecord.csv_headers())
        for book in books:
            record_count += 1
            writer.writerow(book.to_csv_row())
    
    return record_count
```

#### Function 2: Upload CSV to storage
```python
async def upload_csv_to_storage(
    csv_path: Path,
    book_manager,  # BookManager instance with storage and meta_path()
    compression_enabled: bool = True,
) -> str:
    """Upload CSV to storage with optional compression. Returns bucket path."""
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    
    if compression_enabled:
        async with compress_file_to_temp(csv_path) as compressed_path:
            # Upload both latest and timestamped versions
            latest_path = book_manager.meta_path("books_latest.csv.gz")
            timestamped_path = book_manager.meta_path(f"timestamped/books_{timestamp}.csv.gz")
            
            await asyncio.gather(
                book_manager.storage.write_file(latest_path, str(compressed_path)),
                book_manager.storage.write_file(timestamped_path, str(compressed_path))
            )
            return latest_path
    else:
        # Upload uncompressed versions
        latest_path = book_manager.meta_path("books_latest.csv")
        timestamped_path = book_manager.meta_path(f"timestamped/books_{timestamp}.csv")
        
        await asyncio.gather(
            book_manager.storage.write_file(latest_path, str(csv_path)),
            book_manager.storage.write_file(timestamped_path, str(csv_path))
        )
        return latest_path
```

### 2. Update `sync/tasks/export_csv.py`

Keep the existing `main()` function for backward compatibility, but refactor it to use the new functions:

```python
async def main(pipeline: "SyncPipeline") -> Result[ExportCsvData]:
    """Export book metadata to CSV format (wrapper for pipeline compatibility)."""
    csv_path = pipeline.filesystem_manager.staging_path / "meta" / "books_latest.csv"
    
    # Export to file
    record_count = await export_csv_to_file(pipeline.db_tracker, csv_path)
    
    # Upload if using block storage
    bucket_path = None
    if pipeline.uses_block_storage:
        bucket_path = await upload_csv_to_storage(
            csv_path,
            pipeline.book_manager,
            pipeline.config.sync_compression_meta_enabled
        )
        logger.info(f"CSV uploaded to storage: {bucket_path}")
    else:
        logger.info(f"CSV exported locally: {csv_path}")
    
    return Result(
        task_type=TaskType.EXPORT_CSV,
        action=TaskAction.COMPLETED,
        data={
            "csv_file_path": Path(bucket_path) if bucket_path else csv_path,
            "record_count": record_count,
        }
    )
```

### 3. Update `collect_books/__main__.py`

In the finally block (around line 353), after database upload:

```python
# Upload CSV to storage if using block storage
if book_manager and args.storage != "local":
    from grin_to_s3.sync.csv_export import upload_csv_to_storage
    
    # The CSV was already exported locally by collector.export_csv_from_database()
    # Now upload it to storage
    try:
        bucket_path = await upload_csv_to_storage(
            output_file,  # The CSV already created locally
            book_manager,
            run_config.sync_compression_meta_enabled
        )
        logger.info(f"CSV uploaded to storage: {bucket_path}")
    except Exception as e:
        logger.error(f"CSV upload failed: {e}", exc_info=True)
        # Don't fail the entire collection if CSV upload fails
```

## Benefits

1. **Single Responsibility**: Each function does one thing well
2. **Reusability**: Both commands can use the same upload logic
3. **Testability**: Functions can be tested in isolation
4. **Clarity**: Clear function names that describe what they do
5. **Minimal Changes**: Sync command's existing interface remains unchanged

## Implementation Steps

1. Create the new functions in `sync/csv_export.py`
2. Refactor the existing `main()` to use these functions
3. Add the import and upload call to `collect_books/__main__.py`
4. Test both sync and collect commands still work correctly

## Testing

- Verify collect command uploads CSV when using cloud storage
- Verify collect command doesn't upload CSV when using local storage  
- Verify sync command continues to work as before
- Test with compression enabled and disabled
- Verify both latest and timestamped versions are uploaded