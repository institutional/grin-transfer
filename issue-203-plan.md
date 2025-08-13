# Implementation plan for issue 203: Remove per-title enrichment

## Issue description
Remove the feature to enrich titles automatically after download. This adds complexity and takes up a GRIN request slot. Since bulk enrichment is relatively fast, we will:
- Remove all functionality for per-title enrichment, including the separate worker queue
- Don't attempt enrichment in a pipeline cycle at all
- Keep the separate `enrich` entry point for bulk enrichment

## Requirements
- No enrichment workers are spun up
- All per-title enrichment code is removed in all workflows (local and block storage)
- Only enrichment code related to bulk enrichment is retained
- There should be no functions that support single-title enrichment (unless subordinate to a bulk enrichment request)

## Implementation steps

### 1. Create new branch
```bash
git checkout -b issue-203-remove-per-title-enrichment
```

### 2. Remove enrichment worker functionality from sync pipeline

**File: `src/grin_to_s3/sync/pipeline.py`**

Remove the following:
- `enrichment_queue` initialization in `__init__()` (lines around 170-172)
- `enrichment_workers` property assignment
- `_enrichment_workers: list[asyncio.Task]` list
- `enrichment_worker()` method (entire async method around line 435-500)
- `start_enrichment_workers()` method (entire async method)
- `stop_enrichment_workers()` method (entire async method)
- `queue_book_for_enrichment()` method (entire async method around line 763-778)
- Calls to `queue_book_for_enrichment()` in:
  - `_process_download_result()` (line 1212)
  - `_process_upload_result()` (line 1287)
- Enrichment queue size tracking from stats (`enrichment_queue_size`)
- Enrichment phase logging in status display
- Enrichment worker startup in `run()` method

### 3. Remove enrichment configuration options

**File: `src/grin_to_s3/collect_books/__main__.py`**
- Remove `--sync-enrichment-workers` argument definition
- Remove `enrichment_workers` from sync_config dictionary creation
- Remove import of `DEFAULT_SYNC_ENRICHMENT_WORKERS`

**File: `src/grin_to_s3/sync/__main__.py`**
- Remove `--skip-enrichment` argument definition
- Remove `skip_enrichment` parameter from pipeline initialization

**File: `src/grin_to_s3/run_config.py`**
- Remove `DEFAULT_SYNC_ENRICHMENT_WORKERS` constant
- Remove `sync_enrichment_workers()` method
- Remove enrichment workers from config display in print statements

### 4. Update sync models

**File: `src/grin_to_s3/sync/models.py`**
- Remove `enrichment_queue_size` field from `SessionStats` dataclass
- Update default initialization to remove enrichment_queue_size

### 5. Update/remove tests

**Delete entirely:**
- `src/tests/grin_to_s3/sync/test_async_enrichment_integration.py`

**File: `src/tests/grin_to_s3/sync/test_sync_pipeline_concurrency.py`**
Remove the following test methods:
- `test_enrichment_queue_enabled_by_default()`
- `test_enrichment_queue_can_be_disabled()`
- `test_enrichment_queue_size_in_stats()`
- `test_enrichment_queue_size_zero_when_disabled()`
- Any tests checking enrichment_workers configuration

**File: `src/tests/grin_to_s3/sync/conftest.py`**
- Remove `sync_pipeline_with_enrichment` fixture if it exists
- Remove `skip_enrichment` parameter from all fixtures
- Update fixture initialization to not include enrichment parameters

**File: `src/tests/conftest.py`**
- Remove `with_enrichment_workers()` method from `MockRunConfig` class
- Remove enrichment_workers from config dictionary

### 6. Keep bulk enrichment functionality (no changes)

The following should remain unchanged:
- `src/grin_to_s3/metadata/grin_enrichment.py` - bulk enrichment pipeline
- `grin.py` enrich command entry point
- `src/tests/grin_to_s3/enrich/` directory and all tests within
- Database schema for enrichment fields

### 7. Clean up any remaining references

Search for and remove any remaining references to:
- `enrichment_queue`
- `enrichment_worker` 
- `queue_book_for_enrichment`
- `queue_for_enrichment`
- Per-title enrichment in comments or docstrings

### 8. Testing checklist

After making changes:
```bash
# Run full test suite
pytest

# Check types
mypy src/grin_to_s3/ --explicit-package-bases

# Check linting
ruff check

# Test sync workflow
python grin.py sync pipeline --run-name test_run --limit 5

# Test bulk enrichment still works
python grin.py enrich --run-name test_run --limit 5
```

### 9. Create pull request

```bash
# Commit changes
git add -A
git commit -m "Remove per-title enrichment functionality (issue #203)

- Remove enrichment worker queue and background workers
- Remove per-title enrichment during sync operations  
- Keep bulk enrichment command (`grin enrich`) unchanged
- Remove associated tests and configuration options"

# Push and create PR
git push origin issue-203-remove-per-title-enrichment
gh pr create --title "Remove per-title enrichment (issue #203)" \
  --body "Removes automatic per-title enrichment during sync to reduce complexity and GRIN request usage. Bulk enrichment via `grin enrich` command is retained."
```

## Expected outcomes

After implementation:
- Sync operations will no longer attempt to enrich books automatically
- No background enrichment workers will be started
- The `grin enrich` command will continue to work for bulk enrichment
- Simplified codebase with fewer moving parts during sync
- GRIN request slots preserved for more critical operations