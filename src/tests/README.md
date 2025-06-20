# Test Suite

Comprehensive test suite for the book collection system with async processing validation. All tests use consistent mocking to prevent network calls except for manual tests.

## Test Architecture Overview

The test suite validates that parallel workers correctly synthesize their results at the end of each session, ensuring:

- **Data Integrity**: All worker outputs are correctly merged without loss or corruption
- **Progress Tracking**: Worker-specific progress is accurately reported and aggregated  
- **File Operations**: Temporary worker files are properly managed and cleaned up
- **Error Resilience**: The system handles worker failures gracefully
- **Performance Validation**: Parallel processing provides expected benefits

## Directory Structure

- `unit/` - Unit tests for individual components (fully mocked, fast)
- `functional/` - Functional tests for worker synthesis validation (fully mocked) 
- `integration/` - Integration tests via subprocess (uses --test-mode with mocks)
- `manual/` - Manual tests requiring credentials and real network calls
- `mocks.py` - Shared mock classes for consistent testing across all test types

## Mock Strategy

All tests outside the `manual/` directory use shared mocks to eliminate network dependencies:

- **MockGRINClient**: Simulates GRIN API responses with controlled test data
- **MockAuth**: Bypasses credential validation for testing
- **MockBookStorage** and **MockStorage**: Simulates S3 operations without real storage
- **get_test_data()**: Provides consistent 5-record test dataset
- **get_large_test_data()**: Provides 10-record dataset for specific tests
- **setup_mock_exporter()**: Creates properly mocked BookCollector instances

## Running Tests

```bash
# Run all tests (excludes manual directory automatically)
pytest

# Run specific test categories
pytest tests/unit/          # Unit tests (~0.2s, fully mocked)
pytest tests/functional/    # Functional tests (~0.5s, fully mocked) 
pytest tests/integration/   # Integration tests (~0.8s, uses --test-mode)
pytest tests/manual/        # Manual tests (require credentials, make real calls)

# Run with verbose output
pytest -v -s

# Test performance timing
time pytest tests/functional/  # Should complete in under 1 second
```

## Test Categories

### Unit Tests (`tests/unit/`)
- Individual component testing
- **Fully mocked** - no network dependencies
- Fast execution (~0.2s)
- Tests BookRecord, RateLimiter, BookCollector components

### Functional Tests (`tests/functional/`)
- Worker synthesis validation across 9 comprehensive scenarios
- **Fully mocked** - no network dependencies
- Progress tracking verification (~0.5s)
- Error handling testing
- Multi-worker coordination testing

### Integration Tests (`tests/integration/`)
- End-to-end workflow testing via subprocess with --test-mode flag
- **Use mocks via --test-mode** - no real network calls (fast execution ~0.8s)
- Test actual book collection script execution
- Validate worker file cleanup and progress continuity
- Two test files:
  - `test_collect_books_integration.py` - subprocess testing with mocks
  - `test_collect_books_testmode.py` - specialized test-mode validation

### Manual Tests (`tests/manual/`)
- Authentication testing
- Storage backend testing
- Full system verification
- **Require manual setup and credentials**
- Make real network calls by design

## Network Call Policy

- **Unit tests**: NEVER make network calls (use mocks.py)
- **Functional tests**: NEVER make network calls (use mocks.py) 
- **Integration tests**: NEVER make network calls (use --test-mode flag with mocks)
- **Manual tests**: Make real network calls by design

This ensures fast, reliable testing in CI environments while maintaining comprehensive coverage.

## Worker Synthesis Test Coverage

The test suite specifically validates parallel worker functionality through 11 comprehensive scenarios:

### Functional Tests (`test_worker_synthesis.py`)
1. **Basic Worker Synthesis** - Multiple workers produce consistent results
2. **Worker Data Integrity** - Varying worker counts don't affect output quality  
3. **Worker Range Tracking** - Each worker reports specific barcode ranges
4. **Progress File Integrity** - Progress tracking aggregates worker results correctly
5. **CSV Record Counting** - Accurate record counting in final output
6. **Merge Operation Correctness** - Worker file merging without data loss
7. **Concurrent Worker Safety** - Stress testing worker synchronization
8. **Error Resilience** - Graceful handling of malformed data
9. **Storage Integration** - Worker synthesis with storage backend integration
10. **Progress File Archiving** - Automatic backup of progress files before execution
11. **Concurrent Session Prevention** - File locking prevents multiple simultaneous sessions

### Expected Worker Output
```
Worker 0 completed: 2 books (range: TEST001...TEST002)
Worker 1 completed: 2 books (range: TEST003...TEST004) 
Worker 2 completed: 1 book (record: TEST005)
Total records in CSV: 5
```

## Test Performance

Optimized for fast execution in development workflows:

- **Unit tests**: ~0.2s (individual component validation)
- **Functional tests**: ~0.5s (9 worker synthesis scenarios) 
- **Integration tests**: ~0.8s (subprocess with --test-mode)
- **Manual tests**: Variable (real network calls)

**Total automated test time**: ~1.5s for complete validation

## Pytest Configuration

Configured in `/pyproject.toml` to automatically exclude manual tests:

```toml
[tool.pytest.ini_options]
pythonpath = ["."]
testpaths = ["src/tests"]
norecursedirs = ["src/tests/manual", "*.egg", ".git", "_build", "dist", "build", "docs"]
addopts = "--ignore=src/tests/manual"
```

## Manual Test Usage

```bash
# Authentication testing
python src/tests/manual/test_auth.py
python src/tests/manual/test_headless_auth.py

# Client and storage testing  
python src/tests/manual/test_client.py
python src/tests/manual/test_storage.py
python src/tests/manual/test_minio_storage.py

# Full book collection with real data
python src/tests/manual/test_collect_books.py
python grin.py collect --run-name test --limit 5 --storage local

# Test mode for development (no credentials needed)
python grin.py collect --run-name test --test-mode --limit 5 --storage local
```

## Test Data Format

Standardized test data ensures consistent validation:

```
TEST001\t2023-01-01 12:00\t2023-01-02 12:00\t...\thttps://books.google.com/books?id=test001
TEST002\t2023-01-01 12:00\t2023-01-02 12:00\t...\thttps://books.google.com/books?id=test002
...
```

- `get_test_data()`: 5 records for fast tests
- `get_large_test_data()`: 10 records for comprehensive tests
- All test barcodes start with "TEST" prefix for easy identification
- Mock processing states: TEST001, TEST003 marked as "converted"; TEST005 as "failed"

## Success Indicators

✅ **Data Integrity**: All input records appear exactly once in output  
✅ **Worker Coordination**: Each worker reports distinct, non-overlapping ranges  
✅ **Progress Accuracy**: Session totals match CSV record counts  
✅ **File Management**: No temporary files remain after completion  
✅ **Error Recovery**: System continues processing despite individual record failures

This comprehensive test suite ensures the book collection system's async architecture maintains data integrity and provides accurate progress reporting across all operating conditions.