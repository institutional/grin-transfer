# Performance Optimization TODO

## Session Creation Overhead Issue

### Problem
The sync pipeline creates new AWS/R2 sessions for every single operation, causing massive overhead:
- Every CHECK task creates a new `aioboto3.Session()` in `book_manager.get_decrypted_archive_metadata()`
- Every UPLOAD task creates a new session in `storage.write_file()` and `storage.write_text()`
- With ~150,000 books, this results in ~300,000 session creations
- Each session creation involves SSL handshakes and authentication overhead

### Impact
- Current performance: 0.1 books/sec
- Processing time per book: 3+ minutes
- Estimated time for 150,926 books: 445+ hours

### Proposed Solution

#### 1. Implement Session Pooling
- Create a singleton S3 client manager that maintains a reusable session
- Initialize once at pipeline startup and reuse throughout
- Share the client across all tasks to eliminate session overhead

#### 2. Refactor Storage Layer
- Modify `Storage` class to accept an optional pre-configured S3 client
- Update `BookManager` to use the shared client
- Remove per-operation session creation from:
  - `write_file()`
  - `write_text()` 
  - `get_decrypted_archive_metadata()`

#### 3. Implementation Details
- Create `S3ClientManager` class to manage shared session
- Initialize client manager in `SyncPipeline.__init__()`
- Configure with appropriate connection pooling (`max_pool_connections`)
- Enable HTTP keep-alive for connection reuse
- Pass shared client through pipeline to all tasks

### Expected Improvements
- Reduce session creations from ~300,000 to 1
- Improve throughput from 0.1 books/sec to estimated 5-10 books/sec
- Reduce ETA from 445 hours to ~8-15 hours

### Files to Modify
- `/src/grin_to_s3/storage/base.py`
- `/src/grin_to_s3/storage/book_manager.py`
- `/src/grin_to_s3/sync/pipeline.py`
- `/src/grin_to_s3/sync/tasks/check.py`
- `/src/grin_to_s3/sync/tasks/upload.py`



on upload branch:


20/36,048 (0.1%) - 0.3 books/sec - elapsed: 1m 6s (ETA: 32h 54m) [downloads: 1/5, processing: 29, waiting to download: 100, waiting to be processed: 0]
40/36,048 (0.1%) - 0.5 books/sec - elapsed: 1m 47s (ETA: 20h 46m) [downloads: 0/5, processing: 30, waiting to download: 100, waiting to be processed: 0]
60/36,048 (0.2%) - 0.5 books/sec - elapsed: 2m 34s (ETA: 22h 6m) [downloads: 0/5, processing: 33, waiting to download: 100, waiting to be processed: 0]
80/36,048 (0.2%) - 0.4 books/sec - elapsed: 3m 27s (ETA: 23h 29m) [downloads: 1/5, processing: 30, waiting to download: 100, waiting to be processed: 0]
100/36,048 (0.3%) - 0.4 books/sec - elapsed: 4m 7s (ETA: 22h 38m) [downloads: 3/5, processing: 23, waiting to download: 99, waiting to be processed: 0]
120/36,048 (0.3%) - 0.4 books/sec - elapsed: 5m 42s (ETA: 27h 32m) [downloads: 1/5, processing: 36, waiting to download: 100, waiting to be processed: 0]
^C
Received signal 2, finishing sync for books in flight...
Press Control-C again to force immediate exit

Graceful shutdown in progress, no new books will be processed...
140/36,048 (0.4%) - 0.4 books/sec - elapsed: 6m 41s (ETA: 27h 52m) [downloads: 2/5, processing: 31, waiting to download: 95, waiting to be processed: 0]
160/36,048 (0.4%) - 0.4 books/sec - elapsed: 7m 35s (ETA: 27h 41m) [downloads: 0/5, processing: 33, waiting to download: 75, waiting to be processed: 0]
180/36,048 (0.5%) - 0.4 books/sec - elapsed: 8m 8s (ETA: 26h 19m) [downloads: 0/5, processing: 32, waiting to download: 58, waiting to be processed: 0]
200/36,048 (0.6%) - 0.4 books/sec - elapsed: 8m 52s (ETA: 25h 46m) [downloads: 3/5, processing: 26, waiting to download: 40, waiting to be processed: 0]
