# Streaming Architecture Proposal for Task Manager

## Problem Statement

The current task manager processes books in fixed batches. Each batch must complete entirely before the next batch starts. This creates "dead time" in the download queue - when early books are still uploading (slow), the download semaphore sits idle even though new books could be downloading.

**Example Timeline:**
- Batch 1 (books 1-100) starts
- All 100 books download (using 5 concurrent slots)
- Books move to decrypt, then upload
- Upload takes much longer than download
- Download queue sits idle while uploads continue
- Only after ALL books finish does Batch 2 start

## Proposed Solution: Feed-forward queue implementation

Replace batch processing with continuous streaming. Books flow independently through the pipeline - new books can start downloading while others are still uploading.


## Alternatives Considered

1. **Batch Overlap** - Allow multiple batches in flight simultaneously
   - More complex coordination
   - Still has potential gaps

2. **Task-Level Queues** - Separate queue for each pipeline stage
   - Major architectural change
   - Complex inter-stage data passing

3. **Dynamic Batch Refill** - Fetch new books between batches
   - Doesn't solve the core problem
   - Still has dead time

4. **Feed-Forward Queue** - Simple producer-consumer with bounded queue
   - Standard pattern everyone understands
   - Fixed memory overhead regardless of total books
   - Natural backpressure via queue bounds
   - Minimal code changes required

### Chosen processing implementation: Feed-Forward Queue Implementation

This approach uses a standard producer-consumer pattern to eliminate dead time while maintaining bounded memory usage:

```python
async def process_with_simple_queue(barcodes, workers=20):
    queue = asyncio.Queue(maxsize=100)  # Small buffer for backpressure
    
    async def worker():
        while True:
            barcode = await queue.get()
            if barcode is None:  # Poison pill signals completion
                break
            await process_book_pipeline(barcode, ...)
            queue.task_done()
    
    # Start fixed number of workers (bounded task creation)
    worker_tasks = [asyncio.create_task(worker()) for _ in range(workers)]
    
    # Feed queue incrementally (blocks when full)
    for barcode in barcodes:
        await queue.put(barcode)
    
    # Send poison pills to workers
    for _ in worker_tasks:
        await queue.put(None)
    
    await asyncio.gather(*worker_tasks)
```

**Benefits:**
- **No dead time** - Workers continuously pull from queue as capacity allows
- **Bounded memory** - Only queue buffer + worker count tasks in memory
- **Handles millions** - Queue fed incrementally, never creates all tasks at once
- **Standard pattern** - Producer-consumer is well-understood and debuggable
- **Simple error handling** - Worker failures don't affect queue feeding

**Memory Profile:**
- Queue: ~10MB (100 books × 100KB metadata each)
- Workers: ~1GB (20 workers × 50MB peak during processing)
- Total: ~1.1GB (same as current batch system)

**Code Changes:**
- Replace `process_books_batch()` function (~40 lines)
- Keep existing `process_book_pipeline()` unchanged
- Maintain current progress reporting with queue depth

## Recommendation

**Updated Recommendation: Feed-Forward Queue (Option 4)**

After considering implementation complexity and memory constraints, the feed-forward queue approach provides the best balance of:
- **Solving the dead time problem completely** - Workers continuously process books
- **Minimal changes to existing task code** - Only `process_books_batch()` function changes
- **Standard patterns** - Producer-consumer is well-understood and maintainable
- **Bounded memory usage** - Same memory profile as current system (~1.1GB)
- **Handles millions of books** - Queue prevents task explosion

## Requirements for new implementation

- Must be as simple as possible
- No extraneous error handling
- Must still report periodic status after ~20 complete task cycles (configurable)
- Must still be interruptable with ctrl-c and finish current task queue
