# Session Management Optimization Plan

## Problem Statement

The sync pipeline creates new aiohttp sessions for every single GRIN operation, causing massive overhead:
- Every CHECK task creates a new session via `create_http_session()`
- Every DOWNLOAD task creates a new session
- Every `fetch_resource()` call in GRINClient creates a new session
- ProcessingClient, EnrichmentClient, BookCollector all create sessions per request
- With ~150,000 books, this results in 300,000+ session creations
- Each session creation involves TCP handshake, SSL negotiation, and authentication overhead

### Current Performance Impact
- Processing rate: 0.1 books/sec
- Estimated time for 150,926 books: 445+ hours
- Primary bottleneck: Session creation overhead

## Solution: GRINClient Always Manages Session

Make GRINClient always create and manage its own persistent aiohttp session. No backward compatibility needed.

## Implementation Plan

### 1. Update GRINClient (`/src/grin_to_s3/client.py`)

#### Changes:
```python
class GRINClient:
    def __init__(self, ...):
        # Always create and manage a session
        connector = aiohttp.TCPConnector(
            limit=DEFAULT_CONNECTOR_LIMITS["limit"],
            limit_per_host=DEFAULT_CONNECTOR_LIMITS["limit_per_host"]
        )
        timeout_config = aiohttp.ClientTimeout(total=self.timeout, connect=10)
        self.session = aiohttp.ClientSession(
            connector=connector,
            timeout=timeout_config
        )
        
    async def fetch_resource(self, directory: str, resource: str, method: str = "GET") -> str:
        # Use the persistent session instead of creating new one
        url = f"{self.base_url}/{directory}/{resource}"
        response = await self.auth.make_authenticated_request(self.session, url, method=method)
        return await response.text()
    
    async def download_archive(self, url: str) -> aiohttp.ClientResponse:
        """Download a book archive - for use by download.py."""
        return await self.auth.make_authenticated_request(self.session, url)
    
    async def head_archive(self, url: str) -> aiohttp.ClientResponse:
        """HEAD request for archive metadata - for use by check.py."""
        return await self.auth.make_authenticated_request(self.session, url, method="HEAD")
    
    async def close(self):
        """Close the session. Must be called when done with client."""
        await self.session.close()
```

#### Remove:
- All `async with create_http_session()` context managers
- Temporary session creation in `fetch_resource()`
- Temporary session creation in `stream_book_list_html_prefetch()`

### 2. Update download.py (`/src/grin_to_s3/sync/tasks/download.py`)

#### Changes:
```python
# Remove create_http_session import
# from grin_to_s3.common import create_http_session  # REMOVE THIS

async def download_book_to_filesystem(...):
    # Move this URL to the function
    grin_url = f"https://books.google.com/libraries/{library_directory}/{barcode}.tar.gz.gpg"
    
    # OLD CODE (remove):
    # async with create_http_session(timeout=download_timeout) as session:
    #     response = await grin_client.auth.make_authenticated_request(session, grin_url)
    
    # NEW CODE:
    response = await grin_client.download_archive(library_directory, barcode)
    
    # Rest of download logic remains the same
    async for chunk in response.content.iter_chunked(1024 * 1024):
        ...
```

### 3. Update check.py (`/src/grin_to_s3/sync/tasks/check.py`)

#### Changes:
```python
# Remove create_http_session import
# from grin_to_s3.common import create_http_session  # REMOVE THIS

async def grin_head_request(barcode: Barcode, grin_client: GRINClient, library_directory: str):
    # Same as above
    grin_url = f"https://books.google.com/libraries/{library_directory}/{barcode}.tar.gz.gpg"
    
    # OLD CODE (remove):
    # async with create_http_session() as session:
    #     head_response = await grin_client.auth.make_authenticated_request(session, grin_url, method="HEAD")
    
    # NEW CODE:
    head_response = await grin_client.head_archive(grin_url)
    
    # Rest of logic remains the same
```

### 4. Add Cleanup Calls

#### SyncPipeline (`/src/grin_to_s3/sync/pipeline.py`)
- Add cleanup in teardown operations:
```python
async def cleanup(self):
    """Clean up pipeline resources."""
    if hasattr(self, 'grin_client'):
        await self.grin_client.close()
    # ... existing cleanup
```

#### ProcessingClient (`/src/grin_to_s3/processing.py`)
- Update existing cleanup method:
```python
async def cleanup(self):
    """Clean up client resources."""
    if hasattr(self, 'grin_client'):
        await self.grin_client.close()
```

#### EnrichmentClient (`/src/grin_to_s3/metadata/grin_enrichment.py`)
- Update existing cleanup:
```python
async def cleanup(self):
    """Clean up resources."""
    if self.grin_client:
        await self.grin_client.close()
    # ... existing cleanup
```

#### BookCollector (`/src/grin_to_s3/collect_books/collector.py`)
- Update existing cleanup:
```python
async def cleanup(self):
    """Clean up resources."""
    if self.client:
        await self.client.close()
    # ... existing cleanup
```

#### ProcessingMonitor (`/src/grin_to_s3/processing.py`)
- Add cleanup:
```python
async def cleanup(self):
    """Clean up monitor resources."""
    if self.grin_client:
        await self.grin_client.close()
```

### 5. Update Tests

#### Mock close() method in tests:
```python
mock_grin_client = MagicMock()
mock_grin_client.close = AsyncMock()
```

#### Update download/check test mocks:
```python
# Instead of mocking auth.make_authenticated_request
mock_grin_client.download_archive = AsyncMock(return_value=response)
mock_grin_client.head_archive = AsyncMock(return_value=response)
```

## Expected Performance Improvements

### Session Creation Reduction
- **Before**: ~300,000+ session creations
  - 150K CHECK operations
  - 150K DOWNLOAD operations  
  - Thousands of processing requests
  - Thousands of enrichment requests
  - Multiple collection streaming sessions per page

- **After**: ~5-10 total sessions
  - 1 for SyncPipeline
  - 1 for ProcessingClient
  - 1 for EnrichmentClient
  - 1 for BookCollector
  - 1 for ProcessingMonitor

### Expected Throughput Improvement
- Current: 0.1 books/sec
- Expected: 5-10 books/sec (50-100x improvement)
- Time reduction: 445 hours → ~8-15 hours

## Migration Notes

### Breaking Changes
- All code creating GRINClient must call `close()` when done
- Tests need to be updated to mock new methods
- No backward compatibility - all callers must be updated

### Rollout Order
1. Update GRINClient with new session management
2. Update all cleanup methods to call `close()`
3. Update download.py and check.py to use new methods
4. Update tests
5. Verify with integration testing

## Testing Strategy

1. **Unit Tests**: Verify session is reused across multiple calls
2. **Integration Tests**: Verify cleanup properly closes sessions
3. **Performance Tests**: Measure session creation count and throughput
4. **Load Tests**: Verify connection pooling works under concurrent load

## Monitoring

Track these metrics after deployment:
- Session creation rate (should drop to near zero)
- Books/second processing rate
- Connection pool utilization
- Any connection leak warnings