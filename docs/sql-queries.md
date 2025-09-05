# SQL queries for pipeline administration

The tool tracks its status with an internal SQLite database which you are encouraged to explore and integrate with your internal tooling.

The `books` table will contain information about the archives in your collection. The `book_status_history` table will contain rows for each task and job run.

See [docs/schema.sql](schema.sql)

## Table of contents

- [Database access](#database-access)
- [Quick reference](#quick-reference)
- [Status queries](#status-queries)
- [Performance analysis](#performance-analysis)
- [Barcode operations](#barcode-operations)
- [Troubleshooting](#troubleshooting)
- [Corpus information](#corpus-information)

## Database access

Database location: `output/{run-name}/books.db`

```bash
# Interactive
sqlite3 output/run-name/books.db

# Direct query
sqlite3 output/run-name/books.db "SELECT COUNT(*) FROM books;"
```

## Quick reference

Common one-liner checks for pipeline health:

```bash
# Total books by status
sqlite3 -header -column output/run-name/books.db "SELECT 'Total' as category, COUNT(*) as count FROM books UNION ALL SELECT 'Enriched', COUNT(*) FROM books WHERE enrichment_timestamp IS NOT NULL UNION ALL SELECT 'Requested', COUNT(*) FROM books WHERE processing_request_timestamp IS NOT NULL UNION ALL SELECT 'Synced', COUNT(*) FROM books WHERE sync_timestamp IS NOT NULL;"

# Today's activity
sqlite3 -header -column output/run-name/books.db "SELECT status_type, status_value, COUNT(*) as count FROM book_status_history WHERE DATE(timestamp) = DATE('now') GROUP BY status_type, status_value;"

# Processing velocity (last 24 hours)
sqlite3 -header -column output/run-name/books.db "SELECT COUNT(*) as books_completed_24h FROM book_status_history WHERE status_value = 'completed' AND timestamp >= datetime('now', '-24 hours');"
```

## Status queries

### GRIN status

Breakdown by GRIN state:
```sql
SELECT grin_state, COUNT(*) as count
FROM books 
WHERE grin_state IS NOT NULL 
GROUP BY grin_state 
ORDER BY count DESC;
```

#### Example:

```bash
echo "SELECT grin_state, COUNT(*) as count
FROM books
WHERE grin_state IS NOT NULL
GROUP BY grin_state
ORDER BY count DESC;" | sqlite3 -header -column output/full/books.db

grin_state                  count 
--------------------------  ------
PREVIOUSLY_DOWNLOADED       91673
CHECKED_IN                  6970 
CONVERTED                   6995 
IN_PROCESS                  2019 
NOT_AVAILABLE_FOR_DOWNLOAD  60   
NEW                         120   
```

### Processing status

Books requested for processing via this tool:

```sql
SELECT 
    DATE(processing_request_timestamp) as request_date,
    COUNT(*) as books_requested
FROM books 
WHERE processing_request_timestamp IS NOT NULL
GROUP BY DATE(processing_request_timestamp)
ORDER BY request_date DESC;
```

### Sync status

Latest sync status (simplified):
```sql
WITH latest_sync AS (
    SELECT 
        barcode,
        status_value,
        ROW_NUMBER() OVER (PARTITION BY barcode ORDER BY timestamp DESC) as rn
    FROM book_status_history 
    WHERE status_type = 'sync'
)
SELECT 
    ls.status_value,
    COUNT(*) as count
FROM books b
JOIN latest_sync ls ON b.barcode = ls.barcode AND ls.rn = 1
GROUP BY ls.status_value
ORDER BY count DESC;
```

Books flagged as not being available for conversion in GRIN:

```sql
SELECT COUNT(*) as unavailable_count
FROM book_status_history 
WHERE status_type = 'sync' 
  AND status_value = 'unavailable';
```

### Pipeline progress summary

```sql
SELECT 
    'Total books' as status_category,
    COUNT(*) as count
FROM books

UNION ALL

SELECT 
    'Books with enrichment data',
    COUNT(*)
FROM books 
WHERE enrichment_timestamp IS NOT NULL

UNION ALL

SELECT 
    'Books requested for processing',
    COUNT(*)
FROM books 
WHERE processing_request_timestamp IS NOT NULL

UNION ALL

SELECT 
    'Books synced',
    COUNT(*)
FROM books 
WHERE sync_timestamp IS NOT NULL

UNION ALL

SELECT 
    'Books decrypted',
    COUNT(*)
FROM books 
WHERE is_decrypted = 1;
```

## Performance analysis

Use these queries to monitor pipeline performance and identify bottlenecks:

### Processing velocity



Daily throughput summary:
```sql
SELECT 
    DATE(timestamp) as date,
    COUNT(*) as books_completed,
    COUNT(DISTINCT status_type) as status_types_active
FROM book_status_history
WHERE status_value = 'completed'
  AND timestamp >= datetime('now', '-30 days')
GROUP BY DATE(timestamp)
ORDER BY date DESC;
```





### Running total of successfully synced files

```sql
SELECT
    sync_date,
    files_synced,
    SUM(files_synced) OVER (ORDER BY sync_date) as cumulative_synced
FROM (
    SELECT
        DATE(timestamp) as sync_date,
        COUNT(*) as files_synced
    FROM book_status_history
    WHERE status_type = 'sync'
      AND status_value = 'completed'
    GROUP BY DATE(timestamp)
) daily_counts
ORDER BY sync_date;
```

### Recent activity summary

```sql
SELECT
    'Books in collection' as metric,
    COUNT(*) as count,
    MAX(created_at) as latest_timestamp
FROM books

UNION ALL

SELECT
    'Books with enrichment',
    COUNT(*),
    MAX(enrichment_timestamp)
FROM books
WHERE enrichment_timestamp IS NOT NULL

UNION ALL

SELECT
    'Books synced (completed)',
    COUNT(*),
    MAX(timestamp)
FROM book_status_history
WHERE status_type = 'sync' AND status_value = 'completed'

UNION ALL

SELECT
    'Books with text extraction',
    COUNT(*),
    MAX(timestamp)
FROM book_status_history
WHERE status_type = 'text_extraction'

UNION ALL

SELECT
    'Processing requests made',
    COUNT(*),
    MAX(timestamp)
FROM book_status_history
WHERE status_type = 'processing_request';
```

Recent pipeline activity (last 7 days):

```sql
SELECT 
    DATE(timestamp) as date,
    status_type,
    status_value,
    COUNT(*) as count
FROM book_status_history 
WHERE DATE(timestamp) >= date('now', '-7 days')
GROUP BY DATE(timestamp), status_type, status_value
ORDER BY date DESC, status_type, status_value;
```

## Barcode operations

### Export barcodes

All barcodes:
```bash
sqlite3 -separator $'\n' output/run-name/books.db "SELECT barcode FROM books;" > all_barcodes.txt
```

Converted books:
```bash
sqlite3 -separator $'\n' output/run-name/books.db "SELECT barcode FROM books WHERE converted_date IS NOT NULL;" > converted_barcodes.txt
```

Unsynced books:
```bash
sqlite3 -separator $'\n' output/run-name/books.db "SELECT barcode FROM books WHERE sync_timestamp IS NULL;" > unsynced_barcodes.txt
```

GRIN converted state:
```bash
sqlite3 -separator $'\n' output/run-name/books.db "SELECT barcode FROM books WHERE grin_state = 'CONVERTED';" > grin_converted_barcodes.txt
```

Books stuck in processing (requested > 7 days ago, no sync):
```bash
sqlite3 -separator $'\n' output/run-name/books.db "SELECT barcode FROM books WHERE processing_request_timestamp IS NOT NULL AND processing_request_timestamp < datetime('now', '-7 days') AND sync_timestamp IS NULL;" > stuck_books.txt
```

Books requiring retry:
```sql
SELECT DISTINCT b.barcode
FROM books b
JOIN book_status_history bsh ON b.barcode = bsh.barcode
WHERE bsh.status_type = 'sync' 
  AND bsh.status_value IN ('check_failed', 'download_failed', 'decrypt_failed', 'upload_failed', 'failed');
```

Books requiring retry with failure details (never completed):
```sql
SELECT 
    b.barcode,
    b.title,
    bsh.status_value as failure_type,
    bsh.timestamp as failure_time,
    json_extract(bsh.metadata, '$.error') as error_message
FROM books b
JOIN book_status_history bsh ON b.barcode = bsh.barcode
WHERE bsh.status_type = 'sync' 
  AND bsh.status_value IN ('check_failed', 'download_failed', 'decrypt_failed', 'upload_failed', 'failed')
  -- This clause checks that they never eventually completed
  AND NOT EXISTS (
      SELECT 1 FROM book_status_history bsh2 
      WHERE bsh2.barcode = b.barcode 
        AND bsh2.status_type = 'sync' 
        AND bsh2.status_value = 'completed'
  )
ORDER BY bsh.timestamp DESC;
```

### Pipeline integration

```bash
# From file
python grin.py sync pipeline --run-name run-name --barcodes-file converted_barcodes.txt

# Pipe from SQL
sqlite3 -separator $'\n' output/run-name/books.db "SELECT barcode FROM books WHERE sync_timestamp IS NULL LIMIT 100;" | \
  python grin.py sync pipeline --run-name run-name --barcodes-file -
```

## Troubleshooting

### Error detection

Permanent failures:
```sql
SELECT 
    b.barcode,
    b.title,
    bsh.timestamp,
    json_extract(bsh.metadata, '$.reason') as error_reason
FROM books b
JOIN book_status_history bsh ON b.barcode = bsh.barcode
WHERE bsh.status_type = 'sync' 
  AND bsh.status_value = 'unavailable'
ORDER BY bsh.timestamp DESC;
```

Recent failures across all pipeline stages:
```sql
SELECT 
    status_type,
    status_value,
    COUNT(*) as failure_count,
    MAX(timestamp) as latest_failure
FROM book_status_history
WHERE status_value IN ('check_failed', 'download_failed', 'decrypt_failed', 'upload_failed', 'failed', 'unavailable', 'limit_reached')
  AND timestamp >= datetime('now', '-7 days')
GROUP BY status_type, status_value
ORDER BY failure_count DESC;
```

### Missing metadata

Books requiring the enrichment step:
```sql
SELECT COUNT(*) as books_without_enrichment
FROM books 
WHERE enrichment_timestamp IS NULL;
```

Note: It is recommended to just enrich the entire corpus in one go, as bulk enrichment is relatively fast. Use `python grin.py enrich --run-name RUN-NAME`

Missing titles:
```sql
SELECT barcode, created_at
FROM books 
WHERE title IS NULL OR title = ''
ORDER BY created_at DESC;
```

## Corpus information

### Books by publication year
```sql
SELECT 
    marc_date_1 as publication_year,
    COUNT(*) as count
FROM books 
WHERE marc_date_1 IS NOT NULL 
  AND LENGTH(marc_date_1) = 4
  AND marc_date_1 GLOB '[0-9][0-9][0-9][0-9]'
GROUP BY marc_date_1
ORDER BY marc_date_1 DESC;
```

### Books by language
```sql
SELECT 
    marc_language,
    COUNT(*) as count
FROM books 
WHERE marc_language IS NOT NULL 
GROUP BY marc_language
ORDER BY count DESC;
```

### Largest extracted books by page count

```sql
SELECT
    b.barcode,
    b.title,
    json_extract(bsh.metadata, '$.page_count') as page_count
FROM books b
JOIN book_status_history bsh ON b.barcode = bsh.barcode
WHERE bsh.status_type = 'text_extraction'
  AND json_extract(bsh.metadata, '$.page_count') IS NOT NULL
ORDER BY CAST(json_extract(bsh.metadata, '$.page_count') as INTEGER) DESC
LIMIT 20;
```