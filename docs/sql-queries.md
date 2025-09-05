# SQL queries for pipeline administration

SQL queries for managing and optimizing the GRIN-to-S3 pipeline. All queries work with SQLite.

## Table of contents

- [Database access](#database-access)
- [Pipeline optimization](#pipeline-optimization)
- [Status queries](#status-queries)
  - [GRIN status](#grin-status)
  - [Processing status](#processing-status)
  - [Sync status](#sync-status)
- [Barcode operations](#barcode-operations)
  - [Export barcodes](#export-barcodes)
  - [Pipeline integration](#pipeline-integration)
- [Troubleshooting](#troubleshooting)
  - [Error detection](#error-detection)
  - [Missing metadata](#missing-metadata)
  - [Processing bottlenecks](#processing-bottlenecks)
- [Performance analysis](#performance-analysis)

## Database access

Database location: `output/{run-name}/books.db`

```bash
# Interactive
sqlite3 output/run-name/books.db

# Direct query
sqlite3 output/run-name/books.db "SELECT COUNT(*) FROM books;"
```

## Pipeline status


### Books stuck in processing

Books requested >24 hours ago without conversion:
```sql
SELECT barcode, title, processing_request_timestamp,
    (julianday('now') - julianday(processing_request_timestamp)) * 24 as hours_waiting
FROM books 
WHERE processing_request_timestamp IS NOT NULL 
  AND converted_date IS NULL
  AND julianday('now') - julianday(processing_request_timestamp) > 1
ORDER BY processing_request_timestamp;
```

### Re-processing candidates

Books that failed sync but are now converted:
```sql
SELECT b.barcode
FROM books b
JOIN book_status_history bsh ON b.barcode = bsh.barcode
WHERE bsh.status_type = 'sync' 
  AND bsh.status_value = 'verified_unavailable'
  AND b.converted_date IS NOT NULL
  AND b.sync_timestamp IS NULL
GROUP BY b.barcode;
```

Export directly to sync pipeline:
```bash
sqlite3 -separator $'\n' output/run-name/books.db "
SELECT b.barcode FROM books b
JOIN book_status_history bsh ON b.barcode = bsh.barcode
WHERE bsh.status_type = 'sync' AND bsh.status_value = 'verified_unavailable'
  AND b.converted_date IS NOT NULL AND b.sync_timestamp IS NULL
GROUP BY b.barcode;" | python grin.py sync pipeline --run-name run-name --barcodes-file -
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

Viewability:
```sql
SELECT grin_viewability, COUNT(*) as count
FROM books 
WHERE grin_viewability IS NOT NULL 
GROUP BY grin_viewability 
ORDER BY count DESC;
```



Combined GRIN status:
```sql
SELECT 
    grin_state,
    grin_viewability,
    grin_scannable,
    grin_opted_out,
    COUNT(*) as count
FROM books 
WHERE grin_state IS NOT NULL 
GROUP BY grin_state, grin_viewability, grin_scannable, grin_opted_out
ORDER BY count DESC;
```

### Processing status

Books requested:
```sql
SELECT COUNT(*) as books_requested_for_processing
FROM books 
WHERE processing_request_timestamp IS NOT NULL;
```

Request dates:
```sql
SELECT 
    DATE(processing_request_timestamp) as request_date,
    COUNT(*) as books_requested
FROM books 
WHERE processing_request_timestamp IS NOT NULL
GROUP BY DATE(processing_request_timestamp)
ORDER BY request_date DESC;
```

Latest request status:
```sql
SELECT 
    bsh.status_value,
    COUNT(*) as count
FROM books b
JOIN (
    SELECT 
        barcode,
        status_value,
        ROW_NUMBER() OVER (PARTITION BY barcode ORDER BY timestamp DESC) as rn
    FROM book_status_history 
    WHERE status_type = 'processing_request'
) bsh ON b.barcode = bsh.barcode AND bsh.rn = 1
GROUP BY bsh.status_value
ORDER BY count DESC;
```

### Sync status

Books synced:
```sql
SELECT COUNT(*) as books_synced
FROM books 
WHERE sync_timestamp IS NOT NULL;
```

Latest sync status:
```sql
SELECT 
    bsh.status_value,
    COUNT(*) as count
FROM books b
JOIN (
    SELECT 
        barcode,
        status_value,
        ROW_NUMBER() OVER (PARTITION BY barcode ORDER BY timestamp DESC) as rn
    FROM book_status_history 
    WHERE status_type = 'sync'
) bsh ON b.barcode = bsh.barcode AND bsh.rn = 1
GROUP BY bsh.status_value
ORDER BY count DESC;
```

Failed verification:
```sql
SELECT COUNT(*) as verified_unavailable_count
FROM book_status_history 
WHERE status_type = 'sync' 
  AND status_value = 'verified_unavailable';
```

Decrypted books:
```sql
SELECT COUNT(*) as decrypted_books
FROM books 
WHERE is_decrypted = 1;
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
sqlite3 -separator $'\n' output/run-name/books.db "SELECT barcode FROM books WHERE grin_state = 'converted';" > grin_converted_barcodes.txt
```

Scannable, not opted out:
```bash
sqlite3 -separator $'\n' output/run-name/books.db "SELECT barcode FROM books WHERE grin_scannable = 'true' AND (grin_opted_out = 'false' OR grin_opted_out IS NULL);" > scannable_barcodes.txt
```

Books needing processing request:
```bash
sqlite3 -separator $'\n' output/run-name/books.db "
SELECT barcode 
FROM books 
WHERE processing_request_timestamp IS NULL 
  AND grin_scannable = 'true' 
  AND (grin_opted_out = 'false' OR grin_opted_out IS NULL)
;" > barcodes_needing_processing.txt
```

Converted but unsynced:
```bash
sqlite3 -separator $'\n' output/run-name/books.db "
SELECT barcode 
FROM books 
WHERE converted_date IS NOT NULL 
  AND sync_timestamp IS NULL
;" > converted_but_unsynced_barcodes.txt
```

### Pipeline integration

```bash
# From file
python grin.py sync pipeline --run-name run-name --barcodes-file converted_barcodes.txt

# With limit
python grin.py sync pipeline --run-name run-name --barcodes-file converted_barcodes.txt --limit 100

# Direct barcodes
python grin.py sync pipeline --run-name run-name --barcodes "barcode1,barcode2,barcode3"

# Pipe from SQL
sqlite3 -separator $'\n' output/run-name/books.db "SELECT barcode FROM books WHERE sync_timestamp IS NULL LIMIT 100;" | \
  python grin.py sync pipeline --run-name run-name --barcodes-file -
```

## Troubleshooting

### Error detection

Sync failures:
```sql
SELECT 
    b.barcode,
    b.title,
    bsh.timestamp,
    json_extract(bsh.metadata, '$.reason') as error_reason
FROM books b
JOIN book_status_history bsh ON b.barcode = bsh.barcode
WHERE bsh.status_type = 'sync' 
  AND bsh.status_value = 'verified_unavailable'
ORDER BY bsh.timestamp DESC;
```

Failed table entries:
```sql
SELECT 
    f.barcode,
    b.title,
    f.timestamp,
    f.error_message
FROM failed f
LEFT JOIN books b ON f.barcode = b.barcode
ORDER BY f.timestamp DESC;
```

### Missing metadata

No enrichment:
```sql
SELECT COUNT(*) as books_without_enrichment
FROM books 
WHERE enrichment_timestamp IS NULL;
```

No MARC:
```sql
SELECT COUNT(*) as books_without_marc
FROM books 
WHERE marc_extraction_timestamp IS NULL;
```

Missing titles:
```sql
SELECT barcode, created_at
FROM books 
WHERE title IS NULL OR title = ''
ORDER BY created_at DESC
LIMIT 20;
```

### Processing bottlenecks

Requested but not converted:
```sql
SELECT 
    COUNT(*) as books_requested_but_not_converted
FROM books 
WHERE processing_request_timestamp IS NOT NULL 
  AND converted_date IS NULL;
```

Oldest pending requests:
```sql
SELECT 
    barcode,
    title,
    processing_request_timestamp
FROM books 
WHERE processing_request_timestamp IS NOT NULL 
  AND converted_date IS NULL
ORDER BY processing_request_timestamp ASC
LIMIT 20;
```

## Performance analysis

Collection to enrichment time:
```sql
SELECT 
    AVG(
        (julianday(enrichment_timestamp) - julianday(created_at)) * 24 * 60
    ) as avg_minutes_to_enrichment
FROM books 
WHERE enrichment_timestamp IS NOT NULL 
  AND created_at IS NOT NULL;
```

Request to conversion time:
```sql
SELECT 
    AVG(
        (julianday(converted_date) - julianday(processing_request_timestamp)) * 24
    ) as avg_hours_to_conversion
FROM books 
WHERE processing_request_timestamp IS NOT NULL 
  AND converted_date IS NOT NULL;
```

Collection growth:
```sql
SELECT 
    DATE(created_at) as collection_date,
    COUNT(*) as books_collected
FROM books 
GROUP BY DATE(created_at)
ORDER BY collection_date;
```

Recent activity:
```sql
SELECT 
    'Most recent book added' as activity,
    MAX(created_at) as timestamp
FROM books

UNION ALL

SELECT 
    'Most recent enrichment',
    MAX(enrichment_timestamp)
FROM books

UNION ALL

SELECT 
    'Most recent sync',
    MAX(sync_timestamp)
FROM books

UNION ALL

SELECT 
    'Most recent MARC extraction',
    MAX(marc_extraction_timestamp)
FROM books;
```

Daily throughput (last 7 days):
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
ORDER BY marc_date_1 DESC
LIMIT 20;
```

### Books by language
```sql
SELECT 
    marc_language,
    COUNT(*) as count
FROM books 
WHERE marc_language IS NOT NULL 
GROUP BY marc_language
ORDER BY count DESC
LIMIT 20;
```

### Large books by archive size

```sql
SELECT 
    b.barcode,
    b.title,
    json_extract(bsh.metadata, '$.size_bytes') as archive_size_bytes
FROM books b
JOIN book_status_history bsh ON b.barcode = bsh.barcode
WHERE bsh.status_type = 'sync' 
  AND json_extract(bsh.metadata, '$.size_bytes') IS NOT NULL
ORDER BY CAST(json_extract(bsh.metadata, '$.size_bytes') as INTEGER) DESC
LIMIT 20;
```

---

**Requirements**: Initialized GRIN-to-S3 database with tables: books, processed, failed, book_status_history.

See `docs/schema.sql` for schema details.