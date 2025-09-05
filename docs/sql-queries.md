# SQL queries for common admin tasks

This document provides SQL queries for common administrative tasks when working with the GRIN-to-S3 pipeline database. All queries are designed to work directly with SQLite and can be executed using the `sqlite3` command-line tool or any SQLite client.

## Table of contents

- [Database access](#database-access)
- [Status breakdown queries](#status-breakdown-queries)
  - [GRIN status fields](#grin-status-fields)
  - [Processing pipeline status](#processing-pipeline-status)
  - [Sync pipeline status](#sync-pipeline-status)
  - [Combined status reports](#combined-status-reports)
- [Barcode list operations](#barcode-list-operations)
  - [Export all barcodes](#export-all-barcodes)
  - [Export barcodes by status](#export-barcodes-by-status)
  - [Export barcodes needing processing](#export-barcodes-needing-processing)
  - [Using barcode lists with sync pipeline](#using-barcode-lists-with-sync-pipeline)
- [Troubleshooting queries](#troubleshooting-queries)
  - [Find books with errors](#find-books-with-errors)
  - [Find books with missing metadata](#find-books-with-missing-metadata)
  - [Find processing bottlenecks](#find-processing-bottlenecks)
- [Performance analysis queries](#performance-analysis-queries)
  - [Processing time analysis](#processing-time-analysis)
  - [Collection statistics](#collection-statistics)

## Database access

The SQLite database is located in your run directory at `grin-output/{run-name}/books.db`. You can access it directly using:

```bash
sqlite3 grin-output/{run-name}/books.db
```

For example:
```bash
sqlite3 grin-output/harvard_2024/books.db
```

You can also execute queries directly from the command line:
```bash
sqlite3 grin-output/harvard_2024/books.db "SELECT COUNT(*) FROM books;"
```

## Status breakdown queries

### GRIN status fields

Show breakdown of books by GRIN state:
```sql
SELECT grin_state, COUNT(*) as count
FROM books 
WHERE grin_state IS NOT NULL 
GROUP BY grin_state 
ORDER BY count DESC;
```

Show breakdown by viewability:
```sql
SELECT grin_viewability, COUNT(*) as count
FROM books 
WHERE grin_viewability IS NOT NULL 
GROUP BY grin_viewability 
ORDER BY count DESC;
```

Show breakdown by opt-out status:
```sql
SELECT grin_opted_out, COUNT(*) as count
FROM books 
WHERE grin_opted_out IS NOT NULL 
GROUP BY grin_opted_out 
ORDER BY count DESC;
```

Show breakdown by scannable status:
```sql
SELECT grin_scannable, COUNT(*) as count
FROM books 
WHERE grin_scannable IS NOT NULL 
GROUP BY grin_scannable 
ORDER BY count DESC;
```

Combined GRIN status overview:
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

### Processing pipeline status

Show books that have been requested for processing:
```sql
SELECT COUNT(*) as books_requested_for_processing
FROM books 
WHERE processing_request_timestamp IS NOT NULL;
```

Show breakdown of processing request dates:
```sql
SELECT 
    DATE(processing_request_timestamp) as request_date,
    COUNT(*) as books_requested
FROM books 
WHERE processing_request_timestamp IS NOT NULL
GROUP BY DATE(processing_request_timestamp)
ORDER BY request_date DESC;
```

Show latest processing request status from history:
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

### Sync pipeline status

Show books that have been synced:
```sql
SELECT COUNT(*) as books_synced
FROM books 
WHERE sync_timestamp IS NOT NULL;
```

Show latest sync status from history:
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

Show books that failed verification:
```sql
SELECT COUNT(*) as verified_unavailable_count
FROM book_status_history 
WHERE status_type = 'sync' 
  AND status_value = 'verified_unavailable';
```

Show books that are decrypted and available:
```sql
SELECT COUNT(*) as decrypted_books
FROM books 
WHERE is_decrypted = 1;
```

### Combined status reports

Comprehensive status report showing pipeline progress:
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

## Barcode list operations

### Export all barcodes

Export all barcodes to a file:
```bash
sqlite3 -separator $'\n' grin-output/harvard_2024/books.db "SELECT barcode FROM books;" > all_barcodes.txt
```

Or using a SQL query in the sqlite3 shell:
```sql
.output all_barcodes.txt
.separator ""
SELECT barcode FROM books;
.output stdout
```

### Export barcodes by status

Export barcodes for books that have been converted by GRIN:
```bash
sqlite3 -separator $'\n' grin-output/harvard_2024/books.db "SELECT barcode FROM books WHERE converted_date IS NOT NULL;" > converted_barcodes.txt
```

Export barcodes for books that have NOT been synced yet:
```bash
sqlite3 -separator $'\n' grin-output/harvard_2024/books.db "SELECT barcode FROM books WHERE sync_timestamp IS NULL;" > unsynced_barcodes.txt
```

Export barcodes for books with GRIN state 'converted':
```bash
sqlite3 -separator $'\n' grin-output/harvard_2024/books.db "SELECT barcode FROM books WHERE grin_state = 'converted';" > grin_converted_barcodes.txt
```

Export barcodes for books that are scannable and not opted out:
```bash
sqlite3 -separator $'\n' grin-output/harvard_2024/books.db "SELECT barcode FROM books WHERE grin_scannable = 'true' AND (grin_opted_out = 'false' OR grin_opted_out IS NULL);" > scannable_barcodes.txt
```

### Export barcodes needing processing

Export barcodes for books that need to be requested for processing:
```bash
sqlite3 -separator $'\n' grin-output/harvard_2024/books.db "
SELECT barcode 
FROM books 
WHERE processing_request_timestamp IS NULL 
  AND grin_scannable = 'true' 
  AND (grin_opted_out = 'false' OR grin_opted_out IS NULL)
;" > barcodes_needing_processing.txt
```

Export barcodes for books that have been converted but not yet synced:
```bash
sqlite3 -separator $'\n' grin-output/harvard_2024/books.db "
SELECT barcode 
FROM books 
WHERE converted_date IS NOT NULL 
  AND sync_timestamp IS NULL
;" > converted_but_unsynced_barcodes.txt
```

### Using barcode lists with sync pipeline

Once you have exported a barcode list, you can use it with the sync pipeline:

```bash
# Sync specific barcodes from a file
python grin.py sync pipeline --run-name harvard_2024 --barcodes-file converted_barcodes.txt

# Sync a limited number of barcodes from a file
python grin.py sync pipeline --run-name harvard_2024 --barcodes-file converted_barcodes.txt --limit 100
```

You can also use barcodes directly on the command line:
```bash
# Sync specific barcodes
python grin.py sync pipeline --run-name harvard_2024 --barcodes "barcode1,barcode2,barcode3"
```

## Troubleshooting queries

### Find books with errors

Find books that failed during sync:
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

Find books in the failed table:
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

### Find books with missing metadata

Find books without GRIN enrichment data:
```sql
SELECT COUNT(*) as books_without_enrichment
FROM books 
WHERE enrichment_timestamp IS NULL;
```

Find books without MARC metadata:
```sql
SELECT COUNT(*) as books_without_marc
FROM books 
WHERE marc_extraction_timestamp IS NULL;
```

Find books with missing titles:
```sql
SELECT barcode, created_at
FROM books 
WHERE title IS NULL OR title = ''
ORDER BY created_at DESC
LIMIT 20;
```

### Find processing bottlenecks

Find books that have been requested for processing but haven't been converted:
```sql
SELECT 
    COUNT(*) as books_requested_but_not_converted
FROM books 
WHERE processing_request_timestamp IS NOT NULL 
  AND converted_date IS NULL;
```

Find oldest processing requests still pending:
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

## Performance analysis queries

### Processing time analysis

Average time between collection and enrichment:
```sql
SELECT 
    AVG(
        (julianday(enrichment_timestamp) - julianday(created_at)) * 24 * 60
    ) as avg_minutes_to_enrichment
FROM books 
WHERE enrichment_timestamp IS NOT NULL 
  AND created_at IS NOT NULL;
```

Average time between processing request and conversion:
```sql
SELECT 
    AVG(
        (julianday(converted_date) - julianday(processing_request_timestamp)) * 24
    ) as avg_hours_to_conversion
FROM books 
WHERE processing_request_timestamp IS NOT NULL 
  AND converted_date IS NOT NULL;
```

### Collection statistics

Show collection growth over time:
```sql
SELECT 
    DATE(created_at) as collection_date,
    COUNT(*) as books_collected
FROM books 
GROUP BY DATE(created_at)
ORDER BY collection_date;
```

Show most recent activity:
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

Show daily pipeline throughput:
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

## Advanced queries

### Books by publication year

Show distribution of books by publication year (using MARC date_1):
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

Show distribution of books by language:
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

### Large books (by archive size)

Note: This requires that ETag metadata includes size information in the status history.

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

**Note**: All queries assume you're working with a properly initialized GRIN-to-S3 database. If you encounter errors, verify that:

1. The database file exists and is accessible
2. All expected tables are present (books, processed, failed, book_status_history)
3. You have appropriate read permissions

For more information about the database schema, see `docs/schema.sql`.