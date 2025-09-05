# Book Processing Status Definitions

This document defines all status values used in the book processing pipeline, as tracked in the `book_status_history` table.

## Status Types

The system uses different status types to categorise processing stages:

- **`sync`** - Core pipeline operations (check, download, decrypt, upload, etc.)
- **`conversion`** - GRIN conversion requests and responses
- **`text_extraction`** - OCR text extraction from archives
- **`marc_extraction`** - MARC metadata extraction from archives

## Sync Pipeline Statuses

### Success Statuses
- **`checked`** - Archive file existence verified in GRIN
- **`downloaded`** - Archive download from GRIN completed successfully
- **`decrypted`** - Archive successfully decrypted from GPG
- **`unpacked`** - Archive successfully unpacked for processing
- **`uploaded`** - Archive uploaded to storage successfully
- **`completed`** - Full sync pipeline completed successfully

### Failure Statuses
- **`check_failed`** - Archive check failed (e.g., 404 not found in GRIN)
- **`download_failed`** - Download from GRIN failed
- **`decrypt_failed`** - GPG decryption failed
- **`unpack_failed`** - Archive unpacking failed (corrupted archive, extraction error)
- **`upload_failed`** - Upload to storage failed

### Skip Statuses
- **`skipped`** - Task skipped for various reasons (etag match, dry run, etc.)

## Conversion Pipeline Statuses

### Request Statuses
- **`requested`** - Conversion successfully requested from GRIN
- **`failed`** - Conversion request failed (generic failure)
- **`limit_reached`** - GRIN queue limit reached, request rejected
- **`unavailable`** - Book verified as unavailable for conversion in GRIN

### Response Statuses
These reflect GRIN's response to conversion status queries:
- **`in_process`** - Book is being processed by GRIN
- **`skipped`** - Conversion request skipped (already requested, already available, etc.)

## Extraction Pipeline Statuses

### Text Extraction (`text_extraction` type)
- **`completed`** - OCR text extraction completed successfully
- **`failed`** - OCR text extraction failed

### MARC Extraction (`marc_extraction` type)
- **`completed`** - MARC metadata extraction completed successfully
- **`failed`** - MARC metadata extraction failed

## Status Metadata

Each status entry may include JSON metadata with additional context:

### Common Metadata Fields
- **`reason`** - Specific reason for the status (e.g., "skip_etag_match", "fail_archive_missing")
- **`error`** - Error message for failure statuses
- **`etag`** - File ETag for duplicate detection
- **`path`** - File path for upload operations

### Extraction-Specific Metadata
- **`page_count`** - Number of pages extracted (text extraction)
- **`field_count`** - Number of MARC fields extracted (MARC extraction)

## Status Flow

### Normal Success Flow
1. `checked` - Verify archive exists in GRIN
2. `downloaded` - Download archive from GRIN
3. `decrypted` - Decrypt GPG-encrypted archive
4. `uploaded` - Upload decrypted archive to storage
5. `unpacked` - Unpack archive for metadata extraction
6. `completed` (text_extraction) - Extract OCR text
7. `completed` (marc_extraction) - Extract MARC metadata  
8. `completed` (sync) - Full pipeline completed

### Failure Recovery Flows
- `check_failed` → `requested` (request conversion if archive missing)
- Any failure status may be retried by rerunning the pipeline

### Conversion Request Flow
1. `requested` - Request sent to GRIN
2. `in_process` - GRIN processing the book
3. Eventually becomes available for sync pipeline

## Usage in Queries

To get the latest status for each book:
```sql
SELECT 
    barcode,
    status_type,
    status_value,
    timestamp
FROM book_status_history bsh1
WHERE timestamp = (
    SELECT MAX(timestamp) 
    FROM book_status_history bsh2 
    WHERE bsh2.barcode = bsh1.barcode 
      AND bsh2.status_type = bsh1.status_type
)
ORDER BY barcode, status_type;
```

To find books needing retry (failed but never completed):
```sql
SELECT DISTINCT barcode
FROM book_status_history
WHERE status_type = 'sync' 
  AND status_value IN ('check_failed', 'download_failed', 'decrypt_failed', 'unpack_failed', 'upload_failed', 'failed')
  AND barcode NOT IN (
      SELECT barcode FROM book_status_history 
      WHERE status_type = 'sync' AND status_value = 'completed'
  );
```