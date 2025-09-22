-- Drop redundant SQLite indexes to reclaim space from books.db copies.
DROP INDEX IF EXISTS idx_books_barcode;
DROP INDEX IF EXISTS idx_processed_barcode;
DROP INDEX IF EXISTS idx_failed_barcode;
DROP INDEX IF EXISTS idx_books_enrichment;
DROP INDEX IF EXISTS idx_books_marc_extraction;
DROP INDEX IF EXISTS idx_books_processing_timestamp;
DROP INDEX IF EXISTS idx_status_history_barcode;
DROP INDEX IF EXISTS idx_status_history_timestamp;
DROP INDEX IF EXISTS idx_status_history_latest;
DROP INDEX IF EXISTS idx_status_history_etag;

-- Repack the file after index removal.
VACUUM;
