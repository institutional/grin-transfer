-- GRIN-to-S3 Database Schema
-- SQLite schema for book collection, enrichment, and sync tracking

-- Books table: Core repository for all book metadata and sync tracking
CREATE TABLE IF NOT EXISTS books (
    -- Core identification
    barcode TEXT PRIMARY KEY,
    title TEXT,
    
    -- GRIN timestamps (from _all_books endpoint)
    scanned_date TEXT,
    converted_date TEXT,
    downloaded_date TEXT,
    processed_date TEXT,
    analyzed_date TEXT,
    ocr_date TEXT,
    google_books_link TEXT,
    
    -- Processing request tracking (timestamp only, status tracked in history table)
    processing_request_timestamp TEXT, -- ISO timestamp when processing was requested
    
    -- GRIN enrichment fields (populated by enrichment pipeline)
    grin_state TEXT,
    grin_viewability TEXT,
    grin_opted_out TEXT,
    grin_conditions TEXT,
    grin_scannable TEXT,
    grin_tagging TEXT,
    grin_audit TEXT,
    grin_material_error_percent TEXT,
    grin_overall_error_percent TEXT,
    grin_claimed TEXT,
    grin_ocr_analysis_score TEXT,
    grin_ocr_gtd_score TEXT,
    grin_digitization_method TEXT,
    grin_check_in_date TEXT,
    grin_source_library_bibkey TEXT,
    grin_rubbish TEXT,
    grin_allow_download_updated_date TEXT,
    grin_viewability_updated_date TEXT,
    enrichment_timestamp TEXT,
    
    -- Export tracking
    csv_exported TEXT,
    csv_updated TEXT,
    
    -- Sync tracking for storage pipeline (status tracked in history table)
    storage_type TEXT, -- "s3", "local"
    storage_path TEXT, -- Path to encrypted archive in storage
    storage_decrypted_path TEXT, -- Path to decrypted archive in storage
    last_etag_check TEXT, -- ISO timestamp of last ETag verification
    encrypted_etag TEXT, -- Encrypted file's ETag for duplicate detection
    is_decrypted BOOLEAN DEFAULT FALSE, -- Whether decrypted version exists
    sync_timestamp TEXT, -- ISO timestamp of last successful sync
    sync_error TEXT, -- Error message if sync failed
    
    -- Record keeping
    created_at TEXT NOT NULL,
    updated_at TEXT NOT NULL
);

-- Progress tracking for collection pipeline
CREATE TABLE IF NOT EXISTS processed (
    barcode TEXT PRIMARY KEY,
    timestamp TEXT NOT NULL,
    session_id INTEGER NOT NULL
);

-- Failed records tracking
CREATE TABLE IF NOT EXISTS failed (
    barcode TEXT PRIMARY KEY,
    timestamp TEXT NOT NULL,
    session_id INTEGER NOT NULL,
    error_message TEXT
);

-- Status history tracking for atomic status changes
CREATE TABLE IF NOT EXISTS book_status_history (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    barcode TEXT NOT NULL,
    status_type TEXT NOT NULL, -- "processing_request", "sync", "enrichment", etc.
    status_value TEXT NOT NULL, -- "pending", "requested", "in_process", "converted", "failed", etc.
    timestamp TEXT NOT NULL,
    session_id TEXT, -- Optional identifier for tracking batches/runs
    metadata TEXT, -- Optional JSON metadata for additional context
    
    FOREIGN KEY (barcode) REFERENCES books(barcode)
);

-- Performance indexes
CREATE INDEX IF NOT EXISTS idx_books_barcode ON books(barcode);
CREATE INDEX IF NOT EXISTS idx_books_enrichment ON books(enrichment_timestamp);
CREATE INDEX IF NOT EXISTS idx_books_grin_state ON books(grin_state);
CREATE INDEX IF NOT EXISTS idx_books_storage_type ON books(storage_type);
CREATE INDEX IF NOT EXISTS idx_books_processing_timestamp ON books(processing_request_timestamp);

CREATE INDEX IF NOT EXISTS idx_processed_barcode ON processed(barcode);
CREATE INDEX IF NOT EXISTS idx_processed_session ON processed(session_id);
CREATE INDEX IF NOT EXISTS idx_failed_barcode ON failed(barcode);
CREATE INDEX IF NOT EXISTS idx_failed_session ON failed(session_id);

-- Status history indexes
CREATE INDEX IF NOT EXISTS idx_status_history_barcode ON book_status_history(barcode);
CREATE INDEX IF NOT EXISTS idx_status_history_type ON book_status_history(status_type);
CREATE INDEX IF NOT EXISTS idx_status_history_timestamp ON book_status_history(timestamp);
CREATE INDEX IF NOT EXISTS idx_status_history_latest ON book_status_history(barcode, status_type, timestamp DESC);
CREATE INDEX IF NOT EXISTS idx_status_history_etag ON book_status_history(barcode, status_type, timestamp DESC) WHERE json_extract(metadata, '$.encrypted_etag') IS NOT NULL;