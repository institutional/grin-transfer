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
    
    -- Processing state (inferred from GRIN endpoint presence)
    processing_state TEXT, -- "all_books", "converted", "failed", "pending"
    
    -- Processing request tracking
    processing_request_status TEXT, -- "pending", "requested", "in_process", "converted", "failed"
    processing_request_timestamp TEXT, -- ISO timestamp when processing was requested
    
    -- GRIN enrichment fields (populated by enrichment pipeline)
    grin_state TEXT,
    viewability TEXT,
    opted_out TEXT,
    conditions TEXT,
    scannable TEXT,
    tagging TEXT,
    audit TEXT,
    material_error_percent TEXT,
    overall_error_percent TEXT,
    claimed TEXT,
    ocr_analysis_score TEXT,
    ocr_gtd_score TEXT,
    digitization_method TEXT,
    enrichment_timestamp TEXT,
    
    -- Export tracking
    csv_exported TEXT,
    csv_updated TEXT,
    
    -- Sync tracking for storage pipeline
    storage_type TEXT, -- "r2", "minio", "s3", "local"
    storage_path TEXT, -- Path to encrypted archive in storage
    storage_decrypted_path TEXT, -- Path to decrypted archive in storage
    last_etag_check TEXT, -- ISO timestamp of last ETag verification
    google_etag TEXT, -- Google's ETag for duplicate detection
    is_decrypted BOOLEAN DEFAULT FALSE, -- Whether decrypted version exists
    sync_status TEXT, -- "pending", "syncing", "completed", "failed"
    sync_timestamp TEXT, -- ISO timestamp of last sync attempt
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

-- Performance indexes
CREATE INDEX IF NOT EXISTS idx_books_barcode ON books(barcode);
CREATE INDEX IF NOT EXISTS idx_books_processing_state ON books(processing_state);
CREATE INDEX IF NOT EXISTS idx_books_enrichment ON books(enrichment_timestamp);
CREATE INDEX IF NOT EXISTS idx_books_grin_state ON books(grin_state);
CREATE INDEX IF NOT EXISTS idx_books_sync_status ON books(sync_status);
CREATE INDEX IF NOT EXISTS idx_books_storage_type ON books(storage_type);
CREATE INDEX IF NOT EXISTS idx_books_converted_sync ON books(grin_state, sync_status);
CREATE INDEX IF NOT EXISTS idx_books_processing_request ON books(processing_request_status);
CREATE INDEX IF NOT EXISTS idx_books_requested_converted ON books(processing_request_status, grin_state);

CREATE INDEX IF NOT EXISTS idx_processed_barcode ON processed(barcode);
CREATE INDEX IF NOT EXISTS idx_processed_session ON processed(session_id);
CREATE INDEX IF NOT EXISTS idx_failed_barcode ON failed(barcode);
CREATE INDEX IF NOT EXISTS idx_failed_session ON failed(session_id);