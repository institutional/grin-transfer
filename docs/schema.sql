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
    
    -- MARC metadata fields (from METS XML parsing)
    marc_control_number TEXT,
    marc_date_type TEXT,
    marc_date_1 TEXT,
    marc_date_2 TEXT,
    marc_language TEXT,
    marc_lccn TEXT,
    marc_lc_call_number TEXT,
    marc_isbn TEXT,
    marc_oclc_numbers TEXT,
    marc_title TEXT,
    marc_title_remainder TEXT,
    marc_author_personal TEXT,
    marc_author_corporate TEXT,
    marc_author_meeting TEXT,
    marc_subjects TEXT,
    marc_genres TEXT,
    marc_general_note TEXT,
    marc_extraction_timestamp TEXT,
    
    -- Sync tracking for storage pipeline (status tracked in history table)
    storage_path TEXT, -- Path to decrypted archive in storage
    last_etag_check TEXT, -- ISO timestamp of last ETag verification
    encrypted_etag TEXT, -- Encrypted file's ETag for duplicate detection
    is_decrypted BOOLEAN DEFAULT FALSE, -- Whether decrypted version exists
    sync_timestamp TEXT, -- ISO timestamp of last successful sync
    
    -- Record keeping
    created_at TEXT NOT NULL,
    updated_at TEXT NOT NULL
);

-- Progress tracking for collection pipeline (deprecated)
CREATE TABLE IF NOT EXISTS processed (
    barcode TEXT PRIMARY KEY,
    timestamp TEXT NOT NULL,
    session_id INTEGER NOT NULL
);

-- Failed records tracking (deprecated)
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
    status_value TEXT NOT NULL, -- "pending", "requested", "in_process", "converted", "failed", "verified_unavailable", etc.
    timestamp TEXT NOT NULL,
    session_id TEXT, -- Optional identifier for tracking batches/runs
    metadata TEXT, -- Optional JSON metadata for additional context
    
    FOREIGN KEY (barcode) REFERENCES books(barcode)
);

-- Performance indexes
CREATE INDEX IF NOT EXISTS idx_books_grin_state ON books(grin_state);
CREATE INDEX IF NOT EXISTS idx_processed_session ON processed(session_id);
CREATE INDEX IF NOT EXISTS idx_failed_session ON failed(session_id);

-- Status history indexes
CREATE INDEX IF NOT EXISTS idx_status_history_type ON book_status_history(status_type);
CREATE INDEX IF NOT EXISTS idx_status_history_sync_completed ON book_status_history(status_type, status_value, barcode);
