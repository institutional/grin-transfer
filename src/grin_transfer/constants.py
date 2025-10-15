#!/usr/bin/env python3
"""
Constants for grin_transfer application.

Centralized constants to eliminate duplication across the codebase.
"""

# Directory paths
from pathlib import Path
from typing import Literal

OUTPUT_DIR = Path("output")

# File names
BOOKS_EXPORT_CSV_FILENAME = "books_export.csv"

# GRIN API rate limiting
GRIN_RATE_LIMIT_QPS = 5.0

# GRIN queue and processing limits
GRIN_MAX_QUEUE_SIZE = 50000
GRIN_DEFAULT_BATCH_SIZE = 200


# Sync configuration defaults
DEFAULT_SYNC_DISK_SPACE_THRESHOLD = 0.9
DEFAULT_MAX_SEQUENTIAL_FAILURES = 10

# Task concurrency defaults
DEFAULT_SYNC_TASK_CHECK_CONCURRENCY = 2
DEFAULT_SYNC_TASK_DOWNLOAD_CONCURRENCY = 4
DEFAULT_SYNC_TASK_DECRYPT_CONCURRENCY = 40
DEFAULT_SYNC_TASK_UPLOAD_CONCURRENCY = 20
DEFAULT_SYNC_TASK_UNPACK_CONCURRENCY = 20
DEFAULT_SYNC_TASK_EXTRACT_MARC_CONCURRENCY = 20
DEFAULT_SYNC_TASK_EXTRACT_OCR_CONCURRENCY = 20
DEFAULT_SYNC_TASK_EXPORT_CSV_CONCURRENCY = 20
DEFAULT_SYNC_TASK_CLEANUP_CONCURRENCY = 20

# Worker concurrency defaults
DEFAULT_WORKER_CONCURRENCY = 150

# S3 connection pool configuration
# Set to 1.5x the worker concurrency to handle burst traffic
DEFAULT_S3_MAX_POOL_CONNECTIONS = 150

# Default directory names for local storage
LOCAL_STORAGE_DEFAULTS = {"bucket_raw": "raw", "bucket_meta": "meta", "bucket_full": "full"}

# Storage protocols inform behavior (e.g. r2 has the same behavior as s3)
STORAGE_PROTOCOLS = Literal["s3", "gcs", "file"]  # FIXME gcs should not really be here

# Storage types
STORAGE_TYPES = Literal["s3", "gcs", "local", "r2", "minio"]
