#!/usr/bin/env python3
"""
Book Collection Module.

This module provides functionality for collecting library book collections
to CSV format with metadata and processing state information.

Public API:
    BookCollector: Main class for book collection operations
    BookRecord: Data model for book records
    RateLimiter: Rate limiting utility for API requests
"""

from grin_to_s3.common import RateLimiter

from .collector import BookCollector
from .config import ExportConfig
from .models import BookRecord, BoundedSet

# Public API exports
__all__ = [
    "BookCollector",
    "BookRecord",
    "RateLimiter",
    "BoundedSet",
    "ExportConfig",
]

# Version info
__version__ = "2.0.0"
__author__ = "Harvard IDI"
__description__ = "Book Collection functionality for Google Books GRIN data"
