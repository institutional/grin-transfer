"""
Text extraction module for OCR text extraction from book archives.

This module provides functionality to extract OCR text from decrypted
Google Books archives and output as JSON arrays for full-text search.
"""

from .main import main

__all__ = ["main"]
