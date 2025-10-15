#!/usr/bin/env python3
"""
Functions for parsing GRIN row data.

"""

from datetime import datetime
from typing import Any

from grin_transfer.client import GRINRow


def parse_grin_date(date_str: str) -> str:
    """Parse a date string from GRIN format to ISO format.

    Args:
        date_str: Date string in GRIN format (YYYY/MM/DD HH:MM)

    Returns:
        ISO formatted date string, or original if parsing fails
    """
    if not date_str:
        return date_str

    try:
        # GRIN format: YYYY/MM/DD HH:MM
        dt = datetime.strptime(date_str, "%Y/%m/%d %H:%M")
        return dt.isoformat()
    except ValueError:
        return date_str


def parse_grin_row(row: GRINRow) -> dict[str, Any]:
    """Process a GRINRow dict and map to BookRecord field names.

    Args:
        row: GRINRow dict from client with parsed book data

    Returns:
        Dictionary with BookRecord field names and processed values
    """
    if not row or not row.get("barcode"):
        return {}

    barcode = row.get("barcode", "unknown")

    # Create result with standard BookRecord field names
    result = {
        "barcode": barcode,
        "title": "",
        "scanned_date": None,
        "converted_date": None,
        "downloaded_date": None,
        "processed_date": None,
        "analyzed_date": None,
        "ocr_date": None,
        "google_books_link": "",
        "grin_state": None,
    }

    # Map unstructured field names to standard ones
    for key, value in row.items():
        if not value:
            continue

        key_lower = key.lower()

        # Map title field (usually first column after barcode)
        if "title" in key_lower:
            result["title"] = str(value).strip()
        # Map various date fields
        elif "scanned" in key_lower:
            result["scanned_date"] = parse_grin_date(value)
        elif "converted" in key_lower:
            result["converted_date"] = parse_grin_date(value)
        elif "downloaded" in key_lower:
            result["downloaded_date"] = parse_grin_date(value)
        elif "processed" in key_lower:
            result["processed_date"] = parse_grin_date(value)
        elif "analyzed" in key_lower:
            result["analyzed_date"] = parse_grin_date(value)
        elif "ocr" in key_lower:
            result["ocr_date"] = parse_grin_date(value)
        # Map Google Books link
        elif "google" in key_lower or "books" in key_lower:
            if "http" in str(value):
                result["google_books_link"] = str(value).strip()
        # Map state field
        elif "state" in key_lower or "status" in key_lower:
            result["grin_state"] = str(value).strip()

    return result
