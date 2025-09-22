"""
Utility functions for querying GRIN queues
"""

import logging
import time

from grin_to_s3.client import GRINClient

logger = logging.getLogger(__name__)

# Cache for in_process queue data
_in_process_cache: dict[str, tuple[set[str], float]] = {}

BarcodeSet = set[str]


async def get_unconverted_books(db) -> set[str]:
    """Get the barcodes for books which have never been converted for download.

    Returns books that:
    - Have never been requested for processing (processing_request_timestamp IS NULL or empty string)
    - Have never been converted by GRIN (converted_date IS NULL or empty string)
    - Are not checked in (grin_check_in_date IS NULL or empty string)
    - Have no GRIN state or are not in CHECKED_IN or NOT_AVAILABLE_FOR_DOWNLOAD states
    - Are not marked as unavailable

    Args:
        db: Database tracker instance

    Returns:
        set: Set of unconverted book barcodes
    """

    query = """
        SELECT barcode FROM books
        WHERE NULLIF(processing_request_timestamp, '') IS NULL
        AND NULLIF(converted_date, '') IS NULL
        AND NULLIF(grin_check_in_date, '') IS NULL
        AND (NULLIF(grin_state, '') IS NULL OR grin_state NOT IN ('CHECKED_IN', 'NOT_AVAILABLE_FOR_DOWNLOAD'))
        AND barcode NOT IN (
            SELECT DISTINCT barcode FROM book_status_history
            WHERE status_type = 'conversion' AND status_value = 'unavailable'
        )
    """

    cursor = await db._execute_query(query, ())
    rows = await cursor.fetchall()
    unconverted_barcodes = {row[0] for row in rows}

    logger.debug(f"Found {len(unconverted_barcodes)} unconverted books in database")
    return unconverted_barcodes


async def get_converted_books(grin_client: GRINClient, library_directory: str) -> set[str]:
    """Get set of books that are converted and ready for download.
    Args:
        grin_client: GRIN client instance
        library_directory: Library directory name
    Returns:
        set: Set of converted book barcodes
    """
    try:
        response_text = await grin_client.fetch_resource(library_directory, "_converted?format=text", timeout=240)
        lines = response_text.strip().split("\n")
        converted_barcodes = set()
        for line in lines:
            if line.strip() and ".tar.gz.gpg" in line:
                barcode = line.strip().replace(".tar.gz.gpg", "")
                converted_barcodes.add(barcode)
        return converted_barcodes
    except Exception as e:
        error_type = type(e).__name__
        error_msg = str(e) if str(e) else "No error message"
        logger.warning(f"Failed to get converted books: {error_type}: {error_msg}")
        return set()


async def get_in_process_set(grin_client: GRINClient, library_directory: str) -> BarcodeSet:
    """Get set of books currently in GRIN processing queue with caching.
    Args:
        grin_client: GRIN client instance
        library_directory: Library directory name
    Returns:
        Set of barcodes currently in processing queue
    """
    current_time = time.time()
    cache_key = library_directory
    # Check cache (1 hour TTL)
    if cache_key in _in_process_cache:
        books, cached_time = _in_process_cache[cache_key]
        if current_time - cached_time < 3600:
            logger.debug(f"Using cached in_process data for {library_directory}")
            return books
    try:
        response_text = await grin_client.fetch_resource(library_directory, "_in_process?format=text")
        lines = response_text.strip().split("\n")
        in_process_barcodes = set()
        for line in lines:
            clean_line = line.strip()
            if clean_line:
                if ".tar.gz.gpg" in clean_line:
                    barcode = clean_line.replace(".tar.gz.gpg", "")
                    in_process_barcodes.add(barcode)
                else:
                    # Handle bare barcode format
                    in_process_barcodes.add(clean_line)
        # Update cache
        _in_process_cache[cache_key] = (in_process_barcodes, current_time)
        logger.debug(f"Fetched {len(in_process_barcodes)} in_process books for {library_directory}")
        return in_process_barcodes
    except Exception as e:
        error_type = type(e).__name__
        error_msg = str(e) if str(e) else "No error message"
        logger.warning(f"Failed to get in_process books: {error_type}: {error_msg}")
        return set()
