"""
Utility functions for querying GRIN queues
"""

import logging
import time

logger = logging.getLogger(__name__)

# Cache for in_process queue data
_in_process_cache: dict[str, tuple[set[str], float]] = {}

BarcodeSet = set[str]


async def get_converted_books(grin_client, library_directory: str) -> set[str]:
    """Get set of books that are converted and ready for download.
    Args:
        grin_client: GRIN client instance
        library_directory: Library directory name
    Returns:
        set: Set of converted book barcodes
    """
    try:
        response_text = await grin_client.fetch_resource(library_directory, "_converted?format=text")
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


async def get_in_process_set(grin_client, library_directory: str) -> BarcodeSet:
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
